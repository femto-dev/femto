/*
  2024 Michael Ferguson <michaelferguson@acm.org>

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.

  femto/src/ssort_chpl/SuffixSort.chpl
*/

module SuffixSort {


config param DEFAULT_PERIOD = 57;
config param DEFAULT_LCP_SAMPLE = 64;
config param EXTRA_CHECKS = false;
config param TRACE = false;
config param TIMING = false;
config param STATS = false;
config type CACHED_DATA_TYPE = nothing;

// these control readAllFiles / recursive subproblems
//config param TEXT_REPLICATED = false;
//config param TEXT_BLOCK = false;
//config param TEXT_NON_DIST = false;

// don't fall back on non-distributed arrays for CHPL_COMM=none
config param DISTRIBUTE_EVEN_WITH_COMM_NONE = false;

// how much padding does the algorithm need at the end of the input?
param INPUT_PADDING = 8;

config const TRUNCATE_INPUT_TO: int = max(int);
config const VERBOSE_COMMS = false;

/* TODO after https://github.com/chapel-lang/chapel/issues/25569 is fixed
include public module DifferenceCovers;
include private module SuffixSortImpl;
include private module TestSuffixSort;
include private module TestDifferenceCovers;
*/

public use DifferenceCovers;
private use SuffixSortImpl;
private use Utility;

private import IO;
private import Time;
private import List;
private import Help;
private import CommDiagnostics;

proc computeSuffixArray(Input: [], const n: Input.domain.idxType) {
  if !(Input.domain.rank == 1 &&
       Input.domain.low == 0 &&
       Input.domain.high == Input.domain.size-1) {
    halt("computeSuffixArray requires 1-d array over 0..n");
  }
  if n + INPUT_PADDING > Input.size {
    halt("computeSuffixArray needs extra space at the end of the array");
    // expect it to be zero-padded past n.
  }

  const nTasksPerLocale = computeNumTasks(ignoreRunning=true);

  type characterType = Input.eltType;
  type offsetType = Input.idxType;
  type wordType = uint(numBits(offsetType));

  const bitsPerChar = computeBitsPerChar(Input, n);

  if TRACE {
    writeln("computed bitsPerChar=", bitsPerChar);
  }

  // now proceed with suffix sorting with the packed data
  // and a compile-time known bitsPerChar

  proc helper(param pBitsPerChar) {
    if TRACE {
      writeln("using bitsPerChar=", pBitsPerChar);
    }
    // pack using pBitsPerChar
    const packed = packInput(wordType, Input, n, pBitsPerChar);
    assert(pBitsPerChar >= bitsPerChar);
    // configure suffix sorter
    const cfg = new ssortConfig(idxType = Input.idxType,
                                offsetType = Input.idxType,
                                unsignedOffsetType = wordType,
                                loadWordType = wordType,
                                bitsPerChar = pBitsPerChar,
                                n = n,
                                cover = new differenceCover(DEFAULT_PERIOD),
                                locales = Locales,
                                nTasksPerLocale = nTasksPerLocale);
    // suffix sort
    return ssortDcx(cfg, packed);
  }

  // dispatch to the version instantiated for a close bitsPerChar
  // note that 2, 3 or 4 are common with fasta files

       if bitsPerChar <=  2 { return helper(2); }
  else if bitsPerChar <=  4 { return helper(4); }
  else if bitsPerChar <=  8 { return helper(8); }
  else if bitsPerChar <= 16 { return helper(16); }
  else if bitsPerChar <= 32 { return helper(32); }
  else                      { return helper(64); }
}


proc computeSuffixArrayAndLCP(input: [], const n: input.domain.idxType,
                              out SA: [],
                              out LCP) {
  writeln("computing suffix array");
  SA = computeSuffixArray(input, n);
  writeln("computing LCP array");
  LCP = lcpParPlcp(input, n, SA);
}

/* Compute and return the sparse PLCP array based on the input text and suffix
   array. The sparse PLCP array can be used to compute LCP[i] while using less
   space.

   The algorithm is based upon "Permuted Longest-Common-Prefix Array" by Juha
   Kärkkäinen, Giovanni Manzini, and Simon J. Puglisi; and also
   "Fast Parallel Computation of Longest Common Prefixes"
   by Julian Shun.
*/
proc computeSparsePLCP(thetext: [], const n: thetext.domain.idxType,
                       const SA: [], param q=DEFAULT_LCP_SAMPLE) {
  return doComputeSparsePLCP(thetext, n, SA, q);
}

/* Given a sparse PLCP array computed as above in computeSparsePLCP,
   along with the parameter q and a suffix array position 'i', return
   LCP[i]. */
proc lookupLCP(thetext: [], const n: thetext.domain.idxType, const SA: [],
               const sparsePLCP: [], i: n.type, param q=DEFAULT_LCP_SAMPLE) {
  return doLookupLCP(thetext, n, SA, sparsePLCP, i, q);
}

proc usage(args: [] string) {
  writeln("usage: ", args[0], " <files-and-directories>");
  writeln("this program times the suffix array computation");
  Help.printUsage();
}

proc main(args: [] string) throws {
  var inputFilesList: List.list(string);

  for arg in args[1..] {
    if arg == "--help" || arg == "-h" {
      usage(args);
      return 0;
    }
    if arg.startsWith("-") {
      writeln("argument not handled ", arg);
      usage(args);
      return 1;
    }
    gatherFiles(inputFilesList, arg);
  }

  if inputFilesList.size == 0 {
    writeln("please specify input files and directories");
    usage(args);
    return 1;
  }

  var readTime = startTime(true);
  const allData; //: [] uint(8);
  const allPaths; //: [] string;
  const concisePaths; // : [] string
  const fileStarts; //: [] int;
  const totalSize: int;
  const sequenceDescriptions; //: [] string;
  const sequenceStarts; //: [] int;
  readAllFiles(inputFilesList,
               Locales,
               allData=allData,
               allPaths=allPaths,
               concisePaths=concisePaths,
               fileStarts=fileStarts,
               totalSize=totalSize,
               sequenceDescriptions=sequenceDescriptions,
               sequenceStarts=sequenceStarts);

  //writeln("Files are: ", concisePaths);
  //writeln("FileStarts are: ", fileStarts);
  reportTime(readTime, "reading input", totalSize, 1);

  const n = min(TRUNCATE_INPUT_TO, totalSize);

  writeln("Computing suffix array");

  if VERBOSE_COMMS {
    CommDiagnostics.startVerboseComm();
  }

  if totalSize == n {
    var saTime = startTime(true);
    var SA = computeSuffixArray(allData, n);
    reportTime(saTime, "suffix array construction", n, 1);
  } else {
    writeln("Truncating input to ", n, " bytes");
    var TruncatedDom = makeBlockDomain(0..<n+INPUT_PADDING, Locales);
    var TruncatedInput:[TruncatedDom] uint(8);
    TruncatedInput[0..<n] = allData[0..<n];
    var saTime = startTime(true);
    var SA = computeSuffixArray(TruncatedInput, n);
    reportTime(saTime, "suffix array construction", n, 1);
  }

  if VERBOSE_COMMS {
    CommDiagnostics.stopVerboseComm();
  }

  return 0;
}


}
