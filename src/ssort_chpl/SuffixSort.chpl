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


config param DEFAULT_PERIOD = 7;
config param DEFAULT_LCP_SAMPLE = 64;
config param EXTRA_CHECKS = false;
config param TRACE = true;
config param TIMING = false;
config type CACHED_DATA_TYPE = nothing;

// these control readAllFiles / recursive subproblems
//config param TEXT_REPLICATED = false;
//config param TEXT_BLOCK = false;
//config param TEXT_NON_DIST = false;

// don't fall back on non-distributed arrays for CHPL_COMM=none
config param DISTRIBUTE_EVEN_WITH_COMM_NONE = false;

// how much padding does the algorithm need at the end of the input?
param INPUT_PADDING = 8;


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
  if numBits(characterType) <= 16 &&
     numBits(characterType) <= numBits(offsetType) {
    try {
      var bitsPerChar = 0;
      type wordType = uint(numBits(offsetType));
      const packed = packInput(wordType, Input, n, /*out*/ bitsPerChar);
      assert(1 <= bitsPerChar && bitsPerChar <= numBits(characterType));

      proc helper(param pBitsPerChar) {
        assert(pBitsPerChar == bitsPerChar);
        const cfg = new ssortConfig(idxType = Input.idxType,
                                    offsetType = Input.idxType,
                                    unsignedOffsetType = wordType,
                                    loadWordType = wordType,
                                    bitsPerChar = pBitsPerChar,
                                    n = n,
                                    cover = new differenceCover(DEFAULT_PERIOD),
                                    locales = Locales,
                                    nTasksPerLocale = nTasksPerLocale);
        return ssortDcx(cfg, packed);
      }

      // dispatch to the version instantiated for bitsPerChar
           if bitsPerChar ==  1 { return helper(1); }
      else if bitsPerChar ==  2 { return helper(2); }
      else if bitsPerChar ==  3 { return helper(3); }
      else if bitsPerChar ==  4 { return helper(4); }
      else if bitsPerChar ==  5 { return helper(5); }
      else if bitsPerChar ==  6 { return helper(6); }
      else if bitsPerChar ==  7 { return helper(7); }
      else if bitsPerChar ==  8 { return helper(8); }
      else if bitsPerChar ==  9 { return helper(9); }
      else if bitsPerChar == 10 { return helper(10); }
      else if bitsPerChar == 11 { return helper(11); }
      else if bitsPerChar == 12 { return helper(12); }
      else if bitsPerChar == 13 { return helper(13); }
      else if bitsPerChar == 14 { return helper(14); }
      else if bitsPerChar == 15 { return helper(16); }
      else if bitsPerChar == 16 { return helper(16); }

    } catch e: Error {
      writeln(e);
      // we can continue without packing
    }
  }

  halt("unsupported configuration for computeSuffixArray");
  // TODO: support with a more flexible packInput.
  /*
  const cfg = new ssortConfig(idxType = Input.idxType,
                              offsetType = Input.idxType,
                              unsignedOffsetType = uint(numBits(
                              bitsPerChar = numBits(characterType),
                              n = n,
                              cover = new differenceCover(DEFAULT_PERIOD),
                              locales = Locales,
                              nTasksPerLocale = nTasksPerLocale);

  return ssortDcx(cfg, Input);*/
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

  const allData; //: [] uint(8);
  const allPaths; //: [] string;
  const concisePaths; // : [] string
  const fileSizes; //: [] int;
  const fileStarts; //: [] int;
  const totalSize: int;
  readAllFiles(inputFilesList,
               Locales,
               allData=allData,
               allPaths=allPaths,
               concisePaths=concisePaths,
               fileSizes=fileSizes,
               fileStarts=fileStarts,
               totalSize=totalSize);

  const n = totalSize;
  writeln("Files are: ", concisePaths);
  writeln("FileStarts are: ", fileStarts);

  var t: Time.stopwatch;

  writeln("Computing suffix array");
  t.reset();
  t.start();
  var SA = computeSuffixArray(allData, totalSize);
  t.stop();

  writeln("suffix array construction of ", n, " bytes ",
          "took ", t.elapsed(), " seconds");
  writeln(n / 1000.0 / 1000.0 / t.elapsed(), " MB/s");

  return 0;
}


}
