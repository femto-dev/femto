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


config param DEFAULT_PERIOD = 133;
config param DEFAULT_LCP_SAMPLE = 64;
config param EXTRA_CHECKS = false;
config param TRACE = false;
config type CACHED_DATA_TYPE = nothing;
config type LOAD_WORD_TYPE = uint;

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

proc computeSuffixArray(input: [], const n: input.domain.idxType) {
  if !(input.domain.rank == 1 &&
       input.domain.low == 0 &&
       input.domain.high == input.domain.size-1) {
    halt("computeSuffixArray requires 1-d array over 0..n");
  }
  if n + INPUT_PADDING > input.size {
    halt("computeSuffixArray needs extra space at the end of the array");
    // expect it to be zero-padded past n.
  }

  const cfg = new ssortConfig(idxType = input.idxType,
                              characterType = input.eltType,
                              offsetType = input.idxType,
                              cachedDataType = CACHED_DATA_TYPE,
                              loadWordType = LOAD_WORD_TYPE,
                              cover = new differenceCover(DEFAULT_PERIOD));

  return ssortDcx(cfg, input, n);
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

proc main(args: [] string) throws {
  var inputFilesList: List.list(string);

  for arg in args[1..] {
    if arg.startsWith("-") {
      halt("argument not handled ", arg);
    }
    gatherFiles(inputFilesList, arg);
  }

  if inputFilesList.size == 0 {
    writeln("please specify input files and directories");
    return 1;
  }

  const allData; //: [] uint(8);
  const allPaths; //: [] string;
  const concisePaths; // : [] string
  const fileSizes; //: [] int;
  const fileStarts; //: [] int;
  const totalSize: int;
  readAllFiles(inputFilesList,
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

  Time.sleep(20);

  return 0;
}


}
