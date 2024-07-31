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
config param DEFAULT_K = 81;
config param ENABLE_CACHED_TEXT = true;
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

private import IO;
private import Time;

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
                              out LCP: []) {
  SA = computeSuffixArray(input, n);
  LCP = lcpParPlcp(input, n, SA);
}

/*
proc computeUniqueK(input: [], const n: input.domain.idxType) {
  if !(input.domain.rank == 1 &&
       input.domain.low == 0 &&
       input.domain.high == input.domain.size-1) {
    halt("computeUniqueK requires 1-d array over 0..n");
  }
  if n + INPUT_PADDING > input.size {
    halt("computeUniqueK needs extra space at the end of the array");
    // expect it to be zero-padded past n.
  }

  const cfg = new ssortConfig(idxType = input.idxType,
                              characterType = input.eltType,
                              offsetType = input.idxType,
                              cachedDataType = CACHED_DATA_TYPE,
                              loadWordType = LOAD_WORD_TYPE,
                              cover = new differenceCover(DEFAULT_PERIOD));
  minUniqueK(cfg, input, n, DEFAULT_K);
}*/

config const input:string;

proc main() throws {
  if input == "" {
    writeln("please use --input to specify an input file");
    return 1;
  }

  writeln("Reading in ", input);
  var f = IO.open(input, IO.ioMode.r);
  const n = f.size;
  var thetext:[0..<n+INPUT_PADDING] uint(8);
  f.reader().readAll(thetext);

  var t: Time.stopwatch;

  writeln("Computing suffix array");
  t.reset();
  t.start();
  var SA = computeSuffixArray(thetext, n);
  t.stop();

  writeln("suffix array construction took of ", n, " bytes ",
          "took ", t.elapsed(), " seconds");
  writeln(n / 1000.0 / 1000.0 / t.elapsed(), " MB/s");

  return 0;
}


}
