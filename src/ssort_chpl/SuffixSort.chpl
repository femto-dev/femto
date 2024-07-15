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
config param ENABLE_CACHED_TEXT = true;
config param EXTRA_CHECKS = false;
config param TRACE = false;

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

proc computeSuffixArray(input: [], const n: input.domain.idxType) {
  if !(input.domain.rank == 1 &&
       input.domain.low == 0 &&
       input.domain.low == input.domain.first &&
       input.domain.high == input.domain.last &&
       input.domain.high <= n) {
    halt("computeSuffixArray requires 1-d array over 0..n");
  }
  if n + INPUT_PADDING > input.domain.high {
    halt("computeSuffixArray needs extra space at the end of the array");
    // expect it to be zero-padded past n.
  }

  type offsetType = input.domain.idxType;
  type characterType = input.eltType;
  type cachedDataType = if ENABLE_CACHED_TEXT then uint(64) else nothing;

  return ssortDcx(input, n,
                  cover=new differenceCover(DEFAULT_PERIOD),
                  characterType=characterType, offsetType=offsetType);
}


}
