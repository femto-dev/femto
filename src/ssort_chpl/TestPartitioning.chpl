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

  femto/src/ssort_chpl/TestPartitioning.chpl
*/

module TestPartitioning {


import SuffixSort.EXTRA_CHECKS;

use Partitioning;

import Sort.{defaultComparator,isSorted};
use Math;
use Map;

proc testPartition(n: int, nSplit: int, useEqualBuckets: bool, nTasks: int) {
  writeln("testPartition(n=", n, ", nSplit=", nSplit, ", ",
          "useEqualBuckets=", useEqualBuckets, ", nTasks=", nTasks, ")"); 

  var Input: [0..<n] int = 0..<n by -1;
  var Output: [0..<n] int = -1;

  var InputCounts:map(int, int);

  for x in Input {
    InputCounts[x] += 1;
  }

  const nSplitters = (1 << log2(nSplit)) - 1;
  var UseSplitters:[0..<nSplitters] int;
  for i in 0..<nSplitters {
    UseSplitters[i] = 1 + i * n / nSplitters;
  }

  const sp = new splitters(UseSplitters, useEqualBuckets);
  assert(isSorted(sp.sortedStorage));

  const nBuckets = sp.numBuckets;
  const hasEqualityBuckets = sp.hasEqualityBuckets;

  const counts = 
    partition(Input, Output, sp, defaultComparator, 0, n-1, nTasks);
  assert(counts.size == nBuckets);

  const ends = + scan counts;

  for bin in 0..<nBuckets {
    const binSize = counts[bin];
    const binStart = ends[bin] - binSize;
    const binEnd = binStart + binSize - 1;

    if bin == 0 {
      assert(binStart == 0);
    }
    if bin == nBuckets-1 {
      assert(ends[bin] == n);
    }

    var lower = -1;
    var upper = -1;
    var equals = -1;
    if sp.bucketHasLowerBound(bin) {
      lower = sp.bucketLowerBound(bin);
    }
    if sp.bucketHasUpperBound(bin) {
      upper = sp.bucketUpperBound(bin);
    }
    if sp.bucketHasEqualityBound(bin) {
      equals = sp.bucketEqualityBound(bin);
    }

    for i in binStart..binEnd {
      if lower != -1 then
        assert(lower < Output[i]);
      if upper != -1 {
        if hasEqualityBuckets then
          assert(Output[i] < upper);
        else
          assert(Output[i] <= upper);
      }
      if equals != -1 then
        assert(Output[i] == equals);
    }
  }

  var OutputCounts:map(int, int);
  for x in Output {
    OutputCounts[x] += 1;
  }

  assert(InputCounts == OutputCounts);
}

proc main() {
  testPartition(10, 4, false, 1);
  testPartition(10, 4, true, 1);
  testPartition(100, 20, false, 1);
  testPartition(100, 20, true, 1);

  testPartition(10, 4, false, 2);
  testPartition(10, 4, true, 2);
  testPartition(100, 20, false, 2);
  testPartition(100, 20, true, 2);
  testPartition(10000, 100, false, 8);
  testPartition(10000, 100, true, 8);

  writeln("TestPartitioning OK");
}


} // end module TestPartitioning