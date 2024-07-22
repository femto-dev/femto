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
import SuffixSort.TRACE;

use Partitioning;

import Sort.{defaultComparator,isSorted};
import Random;
import Math;
import Map;

// nSplit positive: create that many splitters
// nSplit negative: create a sample from the Input array
proc testPartition(n: int, nSplit: int, useEqualBuckets: bool, nTasks: int) {
  writeln("testPartition(n=", n, ", nSplit=", nSplit, ", ",
          "useEqualBuckets=", useEqualBuckets, ", nTasks=", nTasks, ")");

  var Input: [0..<n] int = 0..<n by -1;
  var Output: [0..<n] int = -1;

  var InputCounts:Map.map(int, int);

  for x in Input {
    InputCounts[x] += 1;
  }

  const nSplitters = (1 << Math.log2(abs(nSplit))) - 1;
  var UseSplitters:[0..<nSplitters+1] int;
  for i in 0..<nSplitters {
    UseSplitters[i] = 1 + i * n / nSplitters;
  }

  const sp;
  if nSplit > 0 {
    sp = new splitters(UseSplitters, useEqualBuckets);
  } else {
    var randNums = new Random.randomStream(int);
    var SplittersSampleDom = {0..<abs(10*nSplit)};
    var SplittersSample:[SplittersSampleDom] int;
    for (x, r) in zip(SplittersSample,
                      randNums.next(SplittersSampleDom, 0, n-1)) {
      x = r;
    }
    sp = new splitters(SplittersSample, abs(nSplit), defaultComparator,
                       howSorted=sortLevel.unsorted);
  }

  assert(isSorted(sp.sortedStorage));

  const nBuckets = sp.numBuckets;
  const hasEqualityBuckets = sp.hasEqualityBuckets;

  const counts =
    partition(Input, Output, sp, defaultComparator, 0, n-1, nTasks);
  assert(counts.size == nBuckets);

  const ends = + scan counts;

  var total = 0;

  for bin in 0..<nBuckets {
    const binSize = counts[bin];
    const binStart = ends[bin] - binSize;
    const binEnd = binStart + binSize - 1;

    total += binSize;

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

  var OutputCounts:Map.map(int, int);
  for x in Output {
    OutputCounts[x] += 1;
  }

  assert(InputCounts == OutputCounts);

  assert(total == n);
}

proc testPartitionsEven(n: int, nSplit: int) {
  writeln("testPartitionsEven(n=", n, ", nSplit=", nSplit, ")");

  var Input: [0..<n] int = 0..<n by -1;
  var Output: [0..<n] int = -1;

  var Sample = Input;
  const sp = new splitters(Sample, nSplit, defaultComparator,
                           howSorted=sortLevel.unsorted);
  assert(isSorted(sp.sortedStorage));

  const nBuckets = sp.numBuckets;
  const hasEqualityBuckets = sp.hasEqualityBuckets;

  const counts = partition(Input, Output, sp, defaultComparator, 0, n-1, 1);
  assert(counts.size == nBuckets);

  var minSize = max(int);
  var maxSize = -1;
  for bin in 0..<nBuckets {
    const binSize = counts[bin];

    if TRACE && nBuckets < 100 {
      writeln("  bucket ", bin, " has ", binSize, " elements");
    }

    minSize = min(minSize, binSize);
    maxSize = max(maxSize, binSize);
  }

  if TRACE {
    writeln("  minSize ", minSize);
    writeln("  maxSize ", maxSize);
  }


  assert(minSize + 2 >= maxSize);
}

proc testPartitionSingleSplitter(n: int) {
  writeln("testPartitionSingleSplitter(n=", n, ")");

  var Input: [0..<n] int = 0..<n by -1;
  var Output: [0..<n] int = -1;

  var Sample = [n/2, n/2, n/2, n/2, n/2, n/2];
  const sp = new splitters(Sample, 100, defaultComparator,
                           howSorted=sortLevel.unsorted);
  assert(isSorted(sp.sortedStorage));

  const nBuckets = sp.numBuckets;
  assert(sp.hasEqualityBuckets);
  assert(nBuckets == 3); // < == and > buckets

  const counts = partition(Input, Output, sp, defaultComparator, 0, n-1, 1);
  assert(counts.size == nBuckets);

  var total = 0;
  var minSize = max(int);
  var maxSize = -1;
  for bin in 0..<nBuckets {
    const binSize = counts[bin];

    total += binSize;
  }

  assert(total == n);
}

proc testPartitions() {
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

  // test with random samples
  testPartition(10, -4, false, 1);
  testPartition(100, -20, false, 1);
  testPartition(10, -4, false, 2);
  testPartition(100, -20, false, 2);
  testPartition(10000, -100, false, 8);

  // test partitions are even with a perfect sample
  testPartitionsEven(10, 4);
  testPartitionsEven(1000, 20);
  testPartitionsEven(1000, 100);
  testPartitionsEven(10000, 80);
  testPartitionsEven(10000, 9876);

  // test that creating a single splitter works OK
  testPartitionSingleSplitter(10);
}

proc main() {
  serial {
    writeln("Testing partitioning with one task");
    testPartitions();
  }

  writeln("Testing partitioning with many tasks");
  testPartitions();

  writeln("TestPartitioning OK");
}


} // end module TestPartitioning
