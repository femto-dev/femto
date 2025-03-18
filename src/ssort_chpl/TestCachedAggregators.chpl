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

  femto/src/ssort_chpl/CachedAggregators.chpl
*/

module TestUtility {

use CachedAggregators;
use BlockDist;
use Random;

proc testCachedDstAggregator(n:int) {
  // store numbers in forward order in Fwd
  var Fwd =  blockDist.createArray(0..<n, int);
  Fwd = 0..<n;

  // store numbers in reverse order in Rev
  var Rev = blockDist.createArray(0..<n, int);
  Rev = 0..<n by -1;

  var A = Rev;
  assert(A.equals(Rev));

  var B = blockDist.createArray(0..<n, int);

  // Set B to the elements of Rev reordered by value
  B = 0;
  forall elt in A with (var agg = new CachedDstAggregator(int)) {
    agg.copy(B[elt], elt);
  }
  assert(B.equals(Fwd));

  // test a simple flush scenario
  {
    A = Rev;
    var agg = new CachedDstAggregator(int);
    agg.copy(A[n-1], 42);
    agg.flush();
    assert(A[n-1] == 42);
  }
}

proc testCachedDstAggregatorRandom(n: int, seed: int) {
  writeln("testCachedDstAggregatorRandom(n=", n, ", seed=", seed, ")");

  var LocIdxs: [0..<n] int = 0..<n;
  Random.shuffle(LocIdxs, seed=seed);

  var Idxs = blockDist.createArray(0..<n, int);
  Idxs = LocIdxs;

  var Fwd = blockDist.createArray(0..<n, int);
  Fwd = 0..<n;

  var A = blockDist.createArray(0..<n, int);
  A = Idxs;

  var B = blockDist.createArray(0..<n, int);

  forall (src, idx) in zip(A, Idxs)
  with (var agg = new CachedDstAggregator(int)) {
    agg.copy(B[idx], src);
  }

  assert(B.equals(Fwd));
}

proc testCachedSrcAggregator(n:int) {
  // store numbers in forward order in Fwd
  var Fwd =  blockDist.createArray(0..<n, int);
  Fwd = 0..<n;

  // store numbers in reverse order in Rev
  var Rev = blockDist.createArray(0..<n, int);
  Rev = 0..<n by -1;

  var A = Rev;
  assert(A.equals(Rev));

  var B = blockDist.createArray(0..<n, int);

  // Set B to the elements of Rev reordered by value
  B = 0;
  // Set B to the elements of A reordered by value
  forall (elt, i) in zip(B, B.domain)
  with (var agg = new CachedSrcAggregator(int)) {
    agg.copy(elt, A[n-1-i]);
  }
  assert(B.equals(Fwd));

  // test a simple flush scenario
  {
    A = Rev;
    A[n-1] = 42;
    var agg = new CachedSrcAggregator(int);
    var x = 0;
    agg.copy(x, A[n-1]);
    agg.flush();
    assert(x == 42);
  }
}
proc testCachedSrcAggregatorRandom(n: int, seed: int) {
  writeln("testCachedSrcDstAggregatorRandom(n=", n, ", seed=", seed, ")");

  var LocIdxs: [0..<n] int = 0..<n;
  Random.shuffle(LocIdxs, seed=seed);

  var Idxs = blockDist.createArray(0..<n, int);
  Idxs = LocIdxs;

  var Fwd = blockDist.createArray(0..<n, int);
  Fwd = 0..<n;

  var A = blockDist.createArray(0..<n, int);
  // set A to the inverse permutation of Idxs
  forall (idx,i) in zip(Idxs, Idxs.domain)
  with (var agg = new CachedDstAggregator(int)) {
    agg.copy(A[idx], i);
  }

  var B = blockDist.createArray(0..<n, int);
  // set B to the combination of the two permutations
  forall (dst, idx) in zip(B, Idxs)
  with (var agg = new CachedSrcAggregator(int)) {
    agg.copy(dst, A[idx]);
  }

  assert(B.equals(Fwd));
}

proc main() {
  for n in [1, 2, 5, 10, 100, 1000, 10_000, 100_000, 1_000_000] {
    writeln("testCachedDstAggregator(", n, ")");
    testCachedDstAggregator(n);

    writeln("testCachedSrcAggregator(", n, ")");
    testCachedSrcAggregator(n);

    for seed in [1, 13, 99] {
      testCachedDstAggregatorRandom(n, seed);
      testCachedSrcAggregatorRandom(n, seed);
    }

    // test also something oversubscribed
    writeln("testCachedDstAggregator(", n, ") oversubscribed with 10 tasks");
    coforall taskId in 1..10 {
      testCachedDstAggregator(n);
    }

    writeln("testCachedSrcAggregator(", n, ") oversubscribed with 10 tasks");
    coforall taskId in 1..10 {
      testCachedSrcAggregator(n);
    }
  }
  writeln("Done");
}


}
