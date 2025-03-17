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

config const n = 1_000_000;

proc testCachedDstAggregator() {
  writeln("testCachedDstAggregator");

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
  forall elt in A with (var agg = new CachedDstAggregator()) {
    agg.copy(B[elt], elt);
  }
  assert(B.equals(Fwd));

  // test a simple flush scenario
  {
    A = Rev;
    var agg = new CachedDstAggregator();
    agg.copy(A[n-1], 42);
    agg.flush();
    assert(A[n-1] == 42);
  }
}
proc testCachedSrcAggregator() {
  writeln("testCachedSrcAggregator");

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
  with (var agg = new CachedSrcAggregator()) {
    agg.copy(elt, A[n-1-i]);
  }
  assert(B.equals(Fwd));

  // test a simple flush scenario
  {
    A = Rev;
    A[n-1] = 42;
    var agg = new CachedSrcAggregator();
    var x = 0;
    agg.copy(x, A[n-1]);
    agg.flush();
    assert(x == 42);
  }
}

testCachedDstAggregator();
testCachedSrcAggregator();
writeln("Done");

}
