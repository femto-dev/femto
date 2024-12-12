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

  femto/src/ssort_chpl/TestUtility.chpl
*/

module TestUtility {


use Utility;
import IO;
import FileSystem;
import BlockDist;

proc testTriangles() {
  writeln("testTriangles");

  assert(triangleSize(0) == 0);
  assert(triangleSize(1) == 0);

  /*
     - -
     0 -
   */
  assert(triangleSize(2) == 1);
  assert(flattenTriangular(1,0) == 0);
  assert(flattenTriangular(0,1) == 0);

  /*
     - - -
     0 - -
     1 2 -
   */
  assert(triangleSize(3) == 3);
  assert(flattenTriangular(1,0) == 0);
  assert(flattenTriangular(0,1) == 0);

  assert(flattenTriangular(2,0) == 1);
  assert(flattenTriangular(0,2) == 1);
  assert(flattenTriangular(2,1) == 2);
  assert(flattenTriangular(1,2) == 2);

  /*
     - - - -
     0 - - -
     1 2 - -
     3 4 5 -
   */

  assert(triangleSize(4) == 6);
  assert(flattenTriangular(1,0) == 0);
  assert(flattenTriangular(0,1) == 0);

  assert(flattenTriangular(2,0) == 1);
  assert(flattenTriangular(0,2) == 1);
  assert(flattenTriangular(2,1) == 2);
  assert(flattenTriangular(1,2) == 2);

  assert(flattenTriangular(3,0) == 3);
  assert(flattenTriangular(0,3) == 3);
  assert(flattenTriangular(3,1) == 4);
  assert(flattenTriangular(1,3) == 4);
  assert(flattenTriangular(3,2) == 5);
  assert(flattenTriangular(2,3) == 5);
}

proc testBsearch() {
  writeln("testBsearch");

  assert(bsearch([1], 0) == -1);
  assert(bsearch([1], 1) == 0);
  assert(bsearch([1], 2) == 0);

  assert(bsearch([1,3], 0) == -1);
  assert(bsearch([1,3], 1) == 0);
  assert(bsearch([1,3], 2) == 0);
  assert(bsearch([1,3], 3) == 1);
  assert(bsearch([1,3], 4) == 1);
}

private proc arrToString(arr: [] uint(8)) {
  var result: string;
  for elt in arr {
    result.appendCodepointValues(elt);
  }
  return result;
}

private proc bytesToArray(s: bytes) {
  const nWithPadding = s.size; // no padding for this test
  const A:[0..<nWithPadding] uint(8) =
    for i in 0..<nWithPadding do
      if i < s.size then s[i] else 0;

  return A;
}

private proc bytesToArray(s: string) {
  return bytesToArray(s:bytes);
}

proc testRevComp() {
  writeln("testRevComp");
  var A:[0..<20] uint(8);
  A[0..<10] = bytesToArray("ACNTTAGGTA");
  reverseComplement(A, 0..<10, A, 10..<20);
  var Expect:[0..<20] uint(8);
  Expect[0..<10]  = bytesToArray("ACNTTAGGTA");
  Expect[10..<20] = bytesToArray("TACCTAANGT");
  assert(A.equals(Expect));
  // check reverseComplement(reverseComplement(input)) == input
  reverseComplement(A, 10..<20, A, 0..<10);
  assert(A.equals(Expect));
}

proc testFastaFiles() throws {
  writeln("testFastaFiles");
  var fileContents = "> test \t seq\nA\n\rC\tG  TTA\nGGT\n\n\nA\n> seq 2\nCCG";
  var expect = ">ACGTTAGGTA>CCG";
  if Utility.INCLUDE_REVERSE_COMPLEMENT {
    expect +=  ">CGG>TACCTAACGT";
  }
  var n = expect.size;
  var filename = "tmp-testFastaFiles-test.fna";
  {
    var w = IO.openWriter(filename);
    w.write(fileContents);
  }
  {
    assert(computeFastaFileSize(filename) == n);
    assert(computeFileSize(filename) == n);
    var A: [0..n+1] uint(8);
    readFastaFileSequence(filename, A, 1..n);
    assert(A[0] == 0);
    assert(A[n+1] == 0);
    var str = arrToString(A[1..n]);
    assert(str == expect);

    A = 0;
    readFileData(filename, A, 1..n);
    assert(A[0] == 0);
    assert(A[n+1] == 0);
    var str2 = arrToString(A[1..n]);
    assert(str2 == expect);
  }

  FileSystem.remove(filename);
}

config const n = 100_000;
proc testAtomicMinMax() {
  writeln("testAtomicMinMax");
  var amin: atomic int = max(int);
  var amax: atomic int = min(int);

  forall i in 1..n {
    atomicStoreMinRelaxed(amin, i);
    atomicStoreMaxRelaxed(amax, i);
  }

  writeln("amin ", amin.read(), " amax ", amax.read());
  assert(amin.read() == 1);
  assert(amax.read() == n);
}

proc testReplicate() {
  writeln("testReplicate");
  const v = "hello";
  const rep = replicate(v, Locales);
  coforall loc in Locales {
    on loc {
      const ref locv = getLocalReplicand(v, rep);
      assert(locv.locale == here);
      assert("hello" == locv);
    }
  }
}

proc testDivideIntoTasks() {
  writeln("testDivideIntoTasks");
  const Dom = BlockDist.blockDist.createDomain(0..<n);
  const nLocales = Dom.targetLocales().size;
  const nTasksPerLocale = computeNumTasks();
  var A:[Dom] int = -1; // store task IDs
  forall (taskId, chunk) in divideIntoTasks(Dom, nTasksPerLocale) {
    for i in chunk {
      assert(A[i] == -1); // should not have any overlap
      A[i] = taskId;
    }
  }
  // check that it works the same even if some tasks are running
  coforall i in 1..10 {
    var B:[Dom] int = -1;
    forall (taskId, chunk) in divideIntoTasks(Dom, nTasksPerLocale) {
      for i in chunk {
        assert(B[i] == -1); // should not have any overlap
        B[i] = taskId;
      }
    }
    assert(B.equals(A));
  }

  // count the number per task. It should be within 1% of the min/max.
  var countPerTask:[0..<nLocales*nTasksPerLocale] int;
  for x in A {
    countPerTask[x] += 1;
  }
  const minCount = min reduce countPerTask;
  const maxCount = max reduce countPerTask;
  writeln("minCount = ", minCount, " maxCount = ", maxCount);
  assert(minCount <= maxCount &&
         maxCount <= minCount + 1 + 0.01*minCount);
}

proc main() throws {
  testTriangles();
  testBsearch();
  testRevComp();
  testFastaFiles();
  serial {
    testAtomicMinMax();
  }
  testAtomicMinMax();

  testReplicate();
  testDivideIntoTasks();
}


}
