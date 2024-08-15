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

proc testFastaFiles() throws {
  writeln("testFastaFiles");
  var fileContents = "> test \t seq\nA\n\rC\tG  TTA\nGGT\n\n\nA\n> seq 2\nCCG";
  var expect = ">ACGTTAGGTA>CCG";
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

proc main() throws {
  testTriangles();
  testBsearch();
  testFastaFiles();
  serial {
    testAtomicMinMax();
  }
  testAtomicMinMax();
}


}
