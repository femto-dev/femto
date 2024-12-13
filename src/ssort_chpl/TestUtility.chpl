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

  // check that the tasks divide the work in an increasing order,
  // that is, the task assignment in A is only increasing.
  // this is important for making the partition stable.
  for i in Dom {
    if i > 0 {
      assert(A[i-1] <= A[i]);
    }
  }
}

proc testPackInput() {
  writeln("testPackInput");

  var Input = [0b111, 0b101, 0b011, 0b101, 0b000, 0b100, 0b100, 0b111,
               0b001, 0b000, 0b010, 0b100, 0b000, 0b001, 0b110, 0b101,
               0b101, 0b010, 0b011, 0b110, 0b111, 0b011, 0b010, 0b001,

               0b100, 0b000, 0b010, 0b100, 0b101, 0b010, 0b011, 0b011,
               0b000, 0b001, 0b010, 0b011, 0b100, 0b101, 0b110, 0b111,
               0b111, 0b110, 0b101, 0b100, 0b011, 0b010, 0b001, 0b000,

               0b110, 0b111, 0, 0, 0, 0, 0, 0, 0, 0];
  const n = 50;
  var bitsPerChar: int;
  var PackedByte = try! packInput(uint(8), Input, n, bitsPerChar);
  assert(bitsPerChar == 3);
  // each line corresponds to a 24-bit row above
  var ba = 0b11110101, bb = 0b11010001, bc = 0b00100111,
      bd = 0b00100001, be = 0b01000000, bf = 0b01110101,
      bg = 0b10101001, bh = 0b11101110, bi = 0b11010001,

      bj = 0b10000001, bk = 0b01001010, bl = 0b10011011,
      bm = 0b00000101, bn = 0b00111001, bo = 0b01110111,
      bp = 0b11111010, bq = 0b11000110, br = 0b10001000,

      bs = 0b11011100;

  assert(PackedByte[0] == ba && PackedByte[1] == bb && PackedByte[2] == bc);
  assert(PackedByte[3] == bd && PackedByte[4] == be && PackedByte[5] == bf);
  assert(PackedByte[6] == bg && PackedByte[7] == bh && PackedByte[8] == bi);
  assert(PackedByte[9] == bj && PackedByte[10] == bk && PackedByte[11] == bl);
  assert(PackedByte[12] == bm && PackedByte[13] == bn && PackedByte[14] == bo);
  assert(PackedByte[15] == bp && PackedByte[16] == bq && PackedByte[17] == br);
  assert(PackedByte[18] == bs);
  assert(PackedByte.size >= 18+8); // should have a words worth of padding
  for x in PackedByte[19..] {
    assert(x == 0);
  }

  // test loading words
  for i in 0..<n {
    assert(Input[i] == loadWord(PackedByte, i, bitsPerChar) >> (8-3));
  }

  var PackedUint = try! packInput(uint, Input, n, bitsPerChar);
  assert(bitsPerChar == 3);
  // compute the words based on the above bytes
  var word0:uint;
  var word1:uint;
  var word2:uint;

  // the first 8 bytes go into word0
  word0 <<= 8; word0 |= ba;
  word0 <<= 8; word0 |= bb;
  word0 <<= 8; word0 |= bc;
  word0 <<= 8; word0 |= bd;
  word0 <<= 8; word0 |= be;
  word0 <<= 8; word0 |= bf;
  word0 <<= 8; word0 |= bg;
  word0 <<= 8; word0 |= bh;

  // the next 8 bytes go into word1
  word1 <<= 8; word1 |= bi;
  word1 <<= 8; word1 |= bj;
  word1 <<= 8; word1 |= bk;
  word1 <<= 8; word1 |= bl;
  word1 <<= 8; word1 |= bm;
  word1 <<= 8; word1 |= bn;
  word1 <<= 8; word1 |= bo;
  word1 <<= 8; word1 |= bp;

  // the last bytes go into word2
  word2 <<= 8; word2 |= bq;
  word2 <<= 8; word2 |= br;
  word2 <<= 8; word2 |= bs;
  word2 <<= 8; // rest are zeros
  word2 <<= 8;
  word2 <<= 8;
  word2 <<= 8;
  word2 <<= 8;

  assert(PackedUint[0] == word0);
  assert(PackedUint[1] == word1);
  assert(PackedUint[2] == word2);
  assert(PackedUint.size >= 3+8); // should have padding
  for x in PackedUint[3..] {
    assert(x == 0);
  }

  // test loading words
  for i in 0..<n {
    assert(Input[i] == loadWord(PackedUint, i, bitsPerChar) >> (64-3));
  }

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
  serial {
    testPackInput();
  }
  testPackInput();
}


}
