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
import Random;
import Math;

// problem size for various tests
config const nAtomicTest = 100_000;
config const n = 1_000;
config const nBuckets = 8*numLocales*computeNumTasks(ignoreRunning=true);

proc testIsDistributed() {
  writeln("testIsDistributed");

  const BlockDomain = BlockDist.blockDist.createDomain(0..100);
  const DefaultDomain = {0..100};

  assert(isDistributedDomain(BlockDomain));
  assert(!isDistributedDomain(DefaultDomain));
}

proc testBulkCopy() {
  writeln("testBulkCopy");

  const Dom = BlockDist.blockDist.createDomain(0..<n);
  const A:[Dom] int = Dom;
  var LocA: [0..n+1] int = 0..n+1;

  // test local dst block src
  for size in [1, 10, 100, n] {
    writeln("testing GETs with max size ", size);
    for i in 0..<n {
      LocA = -1;
      // copy 'size' bytes starting from 'i'
      var sz = size;
      if i+sz >= n {
        sz = n - i;
      }
      assert(A.domain.contains(i..#sz));
      assert(LocA.domain.contains(1..#sz));
      const srcRegion = i..#sz;
      if srcRegion.size > 0 {
        const dstRegion = 1..#srcRegion.size;
        bulkCopy(LocA, dstRegion, A, srcRegion);
        assert(LocA[0] == -1);
        for j in 0..<srcRegion.size {
          assert(LocA[1+j] == A[i+j]);
        }
        assert(LocA[dstRegion.high+1] == -1);
      }
    }
  }

  // test block dst local src
  var B = BlockDist.blockDist.createArray(0..n, int);
  const LocB:[0..n+1] int = 0..n+1;
  for size in [1, 10, 100, n] {
    writeln("testing PUTs with max size ", size);
    for i in 1..<n {
      B = -1;
      // copy 'size' bytes starting from 'i'
      var sz = size;
      if i+sz >= n {
        sz = n - i;
      }
      assert(B.domain.contains(1..#sz));
      assert(LocB.domain.contains(i..#sz));
      const dstRegion = i..#sz;
      if dstRegion.size > 0 {
        const srcRegion = 1..#dstRegion.size;
        bulkCopy(B, dstRegion, LocB, srcRegion);
        assert(B[0] == -1);
        for j in 0..<dstRegion.size {
          assert(B[i+j] == LocB[1+j]);
        }
        assert(B[dstRegion.high+1] == -1);
      }
    }
  }

  // test block dst block src
  {
    var Dst = BlockDist.blockDist.createArray(0..n+1, int);
    var Src = BlockDist.blockDist.createArray(0..n+1, int);
    Src = 0..n+1;
    on Locales[numLocales - 1] {
      for size in [1, 10, 100, n] {
        writeln("testing GET-PUTs with max size ", size);
        for i in 0..<n {
          Dst = -1;
          // copy 'size' bytes starting from 'i'
          var sz = size;
          if i+sz >= n {
            sz = n - i;
          }
          assert(Src.domain.contains(i..#sz));
          assert(Dst.domain.contains(1..#sz));
          const srcRegion = i..#sz;
          if srcRegion.size > 0 {
            const dstRegion = 1..#srcRegion.size;
            bulkCopy(Dst, dstRegion, Src, srcRegion);
            assert(Dst[0] == -1);
            for j in 0..<srcRegion.size {
              assert(Dst[1+j] == Src[i+j]);
            }
            assert(Dst[dstRegion.high+1] == -1);
          }
        }
      }
    }
  }

  // test dst remote src local
  {
    var Dst:[0..n+1] int;
    on Locales[numLocales - 1] {
      var Src:[0..n+1] int;
      Src = 0..n+1;
      for size in [1, 10, 100, n] {
        writeln("testing non-block PUTs with max size ", size);
        for i in 0..<n {
          Dst = -1;
          // copy 'size' bytes starting from 'i'
          var sz = size;
          if i+sz >= n {
            sz = n - i;
          }
          assert(Src.domain.contains(i..#sz));
          assert(Dst.domain.contains(1..#sz));
          const srcRegion = i..#sz;
          if srcRegion.size > 0 {
            const dstRegion = 1..#srcRegion.size;
            bulkCopy(Dst, dstRegion, Src, srcRegion);
            assert(Dst[0] == -1);
            for j in 0..<srcRegion.size {
              assert(Dst[1+j] == Src[i+j]);
            }
            assert(Dst[dstRegion.high+1] == -1);
          }
        }
      }
    }
  }

  // test dst local src remote
  {
    const Src:[0..n+1] int = 0..n+1;
    on Locales[numLocales - 1] {
      var Dst:[0..n+1] int;
      for size in [1, 10, 100, n] {
        writeln("testing non-block GETs with max size ", size);
        for i in 0..<n {
          Dst = -1;
          // copy 'size' bytes starting from 'i'
          var sz = size;
          if i+sz >= n {
            sz = n - i;
          }
          assert(Src.domain.contains(i..#sz));
          assert(Dst.domain.contains(1..#sz));
          const srcRegion = i..#sz;
          if srcRegion.size > 0 {
            const dstRegion = 1..#srcRegion.size;
            bulkCopy(Dst, dstRegion, Src, srcRegion);
            assert(Dst[0] == -1);
            for j in 0..<srcRegion.size {
              assert(Dst[1+j] == Src[i+j]);
            }
            assert(Dst[dstRegion.high+1] == -1);
          }
        }
      }
    }
  }
}

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

proc testBits(type t) {
  writeln("testBits(", t:string, ")");

  var A: [0..<n] int;
  Random.fillRandom(A, min=0, max=1, seed=1);

  const nWords = Math.divCeil(n, numBits(t));
  var bits: [0..<nWords] t;

  for i in 0..<n {
    if A[i] != 0 {
      setBit(bits, i);
    }
  }

  for i in 0..<n {
    const expectBit = A[i] != 0;
    const gotBit = getBit(bits, i);
    assert(gotBit == expectBit);
  }
}

proc testBits() {
  testBits(uint(8));
  testBits(uint(16));
  testBits(uint(32));
  testBits(uint);
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

proc testFastaFile(contents:string, seq:string, revcomp:string) throws {
  var expect = seq;
  if Utility.INCLUDE_REVERSE_COMPLEMENT {
    expect += revcomp;
  }
  var n = expect.size;
  var filename = "tmp-testFastaFiles-test.fna";
  {
    var w = IO.openWriter(filename);
    w.write(contents);
  }
  {
    assert(computeFastaFileSize(filename) == n);
    assert(computeFileSize(filename) == n);
    var A: [0..n+1] uint(8);
    readFastaFileSequence(filename, A, 1..n);
    assert(A[0] == 0);
    assert(A[n+1] == 0);
    var str = arrToString(A[1..n]);
    writeln("Got ", str);
    writeln("Exp ", expect);
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

proc testFastaFiles() throws {
  writeln("testFastaFiles()");

  testFastaFile("> test \t seq\nA\n\rC\tG  TTA\nGGT\n\n\nA\n> seq 2\nCCG",
                ">ACGTTAGGTA>CCG",
                ">CGG>TACCTAACGT");
  testFastaFile(">\n>\n>\nACAT\n>\n>\n", ">>>ACAT>>", ">>>ATGT>>");
  testFastaFile(">\nAAAA>\nTTT>\nCC>\nG", ">AAAA>TTT>CC>G", ">C>GG>AAA>TTTT");
}

proc testAtomicMinMax() {
  writeln("testAtomicMinMax");
  var amin: atomic int = max(int);
  var amax: atomic int = min(int);

  forall i in 1..nAtomicTest {
    atomicStoreMinRelaxed(amin, i);
    atomicStoreMaxRelaxed(amax, i);
  }

  writeln("amin ", amin.read(), " amax ", amax.read());
  assert(amin.read() == 1);
  assert(amax.read() == nAtomicTest);
}

proc testReplicate() {
  writeln("testReplicate");

  // do a check that we can replicate to all locales
  {
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

  // try re-replicate with a subset of locales
  if numLocales >= 3 {
    const v = "goodbye";
    var rep: [BlockDist.blockDist.createDomain(0..<numLocales)]
             owned ReplicatedWrapper(string)?;
    const activeLocales = [Locales[1], Locales[2]];
    reReplicate(v, rep, activeLocales);
    assert(rep[Locales[0].id] == nil); // didn't set Locale 0
    assert(rep[Locales[1].id] != nil); // did set Locale 1
    assert(rep[Locales[2].id] != nil); // did set Locale 2
    if numLocales >= 4 {
      assert(rep[Locales[3].id] == nil); // didn't set Locale 3
    }
    coforall loc in activeLocales {
      on loc {
        const ref locv = getLocalReplicand(v, rep);
        assert(locv.locale == here);
        assert("goodbye" == locv);
      }
    }
  }
}

proc testActiveLocales() {
  writeln("testActiveLocales");

  const Dom = BlockDist.blockDist.createDomain(0..<n);
  const nLocales = Dom.targetLocales().size;
  const nTasksPerLocale = computeNumTasks();
  const EmptyLocales:[1..0] locale;

  assert(computeActiveLocales(Dom, 0..<n).equals(Locales));
  assert(computeActiveLocales(Dom, 1..0).size == 0);

  for region in [0..0, 0..1, 1..10, 0..n/4, n/4..<n/2, 0..<n] {
    var expectActiveElts: [0..<n] int = 0;
    var expectActiveLocs:[0..<numLocales] int = 0;
    var activeElts: [0..<n] int = 0;
    var activeLocs:[0..<numLocales] int = 0;

    // compute 'expect' by iterating over a slice of the domain
    forall i in Dom with (+ reduce expectActiveLocs) {
      if region.contains(i) {
        expectActiveElts[i] = 1;
        expectActiveLocs[here.id] = 1;
      }
    }

    // compute 'got' by computing the active locales
    var mylocs = computeActiveLocales(Dom, region);

    // also check that computing active locales is a local task
    local {
      computeActiveLocales(Dom, region);
    }

    coforall loc in mylocs with (+ reduce activeLocs) {
      on loc {
        //writeln("running on loc ", here);
        var intersect = Dom.localSubdomain()[region];
        assert(intersect.size > 0);
        for i in intersect {
          activeElts[i] = 1;
        }
        activeLocs[here.id] = 1;
      }
    }

    // we don't care about the counts for expectActiveLocs / activeLocs
    forall (a, b) in zip(expectActiveLocs, activeLocs) {
      if a > 1 then a = 1;
      if b > 1 then b = 1;
    }

    /*writeln("expectActiveElts ", expectActiveElts);
    writeln("activeElts       ", activeElts);
    writeln("expectActiveLocs ", expectActiveLocs);
    writeln("activeLocs       ", activeLocs);*/

    assert(expectActiveElts.equals(activeElts));
    assert(expectActiveLocs.equals(activeLocs));
  }
}

proc testDivideIntoTasks() {
  writeln("testDivideIntoTasks");
  const Dom = BlockDist.blockDist.createDomain(0..<n);
  const nLocales = Dom.targetLocales().size;
  const nTasksPerLocale = computeNumTasks();
  var A:[Dom] int = -1; // store task IDs
  forall (activeLocIdx, taskIdInLoc, chunk)
  in divideIntoTasks(Dom, 0..<n, nTasksPerLocale) {
    for i in chunk {
      assert(A[i] == -1); // should not have any overlap
      A[i] = activeLocIdx*nTasksPerLocale + taskIdInLoc;
    }
  }
  // check that it works the same even if some tasks are running
  coforall i in 1..10 {
    var B:[Dom] int = -1;
    forall (activeLocIdx, taskIdInLoc, chunk)
    in divideIntoTasks(Dom, 0..<n, nTasksPerLocale) {
      for i in chunk {
        assert(B[i] == -1); // should not have any overlap
        B[i] = activeLocIdx*nTasksPerLocale + taskIdInLoc;
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

  // check that dividing with region on a single locale
  // only runs on one locale
  coforall loc in Dom.targetLocales() {
    on loc {
      const region:range = Dom.localSubdomain().dim(0);
      local {
        forall (activeLocIdx, taskIdInLoc, chunk)
        in divideIntoTasks(Dom, 0..<n, nTasksPerLocale) {
          // nothing to do here, the point was to check it is local
          var sum = 0;
          for i in chunk {
            sum += A[i]; // accessing to make sure it is local
          }
        }
      }
    }
  }
}

proc testDivideIntoPages() {
  writeln("testDivideIntoPages");

  for lower in [0, 100, 1000, 1024, 4096] {
    for size in [0, 9, 21, 100, 543, 1024*1024] {
      for alignment in [1, 16, 64, 1024] {
        var region = lower..#size;
        var ByTask: [region] atomic int;
        var nUnaligned = 0;

        // check serial
        for pageRange in divideIntoPages(region, alignment) {
          // check alignment
          if pageRange.low % alignment != 0 {
            nUnaligned += 1;
          }
          // count for checking elements are all visited once
          for i in pageRange {
            ByTask[i].add(1);
          }
        }

        assert(nUnaligned <= 1);

        // each position should be visited exactly once
        for elt in ByTask {
          assert(elt.read() == 1);
        }

        // check parallel
        for i in region {
          ByTask[i].write(0);
        }
        nUnaligned = 0;
        forall pageRange in divideIntoPages(region, alignment)
        with (+ reduce nUnaligned) {
          // check alignment
          if pageRange.low % alignment != 0 {
            nUnaligned += 1;
          }
          // count for checking elements are all visited once
          for i in pageRange {
            ByTask[i].add(1);
          }
        }

        assert(nUnaligned <= 1);

        // each position should be visited exactly once
        for elt in ByTask {
          assert(elt.read() == 1);
        }
      }
    }
  }
}

proc testRotateRange() {
  writeln("testRotateRange");

  for lower in [0, 100, 1000, 1024, 4096] {
    for size in [0, 9, 21, 100, 543, 1024*1024] {
      for shift in [0, 1, 13, 16, 64, 1024] {
        var region = lower..#size;
        var ByTask: [region] atomic int;
        var first = false;

        // check serial
        for i in rotateRange(region, shift) {
          if first {
            assert(i == region.low + (shift%size));
          }
          ByTask[i].add(1);
        }
        // each position should be visited exactly once
        for elt in ByTask {
          assert(elt.read() == 1);
        }

        // check parallel
        for elt in ByTask {
          elt.write(0);
        }

        forall i in rotateRange(region, shift) {
          // count for checking elements are all visited once
          ByTask[i].add(1);
        }

        // each position should be visited exactly once
        for elt in ByTask {
          assert(elt.read() == 1);
        }
      }
    }
  }
}

proc testPackInput() {
  writeln("testPackInput");

  var InputElts = [0b111, 0b101, 0b011, 0b101, 0b000, 0b100, 0b100, 0b111,
                   0b001, 0b000, 0b010, 0b100, 0b000, 0b001, 0b110, 0b101,
                   0b101, 0b010, 0b011, 0b110, 0b111, 0b011, 0b010, 0b001,

                   0b100, 0b000, 0b010, 0b100, 0b101, 0b010, 0b011, 0b011,
                   0b000, 0b001, 0b010, 0b011, 0b100, 0b101, 0b110, 0b111,
                   0b111, 0b110, 0b101, 0b100, 0b011, 0b010, 0b001, 0b000,

                   0b110, 0b111, 0, 0, 0, 0, 0, 0, 0, 0];
  const InputUint64 = InputElts : uint(64);
  const InputUint8  = InputElts : uint(8);
  const n = 50;
  var bitsPerChar: int = computeBitsPerChar(InputUint8, n);
  assert(bitsPerChar == 3);
  var PackedByte = packInput(uint(8), InputUint8, n, bitsPerChar);
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
    assert(InputUint8[i] == loadWord(PackedByte, i*bitsPerChar) >> (8-3));
  }

  bitsPerChar = computeBitsPerChar(InputUint64, n);
  assert(bitsPerChar == 3);
  var PackedUint = packInput(uint, InputUint64, n, bitsPerChar);
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
    assert(InputUint64[i] == loadWord(PackedUint, i*bitsPerChar) >> (64-3));
  }
}

proc main() throws {
  testIsDistributed();

  serial {
    testActiveLocales();
  }
  testActiveLocales();

  serial {
    testDivideIntoTasks();
  }
  testDivideIntoTasks();

  serial {
    testDivideIntoPages();
    testRotateRange();
  }
  testDivideIntoPages();
  testRotateRange();

  testBulkCopy();
  testTriangles();
  testBits();
  testBsearch();
  testRevComp();
  testFastaFiles();
  serial {
    testAtomicMinMax();
  }
  testAtomicMinMax();

  testReplicate();

  serial {
    testPackInput();
  }
  testPackInput();

  writeln("OK");
}


}
