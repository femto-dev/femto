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

  femto/src/ssort_chpl/TestSuffixSort.chpl
*/

module TestSuffixSort {


use SuffixSortImpl;
use DifferenceCovers;
use Utility;
use Partitioning;

use Math;
use IO;
use Sort;
use CopyAggregation;

import SuffixSort.{computeSparsePLCP,lookupLCP};
import SuffixSort.TRACE;
import SuffixSort.INPUT_PADDING;
import SuffixSort.DEFAULT_PERIOD;

private proc bytesToArray(s: bytes) {
  const nWithPadding = s.size+INPUT_PADDING;
  const A:[0..<nWithPadding] uint(8)=
    for i in 0..<nWithPadding do
      if i < s.size then s[i] else 0;

  return A;
}
private proc bytesToArray(s: string) {
  return bytesToArray(s:bytes);
}

private proc bytesToUint(s: bytes) {
  var ret: uint = 0;
  for i in 0..<s.size {
    ret <<= 8;
    ret |= s[i];
  }
  return ret;
}
private proc bytesToUint(s: string) {
  return bytesToUint(s:bytes);
}

private proc checkOffsets(got: [], expect: [] int) {
  if got.size != expect.size {
    halt("got size ", got.size, " but expected size ", expect.size);
  }

  for (g, e, i) in zip(got, expect, got.domain) {
    var gotOffset = -1;
    if isSubtype(g.type, offsetAndCached) {
      gotOffset = g.offset: int;
    } else {
      gotOffset = g: int;
    }
    if gotOffset != e {
      writeln("FAILURE, suffix array mismatch");
      writeln("Expect SA ", expect);
      writeln("Got SA    ", got);

      halt("at i=", i, " got offset ", gotOffset, " but expected ", e);
    }
  }
}

private proc checkLCP(got: [], expect: [] int) {
  if got.size != expect.size {
    halt("got size ", got.size, " but expected size ", expect.size);
  }

  for (g, e, i) in zip(got, expect, got.domain) {
    if g != e {
      writeln("FAILURE, LCP array mismatch");
      writeln("Expect LCP ", expect);
      writeln("Got LCP    ", got);

      halt("at i=", i, " got ", g, " but expected ", e);
    }
  }
}

private proc checkCached(got: [] offsetAndCached, expect: []) {
  if got.size != expect.size {
    halt("got size ", got.size, " but expected size ", expect.size);
  }

  for g in got {
    assert(0 <= g.offset && g.offset <= expect.size);
    var e = expect[g.offset];
    if g.cached != e {
      try! halt("at offset=", g.offset, " got cached 0x",
                "%xu".format(g.cached),
                " but expected 0x",
                "%xu".format(e));
    }
  }
}

private proc checkSeeressesCase(inputArr, n:int,
                                expectOffsets,
                                param period=3,
                                type wordType=uint,
                                param bitsPerChar=4,
                                simulateBig=false) {
  if TRACE {
    writeln("checkSeeressesCase period=", period,
            " wordType=", wordType:string,
            " bitsPerChar=", bitsPerChar,
            " simulateBig=", simulateBig);
  }

  const nTasksPerLocale = computeNumTasks(ignoreRunning=true);
  var finalSortNumPasses: int = FINAL_SORT_NUM_PASSES;
  var finalSortSimpleSortLimit: int = SIMPLE_SORT_LIMIT;
  var minBucketsPerTask: int = MIN_BUCKETS_PER_TASK;
  var minBucketsSpace: int = MIN_BUCKETS_SPACE;
  var assumeNonLocal: bool = false;

  if simulateBig {
    finalSortNumPasses = 2;
    finalSortSimpleSortLimit = 2;
    minBucketsPerTask = 8;
    minBucketsSpace = 1000;
    assumeNonLocal = true;
  } else {
    finalSortNumPasses = 1;
    finalSortSimpleSortLimit = 10000;
    minBucketsPerTask = 2;
    minBucketsSpace = 10;
  }

  type offsetType = int(numBits(wordType));
  type unsignedOffsetType = uint(numBits(wordType));
  const nOffset = n:offsetType;
  const cfg = new ssortConfig(idxType=int,
                              offsetType=offsetType,
                              unsignedOffsetType=unsignedOffsetType,
                              loadWordType=unsignedOffsetType,
                              bitsPerChar=bitsPerChar,
                              n=nOffset,
                              cover=new differenceCover(period),
                              locales=Locales,
                              nTasksPerLocale=nTasksPerLocale,
                              finalSortNumPasses=finalSortNumPasses,
                              finalSortSimpleSortLimit=finalSortSimpleSortLimit,
                              minBucketsPerTask=minBucketsPerTask,
                              minBucketsSpace=minBucketsSpace,
                              assumeNonLocal=assumeNonLocal);

  const packed = packInput(cfg.loadWordType,
                           inputArr, n:cfg.offsetType, cfg.bitsPerChar);

  const SA = computeSuffixArrayDirectly(cfg, packed, {0..<n});
  checkOffsets(SA, expectOffsets);

  // try ssortDcx
  const SA2 = ssortDcx(cfg, packed);
  checkOffsets(SA2, expectOffsets);
}

private proc testHelpers() {
  assert(myDivCeil(0,2) == 0);
  assert(myDivCeil(1,2) == 1);
  assert(myDivCeil(2,2) == 1);
  assert(myDivCeil(3,2) == 2);
  assert(myDivCeil(4,2) == 2);

  assert(myDivCeil(0,3) == 0);
  assert(myDivCeil(1,3) == 1);
  assert(myDivCeil(2,3) == 1);
  assert(myDivCeil(3,3) == 1);
  assert(myDivCeil(4,3) == 2);
  assert(myDivCeil(5,3) == 2);
  assert(myDivCeil(6,3) == 2);
  assert(myDivCeil(7,3) == 3);

  proc makeCfg(type wordType, param bitsPerChar) {
    return new ssortConfig(idxType=int,
                           offsetType=int,
                           unsignedOffsetType=wordType,
                           loadWordType=wordType,
                           bitsPerChar=bitsPerChar,
                           n=100,
                           cover=new differenceCover(3),
                           locales=Locales,
                           nTasksPerLocale=1);

  }

  {
    const cfg = makeCfg(uint(8), 8);

    assert(cfg.getPrefixWords(3) == 3);
    assert(cfg.getPrefixWords(7) == 7);
    assert(cfg.getPrefixWords(21) == 21);
  }

  {
    const cfg = makeCfg(uint(16), 8);

    assert(cfg.getPrefixWords(3) == 2);
    assert(cfg.getPrefixWords(7) == 4);
    assert(cfg.getPrefixWords(21) == 11);
  }

  {
    const cfg = makeCfg(uint(32), 8);

    assert(cfg.getPrefixWords(3) == 1);
    assert(cfg.getPrefixWords(7) == 2);
    assert(cfg.getPrefixWords(21) == 6);
  }

  {
    const cfg = makeCfg(uint(64), 8);

    assert(cfg.getPrefixWords(3) == 1);
    assert(cfg.getPrefixWords(7) == 1);
    assert(cfg.getPrefixWords(21) == 3);
  }

  {
    const cfg = makeCfg(uint(64), 64);

    assert(cfg.getPrefixWords(3) == 3);
    assert(cfg.getPrefixWords(7) == 7);
    assert(cfg.getPrefixWords(21) == 21);
  }
}

private proc testPrefixComparisons(type loadWordType) {
  param bitsPerChar=8;
  const cover = new differenceCover(3);
  const inputStr = "aabbccaaddffffffffaabbccaaddff";
                 //           11111111112222222222
                 // 012345678901234567890123456789
                 // * |   *           *
                 // AA|   AA2         AA3
                 //   BB
  const text = bytesToArray(inputStr);
  const n = inputStr.size;

  const cfg = new ssortConfig(idxType=int,
                              offsetType=int(16),
                              bitsPerChar=bitsPerChar,
                              n=n,
                              cover=cover,
                              locales=Locales,
                              nTasksPerLocale=1);
  const nBits = cfg.nBits;

  const packed = packInput(cfg.loadWordType, text, n, cfg.bitsPerChar);

  // these are irrelevant here
  const charsPerMod = 2;
  const ranks:[0..n+INPUT_PADDING+cover.period] cfg.unsignedOffsetType;
  var ranksN = n;

  const prefixAA =  makeOffsetAndCached(cfg, 0, packed, n, nBits);
  const prefixAA2 = makeOffsetAndCached(cfg, 6, packed, n, nBits);
  const prefixAA3 = makeOffsetAndCached(cfg, 18, packed, n, nBits);
  const prefixBB =  makeOffsetAndCached(cfg, 2, packed, n, nBits);

  const prefixAAp = makePrefix(cfg, 0, packed, n, nBits);
  const prefixAA2p = makePrefix(cfg, 6, packed, n, nBits);
  const prefixAA3p = makePrefix(cfg, 18, packed, n, nBits);
  const prefixBBp = makePrefix(cfg, 2, packed, n, nBits);

  const prefixAAs = makePrefixAndSampleRanks(cfg, 0,
                                             packed, ranks, n, nBits);
  const prefixAA2s = makePrefixAndSampleRanks(cfg, 6,
                                              packed, ranks, n, nBits);
   const prefixAA3s = makePrefixAndSampleRanks(cfg, 18,
                                               packed, ranks, n, nBits);

  const prefixBBs = makePrefixAndSampleRanks(cfg, 2,
                                             packed, ranks, n, nBits);

  proc helpCompare(a, b) {
    return comparePrefixes(cfg, a, b, packed, maxPrefixWords=2);
  }

  assert(helpCompare(0, 0)==0);
  assert(helpCompare(0, 2)<0);

  assert(helpCompare(prefixAA, prefixAA)==0);
  assert(helpCompare(prefixAA, prefixAA3)==0);
  assert(helpCompare(prefixAA, prefixAA2)<=0);
  assert(helpCompare(prefixAA, prefixBB)<0);
  assert(helpCompare(prefixBB, prefixAA)>0);

  assert(helpCompare(prefixAAp, prefixAAp)==0);
  assert(helpCompare(prefixAAp, prefixBBp)<0);
  assert(helpCompare(prefixBBp, prefixAAp)>0);

  assert(helpCompare(prefixAA, prefixAAp)==0);
  assert(helpCompare(prefixAA, prefixBBp)<0);
  assert(helpCompare(prefixAAp, prefixBB)<0);
  assert(helpCompare(prefixBBp, prefixAA)>0);
  assert(helpCompare(prefixBB, prefixAAp)>0);

  assert(helpCompare(prefixAAp, prefixAAs)==0);
  assert(helpCompare(prefixAAs, prefixAAp)==0);
  assert(helpCompare(prefixAAs, prefixBBs)<0);
  assert(helpCompare(prefixAAs, prefixBBp)<0);
  assert(helpCompare(prefixAAp, prefixBBs)<0);

  assert(helpCompare(prefixBBs, prefixAAs)>0);
  assert(helpCompare(prefixBBs, prefixAAp)>0);
  assert(helpCompare(prefixBBp, prefixAAs)>0);

  /*
  assert(charactersInCommon(cfg, prefixAAp, prefixAAp) >= cover.period);
  assert(charactersInCommon(cfg, prefixAAs, prefixAAs) >= cover.period);
  assert(charactersInCommon(cfg, prefixAAp, prefixAA2p) == 2);
  assert(charactersInCommon(cfg, prefixAAs, prefixAA2s) == 2);
  assert(charactersInCommon(cfg, prefixAA2p, prefixAA2p) >= cover.period);
  assert(charactersInCommon(cfg, prefixAA2s, prefixAA2s) >= cover.period);
  assert(charactersInCommon(cfg, prefixAAp, prefixBBp) == 0);
  assert(charactersInCommon(cfg, prefixAA3p, prefixAA3p) >= cover.period);
  assert(charactersInCommon(cfg, prefixAA3s, prefixAA3s) >= cover.period);
  assert(charactersInCommon(cfg, prefixAAp, prefixAA3p) >= cover.period);*/
}

proc testRankComparisons3() {
  const cover = new differenceCover(3);
  const n = 16;
  const cfg = new ssortConfig(idxType=int,
                              offsetType=int,
                              bitsPerChar=8,
                              n=n,
                              cover=cover,
                              locales=Locales,
                              nTasksPerLocale=1);
  const nBits = cfg.nBits;

  // create the mapping to the recursive problem
  const charsPerMod = 7;
  const nSample = charsPerMod*cover.sampleSize;
  var Text:[0..<n+INPUT_PADDING] uint(8);
  const Packed = packInput(uint, Text, n, cfg.bitsPerChar);

  var Ranks1:[0..<nSample] uint; // this is sample offset to rank
  var Offsets:[0..<nSample] int; // sample offset to regular offset

  Ranks1   = [14, 13, 10,  9,  6,  5, 12, 11,  8, 12,  4,  3,  2,  1];
  Offsets =  [ 0,  1,  3,  4,  6,  7,  9, 10, 12, 13, 15, 16, 18, 19];
  // sample    0   1   2   3   4   5   6   7   8   9  10  11  12  13
  //  offsets

  var Ranks:[0..<nSample+INPUT_PADDING+cover.period] uint;
  Ranks[0..<nSample] = Ranks1;

  // check offsetToSubproblemOffset and subproblemOffsetToOffset
  for i in 0..<nSample {
    assert(Offsets[i] == sampleRankIndexToOffset(i, cover));
    assert(i == offsetToSampleRanksOffset(Offsets[i], cover));
  }
  // check some other offsets for offsetToSampleRanksOffset
  assert(offsetToSampleRanksOffset(2, cover) == 2); // 2 -> 3 at sample pos 2
  assert(offsetToSampleRanksOffset(5, cover) == 4); // 5 -> 6 at sample pos 4
  assert(offsetToSampleRanksOffset(8, cover) == 6); // 8 -> 9 at sample pos 6

  // check makePrefixAndSampleRanks

  // check a few cases we can see above
  const p1 = makePrefixAndSampleRanks(cfg, offset=1,
                                      Packed, Ranks, n, nBits);
  const p3 = makePrefixAndSampleRanks(cfg, offset=3,
                                      Packed, Ranks, n, nBits);
  const p19 = makePrefixAndSampleRanks(cfg, offset=19,
                                       Packed, Ranks, n, nBits);
  const p2 = makePrefixAndSampleRanks(cfg, offset=2,
                                      Packed, Ranks, n, nBits);
  const p5 = makePrefixAndSampleRanks(cfg, offset=5,
                                      Packed, Ranks, n, nBits);

  assert(p1.r.ranks[0] == 13); // offset 1 -> sample offset 7 -> rank 13
  assert(p1.r.ranks[1] == 10); // offset 3 -> sample offset 1 -> rank 10

  assert(p3.r.ranks[0] == 10); // offset 3 -> sample offset 1 -> rank 10
  assert(p3.r.ranks[1] == 9);  // offset 4 -> sample offset 8 -> rank 9

  assert(p19.r.ranks[0] == 1); // offset 19 -> sample offset 13 -> rank 1
  assert(p19.r.ranks[1] == 0); // offset 21 -> sample offset -  -> rank 0

  assert(p2.r.ranks[0] == 10); // offset 2 -> next offset sample is 3 -> 10
  assert(p2.r.ranks[1] == 9);  // offset 4 -> sample offset 8 -> rank 9

  assert(p5.r.ranks[0] == 6);  // offset 5 -> next offset sample is 6 ->
                               // sample offset 2 -> rank 6
  assert(p5.r.ranks[1] == 5);  // offset 7 -> sample offset 9 -> rank 5


  // check the rest of the cases
  for sampleOffset in 0..<nSample {
    const offset = sampleRankIndexToOffset(sampleOffset, cover);
    const p = makePrefixAndSampleRanks(cfg, offset=offset,
                                       Packed, Ranks, n, nBits);
    // find the next cover.sampleSize offsets in the cover
    var cur = 0;
    for i in 0..<cover.period {
      if offset+i < n && cover.containedInCover((offset + i) % cover.period) {
        const sampleOffset = offsetToSampleRanksOffset(offset + i, cover);
        assert(p.r.ranks[cur] == Ranks[sampleOffset]);
        cur += 1;
      }
    }
  }

  // try some comparisons
  const o1 = makeOffsetAndCached(cfg, 1, Packed, n, nBits);
  const o3 = makeOffsetAndCached(cfg, 3, Packed, n, nBits);
  const o5 = makeOffsetAndCached(cfg, 5, Packed, n, nBits);
  const o19= makeOffsetAndCached(cfg,19, Packed, n, nBits);

  // test self-compares
  assert(compareSampleRanks(o1, o1, n, Ranks, cover) == 0);
  assert(compareSampleRanks(o3, o3, n, Ranks, cover) == 0);
  assert(compareSampleRanks(o5, o5, n, Ranks, cover) == 0);
  assert(compareSampleRanks(o19, o19, n, Ranks, cover) == 0);

  assert(compareSampleRanks(p1, o1, n, Ranks, cover) == 0);
  assert(compareSampleRanks(p3, o3, n, Ranks, cover) == 0);
  assert(compareSampleRanks(p19, o19, n, Ranks, cover) == 0);

  // test 1 vs 3 : 1 has rank 13 and 3 has rank 10
  assert(compareSampleRanks(o1, o3, n, Ranks, cover) > 0);
  assert(compareSampleRanks(p1, o3, n, Ranks, cover) > 0);

  assert(compareSampleRanks(o3, o1, n, Ranks, cover) < 0);
  assert(compareSampleRanks(p3, o1, n, Ranks, cover) < 0);

  // test 3 vs 5 : use k=1, 3->4 has rank 9 ; 5->6 has rank 6
  assert(compareSampleRanks(o3, o5, n, Ranks, cover) > 0);
  assert(compareSampleRanks(p3, o5, n, Ranks, cover) > 0);

  assert(compareSampleRanks(o5, o3, n, Ranks, cover) < 0);

  // test 5 vs 19 : use k=2, 5->7 has rank 5 ; 19->21 has rank 0
  // BUT 19 is beyond the end of the string, so 5 > 19
  assert(compareSampleRanks(o5, o19, n, Ranks, cover) > 0);

  assert(compareSampleRanks(o19, o5, n, Ranks, cover) < 0);
  assert(compareSampleRanks(p19, o5, n, Ranks, cover) < 0);
}

proc testRankComparisons21() {
  const cover = new differenceCover(21); // 0 1 6 8 18
  const n = 24;
  const cfg = new ssortConfig(idxType=int,
                              offsetType=int,
                              bitsPerChar=8,
                              n=n,
                              cover=cover,
                              locales=Locales,
                              nTasksPerLocale=1);
  const nBits = cfg.nBits;

  type offsetType = cfg.offsetType;

  // create the mapping to the recursive problem
  const charsPerMod = 3;
  const nSample = charsPerMod*cover.sampleSize;
  var Text:[0..<n+INPUT_PADDING] uint(8);
  const Packed = packInput(uint, Text, n, cfg.bitsPerChar);

  var Ranks1:[0..<nSample] uint; // this is sample offset to rank
  var Offsets:[0..<nSample] int; // sample offset to regular offset

  Ranks1   = [15, 14, 13, 11, 12,  9, 10,  7,  8,  6,  5,  4,  2,  3,  1];
  Offsets  = [ 0,  1,  6,  8, 18, 21, 22, 27, 29, 39, 42, 43, 48, 50, 60];
  // sample    0   1   2   3   4   5   6   7   8   9  10  11  12  13  14
  //  offsets

  var Ranks:[0..<nSample+INPUT_PADDING+cover.period] uint;
  Ranks[0..<nSample] = Ranks1;

  // check offsetToSubproblemOffset and subproblemOffsetToOffset
  for i in 0..<nSample {
    assert(Offsets[i] == sampleRankIndexToOffset(i, cover));
    assert(i == offsetToSampleRanksOffset(Offsets[i], cover));
  }

  // check self-compares
  for i in 0..<n {
    const o = makeOffsetAndCached(cfg, i, Packed, n, nBits);
    assert(compareSampleRanks(o, o, n, Ranks, cover) == 0);
    if cover.containedInCover(i % cover.period) {
      const sampleOffset = offsetToSampleRanksOffset(i, cover);
      const p = makePrefixAndSampleRanks(cfg, offset=i,
                                         Packed, Ranks, n, nBits);
      assert(compareSampleRanks(p, o, n, Ranks, cover) == 0);
    }
  }

  const o4  = makeOffsetAndCached(cfg, 4, Packed, n, nBits);
  const o20 = makeOffsetAndCached(cfg, 20, Packed, n, nBits);
  const o21 = makeOffsetAndCached(cfg, 21, Packed, n, nBits);
  const p21 = makePrefixAndSampleRanks(cfg, offset=21,
                                       Packed, Ranks, n, nBits);
  const o22 = makeOffsetAndCached(cfg, 22, Packed, n, nBits);
  const p22 = makePrefixAndSampleRanks(cfg, offset=22,
                                       Packed, Ranks, n, nBits);
  const o23 = makeOffsetAndCached(cfg, 23, Packed, n, nBits);

  const p4 = makePrefixAndSampleRanks(cfg, offset=4,
                                      Packed, Ranks, n, nBits);

  const p7 = makePrefixAndSampleRanks(cfg, offset=7,
                                      Packed, Ranks, n, nBits);

  const p11 = makePrefixAndSampleRanks(cfg, offset=11,
                                       Packed, Ranks, n, nBits);

  const p20 = makePrefixAndSampleRanks(cfg, offset=20,
                                       Packed, Ranks, n, nBits);

  // check p21 and p22 are ok
  assert(p21.r.ranks[0] ==  9); // 21+0  = 21
  assert(p21.r.ranks[1] == 10); // 21+1  = 22
  assert(p21.r.ranks[2] ==  7); // 21+6  = 27
  assert(p21.r.ranks[3] ==  8); // 21+8  = 29
  assert(p21.r.ranks[4] ==  6); // 21+18 = 39

  assert(p22.r.ranks[0] == 10); // 22-1+1  = 22
  assert(p22.r.ranks[1] ==  7); // 22-1+6  = 27
  assert(p22.r.ranks[2] ==  8); // 22-1+8  = 29
  assert(p22.r.ranks[3] ==  6); // 22-1+18 = 39
  assert(p22.r.ranks[4] ==  5); // 22-1+21 = 42

  assert(p4.r.ranks[0] == 13); // 6
  assert(p4.r.ranks[1] == 11); // 8
  assert(p4.r.ranks[2] == 12); // 18
  assert(p4.r.ranks[3] ==  9); // 21
  assert(p4.r.ranks[4] == 10); // 22

  assert(p7.r.ranks[0] == 11); // 8
  assert(p7.r.ranks[1] == 12); // 18
  assert(p7.r.ranks[2] ==  9); // 21
  assert(p7.r.ranks[3] == 10); // 22
  assert(p7.r.ranks[4] ==  7); // 27

  assert(p11.r.ranks[0] == 12); // 18
  assert(p11.r.ranks[1] ==  9); // 21
  assert(p11.r.ranks[2] == 10); // 22
  assert(p11.r.ranks[3] ==  7); // 27
  assert(p11.r.ranks[4] ==  8); // 29

  assert(p20.r.ranks[0] ==  9); // 21
  assert(p20.r.ranks[1] == 10); // 22
  assert(p20.r.ranks[2] ==  7); // 27
  assert(p20.r.ranks[3] ==  8); // 29
  assert(p20.r.ranks[4] ==  6); // 39

  // try some comparisons

  // 4 vs 20 k=2 4->6 has rank 13 ; 20->22 has rank 10
  assert(compareSampleRanks(o4, o20, n, Ranks, cover) > 0);
  assert(compareSampleRanks(o20, o4, n, Ranks, cover) < 0);

  // 20 vs 21 k=1  20->21 has rank 9 ; 21->22 has rank 10
  assert(compareSampleRanks(o20, o21, n, Ranks, cover) < 0);
  assert(compareSampleRanks(o21, o20, n, Ranks, cover) > 0);
  assert(compareSampleRanks(p21, o20, n, Ranks, cover) > 0);

  // 21 vs 22 k=0  21 has rank 9 ; 22 has rank 10
  assert(compareSampleRanks(o21, o22, n, Ranks, cover) < 0);
  assert(compareSampleRanks(p21, o22, n, Ranks, cover) < 0);
  assert(compareSampleRanks(o22, o21, n, Ranks, cover) > 0);
  assert(compareSampleRanks(p22, o21, n, Ranks, cover) > 0);

  // 22 vs 23 k=20  42 has rank 5 ; 43 has rank 4
  // BUT n=24 so both are beyond the end of the string, so 42 > 43
  assert(compareSampleRanks(o22, o23, n, Ranks, cover) > 0);
  assert(compareSampleRanks(p22, o23, n, Ranks, cover) > 0);
  assert(compareSampleRanks(o23, o22, n, Ranks, cover) < 0);

  // 21 vs 23 k=6  27 has rank 7 ; 29 has rank 8
  assert(compareSampleRanks(o21, o23, n, Ranks, cover) < 0);
  assert(compareSampleRanks(p21, o23, n, Ranks, cover) < 0);
  assert(compareSampleRanks(o23, o21, n, Ranks, cover) > 0);

  // 4 vs 21 k=18  22 has rank 10 ; 39 has rank 6
  assert(compareSampleRanks(o4, o21, n, Ranks, cover) > 0);
  assert(compareSampleRanks(o21, o4, n, Ranks, cover) < 0);

  // 4 vs 22 k=17  21 has rank 9 ; 39 has rank 6
  assert(compareSampleRanks(o4, o22, n, Ranks, cover) > 0);
  assert(compareSampleRanks(o22, o4, n, Ranks, cover) < 0);

  // 4 vs 23 k=4  8 has rank 11 ; 27 has rank 7
  assert(compareSampleRanks(o4, o23, n, Ranks, cover) > 0);
  assert(compareSampleRanks(o23, o4, n, Ranks, cover) < 0);

  // 11 vs 20 k=7  18 has rank 12 ; 27 has rank 7
  assert(compareSampleRanks(p11, p20, n, Ranks, cover) > 0);

  // k=2
  assert(compareSampleRanks(p4, p20, n, Ranks, cover) > 0);
  // k=18
  assert(compareSampleRanks(p4, p11, n, Ranks, cover) > 0);
  // k=11
  assert(compareSampleRanks(p7, p11, n, Ranks, cover) > 0);
}

private proc testComparisons() {
  testPrefixComparisons(uint(8));
  testPrefixComparisons(uint);

  testRankComparisons3();
  testRankComparisons21();
}

proc testSorts() {
  const inputStr = "aaaaaaaaaaaabbbbbbbbbbaA";
                //            11111111112222
                //  012345678901234567890123

  /* suffixes

   aaaaaaaa aaaabbbb bbbbbbaA  0
   aaaaaaaa aaabbbbb bbbbbaA   1
   aaaaaaaa aabbbbbb bbbbaA    2
   aaaaaaaa abbbbbbb bbbaA     3
   aaaaaaaa bbbbbbbb bbaA      4
   aaaaaaab bbbbbbbb baA       5
   aaaaaabb bbbbbbbb aA        6
   aaaaabbb bbbbbbba A         7
   aaaabbbb bbbbbbaA           8
   aaabbbbb bbbbbaA            9
   aabbbbbb bbbbaA            10
   abbbbbbb bbbaA             11
   bbbbbbbb bbaA              12
   bbbbbbbb baA               13
   bbbbbbbb aA                14
   bbbbbbba A                 15
   bbbbbbaA                   16
   bbbbbaA                    17
   bbbbaA                     18
   bbbaA                      19
   bbaA                       20
   baA                        21
   aA                         22
   A                          23

   sorted suffixes

   0 A                          23
   1 aA                         22

   2 aaaaaaaa aaaabbbb bbbbbbaA  0 this group needs > 1 word
   3 aaaaaaaa aaabbbbb bbbbbaA   1
   4 aaaaaaaa aabbbbbb bbbbaA    2
   5 aaaaaaaa abbbbbbb bbbaA     3
   6 aaaaaaaa bbbbbbbb bbaA      4

   7 aaaaaaab bbbbbbbb baA       5
   8 aaaaaabb bbbbbbbb aA        6
   9 aaaaabbb bbbbbbba A         7
  10 aaaabbbb bbbbbbaA           8
  11 aaabbbbb bbbbbaA            9
  12 aabbbbbb bbbbaA            10
  13 abbbbbbb bbbaA             11

  14 baA                        21
  15 bbaA                       20
  16 bbbaA                      19
  17 bbbbaA                     18
  18 bbbbbaA                    17
  19 bbbbbbaA                   16
  20 bbbbbbba A                 15

  21 bbbbbbbb aA                14 this group needs > 1 word
  22 bbbbbbbb baA               13
  23 bbbbbbbb bbaA              12
  */

  var Expect = [23, 22, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
                21, 20, 19, 18, 17, 16, 15, 14, 13, 12];

  param bitsPerChar=8;
  const cover = new differenceCover(3);
  const text = bytesToArray(inputStr);
  const n = inputStr.size;

  const cfg = new ssortConfig(idxType=int,
                              offsetType=int,
                              unsignedOffsetType=uint,
                              loadWordType=uint,
                              bitsPerChar=bitsPerChar,
                              n=n,
                              cover=cover,
                              locales=Locales,
                              nTasksPerLocale=1);
  const nBits = cfg.nBits;

  const Packed = packInput(cfg.loadWordType, text, n, cfg.bitsPerChar);

  var A: [0..<n] offsetAndCached(cfg.offsetType, cfg.loadWordType);
  var Empty: [A.domain] A.eltType;
  var EmptyBoundaries: [A.domain] uint(8);
  for i in 0..<n {
    A[i] = makeOffsetAndCached(cfg, i, Packed, n, nBits);
  }

  var readAgg = new SrcAggregator(cfg.loadWordType);

  /*writeln("input");
  for i in 0..<n do writeln(i, " ", A[i]);*/

  var B = A;
  var Scratch = Empty;
  var Boundaries = EmptyBoundaries;

  // sort by 1 word
  //var stats: statistics;
  writeln("Sorting by first word");

  sortByPrefixAndMark(cfg, Packed, none, none,
                      B, Scratch, Boundaries, 0..<n, 1);

  /*for i in 0..<n {
    writeln("B[", i, "] = ", B[i], " Boundaries[", i, "] = ", Boundaries[i]);
  }*/

  assert(isBucketBoundary(Boundaries[2]));
  assert(isEqualBucketBoundary(Boundaries[2]));
  assert(isBucketBoundary(Boundaries[21]));
  assert(isEqualBucketBoundary(Boundaries[21]));

  for i in 0..<n {
    if 2 <= i && i <= 6 {
      var off = offset(B[i]);
      assert(0 <= off && off <= 4);
      if i > 2 {
        assert(!isBucketBoundary(Boundaries[i]));
      }
    } else if 21 <= i && i <= 23 {
      var off = offset(B[i]);
      assert(12 <= off && off <= 14);
      if i > 21 {
        assert(!isBucketBoundary(Boundaries[i]));
      }
    } else {
      assert(isBucketBoundary(Boundaries[i]));
      var off = offset(B[i]);
      assert(off == Expect[i]);
    }
  }

  // sort by 2 words
  writeln("Sorting by two words");
  B = A;
  Scratch = Empty;
  Boundaries = EmptyBoundaries;

  sortByPrefixAndMark(cfg, Packed, none, none,
                      B, Scratch, Boundaries, 0..<n, 16);

  /*for i in 0..<n {
    writeln("B[", i, "] = ", B[i], " Boundaries[", i, "] = ", Boundaries[i]);
  }*/

  for i in 0..<n {
    assert(isBucketBoundary(Boundaries[i]));
    var off = offset(B[i]);
    assert(off == Expect[i]);
  }

  // sort by 3 words
  writeln("Sorting by three words");
  B = A;
  Scratch = Empty;
  Boundaries = EmptyBoundaries;

  sortByPrefixAndMark(cfg, Packed, none, none,
                      B, Scratch, Boundaries, 0..<n, 24);

  /*for i in 0..<n {
    writeln("B[", i, "] = ", B[i], " Boundaries[", i, "] = ", Boundaries[i]);
  }*/

  for i in 0..<n {
    assert(isBucketBoundary(Boundaries[i]));
    var off = offset(B[i]);
    assert(off == Expect[i]);
  }
}

// test suffix sorting stuff with "seeresses" as input.
private proc testSeeresses() {
  writeln("Testing suffix sorting of 'seeresses'");

  const input = "seeresses";
  const n = input.size;
  const inputArr = bytesToArray(input);

  /*

    seeresses
    012345678

    here is the suffix array and LCP:
              SA         LCP
    eeresses  1          0
    eresses   2          1
    es        7          1
    esses     4          2
    resses    3          0
    s         8          0
    seeresses 0          1
    ses       6          2
    sses      5          1

    here are the sampled suffixes for DC3

    seeresses
    01 34 67  (initial offsets)
    01 23 45  (sample offsets)

    and their sorted data

              (initial offset)  (sample offset) (rank, 1-based)
    eer esses  1                 1               1
    es         7                 5               2
    ess es     4                 3               3
    res ses    3                 2               4
    see resses 0                 0               5
    ses        6                 4               6

    recursive subproblem input
      pad out to have same number in each section & always include a 0
      [rank(i) with i mod 3 == 0] <pad> [rank(j) with j mod 3 == 1] <pad>

                                                (recursive input this column)
              (initial offset)  (sample offset) (rank, 1-based)
    see resses 0                 0               5
    res ses    3                 1               4
    ses        6                 2               6
    <padding>                    -               0

    eer esses  1                 4               1
    ess es     4                 5               3
    es         7                 6               2
    <padding>                    -               0

    in summary, the recursive subroblem input is:

    54601320
    01234567

    recursive problem suffix array is:
             (offset) (rank)
    0        7        1
    01320    3        2
    1320     4        3
    20       6        4
    320      5        5
    4601320  1        6
    54601320 0        7
    601320   2        8

    recursive subproblem output suffix array
    73465102
    01234567

    ranks from recursive subproblem in original offset order

    (initial offset)  (recursive problem idx) (rank from recursion, 1-based)
    see resses 0                 0               7
    eer esses  1                 4               3
    res ses    3                 1               6
    ess es     4                 5               5
    ses        6                 2               8
    es         7                 6               4
    <padding>                                    2
    <padding>                                    1
  */

  const expectOffsets = [1,2,7,4,3,8,0,6,5];

  // check different cached data types
  checkSeeressesCase(inputArr, n, expectOffsets, period=3);
  checkSeeressesCase(inputArr, n, expectOffsets, period=3, wordType=uint(8));
  checkSeeressesCase(inputArr, n, expectOffsets, period=3, bitsPerChar=8);
  checkSeeressesCase(inputArr, n, expectOffsets, period=3, simulateBig=true);

  testLCP("seeresses", expectOffsets, [0,1,1,2,0,0,1,2,1]);
}

proc helpSparseLCP(inputArr: [], n: int, expectSA: [] int, expectLCP: [] int,
                   param q) {
  // compute the expected PLCP array
  var PLCP:[0..<n] int;
  forall i in 0..<n {
    PLCP[expectSA[i]] = expectLCP[i];
  }

  // compute the sparse PLCP array (which is what we are testing here)
  const SparsePLCP = computeSparsePLCP(inputArr, n, expectSA, q);

  // check that the sparse PLCP array matches the real PLCP array
  for i in SparsePLCP.domain {
    assert(SparsePLCP[i] == PLCP[i*q]);
  }

  // check the lookup function
  for i in 0 ..<n {
    var gotLCPi = lookupLCP(inputArr, n, expectSA, SparsePLCP, i, q);
    assert(gotLCPi == expectLCP[i]);
  }
}

proc checkSparseLCP(inputArr: [], n: int, expectSA: [] int, expectLCP: [] int) {
  helpSparseLCP(inputArr, n, expectSA, expectLCP, 1);
  helpSparseLCP(inputArr, n, expectSA, expectLCP, 2);
  helpSparseLCP(inputArr, n, expectSA, expectLCP, 3);
  helpSparseLCP(inputArr, n, expectSA, expectLCP, 4);
  helpSparseLCP(inputArr, n, expectSA, expectLCP, 5);
  helpSparseLCP(inputArr, n, expectSA, expectLCP, 6);
  helpSparseLCP(inputArr, n, expectSA, expectLCP, 7);
  helpSparseLCP(inputArr, n, expectSA, expectLCP, 8);
  helpSparseLCP(inputArr, n, expectSA, expectLCP, 16);
  helpSparseLCP(inputArr, n, expectSA, expectLCP, 32);
  helpSparseLCP(inputArr, n, expectSA, expectLCP, 64);
}

proc testLCP(inputArr: [], n: int, expectSA: [] int, expectLCP: [] int) {
  var LCP = lcpParPlcp(inputArr, n, expectSA);
  checkLCP(LCP, expectLCP);

  checkSparseLCP(inputArr, n, expectSA, expectLCP);
}

proc testLCP(input: string, expectSA: [] int, expectLCP: [] int) {
  const n = input.size;
  const inputArr = bytesToArray(input);
  testLCP(inputArr, n, expectSA, expectLCP);
}

proc testOtherCase(input: string, expectSA: [] int,
                   param period, type wordType) {
  writeln("testOtherCase(input='", input, "', period=", period,
          ", wordType=", wordType:string, ")");

  const n = input.size;
  const inputArr = bytesToArray(input);

  type offsetType = int(numBits(wordType));
  type unsignedOffsetType = uint(numBits(wordType));
 
  const cfg = new ssortConfig(idxType=int,
                              offsetType=offsetType,
                              unsignedOffsetType=unsignedOffsetType,
                              loadWordType=unsignedOffsetType,
                              bitsPerChar=8,
                              n=n,
                              cover=new differenceCover(period),
                              locales=Locales,
                              nTasksPerLocale=1);

  const Packed = packInput(cfg.loadWordType,
                           inputArr, n, cfg.bitsPerChar);

  const SA = ssortDcx(cfg, Packed);

  if TRACE && n <= 10 {
    writeln("Expect SA ", expectSA);
    writeln("Got SA    ", SA);
  }
  checkOffsets(SA, expectSA);
}

proc testOther(input: string, expectSA: [] int) {
  testOtherCase(input, expectSA, period=3, wordType=uint(8));
  testOtherCase(input, expectSA, period=3, wordType=uint(64));
  testOtherCase(input, expectSA, period=7, wordType=uint(8));
  testOtherCase(input, expectSA, period=7, wordType=uint(64));
}

proc testOthers() {
  /*
   abracadabra with DC3

             1
   01234567890
   abracadabra
   xx xx xx xx -- sample suffixes

   sort sample by first 3 characters

   suffix       offset  rank
   a            10      1
   abr a        7       2     |   these two could be in either order
   abr acadabra 0       2     |
   aca dabra    3       3
   bra cadabra  1       4
   cad abra     4       5
   dab ra       6       6
   ra           9       7

   Input for the recursive subproblem

   012345678
   236704521

   suffix    offset
   <pad>     0
   04521     4
   1         8
   21        7
   236704521 0
   36704521  1
   4521      5
   521       6
   6704521   2
   704521    3


   resulting suffix array

   01234567890
   abracadabra

                SA  LCP
   a            10  0
   abra         7   1
   abracadabra  0   4
   acadabra     3   1
   adabra       5   1
   bra          8   0
   bracadabra   1   3
   cadabra      4   0
   dabra        6   0
   ra           9   0
   racadabra    2   2

          0 1 2 3 4 5 6 7 8 9 10
   PHI :  7 8 9 0 1 3 4 A 5 6 -1
   PLCP1: 4 3 2 1 0 1 0 1 0 0 0
   PLCP2: 4   2   0   0   0   0
  */
  testOther("abracadabra", [10,7,0,3,5,8,1,4,6,9,2]);
  testLCP("abracadabra", [10,7,0,3,5,8,1,4,6,9,2], [0,1,4,1,1,0,3,0,0,0,2]);


  /*
               SA     LCP
   i           10     0
   ippi        7      1
   issippi     4      1
   ississippi  1      4
   mississippi 0      0
   pi          9      0
   ppi         8      1
   sippi       6      0
   sissippi    3      2
   ssippi      5      1
   ssissippi   2      3

   forming subproblem names
                     offset  subproblem name=bkt start offset + 3
   0 i               10      3
   1 ipp i           7       4
   2 iss ippi        4       5
   3 iss issippi     1       5
   4 mis sissippi    0       7
   5 pi              9       8
   6 sip pi          6       9
   7 sis sippi       3      10

   subproblem input
   n0 n3 n6 n9 1 n1 n4 n7 n10 0
    7 10  9  8 1  5  5  4   3 0

   */
  testOther("mississippi", [10,7,4,1,0,9,8,6,3,5,2]);
  testLCP("mississippi", [10,7,4,1,0,9,8,6,3,5,2], [0,1,1,4,0,0,1,0,2,1,3]);

  /*
   aaaacaaaacaaaab with DC3

             11111
   012345678901234
   aaaacaaaacaaaab
   xx xx xx xx xx -- sample suffixes

   sort sample by first 3 characters

   suffix           offset  name
   aaa ab           10      3   | could be in any order
   aaa acaaaacaaaab 0       3   |
   aaa caaaab       6       3   |
   aaa caaaacaaaab  1       3   |
   aab              12      4
   aac aaaab        7       5
   ab               13      6
   aca aaacaaaab    3       7
   caa aab          9       8   | any order
   caa aacaaaab     4       8   |


   subproblem input
       0  1  2  3   4 5  6  7  8   9  10 11
      n0 n3 n6 n9 n12 1 n1 n4 n7 n10 n13 0
       3  7  3  1                  3
      
   charsPerMod 6
                               11
   subproblem input  012345678901
                     4a5b702c8390

                         11   111
      regular offset 036925147036

   suffix           offset
   aaa ab           10
   aaa acaaaab      5
   aaa acaaaacaaaab 0
   aaa b            11
   aaa caaaab       6
   aaa caaaacaaaab  1
   aab              12
   aac aaaab        7
   aac aaaacaaaab   2
   ab               13
   aca aaab         8
   aca aaacaaaab    3
   b                14
   caa aab          9
   caa aacaaaab     4


   Suffix Array
             11111
   012345678901234
   aaaacaaaacaaaab

                   SA   LCP
   aaaab           10   0
   aaaacaaaab      5    4
   aaaacaaaacaaaab 0    9
   aaab            11   3
   aaacaaaab       6    3
   aaacaaaacaaaab  1    8
   aab             12   2
   aacaaaab        7    2
   aacaaaacaaaab   2    7
   ab              13   1
   acaaaab         8    1
   acaaaacaaaab    3    6
   b               14   0
   caaaab          9    0
   caaaacaaaab     4    5
  */
  testOther("aaaacaaaacaaaab", [10,5,0,11,6,1,12,7,2,13,8,3,14,9,4]);
  testLCP("aaaacaaaacaaaab", [10,5,0,11,6,1,12,7,2,13,8,3,14,9,4],
          [0,4,9,3,3,8,2,2,7,1,1,6,0,0,5]);

  testOther("banana$", [6,5,3,1,0,4,2]);
  testLCP("banana$", [6,5,3,1,0,4,2], [0,0,1,3,0,0,2]);


  /*

  abaababa
  01234567

  here is the suffix array and LCP:
            SA         LCP
  a         7          0
  aababa    2          1
  aba       5          1
  abaababa  0          3
  ababa     3          3
  ba        6          0
  baababa   1          2
  baba      4          2
  */
  testOther("abaababa", [7,2,5,0,3,6,1,4]);
  testLCP("abaababa", [7,2,5,0,3,6,1,4], [0,1,1,3,3,0,2,2]);
}

proc testRepeatsCase(c: uint(8), n: int, param period,
                     finalSortSimpleSortLimit: int = SIMPLE_SORT_LIMIT) {
  writeln("testRepeatsCase(c=", c, ", n=", n, ", period=", period,
          ", finalSortSimpleSortLimit=", finalSortSimpleSortLimit, ")");

  var inputArr: [0..<n+INPUT_PADDING] uint(8);
  var expectSA: [0..<n] int;

  forall i in 0..<n {
    inputArr[i] = c;
    expectSA[i] = n-i-1;
  }

  type offsetType = int; // always int for this test

  const cfg = new ssortConfig(idxType=inputArr.idxType,
                              offsetType=offsetType,
                              bitsPerChar=8,
                              n=n,
                              cover=new differenceCover(period),
                              locales=Locales,
                              nTasksPerLocale=computeNumTasks(),
                              finalSortSimpleSortLimit=finalSortSimpleSortLimit,
                              assumeNonLocal=finalSortSimpleSortLimit<SIMPLE_SORT_LIMIT);

  const Packed = packInput(cfg.loadWordType,
                           inputArr, n, cfg.bitsPerChar);

  const SA = ssortDcx(cfg, Packed);

  if TRACE && n <= 50 {
    writeln("Input     ", inputArr[0..<n]);
    writeln("Expect SA ", expectSA);
    writeln("Got SA    ", SA);
  }
  checkOffsets(SA, expectSA);

  // check also the LCP
  var expectLCP: [0..<n] int = 0..<n;
  testLCP(inputArr, n, expectSA, expectLCP);
}

proc testRepeats() {
  // TODO: figure out what's going wrong with n=0;
  // getting a runtime error: size mismatch in zippered iteration
  const sizes = [/*0,*/ 1, 2, 3, 4, 5, 10, 20, 50, 100, 1000, 10000];

  for (size,i) in zip(sizes,1..) {
    const chr = i:uint(8);
    testRepeatsCase(c=chr, n=size, period=3);
    testRepeatsCase(c=0, n=size, period=3);
    testRepeatsCase(c=chr, n=size, period=3, finalSortSimpleSortLimit=3);

    testRepeatsCase(c=chr, n=size, period=7);
    testRepeatsCase(c=0, n=size, period=7);
    testRepeatsCase(c=chr, n=size, period=7, finalSortSimpleSortLimit=3);

    testRepeatsCase(c=chr, n=size, period=13);
    testRepeatsCase(c=0, n=size, period=13);
    testRepeatsCase(c=chr, n=size, period=13, finalSortSimpleSortLimit=3);

    testRepeatsCase(c=chr, n=size, period=21);
    testRepeatsCase(c=0, n=size, period=21);
    testRepeatsCase(c=chr, n=size, period=21, finalSortSimpleSortLimit=3);

    testRepeatsCase(c=chr, n=size, period=133);
    testRepeatsCase(c=0, n=size, period=133);
    testRepeatsCase(c=chr, n=size, period=133, finalSortSimpleSortLimit=3);
  }
}

/* Test sequences consisting of descending characters.
   The pattern
   max, max-1, max-2, ..., 0
   is repeated enough times to create the input.

   max must be at most 256.
 */
proc testDescendingCase(max: int, repeats: int, in n: int, param period) {
  writeln("testDescendingCase(",
          "max=", max, ", repeats=", repeats, ", n=", n, ", ",
          "period=", period, ")");

  var inputArr: [0..<n+INPUT_PADDING] uint(8);
  var expectSA: [0..<n] int;
  var numCycles:int;
  var cycleLen:int;

  // Use a length that is a multiple of max*repeats.
  cycleLen = max*repeats;
  n /= cycleLen;
  n *= cycleLen;
  numCycles = n / (max*repeats);

  assert( n % max == 0 );
  assert( max <= 256 );

  var c = (max-1):uint(8);
  var count = repeats-1;
  for i in 0..<n {
    inputArr[i] = c;
    if count == 0 {
      if c == 0 then c = (max-1):uint(8);
      else c -= 1;
      count = repeats-1;
    } else {
      count -= 1;
    }
  }

  var i = 0;
  {
    // First, the 0# 00# 000# ...
    while i < repeats {
      expectSA[i] = n - i - 1;
      i += 1;
    }
  }
  {
    // Next, the other 003's and then the 003's
    var rep = repeats - 1;
    while rep >= 0 {
      var cycle = numCycles-2;
      while cycle >= 0 {
        expectSA[i] = cycle*cycleLen + cycleLen - rep - 1;
        i += 1;
        cycle -= 1;
      }
      rep -= 1;
    }

    // Finally, the first 1.
    var offset = n - repeats - 1;
    while i < n {
      expectSA[i] = offset;
      offset -= max*repeats;
      if offset < 0 {
        offset += n-1;
      }
      i += 1;
    }
  }

  writeln("descending INPUT ");
  for i in 0..<n {
    writeln("T[", i, "] = ", inputArr[i]);
  }
 
  type offsetType = int; // always int for this test

  const cfg = new ssortConfig(idxType=int,
                              offsetType=offsetType,
                              bitsPerChar=8,
                              n=n,
                              cover=new differenceCover(period),
                              locales=Locales,
                              nTasksPerLocale=computeNumTasks());
  const Packed = packInput(uint, inputArr, n, cfg.bitsPerChar);
  const SA = ssortDcx(cfg, Packed);

 
  if TRACE && n <= 50 {
    writeln("Input     ", inputArr[0..<n]);
    writeln("Expect SA ", expectSA);
    writeln("Got SA    ", SA);
  }
  checkOffsets(SA, expectSA);
}

proc testDescending() {
  const configs = [
                   // small
                   (2, 5, 2*5*4),
                   (2, 8, 2*8*2),
                   (2, 8, 2*8*4),
                   (3, 3, 3*3*2),
                   (4, 1, 4*1*2),
                   (4, 2, 4*2*2),
                   (4, 2, 4*2*3),
                   (4, 3, 4*3*2),
                   (4, 3, 4*3*3),
                   (4, 8, 4*8*3),
                   (4, 8, 4*8*4),

                   // medium
                   (2, 32, 2*32*4),
                   (4, 8, 4*8*10),
                   (4, 8, 4*8*100),
                   (4, 8, 4*8*1024),
                   (20, 2, 20*2*4),
                   (50, 5, 50*5*10),
                   (200, 1, 200),
                   (255, 1, 255),
                   (255, 2, 255*2),
                   (256, 1, 256),
                   (256, 1, 256*1*2),
                   (256, 2, 256*2*2),
                   (256, 8, 256*8*2),
  ];

  for tup in configs {
    const (max, repeats, n) = tup;
    testDescendingCase(max, repeats, n, period=3);

    testDescendingCase(max, repeats, n, period=7);

    testDescendingCase(max, repeats, n, period=13);

    testDescendingCase(max, repeats, n, period=21);

    testDescendingCase(max, repeats, n, period=133);
  }
}


proc runTests() {
  //testDescendingCase(max=2, repeats=8, n=32, period=21);
  //testDescendingCase(max=2, repeats=4, n=56, period=13)

  /*
  for i in 1..1000 {
    for max in 2..16 {
      for repeats in 1..16 {
        testDescendingCase(max, repeats, max*repeats*i, period=21);
        testDescendingCase(max, repeats, max*repeats*i, period=13);
      }
    }
  }*/

  testHelpers();
  testComparisons();
  testSorts();
  testSeeresses();
  testOthers();
  testRepeats();
  testDescending();
}

proc main() {
  serial {
    writeln("Testing with one task");
    runTests();
  }

  writeln("Testing with many tasks");
  runTests();

  writeln("TestSuffixSort OK");
}


}
