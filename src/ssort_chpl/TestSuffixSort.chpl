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
use Math;
use IO;
use Sort;

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

private proc checkSeeressesCase(type offsetType,
                                type cachedDataType,
                                type loadWordType,
                                inputArr, n:int, param period,
                                expectOffsets, expectCached:?t = none) {
  if TRACE {
    writeln("  ", offsetType:string, " offsets, caching ", cachedDataType:string);
  }

  const cfg = new ssortConfig(idxType=inputArr.idxType,
                              characterType=inputArr.eltType,
                              offsetType=offsetType,
                              cachedDataType=cachedDataType,
                              loadWordType=loadWordType,
                              cover=new differenceCover(period),
                              locales=Locales);

  if expectCached.type != nothing {
    const A = buildAllOffsets(cfg, inputArr, n, {0..<n});
    checkCached(A, expectCached);
  }
  const SA = computeSuffixArrayDirectly(cfg, inputArr, n:offsetType,
                                        {0..<n:offsetType});
  checkOffsets(SA, expectOffsets);
  if !isIntegralType(SA.eltType) {
    assert(SA.eltType.cacheType == cachedDataType);
  }

  if !isIntegralType(SA.eltType) {
    checkCached(SA, expectCached);
  }

  // try ssortDcx
  const SA2 = ssortDcx(cfg, inputArr, n:offsetType);
  checkOffsets(SA2, expectOffsets);
  if !isIntegralType(SA2.eltType) {
    assert(SA2.eltType.cacheType == cachedDataType);
  }

  if !isIntegralType(SA2.eltType) {
    checkCached(SA2, expectCached);
  }
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

  {
    const cfg = new ssortConfig(idxType=int,
                                characterType=uint(8),
                                offsetType=int,
                                cachedDataType=nothing,
                                loadWordType=uint(8),
                                cover=new differenceCover(3),
                                locales=Locales);

    assert(cfg.getPrefixSize(3) == 3);
    assert(cfg.getPrefixSize(7) == 7);
    assert(cfg.getPrefixSize(21) == 21);
  }

  {
    const cfg = new ssortConfig(idxType=int,
                                characterType=uint(8),
                                offsetType=int,
                                cachedDataType=nothing,
                                loadWordType=uint(16),
                                cover=new differenceCover(3),
                                locales=Locales);

    assert(cfg.getPrefixSize(3) == 4);
    assert(cfg.getPrefixSize(7) == 8);
    assert(cfg.getPrefixSize(21) == 22);
  }

  {
    const cfg = new ssortConfig(idxType=int,
                                characterType=uint(8),
                                offsetType=int,
                                cachedDataType=nothing,
                                loadWordType=uint(32),
                                cover=new differenceCover(3),
                                locales=Locales);

    assert(cfg.getPrefixSize(3) == 4);
    assert(cfg.getPrefixSize(7) == 8);
    assert(cfg.getPrefixSize(21) == 24);
  }

  {
    const cfg = new ssortConfig(idxType=int,
                                characterType=uint(8),
                                offsetType=int,
                                cachedDataType=nothing,
                                loadWordType=uint(64),
                                cover=new differenceCover(3),
                                locales=Locales);

    assert(cfg.getPrefixSize(3) == 8);
    assert(cfg.getPrefixSize(7) == 8);
    assert(cfg.getPrefixSize(21) == 24);
  }

  {
    const cfg = new ssortConfig(idxType=int,
                                characterType=uint(64),
                                offsetType=int,
                                cachedDataType=uint(64),
                                loadWordType=uint(64),
                                cover=new differenceCover(3),
                                locales=Locales);

    assert(cfg.getPrefixSize(3) == 3);
    assert(cfg.getPrefixSize(7) == 7);
    assert(cfg.getPrefixSize(21) == 21);
  }
}

private proc testPrefixComparisons(type loadWordType, type cachedDataType) {
  const cover = new differenceCover(3);
  const cfg = new ssortConfig(idxType=int,
                              characterType=uint(8),
                              offsetType=int,
                              cachedDataType=cachedDataType,
                              loadWordType=loadWordType,
                              cover=cover,
                              locales=Locales);
  const inputStr = "aabbccaaddffffffffaabbccaaddff";
                 //           11111111112222222222
                 // 012345678901234567890123456789
                 // * |   *           *
                 // AA|   AA2         AA3
                 //   BB
  const text = bytesToArray(inputStr);
  const n = inputStr.size;

  // these are irrelevant here
  const charsPerMod = 2;
  const ranks = for i in text do 0;
  var ranksN = n;

  const prefixAA =  makeOffsetAndCached(cfg, 0, text, n);
  const prefixAA2 = makeOffsetAndCached(cfg, 6, text, n);
  const prefixAA3 = makeOffsetAndCached(cfg, 18, text, n);
  const prefixBB =  makeOffsetAndCached(cfg, 2, text, n);

  const prefixAAp = makePrefix(cfg, 0, text, n);
  const prefixAA2p = makePrefix(cfg, 6, text, n);
  const prefixAA3p = makePrefix(cfg, 18, text, n);
  const prefixBBp = makePrefix(cfg, 2, text, n);

  const prefixAAs = makePrefixAndSampleRanks(cfg, 0,
                                             text, n,
                                             ranks,
                                             charsPerMod=charsPerMod);
  const prefixAA2s = makePrefixAndSampleRanks(cfg, 6,
                                              text, n,
                                              ranks,
                                              charsPerMod=charsPerMod);
   const prefixAA3s = makePrefixAndSampleRanks(cfg, 18,
                                              text, n,
                                              ranks,
                                              charsPerMod=charsPerMod);

  const prefixBBs = makePrefixAndSampleRanks(cfg, 2,
                                             text, n,
                                             ranks,
                                             charsPerMod=charsPerMod);

  assert(comparePrefixes(cfg, 0, 0, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(cfg, 0, 2, text, n, maxPrefix=2)<0);

  assert(comparePrefixes(cfg, prefixAA, prefixAA, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(cfg, prefixAA, prefixAA3, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(cfg, prefixAA, prefixAA2, text, n, maxPrefix=2)<=0);
  assert(comparePrefixes(cfg, prefixAA, prefixBB, text, n, maxPrefix=2)<0);
  assert(comparePrefixes(cfg, prefixBB, prefixAA, text, n, maxPrefix=2)>0);

  assert(comparePrefixes(cfg, prefixAAp, prefixAAp, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(cfg, prefixAAp, prefixBBp, text, n, maxPrefix=2)<0);
  assert(comparePrefixes(cfg, prefixBBp, prefixAAp, text, n, maxPrefix=2)>0);

  assert(comparePrefixes(cfg, prefixAA, prefixAAp, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(cfg, prefixAA, prefixBBp, text, n, maxPrefix=2)<0);
  assert(comparePrefixes(cfg, prefixAAp, prefixBB, text, n, maxPrefix=2)<0);
  assert(comparePrefixes(cfg, prefixBBp, prefixAA, text, n, maxPrefix=2)>0);
  assert(comparePrefixes(cfg, prefixBB, prefixAAp, text, n, maxPrefix=2)>0);

  assert(comparePrefixes(cfg, prefixAAp, prefixAAs, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(cfg, prefixAAs, prefixAAp, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(cfg, prefixAAs, prefixBBs, text, n, maxPrefix=2)<0);
  assert(comparePrefixes(cfg, prefixAAs, prefixBBp, text, n, maxPrefix=2)<0);
  assert(comparePrefixes(cfg, prefixAAp, prefixBBs, text, n, maxPrefix=2)<0);

  assert(comparePrefixes(cfg, prefixBBs, prefixAAs, text, n, maxPrefix=2)>0);
  assert(comparePrefixes(cfg, prefixBBs, prefixAAp, text, n, maxPrefix=2)>0);
  assert(comparePrefixes(cfg, prefixBBp, prefixAAs, text, n, maxPrefix=2)>0);

  assert(charactersInCommon(cfg, prefixAAp, prefixAAp) >= cover.period);
  assert(charactersInCommon(cfg, prefixAAs, prefixAAs) >= cover.period);
  assert(charactersInCommon(cfg, prefixAAp, prefixAA2p) == 2);
  assert(charactersInCommon(cfg, prefixAAs, prefixAA2s) == 2);
  assert(charactersInCommon(cfg, prefixAA2p, prefixAA2p) >= cover.period);
  assert(charactersInCommon(cfg, prefixAA2s, prefixAA2s) >= cover.period);
  assert(charactersInCommon(cfg, prefixAAp, prefixBBp) == 0);
  assert(charactersInCommon(cfg, prefixAA3p, prefixAA3p) >= cover.period);
  assert(charactersInCommon(cfg, prefixAA3s, prefixAA3s) >= cover.period);
  assert(charactersInCommon(cfg, prefixAAp, prefixAA3p) >= cover.period);
}

proc testRankComparisons3() {
  const cover = new differenceCover(3);
  const cfg = new ssortConfig(idxType=int,
                              characterType=uint(8),
                              offsetType=int,
                              cachedDataType=nothing,
                              loadWordType=uint(8),
                              cover=cover,
                              locales=Locales);

  // create the mapping to the recursive problem
  const n = 16;
  const charsPerMod = 7;
  const nSample = charsPerMod*cover.sampleSize;
  var Text:[0..<n+INPUT_PADDING] uint(8);
  var Ranks:[0..<nSample] int; // this is sample offset to rank
  var Offsets:[0..<nSample] int; // sample offset to regular offset

  Ranks    = [14, 10,  6, 12,  8,  4,  2, 13,  9,  5, 11,  7,  3,  1];
  Offsets =  [ 0,  3,  6,  9, 12, 15, 18,  1,  4,  7, 10, 13, 16, 19];
  // sample    0   1   2   3   4   5   6   7   8   9  10  11  12  13
  //  offsets

  // check offsetToSubproblemOffset and subproblemOffsetToOffset
  for i in 0..<nSample {
    assert(Offsets[i] == subproblemOffsetToOffset(i, cover, charsPerMod));
    assert(i == offsetToSubproblemOffset(Offsets[i], cover, charsPerMod));
  }

  // check makePrefixAndSampleRanks

  // check a few cases we can see above
  const p1 = makePrefixAndSampleRanks(cfg, offset=1, Text, n,
                                      Ranks, charsPerMod=charsPerMod);
  const p3 = makePrefixAndSampleRanks(cfg, offset=3, Text, n,
                                      Ranks, charsPerMod=charsPerMod);
  const p19 = makePrefixAndSampleRanks(cfg, offset=19, Text, n,
                                      Ranks, charsPerMod=charsPerMod);
  const p2 = makePrefixAndSampleRanks(cfg, offset=2, Text, n,
                                      Ranks, charsPerMod=charsPerMod);
  const p5 = makePrefixAndSampleRanks(cfg, offset=5, Text, n,
                                      Ranks, charsPerMod=charsPerMod);

  assert(p1.ranks[0] == 13); // offset 1 -> sample offset 7 -> rank 13
  assert(p1.ranks[1] == 10); // offset 3 -> sample offset 1 -> rank 10

  assert(p3.ranks[0] == 10); // offset 3 -> sample offset 1 -> rank 10
  assert(p3.ranks[1] == 9);  // offset 4 -> sample offset 8 -> rank 9

  assert(p19.ranks[0] == 1); // offset 19 -> sample offset 13 -> rank 1
  assert(p19.ranks[1] == 0); // offset 21 -> sample offset -  -> rank 0

  assert(p2.ranks[0] == 10); // offset 2 -> next offset sample is 3 ->
                             // sample offset 1 -> rank 10
  assert(p2.ranks[1] == 9);  // offset 4 -> sample offset 8 -> rank 9

  assert(p5.ranks[0] == 6);  // offset 5 -> next offset sample is 6 ->
                             // sample offset 2 -> rank 6
  assert(p5.ranks[1] == 5);  // offset 7 -> sample offset 9 -> rank 5


  // check the rest of the cases
  for sampleOffset in 0..<nSample {
    const offset = subproblemOffsetToOffset(sampleOffset, cover, charsPerMod);
    const p = makePrefixAndSampleRanks(cfg, offset=offset, Text, n,
                                       Ranks, charsPerMod=charsPerMod);
    // find the next cover.sampleSize offsets in the cover
    var cur = 0;
    for i in 0..<cover.period {
      if offset+i < n && cover.containedInCover((offset + i) % cover.period) {
        const sampleOffset =
          offsetToSubproblemOffset(offset + i, cover, charsPerMod);
        assert(p.ranks[cur] == Ranks[sampleOffset]);
        cur += 1;
      }
    }
  }

  // try some comparisons
  const o1 = makeOffsetAndCached(cfg, 1, Text, n);
  const o3 = makeOffsetAndCached(cfg, 3, Text, n);
  const o5 = makeOffsetAndCached(cfg, 5, Text, n);
  const o19= makeOffsetAndCached(cfg,19, Text, n);

  // test self-compares
  assert(compareSampleRanks(o1, o1, n, Ranks, charsPerMod, cover) == 0);
  assert(compareSampleRanks(o3, o3, n, Ranks, charsPerMod, cover) == 0);
  assert(compareSampleRanks(o5, o5, n, Ranks, charsPerMod, cover) == 0);
  assert(compareSampleRanks(o19, o19, n, Ranks, charsPerMod, cover) == 0);

  assert(compareSampleRanks(p1, o1, n, Ranks, charsPerMod, cover) == 0);
  assert(compareSampleRanks(p3, o3, n, Ranks, charsPerMod, cover) == 0);
  assert(compareSampleRanks(p19, o19, n, Ranks, charsPerMod, cover) == 0);

  // test 1 vs 3 : 1 has rank 13 and 3 has rank 10
  assert(compareSampleRanks(o1, o3, n, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(p1, o3, n, Ranks, charsPerMod, cover) > 0);

  assert(compareSampleRanks(o3, o1, n, Ranks, charsPerMod, cover) < 0);
  assert(compareSampleRanks(p3, o1, n, Ranks, charsPerMod, cover) < 0);

  // test 3 vs 5 : use k=1, 3->4 has rank 9 ; 5->6 has rank 6
  assert(compareSampleRanks(o3, o5, n, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(p3, o5, n, Ranks, charsPerMod, cover) > 0);

  assert(compareSampleRanks(o5, o3, n, Ranks, charsPerMod, cover) < 0);

  // test 5 vs 19 : use k=2, 5->7 has rank 5 ; 19->21 has rank 0
  // BUT 19 is beyond the end of the string, so 5 > 19
  assert(compareSampleRanks(o5, o19, n, Ranks, charsPerMod, cover) > 0);

  assert(compareSampleRanks(o19, o5, n, Ranks, charsPerMod, cover) < 0);
  assert(compareSampleRanks(p19, o5, n, Ranks, charsPerMod, cover) < 0);
}

proc testRankComparisons21() {
  const cover = new differenceCover(21); // 0 1 6 8 18
  const cfg = new ssortConfig(idxType=int,
                              characterType=uint(8),
                              offsetType=int,
                              cachedDataType=nothing,
                              loadWordType=uint(8),
                              cover=cover,
                              locales=Locales);

  type offsetType = cfg.offsetType;
  type cachedDataType = cfg.cachedDataType;

  // create the mapping to the recursive problem
  const n = 24;
  const charsPerMod = 3;
  const nSample = charsPerMod*cover.sampleSize;
  var Text:[0..<n+INPUT_PADDING] uint(8);
  var Ranks:[0..<nSample] int; // this is sample offset to rank
  var Offsets:[0..<nSample] int; // sample offset to regular offset

  Ranks    = [15,  9,  5, 14, 10,  4, 13,  7,  2, 11,  8,  3, 12,  6,  1];
  Offsets  = [ 0, 21, 42,  1, 22, 43,  6, 27, 48,  8, 29, 50, 18, 39, 60];
  // sample    0   1   2   3   4   5   6   7   8   9  10  11  12  13  14
  //  offsets

  // check offsetToSubproblemOffset and subproblemOffsetToOffset
  for i in 0..<nSample {
    assert(Offsets[i] == subproblemOffsetToOffset(i, cover, charsPerMod));
    assert(i == offsetToSubproblemOffset(Offsets[i], cover, charsPerMod));
  }

  // check self-compares
  for i in 0..<n {
    const o = makeOffsetAndCached(cfg, i, Text,n);
    assert(compareSampleRanks(o, o, n, Ranks, charsPerMod, cover) == 0);
    if cover.containedInCover(i % cover.period) {
      const sampleOffset = offsetToSubproblemOffset(i, cover, charsPerMod);
      const p = makePrefixAndSampleRanks(cfg, offset=i, Text, n,
                                         Ranks, charsPerMod=charsPerMod);
      assert(compareSampleRanks(p, o, n, Ranks, charsPerMod, cover) == 0);
    }
  }

  const o4  = makeOffsetAndCached(cfg, 4, Text, n);
  const o20 = makeOffsetAndCached(cfg, 20, Text, n);
  const o21 = makeOffsetAndCached(cfg, 21, Text, n);
  const p21 = makePrefixAndSampleRanks(cfg, offset=21, Text, n,
                                       Ranks, charsPerMod=charsPerMod);
  const o22 = makeOffsetAndCached(cfg, 22, Text, n);
  const p22 = makePrefixAndSampleRanks(cfg, offset=22, Text, n,
                                       Ranks, charsPerMod=charsPerMod);
  const o23 = makeOffsetAndCached(cfg, 23, Text, n);

  const p4 = makePrefixAndSampleRanks(cfg, offset=4, Text, n,
                                      Ranks, charsPerMod=charsPerMod);

  const p7 = makePrefixAndSampleRanks(cfg, offset=7, Text, n,
                                      Ranks, charsPerMod=charsPerMod);

  const p11 = makePrefixAndSampleRanks(cfg, offset=11, Text, n,
                                       Ranks, charsPerMod=charsPerMod);

  const p20 = makePrefixAndSampleRanks(cfg, offset=20, Text, n,
                                       Ranks, charsPerMod=charsPerMod);

  // check p21 and p22 are ok
  assert(p21.ranks[0] ==  9); // 21+0  = 21
  assert(p21.ranks[1] == 10); // 21+1  = 22
  assert(p21.ranks[2] ==  7); // 21+6  = 27
  assert(p21.ranks[3] ==  8); // 21+8  = 29
  assert(p21.ranks[4] ==  6); // 21+18 = 39

  assert(p22.ranks[0] == 10); // 22-1+1  = 22
  assert(p22.ranks[1] ==  7); // 22-1+6  = 27
  assert(p22.ranks[2] ==  8); // 22-1+8  = 29
  assert(p22.ranks[3] ==  6); // 22-1+18 = 39
  assert(p22.ranks[4] ==  5); // 22-1+21 = 42

  assert(p4.ranks[0] == 13); // 6
  assert(p4.ranks[1] == 11); // 8
  assert(p4.ranks[2] == 12); // 18
  assert(p4.ranks[3] ==  9); // 21
  assert(p4.ranks[4] == 10); // 22

  assert(p7.ranks[0] == 11); // 8
  assert(p7.ranks[1] == 12); // 18
  assert(p7.ranks[2] ==  9); // 21
  assert(p7.ranks[3] == 10); // 22
  assert(p7.ranks[4] ==  7); // 27

  assert(p11.ranks[0] == 12); // 18
  assert(p11.ranks[1] ==  9); // 21
  assert(p11.ranks[2] == 10); // 22
  assert(p11.ranks[3] ==  7); // 27
  assert(p11.ranks[4] ==  8); // 29

  assert(p20.ranks[0] ==  9); // 21
  assert(p20.ranks[1] == 10); // 22
  assert(p20.ranks[2] ==  7); // 27
  assert(p20.ranks[3] ==  8); // 29
  assert(p20.ranks[4] ==  6); // 39

  // try some comparisons

  // 4 vs 20 k=2 4->6 has rank 13 ; 20->22 has rank 10
  assert(compareSampleRanks(o4, o20, n, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(o20, o4, n, Ranks, charsPerMod, cover) < 0);

  // 20 vs 21 k=1  20->21 has rank 9 ; 21->22 has rank 10
  assert(compareSampleRanks(o20, o21, n, Ranks, charsPerMod, cover) < 0);
  assert(compareSampleRanks(o21, o20, n, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(p21, o20, n, Ranks, charsPerMod, cover) > 0);

  // 21 vs 22 k=0  21 has rank 9 ; 22 has rank 10
  assert(compareSampleRanks(o21, o22, n, Ranks, charsPerMod, cover) < 0);
  assert(compareSampleRanks(p21, o22, n, Ranks, charsPerMod, cover) < 0);
  assert(compareSampleRanks(o22, o21, n, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(p22, o21, n, Ranks, charsPerMod, cover) > 0);

  // 22 vs 23 k=20  42 has rank 5 ; 43 has rank 4
  // BUT n=24 so both are beyond the end of the string, so 42 > 43
  assert(compareSampleRanks(o22, o23, n, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(p22, o23, n, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(o23, o22, n, Ranks, charsPerMod, cover) < 0);

  // 21 vs 23 k=6  27 has rank 7 ; 29 has rank 8
  // BUT n=24, so both of these are beyond the string, so 27 > 29
  assert(compareSampleRanks(o21, o23, n, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(p21, o23, n, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(o23, o21, n, Ranks, charsPerMod, cover) < 0);

  // 4 vs 21 k=18  22 has rank 10 ; 39 has rank 6
  // BUT n=24, so 39 is beyond the end of the string, so 22 > 39
  assert(compareSampleRanks(o4, o21, n, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(o21, o4, n, Ranks, charsPerMod, cover) < 0);

  // 4 vs 22 k=17  21 has rank 9 ; 39 has rank 6
  // BUT n=24, so 39 is beyond the end of the string, so 21 > 39
  assert(compareSampleRanks(o4, o22, n, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(o22, o4, n, Ranks, charsPerMod, cover) < 0);

  // 4 vs 23 k=4  8 has rank 11 ; 27 has rank 7
  assert(compareSampleRanks(o4, o23, n, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(o23, o4, n, Ranks, charsPerMod, cover) < 0);

  // 11 vs 20 k=7  18 has rank 12 ; 27 has rank 7
  assert(compareSampleRanks(p11, p20, n, Ranks, charsPerMod, cover) > 0);

  // k=2
  assert(compareSampleRanks(p4, p20, n, Ranks, charsPerMod, cover) > 0);
  // k=18
  assert(compareSampleRanks(p4, p11, n, Ranks, charsPerMod, cover) > 0);
  // k=11
  assert(compareSampleRanks(p7, p11, n, Ranks, charsPerMod, cover) > 0);
}

private proc testComparisons() {
  testPrefixComparisons(uint(8), nothing);
  testPrefixComparisons(uint, nothing);
  testPrefixComparisons(uint, uint);

  testRankComparisons3();
  testRankComparisons21();
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
    res ses    3                 2               4
    ses        6                 4               6
    <padding>                                    0

    eer esses  1                 1               1
    ess es     4                 3               3
    es         7                 5               2
    <padding>                                    0

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

    ranks from recursive subproblem
    76823541
  */

  const expectOffsets = [1,2,7,4,3,8,0,6,5];

  const expectCached1 = [bytesToUint("s"),
                         bytesToUint("e"),
                         bytesToUint("e"),
                         bytesToUint("r"),
                         bytesToUint("e"),
                         bytesToUint("s"),
                         bytesToUint("s"),
                         bytesToUint("e"),
                         bytesToUint("s")];
  const expectCached2 = [bytesToUint("se"),
                         bytesToUint("ee"),
                         bytesToUint("er"),
                         bytesToUint("re"),
                         bytesToUint("es"),
                         bytesToUint("ss"),
                         bytesToUint("se"),
                         bytesToUint("es"),
                         bytesToUint("s\x00")];
  const expectCached4 = [bytesToUint("seer"),
                         bytesToUint("eere"),
                         bytesToUint("eres"),
                         bytesToUint("ress"),
                         bytesToUint("esse"),
                         bytesToUint("sses"),
                         bytesToUint("ses\x00"),
                         bytesToUint("es\x00\x00"),
                         bytesToUint("s\x00\x00\x00")];
  const expectCached8 = [bytesToUint("seeresse"),
                         bytesToUint("eeresses"),
                         bytesToUint("eresses\x00"),
                         bytesToUint("resses\x00\x00"),
                         bytesToUint("esses\x00\x00\x00"),
                         bytesToUint("sses\x00\x00\x00\x00"),
                         bytesToUint("ses\x00\x00\x00\x00\x00"),
                         bytesToUint("es\x00\x00\x00\x00\x00\x00"),
                         bytesToUint("s\x00\x00\x00\x00\x00\x00\x00")];

  // check different cached data types
  checkSeeressesCase(offsetType=int, cachedDataType=nothing,
                     loadWordType=uint(8),
                     inputArr, n, 3, expectOffsets);
  checkSeeressesCase(offsetType=int, cachedDataType=uint(8),
                     loadWordType=uint(8),
                     inputArr, n, 7, expectOffsets, expectCached1);
  checkSeeressesCase(offsetType=int, cachedDataType=uint(16),
                     loadWordType=uint(16),
                     inputArr, n, 3, expectOffsets, expectCached2);
  checkSeeressesCase(offsetType=int, cachedDataType=uint(32),
                     loadWordType=uint(32),
                     inputArr, n, 13, expectOffsets, expectCached4);
  checkSeeressesCase(offsetType=int, cachedDataType=uint(64),
                     loadWordType=uint(64),
                     inputArr, n, 3, expectOffsets, expectCached8);

  // check some different offset types
  // TODO: fix Chapel module errors with these other types
  //checkSeeressesCase(offsetType=uint(32), cachedDataType=nothing,
  //                   inputArr, n, 3, expectOffsets);
  checkSeeressesCase(offsetType=int, cachedDataType=nothing,
                     loadWordType=uint(8),
                     inputArr, n, 3, expectOffsets);
  //checkSeeressesCase(offsetType=uint, cachedDataType=nothing,
  //                   inputArr, n, 3, expectOffsets);


  // check load word uint + uint(8) charactercs
  checkSeeressesCase(offsetType=int, cachedDataType=nothing,
                     loadWordType=uint,
                     inputArr, n, 3, expectOffsets);

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
                   param period, type cachedDataType) {
  writeln("testOtherCase(input='", input, "', period=", period, ", ",
                           "cachedDataType=", cachedDataType:string, ")");

  const n = input.size;
  const inputArr = bytesToArray(input);

  type offsetType = int; // always int for this test

  const cfg = new ssortConfig(idxType=inputArr.idxType,
                              characterType=inputArr.eltType,
                              offsetType=offsetType,
                              cachedDataType=cachedDataType,
                              loadWordType=
                                (if cachedDataType != nothing
                                 then cachedDataType
                                 else inputArr.eltType),
                              cover=new differenceCover(period),
                              locales=Locales);
  const SA = ssortDcx(cfg, inputArr, n:offsetType);

  if TRACE && n <= 10 {
    writeln("Expect SA ", expectSA);
    writeln("Got SA    ", SA);
  }
  checkOffsets(SA, expectSA);
}

proc testOther(input: string, expectSA: [] int) {
  testOtherCase(input, expectSA, period=3, cachedDataType=nothing);
  testOtherCase(input, expectSA, period=3, cachedDataType=uint);

  testOtherCase(input, expectSA, period=7, cachedDataType=nothing);
  testOtherCase(input, expectSA, period=7, cachedDataType=uint);
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

   suffix           offset  rank
   aaa ab           10      1   | could be in any order
   aaa acaaaacaaaab 0       1   |
   aaa caaaab       6       1   |
   aaa caaaacaaaab  1       1   |
   aab              12      2
   aac aaaab        7       3
   ab               13      4
   aca aaacaaaab    3       5
   caa aab          9       7   | any order
   caa aacaaaab     4       7   |


   charsPerMod 6
                               11
   subproblem offset 012345678901

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

proc testRepeatsCase(c: uint(8), n: int, param period, type cachedDataType) {
  writeln("testRepeatsCase(c=", c, ", n=", n, ", period=", period, ", ",
                           "cachedDataType=", cachedDataType:string, ")");

  var inputArr: [0..<n+INPUT_PADDING] uint(8);
  var expectSA: [0..<n] int;

  forall i in 0..<n {
    inputArr[i] = c;
    expectSA[i] = n-i-1;
  }

  type offsetType = int; // always int for this test

  const cfg = new ssortConfig(idxType=inputArr.idxType,
                              characterType=inputArr.eltType,
                              offsetType=offsetType,
                              cachedDataType=cachedDataType,
                              loadWordType=
                                (if cachedDataType != nothing
                                 then cachedDataType
                                 else uint),
                              cover=new differenceCover(period),
                              locales=Locales);
  const SA = ssortDcx(cfg, inputArr, n:offsetType);

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
    testRepeatsCase(c=chr, size, period=3, cachedDataType=nothing);
    testRepeatsCase(c=chr, n=size, period=3, cachedDataType=uint);
    testRepeatsCase(c=0, size, period=3, cachedDataType=nothing);
    testRepeatsCase(c=0, n=size, period=3, cachedDataType=uint);

    testRepeatsCase(c=chr, n=size, period=7, cachedDataType=nothing);
    testRepeatsCase(c=chr, n=size, period=7, cachedDataType=uint);
    testRepeatsCase(c=0, n=size, period=7, cachedDataType=nothing);
    testRepeatsCase(c=0, n=size, period=7, cachedDataType=uint);

    testRepeatsCase(c=chr, n=size, period=13, cachedDataType=nothing);
    testRepeatsCase(c=chr, n=size, period=13, cachedDataType=uint);
    testRepeatsCase(c=0, n=size, period=13, cachedDataType=nothing);
    testRepeatsCase(c=0, n=size, period=13, cachedDataType=uint);

    testRepeatsCase(c=chr, n=size, period=21, cachedDataType=nothing);
    testRepeatsCase(c=chr, n=size, period=21, cachedDataType=uint);
    testRepeatsCase(c=0, n=size, period=21, cachedDataType=nothing);
    testRepeatsCase(c=0, n=size, period=21, cachedDataType=uint);

    testRepeatsCase(c=chr, n=size, period=133, cachedDataType=nothing);
    testRepeatsCase(c=chr, n=size, period=133, cachedDataType=uint);
    testRepeatsCase(c=0, n=size, period=133, cachedDataType=nothing);
    testRepeatsCase(c=0, n=size, period=133, cachedDataType=uint);
  }
}

/* Test sequences consisting of descending characters.
   The pattern
   max, max-1, max-2, ..., 0
   is repeated enough times to create the input.

   max must be at most 256.
 */
proc testDescendingCase(max: int, repeats: int, in n: int,
                        param period, type cachedDataType) {
  writeln("testDescendingCase(",
          "max=", max, ", repeats=", repeats, ", n=", n, ", ",
          "period=", period, ", cachedDataType=", cachedDataType:string, ")");

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

  type offsetType = int; // always int for this test

  const cfg = new ssortConfig(idxType=inputArr.idxType,
                              characterType=inputArr.eltType,
                              offsetType=offsetType,
                              cachedDataType=cachedDataType,
                              loadWordType=
                                (if cachedDataType != nothing
                                 then cachedDataType
                                 else uint),
                              cover=new differenceCover(period),
                              locales=Locales);
  const SA = ssortDcx(cfg, inputArr, n:offsetType);

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
    testDescendingCase(max, repeats, n, period=3, cachedDataType=nothing);
    testDescendingCase(max, repeats, n, period=3, cachedDataType=uint);

    testDescendingCase(max, repeats, n, period=7, cachedDataType=nothing);
    testDescendingCase(max, repeats, n, period=7, cachedDataType=uint);

    testDescendingCase(max, repeats, n, period=13, cachedDataType=nothing);
    testDescendingCase(max, repeats, n, period=13, cachedDataType=uint);

    testDescendingCase(max, repeats, n, period=21, cachedDataType=nothing);
    testDescendingCase(max, repeats, n, period=21, cachedDataType=uint);

    testDescendingCase(max, repeats, n, period=133, cachedDataType=nothing);
    testDescendingCase(max, repeats, n, period=133, cachedDataType=uint);
  }
}


proc runTests() {
  testHelpers();
  testComparisons();
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
