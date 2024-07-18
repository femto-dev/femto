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
      halt("at i=", i, " got offset ", gotOffset, " but expected ", e);
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

private proc checkSeeressesCase(type offsetType, type cachedDataType,
                                inputArr, n:int, param period,
                                expectOffsets, expectCached:?t = none) {
  if TRACE {
    writeln("  ", offsetType:string, " offsets, caching ", cachedDataType:string);
  }

  if expectCached.type != nothing {
    const A = buildAllOffsets(offsetType=offsetType,
                              cachedDataType=cachedDataType,
                              inputArr, n:offsetType);
    checkCached(A, expectCached);
  }
  const SA = computeSuffixArrayDirectly(inputArr, n:offsetType,
                                        inputArr.eltType,
                                        offsetType=offsetType,
                                        cachedDataType=cachedDataType);
  checkOffsets(SA, expectOffsets);
  assert(SA.eltType.cacheType == cachedDataType);

  if expectCached.type != nothing {
    checkCached(SA, expectCached);
  }

  // try ssortDcx
  const cfg = new ssortConfig(idxType=inputArr.idxType,
                              characterType=inputArr.eltType,
                              offsetType=offsetType,
                              cachedDataType=cachedDataType,
                              cover=new differenceCover(period));
  const SA2 = ssortDcx(cfg, inputArr, n:offsetType);
  checkOffsets(SA2, expectOffsets);
  assert(SA2.eltType.cacheType == cachedDataType);

  if expectCached.type != nothing {
    checkCached(SA2, expectCached);
  }
}

private proc testMyDivCeil() {
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
}

private proc testPrefixComparisons(type cachedDataType) {
  const cover = new differenceCover(3);
  const cfg = new ssortConfig(idxType=int,
                              characterType=uint(8),
                              offsetType=int,
                              cachedDataType=cachedDataType,
                              cover=cover);
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

  const prefixAA =  makeOffsetAndCached(cfg.offsetType, cfg.cachedDataType,
                                        0, text, n);
  const prefixAA2 = makeOffsetAndCached(cfg.offsetType, cfg.cachedDataType,
                                        6, text, n);
  const prefixAA3 = makeOffsetAndCached(cfg.offsetType, cfg.cachedDataType,
                                        18, text, n);
  const prefixBB =  makeOffsetAndCached(cfg.offsetType, cfg.cachedDataType,
                                        2, text, n);

  const prefixAAp = makePrefix(0, text, n, cfg.cover, uint);
  const prefixAA2p = makePrefix(6, text, n, cfg.cover, uint);
  const prefixAA3p = makePrefix(18, text, n, cfg.cover, uint);
  const prefixBBp = makePrefix(2, text, n, cfg.cover, uint);

  const prefixAAs = makePrefixAndSampleRanks(0,
                                             text, n,
                                             0, ranks, ranksN,
                                             charsPerMod=charsPerMod,
                                             cfg.cover, uint);
  const prefixAA2s = makePrefixAndSampleRanks(6,
                                              text, n,
                                              0, ranks, ranksN,
                                              charsPerMod=charsPerMod,
                                              cfg.cover, uint);
   const prefixAA3s = makePrefixAndSampleRanks(18,
                                              text, n,
                                              0, ranks, ranksN,
                                              charsPerMod=charsPerMod,
                                              cfg.cover, uint);

  const prefixBBs = makePrefixAndSampleRanks(2,
                                             text, n,
                                             0, ranks, ranksN,
                                             charsPerMod=charsPerMod,
                                             cfg.cover, uint);

  assert(comparePrefixes(0, 0, cfg, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(0, 2, cfg, text, n, maxPrefix=2)<0);

  assert(comparePrefixes(prefixAA, prefixAA, cfg, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(prefixAA, prefixAA3, cfg, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(prefixAA, prefixAA2, cfg, text, n, maxPrefix=2)<=0);
  assert(comparePrefixes(prefixAA, prefixBB, cfg, text, n, maxPrefix=2)<0);
  assert(comparePrefixes(prefixBB, prefixAA, cfg, text, n, maxPrefix=2)>0);

  assert(comparePrefixes(prefixAAp, prefixAAp, cfg, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(prefixAAp, prefixBBp, cfg, text, n, maxPrefix=2)<0);
  assert(comparePrefixes(prefixBBp, prefixAAp, cfg, text, n, maxPrefix=2)>0);

  assert(comparePrefixes(prefixAA, prefixAAp, cfg, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(prefixAA, prefixBBp, cfg, text, n, maxPrefix=2)<0);
  assert(comparePrefixes(prefixAAp, prefixBB, cfg, text, n, maxPrefix=2)<0);
  assert(comparePrefixes(prefixBBp, prefixAA, cfg, text, n, maxPrefix=2)>0);
  assert(comparePrefixes(prefixBB, prefixAAp, cfg, text, n, maxPrefix=2)>0);

  assert(comparePrefixes(prefixAAp, prefixAAs, cfg, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(prefixAAs, prefixAAp, cfg, text, n, maxPrefix=2)==0);
  assert(comparePrefixes(prefixAAs, prefixBBs, cfg, text, n, maxPrefix=2)<0);
  assert(comparePrefixes(prefixAAs, prefixBBp, cfg, text, n, maxPrefix=2)<0);
  assert(comparePrefixes(prefixAAp, prefixBBs, cfg, text, n, maxPrefix=2)<0);

  assert(comparePrefixes(prefixBBs, prefixAAs, cfg, text, n, maxPrefix=2)>0);
  assert(comparePrefixes(prefixBBs, prefixAAp, cfg, text, n, maxPrefix=2)>0);
  assert(comparePrefixes(prefixBBp, prefixAAs, cfg, text, n, maxPrefix=2)>0);

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

proc testRankComparisons() {
  const cover = new differenceCover(3);
  const cfg = new ssortConfig(idxType=int,
                              characterType=uint(8),
                              offsetType=int,
                              cachedDataType=nothing,
                              cover=cover);

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
  const p1 = makePrefixAndSampleRanks(offset=1, Text, n,
                                      sampleOffset=7, Ranks, nSample,
                                      charsPerMod=charsPerMod,
                                      cover, uint);
  const p3 = makePrefixAndSampleRanks(offset=3, Text, n,
                                      sampleOffset=1, Ranks, nSample,
                                      charsPerMod=charsPerMod,
                                      cover, uint);
  const p19 = makePrefixAndSampleRanks(offset=19, Text, n,
                                      sampleOffset=13, Ranks, nSample,
                                      charsPerMod=charsPerMod,
                                      cover, uint);

  assert(p1.ranks[0] == 13); // offset 1 -> sample offset 7 -> rank 13
  assert(p1.ranks[1] == 10); // offset 3 -> sample offset 1 -> rank 10

  assert(p3.ranks[0] == 10); // offset 3 -> sample offset 1 -> rank 10
  assert(p3.ranks[1] == 9);  // offset 4 -> sample offset 8 -> rank 9

  writeln(p19);
  assert(p19.ranks[0] == 1); // offset 19 -> sample offset 13 -> rank 1
  assert(p19.ranks[1] == 0); // offset 21 -> sample offset -  -> rank 0

  // check the rest of the cases
  for sampleOffset in 0..<nSample {
    const offset = subproblemOffsetToOffset(sampleOffset, cover, charsPerMod);
    const p = makePrefixAndSampleRanks(offset=offset, Text, n,
                                       sampleOffset=sampleOffset,
                                       Ranks, nSample,
                                       charsPerMod=charsPerMod,
                                       cover, uint);
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
  const o1 = makeOffsetAndCached(cfg.offsetType,cfg.cachedDataType, 1, Text, n);
  const o3 = makeOffsetAndCached(cfg.offsetType,cfg.cachedDataType, 3, Text, n);
  const o5 = makeOffsetAndCached(cfg.offsetType,cfg.cachedDataType, 5, Text, n);
  const o19= makeOffsetAndCached(cfg.offsetType,cfg.cachedDataType,19, Text, n);

  // test self-compares
  assert(compareSampleRanks(o1, o1, Ranks, charsPerMod, cover) == 0);
  assert(compareSampleRanks(o3, o3, Ranks, charsPerMod, cover) == 0);
  assert(compareSampleRanks(o5, o5, Ranks, charsPerMod, cover) == 0);
  assert(compareSampleRanks(o19, o19, Ranks, charsPerMod, cover) == 0);

  assert(compareSampleRanks(p1, o1, Ranks, charsPerMod, cover) == 0);
  assert(compareSampleRanks(p3, o3, Ranks, charsPerMod, cover) == 0);
  assert(compareSampleRanks(p19, o19, Ranks, charsPerMod, cover) == 0);

  // test 1 vs 3 : 1 has rank 13 and 3 has rank 10
  assert(compareSampleRanks(o1, o3, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(p1, o3, Ranks, charsPerMod, cover) > 0);

  assert(compareSampleRanks(o3, o1, Ranks, charsPerMod, cover) < 0);
  assert(compareSampleRanks(p3, o1, Ranks, charsPerMod, cover) < 0);

  // test 3 vs 5 : use k=1, 3->4 has rank 9 ; 5->6 has rank 6
  assert(compareSampleRanks(o3, o5, Ranks, charsPerMod, cover) > 0);
  assert(compareSampleRanks(p3, o5, Ranks, charsPerMod, cover) > 0);

  assert(compareSampleRanks(o5, o3, Ranks, charsPerMod, cover) < 0);

  // test 5 vs 19 : use k=2, 5->7 has rank 5 ; 19->21 has rank 0
  assert(compareSampleRanks(o5, o19, Ranks, charsPerMod, cover) > 0);

  assert(compareSampleRanks(o19, o5, Ranks, charsPerMod, cover) < 0);
  assert(compareSampleRanks(p19, o5, Ranks, charsPerMod, cover) < 0);
}

private proc testComparisons() {
  testPrefixComparisons(nothing);
  testPrefixComparisons(uint);

  testRankComparisons();
}


// test suffix sorting stuff with "seeresses" as input.
private proc testSeeresses() {
  if TRACE {
    writeln("Testing suffix sorting of 'seeresses'");
  }

  const input = "seeresses";
  const n = input.size;
  const inputArr = bytesToArray(input);

  /*

    seeresses
    012345678

    here is the suffix array:

    eeresses  1
    eresses   2
    es        7
    esses     4
    resses    3
    s         8
    seeresses 0
    ses       6
    sses      5

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
                     inputArr, n, 3, expectOffsets);
  checkSeeressesCase(offsetType=int, cachedDataType=uint(8),
                     inputArr, n, 7, expectOffsets, expectCached1);
  checkSeeressesCase(offsetType=int, cachedDataType=uint(16),
                     inputArr, n, 3, expectOffsets, expectCached2);
  checkSeeressesCase(offsetType=int, cachedDataType=uint(32),
                     inputArr, n, 13, expectOffsets, expectCached4);
  checkSeeressesCase(offsetType=int, cachedDataType=uint(64),
                     inputArr, n, 3, expectOffsets, expectCached8);

  // check some different offset types
  // TODO: fix Chapel module errors with these other types
  //checkSeeressesCase(offsetType=uint(32), cachedDataType=nothing,
  //                   inputArr, n, 3, expectOffsets);
  checkSeeressesCase(offsetType=int, cachedDataType=nothing,
                     inputArr, n, 3, expectOffsets);
  //checkSeeressesCase(offsetType=uint, cachedDataType=nothing,
  //                   inputArr, n, 3, expectOffsets);
}

proc testOtherCase(input: string, expectSA: [] int,
                   param period, type cachedDataType) {
  if TRACE {
    writeln("testOtherCase(input='", input, "', period=", period, ", ",
                             "cachedDataType=", cachedDataType:string, ")");
  }

  const n = input.size;
  const inputArr = bytesToArray(input);

  type offsetType = int; // always int for this test

  const cfg = new ssortConfig(idxType=inputArr.idxType,
                              characterType=inputArr.eltType,
                              offsetType=offsetType,
                              cachedDataType=cachedDataType,
                              cover=new differenceCover(period));
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

  */
  testOther("abracadabra", [10,7,0,3,5,8,1,4,6,9,2]);
  testOther("mississippi", [10,7,4,1,0,9,8,6,3,5,2]);
  testOther("aaaacaaaacaaaab", [10,5,0,11,6,1,12,7,2,13,8,3,14,9,4]);
}

proc testRepeatsCase(c: uint(8), n: int, param period, type cachedDataType) {
  if TRACE {
    writeln("testRepeatsCase(c=", c, ", n=", n, ", period=", period, ", ",
                             "cachedDataType=", cachedDataType:string, ")");
  }

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
                              cover=new differenceCover(period));
  const SA = ssortDcx(cfg, inputArr, n:offsetType);

  if TRACE && n <= 50 {
    writeln("Input     ", inputArr[0..<n]);
    writeln("Expect SA ", expectSA);
    writeln("Got SA    ", SA);
  }
  checkOffsets(SA, expectSA);
}

proc testRepeats() {
  const sizes = [0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 1000, 10000];

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
  if TRACE {
    writeln("testDescendingCase(",
            "max=", max, ", repeats=", repeats, ", n=", n, ", ",
            "period=", period, ", cachedDataType=", cachedDataType:string, ")");
  }

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
                              cover=new differenceCover(period));
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


proc main() {
  testRepeats();

  /*
  testMyDivCeil();
  testComparisons();
  testSeeresses();
  testOthers();
  testRepeats();
  testDescending();
  */

  writeln("TestSuffixSort OK");
}


}
