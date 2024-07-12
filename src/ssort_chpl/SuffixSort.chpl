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

  femto/src/ssort_chpl/SuffixSort.chpl
*/

module SuffixSort {


use DifferenceCovers;
use Math;
use IO;
use Sort;

config param DEFAULT_PERIOD = 133;
config param ENABLE_CACHED_TEXT = true;
config param EXTRA_CHECKS = false;
config param TRACE = false;

// how much padding does the algorithm need at the end of the input?
param INPUT_PADDING = 8;

/**
 This record contains the configuration for the suffix sorting
 problem or subproblem. It's just a record to bundle up the generic
 information.

 It doesn't contain the 'text' input array because Chapel currently
 doesn't support reference fields.
 */
record ssortConfig {
  // these should all be integral types:

  type idxType;        // for accessing 'text'; should be text.domain.idxType
  type characterType;  // text.domain.eltType

  type offsetType;     // type for storing offsets
  type cachedDataType; // cache this much text data along with offsets
                       // (no caching if this is 'nothing')
  proc wordType type {
    return if cachedDataType != nothing
           then cachedDataType
           else characterType;
  }

  const cover: differenceCover(?);
  const n: offsetType;
}

/**
  This record helps to avoid indirect access at the expense of using
  more memory. Here we store together an offset for the suffix array
  along with some of the data that is present at that offset.
  */
record offsetAndCached {
  type offsetType;
  type cacheType;

  var offset: offsetType;
  var cached: cacheType;
}

/**
  Read a "word" of data from 'text' index 'i'.
  Assumes that there are 8 bytes of padding past the real data.
  */
inline proc loadWord(const text, n: integral, i: integral, type wordType) {
  // handle some simple cases first
  if wordType == nothing then return none;
  if wordType == text.eltType then return text[i];

  param wordBytes = numBytes(wordType);
  param textCharBytes = numBytes(text.eltType);
  param textCharBits = textCharBytes*8;
  param numToRead = wordBytes / textCharBytes;
  if wordBytes < textCharBytes || !isUintType(wordType) {
    compilerError("invalid loadWord call");
  }

  if EXTRA_CHECKS {
    assert(0 <= i && i:uint < n:uint);
  }

  // I expect this loop to be folded away by the backend compiler &
  // turn into a bswap instruction.
  var ret: wordType = 0;
  for j in 0..<numToRead {
    ret <<= textCharBits;
    ret |= text[i+j];
  }

  return ret;
}

/**
 Construct an offsetAndCached for offset 'i' in the input.
 */
proc makeOffsetAndCached(type offsetType, type cachedDataType,
                         i: offsetType,
                         const text, n: offsetType) {
  return new offsetAndCached(offsetType=offsetType,
                             cacheType=cachedDataType,
                             offset=i,
                             cached=loadWord(text, n, i, cachedDataType));
}

/**
  Construct an array of suffixes (not yet sorted)
  for all of the offsets in 0..<n.
 */
proc buildAllOffsets(type offsetType, type cachedDataType,
                     const text, n: offsetType) {
  const Dom = {0..<n};
  var SA:[Dom] offsetAndCached(offsetType, cachedDataType) =
    forall i in Dom do
      makeOffsetAndCached(offsetType=offsetType, cachedDataType=cachedDataType,
                          i, text, n);
  return SA;
}

// can be called from keyPart(offsetAndCached, i)
inline proc getKeyPart(const a: offsetAndCached(?), i: integral,
                       const cfg:ssortConfig(?), const text,
                       n: cfg.offsetType,
                       maxPrefix: cfg.offsetType) {
  type idxType = cfg.idxType;
  type characterType = cfg.characterType;
  type offsetType = cfg.offsetType;
  type cachedDataType = cfg.cachedDataType;
  type wordType = if cachedDataType != nothing
                  then cachedDataType
                  else characterType;

  param sectionReturned = 0:int(8);
  param sectionEnd = -1:int(8);
  if a.cacheType != nothing && i == 0 {
    // return the cached data
    return (sectionReturned, a.cached);
  }

  param eltsPerWord = numBytes(wordType) / numBytes(characterType);
  const iOff = i:offsetType;
  const nCharsIn:offsetType = iOff*eltsPerWord;
  const startIdx:offsetType = a.offset + nCharsIn;
  const startIdxCast = startIdx: idxType;
  if nCharsIn < maxPrefix && startIdxCast < n:idxType {
    // return further data by loading from the text array
    return (sectionReturned, loadWord(text, n, startIdxCast, wordType));
  }

  // otherwise, return that we reached the end
  return (sectionEnd, 0:wordType);
}

proc comparePrefixes(const a: offsetAndCached(?), const b: offsetAndCached(?),
                     const cfg:ssortConfig(?), const text, n: cfg.offsetType,
                     maxPrefix: cfg.offsetType): int {
  var curPart = 0;
  while true {
    var (aSection, aPart) = getKeyPart(a, curPart, cfg, text, n, maxPrefix);
    var (bSection, bPart) = getKeyPart(b, curPart, cfg, text, n, maxPrefix);
    if aSection != 0 || bSection != 0 {
      return aSection - bSection;
    }
    if aPart < bPart {
      return -1;
    }
    if aPart > bPart {
      return 1;
    }

    curPart += 1;
  }

  // This is never reached, but the return below keeps the compiler happy.
  return 1;
}

/* This is helpful for computing ranks based on first v characters. */
private
proc prefixDiffersFromPrevious(i: int,
                               const Sample: [] offsetAndCached(?),
                               const cfg:ssortConfig(?), const text,
                               n: cfg.offsetType,
                               maxPrefix: cfg.offsetType): cfg.offsetType {
  type offsetType = cfg.offsetType;

  // handle base case, where i-1 does not exist
  if i == 0 {
    return 1:offsetType; // assign a new rank
  }

  // otherwise, compare this element and the previous
  const cmp = comparePrefixes(Sample[i], Sample[i-1],
                              cfg, text, n=n, maxPrefix=maxPrefix);
  if cmp == 0 {
    return 0:offsetType; // same prefix, so don't assign a new rank
  }

  return 1:offsetType; // not equal, so assign a new rank
}

/** Sort suffixes that we have already computed in A
    by the first maxPrefix character values.
 */
proc sortSuffixes(const cfg:ssortConfig(?), const thetext,
                  ref A: [] offsetAndCached(?),
                  maxPrefix: A.eltType.offsetType) {
  type idxType = cfg.idxType;
  type characterType = cfg.characterType;
  type offsetType = cfg.offsetType;
  type cachedDataType = cfg.cachedDataType;
  type wordType = cfg.wordType;
  const n = cfg.n;
  // Define a comparator to support radix sorting by the first maxPrefix
  // character values.
  record myPrefixComparator {
    proc keyPart(a: offsetAndCached(?), i: int):(int(8), wordType) {
      return getKeyPart(a, i=i, cfg, thetext, n=n, maxPrefix=maxPrefix);
    }
  }

  sort(A, new myPrefixComparator());
}

/**
  Create a suffix array for the suffixes 0..<n for 'text'
  by sorting the data at those suffixes directly.

  This is useful as a base case, but shouldn't be used generally
  for creating a suffix array as it is O(n**2).

  Return an array representing this suffix array.
  */
proc computeSuffixArrayDirectly(const text, n:integral,
                                type characterType, type offsetType,
                                type cachedDataType) {
  const useN = n:offsetType;
  const cfg = new ssortConfig(idxType = text.idxType,
                              characterType = characterType,
                              offsetType = offsetType,
                              cachedDataType = cachedDataType,
                              cover = new differenceCover(DEFAULT_PERIOD),
                              n=n:offsetType);

  // First, construct the offsetAndCached array that will be sorted.
  var A = buildAllOffsets(offsetType=offsetType, cachedDataType=cachedDataType,
                          text, useN);

  sortSuffixes(cfg, text, A, maxPrefix=max(offsetType));

  return A;
}

proc makeSampleOffset(type offsetType, type cachedDataType,
                      i: offsetType,
                      const text, n: offsetType,
                      cover: differenceCover(?)) {
  // i is a packed index into the offsets to sample
  // we have to unpack it to get the regular offset
  const whichPeriod = i / cover.sampleSize;
  const periodStart = whichPeriod * cover.sampleSize;
  const coverIdx = i - periodStart;
  const unpackedIdx = whichPeriod * cover.period + cover.cover[coverIdx];
  return makeOffsetAndCached(offsetType=offsetType,
                             cachedDataType=cachedDataType,
                             unpackedIdx, text, n);
}

/**
  Construct an array of suffixes (not yet sorted)
  for only those offsets in 0..<n that are also in the difference cover.
 */
proc buildSampleOffsets(type offsetType, type cachedDataType,
                        const text, n: offsetType,
                        const cover: differenceCover(?)) {
  const nPeriods = divCiel(n, cover.period); // nPeriods * period >= n
  const sampleN = cover.sampleSize * nPeriods;
  const Dom = {0..<sampleN};
  var SA:[Dom] offsetAndCached(offsetType, cachedDataType) =
    forall i in Dom do
      makeSampleOffset(offsetType=offsetType, cachedDataType=cachedDataType,
                       i=i, text, n=n, cover);

  return SA;
}


/** Create and return a sorted suffix array for the suffixes 0..<n
    referring to 'text'. */
proc ssortDcx(const cfg:ssortConfig(?), const text) {

  // figure out how big the sample will be, including a 0 after each mod
  type offsetType = cfg.offsetType;
  const n = cfg.n;
  const cover = cfg.cover;
  const charsPerMod = 1 + divCiel(n, cover.period);
  const sampleN = cover.sampleSize * charsPerMod;

  if n + INPUT_PADDING > text.domain.high {
    halt("sortDcx needs extra space at the end of the array");
    // expect it to be zero-padded past n.
  }

  //// Base Case ////

  // base case: just sort them if the number of characters is
  // less than the cover period, or sample.n is not less than n
  if n <= cover.period || sampleN >= n {
    if TRACE {
      writeln("Base case suffix sort for n=", n);
    }
    return computeSuffixArrayDirectly(text, n,
                                      characterType=cfg.characterType,
                                      offsetType=offsetType,
                                      cachedDataType=cfg.cachedDataType,
                                      maxPrefix=max(offsetType));
  }

  //// Step 1: Sort Sample Suffixes ////

  // begin by computing the input text for the recursive subproblem
  var SampleText:[0..<sampleN+INPUT_PADDING] offsetType;
  var allSamplesHaveUniqueRanks = false;
  {
    // create an array storing offsets at sample positions
    var Sample = buildSampleOffsets(offsetType=offsetType,
                                    cachedDataType=cfg.cachedDataType,
                                    text, n, cover);
    // then sort the these by the first cover.period characters
    sortSuffixes(cfg, text, Sample, n=n, maxPrefix=cover.period);

    // now, compute the rank of each of these. we need to compare
    // the first cover.period characters & assign different ranks when these
    // differ.
    const Ranks = + scan
                    [i in Sample.domain]
                      prefixDiffersFromPrevious(i,
                                                Sample, cfg,
                                                maxPrefix=cover.period);

    // TODO: CHECK FOR OFF-BY-ONE HERE
    allSamplesHaveUniqueRanks = Ranks.last == Sample.size;

    // create the input for the recursive subproblem from the offsets and ranks
    SampleText = 0; // PERF TODO: noinit it
                    // and write a loop to zero what is not initalized below

    forall (offset, rank) in zip(Sample, Ranks) {
      // offset is an unpacked offset, but we need to pack it.
      // do so in a way that arranges for SampleText to consist of
      // all sample inputs at a particular modulus, followed by other modulus
      const whichPeriod = offset.offset / cover.period;
      const periodStart = whichPeriod * cover.period;
      const phase = offset.offset - periodStart;
      const coverIdx = cover.coverIndex(phase);
      const useIdx = coverIdx * charsPerMod;

      // this is not a data race because Sample.offsets are a permutation
      // of the offsets.
      SampleText[useIdx] = rank;
    }
  }

  // TODO: no need to recurse if all offsets had unique Ranks
  // i.e. each character in SampleText occurs only once
  // i.e. each character in SampleText is the rank

  {
    // recursively sort the sample suffixes
    const subCfg = new ssortConfig(idxType=SampleText.idxType,
                                   characterType=SampleText.eltType,
                                   offsetType=offsetType,
                                   cachedDataType=cfg.cachedDataType,
                                   cover=cover,
                                   n=sampleN);
    const SubSA = ssortDcx(subCfg, SampleText);
    // Replace the values in SampleText with their ranks from the
    // recursive suffix array.
    forall (offset,rank) in zip(SubSA, SubSA.domain) {
      SampleText[offset] = rank;
    }
  }

  //// Step 2: Sort everything all together ////

  // TODO: investigate radix sort + comparison on ties
  // TODO: investigate parallel multi-way merge

  var SA = buildAllOffsets(text, cfg.n, offsetType, cfg.cachedDataType);

  record myComparator {
    inline proc offsetToSampleOffset(i: offsetType) {
      const mod = i % cover.period;
      const whichPeriod = i / cover.period;
      const sampleIdx = cover.coverIndex(mod);
      return charsPerMod * sampleIdx + whichPeriod;
    }
    proc compare(a: offsetAndCached(?), b: offsetAndCached(?)) {
      // first, compare the first cover.period characters of text
      const prefixCmp = comparePrefixes(a, b, cfg, text, cover.period);
      if prefixCmp != 0 {
        return prefixCmp;
      }
      // if the prefixes are the same, compare the nearby sample
      // rank from the recursive subproblem.
      const k = findInCover(a.offset % cover.period, b.offset % cover.period);
      const aSampleOffset = offsetToSampleOffset(a.offset + k);
      const bSampleOffset = offsetToSampleOffset(b.offset + k);
      const rankA = SampleText[aSampleOffset];
      const rankB = SampleText[bSampleOffset];
    }
  }
  Sort.TwoArraySampleSort.twoArraySampleSort(SA, new myComparator());

  return SA;
}

proc computeSuffixArray(input: [], const n: input.domain.idxType) {
  if !(input.domain.rank == 1 &&
       input.domain.low == 0 &&
       input.domain.low == input.domain.first &&
       input.domain.high == input.domain.last &&
       input.domain.high <= n) {
    halt("computeSuffixArray requires 1-d array over 0..n");
  }
  if n + INPUT_PADDING > input.domain.high {
    halt("computeSuffixArray needs extra space at the end of the array");
    // expect it to be zero-padded past n.
  }

  type offsetType = input.domain.idxType;
  type characterType = input.eltType;
  type cachedDataType = if ENABLE_CACHED_TEXT then uint(64) else nothing;

  return ssortDcx(input, n,
                  cover=new differenceCover(DEFAULT_PERIOD),
                  characterType=characterType, offsetType=offsetType);
}


/////////// Begin Testing Code ////////////

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
                                inputArr, n:int,
                                expectOffsets, expectCached:?t = none) {
  writeln("  ", offsetType:string, " offsets, caching ", cachedDataType:string);

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
  checkSeeressesCase(offsetType=uint(8), cachedDataType=nothing,
                     inputArr, n, expectOffsets);
  checkSeeressesCase(offsetType=uint(8), cachedDataType=uint(8),
                     inputArr, n, expectOffsets, expectCached1);
  checkSeeressesCase(offsetType=uint(8), cachedDataType=uint(16),
                     inputArr, n, expectOffsets, expectCached2);
  checkSeeressesCase(offsetType=uint(8), cachedDataType=uint(32),
                     inputArr, n, expectOffsets, expectCached4);
  checkSeeressesCase(offsetType=uint(8), cachedDataType=uint(64),
                     inputArr, n, expectOffsets, expectCached8);

  // check some different offset types
  checkSeeressesCase(offsetType=uint(32), cachedDataType=nothing,
                     inputArr, n, expectOffsets);
  checkSeeressesCase(offsetType=int, cachedDataType=nothing,
                     inputArr, n, expectOffsets);
  checkSeeressesCase(offsetType=uint, cachedDataType=nothing,
                     inputArr, n, expectOffsets);
}

proc main() {
  DifferenceCovers.testCovers();

  testSeeresses();
}


}
