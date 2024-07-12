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
config param EXTRA_CHECKS = false;
config param TRACE = false;

// how much padding does the algorithm need at the end of the input?
param INPUT_PADDING = 8;

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
proc loadWord(const text, const n: integral, i: integral, type wordType) {
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
  Construct an array of suffixes (not yet sorted)
  for all of the offsets in 0..<n.
 */
proc buildAllOffsets(const text, const n: int,
                     type offsetType, type cachedDataType) {
  const Dom = {0..<n}; 
  var SA:[Dom] offsetAndCached(offsetType, cachedDataType) =
    forall i in Dom do
      new offsetAndCached(offsetType=offsetType,
                          cacheType=cachedDataType,
                          offset=i:offsetType,
                          cached=loadWord(text, n, i, cachedDataType));
  return SA;
}

/**
  Construct an array of suffixes (not yet sorted)
  for only those offsets in 0..<n that are also in the difference cover.
 */
proc buildSampleOffsets(const text, const n: int,
                        const sampleN: int, const charsPerMod: int,
                        type offsetType, type dataType)
{
  const Dom = {0..<sampleN}; 
  var SA:[Dom] offsetAndCached(offsetType, dataType) =
    // TODO TODO TODO
    forall i in Dom do
      new offsetAndCached(i, loadWord(text, n, i, dataType));
  return SA;
}


/**
  Create a suffix array for the suffixes 0..<n for 'text'
  by sorting the data at those suffixes directly.

  This is useful as a base case, but shouldn't be used generally
  for creating a suffix array as it is O(n**2).

  Return an array representing this suffix array.
  */
proc sortSuffixesDirectly(const text, const n:int,
                          type characterType, type offsetType,
                          type cachedDataType) {
  // First, construct the offsetAndCached array that will be sorted.
  var SA = buildAllOffsets(text, n, offsetType, cachedDataType);

  type wordType = if cachedDataType != nothing
                  then cachedDataType
                  else characterType;

  type idxType = text.idxType;

  // Now, sort the offsetAndCached
  record myPrefixComparator {
    proc keyPart(a: offsetAndCached(?), i: int):(int(8), wordType) {
      param sectionReturned = 0:int(8);
      param sectionEnd = -1:int(8);
      if a.cacheType != nothing && i == 0 {
        // return the cached data
        return (sectionReturned, a.cached);
      }

      param eltsPerWord = numBytes(wordType) / numBytes(characterType);
      const iOff = i:offsetType;
      const startIdx = a.offset + iOff*eltsPerWord;
      const startIdxCast = startIdx: idxType;
      if startIdxCast < n:idxType {
        // return further data by loading from the text array
        return (sectionReturned, loadWord(text, n, startIdxCast, wordType));
      }

      // otherwise, return that we reached the end
      return (sectionEnd, 0:wordType);
    }
  }

  sort(SA, new myPrefixComparator());

  return SA;
}

/** Create and return a sorted suffix array for the suffixes 0..<n
    referring to 'text'. */
proc ssortDcx(const text,
              const n: int,
              const cover: differenceCover(?),
              type characterType,
              type offsetType) {
  // figure out how big the sample will be, including a 0 after each mod
  const charsPerMod = 1 + divCiel(n, cover.period);
  const sampleN = cover.sampleSize * charsPerMod;

  // base case: just sort them if the number of characters is
  // less than the cover period, or sample.n is not less than n
  if n <= cover.period || sampleN >= n {
    if TRACE {
      writeln("Base case suffix sort for n=", n);
    }
    return sortSuffixesByPrefix(text, n, characterType, offsetType);
  }
}

proc computeSuffixArray(input: [], const n: input.domain.idxType) {
  if !(input.domain.rank == 1 &&
       input.domain.low == 0 &&
       input.domain.low == input.domain.first &&
       input.domain.high == input.domain.last &&
       input.domain.high <= n) {
    halt("computeSuffixArray requires 1-d array over 0..n");
  }
  if n + 8 > input.domain.high {
    halt("computeSuffixArray needs extra space at the end of the array");
    // expect it to be zero-padded past n.
  }

  type offsetType = input.domain.idxType;
  type characterType = input.eltType;

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

  {
    writeln("  uint(8) offsets, caching nothing");
    const SA = sortSuffixesDirectly(inputArr, n, inputArr.eltType,
                                    offsetType=uint(8), cachedDataType=nothing);
    checkOffsets(SA, expectOffsets);
    assert(SA.eltType.cacheType == nothing);
  }
  {
    writeln("  uint(8) offsets, caching uint(8)");
    const A = buildAllOffsets(inputArr, n,
                              offsetType=uint(8), cachedDataType=uint(8));
    checkCached(A, expectCached1);
    const SA = sortSuffixesDirectly(inputArr, n, inputArr.eltType,
                                    offsetType=uint(8), cachedDataType=uint(8));
    checkCached(SA, expectCached1);
    checkOffsets(SA, expectOffsets);
  }
  {
    writeln("  uint(8) offsets, caching uint(16)");
    const A = buildAllOffsets(inputArr, n,
                              offsetType=uint(8), cachedDataType=uint(16));
    checkCached(A, expectCached2);
    const SA = sortSuffixesDirectly(inputArr, n, inputArr.eltType,
                                    offsetType=uint(8),
                                    cachedDataType=uint(16));
    checkCached(SA, expectCached2);
    checkOffsets(SA, expectOffsets);
  }
  {
    writeln("  uint(8) offsets, caching uint(32)");
    const A = buildAllOffsets(inputArr, n,
                              offsetType=uint(8), cachedDataType=uint(32));
    checkCached(A, expectCached4);
    const SA = sortSuffixesDirectly(inputArr, n, inputArr.eltType,
                                    uint(8), uint(32));
    checkCached(SA, expectCached4);
    checkOffsets(SA, expectOffsets);
  }
  {
    writeln("  uint(8) offsets, caching uint(64)");
    const A = buildAllOffsets(inputArr, n,
                              offsetType=uint(8), cachedDataType=uint(64));
    checkCached(A, expectCached8);
    const SA = sortSuffixesDirectly(inputArr, n, inputArr.eltType,
                                    uint(8), uint(64));
    checkCached(SA, expectCached8);
    checkOffsets(SA, expectOffsets);
  }

  // check some different offset types
  {
    writeln("  uint(32) offsets, caching nothing");
    const SA = sortSuffixesDirectly(inputArr, n, inputArr.eltType,
                                    offsetType=uint(32),
                                    cachedDataType=nothing);
    checkOffsets(SA, expectOffsets);
  }
  {
    writeln("  int offsets, caching nothing");
    const SA = sortSuffixesDirectly(inputArr, n, inputArr.eltType,
                                    offsetType=int,
                                    cachedDataType=nothing);
    checkOffsets(SA, expectOffsets);
  }
  {
    writeln("  uint offsets, caching nothing");
    const SA = sortSuffixesDirectly(inputArr, n, inputArr.eltType,
                                    offsetType=uint,
                                    cachedDataType=nothing);
    checkOffsets(SA, expectOffsets);
  }
}

proc main() {
  DifferenceCovers.testCovers();

  testSeeresses();
}


}
