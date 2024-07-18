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

  femto/src/ssort_chpl/SuffixSortImpl.chpl
*/

module SuffixSortImpl {


use DifferenceCovers;
use Partitioning;

use Math;
use IO;
use Sort;
import Random;
import BitOps;

import SuffixSort.DEFAULT_PERIOD;
import SuffixSort.ENABLE_CACHED_TEXT;
import SuffixSort.EXTRA_CHECKS;
import SuffixSort.TRACE;
import SuffixSort.INPUT_PADDING;

// how much more should we sample to create splitters?
// 1.0 would be only to sample enough for the splitters
config const SAMPLE_RATIO = 1.5;
config const PARTITION_SORT_SAMPLE = false;
config const PARTITION_SORT_ALL = false;

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

  const cover: differenceCover(?);

  proc wordType type {
    return if cachedDataType != nothing
           then cachedDataType
           else characterType;
  }

  // this type is used in record prefix / record prefixAndSampleRanks
  proc prefixWordType type do return uint;
}

/**
  This record helps to avoid indirect access at the expense of using
  more memory. Here we store together an offset for the suffix array
  along with some of the data that is present at that offset.
  */
record offsetAndCached : writeSerializable {
  type offsetType;
  type cacheType;

  var offset: offsetType;
  var cached: cacheType;

  // this function is a debugging aid
  proc serialize(writer, ref serializer) throws {
    if cacheType == nothing {
      writer.write(offset);
    } else {
      writer.writef("%i (%016xu)", offset, cached);
    }
  }

  // I would think these are not necessary?
  // Added them to avoid a compilation error
  proc init=(const rhs: offsetAndCached(?)) {
    this.offsetType = rhs.offsetType;
    this.cacheType = rhs.cacheType;
    this.offset = rhs.offset;
    this.cached = rhs.cached;
  }
  operator =(ref lhs : offsetAndCached(?), const rhs: offsetAndCached(?)) {
    lhs.offset = rhs.offset;
    lhs.cached = rhs.cached;
  }
}

/**
  This record holds a whole record with a prefix.
  This is useful for splitters.

  It could store an offset as well but that isn't actually needed.
 */
record prefix : writeSerializable {
  type wordType;
  param nWords;
  var words: nWords*wordType;

  // this function is a debugging aid
  proc serialize(writer, ref serializer) throws {
    writer.write("(");
    for i in 0..<nWords {
      writer.writef("%016xu", words[i]);
    }
    writer.write(")");
  }
}

/**
  This record holds a prefix and the next cover period sample ranks.
  This is useful for splitters.
 */
record prefixAndSampleRanks : writeSerializable {
  type wordType;
  type offsetType;
  param nWords;
  param nRanks;

  var offset: offsetType;
  var words: nWords*wordType;
  var ranks: nRanks*offsetType;

  // this function is a debugging aid
  proc serialize(writer, ref serializer) throws {
    writer.write(offset);
    writer.write("(");
    for i in 0..<nWords {
      writer.writef("%016xu", words[i]);
    }
    writer.write("|");
    for i in 0..<nRanks {
      if i != 0 then writer.write(",");
      writer.write(ranks[i]);
    }
    writer.write(")");
  }
}

inline proc myDivCeil(x: integral, param y: integral) {
  return (x + y - 1) / y;
}
inline proc myDivCeil(param x: integral, param y: integral) param {
  return (x + y - 1) / y;
}


/**
  Read a "word" of data from 'text' character index 'i'.
  Assumes that there are 8 bytes of padding past the real data.
  */
inline proc loadWord(const text, n: integral, i: integral, type wordType) {
  //writeln("loadWord(", i, ", ", wordType:string, ")");

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

  //writef("loadWord returning %016xu\n", ret);
  return ret;
}

proc ssortConfig.getWordType(a: integral) type {
  return this.prefixWordType;
}
proc ssortConfig.getWordType(a: offsetAndCached(?)) type {
  return if a.cacheType != nothing
         then a.cacheType
         else this.characterType;
}
proc ssortConfig.getWordType(a: prefix(?)) type {
  return a.wordType;
}
proc ssortConfig.getWordType(a: prefixAndSampleRanks(?)) type {
  return a.wordType;
}

/**
 Construct an offsetAndCached for offset 'i' in the input.
 */
inline proc makeOffsetAndCached(type offsetType, type cachedDataType,
                                i: offsetType,
                                const text, n: offsetType) {
  const cached: cachedDataType;
  if cachedDataType == nothing {
    cached = none;
  } else if i < n {
    cached = loadWord(text, n, i, cachedDataType);
  } else {
    cached = 0;
  }

  return new offsetAndCached(offsetType=offsetType,
                             cacheType=cachedDataType,
                             offset=i,
                             cached=cached);
}

/**
  Construct an prefix record for offset 'offset' in the input
  by loading the relevant data from 'text'.
 */
proc makePrefix(offset: integral, const text, n: integral, cover, type wordType)
{
  type characterType = text.eltType;
  // how many words do we need in order to hold cover.period characters?
  param wordBytes = numBytes(wordType);
  param textCharBytes = numBytes(characterType);
  param charsPerWord = wordBytes / textCharBytes;
  param nWords = myDivCeil(cover.period, charsPerWord);
  if wordBytes < textCharBytes || !isUintType(wordType) {
    compilerError("invalid makeOffsetAndCached call");
  }

  var result = new prefix(wordType=wordType, nWords=nWords);
  // fill in the words
  for i in 0..<nWords {
    type idxType = text.idxType;
    param eltsPerWord = numBytes(wordType) / numBytes(characterType);
    const castOffset = offset:idxType;
    const castI = i:idxType;
    const idx = castOffset + castI*eltsPerWord;
    if idx < n {
      result.words[i] = loadWord(text, n, idx, wordType);
    } else {
      result.words[i] = 0;
    }
  }

  return result;
}

/**
  Construct an prefixAndSampleRanks record for offset 'i' in the input
  by loading the relevant data from 'text' and 'ranks'.
 */
proc makePrefixAndSampleRanks(offset: integral,
                              const text, n: integral,
                              sampleOffset: integral,
                              const Ranks, ranksN: integral,
                              charsPerMod: integral,
                              cover, type wordType) {
  // compute the type information for creating a prefix
  type prefixType = makePrefix(offset, text, n, cover, wordType).type;
  type characterType = text.eltType;

  var result = new prefixAndSampleRanks(wordType=wordType,
                                        offsetType=Ranks.eltType,
                                        nWords=prefixType.nWords,
                                        nRanks=cover.sampleSize,
                                        offset=offset);

  // fill in the words
  for i in 0..<result.nWords {
    type idxType = text.idxType;
    param eltsPerWord = numBytes(wordType) / numBytes(characterType);
    const castOffset = offset:idxType;
    const castI = i:idxType;
    const idx = castOffset + castI*eltsPerWord;
    if idx < n {
      result.words[i] = loadWord(text, n, idx, wordType);
    } else {
      result.words[i] = 0;
    }
  }

  // fill in the ranks
  const extendedN = charsPerMod * cover.period;
  var cur = 0;
  for i in 0..<cover.period {
    if cover.containedInCover((offset + i) % cover.period) {
      const sampleOffset =
        offsetToSubproblemOffset(offset + i, cover, charsPerMod);
      if offset + i < extendedN {
        result.ranks[cur] = Ranks[sampleOffset];
      } else {
        result.ranks[cur] = 0;
      }
      cur += 1;
    }
  }

  return result;
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

// can be called from keyPart(prefix, i)
inline proc getKeyPartForPrefix(const p: prefix(?), i: integral) {
  param sectionReturned = 0:int(8);
  param sectionEnd = -1:int(8);

  if i < p.nWords {
    return (sectionReturned, p.words[i]);
  }

  // otherwise, return that we reached the end
  return (sectionEnd, 0:p.wordType);
}

// can be called from keyPart(prefix, i)
inline proc getKeyPartForPrefix(const p: prefixAndSampleRanks(?), i: integral) {
  param sectionReturned = 0:int(8);
  param sectionEnd = -1:int(8);

  if i < p.nWords {
    return (sectionReturned, p.words[i]);
  }

  // otherwise, return that we reached the end
  return (sectionEnd, 0:p.wordType);
}

// considers the prefix and the 1st sample rank only
inline proc getKeyPartForPrefixAndFirstRank(const p: prefixAndSampleRanks(?),
                                            i: integral) {
  param sectionReturned = 0:int(8);
  param sectionEnd = -1:int(8);

  if i < p.nWords {
    return (sectionReturned, p.words[i]);
  }

  if i == p.nWords {
    return (sectionReturned, p.ranks[0]);
  }

  // otherwise, return that we reached the end
  return (sectionEnd, 0:p.wordType);
}

record prefixAndSampleRanksComparePrefixAndFirstRank {
  proc keyPart(a: prefixAndSampleRanks(?), i: int):(int(8), a.wordType) {
    return getKeyPartForPrefixAndFirstRank(a, i);
  }
}

// can be called from keyPart(someOffset, i)
// gets the key part for sorting the suffix starting at
// offset 'offset' within 'text' by the first 'maxPrefix characters.
inline proc getKeyPartForOffset(const offset: integral, i: integral,
                                const cfg:ssortConfig(?), type wordType,
                                const text, n: cfg.offsetType,
                                maxPrefix: cfg.offsetType) {
  type idxType = cfg.idxType;
  type characterType = cfg.characterType;
  type offsetType = cfg.offsetType;
  param sectionReturned = 0:int(8);
  param sectionEnd = -1:int(8);

  param eltsPerWord = numBytes(wordType) / numBytes(characterType);
  const iOff = i:offsetType;
  const nCharsIn:offsetType = iOff*eltsPerWord;
  const startIdx:offsetType = offset + nCharsIn;
  const startIdxCast = startIdx: idxType;
  if nCharsIn < maxPrefix && startIdxCast < n:idxType {
    // return further data by loading from the text array
    return (sectionReturned, loadWord(text, n, startIdxCast, wordType));
  }

  // otherwise, return that we reached the end
  return (sectionEnd, 0:wordType);
}


// can be called from keyPart(offsetAndCached, i)
inline proc getKeyPartForOffsetAndCached(const a: offsetAndCached(?),
                                         i: integral,
                                         const cfg:ssortConfig(?),
                                         const text, n: cfg.offsetType,
                                         maxPrefix: cfg.offsetType,
                                         type wordType=cfg.wordType) {
  type idxType = cfg.idxType;
  type characterType = cfg.characterType;
  type offsetType = cfg.offsetType;
  type cachedDataType = cfg.cachedDataType;

  param sectionReturned = 0:int(8);
  param sectionEnd = -1:int(8);
  if a.cacheType != nothing && wordType == a.cacheType && i == 0 {
    // return the cached data
    return (sectionReturned, a.cached);
  }

  return getKeyPartForOffset(a.offset, i, cfg, wordType,
                             text, n, maxPrefix=maxPrefix);
}

// these getPrefixKeyPart overloads call the above to adapt
// to different types.
inline proc getPrefixKeyPart(const a: offsetAndCached(?), i: integral,
                             const cfg:ssortConfig(?),
                             const text, n: cfg.offsetType,
                             maxPrefix: cfg.offsetType,
                             type wordType=cfg.wordType) {
  return getKeyPartForOffsetAndCached(a, i, cfg, text, n, maxPrefix, wordType);
}
inline proc getPrefixKeyPart(const a: integral, i: integral,
                             const cfg:ssortConfig(?),
                             const text, n: cfg.offsetType,
                             maxPrefix: cfg.offsetType,
                             type wordType=cfg.wordType) {
  return getKeyPartForOffset(a, i, cfg, wordType, text, n, maxPrefix);
}
inline proc getPrefixKeyPart(const a: prefix(?), i: integral,
                             const cfg:ssortConfig(?),
                             const text, n: cfg.offsetType,
                             maxPrefix: cfg.offsetType,
                             type wordType=cfg.prefixWordType) {
  return getKeyPartForPrefix(a, i);
}
inline proc getPrefixKeyPart(const a: prefixAndSampleRanks(?), i: integral,
                             const cfg:ssortConfig(?),
                             const text, n: cfg.offsetType,
                             maxPrefix: cfg.offsetType,
                             type wordType=cfg.prefixWordType) {
  return getKeyPartForPrefix(a, i);
}

inline proc comparePrefixes(const a, const b,
                            const cfg:ssortConfig(?),
                            const text, n: cfg.offsetType,
                            maxPrefix: cfg.offsetType): int {
  type aWordType = cfg.getWordType(a);
  type bWordType = cfg.getWordType(b);
  type wordType = if numBits(aWordType) > numBits(bWordType)
                  then aWordType
                  else bWordType;
  param charsPerWord = numBits(wordType) / numBits(cfg.characterType);
  const m = myDivCeil(maxPrefix, charsPerWord);
  var curPart = 0;
  while curPart < m {
    var (aSection, aPart) = getPrefixKeyPart(a, curPart,
                                             cfg, text, n, maxPrefix,
                                             wordType=wordType);
    var (bSection, bPart) = getPrefixKeyPart(b, curPart,
                                             cfg, text, n, maxPrefix,
                                             wordType=wordType);
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

  // if we reached this point, they are the same
  return 0;
}

/* This is helpful for computing ranks based on first v characters. */
private
proc prefixDiffersFromPrevious(const cfg:ssortConfig(?),
                               i: cfg.offsetType,
                               const Sample: [] offsetAndCached(?),
                               const text, n: cfg.offsetType,
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

proc charactersInCommon(const cfg:ssortConfig(?), const a, const b): int
  where a.type == b.type &&
        (isSubtype(a.type, prefix) ||
         isSubtype(a.type, prefixAndSampleRanks))
{
  type aWordType = cfg.getWordType(a);
  type bWordType = cfg.getWordType(a);
  if aWordType != bWordType then
    compilerError("bad call to charactersInCommon");

  var curPart = 0;
  var bitsInCommon = 0;
  while true {
    var (aSection, aPart) = getKeyPartForPrefix(a, curPart);
    var (bSection, bPart) = getKeyPartForPrefix(b, curPart);
    if aSection != 0 || bSection != 0 {
      break;
    }
    if aPart == bPart {
      bitsInCommon += numBits(aPart.type);
    } else {
      // compute the common number of bits
      bitsInCommon += BitOps.clz(aPart ^ bPart):int;
      break;
    }

    curPart += 1;
  }

  // now divide the bits in common by the number of bits per character
  // to get the number of characters in common.
  return bitsInCommon / numBits(cfg.characterType);
}

/** Sort suffixes that we have already initialized in A
    by the first maxPrefix character values.
 */
proc sortSuffixesByPrefix(const cfg:ssortConfig(?),
                          const thetext, n: cfg.offsetType,
                          ref A: [] offsetAndCached(?),
                          maxPrefix: A.eltType.offsetType) {
  type idxType = cfg.idxType;
  type characterType = cfg.characterType;
  type offsetType = cfg.offsetType;
  type cachedDataType = cfg.cachedDataType;
  type wordType = cfg.wordType;
  // Define a comparator to support radix sorting by the first maxPrefix
  // character values.
  record myPrefixComparator1 {
    proc keyPart(a: offsetAndCached(?), i: int):(int(8), wordType) {
      return getPrefixKeyPart(a, i=i, cfg, thetext, n=n, maxPrefix=maxPrefix);
    }
  }

  sort(A, new myPrefixComparator1());
}

proc sortSuffixesByPrefixBounded(const cfg:ssortConfig(?),
                                 const thetext, n: cfg.offsetType,
                                 ref A: [] offsetAndCached(?),
                                 lowerBound: prefix(?),
                                 upperBound: prefix(?),
                                 maxPrefix: A.eltType.offsetType) {
  type idxType = cfg.idxType;
  type characterType = cfg.characterType;
  type offsetType = cfg.offsetType;
  type cachedDataType = cfg.cachedDataType;
  type wordType = cfg.wordType;

  // compute the number of characters in common between lowerBound and
  // upperBound.
  const nCharsCommon = charactersInCommon(cfg, lowerBound, upperBound);

  if nCharsCommon == 0 ||
     (cachedDataType != nothing &&
      numBits(characterType)*nCharsCommon < numBits(cachedDataType)) {
    // use the other sorter if there is no savings here
    sortSuffixesByPrefix(cfg, thetext, n, A, maxPrefix);
    return;
  }

  const useMaxPrefix=max(maxPrefix-nCharsCommon, 0);

  // Define a comparator to support radix sorting by the next
  // characters up to maxPrefix that it's not already sorted by.
  record myPrefixComparator2 {
    proc keyPart(a: offsetAndCached(?), i: int):(int(8), wordType) {
      return getKeyPartForOffset(a.offset + nCharsCommon, i=i,
                                 cfg, wordType,
                                 thetext, n=n,
                                 maxPrefix=useMaxPrefix);
    }
  }

  sort(A, new myPrefixComparator2());
}


/* If we computed the suffix array for text using cachedDataType!=nothing,
   there is some ambiguity between 0s due to end-of-string/padding
   vs 0s due to the input. This function resolves the issue
   by adjusting the first several suffix array entries.

   This fix does not need to apply to suffix sorting done with
   a recursive subproblem (rather than with the base case)
   as compareSampleRanks will cover it.
 */
proc fixTrailingZeros(const text, n:integral, ref A: [],
                      type characterType, type offsetType,
                      type cachedDataType) {

  // We use 0s to indicate padding which can happen at the end of
  // the string. If the input also ended with 0s, then we need to
  // re-sort the suffixes at the end of the string. Since they
  // all end in zero, we know that the suffix array order here
  // is the offsets in descending order.

  var firstNonZero = -1;
  // loop starting at the end of the string, stop when we hit a nonzero
  for i in 0..<n by -1 {
    if text[i] != 0 {
      firstNonZero = i;
      break;
    }
  }
  var firstZero = firstNonZero+1;
  var nZero = n-firstZero;

  forall i in 0..<nZero {
    A[i].offset = n-1-i;
  }
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
                              cover = new differenceCover(DEFAULT_PERIOD));

  // First, construct the offsetAndCached array that will be sorted.
  var A = buildAllOffsets(offsetType=offsetType, cachedDataType=cachedDataType,
                          text, useN);

  sortSuffixesByPrefix(cfg, text, n=useN, A, maxPrefix=max(offsetType));

  fixTrailingZeros(text, n, A,
                   characterType=characterType,
                   offsetType=offsetType,
                   cachedDataType=cachedDataType);

  return A;
}

proc makeSampleOffset(type offsetType, type cachedDataType,
                      i: offsetType,
                      const text, n: offsetType,
                      cover: differenceCover(?)) {
  // i is a packed index into the offsets to sample
  // we have to unpack it to get the regular offset
  const whichPeriod = i / cover.sampleSize;
  const phase = i % cover.sampleSize;
  const coverVal = cover.cover[phase]:offsetType;
  const unpackedIdx = whichPeriod * cover.period + coverVal;

  return makeOffsetAndCached(offsetType=offsetType,
                             cachedDataType=cachedDataType,
                             unpackedIdx, text, n);
}

proc chooseIdxType(type offsetType) {
  // workaround for Chapel issue #25559 otherwise
  // we could just use offsetType.
  return if offsetType == uint then uint else int;
}

/**
  Construct an array of suffixes (not yet sorted)
  for only those offsets in 0..<n that are also in the difference cover.
 */
proc buildSampleOffsets(type offsetType, type cachedDataType,
                        const text, n: offsetType,
                        const cover: differenceCover(?),
                        sampleN: offsetType) {
  const nPeriods = myDivCeil(n, cover.period); // nPeriods * period >= n
  assert(sampleN == cover.sampleSize * nPeriods);

  const Dom = {0..<sampleN};
  var SA:[Dom] offsetAndCached(offsetType, cachedDataType) =
    forall i in Dom do
      makeSampleOffset(offsetType=offsetType, cachedDataType=cachedDataType,
                       i=i, text, n=n, cover);

  return SA;
}

/* Returns an array of the sample offsets sorted
   by the first cover.period characters.
 */
proc sortSampleOffsets(const cfg:ssortConfig(?),
                       const thetext, n: cfg.offsetType,
                       const nTasks: int,
                       const requestedNumBuckets: int,
                       out sampleN: cfg.offsetType) {
  const ref cover = cfg.cover;
  const nPeriods = myDivCeil(n, cover.period); // nPeriods * period >= n
  sampleN = cover.sampleSize * nPeriods;
  var nToSampleForSplitters = (SAMPLE_RATIO*requestedNumBuckets):int;
  if !PARTITION_SORT_SAMPLE || nToSampleForSplitters >= sampleN {
    // Simpler approach: build sample offsets and sort them
    // does more random access and/or uses more memory (if caching data)
    var Sample = buildSampleOffsets(offsetType=cfg.offsetType,
                                    cachedDataType=cfg.cachedDataType,
                                    thetext, n, cover, /*out*/ sampleN);
    // then sort the these by the first cover.period characters;
    // note that these offsets are in 0..<n not 0..<mySampleN
    sortSuffixesByPrefix(cfg, thetext, n=n, Sample, maxPrefix=cover.period);

    return Sample;
  } else {
    // To better avoid random access,
    // go through the input & partition by a splitter
    // while creating the offset & storing it into an output array
    // for the Sample.
    type offsetType = cfg.offsetType;
    type cachedDataType = cfg.cachedDataType;
    type wordType = uint(64);

    record myPrefixComparator3 {
      proc keyPart(a: offsetAndCached(?), i: int):(int(8), wordType) {
        if a.cacheType == wordType {
          return getKeyPartForOffsetAndCached(a, i=i,
                                              cfg, thetext, n=n,
                                              maxPrefix=cover.period);
        } else {
          return getKeyPartForOffset(a.offset, i=i, cfg, wordType=wordType,
                                     thetext, n, maxPrefix=cover.period);
        }
      }
      proc keyPart(a: prefix(?), i: int):(int(8), wordType) {
        return getKeyPartForPrefix(a, i);
      }
    }

    record offsetProducer1 {
      proc eltType type do return offsetAndCached(offsetType, cachedDataType);
      proc this(i: offsetType) {
        return makeSampleOffset(offsetType, cachedDataType, i,
                                thetext, n, cover);
      }
    }

    const comparator = new myPrefixComparator3();
    const InputProducer = new offsetProducer1();

    // first, create a sorting sample of offsets in the cover
    const sp; // initialized below
    {
      var randNums = new Random.randomStream(cfg.offsetType);
      var SplittersSampleDom = {0..<nToSampleForSplitters};
      type prefixType = makePrefix(0,thetext,n,cover,cfg.prefixWordType).type;
      var SplittersSample:[SplittersSampleDom] prefixType;
      for (x, r) in zip(SplittersSample,
                        randNums.next(SplittersSampleDom, 0, sampleN-1)) {
        // r is a packed index into the offsets to sample
        // we have to unpack it to get the regular offset
        const whichPeriod = r / cover.sampleSize;
        const phase = r % cover.sampleSize;
        const coverVal = cover.cover[phase]:offsetType;
        const unpackedIdx = whichPeriod * cover.period + coverVal;
        x = makePrefix(unpackedIdx, thetext, n, cover, cfg.prefixWordType);
      }

      // sort the sample and create the splitters
      sp = new splitters(SplittersSample, requestedNumBuckets, comparator,
                         sampleIsSorted=false);
    }

    var Sample: [0..<sampleN] offsetAndCached(offsetType, cachedDataType);

    // now, count & partition by the prefix by traversing over the input
    const Counts = partition(InputProducer, Sample, sp, comparator,
                             0, sampleN-1, nTasks);

    const Ends = + scan Counts;

    // now, consider each bucket & sort within that bucket
    const nBuckets = sp.numBuckets;
    forall bucketIdx in 0..<nBuckets {
      const bucketSize = Counts[bucketIdx];
      const bucketStart = Ends[bucketIdx] - bucketSize;
      const bucketEnd = bucketStart + bucketSize - 1;

      if TRACE {
        writeln("sortSampleOffsets bucket ", bucketIdx,
                " has ", bucketSize, " suffixes");
      }

      if bucketSize > 1 {
        if !sp.bucketHasEqualityBound(bucketIdx) &&
           sp.bucketHasLowerBound(bucketIdx) &&
           sp.bucketHasUpperBound(bucketIdx) {
          sortSuffixesByPrefixBounded(cfg, thetext, n=n,
                                      Sample[bucketStart..bucketEnd],
                                      sp.bucketLowerBound(bucketIdx),
                                      sp.bucketUpperBound(bucketIdx),
                                      maxPrefix=cover.period);
        } else {
          sortSuffixesByPrefix(cfg, thetext, n=n,
                               Sample[bucketStart..bucketEnd],
                               maxPrefix=cover.period);
        }
        // TODO: adjust sort library call to avoid the ~2x array view overhead
        //   * by optimizing down to c_ptr for contiguous arrays, or
        //   * by allowing passing the array bounds
        // Or, consider using MSB Radix Sort to avoid that overhead here.
      }
    }

    return Sample;
  }
}

/*

  Given an offset in the current problem that refers to a sample position
  (that is, a position that is in the difference cover), compute
  the corresponding offset within the recursive subproblem.

  This function arranges for the subproblem to consist of sample
  offsets with a particular modules; e.g. for DC3, the subproblem
  consists of these offsets:
    0 3 6 9 ...      1 4 7 10 ...
    (i == 0 mod 3)   (i == 1 mod 3)
*/
proc offsetToSubproblemOffset(offset: integral, cover, charsPerMod: integral) {
  const whichPeriod = offset / cover.period;
  const phase = offset % cover.period;
  const coverIdx = cover.coverIndex(phase);
  if EXTRA_CHECKS then assert(0 <= coverIdx && coverIdx < cover.sampleSize);
  const useIdx = coverIdx * charsPerMod + whichPeriod;
  return useIdx;
}

/*

  Given an offset in the subproblem, compute the offset in the problem
  that refers to that sample position (that is, a position in the difference
  cover).

  This is the inverse of offsetToSubproblemOffset.
 */
proc subproblemOffsetToOffset(subOffset: integral, cover, charsPerMod: integral)
{
  const coverIdx = subOffset / charsPerMod;
  const whichPeriod = subOffset - coverIdx * charsPerMod;
  const phase = cover.cover[coverIdx];
  const offset = whichPeriod * cover.period + phase;
  if EXTRA_CHECKS {
    assert(offsetToSubproblemOffset(offset, cover, charsPerMod) == subOffset);
  }
  return offset;
}

/*
   This function helps with compareSampleRanks when considering offsets
   after the end of the string, which can come up in the difference
   cover computation. We imagine that after the end of the string
   there are 0s and then a terminator. The suffix array ordering for
   these will be to sort whatever offset is closest to the terminator
   first; that is, to sort the offsets in reverse order.
 */
proc compareEndOfString(a: integral, b: integral, n: integral) {
  if a == b {
    return 0;
  }

  if a >= n && b >= n {
    // if they are both past the end of the string, sort
    // them in reverse offset order
    if a > b {
      return -1; // a sorts before b
    } else if a < b {
      return 1;
    }
    // a == b handled above
  }

  if a >= n { // && b < n, or would have returned above
    return -1; // a sorts before b, a is at end of string and b is not
  }

  if b >= n { // && a < n, or would have returned above
    return 1; // b sorts before a, b is at the end of the string and a is not
  }

  return 0; // a < n && b < n, so nothing to say here about the ordering
}

/*
  Assuming the prefix at two offsets matches, compare the offsets
  using the sample rank from the recursive subproblem.
 */
proc compareSampleRanks(a: offsetAndCached(?), b: offsetAndCached(?),
                        n: integral, const SampleRanks, charsPerMod, cover) {
  // find k such that a.offset+k and b.offset+k are both in the cover
  // (i.e. both are in the sample solved in the recursive problem)
  const k = cover.findInCover(a.offset % cover.period,
                              b.offset % cover.period);

  const aSampleOffset = offsetToSubproblemOffset(a.offset + k,
                                                 cover, charsPerMod);
  const bSampleOffset = offsetToSubproblemOffset(b.offset + k,
                                                 cover, charsPerMod);
  var rankA = SampleRanks[aSampleOffset];
  var rankB = SampleRanks[bSampleOffset];

  const cmp = compareEndOfString(a.offset + k, b.offset + k, n);
  if cmp != 0 {
    return cmp;
  }

  if rankA < rankB {
    return -1;
  }
  if rankA > rankB {
    return 1;
  }
  return 0;
}
proc compareSampleRanks(a: prefixAndSampleRanks(?), b: offsetAndCached(?),
                        n: integral, const SampleRanks, charsPerMod, cover) {
  // find k such that a.offset+k and b.offset+k are both in the cover
  // (i.e. both are in the sample solved in the recursive problem)
  const k = cover.findInCover(a.offset % cover.period,
                              b.offset % cover.period);
  const aPlusKCoverIdx = cover.coverIndex((a.offset + k) % cover.period);
  const aCoverIdx = cover.coverIndex(a.offset % cover.period);
  var aRankIdx = aPlusKCoverIdx - aCoverIdx;
  if aRankIdx < 0 then aRankIdx += cover.sampleSize;

  const bSampleOffset = offsetToSubproblemOffset(b.offset + k,
                                                 cover, charsPerMod);
  var rankA = a.ranks[aRankIdx];
  var rankB = SampleRanks[bSampleOffset];

  const cmp = compareEndOfString(a.offset + k, b.offset + k, n);
  if cmp != 0 {
    return cmp;
  }

  if rankA < rankB {
    return -1;
  }
  if rankA > rankB {
    return 1;
  }
  return 0;
}


/* Sort suffixes by prefix and by the sample ranks.
   This puts them into final sorted order when computing the suffix array.
 */
proc sortSuffixesCompletely(const cfg:ssortConfig(?),
                            const thetext, n: cfg.offsetType,
                            const SampleRanks, charsPerMod: cfg.offsetType,
                            ref A: [] offsetAndCached(?)) {
  record finalComparator1 {
    proc compare(a: offsetAndCached(?), b: offsetAndCached(?)) {
      // first, compare the first cover.period characters of text
      const prefixCmp=comparePrefixes(a, b, cfg, thetext, n, cfg.cover.period);
      if prefixCmp != 0 {
        return prefixCmp;
      }
      // if the prefixes are the same, compare the nearby sample
      // rank from the recursive subproblem.
      return compareSampleRanks(a, b, n, SampleRanks, charsPerMod, cfg.cover);
    }
  }

  // TODO: consider sorting each non-sample suffix position (with radix sort)
  // and then doing a multi-way merge.

  sort(A, new finalComparator1());
}

proc sortSuffixesCompletelyBounded(
                            const cfg:ssortConfig(?),
                            const thetext, n: cfg.offsetType,
                            const SampleRanks, charsPerMod: cfg.offsetType,
                            ref A: [] offsetAndCached(?),
                            const lowerBound: prefixAndSampleRanks(?),
                            const upperBound: prefixAndSampleRanks(?)) {

  type characterType = cfg.characterType;
  type cachedDataType = cfg.cachedDataType;
  type wordType = cfg.wordType;

  // compute the number of characters in common between lowerBound and
  // upperBound.
  const nCharsCommon = charactersInCommon(cfg, lowerBound, upperBound);

  if nCharsCommon == 0 ||
     (cachedDataType != nothing &&
      numBits(characterType)*nCharsCommon < numBits(cachedDataType)) {
    // use the other sorter if there is no savings here
    sortSuffixesCompletely(cfg, thetext, n, SampleRanks, charsPerMod, A);
    return;
  }

  const useMaxPrefix=max(cfg.cover.period-nCharsCommon, 0);

  record finalComparator2 {
    proc compare(a: offsetAndCached(?), b: offsetAndCached(?)) {
      // first, compare the first cover.period characters of text
      if useMaxPrefix > 0 {
        const aOffset = a.offset + nCharsCommon;
        const bOffset = b.offset + nCharsCommon;
        const prefixCmp = comparePrefixes(aOffset, bOffset,
                                          cfg, thetext, n,
                                          maxPrefix=useMaxPrefix);
        if prefixCmp != 0 {
          return prefixCmp;
        }
      }
      // if the prefixes are the same, compare the nearby sample
      // rank from the recursive subproblem.
      return compareSampleRanks(a, b, n, SampleRanks, charsPerMod, cfg.cover);
    }
  }

  // TODO: consider sorting each non-sample suffix position (with radix sort)
  // and then doing a multi-way merge.

  sort(A, new finalComparator2());
}

/** Create and return a sorted suffix array for the suffixes 0..<n
    referring to 'text'. */
proc ssortDcx(const cfg:ssortConfig(?), const thetext, n: cfg.offsetType)
 : [0..<n] offsetAndCached(cfg.offsetType, cfg.cachedDataType) {

  // figure out how big the sample will be, including a 0 after each mod
  type offsetType = cfg.offsetType;
  type cachedDataType = cfg.cachedDataType;
  const ref cover = cfg.cover;
  const charsPerMod = 1+myDivCeil(n, cover.period);
  const sampleN = cover.sampleSize * charsPerMod;

  if TRACE {
    writeln("in ssortDcx ", cfg.type:string, " n=", n);
    writeln("thetext is ", thetext[0..<n]); // TODO remove me
    writeln("charsPerMod is ", charsPerMod);
  }

  if thetext.domain.low != 0 {
    halt("sortDcx expects input array to start at 0");
  }
  if n + INPUT_PADDING > thetext.size {
    // expect it to be zero-padded past n so that
    // getKeyPart / loadWord does not have to check n
    halt("sortDcx needs extra space at the end of the array");
  }

  //// Base Case ////

  // base case: just sort them if the number of characters is
  // less than the cover period, or sample.n is not less than n
  if n <= cover.period || sampleN >= n {
    if TRACE {
      writeln("Base case suffix sort for n=", n);
    }
    return computeSuffixArrayDirectly(thetext, n,
                                      characterType=cfg.characterType,
                                      offsetType=offsetType,
                                      cachedDataType=cachedDataType);
  }

  // set up information for recursive subproblem
  type subCached =
    if (cachedDataType == nothing ||
        numBits(cachedDataType) >= numBits(offsetType))
    then cachedDataType
    else offsetType;

  const subCfg = new ssortConfig(idxType=cfg.idxType,
                                 characterType=offsetType,
                                 offsetType=offsetType,
                                 cachedDataType=subCached,
                                 cover=cover);

  var nTasks = computeNumTasks() * thetext.targetLocales().size;
  writeln("nTasks is ", nTasks);
  var requestedNumBuckets = 8 * nTasks;

  //// Step 1: Sort Sample Suffixes ////

  // begin by computing the input text for the recursive subproblem
  var SampleText:[0..<sampleN+INPUT_PADDING] subCfg.characterType;
  var allSamplesHaveUniqueRanks = false;
  // create a sample splitters that can be replaced later
  var unusedSplitter = makePrefixAndSampleRanks(0, thetext, n,
                                                0, SampleText, sampleN,
                                                charsPerMod=charsPerMod,
                                                cover, cfg.prefixWordType);
  // these are initialized below
  const SampleSplitters1; // used if allSamplesHaveUniqueRanks
  const SampleSplitters2; // used otherwise

  {
    var mySampleN: offsetType;
    // Sample is an array of sorted offsets
    const Sample = sortSampleOffsets(cfg, thetext, n,
                                     nTasks=nTasks,
                                     requestedNumBuckets=requestedNumBuckets,
                                     /*out*/ mySampleN);
    writeln("Sample ", Sample);

    // now, compute the rank of each of these. we need to compare
    // the first cover.period characters & assign different ranks when these
    // differ.
    // NOTE: this is the main place where caching in OffsetAndCached
    // can offer some benefit.
    // TODO: skip the temporary array once Chapel issue #12482 is addressed
    const Tmp = [i in Sample.domain]
                  prefixDiffersFromPrevious(cfg,
                                            i,
                                            Sample, thetext, n,
                                            maxPrefix=cover.period);

    // note: inclusive scan causes Ranks[0] to be 1, so Ranks is 1-based
    const Ranks = + scan Tmp;


    allSamplesHaveUniqueRanks = Ranks.last == mySampleN + 1;
    writeln("Naming ranks ", Ranks);
    writeln("allSamplesHaveUniqueRanks ", allSamplesHaveUniqueRanks);

    // create the input for the recursive subproblem from the offsets and ranks
    SampleText = 0; // PERF TODO: noinit it
                    // and write a loop to zero what is not initalized below

    forall (offset, rank) in zip(Sample, Ranks) {
      // offset is an unpacked offset. find the offset in
      // the recursive problem input to store the rank into.
      // Do so in a way that arranges for SampleText to consist of
      // all sample inputs at a particular mod, followed by other modulus.
      // We have charsPerMod characters for each mod in the cover.
      const useIdx=offsetToSubproblemOffset(offset.offset, cover, charsPerMod);
      // this is not a data race because Sample.offsets are a permutation
      // of the offsets.
      SampleText[useIdx] = rank;
    }

    writeln("SampleText ", SampleText[0..<mySampleN]);

    if PARTITION_SORT_ALL && allSamplesHaveUniqueRanks {
      // set SampleSplitters to one based upon Sample sorted offsets
      // and SampleText ranks.
      record sampleCreator1 {
        proc eltType type do return unusedSplitter.type;
        proc size do return mySampleN;
        proc this(i: int) {
          // i is an index into the sorted subproblem suffixes, <mySampleN.
          // find the offset in the subproblem
          const subOffset = Sample[i].offset;
          // find the index in the parent problem.
          const offset =
            subproblemOffsetToOffset(subOffset, cover, charsPerMod);
          return makePrefixAndSampleRanks(offset, thetext, n,
                                          subOffset, SampleText, sampleN,
                                          charsPerMod=charsPerMod,
                                          cover, cfg.prefixWordType);
        }
      }
      const comparator = new prefixAndSampleRanksComparePrefixAndFirstRank();
      // split-init SampleSplitters1
      writeln("initing SampleSplitters1");
      SampleSplitters1 = new splitters(new sampleCreator1(),
                                       requestedNumBuckets,
                                       comparator,
                                       sampleIsSorted = true);
    } else {
      // This will not be used -- initializing it to keep compiler happy
      SampleSplitters1 = new splitters([unusedSplitter, unusedSplitter], false);
    }
  }

  if !allSamplesHaveUniqueRanks {
    //// recursively sort the subproblem ////
    const SubSA = ssortDcx(subCfg, SampleText, sampleN);
    if TRACE {
      writeln("back in ssortDcx n=", n);
      writeln("SubSA is ", SubSA);
    }
    // Replace the values in SampleText with
    // 1-based ranks from the suffix array.
    forall (offset,rank) in zip(SubSA, SubSA.domain) {
      SampleText[offset.offset] = rank+1;
    }
    writeln("SampleText is ", SampleText);
    if PARTITION_SORT_ALL {
      // replace SampleSplitters with one based the SubSA suffix array
      // and SampleText ranks.
      record sampleCreator2 {
        proc eltType type do return unusedSplitter.type;
        proc size do return sampleN;
        proc this(i: int) {
          // i is an index into the subproblem suffix array, <sampleN.
          // find the offset in the subproblem
          const subOffset = SubSA[i].offset;
          // find the index in the parent problem.
          const offset =
            subproblemOffsetToOffset(subOffset, cover, charsPerMod);
          return makePrefixAndSampleRanks(offset, thetext, n,
                                          subOffset, SampleText, sampleN,
                                          charsPerMod=charsPerMod,
                                          cover, cfg.prefixWordType);
        }
      }
      const comparator = new prefixAndSampleRanksComparePrefixAndFirstRank();
      writeln("initing SampleSplitters2");
      SampleSplitters2 = new splitters(new sampleCreator2(),
                                       requestedNumBuckets,
                                       comparator,
                                       sampleIsSorted = true);
    } else {
       SampleSplitters2 = new splitters([unusedSplitter, unusedSplitter],
                                        false); // dummy to support split init
    }
  } else {
    // No need to recurse if all offsets had unique Ranks
    // i.e. each character in SampleText occurs only once
    // i.e. each character in SampleText is already the rank
    SampleSplitters2 = new splitters([unusedSplitter, unusedSplitter],
                                     false); // dummy to support split init
  }

  //// Step 2: Sort everything all together ////
  if !PARTITION_SORT_ALL {
    // simple sort of everything all together
    var SA = buildAllOffsets(offsetType=offsetType,
                             cachedDataType=cfg.cachedDataType,
                             thetext, n);

    record finalComparator1 {
      proc compare(a: offsetAndCached(?), b: offsetAndCached(?)) {
        // first, compare the first cover.period characters of text
        const prefixCmp = comparePrefixes(a, b, cfg, thetext, n, cover.period);
        if prefixCmp != 0 {
          return prefixCmp;
        }
        // if the prefixes are the same, compare the nearby sample
        // rank from the recursive subproblem.
        return compareSampleRanks(a, b, n, SampleText, charsPerMod, cover);
      }
    }

    writeln("final sort (simple) ranks are ", SampleText[0..<sampleN]);

    sort(SA, new finalComparator1());

    /*fixTrailingZeros(thetext, n, SA,
                   characterType=cfg.characterType,
                   offsetType=offsetType,
                   cachedDataType=cfg.cachedDataType);*/

    writeln("returning SA ", SA);
    return SA;

  } else {
    // this implementation is more complicated but should be more efficient

    // in a pass over the input,
    // partition the suffixes according to the splitters

    record offsetProducer2 {
      proc eltType type do return offsetAndCached(offsetType, cachedDataType);
      proc this(i: offsetType) {
        const ret = makeOffsetAndCached(offsetType, cachedDataType, i,
                                        thetext, n);
        writeln("offsetProducer2(", i, ") generated ", ret);
        return ret;
      }
    }

    record finalPartitionComparator {
      // note: this one should just be used for EXTRA_CHECKS
      proc compare(a: prefixAndSampleRanks(?), b: prefixAndSampleRanks(?)) {
        // first, compare the first cover.period characters of text
        const prefixCmp = comparePrefixes(a, b, cfg, thetext, n, cover.period);
        if prefixCmp != 0 {
          return prefixCmp;
        }
        // if the prefixes are the same, compare the nearby sample
        // rank from the recursive subproblem.
        return a.ranks[0] - b.ranks[0];
      }
      // this is the main compare function used in the partition
      proc compare(a: prefixAndSampleRanks(?), b: offsetAndCached(?)) {
        // first, compare the first cover.period characters of text
        const prefixCmp = comparePrefixes(a, b, cfg, thetext, n, cover.period);
        if prefixCmp != 0 {
          return prefixCmp;
        }
        // if the prefixes are the same, compare the nearby sample
        // rank from the recursive subproblem.
        return compareSampleRanks(a, b, n, SampleText, charsPerMod, cover);
      }
    }

    const comparator = new finalPartitionComparator();
    const InputProducer = new offsetProducer2();

    var SA: [0..<n] offsetAndCached(offsetType, cachedDataType);

    const ref SampleSplitters = if allSamplesHaveUniqueRanks
                                then SampleSplitters1
                                else SampleSplitters2;

    writeln("SampleSplitters ", SampleSplitters.sortedStorage);

    const Counts = partition(InputProducer, SA, SampleSplitters, comparator,
                             0, n-1, nTasks);

    writeln("final sort ranks are ", SampleText[0..<sampleN]);
    writeln("final sort after partition SA is ", SA);

    const Ends = + scan Counts;

    // now, consider each bucket & sort within that bucket
    const nBuckets = SampleSplitters.numBuckets;
    forall bucketIdx in 0..<nBuckets {
      const bucketSize = Counts[bucketIdx];
      const bucketStart = Ends[bucketIdx] - bucketSize;
      const bucketEnd = bucketStart + bucketSize - 1;

      if TRACE {
        writeln("final sort bucket ", bucketIdx,
                " has ", bucketSize, " suffixes");
        if SampleSplitters.bucketHasLowerBound(bucketIdx) {
          writeln("lower bound ", SampleSplitters.bucketLowerBound(bucketIdx));
        }
        if SampleSplitters.bucketHasEqualityBound(bucketIdx) {
          writeln("equal bound ",
                   SampleSplitters.bucketEqualityBound(bucketIdx));
        }
        if SampleSplitters.bucketHasUpperBound(bucketIdx) {
          writeln("upper bound ", SampleSplitters.bucketUpperBound(bucketIdx));
        }

        writeln("Bucket is ", SA[bucketStart..bucketEnd]);
      }

      if bucketSize > 1 && !SampleSplitters.bucketHasEqualityBound(bucketIdx) {
        if SampleSplitters.bucketHasLowerBound(bucketIdx) &&
           SampleSplitters.bucketHasUpperBound(bucketIdx) {
          sortSuffixesCompletelyBounded(
                                 cfg, thetext, n=n,
                                 SampleText, charsPerMod,
                                 SA[bucketStart..bucketEnd],
                                 SampleSplitters.bucketLowerBound(bucketIdx),
                                 SampleSplitters.bucketUpperBound(bucketIdx));
        } else {
          sortSuffixesCompletely(cfg, thetext, n=n,
                                 SampleText, charsPerMod,
                                 SA[bucketStart..bucketEnd]);
        }
      }
    }

    assert(Ends.last == n);

    /*
    fixTrailingZeros(thetext, n, SA,
                   characterType=cfg.characterType,
                   offsetType=offsetType,
                   cachedDataType=cfg.cachedDataType);*/

    writeln("returning SA ", SA);
    return SA;
  }
}


}
