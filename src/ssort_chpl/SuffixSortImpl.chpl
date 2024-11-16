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
import Utility.{computeNumTasks,makeBlockDomain,replicate,getLocalReplicand};

use BlockDist;
use Math;
use IO;
use Sort;
use Random; // 'use' (vs 'import') to work around an error about
            // PCGRandomPrivate_iterate_bounded
import BitOps;
import Reflection;
import CTypes.c_sizeof;
import Time;

import SuffixSort.DEFAULT_PERIOD;
import SuffixSort.EXTRA_CHECKS;
import SuffixSort.TRACE;
import SuffixSort.TIMING;
import SuffixSort.INPUT_PADDING;

// how much more should we sample to create splitters?
// 1.0 would be only to sample enough for the splitters
config const sampleRatio = 1.5;

config const seed = 1;
config const minBucketsPerTask = 8;
config const minBucketsSpace = 2_000_000; // a size in bytes

// upper-case names for the config constants to better identify them in code
const SAMPLE_RATIO = sampleRatio;
const SEED = seed;
const MIN_BUCKETS_PER_TASK = minBucketsPerTask;
const MIN_BUCKETS_SPACE = minBucketsSpace;

// use a partition-based sorting startegy for improved parallelism
// and memory usage
config param PARTITION_SORT_ALL = true;
// and also for sorting the sample by the first characters
config param PARTITION_SORT_SAMPLE = true;
// if this is set, separately sort each nonsample, and do k-way merge.
// this should be faster for large problem sizes since the merge step
// depends on the cover size rather than log n.
config param IMPROVED_SORT_ALL = false; // TODO: re-enable
                                        // after identifying communication


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

  type loadWordType; // load this much text data when doing comparisons
                     // or when sorting. it's like cachedDataType
                     // but doesn't cause caching.

  const cover: differenceCover(?);

  const locales; // an array of locales to use
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

/** Helper type function to use a simple integer offset
    when there is no cached data */
proc offsetAndCachedT(type offsetType, type cacheType) type {
  if cacheType == nothing {
    return offsetType;
  } else {
    return offsetAndCached(offsetType, cacheType);
  }
}


/**
  This record holds a whole prefix of cover.period characters
  packed into words.

  This is useful for splitters.
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
  This record holds a prefix and an offset.
 */
record prefixAndOffset : writeSerializable {
  type wordType;
  type offsetType;
  param nWords;

  var offset: offsetType;
  var p: prefix(wordType, nWords);

  // this function is a debugging aid
  proc serialize(writer, ref serializer) throws {
    writer.write(offset, "(");
    for i in 0..<nWords {
      writer.writef("%016xu", p.words[i]);
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
  var p: prefix(wordType, nWords);
  var ranks: nRanks*offsetType;

  // this function is a debugging aid
  proc serialize(writer, ref serializer) throws {
    writer.write(offset);
    writer.write("(");
    for i in 0..<nWords {
      writer.writef("%016xu", p.words[i]);
    }
    writer.write("|");
    for i in 0..<nRanks {
      if i != 0 then writer.write(",");
      writer.write(ranks[i]);
    }
    writer.write(")");
  }
}

// helpers for a param divide while rounding up
inline proc myDivCeil(x: integral, param y: integral) {
  return (x + y - 1) / y;
}
inline proc myDivCeil(param x: integral, param y: integral) param {
  return (x + y - 1) / y;
}

// helper to allow handling integer offsets or offsetAndCached.
inline proc offset(a: integral) {
  return a;
}
inline proc offset(a: offsetAndCached(?)) {
  return a.offset;
}
inline proc offset(a: prefixAndOffset(?)) {
  return a.offset;
}
inline proc offset(a: prefixAndSampleRanks(?)) {
  return a.offset;
}

// these casts from prefixAndSampleRanks help with multiWayMerge
operator :(x: prefixAndSampleRanks(?), type t:x.offsetType) {
  return offset(x);
}
operator :(x: prefixAndSampleRanks(?),
           type t:offsetAndCached(x.offsetType,nothing)) {
  return new offsetAndCached(offsetType=x.offsetType,
                             cacheType=nothing,
                             offset=offset(x),
                             cached=none);
}
operator :(x: prefixAndSampleRanks(?),
           type t:offsetAndCached(x.offsetType,x.wordType)) {
  return new offsetAndCached(offsetType=x.offsetType,
                             cacheType=x.wordType,
                             offset=offset(x),
                             cached=x.words[0]);
}


/**
  Read a "word" of data from 'text' character index 'i'.
  Assumes that there are 8 bytes of padding past the real data.
  */
inline proc loadWord(const cfg: ssortConfig(?),
                     offset: cfg.offsetType,
                     const text, n: cfg.offsetType) {

  if EXTRA_CHECKS {
    assert(0 <= offset && offset:uint < n:uint);
  }

  // handle some simple cases first
  type wordType = cfg.loadWordType;

  if numBits(wordType) == numBits(text.eltType) {
    return text[offset]: wordType;
  }

  param wordBytes = numBytes(wordType);
  param textCharBytes = numBytes(text.eltType);
  param textCharBits = textCharBytes*8;
  param numToRead = wordBytes / textCharBytes;
  if wordBytes <= textCharBytes || !isUintType(wordType) {
    compilerError("invalid loadWord call");
  }

  // I expect this loop to be folded away by the backend compiler &
  // turn into a bswap instruction.
  var ret: wordType = 0;
  for j in 0..<numToRead {
    ret <<= textCharBits;
    ret |= text[offset+j];
  }

  return ret;
}

proc ssortConfig.checkWordType(a: integral) {
  return true;
}
proc ssortConfig.checkWordType(a: offsetAndCached(?)) param {
  if a.cacheType != nothing {
    if a.cacheType != this.loadWordType {
      compilerError("bad configuration for offsetAndCached");
    }
  }
  return true;
}
proc ssortConfig.checkWordType(a: prefix(?)) param {
  if a.wordType != this.loadWordType {
      compilerError("bad configuration for prefix");
  }

  return true;
}
proc ssortConfig.checkWordType(a: prefixAndOffset(?)) param {
  if a.wordType != this.loadWordType {
      compilerError("bad configuration for prefixAndOffset");
  }

  return true;
}
proc ssortConfig.checkWordType(a: prefixAndSampleRanks(?)) param {
  if a.wordType != this.loadWordType {
      compilerError("bad configuration for prefixAndSampleRanks");
  }

  return true;
}

/**
  When sorting using 'loadWordType', how many characters should
  be considered in order to match 'minChars' characters
  that are handled a 'loadWordType' at a time?

  The result will fit eveny into 'loadWordType' and be >= 'minChars'.
 */
proc ssortConfig.getPrefixSize(param minChars) param {
  // how many words do we need in order to hold cover.period characters?
  param wordBytes = numBytes(loadWordType);
  param textCharBytes = numBytes(characterType);
  param nWords = myDivCeil(minChars * textCharBytes, wordBytes);
  return nWords*wordBytes / textCharBytes;
}

/**
 Construct an offsetAndCached (or integer) for offset 'i' in the input.
 */
inline proc makeOffsetAndCached(const cfg: ssortConfig(?),
                                offset: cfg.offsetType,
                                const text, n: cfg.offsetType) {
  if cfg.cachedDataType == nothing {
    return offset;
  } else {
    if cfg.cachedDataType != cfg.loadWordType {
      compilerError("cachedDataType must be nothing or match loadWordType");
    }
    const cached: cfg.cachedDataType;
    if offset < n {
      cached = loadWord(cfg, offset, text, n);
    } else {
      cached = 0;
    }

    return new offsetAndCached(offsetType=cfg.offsetType,
                               cacheType=cfg.cachedDataType,
                               offset=offset,
                               cached=cached);
  }
}

/**
  Construct an prefix record for offset 'offset' in the input
  by loading the relevant data from 'text'. The prefix stores
  at least k characters.
 */
proc makePrefix(const cfg: ssortConfig(?), offset: cfg.offsetType,
                const text, n: cfg.offsetType /*, param k = cfg.cover.period*/) {
  type characterType = cfg.characterType;
  type wordType = cfg.loadWordType;
  const ref cover = cfg.cover;
  param k = cover.period;
  // how many words do we need in order to hold cover.period characters?
  param wordBytes = numBytes(wordType);
  param textCharBytes = numBytes(characterType);
  param charsPerWord = wordBytes / textCharBytes;
  param nWords = myDivCeil(k, charsPerWord);
  if wordBytes < textCharBytes || !isUintType(wordType) {
    compilerError("invalid makePrefix call");
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
      result.words[i] = loadWord(cfg, idx, text, n);
    } else {
      result.words[i] = 0;
    }
  }

  return result;
}

proc makePrefixAndOffset(const cfg: ssortConfig(?), offset: cfg.offsetType,
                         const text, n: cfg.offsetType) {
  type characterType = cfg.characterType;
  type wordType = cfg.loadWordType;
  const ref cover = cfg.cover;
  type prefixType = makePrefix(cfg, offset, text, n).type;
  param nWords = prefixType.nWords;

  var result = new prefixAndOffset(wordType=wordType,
                                   offsetType=cfg.offsetType,
                                   nWords=nWords,
                                   offset=offset,
                                   p=makePrefix(cfg, offset, text, n));
  return result;
}


/**
  Construct an prefixAndSampleRanks record for offset 'i' in the input
  by loading the relevant data from 'text' and 'ranks'.
 */
proc makePrefixAndSampleRanks(const cfg: ssortConfig(?),
                              offset: cfg.offsetType,
                              const text, n: cfg.offsetType,
                              const Ranks,
                              charsPerMod: cfg.offsetType) {
  const ref cover = cfg.cover;
  // compute the type information for creating a prefix
  type prefixType = makePrefix(cfg, offset, text, n).type;
  type characterType = text.eltType;
  type wordType = cfg.loadWordType;

  var result = new prefixAndSampleRanks(wordType=wordType,
                                        offsetType=cfg.offsetType,
                                        nWords=prefixType.nWords,
                                        nRanks=cover.sampleSize,
                                        offset=offset,
                                        p=makePrefix(cfg, offset, text, n));

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
proc buildAllOffsets(const cfg:ssortConfig(?), const text, n: cfg.offsetType,
                     resultDom: domain(?)) {
  var SA:[resultDom] offsetAndCachedT(cfg.offsetType, cfg.cachedDataType) =
    forall i in resultDom do
      makeOffsetAndCached(cfg, i, text, n);

  return SA;
}

// can be called from keyPart(prefix, i)
inline proc getKeyPartForPrefix(const p: prefix(?), i: integral) {
  if i < p.nWords {
    return (keyPartStatus.returned, p.words[i]);
  }

  // otherwise, return that we reached the end
  return (keyPartStatus.pre, 0:p.wordType);
}

// can be called from keyPart(prefix, i)
inline proc getKeyPartForPrefix(const p: prefixAndOffset(?), i: integral) {
  if i < p.nWords {
    return (keyPartStatus.returned, p.p.words[i]);
  }

  // otherwise, return that we reached the end
  return (keyPartStatus.pre, 0:p.wordType);
}

// can be called from keyPart(prefix, i)
inline proc getKeyPartForPrefix(const p: prefixAndSampleRanks(?), i: integral) {
  if i < p.nWords {
    return (keyPartStatus.returned, p.p.words[i]);
  }

  // otherwise, return that we reached the end
  return (keyPartStatus.pre, 0:p.wordType);
}

// can be called from keyPart(someOffset, i)
// gets the key part for sorting the suffix starting at
// offset 'offset' within 'text' by the first 'maxPrefix characters.
inline proc getKeyPartForOffset(const cfg: ssortConfig(?),
                                const offset: cfg.offsetType, i: integral,
                                const text, n: cfg.offsetType,
                                maxPrefix: cfg.offsetType) {
  type idxType = cfg.idxType;
  type characterType = cfg.characterType;
  type offsetType = cfg.offsetType;
  type wordType = cfg.loadWordType;

  param eltsPerWord = numBytes(wordType) / numBytes(characterType);
  const iOff = i:offsetType;
  const nCharsIn:offsetType = iOff*eltsPerWord;
  const startIdx:offsetType = offset + nCharsIn;
  if nCharsIn < maxPrefix && startIdx < n {
    // return further data by loading from the text array
    return (keyPartStatus.returned, loadWord(cfg, startIdx, text, n));
  }

  // otherwise, return that we reached the end
  return (keyPartStatus.pre, 0:wordType);
}


// can be called from keyPart(offsetAndCached, i)
inline proc getKeyPartForOffsetAndCached(const cfg: ssortConfig(?),
                                         const a: offsetAndCached(?),
                                         i: integral,
                                         const text, n: cfg.offsetType,
                                         maxPrefix: cfg.offsetType) {
  if a.cacheType != nothing && cfg.loadWordType == a.cacheType && i == 0 {
    // return the cached data
    return (keyPartStatus.returned, a.cached);
  }

  return getKeyPartForOffset(cfg, a.offset, i, text, n, maxPrefix=maxPrefix);
}
inline proc getKeyPartForOffsetAndCached(const cfg: ssortConfig(?),
                                         const a: cfg.offsetType,
                                         i: integral,
                                         const text, n: cfg.offsetType,
                                         maxPrefix: cfg.offsetType) {
  return getKeyPartForOffset(cfg, a, i, text, n, maxPrefix=maxPrefix);
}


// these getPrefixKeyPart overloads call the above to adapt
// to different types.
inline proc getPrefixKeyPart(const cfg: ssortConfig(?),
                             const a: offsetAndCached(?), i: integral,
                             const text, n: cfg.offsetType,
                             maxPrefix: cfg.offsetType) {
  cfg.checkWordType(a);
  return getKeyPartForOffsetAndCached(cfg, a, i, text, n, maxPrefix);
}
inline proc getPrefixKeyPart(const cfg: ssortConfig(?),
                             const a: cfg.offsetType, i: integral,
                             const text, n: cfg.offsetType,
                             maxPrefix: cfg.offsetType) {
  return getKeyPartForOffset(cfg, a, i, text, n, maxPrefix);
}
inline proc getPrefixKeyPart(const cfg:ssortConfig(?),
                             const a: prefix(?), i: integral,
                             const text, n: cfg.offsetType,
                             maxPrefix: cfg.offsetType) {
  cfg.checkWordType(a);
  return getKeyPartForPrefix(a, i);
}
inline proc getPrefixKeyPart(const cfg:ssortConfig(?),
                             const a: prefixAndOffset(?), i: integral,
                             const text, n: cfg.offsetType,
                             maxPrefix: cfg.offsetType) {
  cfg.checkWordType(a);
  return getKeyPartForPrefix(a, i);
}
inline proc getPrefixKeyPart(const cfg:ssortConfig(?),
                             const a: prefixAndSampleRanks(?), i: integral,
                             const text, n: cfg.offsetType,
                             maxPrefix: cfg.offsetType) {
  cfg.checkWordType(a);
  return getKeyPartForPrefix(a, i);
}

inline proc comparePrefixes(const cfg: ssortConfig(?),
                            const a, const b,
                            const text, n: cfg.offsetType,
                            maxPrefix: cfg.offsetType): int {

  cfg.checkWordType(a);
  cfg.checkWordType(b);
  type wordType = cfg.loadWordType;

  param charsPerWord = numBits(wordType) / numBits(cfg.characterType);
  const m = myDivCeil(maxPrefix, charsPerWord);
  var curPart = 0;
  while curPart < m {
    var (aSection, aPart) = getPrefixKeyPart(cfg, a, curPart,
                                             text, n, maxPrefix=maxPrefix);
    var (bSection, bPart) = getPrefixKeyPart(cfg, b, curPart,
                                             text, n, maxPrefix=maxPrefix);
    if aSection != keyPartStatus.returned ||
       bSection != keyPartStatus.returned {
      return aSection:int - bSection:int;
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
proc prefixDiffersFromPrevious(const cfg:ssortConfig(?),
                               i: cfg.offsetType,
                               const Sample: [], // integral or offsetAndCached
                               const text, n: cfg.offsetType,
                               maxPrefix: cfg.offsetType): cfg.offsetType {
  type offsetType = cfg.offsetType;

  // handle base case, where i-1 does not exist
  if i == 0 {
    return 1:offsetType; // assign a new rank
  }

  // otherwise, compare this element and the previous
  var cmp = comparePrefixes(cfg, Sample[i], Sample[i-1],
                            text, n=n, maxPrefix=maxPrefix);
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
  cfg.checkWordType(a);
  cfg.checkWordType(b);

  var curPart = 0;
  var bitsInCommon = 0;
  while true {
    var (aSection, aPart) = getKeyPartForPrefix(a, curPart);
    var (bSection, bPart) = getKeyPartForPrefix(b, curPart);
    if aSection != keyPartStatus.returned ||
       bSection != keyPartStatus.returned {
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

// this is a compatability function to allow this code to compile
// before and after PR #25636.
proc sortRegion(ref A: [], comparator, region: range(?)) {
  if isDistributedDomain(A.domain) {
    // copy to a local array, sort, and copy back.
    // this situation occurs regularly within sortSuffixesByPrefix.
    // TODO: can try to do sort in-place with an array view if it's all local
    var localDom: domain(1) = {region,};
    var localA:[localDom] A.eltType = A[region];
    local {
      sortRegion(localA, comparator, region);
    }
    A[region] = localA;
  } else {
    if Reflection.canResolve("sort", A, comparator, region) {
      sort(A, comparator, region);
    } else {
      compilerWarning("Falling back on sort with array view; " +
                      "please update to a Chapel version including PR #25636");
      sort(A[region], comparator);
    }
  }
}

/**
  Sort suffixes that we have already initialized in A
  by the first maxPrefix character values.

  Sorts only A[region].
 */
proc sortSuffixesByPrefix(const cfg:ssortConfig(?),
                          const thetext, n: cfg.offsetType,
                          ref A: [], // integral or offsetAndCached
                          region: range(?),
                          maxPrefix: cfg.offsetType) {
  type idxType = cfg.idxType;
  type characterType = cfg.characterType;
  type offsetType = cfg.offsetType;
  type cachedDataType = cfg.cachedDataType;
  type wordType = cfg.loadWordType;
  // Define a comparator to support radix sorting by the first maxPrefix
  // character values.
  record myPrefixComparator1 : keyPartComparator {
    proc keyPart(a, i: int):(keyPartStatus, wordType) {
      return getPrefixKeyPart(cfg, a, i, thetext, n, maxPrefix=maxPrefix);
    }
  }

  sortRegion(A, new myPrefixComparator1(), region=region);
}


/* If we computed the suffix array for text using cachedDataType!=nothing,
   there is some ambiguity between 0s due to end-of-string/padding
   vs 0s due to the input. This function resolves the issue
   by adjusting the first several suffix array entries.

   This fix does not need to apply to suffix sorting done with
   a recursive subproblem (rather than with the base case)
   as compareSampleRanks will cover it with compareEndOfString.
 */
proc fixTrailingZeros(const text, n:integral, ref A: []) {

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
    const off = n-1-i;
    if isIntegralType(A.eltType) {
      A[i] = off;
    } else {
      A[i].offset = off;
    }
  }
}

// check to see if a domain is distributed
proc isDistributedDomain(dom) param {
  // this uses unstable / undocumented features. a better way is preferred.
  return !chpl_domainDistIsLayout(dom);
}

/**
  Create a suffix array for the suffixes 0..<n for 'text'
  by sorting the data at those suffixes directly.

  This is useful as a base case, but shouldn't be used generally
  for creating a suffix array as it is O(n**2).

  Return an array representing this suffix array.
  */
proc computeSuffixArrayDirectly(const cfg:ssortConfig(?),
                                const text, n: cfg.offsetType,
                                resultDom: domain(?)) {

  if isDistributedDomain(resultDom) {
    // when directly computing the suffix array on a distributed array,
    // move everything local first and then copy back to the result array.

    // This could just be = resultDom but this way of writing avoids a warning.
    var localDom: domain(1) = {resultDom.dim(0),};
    var localA = computeSuffixArrayDirectly(cfg, text, n, localDom);
    const A: [resultDom] localA.eltType = localA;
    return A;
  }

  // First, construct the offsetAndCached array that will be sorted.
  var A = buildAllOffsets(cfg, text, n, resultDom);

  sortSuffixesByPrefix(cfg, text, n, A, 0..<n,
                       maxPrefix=max(cfg.offsetType));

  fixTrailingZeros(text, n, A);

  if isIntegralType(A.eltType) {
    return A;
  }

  // otherwise, convert cached type to int
  const SAOffsets: [resultDom] cfg.offsetType =
    forall elt in A do offset(elt);
  return SAOffsets;
}

proc makeSampleOffset(const cfg: ssortConfig(?),
                      i: cfg.offsetType,
                      const text, n: cfg.offsetType) {
  // i is a packed index into the offsets to sample
  // we have to unpack it to get the regular offset
  type offsetType = cfg.offsetType;
  const ref cover = cfg.cover;
  const whichPeriod = i / cover.sampleSize;
  const phase = i % cover.sampleSize;
  const coverVal = cover.cover[phase]:offsetType;
  const unpackedIdx = whichPeriod * cover.period + coverVal;

  return makePrefixAndOffset(cfg, unpackedIdx, text, n);
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
proc buildSampleOffsets(const cfg: ssortConfig(?),
                        const text, n: cfg.offsetType,
                        sampleN: cfg.offsetType) {
  const ref cover = cfg.cover;
  const nPeriods = myDivCeil(n, cover.period); // nPeriods * period >= n
  assert(sampleN == cover.sampleSize * nPeriods);

  const Dom = makeBlockDomain({0..<sampleN}, targetLocales=cfg.locales);
  type prefixAndOffsetType = makePrefixAndOffset(cfg, 0, text, n).type;
  var SA:[Dom] prefixAndOffsetType =
    forall i in Dom do makeSampleOffset(cfg, i, text, n);

  return SA;
}

/* Returns an array of the sample offsets sorted
   by the first cover.period characters.

   The returned array is Block distributed over cfg.locales if CHPL_COMM!=none.
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
    if TRACE {
      writeln("sortSampleOffsets simple");
    }
    // Simpler approach: build sample offsets and sort them
    // does more random access and/or uses more memory (if caching data)
    var Sample = buildSampleOffsets(cfg, thetext, n, sampleN);
    // then sort the these by the first cover.period characters;
    // note that these offsets are in 0..<n not 0..<mySampleN
    param coverPrefix = cfg.getPrefixSize(cover.period);
    sortSuffixesByPrefix(cfg, thetext, n=n, Sample, 0..<sampleN,
                         maxPrefix=coverPrefix);

    return Sample;
  } else {
    if TRACE {
      writeln("sortSampleOffsets partitioning");
    }
    // To better avoid random access,
    // go through the input & partition by a splitter
    // while creating the offset & storing it into an output array
    // for the Sample.
    type offsetType = cfg.offsetType;
    type cachedDataType = cfg.cachedDataType;
    type wordType = cfg.loadWordType;
    param coverPrefix = cfg.getPrefixSize(cover.period);
    type prefixAndOffsetType = makePrefixAndOffset(cfg, 0,thetext, n).type;

    //writeln("PARTITION_SORT_SAMPLE with coverPrefix=", coverPrefix);

    record myPrefixComparator3 : keyPartComparator {
      proc keyPart(a, i: int) : (keyPartStatus, wordType) {
        if !isIntegralType(a.type) && a.cacheType == wordType {
          return getKeyPartForOffsetAndCached(cfg, a, i,
                                              thetext, n,
                                              maxPrefix=coverPrefix);
        } else {
          return getKeyPartForOffset(cfg, offset(a), i,
                                     thetext, n, maxPrefix=coverPrefix);
        }
      }
      proc keyPart(a: prefixAndOffset(?), i: int):(keyPartStatus, wordType) {
        return getKeyPartForPrefix(a, i);
      }
      proc keyPart(a: prefix(?), i: int):(keyPartStatus, wordType) {
        return getKeyPartForPrefix(a, i);
      }
    }

    record offsetProducer1 {
      proc eltType type do return prefixAndOffsetType;
      proc this(i: offsetType) {
        return makeSampleOffset(cfg, i, thetext, n);
      }
    }

    const comparator = new myPrefixComparator3();
    const InputProducer = new offsetProducer1();

    // first, create a sorting sample of offsets in the cover
    const sp; // initialized below
    {
      var randNums;
      if SEED == 0 {
        randNums = new Random.randomStream(cfg.offsetType);
      } else {
        randNums = new Random.randomStream(cfg.offsetType, seed=SEED);
      }
      var SplittersSampleDom = {0..<nToSampleForSplitters};
      type prefixType = makePrefix(cfg, 0,thetext, n).type;
      var SplittersSample:[SplittersSampleDom] prefixType;
      // TODO: this could be a forall loop, but running into
      // some kind of error about PCGRandomPrivate_iterate_bounded
      forall (x, r) in zip(SplittersSample,
                           randNums.next(SplittersSampleDom, 0, sampleN-1)) {
        // r is a packed index into the offsets to sample
        // we have to unpack it to get the regular offset
        const whichPeriod = r / cover.sampleSize;
        const phase = r % cover.sampleSize;
        const coverVal = cover.cover[phase]:offsetType;
        const unpackedIdx = whichPeriod * cover.period + coverVal;
        x = makePrefix(cfg, unpackedIdx, thetext, n);
      }

      // sort the sample and create the splitters
      sp = new splitters(SplittersSample, requestedNumBuckets, comparator,
                         howSorted=sortLevel.unsorted);
    }

    const replSp = replicateSplitters(sp, cfg.locales);
    const SampleDom = makeBlockDomain({0..<sampleN},
                                      targetLocales=cfg.locales);
    var Sample: [SampleDom] prefixAndOffsetType;

    // now, count & partition by the prefix by traversing over the input
    const Counts = partition(InputProducer, Sample, sp, replSp, comparator,
                             start=0, end=sampleN-1,
                             locales=cfg.locales, nTasks);

    const Ends = + scan Counts;

    // now, consider each bucket & sort within that bucket.
    // this will be distributed because partition returns a Block array
    const nBuckets = sp.numBuckets;
    var minBucketSize = max(int);
    var maxBucketSize = min(int);
    var sumBucketSizes = 0;
    var countBucketsConsidered = 0;
    forall (bucketSize, bucketIdx) in zip(Counts, Counts.domain)
                                   with (min reduce minBucketSize,
                                         max reduce maxBucketSize,
                                         + reduce sumBucketSizes,
                                         + reduce countBucketsConsidered) {
      const bucketStart = Ends[bucketIdx] - bucketSize;
      const bucketEnd = bucketStart + bucketSize - 1;
      const ref mySp = localSplitter(sp, replSp);

      // skip empty buckets and buckets with equal elements
      if bucketSize > 1 && !mySp.bucketHasEqualityBound(bucketIdx) {
        // note statistics
        minBucketSize reduce= bucketSize;
        maxBucketSize reduce= bucketSize;
        sumBucketSizes += bucketSize;
        countBucketsConsidered += 1;

        sortSuffixesByPrefix(cfg, thetext, n=n,
                             Sample, bucketStart..bucketEnd,
                             maxPrefix=coverPrefix);
      }
    }

    if TRACE {
      writeln(" bucket size statistics for sortSampleOffsets",
              " n=", countBucketsConsidered,
              " min=", minBucketSize,
              " avg=", sumBucketSizes:real / countBucketsConsidered,
              " max=", maxBucketSize);
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
  This function just helps to return the comparison result between two integers.
  We could just return a - b, but that does not behave as expected with unsigned
  types.
 */
inline proc compareIntegers(a: integral, b: a.type) {
  if a < b {
    return -1;
  }
  if a > b {
    return 1;
  }

  return 0;
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
    return compareIntegers(b, a);
  }

  if a >= n { // && b < n, or would have returned above
    return -1; // a sorts before b, a is at end of string and b is not
  }

  if b >= n { // && a < n, or would have returned above
    return 1; // b sorts before a, b is at the end of the string and a is not
  }

  return 0; // a < n && b < n, so nothing to say here about the ordering
}

inline proc comparePrefixAndSampleRanks(const cfg: ssortConfig(?),
                                        const a: prefixAndSampleRanks(?),
                                        const b: prefixAndSampleRanks(?),
                                        const text, n: cfg.offsetType,
                                        maxPrefix: cfg.offsetType,
                                        charsPerMod, cover) {
  //writeln("comparePrefixAndSampleRanks(", a, ", ", b, ")");

  // first, compare the first cover.period characters of text
  const prefixCmp = comparePrefixes(cfg, a, b, text, n, maxPrefix);
  if prefixCmp != 0 {
    //writeln("returnA ", prefixCmp);
    return prefixCmp;
  }

  // TODO: this is wrong
  //const rankA = a.ranks[0];
  //const rankB = b.ranks[0];


  // if the prefixes are the same, consider the end-of-string behavior
  const cmpEnd = compareEndOfString(a.offset, b.offset, n);
  if cmpEnd != 0 {
    //writeln("returnB ", cmpEnd);
    return cmpEnd;
  }

  // lastly, compare the sample ranks
  //writeln("returnC ", compareIntegers(rankA, rankB));
  //return compareIntegers(rankA, rankB);
  return compareSampleRanks(a, b, n, none, charsPerMod, cover);
}


/*
  Assuming the prefix at two offsets matches, compare the offsets
  using the sample rank from the recursive subproblem.

  a and b should be integral or offsetAndCached.
 */
proc compareSampleRanks(a, b,
                        n: integral, const SampleRanks, charsPerMod, cover) {
  writeln("in testing-only compareSampleRanks");
  //writeln("compareSampleRanks(", a, ", ", b, ")");

  // find k such that a.offset+k and b.offset+k are both in the cover
  // (i.e. both are in the sample solved in the recursive problem)
  const k = cover.findInCover(offset(a) % cover.period,
                              offset(b) % cover.period);
  //writeln("k is ", k);

  const aSampleOffset = offsetToSubproblemOffset(offset(a) + k,
                                                 cover, charsPerMod);
  const bSampleOffset = offsetToSubproblemOffset(offset(b) + k,
                                                 cover, charsPerMod);
  const rankA = SampleRanks[aSampleOffset];
  const rankB = SampleRanks[bSampleOffset];

  const cmp = compareEndOfString(offset(a) + k, offset(b) + k, n);
  if cmp != 0 {
    return cmp;
  }

  return compareIntegers(rankA, rankB);
}
proc compareSampleRanks(a: prefixAndSampleRanks(?), b,
                        n: integral, const SampleRanks, charsPerMod, cover) {
  writeln("in testing-only compareSampleRanks2");

  // find k such that a.offset+k and b.offset+k are both in the cover
  // (i.e. both are in the sample solved in the recursive problem)
  const k = cover.findInCover(offset(a) % cover.period,
                              offset(b) % cover.period);
  const aPlusKCoverIdx = cover.coverIndex((offset(a) + k) % cover.period);
  const aCoverIdx = cover.coverIndex(offset(a) % cover.period);
  var aRankIdx = aPlusKCoverIdx - aCoverIdx;
  if aRankIdx < 0 then aRankIdx += cover.sampleSize;

  const bSampleOffset = offsetToSubproblemOffset(offset(b) + k,
                                                 cover, charsPerMod);
  const rankA = a.ranks[aRankIdx];
  const rankB = SampleRanks[bSampleOffset];

  const cmp = compareEndOfString(offset(a) + k, offset(b) + k, n);
  if cmp != 0 {
    return cmp;
  }

  return compareIntegers(rankA, rankB);
}

proc compareSampleRanks(a: prefixAndSampleRanks(?), b: prefixAndSampleRanks(?),
                        n: integral, const SampleRanks, charsPerMod, cover) {
  // find k such that a.offset+k and b.offset+k are both in the cover
  // (i.e. both are in the sample solved in the recursive problem)
  const k = cover.findInCover(offset(a) % cover.period,
                              offset(b) % cover.period);
  const aj = cover.nextCoverIndex(offset(a) % cover.period);
  const bj = cover.nextCoverIndex(offset(b) % cover.period);
  // a + k and a + aj are both in the cover
  // a + aj is the offset which represents the first cover position here
  const aPlusKCoverIdx = cover.coverIndex((offset(a) + k) % cover.period);
  const aPlusJCoverIdx = cover.coverIndex((offset(a) + aj) % cover.period);
  var aRankIdx = aPlusKCoverIdx - aPlusJCoverIdx;
  if aRankIdx < 0 then aRankIdx += cover.sampleSize;

  const bPlusKCoverIdx = cover.coverIndex((offset(b) + k) % cover.period);
  const bPlusJCoverIdx = cover.coverIndex((offset(b) + bj) % cover.period);
  var bRankIdx = bPlusKCoverIdx - bPlusJCoverIdx;
  if bRankIdx < 0 then bRankIdx += cover.sampleSize;

  const rankA = a.ranks[aRankIdx];
  const rankB = b.ranks[bRankIdx];

  const cmp = compareEndOfString(offset(a) + k, offset(b) + k, n);
  if cmp != 0 {
    return cmp;
  }

  return compareIntegers(rankA, rankB);
}


/* Sort suffixes by prefix and by the sample ranks.
   This puts them into final sorted order when computing the suffix array.
   Sorts only A[region].

   The computation in this function is not distributed because
   it's expected to be called from within a distributed forall loop.
 */
proc sortSuffixesCompletely(const cfg:ssortConfig(?),
                            const thetext, n: cfg.offsetType,
                            const SampleRanks, charsPerMod: cfg.offsetType,
                            ref A: [], // integral or offsetAndCached(?)
                            region: range(?),
                            // these are for gathering timing data
                            out partitionTime:real,
                            out lookupTime:real,
                            out sortEachNonsampleTime:real,
                            out mergeTime:real) {
  type wordType = cfg.loadWordType;
  type characterType = cfg.characterType;
  const ref cover = cfg.cover;
  param coverPrefix = cfg.getPrefixSize(cover.period);

  record finalComparator : relativeComparator {
    proc compare(a, b) { // integral or offsetAndCached
      // first, compare the first cover.period characters of text
     const prefixCmp =
        comparePrefixes(cfg, a, b, thetext, n, maxPrefix=coverPrefix);

      if prefixCmp != 0 {
        return prefixCmp;
      }
      // if the prefixes are the same, compare the nearby sample
      // rank from the recursive subproblem.
      return compareSampleRanks(a, b, n, SampleRanks, charsPerMod, cover);
    }
  }

  // This comparator helps to sort suffixes that all have the same
  // distance to a sample suffix.
  // Sample suffixes always have distance 0 to sample suffixes.
  // Other suffixes have a distance according to their phase.
  record phaseComparator : keyPartComparator {
    const phase: int;
    const k: int; // offset + k will be in the cover
    const nPrefixWords: int; // number of words of prefix to compare
    proc init(phase: int) {
      param eltsPerWord = numBytes(wordType) / numBytes(characterType);

      var nextsample = 0;
      for k in 0..cover.period {
        if cover.containedInCover((phase + k)%cover.period) {
          nextsample = k;
          break;
        }
      }

      this.phase = phase;
      this.k = nextsample;
      this.nPrefixWords = myDivCeil(this.k, eltsPerWord);

      //writeln("phase ", phase, " k is ", k);
    }
    proc keyPart(a, i: int):(keyPartStatus, wordType) {
      if EXTRA_CHECKS {
        if phase == 0 {
          assert(cover.containedInCover(offset(a) % cover.period));
        } else {
          assert(offset(a) % cover.period == phase);
          assert(cover.containedInCover((offset(a) + k) % cover.period));
        }
      }

      if i < this.nPrefixWords {
        // compare the prefix for the first nPrefixWords
        return getPrefixKeyPart(cfg, a, i, thetext, n, maxPrefix=cover.period);
      }
      if i == this.nPrefixWords {
        // compare the sample rank
        const rank;
        if isSubtype(a.type, prefixAndSampleRanks) {
          rank = a.ranks[0];
        } else {
          const sampleOffset = offsetToSubproblemOffset(offset(a) + k,
                                                        cover, charsPerMod);
          rank = SampleRanks[sampleOffset];
          assert(false);
        }
        return (keyPartStatus.returned, rank:wordType);
      }

      return (keyPartStatus.pre, 0:wordType);
    }
  }


  if numBits(wordType) != numBits(cfg.offsetType) || !IMPROVED_SORT_ALL {
    sortRegion(A, new finalComparator(), region=region);

  } else {
    // partition by putting sample offsets in bucket 0
    // and each nonsample offset in its own bucket.

    // help to distribute into buckets, bucket 0 has all sample positions,
    // other than that, they are sorted by mod cover.period
    record phaseSplitter {
      proc numBuckets param {
        return cover.period;
      }
      // yields (value, bucket index) for start_n..end_n
      // gets the elements by calling Input[i] to get element i
      // Input does not have to be an array, but it should have an eltType.
      iter classify(Input, start_n, end_n, comparator) {
        foreach i in start_n..end_n {
          const elt = Input[i];
          const phase = offset(elt) % cover.period;
          // this code relies on the assumption that 0 is in the cover
          // (since it uses 0 for the bucket containing sample suffixes)
          if EXTRA_CHECKS {
            assert(cover.containedInCover(0));
          }
          const bucket = if cover.containedInCover(phase) then 0 else phase;
          //writeln( (elt, bucket) );
          yield (elt, bucket);
        }
      }
    }

    // this assumption is used here
    assert(cover.containedInCover(0));

    //writeln("Partitioning by phase region ", region);
    var partitionTimer : Time.stopwatch;
    if TIMING {
      partitionTimer.start();
    }

    // destination for partitioning
    // this is a non-distributed (local) array even if A is distributed
    var B:[region] A.eltType;

    const unusedComparator = new finalComparator();
    const subTasks = computeNumTasks();
    const sp = new phaseSplitter();
    const rsp = none;
    const Counts = partition(A, B, sp, rsp, unusedComparator,
                             start=region.low, end=region.high,
                             locales=none, nTasks=subTasks);

    if isDistributedDomain(Counts.domain) then
      compilerError("Was not expecting it to be distributed");

    const Ends = + scan Counts;

    assert(Ends.last == region.size);

    if TIMING {
      partitionTimer.stop();
      partitionTime = partitionTimer.elapsed();
    }

    //writeln("Sorting buckets");
    var sortEachNonsampleTimer : Time.stopwatch;
    if TIMING {
      sortEachNonsampleTimer.start();
    }

    // now, consider each bucket & sort within that bucket
    const nBuckets = sp.numBuckets;
    var nNonZero = 0;
    forall (bucketSize, bucketIdx) in zip(Counts, Counts.domain)
                                   with (+ reduce nNonZero) {
      const bucketStart = region.low + Ends[bucketIdx] - bucketSize;
      const bucketEnd = bucketStart + bucketSize - 1; // inclusive

      if bucketSize > 0 && bucketIdx < cover.period {
        // sort the bucket data, which is currently in B
        sortRegion(B, new phaseComparator(bucketIdx),
                   region=bucketStart..bucketEnd);
        nNonZero += 1;
      }
    }

    if TIMING {
      sortEachNonsampleTimer.stop();
      sortEachNonsampleTime = sortEachNonsampleTimer.elapsed();
    }

    // Gather the ranges for input to multiWayMerge
    var InputRanges: [0..<nNonZero] range;
    var cur = 0;
    for bucketIdx in 0..<nBuckets {
      const bucketSize = Counts[bucketIdx];
      const bucketStart = region.low + Ends[bucketIdx] - bucketSize;
      const bucketEnd = bucketStart + bucketSize - 1; // inclusive

      if bucketSize > 0 && bucketIdx < cover.period {
        InputRanges[cur] = bucketStart..bucketEnd;
        cur += 1;
      }
    }

    //writeln("Multi-way merge");
    //writeln("region ", region, " InputRanges ", InputRanges);
    var mergeTimer : Time.stopwatch;
    if TIMING {
      mergeTimer.start();
    }

    // do the serial multi-way merging from B back into A
    multiWayMerge(B, InputRanges, A, region, new finalComparator());

    if TIMING {
      mergeTimer.stop();
      mergeTime = mergeTimer.elapsed();
    }
  }
}

/** Create and return a sorted suffix array for the suffixes 0..<n
    referring to 'thetext'.

    The returned array is Block distributed over cfg.locales if CHPL_COMM!=none.
*/
proc ssortDcx(const cfg:ssortConfig(?), const thetext, n: cfg.offsetType,
              resultDom = makeBlockDomain({0..<n},
                                          targetLocales=cfg.locales))
 : [resultDom] cfg.offsetType {

  var total : Time.stopwatch;

  type offsetType = cfg.offsetType;
  type cachedDataType = cfg.cachedDataType;
  const ref cover = cfg.cover;
  param coverPrefix = cfg.getPrefixSize(cover.period);

  // figure out how big the sample will be, including a 0 after each mod
  const charsPerMod = 1+myDivCeil(n, cover.period);
  const sampleN = cover.sampleSize * charsPerMod;

  if !isDistributedDomain(thetext.domain) && isDistributedDomain(resultDom) &&
     resultDom.targetLocales().size > 1 {
    writeln("warning: thetext not distributed but result is");
  }

  if TIMING {
    writeln("begin ssortDcx n=", n);
    total.start();
  }
  defer {
    if TIMING {
      total.stop();
      writeln("end ssortDcx n=", n, " after ", total.elapsed(), " s");
    }
  }
  if TRACE {
    writeln("in ssortDcx ", cfg.type:string, " n=", n);
    //writeln("thetext is ", thetext[0..<n]); // TODO remove me
    //writeln("charsPerMod is ", charsPerMod);
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
    return computeSuffixArrayDirectly(cfg, thetext, n, resultDom);
  }

  // set up information for recursive subproblem
  type subCached =
    if (cachedDataType == nothing ||
        numBits(cachedDataType) >= numBits(offsetType))
    then cachedDataType
    else uint;
  type subLoad =
    if numBits(cfg.loadWordType) >= numBits(offsetType)
    then cfg.loadWordType
    else uint;

  const subCfg = new ssortConfig(idxType=cfg.idxType,
                                 characterType=offsetType,
                                 offsetType=offsetType,
                                 cachedDataType=subCached,
                                 loadWordType=subLoad,
                                 cover=cover,
                                 locales=cfg.locales);

  //// Step 1: Sort Sample Suffixes ////

  // TODO: allocate output array here in order to avoid memory fragmentation

  // begin by computing the input text for the recursive subproblem
  var SampleDom = makeBlockDomain({0..<sampleN+INPUT_PADDING},
                                  targetLocales=cfg.locales);
  var SampleText:[SampleDom] subCfg.characterType;
  var allSamplesHaveUniqueRanks = false;

  // create a sample splitters that can be replaced later
  var unusedSplitter = makePrefixAndSampleRanks(cfg, 0, thetext, n,
                                                SampleText, charsPerMod);

  // compute number of buckets for sample partition & after recursion partition
  const splitterSize = c_sizeof(unusedSplitter.type):int;
  var nTasks = computeNumTasks() * resultDom.targetLocales().size;
  var requestedNumBuckets = max(MIN_BUCKETS_PER_TASK * nTasks,
                                MIN_BUCKETS_SPACE / splitterSize,
                                sqrt(n):int);

  if TRACE {
    writeln(" each prefixAndSampleRank is ", splitterSize, " bytes");
    writeln(" requesting ", requestedNumBuckets, " buckets");
    writeln(" nTasks is ", nTasks);
  }

  // these are initialized below
  const SampleSplitters1; // used if allSamplesHaveUniqueRanks
  const SampleSplitters2; // used otherwise

  {
    var pre : Time.stopwatch;
    if TIMING {
      pre.start();
    }
    defer {
      if TIMING {
        pre.stop();
        writeln("pre in ", pre.elapsed(), " s");
      }
    }

    var mySampleN: offsetType;
    // Sample is an array of sorted offsets
    const Sample = sortSampleOffsets(cfg, thetext, n,
                                     nTasks=nTasks,
                                     requestedNumBuckets=requestedNumBuckets,
                                     /*out*/ mySampleN);
    //writeln("Sample ", Sample);

    if EXTRA_CHECKS {
      forall off in Sample {
        assert(cover.containedInCover(offset(off) % cover.period));
      }
    }

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
    //writeln("Naming ranks ", Ranks);
    //writeln("allSamplesHaveUniqueRanks ", allSamplesHaveUniqueRanks);

    // create the input for the recursive subproblem from the offsets and ranks
    SampleText = 0; // PERF TODO: noinit it
                    // and write a loop to zero what is not initalized below

    forall (off, rank) in zip(Sample, Ranks) {
      // offset is an unpacked offset. find the offset in
      // the recursive problem input to store the rank into.
      // Do so in a way that arranges for SampleText to consist of
      // all sample inputs at a particular mod, followed by other modulus.
      // We have charsPerMod characters for each mod in the cover.
      const useIdx = offsetToSubproblemOffset(offset(off), cover, charsPerMod);
      // this is not a data race because Sample.offsets are a permutation
      // of the offsets.
      SampleText[useIdx] = rank;
    }

    //writeln("SampleText ", SampleText[0..<mySampleN]);

    if PARTITION_SORT_ALL && allSamplesHaveUniqueRanks {
      assert(false);
      // set SampleSplitters to one based upon Sample sorted offsets
      // and SampleText ranks.
      record sampleCreator1 {
        proc eltType type do return unusedSplitter.type;
        proc size do return mySampleN;
        proc this(i: int) {
          // i is an index into the sorted subproblem suffixes, <mySampleN.
          // find the offset in the subproblem
          const subOff = offset(Sample[i]);
          // find the index in the parent problem.
          const off = subproblemOffsetToOffset(subOff, cover, charsPerMod);
          return makePrefixAndSampleRanks(cfg, off, thetext, n,
                                          SampleText, charsPerMod);
        }
      }

      record sampleComparator1 : relativeComparator {
        proc compare(a: prefixAndSampleRanks(?), b: prefixAndSampleRanks(?)) {
          return comparePrefixAndSampleRanks(cfg, a, b, thetext, n,
                                             maxPrefix=coverPrefix,
                                             charsPerMod, cover);
        }
      }

      const comparator = new sampleComparator1();
      // split-init SampleSplitters1
      //writeln("initing SampleSplitters1");
      SampleSplitters1 = new splitters(new sampleCreator1(),
                                       requestedNumBuckets,
                                       comparator,
                                       howSorted=sortLevel.approximately);
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
      //writeln("SubSA is ", SubSA);
    }

    /*
    var update : Time.stopwatch;
    if TIMING {
      update.start();
    }
    defer {
      if TIMING {
        update.stop();
        writeln("update SampleText in ", update.elapsed(), " s");
      }
    }*/

    // Replace the values in SampleText with
    // 1-based ranks from the suffix array.
    forall (off,rank) in zip(SubSA, SubSA.domain) {
      // TODO: use a more compactified addressing here
      SampleText[offset(off)] = rank+1;
    }
    //writeln("SampleText is ", SampleText);
    if PARTITION_SORT_ALL {
      // replace SampleSplitters with one based the SubSA suffix array
      // and SampleText ranks.
      record sampleCreator2 {
        proc eltType type do return unusedSplitter.type;
        proc size do return sampleN;
        proc this(i: int) {
          // i is an index into the subproblem suffix array, <sampleN.
          // find the offset in the subproblem
          var subOff = offset(SubSA[i]);
          // find the index in the parent problem.
          var off = subproblemOffsetToOffset(subOff, cover, charsPerMod);

          return makePrefixAndSampleRanks(cfg, off, thetext, n,
                                          SampleText, charsPerMod);
        }
      }

      record sampleComparator2 : relativeComparator {
        proc compare(a: prefixAndSampleRanks(?), b: prefixAndSampleRanks(?)) {
          return comparePrefixAndSampleRanks(cfg, a, b, thetext, n,
                                             maxPrefix=coverPrefix,
                                             charsPerMod, cover);
        }
      }

      const comparator = new sampleComparator2();
      //writeln("initing SampleSplitters2");
      SampleSplitters2 = new splitters(new sampleCreator2(),
                                       requestedNumBuckets,
                                       comparator,
                                       howSorted=sortLevel.approximately);
    } else {
      // this case is for !PARTITION_SORT_ALL
      SampleSplitters2 = new splitters([unusedSplitter, unusedSplitter],
                                       false); // dummy to support split init
    }
  } else {
    // this case is for allSamplesHaveUniqueRanks==true.
    // No need to recurse if all offsets had unique Ranks
    // i.e. each character in SampleText occurs only once
    // i.e. each character in SampleText is already the rank
    SampleSplitters2 = new splitters([unusedSplitter, unusedSplitter],
                                     false); // dummy to support split init
  }

  /*
  var replicateTimer : Time.stopwatch;
  if TIMING {
    replicateTimer.start();
  }
  const RepSampleRanks = replicate(SampleText, targetLocales=cfg.locales);
  const RepTheText = replicate(thetext, targetLocales=cfg.locales);
  if TIMING {
    replicateTimer.stop();
    writeln("replicate in ", replicateTimer.elapsed(), " s");
  }*/

  var post : Time.stopwatch;
  if TIMING {
    post.start();
  }
  defer {
    if TIMING {
      post.stop();
      writeln("post in ", post.elapsed(), " s");
    }
  }


  //// Step 2: Sort everything all together ////
  /*if !PARTITION_SORT_ALL {
    assert(false);

    //writeln("simple sort");

    // simple sort of everything all together
    var SA = buildAllOffsets(cfg, thetext, n, resultDom);

    var partitionTime, lookupTime, sortEachNonsampleTime, mergeTime: real;

    sortSuffixesCompletely(cfg, thetext, n=n, RepSampleRanks, charsPerMod,
                           SA, 0..<n,
                           partitionTime, lookupTime,
                           sortEachNonsampleTime, mergeTime);

    //writeln("returning SA ", SA);
    return SA;

  } else*/ {
    //writeln("partitioned sort");

    // this implementation is more complicated but should be more efficient
    // because it has better parallelism

    // in a pass over the input,
    // partition the suffixes according to the splitters

    record offsetProducer2 {
      proc eltType type do return unusedSplitter.type;
      proc this(i: offsetType) {
        const ret = makePrefixAndSampleRanks(cfg, i, thetext, n,
                                             SampleText, charsPerMod);
        //writeln("offsetProducer2(", i, ") generated ", ret);
        return ret;
      }
    }

    record finalPartitionComparator : relativeComparator {
      // note: this one should just be used for EXTRA_CHECKS
      proc compare(a: prefixAndSampleRanks(?), b: prefixAndSampleRanks(?)) {
        return comparePrefixAndSampleRanks(cfg, a, b, thetext, n, coverPrefix,
                                           charsPerMod, cover);
      }
      // this is the main compare function used in the partition
      /*proc compare(a: prefixAndSampleRanks(?), b) {
        const ref localText = getLocalReplicand(RepTheText, cfg.locales);
        // b integral or offsetAndCached

        // first, compare the first cover.period characters of text
        const prefixCmp = comparePrefixes(cfg, a, b, localText, n, coverPrefix);
        if prefixCmp != 0 {
          return prefixCmp;
        }
        const ref localRanks = getLocalReplicand(RepSampleRanks, cfg.locales);
        // if the prefixes are the same, compare the nearby sample
        // rank from the recursive subproblem.
        return compareSampleRanks(a, b, n, localRanks, charsPerMod, cover);
      }*/
    }

    var makeBuckets : Time.stopwatch;
    if TIMING {
      makeBuckets.start();
    }

    const comparator = new finalPartitionComparator();
    const InputProducer = new offsetProducer2();

    var SA: [resultDom] InputProducer.eltType;

    const ref SampleSplitters = if allSamplesHaveUniqueRanks
                                then SampleSplitters1
                                else SampleSplitters2;
    const ReplSampleSplitters = replicateSplitters(SampleSplitters,
                                                   cfg.locales);

    //writeln("SampleSplitters is ", SampleSplitters.sortedStorage);

    const Counts = partition(InputProducer, SA,
                             SampleSplitters, ReplSampleSplitters, comparator,
                             start=0, end=n-1,
                             locales=cfg.locales, nTasks);

    //writeln("final sort ranks are ", RepSampleRanks[0..<sampleN]);
    //writeln("final sort after partition SA is ", SA);

    const Ends = + scan Counts;

    if TIMING {
      makeBuckets.stop();
      writeln("makeBuckets in ", makeBuckets.elapsed(), " s");
    }

    var sortBuckets : Time.stopwatch;
    if TIMING {
      sortBuckets.start();
    }

    // now, consider each bucket & sort within that bucket.
    // this will be distributed because partition returns a Block array
    const nBuckets = SampleSplitters.numBuckets;
    var minBucketSize = max(int);
    var maxBucketSize = min(int);
    var sumBucketSizes = 0;
    var countBucketsConsidered = 0;
    var minCommon = max(int);
    var maxCommon = 0;
    var sumCommon = 0;
    var countBucketsWithCommon = 0;
    var partitionTime = 0.0;
    var lookupTime = 0.0;
    var sortEachNonsampleTime = 0.0;
    var mergeTime = 0.0;
    forall (bucketSize, bucketIdx) in zip(Counts, Counts.domain)
                                   with (min reduce minBucketSize,
                                         max reduce maxBucketSize,
                                         + reduce sumBucketSizes,
                                         + reduce countBucketsConsidered,
                                         min reduce minCommon,
                                         max reduce maxCommon,
                                         + reduce sumCommon,
                                         + reduce countBucketsWithCommon,
                                         + reduce partitionTime,
                                         + reduce lookupTime,
                                         + reduce sortEachNonsampleTime,
                                         + reduce mergeTime) {
      const bucketStart = Ends[bucketIdx] - bucketSize;
      const bucketEnd = bucketStart + bucketSize - 1;
      const ref MySampleSplitters = localSplitter(SampleSplitters,
                                                  ReplSampleSplitters);

      if bucketSize > 1 && !MySampleSplitters.bucketHasEqualityBound(bucketIdx)
      {
        // note statistics
        minBucketSize reduce= bucketSize;
        maxBucketSize reduce= bucketSize;
        sumBucketSizes += bucketSize;
        countBucketsConsidered += 1;

        var myPartitionTime = 0.0;
        var myLookupTime = 0.0;
        var mySortEachNonsampleTime = 0.0;
        var myMergeTime = 0.0;

        //const ref localText = getLocalReplicand(RepTheText, cfg.locales);
        //const ref localRanks = getLocalReplicand(RepSampleRanks, cfg.locales);

        if MySampleSplitters.bucketHasLowerBound(bucketIdx) &&
           MySampleSplitters.bucketHasUpperBound(bucketIdx) {

          const ref lowerBound = MySampleSplitters.bucketLowerBound(bucketIdx);
          const ref upperBound = MySampleSplitters.bucketUpperBound(bucketIdx);
          // compute the number of characters in common between lowerBound and
          // upperBound.
          const nCharsCommon = charactersInCommon(cfg, lowerBound, upperBound);

          // note statistics
          minCommon reduce= nCharsCommon;
          maxCommon reduce= nCharsCommon;
          sumCommon += nCharsCommon;
          countBucketsWithCommon += 1;
        }

        var localSA: [bucketStart..bucketEnd] SA.eltType;
        localSA = SA[bucketStart..bucketEnd];

        local {
        sortSuffixesCompletely(cfg, thetext, n=n,
                               SampleText, charsPerMod,
                               localSA, bucketStart..bucketEnd,
                               myPartitionTime, myLookupTime,
                               mySortEachNonsampleTime, myMergeTime);
        }

        SA[bucketStart..bucketEnd] = localSA;

        partitionTime += myPartitionTime;
        lookupTime += myLookupTime;
        sortEachNonsampleTime += mySortEachNonsampleTime;
        mergeTime += myMergeTime;
      }
    }

    assert(Ends.last == n);

    if TIMING {
      sortBuckets.stop();
      writeln("sortBuckets in ", sortBuckets.elapsed(), " s");
      writeln(" and inside that (adding times from all tasks)");
      writeln(" partitionTime ", partitionTime, " s");
      writeln(" lookupTime ", lookupTime, " s");
      writeln(" sortEachNonsampleTime ", sortEachNonsampleTime, " s");
      writeln(" mergeTime ", mergeTime, " s");
    }

    if TRACE {
      writeln(" bucket size statistics for final sort",
              " n=", countBucketsConsidered,
              " min=", minBucketSize,
              " avg=", sumBucketSizes:real / countBucketsConsidered,
              " max=", maxBucketSize);
      writeln(" bucket common prefix statistics for final sort",
              " n=", countBucketsWithCommon,
              " min=", minCommon,
              " max=", maxCommon,
              " avg=", sumCommon:real / countBucketsWithCommon);
    }

    //writeln("returning SA ", SA);

    // create a suffix array just from the offsets and return that
    const SAOffsets: [resultDom] cfg.offsetType =
      forall elt in SA do offset(elt);
    return SAOffsets;
  }
}

/* Compute and return the LCP array based on the input text and suffix array.
   This is based upon "Fast Parallel Computation of Longest Common Prefixes"
   by Julian Shun.

 */
proc lcpParPlcp(thetext: [], const n: thetext.domain.idxType, const SA: []) {
  const nTasks = computeNumTasks();
  type offsetType = (offset(SA[0])).type;

  var PLCP: [SA.domain] offsetType;
  {
    var PHI: [SA.domain] offsetType;

    PHI[offset(SA[0])] = -1;
    forall i in 1..<n {
      PHI[offset(SA[i])] = offset(SA[i-1]);
    }

    const blockSize = divCeil(n, nTasks);
    coforall j in 0..<nTasks {
      var taskStart = j * blockSize;
      var h = 0;
      for i in taskStart..#blockSize {
        if i >= n {
          break;
        }

        if PHI[i] == -1 {
          h = 0;
        } else {
          var k = PHI[i];
          while i+h < n && k+h < n && thetext[i+h] == thetext[k+h] {
            h += 1;
          }
        }
        PLCP[i] = h;
        h = max(h-1, 0);
      }
    }

    // deallocate PHI as it is no longer needed
  }

  var LCP: [SA.domain] offsetType;
  forall i in 0..<n {
    LCP[i] = PLCP[offset(SA[i])];
  }

  return LCP;
}

/* Compute and return the sparse PLCP array based on the input text and suffix
   array. This is based upon "Permuted Longest-Common-Prefix Array" by Juha
   Kärkkäinen, Giovanni Manzini, and Simon J. Puglisi; and also
   "Fast Parallel Computation of Longest Common Prefixes"
   by Julian Shun.

 */
proc doComputeSparsePLCP(thetext: [], const n: thetext.domain.idxType,
                         const SA: [], param q) {
  // TODO: get this distributed
  //  - PHI can be block distributed
  //  - the coforall loop can be coforall + on PHI[taskStart]
  const nTasks = computeNumTasks();
  type offsetType = (offset(SA[0])).type;

  const nSample = myDivCeil(n, q);
  var PLCP: [0..<nSample] offsetType;
  {
    //writeln("computeSparsePLCP(q=", q, ")");

    var PHI: [0..<nSample] offsetType;
    forall i in 0..<n {
      const sai = offset(SA[i]);
      if sai % q == 0 {
        const prev = if i > 0 then offset(SA[i-1]) else -1;
        PHI[sai/q] = prev;
      }
    }

    //writeln("PHI ", PHI);

    const blockSize = divCeil(nSample, nTasks);
    coforall tid in 0..<nTasks {
      var taskStart = tid * blockSize;
      var h = 0; // called l in "Permuted Longest-Common-Prefix Array" paper
      for i in taskStart..#blockSize {
        if i >= nSample {
          break;
        }

        var j = PHI[i];
        if j == -1 {
          h = 0;
        } else {
          while i*q+h < n && j+h < n && thetext[i*q+h] == thetext[j+h] {
            h += 1;
          }
        }
        PLCP[i] = h;
        h = max(h-q, 0);
      }
    }

    // deallocate PHI as it is no longer needed
  }

  //writeln("Computed sparse PLCP (q=", q, ") ", PLCP);
  return PLCP;
}

/* Given a sparse PLCP array computed as above in computeSparsePLCP,
   along with the parameter q and a suffix array position 'i', return
   LCP[i]. */
proc doLookupLCP(thetext: [], const n: thetext.domain.idxType, const SA: [],
                 const sparsePLCP: [], i: n.type, param q) {

  //writeln("lookupLCP i=", i, " q=", q, " sparsePLCP=", sparsePLCP);

  // handle i==0 here since otherwise we couldn't access SA[i-1] below.
  if i == 0 {
    return 0;
  }

  // if it's a sampled offset, we can return PLCP directly.
  const sai = offset(SA[i]);
  if sai % q == 0 { // i.e., SA[i] = qk
    return sparsePLCP[sai / q]; // i.e., PLCPq[k]
  }

  // otherwise, let aq + b = SA[i]
  const a = sai / q;
  const b = sai % q;

  var lower: int; // where to start comparing
  var upper: int; // where to stop comparing (inclusive)

  // Lemma 2 from "Permuted Longest-Common-Prefix Array":
  //   if (a+1)q <= n-1, PLCPq[a] - b <= PLCP[x] <= PLCPq[a+1] + q - b
  //   else PLCPq[a] - b <= PLCP[x] <= n - x <= q

  if (a + 1) * q <= n - 1 {
    lower = sparsePLCP[a] - b;
    upper = sparsePLCP[a+1] + q - b;
  } else {
    lower = sparsePLCP[a] - b;
    upper = n - sai;
  }

  const saPrev = offset(SA[i-1]);

  lower = max(lower, 0);        // can't have a negative number in common
  upper = min(upper, n-saPrev); // common can't reach past end of string

  //writeln("lower=", lower, " upper=", upper);

  // we know lower <= PLCP[i] <= upper
  // compute the rest of PLCP[i] by comparing suffixes SA[i] and SA[i-1].
  var common = lower;
  while common < upper {
    //writeln("in loop, common is ", common);
    //writeln("comparing ", sai+common, " vs ", saPrev+common);
    if thetext[sai + common] == thetext[saPrev + common] {
      common += 1;
    } else {
      break;
    }
  }

  //writeln("returning ", common);

  return common;
}


}
