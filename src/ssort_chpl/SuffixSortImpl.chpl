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
import CTypes.{c_sizeof,c_array};
import Time;
import CopyAggregation.{SrcAggregator,DstAggregator};

import SuffixSort.DEFAULT_PERIOD;
import SuffixSort.EXTRA_CHECKS;
import SuffixSort.TRACE;
import SuffixSort.TIMING;
import SuffixSort.STATS;
import SuffixSort.INPUT_PADDING;

config const logBucketsSerial = 8;
config const minBucketsPerTask = 8;
config const minBucketsSpace = 2_000_000; // a size in bytes
config const simpleSortLimit = 1000; // for sizes >= this,
                                     // use radix sort + multi-way merge
config const finalSortPasses = 8;

// upper-case names for the config constants to better identify them in code
const MIN_BUCKETS_PER_TASK = minBucketsPerTask;
const MIN_BUCKETS_SPACE = minBucketsSpace;
const SIMPLE_SORT_LIMIT = simpleSortLimit;
const FINAL_SORT_NUM_PASSES = finalSortPasses;
const LOG_BUCKETS_SERIAL = logBucketsSerial;

config param RADIX_BITS = 8;
config param BIG_RADIX_BITS = 16;

/**
 This record contains the configuration for the suffix sorting
 problem or subproblem. It's just a record to bundle up the generic
 information.

 It doesn't contain the 'text' input array because Chapel currently
 doesn't support reference fields.
 */
record ssortConfig {
  // these should all be integral types:

  type idxType=int;    // for accessing 'text'; should be text.domain.idxType

  type offsetType;     // type for storing offsets

  type unsignedOffsetType = uint(numBits(offsetType));
                     // use this for sample ranks

  type loadWordType = unsignedOffsetType;
                     // load this much text data when doing comparisons
                     // or when sorting.

  // this is param to support prefix records having known size
  param bitsPerChar: int; // number of bits occupied by each packed character

  const n: idxType; // number of characters, not counting padding

  const nBits: idxType = n*bitsPerChar; // number of bits of data, no padding

  const cover: differenceCover(?);

  const locales; // an array of locales to use

  const nTasksPerLocale: int;

  // these are implementation details & can be overridden for testing
  const finalSortNumPasses: int = FINAL_SORT_NUM_PASSES;
  const finalSortSimpleSortLimit: int = SIMPLE_SORT_LIMIT;
  const minBucketsPerTask: int = MIN_BUCKETS_PER_TASK;
  const minBucketsSpace: int = MIN_BUCKETS_SPACE;
  const logBucketsSerial: int = LOG_BUCKETS_SERIAL;
  const assumeNonlocal: bool = false;
}

record statistics {
  var nRandomTextReads: int;
  var nRandomRanksReads: int;
};

operator +(x: statistics, y: statistics) {
  var ret: statistics;
  if STATS {
    ret.nRandomTextReads = x.nRandomTextReads + y.nRandomTextReads;
    ret.nRandomRanksReads = x.nRandomRanksReads + y.nRandomRanksReads;
  }
  return ret;
}

/**
  This record helps to avoid indirect access at the expense of using
  more memory. Here we store together an offset for the suffix array
  along with some of the data that is present at that offset.
  */
record offsetAndCached : writeSerializable {
  type offsetType;
  type cacheType; // should be cfg.loadWordType

  var offset: offsetType;
  var cached: cacheType;

  // this function is a debugging aid
  proc serialize(writer, ref serializer) throws {
    var ismarked = isMarkedOffset(this);
    var off = unmarkedOffset(this);
    if cacheType == nothing {
      writer.write(off);
    } else {
      writer.writef("%i (%016xu)", off, cached);
    }
    if ismarked {
      writer.write("*");
    }
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
  type wordType; // should be cfg.loadWordType
  param nWords;
  //var words: c_array(wordType, nWords);
  var words: nWords*wordType;
  // it would be a tuple nWords*wordType but that compiles slower

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
  type wordType;   // should be cfg.loadWordType
  type offsetType; // should be cfg.offsetType
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
  This record holds a the next cover period sample ranks.
 */
record sampleRanks : writeSerializable {
  type rankType; // should be cfg.unsignedOffsetType
  param nRanks;

  //var ranks: c_array(rankType, nRanks);
  var ranks: nRanks*rankType;
  // it would be a tuple nRanks*rankType but that compiles slower

  // this function is a debugging aid
  proc serialize(writer, ref serializer) throws {
    for i in 0..<nRanks {
      if i != 0 then writer.write(",");
      writer.write(ranks[i]);
    }
  }
}

record offsetAndSampleRanks : writeSerializable {
  type offsetType; // should be cfg.offsetType
  type rankType; // should be cfg.unsignedOffsetType
  param nRanks;

  var offset: offsetType;
  var r: sampleRanks(rankType, nRanks);

  // this function is a debugging aid
  proc serialize(writer, ref serializer) throws {
    writer.write(offset);
    writer.write("(|");
    for i in 0..<nRanks {
      if i != 0 then writer.write(",");
      writer.write(ranks[i]);
    }
    writer.write(")");
  }
}

/**
  This record holds a prefix and the next cover period sample ranks.
  This is useful for splitters.
 */
record prefixAndSampleRanks : writeSerializable {
  type wordType;   // should be cfg.loadWordType
  type rankType;   // should be cfg.unsignedOffsetType
  type offsetType; // should be cfg.offsetType
  param nWords;
  param nRanks;

  var offset: offsetType;
  var p: prefix(wordType, nWords);
  var r: sampleRanks(rankType, nRanks);

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
      writer.write(r.ranks[i]);
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
  When sorting using 'loadWordType', how many words should
  be considered in order to match 'minChars' characters
  that are handled a 'loadWordType' at a time?
 */
proc ssortConfig.getPrefixWords(param minChars: int) param {
  return myDivCeil(minChars * bitsPerChar, numBits(loadWordType));
}

/**
 Construct an offsetAndCached (or integer) for offset 'i' in the input.
 */
inline proc makeOffsetAndCached(const cfg: ssortConfig(?),
                                offset: cfg.idxType,
                                const PackedText: [] cfg.loadWordType,
                                const n: cfg.idxType,
                                const nBits: cfg.idxType) {
  type wordType = cfg.loadWordType;
  param bitsPerChar = cfg.bitsPerChar;
  const bitIdx = offset*bitsPerChar;

  var cached: wordType = 0;
  if bitsPerChar == numBits(wordType) {
    if offset < n {
      cached = PackedText[offset];
    }
  } else {
    if bitIdx < nBits {
      cached = loadWord(PackedText, bitIdx);
    }
  }

  return new offsetAndCached(offsetType=cfg.offsetType,
                             cacheType=wordType,
                             offset=offset:cfg.offsetType,
                             cached=cached);
}

/**
  Construct an prefix record for offset 'offset' in the input
  by loading the relevant data from 'text'. The prefix stores
  at least k characters.
 */
proc makePrefix(const cfg: ssortConfig(?), offset: cfg.idxType,
                const PackedText: [] cfg.loadWordType,
                const n: cfg.idxType,
                const nBits: cfg.idxType) {
  type wordType = cfg.loadWordType;
  const ref cover = cfg.cover;
  param bitsPerChar = cfg.bitsPerChar;
  param nPrefixWords = cfg.getPrefixWords(cover.period);
  if !isUintType(wordType) {
    compilerError("invalid makePrefix call");
  }

  var result = new prefix(wordType=wordType, nWords=nPrefixWords);
  // fill in the words
  for i in 0..<nPrefixWords {
    const bitIdx = offset*bitsPerChar + i*numBits(wordType);
    var word: wordType = 0;
    if bitsPerChar == numBits(wordType) {
      if offset < n {
        word = PackedText[offset+i];
      }
    } else {
      if bitIdx < nBits {
        word = loadWord(PackedText, bitIdx);
      }
    }
    result.words[i] = word;
  }

  return result;
}

proc makePrefixAndOffset(const cfg: ssortConfig(?),
                         offset: cfg.idxType,
                         const PackedText: [] cfg.loadWordType) {
  type wordType = cfg.loadWordType;
  const ref cover = cfg.cover;
  type prefixType = makePrefix(cfg, offset, PackedText).type;
  param nWords = prefixType.nWords;

  var result = new prefixAndOffset(wordType=wordType,
                                   offsetType=cfg.offsetType,
                                   nWords=nWords,
                                   offset=offset:cfg.offsetType,
                                   p=makePrefix(cfg, offset, PackedText));
  return result;
}


/**
  Construct an sampleRanks record for offset 'offset' in the input
  by loading the relevant data from 'SampleRanks'.
 */
proc makeSampleRanks(const cfg: ssortConfig(?),
                     offset: cfg.idxType,
                     const SampleRanks: [] cfg.unsignedOffsetType) {
  const ref cover = cfg.cover;

  var result = new sampleRanks(rankType=cfg.unsignedOffsetType,
                               nRanks=cover.sampleSize);

  // fill in the ranks
  const start = offsetToSampleRanksOffset(offset, cfg.cover);
  for i in 0..<cover.sampleSize {
    result.ranks[i] = SampleRanks[start+i];
  }

  return result;
}

proc makeOffsetAndSampleRanks(const cfg: ssortConfig(?),
                              offset: cfg.idxType,
                              const SampleRanks: [] cfg.unsignedOffsetType) {
  type sampleRanksType = makeSampleRanks(cfg, offset, SampleRanks).type;

  var result =
    new offsetAndSampleRanks(offsetType=cfg.offsetType,
                             rankType=cfg.unsignedOffsetType,
                             nRanks=sampleRanksType.nRanks,
                             offset=offset,
                             r=makeSampleRanks(cfg, offset, SampleRanks));
  return result;
}



/**
  Construct an prefixAndSampleRanks record for offset 'i' in the input
  by loading the relevant data from 'text' and 'ranks'.
 */
proc makePrefixAndSampleRanks(const cfg: ssortConfig(?),
                              offset: cfg.idxType,
                              const PackedText: [] cfg.loadWordType,
                              const SampleRanks: [] cfg.unsignedOffsetType,
                              const n: cfg.idxType,
                              const nBits: cfg.idxType) {
  const ref cover = cfg.cover;
  // compute the type information for creating a prefix
  type prefixType = makePrefix(cfg, offset, PackedText, n, nBits).type;
  type sampleRanksType = makeSampleRanks(cfg, offset, SampleRanks).type;

  var result =
    new prefixAndSampleRanks(wordType=prefixType.wordType,
                             rankType=sampleRanksType.rankType,
                             offsetType=cfg.offsetType,
                             nWords=prefixType.nWords,
                             nRanks=sampleRanksType.nRanks,
                             offset=offset:cfg.offsetType,
                             p=makePrefix(cfg, offset, PackedText, n, nBits),
                             r=makeSampleRanks(cfg, offset, SampleRanks));

  return result;
}


/**
  Construct an array of suffixes (not yet sorted)
  for all of the offsets in 0..<n.
 */
proc buildAllOffsets(const cfg:ssortConfig(?),
                     resultDom: domain(?)) {
  var SA:[resultDom] cfg.offsetType = resultDom:cfg.offsetType;
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
// offset 'offset' within 'text' by the first 'maxPrefixWords' words
inline proc getKeyPartForOffset(const cfg: ssortConfig(?),
                                const offset: cfg.idxType, i: integral,
                                const PackedText: [] cfg.loadWordType,
                                maxPrefixWords: cfg.idxType) {
  type wordType = cfg.loadWordType;

  if cfg.bitsPerChar == numBits(wordType) {
    const n = cfg.n;
    if i < maxPrefixWords && offset + i < n {
      return (keyPartStatus.returned, PackedText[offset+i]);
    }
    // otherwise, return that we reached the end
    return (keyPartStatus.pre, 0:wordType);
  }

  param bitsPerChar = cfg.bitsPerChar;
  const nBits = cfg.nBits;
  const startBit = offset*bitsPerChar + i*numBits(wordType);

  if i < maxPrefixWords && startBit < nBits {
    // return further data by loading from the text array
    return (keyPartStatus.returned, loadWord(PackedText, startBit));
  }

  // otherwise, return that we reached the end
  return (keyPartStatus.pre, 0:wordType);
}


// can be called from keyPart(offsetAndCached, i)
inline proc getKeyPartForOffsetAndCached(const cfg: ssortConfig(?),
                                         const a: offsetAndCached(?),
                                         i: integral,
                                         const PackedText: [] cfg.loadWordType,
                                         maxPrefixWords: cfg.idxType) {
  if a.cacheType != nothing && cfg.loadWordType == a.cacheType && i == 0 {
    // return the cached data
    return (keyPartStatus.returned, a.cached);
  }

  return getKeyPartForOffset(cfg, a.offset, i, PackedText, maxPrefixWords);
}
inline proc getKeyPartForOffsetAndCached(const cfg: ssortConfig(?),
                                         const a: cfg.idxType,
                                         i: integral,
                                         const PackedText: [] cfg.loadWordType,
                                         maxPrefixWords: cfg.idxType) {
  return getKeyPartForOffset(cfg, a, i, PackedText, maxPrefixWords);
}


// these getPrefixKeyPart overloads call the above to adapt
// to different types.
inline proc getPrefixKeyPart(const cfg: ssortConfig(?),
                             const a: offsetAndCached(?), i: integral,
                             const PackedText: [] cfg.loadWordType,
                             maxPrefixWords: cfg.idxType) {
  cfg.checkWordType(a);
  return getKeyPartForOffsetAndCached(cfg, a, i, PackedText, maxPrefixWords);
}
inline proc getPrefixKeyPart(const cfg: ssortConfig(?),
                             const a: cfg.idxType, i: integral,
                             const PackedText: [] cfg.loadWordType,
                             maxPrefixWords: cfg.idxType) {
  return getKeyPartForOffset(cfg, a, i, PackedText, maxPrefixWords);
}
inline proc getPrefixKeyPart(const cfg:ssortConfig(?),
                             const a: prefix(?), i: integral,
                             const PackedText: [] cfg.loadWordType,
                             maxPrefixWords: cfg.idxType) {
  cfg.checkWordType(a);
  return getKeyPartForPrefix(a, i);
}
inline proc getPrefixKeyPart(const cfg:ssortConfig(?),
                             const a: prefixAndOffset(?), i: integral,
                             const PackedText: [] cfg.loadWordType,
                             maxPrefixWords: cfg.idxType) {
  cfg.checkWordType(a);
  return getKeyPartForPrefix(a, i);
}
inline proc getPrefixKeyPart(const cfg:ssortConfig(?),
                             const a: prefixAndSampleRanks(?), i: integral,
                             const PackedText: [] cfg.loadWordType,
                             maxPrefixWords: cfg.idxType) {
  cfg.checkWordType(a);
  return getKeyPartForPrefix(a, i);
}

inline proc comparePrefixes(const cfg: ssortConfig(?),
                            const a, const b,
                            const PackedText: [] cfg.loadWordType,
                            maxPrefixWords: cfg.idxType): int {
  cfg.checkWordType(a);
  cfg.checkWordType(b);

  var curPart = 0;
  while curPart < maxPrefixWords {
    var (aSection, aPart) = getPrefixKeyPart(cfg, a, curPart,
                                             PackedText, maxPrefixWords);
    var (bSection, bPart) = getPrefixKeyPart(cfg, b, curPart,
                                             PackedText, maxPrefixWords);
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

/*
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
}*/

proc radixSortRegion(ref A: [], comparator, region: range) {

  // no need to sort if there are 0 or 1 elements
  if region.size <= 1 {
    return;
  }

  // Note: 'sort(A, comparator, region)' is conceptually the same as
  // 'sort(A[region], comparator)'; but the slice version might be slower.
  if isDistributedDomain(A.domain) {
    if EXTRA_CHECKS {
      const regionDom: domain(1) = {region,};
      assert(A.domain.localSubdomain().contains(regionDom));
    }
  }

  local {
    if region.size == 2 {
      const i = region.low;
      const j = region.low + 1;
      if mycompare(A[i], A[j], comparator) > 0 {
        A[i] <=> A[j];
      }
      return;
    }

    //sort(A, comparator, region);
    MSBRadixSort.msbRadixSort(A, comparator, region);
  }
}

proc sortRegion(ref A: [], comparator, region: range) {

  // no need to sort if there are 0 or 1 elements
  if region.size <= 1 {
    return;
  }

  // Note: 'sort(A, comparator, region)' is conceptually the same as
  // 'sort(A[region], comparator)'; but the slice version might be slower.
  if isDistributedDomain(A.domain) {
    if EXTRA_CHECKS {
      const regionDom: domain(1) = {region,};
      assert(A.domain.localSubdomain().contains(regionDom));
    }
  }

  local {
    if region.size == 2 {
      const i = region.low;
      const j = region.low + 1;
      if mycompare(A[i], A[j], comparator) > 0 {
        A[i] <=> A[j];
      }
      return;
    }

    sort(A, comparator, region);
  }
}


/* Marks an offset if it was not already marked */
inline proc markOffset(ref elt: offsetAndCached(?)) {
  if elt.offset >= 0 {
    elt.offset = ~elt.offset;
  }
}
inline proc unmarkOffset(ref elt: offsetAndCached(?)) {
  if elt.offset < 0 {
    elt.offset = ~elt.offset;
  }
}

/* Returns true if the offset is marked */
inline proc isMarkedOffset(elt: offsetAndCached(?)) {
  return elt.offset < 0;
}
/* Returns an unmarked offset (but does not remove a mark on 'elt')*/
inline proc unmarkedOffset(elt: offsetAndCached(?)) {
  var ret = elt.offset;
  if ret < 0 {
    ret = ~ret;
  }
  return ret;
}

/* Assuming that A[i] is marked if it differs from A[i-1],
   this iterator yields subranges of 'region' where
   the elements are not yet fully sorted. */
iter unsortedRegionsFromMarks(A:[] offsetAndCached(?), region: range) {
  // find each subregion starting from each marked offset (or region.low)
  // up to but not including the next marked offset
  var cur = region.low;
  const end = region.high+1;
  while cur < end {
    // TODO: this code is probably wrong. Add a test!

    // find the next marked offset
    var next = cur + 1;
    while next < end && !isMarkedOffset(A[next]) {
      next += 1;
    }
    var r = cur..<next;
    if r.size <= 1 {
      // no need to yield since such a region is already sorted
    } else {
      yield r;
    }

    // proceed starting from 'next'
    cur = next;
  }
}

/**
 Loads the next word into A.cached for anything in an equal or unsorted bucket.
 Uses Scratch.cached as temporary storage.

 For all equal buckets, resets them to be unsorted buckets with 0 as startbit.

 Returns the number of equal / unsorted buckets encountered.

 Runs distributed parallel.
 */
proc loadNextWords(const cfg:ssortConfig(?),
                   const PackedText: [] cfg.loadWordType,
                   ref A:[] offsetAndCached(cfg.offsetType,
                                            cfg.loadWordType),
                   ref Scratch:[] offsetAndCached(cfg.offsetType,
                                                  cfg.loadWordType),
                   ref BucketBoundaries:[] uint(8),
                   const region: range,
                   const sortedByBits: int) {
  if region.size == 0 {
    return 0;
  }

  type wordType = cfg.loadWordType;
  param wordBits = numBits(wordType);
  param bitsPerChar = cfg.bitsPerChar;
  const n = cfg.n;
  const nBits = cfg.nBits;
  const nTasksPerLocale = cfg.nTasksPerLocale;


  // update the cached value for anything in an equal bucket
  // change equal buckets to be unsorted buckets
  var nUnsortedBuckets = 0;
  forall (activeLocIdx, taskIdInLoc, taskRegion)
  in divideIntoTasks(A.domain, region, nTasksPerLocale)
  with (in cfg,
        var readAgg = new SrcAggregator(wordType),
        var bktAgg = new DstAggregator(uint(8)),
        + reduce nUnsortedBuckets) {

    var nUnsortedBucketsThisTask = 0;

    for i in taskRegion {
      const bktType = BucketBoundaries[i];
      if !isBaseCaseBoundary(bktType) {
        nUnsortedBucketsThisTask += 1;
        // load it
        if bitsPerChar == wordBits {
          // load directly into 'cached', no need to shift
          const bitOffset = A[i].offset*bitsPerChar + sortedByBits;
          const wordIdx = bitOffset / wordBits; // divides evenly in this case
          if bitOffset < nBits {
            readAgg.copy(A[i].cached, PackedText[wordIdx]);
          } else {
            A[i].cached = 0; // word starts after the end of the string
          }
        } else {
          // load into 'A.cached' and 'Scratch.cached' and then combine
          // these later
          // the next bits might not lie on a word boundary in PackedText
          const bitOffset = A[i].offset*bitsPerChar + sortedByBits;
          const wordIdx = bitOffset / wordBits;
          const shift = bitOffset % wordBits;
          if bitOffset < nBits {
            readAgg.copy(A[i].cached, PackedText[wordIdx]);
          } else {
            A[i].cached = 0; // word starts after the end of the string
          }
          // also load the next word if it will be needed
          if shift != 0 {
            if bitOffset + wordBits < nBits {
              // load an additional word to 'Scratch.cached'
              // stats don't count this one assuming it comes from prev
              readAgg.copy(Scratch.cached[i], PackedText[wordIdx + 1]);
            } else {
              Scratch.cached[i] = 0; // next word starts after end
            }
          }
        }
      }
    }

    if nUnsortedBucketsThisTask > 0 {
      nUnsortedBuckets += nUnsortedBucketsThisTask;

      readAgg.flush(); // since we use the results below

      // combine the two words as needed
      for i in taskRegion {
        const bktType = BucketBoundaries[i];
        if !isBaseCaseBoundary(bktType) {

          if isBucketBoundary(bktType) {
            var boundaryType: uint(8);
            var bktSize: int;
            var bktStartBit: int;
            readBucketBoundary(BucketBoundaries, region, i,
                               /*out*/ boundaryType, bktSize, bktStartBit);

            // reset the bucket boundary (so it will be sorted anew)
            setBucketBoundary(BucketBoundaries, boundaryTypeUnsortedBucketInA,
                              i, bktSize, bktStartBit=0, bktAgg);
          }
          const b = A[i].offset*bitsPerChar + sortedByBits;
          A[i].cached = loadWordWithWords(A[i].cached, Scratch[i].cached, b);
        }
      }
    }
  }
}

/**
  Sort suffixes in A[region] by the first maxPrefix character values.
  Assumes that A[i].offset and A[i].cached are already set up,
  where A[i].cached should be the first word of character data,
  and that A is not yet sorted by 'cached'.

  Bkts can be passed with size > 1 if A is already partitioned by prefix.
  In that case, 'SplitForBkts' should also be passed.

  Leaves partially sorted suffixes in A and stores the bucket boundaries
  in BucketBoundaries.

  This is a distributed, parallel operation.
 */
proc sortByPrefixAndMark(const cfg:ssortConfig(?),
                         const PackedText: [] cfg.loadWordType,
                         const SplitForBkts,
                         const ref Bkts: [] bktCount,
                         ref A:[] offsetAndCached(cfg.offsetType,
                                                  cfg.loadWordType),
                         ref Scratch:[] offsetAndCached(cfg.offsetType,
                                                        cfg.loadWordType),
                         ref BucketBoundaries:[] uint(8),
                         region: range,
                         /*ref readAgg: SrcAggregator(cfg.loadWordType),*/
                         maxPrefix: cfg.idxType
                         /*ref stats: statistics*/) {

  if region.size == 0 {
    return;
  }

  type wordType = cfg.loadWordType;
  param wordBits = numBits(wordType);
  param bitsPerChar = cfg.bitsPerChar;
  const n = cfg.n;
  const nBits = cfg.nBits;
  const nTasksPerLocale = cfg.nTasksPerLocale;

  // to help sort by 'cached'
  record byCached1 : keyComparator {
    proc key(elt) { return elt.cached; }
  }

  // Sort A by cached
  if Bkts.size > 1 && SplitForBkts.type != nothing {
    const sorter =
      new partitioningSorter(eltType=A.eltType,
                             splitterType=radixSplitters(RADIX_BITS),
                             radixBits=RADIX_BITS,
                             logBuckets=RADIX_BITS,
                             nTasksPerLocale=nTasksPerLocale,
                             endbit=wordBits,
                             markAllEquals=true,
                             useExistingBuckets=true);

    // mark the boundaries from the existing partition
    sorter.markBoundaries(BucketBoundaries, SplitForBkts, Bkts,
                          nowInA=true, nextbit=0);

    // sort the rest of the way
    sorter.psort(A, Scratch, BucketBoundaries, region, new byCached1());
  } else {
    const sorter =
      new partitioningSorter(eltType=A.eltType,
                             splitterType=radixSplitters(RADIX_BITS),
                             radixBits=RADIX_BITS,
                             logBuckets=RADIX_BITS,
                             nTasksPerLocale=nTasksPerLocale,
                             endbit=wordBits,
                             markAllEquals=true,
                             useExistingBuckets=false);

    // sort the rest of the way
    sorter.psort(A, Scratch, BucketBoundaries, region, new byCached1());
  }

  // now the data is in A sorted by cached, and BucketBoundaries
  // indicates which buckets are so far equal

  var sortedByBits = wordBits;
  const prefixBits = maxPrefix*bitsPerChar;
  while sortedByBits < prefixBits {
    /*writeln("in sortByPrefixAndMark sorted by ", sortedByBits, " for ", region);
    for i in region {
      writeln("A[", i, "] = ", A[i]);
    }*/

    // update the cached value for anything in an equal bucket
    // change equal buckets to be unsorted buckets
    var nUnsortedBuckets = loadNextWords(cfg, PackedText, A, Scratch,
                                         BucketBoundaries, region,
                                         sortedByBits);

    // stop if there were no unsorted regions
    if nUnsortedBuckets == 0 {
      break;
    }

    // sort by 'cached' again, while respecting existing bucket boundaries
    const sorter =
      new partitioningSorter(eltType=A.eltType,
                             splitterType=radixSplitters(RADIX_BITS),
                             radixBits=RADIX_BITS,
                             logBuckets=RADIX_BITS,
                             nTasksPerLocale=nTasksPerLocale,
                             endbit=wordBits,
                             markAllEquals=true,
                             useExistingBuckets=true);
    sorter.psort(A, Scratch, BucketBoundaries, region, new byCached1());

    // now we have sorted by an additional word
    sortedByBits += wordBits;
  }
}


/* If we computed the suffix array for PackedText
   there is some ambiguity between 0s due to end-of-string/padding
   vs 0s due to the input. This function resolves the issue
   by adjusting the first several suffix array entries.

   This fix does not need to apply to suffix sorting done with
   a recursive subproblem (rather than with the base case)
   as compareSampleRanks will cover it with compareEndOfString.
 */
proc fixTrailingZeros(const cfg:ssortConfig(?),
                      const PackedText: [] cfg.loadWordType,
                      n:integral,
                      ref A: []) {

  // We use 0s to indicate padding which can happen at the end of
  // the string. If the input also ended with 0s, then we need to
  // re-sort the suffixes at the end of the string. Since they
  // all end in zero, we know that the suffix array order here
  // is the offsets in descending order.

  var firstNonZero = -1;
  // loop starting at the end of the string, stop when we hit a nonzero
  for i in 0..<n by -1 {
    if loadWord(PackedText, i*cfg.bitsPerChar) != 0 {
      firstNonZero = i;
      break;
    }
  }
  var firstZero = firstNonZero+1;
  var nZero = n-firstZero;

  forall i in 0..<nZero {
    const off = n-1-i;
    if isIntegralType(A.eltType) {
      A[i] = off: cfg.offsetType;
    } else {
      A[i].offset = off : cfg.offsetType;
    }
  }
}

/**
  Create a suffix array for the suffixes 0..<n for 'text'
  by sorting the data at those suffixes directly.

  This is useful as a base case, but shouldn't be used generally
  for creating a suffix array as it is O(n**2).

  Return an array representing this suffix array.
  */
proc computeSuffixArrayDirectly(const cfg:ssortConfig(?),
                                const PackedText: [] cfg.loadWordType,
                                resultDom: domain(?)) {

  if isDistributedDomain(resultDom) || isDistributedDomain(PackedText.domain) {
    // When directly computing the suffix array on a distributed array,
    // move everything local first and then copy back to the result array.
    //
    // This avoids the need for a distributed sort and should be
    // sufficient for the base case.

    // This could just be = resultDom but this way of writing avoids a warning.
    const LocalDom: domain(1) = {resultDom.dim(0),};
    const LocalTextDom: domain(1) = {PackedText.dim(0),};
    const LocalPackedText: [LocalTextDom] cfg.loadWordType = PackedText;

    var LocalA = computeSuffixArrayDirectly(cfg, LocalPackedText, LocalDom);

    const A: [resultDom] cfg.offsetType = LocalA;
    return A;
  }

  const n = cfg.n;

  // First, construct the offsetAndCached array that will be sorted.
  var A = buildAllOffsets(cfg, resultDom);

  record directComparator : keyPartComparator {
    proc keyPart(a, i: int) {
      return getPrefixKeyPart(cfg, a, i, PackedText,
                              maxPrefixWords=max(cfg.offsetType));
    }
  }

  radixSortRegion(A, new directComparator(), 0..<n);

  fixTrailingZeros(cfg, PackedText, n, A);

  return A;
}

/**
  Construct an array of suffixes (not yet sorted)
  for only those offsets in 0..<n that are also in the difference cover.
 */
proc buildSampleOffsets(const cfg: ssortConfig(?),
                        const PackedText: [] cfg.loadWordType,
                        sampleN: cfg.idxType) {
  type offsetType = cfg.offsetType;
  const n = cfg.n;
  const cover = cfg.cover;
  const nPeriods = myDivCeil(n, cover.period); // nPeriods * period >= n
  assert(sampleN == cover.sampleSize * nPeriods);

  const Dom = makeBlockDomain(0..<sampleN, targetLocales=cfg.locales);
  var SA:[Dom] offsetType =
    forall i in Dom do sampleRankIndexToOffset(i, cover);

  return SA;
}

proc setName(const cfg:ssortConfig(?),
             bktStart: int,
             i: int,
             charsPerMod: cfg.idxType,
             const ref Sample: [] offsetAndCached(cfg.offsetType,
                                                  cfg.loadWordType),
             ref SampleNames:[] cfg.unsignedOffsetType,
             ref writeAgg: DstAggregator(cfg.unsignedOffsetType)) {
  const off = Sample[i].offset;

  // offset is an unpacked offset. find the offset in
  // the recursive problem input to store the rank into.
  // Do so in a way that arranges for SampleText to consist of
  // all sample inputs at a particular mod, followed by other modulus.
  // We have charsPerMod characters for each mod in the cover.
  const useIdx = offsetToSubproblemOffset(off, cfg.cover, charsPerMod);

  const useName = (bktStart+1):cfg.unsignedOffsetType;
  writeAgg.copy(SampleNames[useIdx], useName);
}

/* Returns an array of the sample offsets sorted
   by at least the first cover.period characters.

   Works in parallel and disttributed.

   The returned array is Block distributed over cfg.locales if CHPL_COMM!=none.
 */
proc sortAndNameSampleOffsets(const cfg:ssortConfig(?),
                              const PackedText: [] cfg.loadWordType,
                              const requestedNumBuckets: int,
                              ref SampleNames: [] cfg.unsignedOffsetType,
                              charsPerMod: cfg.idxType,
                              ref stats: statistics) {
  const n = cfg.n;
  const nBits = cfg.nBits;
  const nWords = cfg.nBits / numBits(cfg.loadWordType);
  const cover = cfg.cover;
  const nTasksPerLocale = cfg.nTasksPerLocale;
  const nPeriods = myDivCeil(n, cover.period); // nPeriods * period >= n
  const sampleN = cover.sampleSize * nPeriods;
  var nToSampleForSplitters = (SAMPLE_RATIO*requestedNumBuckets):int;

  type offsetType = cfg.offsetType;
  type wordType = cfg.loadWordType;
  param prefixWords = cfg.getPrefixWords(cover.period);
  type prefixType = makePrefix(cfg, 0, PackedText, n, nBits).type;

  record myPrefixComparator3 : keyPartComparator {
    proc keyPart(a: offsetAndCached(?), i: int) {
      return getKeyPartForOffsetAndCached(cfg, a, i,
                                          PackedText,
                                          maxPrefixWords=prefixWords);
    }
    proc keyPart(a: integral, i: int) {
      return getKeyPartForOffset(cfg, a, i,
                                 PackedText,
                                 maxPrefixWords=prefixWords);
    }
    proc keyPart(a: prefixAndOffset(?), i: int) {
      return getKeyPartForPrefix(a, i);
    }
    proc keyPart(a: prefix(?), i: int) {
      return getKeyPartForPrefix(a, i);
    }
  }

  record inputProducer1 {
    proc eltType type do return offsetAndCached(offsetType, wordType);
    proc this(i: cfg.idxType) {
      return makeOffsetAndCached(cfg,
                                 sampleRankIndexToOffset(i, cover),
                                 PackedText, n, nBits);
    }
  }

  record sampleProducer1 {
    proc eltType type do return prefixType;
    proc this(i: cfg.idxType) {
      // produces prefix records based on PackedText
      // without worrying about sample vs non-sample or even
      // possibly periodic data patterns
      var ret: prefixType;
      for j in 0..<prefixWords {
        if i + j < nWords {
          ret.words[j] = PackedText[i+j];
        }
      }
      return ret;
    }
  }

  const comparator = new myPrefixComparator3();
  const InputProducer = new inputProducer1();
  const SampleProducer = new sampleProducer1();

  // first, create a sorting sample of offsets in the cover
  const sp = createSampleSplitters(PackedText.domain,
                                   SampleProducer,
                                   0..<nWords,
                                   comparator,
                                   activeLocs=cfg.locales,
                                   nTasksPerLocale=nTasksPerLocale,
                                   logBuckets=log2int(requestedNumBuckets));

  const SampleDom = makeBlockDomain(0..<sampleN,
                                    targetLocales=cfg.locales);

  var Sample: [SampleDom] offsetAndCached(offsetType, wordType);
  var Scratch: [SampleDom] offsetAndCached(offsetType, wordType);
  var BucketBoundaries: [SampleDom] uint(8);

  // Now, count & partition by the prefix by traversing over the input.
  // This uses full-length partitioning splitters (because this initial
  // read can efficiently read prefixes of the data without random access)
  const Bkts = partition(SampleDom, 0..<sampleN, InputProducer,
                         OutputShift=none, Output=Sample,
                         sp, comparator, nTasksPerLocale,
                         activeLocs=cfg.locales);

  // Mark the bucket boundaries and sort the rest of the way by 'cached'
  sortByPrefixAndMark(cfg, PackedText, Bkts, sp,
                      Sample, Scratch, BucketBoundaries,
                      0..<sampleN,
                      nowInA=true, maxPrefix=cover.period);

  // give each sample position a "name" that is just the offset
  // where its bucket starts
  forall (activeLocIdx, taskIdInLoc, taskRegion)
  in divideIntoTasks(Scratch.domain, 0..<sampleN, nTasksPerLocale, cfg.locales)
  with (in cfg,
        var writeAgg = new DstAggregator(SampleNames.eltType),
        const locRegion = Scratch.domain.localSubdomain().dim(0)) {
    // find buckets that start in taskRegion
    var cur = taskRegion.low;
    var end = taskRegion.high+1;
    while cur < end {
      const bktStart = cur;
      var bktType: uint(8);
      var bkt = nextBucket(BucketBoundaries, taskRegion, 0..<sampleN, cur,
                           /*out*/ bktType);
      if bkt.size <= 0 {
        // nothing to do
      } else if bkt.size == 1 {
        // this is a common case
        setName(cfg, bktStart, bktStart, charsPerMod,
                Sample, SampleNames, writeAgg);
      } else if bkt.size > 1 {
        // compute the local portion and the nonlocal portion
        const localPart = bkt[locRegion];
        const otherPart = bkt[localPart.high+1..];
        for i in localPart {
          setName(cfg, bktStart, i, charsPerMod,
                  Sample, SampleNames, writeAgg);
        }
        if otherPart.size > 0 {
          forall (activeLocIdx, taskIdInLoc, chunk)
          in divideIntoTasks(Sample.dom, otherPart, nTasksPerLocale) {
            for i in chunk {
              setName(cfg, bktStart, i, charsPerMod,
                      Sample, SampleNames, writeAgg);
            }
          }
        }
      }
    }
  }
}

/* Sort suffixes in a region by the sample ranks.
   The assumption is that all suffixes in 'region' are already
   sorted by the same cover.period characters. This function
   sorts these by the sample ranks to put them in final order.

   Sorts only A[region].

   Assumes that the relevant sample ranks have already been loaded
   into LoadedSampleRanks and that for each element in A,
   elt.cached is the index into LoadedSampleRanks of the sample ranks
   for elt.offset.

   This function is serial and local.
   TODO: make a version of it that can start by partitioning
         & so can run in parallel.
 */
proc linearSortOffsetsInRegionBySampleRanksSerial(
                            const cfg:ssortConfig(?),
                            ref A: [] offsetAndSampleRanks(?),
                            ref Scratch: [] offsetAndSampleRanks(?),
                            region: range) {

  //writeln("in sortOffsetsInRegionBySampleRanks ", region, " size=", region.size);

  const cover = cfg.cover;
  const n = cfg.n;
  const finalSortSimpleSortLimit = cfg.finalSortSimpleSortLimit;

  // the comparator to sort by sample ranks
  record finalComparator : relativeComparator {
    proc compare(a: prefixAndSampleRanks(?), b: prefixAndSampleRanks(?)) {
      return compareLoadedSampleRanks(a, b, a.r, b.r, n, cover);
    }
  }

  writeln("in sortOffsetsInRegionBySampleRanks running v-way merge", " for size=", region.size);

  writeln("A.domain is ", A.domain, " region is ", region, " A.locales is ",
      A.targetLocales());

  var maxDistanceTmp = 0;
  for i in 0..<cover.period {
    maxDistanceTmp = max(maxDistanceTmp, cover.nextCoverIndex(i));
  }
  const nDistanceToSampleBuckets = maxDistanceTmp+1;

  // Help to distribute into sub-buckets where each sub-bucket
  // has the same distance to a sample suffix.
  // Then each sub-bucket can be radix sorted by sample suffix rank
  // (with the next comparator).
  record distanceToSampleSplitter {
    proc numBuckets {
      return nDistanceToSampleBuckets;
    }
    iter classify(Input, start_n, end_n, comparator) {
      foreach i in start_n..end_n {
        const elt = Input[i];
        const off = unmarkedOffset(elt);
        const j = cover.nextCoverIndex(off % cover.period);
        yield (elt, j);
      }
    }
  }

  // This comparator helps to sort suffixes that all have the same
  // distance to a sample suffix.
  // Sample suffixes always have distance 0 to sample suffixes.
  // Other suffixes have a distance according to their phase.
  record fixedDistanceToSampleComparator : keyComparator {
    const j: int; // offset + j will be in the cover

    proc key(a: offsetAndCached(?)) {
      const off = unmarkedOffset(a);
      if EXTRA_CHECKS {
        assert(cover.containedInCover((off + j) % cover.period));
      }
      const idx = sampleRankIndex(off, j, cover);
      const ref ranks = LoadedSampleRanks[a.cached:int];
      return ranks.ranks[idx];
    }
  }

  // partition by the distance to a sample suffix
  const Counts = partition(A.domain, region, A,
                           OutputShift=region.low, Output=Scratch,
                           split=new distanceToSampleSplitter(),
                           comparator=new finalComparator(),
                           nTasksPerLocale=1);

  if isDistributedDomain(Counts.domain) then
    compilerError("Was not expecting it to be distributed");

  const Ends = + scan Counts;

  assert(Ends.last == region.size);

  var nNonEmptyBuckets = 0;

  // radix sort each sub-bucket within each partition
  for bucketIdx in 0..<nDistanceToSampleBuckets {
    const bucketSize = Counts[bucketIdx];
    const bucketStart = region.low + Ends[bucketIdx] - bucketSize;
    const bucketEnd = bucketStart + bucketSize - 1; // inclusive

    if bucketSize > 1 {
      const k = bucketIdx; // offset + k will be in the cover
      if EXTRA_CHECKS {
        for i in bucketStart..bucketEnd {
          const off = unmarkedOffset(B[i]);
          assert(cover.containedInCover((off + k) % cover.period));
        }
      }

      // sort by the sample at offset + k
      radixSortRegion(Scratch, new fixedDistanceToSampleComparator(k),
                      bucketStart..bucketEnd);
    }

    if bucketSize > 0 {
      nNonEmptyBuckets += 1;
    }
  }

  // Gather the ranges for input to multiWayMerge
  var InputRanges: [0..<nNonEmptyBuckets] range;
  var cur = 0;
  for bucketIdx in 0..<nDistanceToSampleBuckets {
    const bucketSize = Counts[bucketIdx];
    const bucketStart = region.low + Ends[bucketIdx] - bucketSize;
    const bucketEnd = bucketStart + bucketSize - 1; // inclusive

    if bucketSize > 0 {
      InputRanges[cur] = bucketStart..bucketEnd;
      cur += 1;
    }
  }

  // do the serial multi-way merging from B back into A
  multiWayMerge(Scratch, InputRanges, A, region, new finalComparator());
}

/* Sort the offsetAndSampleRanks values in A
   Copy the resulting offsets back to SA[saStart..]
 */
proc linearSortOffsetsInRegionBySampleRanks(
                            const cfg:ssortConfig(?),
                            ref A: [] offsetAndSampleRanks(?),
                            ref Scratch: [] offsetAndSampleRanks(?),
                            region: range,
                            ref SA: [],
                            saStart: int) {
  const n = cfg.n;
  const cover = cfg.cover;
  const nTasksPerLocale = cfg.nTasksPerLocale;
  type offsetType = cfg.offsetType;

  record finalComparator2 : relativeComparator {
    proc compare(a: prefixAndSampleRanks(?), b: prefixAndSampleRanks(?)) {
      return compareLoadedSampleRanks(a, b, a.r, b.r, n, cover);
    }
  }

  const comparator = new finalComparator2();

  // create some splitters
  const activeLocs = computeActiveLocales(A.domain, region);
  const nTasks = activeLocs.size * nTasksPerLocale;
  var requestBuckets = max(cfg.minBucketsPerTask * nTasks,
                           cfg.minBucketsSpace / c_sizeof(A.eltType));
  requestBuckets = min(requestBuckets, region.size / 2);

  const sp = createSampleSplitters(A, region, comparator,
                                   activeLocs=activeLocs,
                                   nTasksPerLocale=nTasksPerLocale,
                                   logBuckets=log2int(requestBuckets));

  // partition from A to Scratch
  const Bkts = partition(A.domain, region, A,
                         OutputShift=region.low, Output=Scratch,
                         sp, comparator, nTasksPerLocale,
                         activeLocs=activeLocs);


  // process each bucket
  forall (bkt, bktIndex, activeLocIdx, taskIdInLoc)
  in divideByBuckets(A, region, Bkts, nTasksPerLocale, activeLocs)
  with (in cfg,
        const locRegion = A.domain.localSubdomain().dim(0),
        var writeAgg = new DstAggregator(offsetType)) {
    if locRegion.contains(bkt) && !cfg.assumeNonLocal {
      // sort it
      linearSortOffsetsInRegionBySampleRanksSerial(cfg, A, Scratch, bkt);
      // copy sorted values back to SA
      for i in bkt {
        const off = A[i].offset;
        writeAgg.copy(SA[saStart+i], off);
      }
    } else {
      var LocA:[bkt] A.eltType;
      var LocScratch:[bkt] A.eltType;
      // copy to local temp
      TmpA[bkt] = SampleRanksA[bkt];
      // sort it
      linearSortOffsetsInRegionBySampleRanksSerial(cfg, LocA, LocScratch, bkt);
      // copy sorted values back to SA
      for i in bkt {
        const off = LocA[i].offset;
        writeAgg.copy(SA[saStart+i], off);
      }
    }
  }
}


/* Sorts offsets in a region using a difference cover sample.
   Assumes that A[i].offset and A[i].cached are set up and contain
   the offset and first word of data for each suffix.

   This is distributed & parallel.

   Updates the suffix array SA with the result.
 */
proc sortAllOffsetsInRegion(const cfg:ssortConfig(?),
                            const PackedText: [] cfg.loadWordType,
                            const SampleRanks: [] cfg.unsignedOffsetType,
                            ref A: [] offsetAndCached(cfg.offsetType,
                                                            cfg.loadWordType),
                            ref Scratch: [] offsetAndCached(cfg.offsetType,
                                                            cfg.loadWordType),
                            ref SampleRanksA: [] offsetAndSampleRanks(?),
                            ref SampleRanksScratch: [] offsetAndSampleRanks(?),
                            ref BucketBoundaries: [] uint(8),
                            region: range,
                            ref SA: [],
                            const saStart: cfg.idxType
                            /*ref readAgg: SrcAggregator(cfg.loadWordType),
                            ref writeAgg: DstAggregator(cfg.offsetType),
                            ref stats: statistics*/) {
  if region.size <= 1 {
    return;
  }

  const cover = cfg.cover;
  const n = cfg.n;
  const nTasksPerLocale = cfg.nTasksPerLocale;
  const finalSortSimpleSortLimit = cfg.finalSortSimpleSortLimit;

  type wordType = cfg.loadWordType;
  type unsignedOffsetType = cfg.unsignedOffsetType;
  type sampleRanksType = makeSampleRanks(cfg, 0, SampleRanks).type;
  type rankType = sampleRanksType.rankType;
  type offsetType = cfg.offsetType;

  record byCached1 : keyComparator {
    proc key(elt) { return elt.cached; }
  }

  record finalComparator1 : relativeComparator {
    proc compare(a: prefixAndSampleRanks(?), b: prefixAndSampleRanks(?)) {
      return compareLoadedSampleRanks(a, b, a.r, b.r, n, cover);
    }
  }

  var EmptyBkts: [1..0] bktCount;

  sortByPrefixAndMark(cfg, PackedText, EmptyBkts, none,
                      A, Scratch, BucketBoundaries,
                      region, maxPrefix=cover.period);

  /*writeln("after sortByPrefixAndMark A[", region, "]");
  for i in region {
    writeln("A[", i, "] = ", A[i]);
  }*/

  // Load anything that needs to be sorted by sample ranks into SampleRanksA
  // Reset any bucket boundaries for unsorted regions
  // Store any suffixes ordered by the prefix back to SA
  forall (activeLocIdx, taskIdInLoc, chunk)
  in divideIntoTasks(BucketBoundaries.domain, region, nTasksPerLocale)
  with (var readAgg = new SrcAggregator(rankType),
        var writeAgg = new DstAggregator(offsetType)) {
    for i in chunk {
      const bktType = BucketBoundaries[i];
      if isBaseCaseBoundary(bktType) {
        // copy anything sorted by the prefix back to SA
        const off = A[i].offset;
        writeAgg.copy(SA[saStart+i], off);
      } else {
        // it represents an equality bucket start or value

        if isBucketBoundary(bktType) {
          // change it to an unsorted bucket
          BucketBoundaries[i] = boundaryTypeUnsortedBucketInA;
        }

        // set up the value in SampleRanksA[i]
        const off = A[i].offset;
        SampleRanksA[i].offset = off;
        const start = offsetToSampleRanksOffset(off, cfg.cover);
        for j in 0..<sampleRanksType.nRanks {
          readAgg.copy(SampleRanksA[i].ranks[j],
                       SampleRanks[start+j]);
        }
      }
    }
  }

  // Sort any sample ranks regions
  const s = new partitioningSorter(eltType=A.eltType,
                                   splitterType=splitters(A.eltType),
                                   radixBits=RADIX_BITS,
                                   logBuckets=RADIX_BITS,
                                   nTasksPerLocale=nTasksPerLocale,
                                   endbit=0,
                                   markAllEquals=false,
                                   useExistingBuckets=true);

  forall (activeLocIdx, taskIdInLoc, taskRegion)
  in divideIntoTasks(BucketBoundaries.domain, region, nTasksPerLocale)
  with (in s, in cfg,
        const locRegion = SampleRanksA.domain.localSubdomain().dim(0),
        var readAgg = new SrcAggregator(rankType),
        var writeAgg = new DstAggregator(offsetType)) {
    var cur = taskRegion.low;
    var end = taskRegion.high+1;
    while cur < end {
      // find the next unsorted bucket starting at 'cur'
      var bktType: uint(8);
      var bktStartBit: int;
      var bkt = nextUnsortedBucket(BucketBoundaries, taskRegion, region, cur,
                                   /* out */ bktType, bktStartBit);
      // if the initial position has moved forward, record that in 'cur'
      cur = bkt.low;

      if cur >= end {
        break;
      }

      if bkt.size > 1 {
        if region.size < finalSortSimpleSortLimit {
          if locRegion.contains(bkt) && !cfg.assumeNonlocal {
            local {
              sortRegion(SampleRanksA, new finalComparator1(), region);
            }
            // copy sorted values back to SA
            for i in bkt {
              const off = SampleRanksA[i].offset;
              writeAgg.copy(SA[saStart+i], off);
            }
          } else {
            var TmpA:[bkt] SampleRanksA.eltType;
            // copy to local temp
            TmpA[bkt] = SampleRanksA[bkt];
            // sort
            local {
              sortRegion(TmpA, new finalComparator1(), region);
            }
            // copy sorted values back to SA
            for i in bkt {
              const off = TmpA[i].offset;
              writeAgg.copy(SA[saStart+i], off);
            }
          }
        } else {
          linearSortOffsetsInRegionBySampleRanks(cfg, SampleRanksA,
                                                 SampleRanksScratch,
                                                 bkt, saStart);
        }
      }
    }
  }
}

/* Sorts all offsets using the ranks of the difference cover sample.

   Works in distributed parallel.

   Returns a suffix array. */
proc sortAllOffsets(const cfg:ssortConfig(?),
                    const PackedText: [] cfg.loadWordType,
                    const SampleRanks: [] cfg.unsignedOffsetType,
                    const Splitters,
                    resultDom: domain(?),
                    ref stats: statistics) {
  // in a pass over the input,
  // partition the suffixes according to the splitters
  const n = cfg.n;
  const nBits = cfg.nBits;
  type offsetType = cfg.offsetType;
  type wordType = cfg.loadWordType;

  record offsetProducer2 {
    //proc eltType type do return offsetAndCached(offsetType, wordType);
    proc eltType type do return offsetType;
    proc this(i: cfg.idxType) {
      return i: offsetType;
      //return makeOffsetAndCached(cfg, i, PackedText, n, nBits);
    }
  }

  record finalPartitionComparator : relativeComparator {
    // note: this one should just be used for EXTRA_CHECKS
    proc compare(a: prefixAndSampleRanks(?), b: prefixAndSampleRanks(?)) {
      return comparePrefixAndSampleRanks(cfg, a, b,
                                         PackedText, n, SampleRanks, cfg.cover);
    }
    // this is the main compare function used in the partition
    proc compare(a: prefixAndSampleRanks(?), b) {
      return comparePrefixAndSampleRanks(cfg, a, b,
                                         PackedText, n, SampleRanks, cfg.cover);
    }
  }

  var makeBuckets : Time.stopwatch;
  if TIMING {
    makeBuckets.start();
  }

  const comparator = new finalPartitionComparator();
  const InputProducer = new offsetProducer2();

  var SA: [resultDom] offsetType;

  const TextDom = makeBlockDomain(0..<n, cfg.locales);

  var UnusedOutput = none;

  const nTasksPerLocale=cfg.nTasksPerLocale;

  //writeln("outer partition");
  //writeln("Splitters are");
  //writeln(Splitters);

  const Bkts = partition(TextDom, 0..<n, InputProducer,
                         OutputShift=none, Output=SA,
                         Splitters, finalPartitionComparator(),
                         nTasksPerLocale, cfg.locales);

  var maxBktSize = max reduce [b in Bkts] b.count;

  const ScratchDom = makeBlockDomain(0..<maxBktSize, cfg.locales);
  var Offsets: [ScratchDom] offsetType;
  var A: [ScratchDom] offsetAndCached(offsetType, wordType);
  var Scratch: [ScratchDom] offsetAndCached(offsetType, wordType);
  var BucketBoundaries: [ScratchDom] offsetAndCached(offsetType, wordType);
  type offsetAndSampleRanksType =
    makeOffsetSampleRanks(cfg, 0, SampleRanks).type;
  var SampleRanksA: [ScratchDom] offsetAndSampleRanksType;
  var SampleRanksScratch: [ScratchDom] offsetAndSampleRanksType;

  for bkt in Bkts {
    // Reset BucketBoundaries
    BucketBoundaries = 0;

    // Copy the offsets from SA into A
    Offsets[0..<bkt.count] = SA[bkt.start..#bkt.count];
    forall (elt, offset) in zip(A, Offsets) {
      elt.offset = offset;
    }

    // Load the first word into A.cached
    loadNextWords(cfg, PackedText, A, Scratch, BucketBoundaries,
                  0..<bkt.count, 0);

    // Sort the offsets & store the result in SA
    sortAllOffsetsInRegion(cfg, PackedText, SampleRanks,
                           A, Scratch, SampleRanksA, SampleRanksScratch,
                           BucketBoundaries,
                           0..<bkt.count,
                           SA,
                           bkt.start);

    // Copy offsets from A back into SA
    /*forall (elt, offset) in zip(A, Offsets) {
      offset = elt.offset;
    }
    SA[bkt.start..#bkt.count] = Offsets[0..<bkt.count];*/
  }

  /*writeln("SA:");
  for i in SA.domain {
    writeln("SA[", i, "] = ", SA[i]);
  }*/

  return SA;
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
  return useIdx : offset.type;
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
  return offset : subOffset.type;
}

/* Given an offset, compute the offset at which the sample ranks
   start within the SampleText.
   This is different from offsetToSubproblemOffset because it
   uses a more packed form, where the sample ranks are in offset order. */
proc offsetToSampleRanksOffset(offset: integral, const cover) {
  // compute j such that offset + j is in the difference cover
  const j = cover.nextCoverIndex(offset % cover.period);
  const sampleOffset = offset + j;
  const group = sampleOffset / cover.period;
  const coverIdx = cover.coverIndex((sampleOffset) % cover.period);
  const sampleRankOffset = group*cover.sampleSize + coverIdx;
  return sampleRankOffset : offset.type;
}

/* Given a sample rank offset, compute the regular offset.
   This is the inverse of offsetToSampleRanksOffset.
 */
proc sampleRankIndexToOffset(sampleRankOffset: integral, const cover) {
  const group = sampleRankOffset / cover.sampleSize;
  const coverIdx = sampleRankOffset % cover.sampleSize;
  const offset = group*cover.period + cover.cover[coverIdx];
  if EXTRA_CHECKS {
    assert(sampleRankOffset == offsetToSampleRanksOffset(offset, cover));
  }
  return offset : sampleRankOffset.type;
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

//proc offsetToSampleRanksOffset(offset: integral, const cover) {

inline
proc comparePrefixAndSampleRanks(const cfg: ssortConfig(?),
                                 const a,
                                 const b,
                                 const PackedText: [] cfg.loadWordType,
                                 const n,
                                 const SampleRanks: [] cfg.unsignedOffsetType,
                                 const cover) {
  param maxPrefixWords = cfg.getPrefixWords(cover.period);

  // first, compare the first maxPrefixWords words of them
  const prefixCmp = comparePrefixes(cfg, a, b, PackedText, maxPrefixWords);
  if prefixCmp != 0 {
    return prefixCmp;
  }

  // lastly, compare the sample ranks
  return compareSampleRanks(a, b, n, SampleRanks, cover);
}


/*
  Assuming the prefix at two offsets matches, compare the offsets
  using the sample rank from the recursive subproblem.

  a and b should be integral or offsetAndCached.
 */
proc compareSampleRanks(a, b,
                        n: integral, const SampleRanks, cover) {
  // find k such that a.offset+k and b.offset+k are both in the cover
  // (i.e. both are in the sample solved in the recursive problem)
  const k = cover.findInCover(offset(a) % cover.period,
                              offset(b) % cover.period);

  const aSampleOffset = offsetToSampleRanksOffset(offset(a) + k, cover);
  const bSampleOffset = offsetToSampleRanksOffset(offset(b) + k, cover);
  const rankA = SampleRanks[aSampleOffset];
  const rankB = SampleRanks[bSampleOffset];

  const cmp = compareEndOfString(offset(a) + k, offset(b) + k, n);
  if cmp != 0 {
    return cmp;
  }

  return compareIntegers(rankA, rankB);
}

/* Suppose we have an offset and we also have the sample ranks
   starting after that offset available.

   If 'a + k' is in the difference cover, this function
   returns the index into the sample ranks starting at 'a'
   to find the sample rank for 'a + k'.
 */
inline proc sampleRankIndex(a, k: integral, cover: differenceCover(?)) {
  const off = offset(a);
  // off + j is the nearest offset in the cover
  const j = cover.nextCoverIndex(off % cover.period);
  // now off + k and off + j are both in the cover, what indices?
  const aPlusKCoverIdx = cover.coverIndex((off + k) % cover.period);
  const aPlusJCoverIdx = cover.coverIndex((off + j) % cover.period);
  var aRankIdx = aPlusKCoverIdx - aPlusJCoverIdx;
  if aRankIdx < 0 then aRankIdx += cover.sampleSize;

  return aRankIdx;
}

/* As above but works with a being prefixAndSampleRanks
   (as comes up with splitters) */
proc compareSampleRanks(a: prefixAndSampleRanks(?), b,
                        n: integral, const SampleRanks, cover) {
  // find k such that a.offset+k and b.offset+k are both in the cover
  // (i.e. both are in the sample solved in the recursive problem)
  const k = cover.findInCover(offset(a) % cover.period,
                              offset(b) % cover.period);
  const aRankIdx = sampleRankIndex(a, k, cover);
  const bSampleOffset = offsetToSampleRanksOffset(offset(b) + k, cover);

  const rankA = a.r.ranks[aRankIdx];
  const rankB = SampleRanks[bSampleOffset];

  const cmp = compareEndOfString(offset(a) + k, offset(b) + k, n);
  if cmp != 0 {
    return cmp;
  }

  return compareIntegers(rankA, rankB);
}

proc compareLoadedSampleRanks(a, b, // anything where offset(a) works
                              aRanks: sampleRanks(?), bRanks: sampleRanks(?),
                              n: integral, cover) {
  // find k such that a.offset+k and b.offset+k are both in the cover
  // (i.e. both are in the sample solved in the recursive problem)
  const k = cover.findInCover(offset(a) % cover.period,
                              offset(b) % cover.period);
  const aRankIdx = sampleRankIndex(a, k, cover);
  const bRankIdx = sampleRankIndex(b, k, cover);

  const rankA = aRanks.ranks[aRankIdx];
  const rankB = bRanks.ranks[bRankIdx];

  const cmp = compareEndOfString(offset(a) + k, offset(b) + k, n);
  if cmp != 0 {
    return cmp;
  }

  return compareIntegers(rankA, rankB);
}

proc compareSampleRanks(a: prefixAndSampleRanks(?), b: prefixAndSampleRanks(?),
                        n: integral, const SampleRanks, cover) {
  return compareLoadedSampleRanks(a, b,
                                  a.r, b.r,
                                  n, cover);
}

proc compareSampleRanks(a: offsetAndSampleRanks(?), b: offsetAndSampleRanks(?),
                        n: integral, const SampleRanks, cover) {
  return compareLoadedSampleRanks(a, b,
                                  a.r, b.r,
                                  n, cover);
}


/** Create and return a sorted suffix array for the suffixes 0..<n
    referring to 'thetext'.

    The returned array is Block distributed over cfg.locales if CHPL_COMM!=none.
*/
proc ssortDcx(const cfg:ssortConfig(?),
              const PackedText: [] cfg.loadWordType,
              ResultDom = makeBlockDomain(0..<(cfg.n:cfg.idxType), cfg.locales))
 : [ResultDom] cfg.offsetType {

  var total : Time.stopwatch;

  type offsetType = cfg.offsetType;
  const ref cover = cfg.cover;

  // figure out how big the sample will be, including a 0 after each mod
  const n = cfg.n;
  const nBits = cfg.nBits;
  const charsPerMod = 1+myDivCeil(n, cover.period);
  const sampleN = cover.sampleSize * charsPerMod;

  var stats: statistics;

  //writeln("charsPerMod ", charsPerMod);

  if !isDistributedDomain(PackedText.domain) &&
     isDistributedDomain(ResultDom) &&
     ResultDom.targetLocales().size > 1 {
    writeln("warning: PackedText not distributed but result is");
  }
  if PackedText.eltType != cfg.loadWordType {
    compilerError("word type needs to match PackedText.eltType");
  }
  if cfg.unsignedOffsetType != cfg.loadWordType {
    compilerError("word type needs to match unsigned offset type");
  }
  assert(PackedText.domain.rank == 1 &&
         PackedText.domain.dim(0).low == 0);

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
  }

  if PackedText.domain.low != 0 {
    halt("sortDcx expects input array to start at 0");
  }
  const textWords = divCeil(n*cfg.bitsPerChar, numBits(cfg.loadWordType));
  /*writeln(cfg);
  writeln("sampleN = ", sampleN);
  writeln("n = ", n, " textWords = ", textWords,
          " PackedText.size = ", PackedText.size);*/
  if textWords + INPUT_PADDING > PackedText.size {
    // expect it to be zero-padded past n so that
    // getKeyPart / loadWord does not have to check n
    halt("sortDcx needs extra space at the end PackedText");
  }

  //// Base Case ////

  // base case: just sort them if the number of characters is
  // less than the cover period, or sample.n is not less than n
  if n <= cover.period || sampleN >= n {
    if TRACE {
      writeln("Base case suffix sort for n=", n);
    }
    return computeSuffixArrayDirectly(cfg, PackedText, ResultDom);
  }

  // set up information for recursive subproblem
  const subCfg = new ssortConfig(idxType=cfg.idxType,
                                 offsetType=offsetType,
                                 bitsPerChar=numBits(offsetType),
                                 n=sampleN,
                                 cover=cover,
                                 locales=cfg.locales,
                                 nTasksPerLocale=cfg.nTasksPerLocale);

  //// Step 1: Sort Sample Suffixes ////

  // begin by computing the input text for the recursive subproblem
  var SampleDom = makeBlockDomain(0..<sampleN+INPUT_PADDING+cover.period,
                                  cfg.locales);
  var SampleText:[SampleDom] cfg.unsignedOffsetType;
  var allSamplesHaveUniqueRanks = false;

  // create a sample splitters that can be replaced later
  var unusedPrefix = makePrefix(cfg, 0, PackedText, n, nBits);
  const prefixSize = c_sizeof(unusedPrefix.type):int;
  var unusedPrefixAndSampleRanks =
    makePrefixAndSampleRanks(cfg, 0, PackedText, SampleText, n, nBits);
  const prefixAndSampleRanksSize =
    c_sizeof(unusedPrefixAndSampleRanks.type):int;

  // compute number of buckets for sample partition & after recursion partition
  var nTasks = ResultDom.targetLocales().size * cfg.nTasksPerLocale;
  var requestedNumPrefixBuckets = max(cfg.minBucketsPerTask * nTasks,
                                      cfg.minBucketsSpace / prefixSize);

  // don't request more prefix buckets than we can produce with sample
  requestedNumPrefixBuckets = min(requestedNumPrefixBuckets, sampleN / 2);

  // create space for final step splitters now to avoid memory fragmentation
  const splittersBits = cfg.logBucketsSerial;
  var numSplitters = (1<<splittersBits) - 1;
  var saveSplitters:[0..numSplitters] unusedPrefixAndSampleRanks.type;

  if TRACE {
    writeln(" each prefix is ", prefixSize, " bytes");
    writeln(" each prefixAndSampleRank is ",
            prefixAndSampleRanksSize, " bytes");
    writeln(" requesting ", requestedNumPrefixBuckets,
            " prefix buckets for sample");
    writeln(" final sort with ", numSplitters+1, " serial buckets");
    writeln(" nTasksPerLocale is ", cfg.nTasksPerLocale);
  }

  // these are initialized below
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
      if STATS {
        writeln("pre statistics ", stats);
      }
    }

    // compute the name (approximate rank) for each sample suffix
    sortAndNameSampleOffsets(cfg, PackedText, requestedNumPrefixBuckets,
                             SampleText, charsPerMod, stats);
  }

  //// recursively sort the subproblem ////
  {
    //writeln("Recursive Input");
    //writeln(SampleText);

    /*for i in 0..<subCfg.n {
      writeln("SampleText[", i, "] = ", SampleText[i]);
    }*/

    const SubSA = ssortDcx(subCfg, SampleText);

    //writeln("Recursive Output");
    //writeln(SubSA);

    if TRACE {
      writeln("back in ssortDcx n=", n);
      //writeln("SubSA is ", SubSA);
    }

    {
      var update : Time.stopwatch;
      if TIMING {
        update.start();
      }
      defer {
        if TIMING {
          update.stop();
          writeln("update SampleText ranks in ", update.elapsed(), " s");
        }
      }

      // Replace the values in SampleText with
      // 1-based ranks from the suffix array.
      forall (subOffset,rank) in zip(SubSA, SubSA.domain)
      with (var cover = cfg.cover,
            var agg = new DstAggregator(cfg.unsignedOffsetType)) {
        const offset = subproblemOffsetToOffset(subOffset, cover, charsPerMod);
        const rankOffset = offsetToSampleRanksOffset(offset, cover);
        /*writeln("SubSA[", rank, "] subOffset=",
                subOffset, " offset=", offset,
                " rankOffset=", rankOffset);*/
        var useRank = rank+1;
        agg.copy(SampleText[rankOffset], useRank:cfg.unsignedOffsetType);
      }

      /*for i in 0..<sampleN {
        writeln("SampleRanks[", i, "] = ", SampleText[i]);
      }*/
    }

    // gather splitters and store them in saveSplitters

    const perSplitter = sampleN:real / (numSplitters+1):real;
    var start = perSplitter:int;

    // note: this does a bunch of GETs, is not distributed or aggregated
    // compare with createSampleSplitters which is more distributed
    forall i in 0..<numSplitters {
      var sampleIdx = start + (i*perSplitter):int;
      sampleIdx = min(max(sampleIdx, 0), sampleN-1);

      // sampleIdx is an index into the subproblem suffix array, <sampleN.
      // find the offset in the subproblem
      var subOffset = offset(SubSA[sampleIdx]);
      // find the index in the parent problem.
      var off = sampleRankIndexToOffset(subOffset, cover);
      var ret = makePrefixAndSampleRanks(cfg, off,
                                         PackedText, SampleText,
                                         n, nBits);

      // writeln("sampleCreator(", i, ") :: SA[i] = ", subOffset, " -> offset ", off, " -> ", ret);

      saveSplitters[i] = ret;
    }

    record sampleComparator : relativeComparator {
      proc compare(a: prefixAndSampleRanks(?), b: prefixAndSampleRanks(?)) {
        return comparePrefixAndSampleRanks(cfg, a, b,
                                           PackedText, n,
                                           SampleText, cover);
      }
    }


    // note, a bunch of serial work inside this call
    const tmp = new splitters(saveSplitters,
                              saveSplitters.size,
                              new sampleComparator(),
                              howSorted=sortLevel.approximately);
    numSplitters = tmp.myNumBuckets;
    saveSplitters[0..<numSplitters] = tmp.sortedStorage[0..<numSplitters];
  }

  //// Step 2: Sort everything all together ////
  var post : Time.stopwatch;
  if TIMING {
    post.start();
  }
  defer {
    if TIMING {
      post.stop();
      writeln("post in ", post.elapsed(), " s");
    }
    if STATS {
      writeln("pre+post statistics ", stats);
    }
  }

  const SampleSplitters = new splitters(saveSplitters[0..<numSplitters],
                                        /* equal buckets */ false);

  return sortAllOffsets(cfg, PackedText, SampleText, SampleSplitters,
                        ResultDom, stats);
}

// TODO: move this LCP stuff to a different file

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
