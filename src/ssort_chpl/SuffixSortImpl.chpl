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
import SuffixSort.INPUT_PADDING;

// how much more should we sample to create splitters?
// 1.0 would be only to sample enough for the splitters
config const sampleRatio = 1.5;

config const seed = 1;
config const minBucketsPerTask = 8;
config const minBucketsSpace = 2_000_000; // a size in bytes
config const simpleSortLimit = 1000; // for sizes >= this,
                                     // use radix sort + multi-way merge
config const finalSortPasses = 8;

// upper-case names for the config constants to better identify them in code
const SAMPLE_RATIO = min(1.0, sampleRatio);
const SEED = seed;
const MIN_BUCKETS_PER_TASK = minBucketsPerTask;
const MIN_BUCKETS_SPACE = minBucketsSpace;
const SIMPLE_SORT_LIMIT = simpleSortLimit;
const FINAL_SORT_NUM_PASSES = finalSortPasses;

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

  if region.size == 2 {
    const i = region.low;
    const j = region.low + 1;
    if mycompare(A[i], A[j], comparator) > 0 {
      A[i] <=> A[j];
    }
    return;
  }

  local {
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
  Sort suffixes in A[region] by the first maxPrefix character values.
  In the process, mark every offset that differs from a previous offset
  with bit complement. The first offset is always marked.
  Leaves partially sorted suffixes in A.

  This is a single-locale operation.
 */
proc sortByPrefixAndMark(const cfg:ssortConfig(?),
                         const PackedText: [] cfg.loadWordType,
                         ref A:[] offsetAndCached(cfg.offsetType,
                                                  cfg.loadWordType),
                         region: range,
                         ref readAgg: SrcAggregator(cfg.loadWordType),
                         maxPrefix: cfg.idxType) {

  if region.size == 0 {
    return;
  }

  type wordType = cfg.loadWordType;
  param wordBits = numBits(wordType);
  param bitsPerChar = cfg.bitsPerChar;
  const n = cfg.n;
  const nBits = cfg.nBits;

  // this code should only be called with A being local (or local enough)
  assert(A.domain.localSubdomain().contains(region));

  // allocate temporary storage
  // TODO: this is not needed for cfg.bitsPerChar == numBits(wordType)
  var loadWords:[region] wordType;

  var sortedByBits = 0;
  const prefixBits = maxPrefix*bitsPerChar;
  while sortedByBits < prefixBits {
    writeln("in sortByPrefixAndMark sorted by ", sortedByBits, " for ", region);
    for i in region {
      writeln("A[", i, "] = ", A[i]);
    }

    // sort by 'cached'
    record byCached : keyComparator {
      proc key(elt) { return elt.cached; }
    }
    const byCachedComparator = new byCached();
    if sortedByBits == 0 {
      writeln("sorting full region ", region);
      sortRegion(A, byCachedComparator, region);
    } else {
      // sort each subregion starting from each marked offset
      // up to but not including the next marked offset
      for r in unsortedRegionsFromMarks(A, region) {
        // clear the mark on the 1st element since it might move later
        unmarkOffset(A[r.low]);
        writeln("sorting subregion ", r);
        sortRegion(A, byCachedComparator, r);
        // put the mark back now that a different element might be there
        markOffset(A[r.low]);
      }
    }

    // mark any elements that differ from the previous element
    // (note, the first element is marked later, after it
    //  must be sorted in to place)
    var anyUnsortedRegions = false;
    for r in unsortedRegionsFromMarks(A, region) {
      anyUnsortedRegions = true;
      var lastCached = A[r.low].cached;
      for i in r {
        ref elt = A[i];
        if elt.cached != lastCached {
          markOffset(elt);
          lastCached = elt.cached;
          writeln("marked ", elt);
        }
      }
    }

    // now we have sorted by an additional word
    sortedByBits += wordBits;

    // stop if there were no unsorted regions
    if !anyUnsortedRegions {
      break;
    }

    writeln("in sortByPrefixAndMark now sorted by ", sortedByBits);
    for i in region {
      writeln("A[", i, "] = ", A[i]);
    }


    // get the next word to sort by and store it in 'cached' for each entry
    if sortedByBits < prefixBits {
      if cfg.bitsPerChar == wordBits {
        // load directly into 'cached', no need to shift
        for i in region {
          const bitOffset = unmarkedOffset(A[i])*bitsPerChar + sortedByBits;
          const wordIdx = bitOffset / wordBits; // divides evenly in this case
          if bitOffset < nBits {
            readAgg.copy(A[i].cached, PackedText[wordIdx]);
          } else {
            A[i].cached = 0; // word starts after the end of the string
          }
        }
        readAgg.flush();
      } else {
        // load into 'cached' and 'loadWords' and then combine these
        // since the next bits might not lie on a word boundary in PackedText
        for i in region {
          const bitOffset = unmarkedOffset(A[i])*bitsPerChar + sortedByBits;
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
              // load an additional word to 'loadWords'
              readAgg.copy(loadWords[i], PackedText[wordIdx + 1]);
            } else {
              loadWords[i] = 0; // next word starts after the end of the string
            }
          }
        }
        readAgg.flush();
        // combine the two words as needed
        for i in region {
          const bitOffset = unmarkedOffset(A[i])*bitsPerChar + sortedByBits;
          A[i].cached = loadWordWithWords(A[i].cached, loadWords[i], bitOffset);
        }
      }
    }
  }

  // now that we know which element is the first element
  // (because it is sorted), mark the first element.
  markOffset(A[region.low]);
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

  if isDistributedDomain(resultDom) {
    // When directly computing the suffix array on a distributed array,
    // move everything local first and then copy back to the result array.
    //
    // This avoids the need for a distributed sort and should be
    // sufficient for the base case.

    // This could just be = resultDom but this way of writing avoids a warning.
    var localDom: domain(1) = {resultDom.dim(0),};
    var localA = computeSuffixArrayDirectly(cfg, PackedText, localDom);
    const A: [resultDom] cfg.offsetType = localA;
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

  sortRegion(A, new directComparator(), 0..<n);

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

/* Fill in SampleNames for a region within Sample after partitioning.
   The Sample[region] is not sorted yet, but contains the right
   elements (from partitioning).

   Runs on one locale & does not need to be parallel.

   Sorts the sample by the the first cover.period characters
   and then computes unique names for each cover.period prefix,
   storing these unique names in SampleNames. */
proc sortAndNameSampleOffsetsInRegion(const cfg:ssortConfig(?),
                                      const PackedText: [] cfg.loadWordType,
                                      ref Sample: []
                                           offsetAndCached(cfg.offsetType,
                                                           cfg.loadWordType),
                                      region: range,
                                      regionIsEqual: bool,
                                      ref readAgg:
                                          SrcAggregator(cfg.loadWordType),
                                      ref writeAgg:
                                          DstAggregator(cfg.unsignedOffsetType),
                                      ref SampleNames:[] cfg.unsignedOffsetType,
                                      charsPerMod: cfg.idxType) {
  const cover = cfg.cover;
  param prefixWords = cfg.getPrefixWords(cover.period);

  // sort the suffixes in a way that marks offsets
  // of suffixes that differ from the previous according
  // to the prefixWords words of data from PackedText.

  assert(Sample.domain.localSubdomain().contains(region));

  sortByPrefixAndMark(cfg, PackedText, Sample, region,
                      readAgg, maxPrefix=cover.period);

  // remove a mark on the first offset in the bucket
  // since we are using the bucket start as the initial name,
  // we don't want to increment the name for the first one.
  // this allows the below loop to be simpler.
  {
    ref elt = Sample[region.low];
    elt.offset = unmarkedOffset(elt);
  }

  // assign names to each sample position
  // note: uses the bucket start as the initial name within
  // each bucket. this way of leaving gaps allows the process
  // to be simpler. the names are still < n.
  var curName = region.low;
  for i in region {
    ref elt = Sample[i];
    if isMarkedOffset(elt) {
      curName += 1;
    }
    const off = unmarkedOffset(elt);

    // offset is an unpacked offset. find the offset in
    // the recursive problem input to store the rank into.
    // Do so in a way that arranges for SampleText to consist of
    // all sample inputs at a particular mod, followed by other modulus.
    // We have charsPerMod characters for each mod in the cover.
    const useIdx = offsetToSubproblemOffset(off, cover, charsPerMod);

    // store the name into SampleNames
    // note: each useIdx value is only set once here
    const useName = (curName+1):cfg.unsignedOffsetType;
    writeAgg.copy(SampleNames[useIdx], useName);
  }
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
                              charsPerMod: cfg.idxType) {
  const n = cfg.n;
  const nBits = cfg.nBits;
  const cover = cfg.cover;
  const nTasksPerLocale = cfg.nTasksPerLocale;
  const nPeriods = myDivCeil(n, cover.period); // nPeriods * period >= n
  const sampleN = cover.sampleSize * nPeriods;
  var nToSampleForSplitters = (SAMPLE_RATIO*requestedNumBuckets):int;
  // To better avoid random access,
  // go through the input & partition by a splitter
  // while creating the offset & storing it into an output array
  // for the Sample.
  type offsetType = cfg.offsetType;
  type wordType = cfg.loadWordType;
  param prefixWords = cfg.getPrefixWords(cover.period);

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

  const comparator = new myPrefixComparator3();
  const InputProducer = new inputProducer1();

  // first, create a sorting sample of offsets in the cover
  const sp; // initialized below
  {
    var randNums;
    if SEED == 0 {
      randNums = new Random.randomStream(cfg.idxType);
    } else {
      randNums = new Random.randomStream(cfg.idxType, seed=SEED);
    }
    var SplittersSampleDom = {0..<nToSampleForSplitters};
    type prefixType = makePrefix(cfg, 0, PackedText, n, nBits).type;
    var SplittersSample:[SplittersSampleDom] prefixType;
    forall (x, r) in zip(SplittersSample,
                         randNums.next(SplittersSampleDom, 0, sampleN-1)) {
      // r is a packed index into the offsets to sample
      // we have to unpack it to get the regular offset
      const whichPeriod = r / cover.sampleSize;
      const phase = r % cover.sampleSize;
      const coverVal = cover.cover[phase]:offsetType;
      const unpackedIdx = whichPeriod * cover.period + coverVal;
      x = makePrefix(cfg, unpackedIdx, PackedText, n, nBits);
    }

    // sort the sample and create the splitters
    sp = new splitters(SplittersSample, requestedNumBuckets, comparator,
                       howSorted=sortLevel.unsorted);
  }

  const replSp = replicate(sp, cfg.locales);
  const SampleDom = makeBlockDomain(0..<sampleN,
                                    targetLocales=cfg.locales);
  var Sample: [SampleDom] offsetAndCached(offsetType, wordType);

  // now, count & partition by the prefix by traversing over the input
  const Counts = partition(SampleDom, InputProducer,
                           SampleDom, Sample,
                           sp, replSp, comparator,
                           cfg.nTasksPerLocale);

  const Ends = + scan Counts;

  const maxBucketSize = max reduce Counts;


  // now, consider each bucket & sort within that bucket.
  forall (bktRegion, bktIdx, taskId)
  in divideByBuckets(Sample, Counts, Ends, nTasksPerLocale)
  with (in cfg,
        var readAgg = new SrcAggregator(wordType),
        var writeAgg = new DstAggregator(SampleNames.eltType)) {

    // skip empty buckets
    if bktRegion.size > 0 {
      const ref mysplit = getLocalReplicand(sp, replSp);

      var regionIsEqual = false;
      if bktRegion.size == 1 || mysplit.bucketHasEqualityBound(bktIdx) {
        // no need to sort or mark such buckets
        regionIsEqual = true;
      }

      const regionDom: domain(1) = {bktRegion,};
      if Sample.domain.localSubdomain().contains(regionDom) {
        sortAndNameSampleOffsetsInRegion(cfg, PackedText, Sample,
                                         bktRegion, regionIsEqual,
                                         readAgg, writeAgg,
                                         SampleNames, charsPerMod);
      } else {
        // copy to a local array and then proceed
        var LocSample:[regionDom] Sample.eltType;
        LocSample[bktRegion] = Sample[bktRegion];
        sortAndNameSampleOffsetsInRegion(cfg, PackedText, LocSample,
                                         bktRegion, regionIsEqual,
                                         readAgg, writeAgg,
                                         SampleNames, charsPerMod);
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
 */
proc sortOffsetsInRegionBySampleRanks(
                            const cfg:ssortConfig(?),
                            const LoadedSampleRanks: [] sampleRanks(?),
                            ref A: [] offsetAndCached(cfg.offsetType,
                                                      cfg.loadWordType),
                            region: range,
                            cover: differenceCover(?)) {

  //writeln("in sortOffsetsInRegionBySampleRanks ", region, " size=", region.size);

  const n = cfg.n;
  const finalSortSimpleSortLimit = cfg.finalSortSimpleSortLimit;

  // the comparator to sort by sample ranks
  record finalComparator : relativeComparator {
    proc compare(a: offsetAndCached(?), b: offsetAndCached(?)) {
      const ref aRanks = LoadedSampleRanks[a.cached:int];
      const ref bRanks = LoadedSampleRanks[b.cached:int];
      // assuming the prefixes are the same, compare the nearby sample
      // rank from the recursive subproblem.
      return compareLoadedSampleRanks(unmarkedOffset(a),
                                      unmarkedOffset(b),
                                      aRanks, bRanks, n, cover);
    }
  }

  if region.size < finalSortSimpleSortLimit {
    // just run a comparison sort
    sortRegion(A, new finalComparator(), region);
    return;
  }

  writeln("in sortOffsetsInRegionBySampleRanks running v-way merge", " for size=", region.size);

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

  // destination for partitioning
  // this is a non-distributed (local) array even if A is distributed
  var B:[region] A.eltType;

  // partition by the distance to a sample suffix
  const Counts = partition(A.domain[region], A,
                           B.domain, B,
                           split=new distanceToSampleSplitter(), rsplit=none,
                           comparator=new finalComparator(), /* unused */
                           nTasksPerLocale=cfg.nTasksPerLocale);

  if isDistributedDomain(Counts.domain) then
    compilerError("Was not expecting it to be distributed");

  const Ends = + scan Counts;

  assert(Ends.last == region.size);

  var nNonEmptyBuckets = 0;

  // radix sort each sub-bucket within each partition
  // note: forall and divideByBuckets not strictly necessary here;
  // this could be serial since it's called in an outer forall.
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
      sortRegion(B, new fixedDistanceToSampleComparator(k),
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
  multiWayMerge(B, InputRanges, A, region, new finalComparator());
}


/* Sorts offsets in a region using a difference cover sample.
   Runs on one locale & does not need to be parallel.
   Updates the suffix array SA with the result.
 */
proc sortAllOffsetsInRegion(const cfg:ssortConfig(?),
                            const PackedText: [] cfg.loadWordType,
                            const SampleRanks: [] cfg.unsignedOffsetType,
                            ref Scratch: [] offsetAndCached(cfg.offsetType,
                                                            cfg.loadWordType),
                            region: range,
                            ref readAgg: SrcAggregator(cfg.loadWordType),
                            ref writeAgg: DstAggregator(cfg.offsetType),
                            ref SA: []) {
  const cover = cfg.cover;

  if region.size == 0 {
    return;
  }

  if region.size == 1 {
    // store the result into SA
    const i = region.low;
    const elt = Scratch[i];
    const off = unmarkedOffset(elt);
    writeAgg.copy(SA[i], off);
  }

  // sort by the first cover.period characters
  sortByPrefixAndMark(cfg, PackedText, Scratch, region, readAgg,
                      maxPrefix=cover.period);


  writeln("after sortByPrefixAndMark Scratch[", region, "]");
  for i in region {
    writeln("Scratch[", i, "] = ", Scratch[i]);
  }

  // Compute the number of unsorted elements &
  // Adjust each element's 'cached' value to be an offset into
  // LoadedSampleRanks.
  var nextLoadedIdx = 0;
  for r in unsortedRegionsFromMarks(Scratch, region) {
    for i in r {
      ref elt = Scratch[i];
      elt.cached = nextLoadedIdx : cfg.loadWordType;
      nextLoadedIdx += 1;
    }
  }

  // allocate LoadedSampleRanks of appropriate size
  type sampleRanksType = makeSampleRanks(cfg, 0, SampleRanks).type;
  var LoadedSampleRanks:[0..<nextLoadedIdx] sampleRanksType;

  // Load the sample ranks into LoadedSampleRanks
  for r in unsortedRegionsFromMarks(Scratch, region) {
    for i in r {
      const elt = Scratch[i];
      const off = unmarkedOffset(elt);
      const loadedIdx = elt.cached : int;
      const start = offsetToSampleRanksOffset(off, cfg.cover);
      for j in 0..<sampleRanksType.nRanks {
        readAgg.copy(LoadedSampleRanks[loadedIdx].ranks[j],
                     SampleRanks[start+j]);
      }
    }
  }
  // make sure that the aggregator is done
  readAgg.flush();

  writeln("after loading  Scratch[", region, "]");
  for r in unsortedRegionsFromMarks(Scratch, region) {
    for i in r {
      writeln("Scratch[", i, "] = ", Scratch[i], " ",
              LoadedSampleRanks[Scratch[i].cached:int]);
    }
  }

  // now use the sample ranks to compute the final sorting
  for r in unsortedRegionsFromMarks(Scratch, region) {
    writeln("sorting by sample ranks ", r);
    sortOffsetsInRegionBySampleRanks(cfg, LoadedSampleRanks, Scratch, r, cover);

    // the marks are irrelevant (but wrong) at this point
    // since the first element might have been sorted later.

  }

  writeln("after sorting by sample ranks  Scratch[", region, "]");
  for i in region {
    writeln(" Scratch[", i, "] = ", Scratch[i]);
  }

  // store the data back into SA
  for i in region {
    const elt = Scratch[i];
    const off = unmarkedOffset(elt);
    writeAgg.copy(SA[i], off);
  }
}

/* Sorts all offsets using the ranks of the difference cover sample.

   Works in distributed parallel.

   Returns a suffix array. */
proc sortAllOffsets(const cfg:ssortConfig(?),
                    const PackedText: [] cfg.loadWordType,
                    const SampleRanks: [] cfg.unsignedOffsetType,
                    const Splitters,
                    resultDom: domain(?)) {
  // in a pass over the input,
  // partition the suffixes according to the splitters
  const n = cfg.n;
  const nBits = cfg.nBits;
  type offsetType = cfg.offsetType;
  type wordType = cfg.loadWordType;

  record offsetProducer2 {
    proc eltType type do return offsetAndCached(offsetType, wordType);
    proc this(i: cfg.idxType) {
      return makeOffsetAndCached(cfg, i, PackedText, n, nBits);
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

  const ReplSplitters = replicate(Splitters, cfg.locales);

  const TextDom = makeBlockDomain(0..<n, cfg.locales);

  // we process the input in a bunch of passes to reduce memory
  // usage while caching some of each suffixes prefix when sorting.

  // decide how many passes to do
  const nPasses = min(cfg.finalSortNumPasses, Splitters.numBuckets);

  var UnusedOutput = none;

  writeln("outer partition");
  //writeln("Splitters are");
  //writeln(Splitters);

  const OuterCounts = partition(TextDom, InputProducer,
                                SA.domain, /* count only here */ UnusedOutput,
                                Splitters, ReplSplitters, comparator,
                                cfg.nTasksPerLocale);

  const OuterEnds = + scan OuterCounts;

  writeln("Performing ", nPasses, " passes over input");
  writeln("TextDom = ", TextDom, " SA.domain = ", SA.domain);

  var nBucketsPerPass = divCeil(Splitters.numBuckets, nPasses);

  /*
  for (count, bktIdx) in zip (OuterCounts, OuterCounts.domain) {
    writeln(bktIdx, " bucket has ", count, " elements");
  }*/

  // process the input in nPasses passes
  // each pass handles nBucketsPerPass buckets.
  for pass in 0..<nPasses {
    const startBucket = pass*nBucketsPerPass;
    const endBucket = startBucket + nBucketsPerPass; // exclusive
    var endPrevBucket = 0;
    if startBucket > 0 {
      endPrevBucket = OuterEnds[startBucket-1];
    }
    assert(endBucket > 0);

    // compute the index in the SA that this pass starts at
    const passEltStart = OuterEnds[startBucket] - OuterCounts[startBucket];

    // compute the number of elements to be processed by this pass
    const groupElts = OuterEnds[endBucket-1] - endPrevBucket;

    writeln("pass ", pass, " processing ", groupElts,
            " elements starting at ", passEltStart);

    if groupElts == 0 {
      continue; // nothing to do if there are no elements
    }

    const ScratchDom = makeBlockDomain(passEltStart..#groupElts, cfg.locales);
    var Scratch:[ScratchDom] offsetAndCached(offsetType, wordType);
    writeln("ScratchDom = ", ScratchDom);

    record filter1 {
      proc this(bkt) {
        return startBucket <= bkt && bkt < endBucket;
      }
    }

    const InnerCounts = partition(TextDom, InputProducer,
                                  Scratch.domain, Scratch,
                                  Splitters, ReplSplitters, comparator,
                                  cfg.nTasksPerLocale,
                                  filterBucket=new filter1());

    const InnerEnds = + scan InnerCounts;

    forall (bktRegion, bktIdx, taskId)
    in divideByBuckets(Scratch, InnerCounts, InnerEnds, cfg.nTasksPerLocale)
    with (in cfg,
          var readAgg = new SrcAggregator(wordType),
          var writeAgg = new DstAggregator(offsetType)) {
      // skip empty buckets
      if bktRegion.size > 0 {
        writeln("Scratch[", bktRegion, "]");
        for i in bktRegion {
          writeln("Scratch[", i, "] = ", Scratch[i]);
        }

        const regionDom: domain(1) = {bktRegion,};
        if Scratch.domain.localSubdomain().contains(regionDom) {
          sortAllOffsetsInRegion(cfg, PackedText, SampleRanks,
                                 Scratch, bktRegion,
                                 readAgg, writeAgg, SA);
        } else {
          // copy to a local array and then proceed
          var LocScratch:[regionDom] Scratch.eltType;
          LocScratch[bktRegion] = Scratch[bktRegion];
          sortAllOffsetsInRegion(cfg, PackedText, SampleRanks,
                                 LocScratch, bktRegion,
                                 readAgg, writeAgg, SA);
        }
      }
    }
  }

  writeln("SA:");
  for i in SA.domain {
    writeln("SA[", i, "] = ", SA[i]);
  }

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

  writeln("charsPerMod ", charsPerMod);

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
  writeln(cfg);
  writeln("sampleN = ", sampleN);
  writeln("n = ", n, " textWords = ", textWords,
          " PackedText.size = ", PackedText.size);
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

  // TODO: allocate output array here in order to avoid memory fragmentation

  // begin by computing the input text for the recursive subproblem
  var SampleDom = makeBlockDomain(0..<sampleN+INPUT_PADDING+cover.period,
                                  cfg.locales);
  var SampleText:[SampleDom] cfg.unsignedOffsetType;
  var allSamplesHaveUniqueRanks = false;

  // create a sample splitters that can be replaced later
  var unusedSplitter = makePrefixAndSampleRanks(cfg, 0,
                                                PackedText, SampleText,
                                                n, nBits);

  // compute number of buckets for sample partition & after recursion partition
  const splitterSize = c_sizeof(unusedSplitter.type):int;
  var nTasks = ResultDom.targetLocales().size * cfg.nTasksPerLocale;
  var requestedNumBuckets = max(cfg.minBucketsPerTask * nTasks,
                                cfg.minBucketsSpace / splitterSize);

  // create space for splitters now to avoid memory fragmentation
  var saveSplitters:[0..<2*requestedNumBuckets] unusedSplitter.type;
  var nSaveSplitters: int;

  // don't request more buckets than we can produce with sample
  requestedNumBuckets = min(requestedNumBuckets, (sampleN / SAMPLE_RATIO):int);

  if TRACE {
    writeln(" each prefixAndSampleRank is ", splitterSize, " bytes");
    writeln(" requesting ", requestedNumBuckets, " buckets");
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
    }

    // compute the name (approximate rank) for each sample suffix
    sortAndNameSampleOffsets(cfg, PackedText, requestedNumBuckets,
                             SampleText, charsPerMod);
  }

  //// recursively sort the subproblem ////
  {
    //writeln("Recursive Input");
    //writeln(SampleText);

    for i in 0..<subCfg.n {
      writeln("SampleText[", i, "] = ", SampleText[i]);
    }

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
        writeln("SubSA[", rank, "] subOffset=",
                subOffset, " offset=", offset,
                " rankOffset=", rankOffset);
        var useRank = rank+1;
        agg.copy(SampleText[rankOffset], useRank:cfg.unsignedOffsetType);
      }

      for i in 0..<sampleN {
        writeln("SampleRanks[", i, "] = ", SampleText[i]);
      }
    }

    // create splitters and store them in saveSplitters
    record sampleCreator {
      proc eltType type do return unusedSplitter.type;
      proc size do return sampleN;
      proc this(i: int) {
        // i is an index into the subproblem suffix array, <sampleN.
        // find the offset in the subproblem
        var subOffset = offset(SubSA[i]);
        // find the index in the parent problem.
        var off = sampleRankIndexToOffset(subOffset, cover);
        var ret = makePrefixAndSampleRanks(cfg, off,
                                           PackedText, SampleText,
                                           n, nBits);
        // writeln("sampleCreator(", i, ") :: SA[i] = ", subOffset, " -> offset ", off, " -> ", ret);
        return ret;
      }
    }

    record sampleComparator : relativeComparator {
      proc compare(a: prefixAndSampleRanks(?), b: prefixAndSampleRanks(?)) {
        return comparePrefixAndSampleRanks(cfg, a, b,
                                           PackedText, n,
                                           SampleText, cover);
      }
    }

    const tmp  = new splitters(new sampleCreator(),
                               requestedNumBuckets,
                               new sampleComparator(),
                               howSorted=sortLevel.approximately);

    // save the splitters for later
    nSaveSplitters = tmp.myNumBuckets;
    saveSplitters[0..<nSaveSplitters] = tmp.sortedStorage[0..<nSaveSplitters];

    //writeln("requestedNumBuckets is ", requestedNumBuckets);
    //writeln("saveSplitters have ", nSaveSplitters, " buckets and are");
    //writeln(saveSplitters);
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
  }

  const SampleSplitters = new splitters(saveSplitters[0..<nSaveSplitters],
                                        /* equal buckets */ false);

  return sortAllOffsets(cfg, PackedText, SampleText, SampleSplitters,
                        ResultDom);
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
