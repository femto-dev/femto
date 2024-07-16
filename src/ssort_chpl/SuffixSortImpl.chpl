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
use Random;

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

  proc serialize(writer, ref serializer) throws {
    if cacheType == nothing {
      writer.write(offset);
    } else {
      writer.writef("%i (%016xu)", offset, cached);
    }
  }
}

/**
  This record holds a whole record with a prefix.
  This is useful for splitters.

  It could store an offset as well but that isn't actually needed.
 */
record prefix {
  type wordType;
  param nWords;
  var words: nWords*wordType;
}

/**
  This record holds a prefix and the next cover period sample ranks.
  This is useful for splitters.

  It could store an offset as well but that isn't actually needed.
 */
record prefixAndSampleRanks {
  type wordType;
  type offsetType;
  param nWords;
  param nRanks;

  var words: nWords*wordType;
  var ranks: nRanks*offsetType;
}


inline proc myDivCeil(x: integral, param y: integral) {
  return (x + y - 1) / y;
}
inline proc myDivCeil(param x: integral, param y: integral) param {
  return (x + y - 1) / y;
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
  Construct an prefix record for offset 'i' in the input
  by loading the relevant data from 'text'.
 */
proc makePrefix(i: integral, const text, n: integral, cover) {
  type wordType = uint(64);
  type characterType = text.eltType;
  // how many words do we need in order to hold cover.period characters?
  param wordBytes = cover.period * numBytes(wordType);
  param textCharBytes = numBytes(characterType);
  param nWords = myDivCeil(wordBytes, textCharBytes);
  if wordBytes < textCharBytes || !isUintType(wordType) {
    compilerError("invalid makeOffsetAndCached call");
  }

  var result = new prefix(wordType=wordType, nWords=nWords);
  // fill in the words
  for i in 0..<nWords {
    result.words[i] = loadWord(text, n, i, wordType);
  }
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


// can be called from keyPart(someOffset, i)
inline proc getKeyPartForOffset(const offset: integral, i: integral,
                                const cfg:ssortConfig(?),type wordType,
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
inline proc getKeyPart(const a: offsetAndCached(?), i: integral,
                       const cfg:ssortConfig(?),
                       const text, n: cfg.offsetType,
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

/** Sort suffixes that we have already initialized in A
    by the first maxPrefix character values.
 */
proc sortSuffixes(const cfg:ssortConfig(?), const thetext,
                  ref A: [] offsetAndCached(?), n: cfg.offsetType,
                  maxPrefix: A.eltType.offsetType) {
  type idxType = cfg.idxType;
  type characterType = cfg.characterType;
  type offsetType = cfg.offsetType;
  type cachedDataType = cfg.cachedDataType;
  type wordType = cfg.wordType;
  // Define a comparator to support radix sorting by the first maxPrefix
  // character values.
  record myPrefixComparator {
    proc keyPart(a: offsetAndCached(?), i: int):(int(8), wordType) {
      return getKeyPart(a, i=i, cfg, thetext, n=n, maxPrefix=maxPrefix);
    }
  }

  sort(A, new myPrefixComparator());
}

/* If we computed the suffix array for text using cachedDataType!=nothing,
   there is some ambiguity between 0s due to end-of-string/padding
   vs 0s due to the input. This function resolves the issue
   by adjusting the first several suffix array entries.
 */
proc fixTrailingZeros(const text, n:integral, ref A: [],
                      type characterType, type offsetType,
                      type cachedDataType) {
  if cachedDataType != nothing &&
     numBits(cachedDataType) != numBits(characterType) {
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
      if EXTRA_CHECKS then assert(A[i].cached == 0);
      A[i].offset = n-1-i;
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

  sortSuffixes(cfg, text, A, n=useN, maxPrefix=max(offsetType));

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
                        out sampleN: offsetType) {
  const nPeriods = myDivCeil(n, cover.period); // nPeriods * period >= n
  sampleN = cover.sampleSize * nPeriods;
  const Dom = {0..<sampleN:offsetType};
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
                       const text, n: cfg.offsetType,
                       const nTasks: int,
                       const nBuckets: int,
                       out sampleN: cfg.offsetType) {
  const ref cover = cfg.cover;
  const nPeriods = myDivCeil(n, cover.period); // nPeriods * period >= n
  sampleN = cover.sampleSize * nPeriods;
  var nToSampleForSplitters = (SAMPLE_RATIO*nBuckets):int;
  if !PARTITION_SORT_SAMPLE || nToSampleForSplitters >= sampleN {
    // build sample offsets and sort them
    // does more random access and/or uses more memory (if caching data)
    var Sample = buildSampleOffsets(offsetType=cfg.offsetType,
                                    cachedDataType=cfg.cachedDataType,
                                    text, n, cover, /*out*/ sampleN);
    // then sort the these by the first cover.period characters;
    // note that these offsets are in 0..<n not 0..<mySampleN
    sortSuffixes(cfg, text, Sample, n=n, maxPrefix=cover.period);

    return Sample;
  } else {
    halt("Option 2 not ready");
    /*

    // option 2: partition the sample for parallelism and to
    // start the sorting process without requiring random access.
    // This should make caching data with the offsets unnecessary.
    type wordType = uint(64);

    record myPrefixComparator {
      proc keyPart(a: offsetAndCached(?), i: int):(int(8), wordType) {
        if a.cacheType == wordType {
          return getKeyPart(a, i=i, cfg, thetext, n=n, maxPrefix=cover.period);
        } else {
          return getKeyPartForOffset(a.offset, i=i, cfg, wordType=wordType,
                                     text, n, maxPrefix=cover.period);
        }
      }
      proc keyPart(a: prefix(?), i: int):(int(8), wordType) {
        return getKeyPartForPrefix(a, i);
      }
    }

    record offsetProducer {
      proc this(i: offsetType) {
        return makeSampleOffset(offsetType, cachedDataType, i,
                                text, n, cover);
      }
    }

    const comparator = new myPrefixComparator();
    const InputProducer = new offsetProducer();

    // first, create a sample of offsets in the cover
    var randNums = new randomStream(cfg.offsetType);
    var SplittersSampleDom = {0..<nToSampleForSplitters};
    var SplittersSample:[0..<nToSampleForSplitters] prefix;
    for (x, r) in zip(SplittersSample,
                      randNums.next(SplittersSampleDom, 0, sampleN-1)) {
      // r is a packed index into the offsets to sample
      // we have to unpack it to get the regular offset
      const whichPeriod = r / cover.sampleSize;
      const phase = r % cover.sampleSize;
      const coverVal = cover.cover[phase]:offsetType;
      const unpackedIdx = whichPeriod * cover.period + coverVal;
      x = makePrefix(unpackedIdx, text, n, cover);
    }

    // next, sort the sample and create the splitters
    const sp = new splitters(SplittersSample, nBuckets, comparator,
                             sampleIsSorted=false);

    // clear SplittersSample since it is no longer needed
    SplittersSampleDom = {1..0};

    // now, count the number in each bin by traversing over the input

    // Divide the input into nTasks chunks.
    const countsSize = nTasks * nBuckets;
    const blockSize = divCeil(sampleN, nTasks);
    const nBlocks = divCeil(sampleN, blockSize);

    coforall tid in 0..#nTasks with (ref state) {
      var start = tid * blockSize;
      var end = start + blockSize - 1;
      if end > sampleN - 1 {
        end = sampleN - 1;
      }

      ref counts = state.localState[tid].localCounts;

      for bin in 0..#nBuckets {
        counts[bin] = 0;
      }

      var cur = start;
      while cur < end {
        param tupSize = Partitioning.classifyUnrollFactor;
        var tup: tupSize * offsetAndCached(offsetType, cachedDataType);
        var nFilled = min(tupSize, end - cur);
        // fill in the tuple elements
        for j in 0..<tupSize {
          if j < nFilled {
            tup[j] = makeSampleOffset(offsetType, cachedDataType, cur+j,
                                      text, n, cover);
          }
        }
        // classify these tuple elements
        for (_,bin) in sp.classify(tup, 0, nFilled, comparator) {
          counts[bin] += 1;
        }
      }
    }

    var b = new bucketizer(nTasks=parTasks, numBuckets=sp.numBuckets);
    */
  }
}

/** Create and return a sorted suffix array for the suffixes 0..<n
    referring to 'text'. */
proc ssortDcx(const cfg:ssortConfig(?), const text, n: cfg.offsetType)
 : [0..<n] offsetAndCached(cfg.offsetType, cfg.cachedDataType) {

  // figure out how big the sample will be, including a 0 after each mod
  type offsetType = cfg.offsetType;
  type cachedDataType = cfg.cachedDataType;
  const cover = cfg.cover;
  const charsPerMod = 1 + myDivCeil(n, cover.period);
  const sampleN = cover.sampleSize * charsPerMod;

  if TRACE {
    writeln("in ssortDcx ", cfg.type:string, " n=", n);
  }

  if text.domain.low != 0 {
    halt("sortDcx expects input array to start at 0");
  }
  if n + INPUT_PADDING > text.size {
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
    return computeSuffixArrayDirectly(text, n,
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

  var parTasks = computeNumTasks() * text.targetLocales().size;
  var nBuckets = 8 * parTasks;

  //// Step 1: Sort Sample Suffixes ////

  // begin by computing the input text for the recursive subproblem
  var SampleText:[0..<sampleN+INPUT_PADDING] subCfg.characterType;
  var allSamplesHaveUniqueRanks = false;
  //var SampleSplitters: [0..<nSplitters] offsetAndCached(offsetType, cachedDataType);
  {
    var mySampleN: offsetType;
    const Sample = sortSampleOffsets(cfg, text, n,
                                     nTasks=parTasks, nBuckets=nBuckets,
                                     /*out*/ mySampleN);

    // create an array storing offsets at sample positions
    /*var mySampleN: offsetType;
    var Sample = buildSampleOffsets(offsetType=offsetType,
                                    cachedDataType=cfg.cachedDataType,
                                    text, n, cover, /*out*/ mySampleN);
    // then sort the these by the first cover.period characters;
    // note that these offsets are in 0..<n not 0..<mySampleN
    sortSuffixes(cfg, text, Sample, n=n, maxPrefix=cover.period);*/



    // now, compute the rank of each of these. we need to compare
    // the first cover.period characters & assign different ranks when these
    // differ.
    // NOTE: this is the main place where caching in OffsetAndCached
    // can offer some benefit.
    // TODO: skip the temporary array once Chapel issue #12482 is addressed
    const Tmp = [i in Sample.domain]
                  prefixDiffersFromPrevious(cfg,
                                            i,
                                            Sample, text, n,
                                            maxPrefix=cover.period);

    // note: inclusive scan causes Ranks[0] to be 1, so Ranks is 1-based
    const Ranks = + scan Tmp;

    allSamplesHaveUniqueRanks = Ranks.last == mySampleN + 1;

    // create the input for the recursive subproblem from the offsets and ranks
    SampleText = 0; // PERF TODO: noinit it
                    // and write a loop to zero what is not initalized below

    forall (offset, rank) in zip(Sample, Ranks) {
      // offset is an unpacked offset. find the offset in
      // the recursive problem input to store the rank into.
      // Do so in a way that arranges for SampleText to consist of
      // all sample inputs at a particular mod, followed by other modulus.
      // We have charsPerMod characters for each mod in the cover.
      const whichPeriod = offset.offset / cover.period;
      const phase = offset.offset % cover.period;
      const coverIdx = cover.coverIndex(phase);
      if EXTRA_CHECKS then assert(0 <= coverIdx && coverIdx < cover.sampleSize);
      const useIdx = coverIdx * charsPerMod + whichPeriod;

      // this is not a data race because Sample.offsets are a permutation
      // of the offsets.
      SampleText[useIdx] = rank;
    }

    if allSamplesHaveUniqueRanks {
      // create the splitters
    } else {
      // create splitters that won't be used

    }
  }

  // No need to recurse if all offsets had unique Ranks
  // i.e. each character in SampleText occurs only once
  // i.e. each character in SampleText is already the rank

  if !allSamplesHaveUniqueRanks {
    // recursively sort the sample suffixes
    const SubSA = ssortDcx(subCfg, SampleText, sampleN);
    // Replace the values in SampleText with
    // 1-based ranks from the suffix array.
    forall (offset,rank) in zip(SubSA, SubSA.domain) {
      SampleText[offset.offset] = rank+1;
    }
  }

  //// Step 2: Sort everything all together ////

  // TODO: * create splitters to make bins for parallelism
  //       * count number of suffixes in each bin with a pass over text
  //       * distributed suffixes to bins with a pass over text
  //       * forall bins
  //           consider splitters for min/max so that we can
  //               skip lookups into text entirely in some cases
  //           copy to scratch space with lookup next text
  //           in scratch, sort sample, sort each nonsample independently
  //           heap-based merge of these sorted lists back to bin

  // TODO: investigate radix sort + comparison on ties
  // TODO: investigate parallel multi-way merge

  if !PARTITION_SORT_ALL {
    // simple sort of everything all together
    var SA = buildAllOffsets(offsetType=offsetType,
                             cachedDataType=cfg.cachedDataType,
                             text, n);

    record myComparator {
      inline proc offsetToSampleOffset(i: offsetType) {
        const mod = i % cover.period;
        const whichPeriod = i / cover.period;
        const sampleIdx = cover.coverIndex(mod);
        return charsPerMod * sampleIdx + whichPeriod;
      }
      proc compare(a: offsetAndCached(?), b: offsetAndCached(?)) {
        // first, compare the first cover.period characters of text
        const prefixCmp = comparePrefixes(a, b, cfg, text, n, cover.period);
        if prefixCmp != 0 {
          return prefixCmp;
        }
        // if the prefixes are the same, compare the nearby sample
        // rank from the recursive subproblem.
        const k = cover.findInCover(a.offset % cover.period,
                                    b.offset % cover.period);
        const aSampleOffset = offsetToSampleOffset(a.offset + k);
        const bSampleOffset = offsetToSampleOffset(b.offset + k);
        const rankA = SampleText[aSampleOffset];
        const rankB = SampleText[bSampleOffset];
        return rankA - rankB;
      }
    }

    sort(SA, new myComparator());

    fixTrailingZeros(text, n, SA,
                   characterType=cfg.characterType,
                   offsetType=offsetType,
                   cachedDataType=cfg.cachedDataType);

    return SA;

  } else {
    // compute splitters from the sample
    halt("Option 2All not ready");
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


}
