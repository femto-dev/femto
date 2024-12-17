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

  femto/src/ssort_chpl/Partitioning.chpl
*/

module Partitioning {


// This code is based upon Chapel's package module Sort SampleSortHelp module
// which in turn was based on the IPS4 implementation

import SuffixSort.EXTRA_CHECKS;

use Utility;

import Reflection.canResolveMethod;
import Sort.{sort, DefaultComparator, keyPartStatus};
import Math.{log2, divCeil};
import CTypes.c_array;
import BlockDist.blockDist;

// These settings control the sample sort and classification process
param classifyUnrollFactor = 7;
const equalBucketThreshold = 5;

// compute logarithm base 2 rounded down
proc log2int(n: int) {
  if n <= 0 then
    return 0;
  return log2(n);
}

// compare two records according to a comparator, but allow them
// to be different types.
inline proc mycompare(a, b, comparator) {
  if canResolveMethod(comparator, "key", a) &&
     canResolveMethod(comparator, "key", b) {
    // Use the default comparator to compare the integer keys
    const d = new DefaultComparator();
    return d.compare(comparator.key(a), comparator.key(b));
  // Use comparator.compare(a, b) if is defined by user
  } else if canResolveMethod(comparator, "compare", a, b) {
    return comparator.compare(a ,b);
  } else if canResolveMethod(comparator, "keyPart", a, 0) &&
            canResolveMethod(comparator, "keyPart", b, 0) {
    return myCompareByPart(a, b, comparator);
  } else {
    compilerError("The comparator " + comparator.type:string + " requires a 'key(a)', 'compare(a, b)', or 'keyPart(a, i)' method");
  }
}

private inline proc myCompareByPart(a, b, comparator) {
  var curPart = 0;
  while true {
    var (aSection, aPart) = comparator.keyPart(a, curPart);
    var (bSection, bPart) = comparator.keyPart(b, curPart);
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

  // This is never reached. The return below is a workaround for issue #10447.
  return 1;
}

/* This enum describes to what extent the sample is already sorted */
enum sortLevel {
  unsorted,
  approximately,
  fully
}

// Compute splitters from a sorted sample.
// Returns an array of splitters that is of size 2**n,
// where only the first 2**n-1 elements are used.
// Assumes that SortedSample is 0-based and non-strided.
private proc computeSplitters(const SortedSample,
                              in requestedNumBuckets: int,
                              comparator,
                              reSort: bool,
                              out useEqualBuckets: bool) {
  if requestedNumBuckets > SortedSample.size then
    requestedNumBuckets = SortedSample.size;
  var myNumBuckets = max(2, 1 << log2int(requestedNumBuckets));
  var numSplitters = myNumBuckets-1;
  const perSplitter = SortedSample.size:real / (numSplitters+1):real;
  var SortedSplitters:[0..<myNumBuckets] SortedSample.eltType;

  var start = perSplitter:int;

  for i in 0..<numSplitters {
    var sampleIdx = start + (i*perSplitter):int;
    sampleIdx = min(max(sampleIdx, 0), SortedSample.size-1);
    SortedSplitters[i] = SortedSample[sampleIdx];
  }

  if reSort {
    sort(SortedSplitters[0..<numSplitters], comparator);
    if EXTRA_CHECKS {
      assert(isSorted(SortedSplitters[0..<numSplitters], comparator));
    }
  }

  // check for duplicates.
  var nDuplicates = 0;
  for i in 1..<numSplitters {
    if mycompare(SortedSplitters[i-1], SortedSplitters[i], comparator) == 0 {
      nDuplicates += 1;
    }
  }

  // if there are no duplicates, proceed with what we have
  if nDuplicates == 0 {
    useEqualBuckets = false;
    return SortedSplitters;
  }

  // copy the last element to make the following code simpler
  // (normally we leave space in the last element for use in build())
  SortedSplitters[numSplitters] = SortedSplitters[numSplitters-1];

  // if there were duplicates, reduce the number of splitters accordingly,
  // activate equality buckets, and return a de-duplicated array.
  const nUnique = numSplitters - nDuplicates;
  // keep the same number of buckets if there were not too many duplicates
  const oldNumBuckets = myNumBuckets;
  myNumBuckets = min(oldNumBuckets, max(2, 1 << (1+log2int(nUnique))));
  numSplitters = myNumBuckets-1;
  var UniqueSplitters:[0..<myNumBuckets] SortedSample.eltType;
  UniqueSplitters[0] = SortedSplitters[0];
  var next = 1;
  for i in 1..<oldNumBuckets {
    if next >= numSplitters then break;
    if mycompare(UniqueSplitters[next-1], SortedSplitters[i], comparator) != 0 {
      UniqueSplitters[next] = SortedSplitters[i];
      next += 1;
    }
  }

  // repeat the last splitter to get to the power of 2
  // note: myNumBuckets-1 is not set here, it is set in build()
  while next < numSplitters {
    UniqueSplitters[next] = UniqueSplitters[next-1];
    next += 1;
  }

  useEqualBuckets = true;
  return UniqueSplitters;
}

/*
   The splitters record helps with distribution sorting, where input elements
   are split among buckets according to how they compares with a group of
   splitter elements.

   It creates a binary comparison tree and uses that to classify the input in an
   optimized manner.
 */
record splitters : writeSerializable {
  type eltType;

  var logBuckets: int;
  var myNumBuckets: int;
  var equalBuckets: bool;

  // filled from 1..<myNumBuckets
  var storage: [0..<myNumBuckets] eltType;
  // filled from 0..myNumBuckets-2; myNumBuckets-1 is a duplicate of previous
  var sortedStorage: [0..<myNumBuckets] eltType;

  proc init(type eltType) {
    // default init, creates invalid splitters, but useful for replicating
    this.eltType = eltType;
  }

  // Create splitters based on some precomputed, already sorted splitters
  // useSplitters needs to be of size 2**n and the last element will
  // not be used.
  // Assumes that UseSplitters starts at 0 and is not strided.
  proc init(in UseSplitters: [], useEqualBuckets: bool) {
    assert(UseSplitters.size >= 2);
    this.eltType = UseSplitters.eltType;
    this.logBuckets = log2int(UseSplitters.size);
    this.myNumBuckets = 1 << logBuckets;
    assert(this.myNumBuckets == UseSplitters.size);
    assert(this.myNumBuckets >= 2);
    this.equalBuckets = useEqualBuckets;
    this.sortedStorage = UseSplitters;
    init this;

    // Build the tree in 'storage'
    this.build();
  }

  // create splitters based upon a sample of data.
  proc init(const Sample,
            requestedNumBuckets: int,
            comparator,
            param howSorted: sortLevel) where howSorted!=sortLevel.unsorted {
    var useEqualBuckets = false;
    const Splitters = computeSplitters(Sample, requestedNumBuckets,
                                       comparator,
                                       reSort=
                                         (howSorted==sortLevel.approximately),
                                       /*out*/ useEqualBuckets);

    this.init(Splitters, useEqualBuckets);
  }

  // create splitters based upon a sample of data by sorting it
  proc init(ref Sample:[],
            requestedNumBuckets: int,
            comparator,
            param howSorted: sortLevel) where howSorted==sortLevel.unsorted {
    // sort the sample
    sort(Sample, comparator);

    var useEqualBuckets = false;
    const Splitters = computeSplitters(Sample, requestedNumBuckets,
                                       comparator, reSort=false,
                                       /*out*/ useEqualBuckets);

    this.init(Splitters, useEqualBuckets);
  }

  proc serialize(writer, ref serializer) throws {
    writer.write("splitters(");
    writer.write("\n logBuckets=", logBuckets);
    writer.write("\n myNumBuckets=", myNumBuckets);
    writer.write("\n equalBuckets=", equalBuckets);
    writer.write("\n storage=");
    for i in 0..<myNumBuckets {
      writer.write((try! " %xt".format(storage[i])));
    }
    writer.write("\n sortedStorage=");
    for i in 0..<myNumBuckets {
      writer.write(try! " %xt".format(sortedStorage[i]));
    }
    writer.write(")\n");
  }

  proc numBuckets {
    if equalBuckets {
      return myNumBuckets*2-1;
    } else {
      return myNumBuckets;
    }
  }

  proc hasEqualityBuckets {
    return equalBuckets;
  }

  proc bucketHasLowerBound(bucketIdx: int) {
    // bucket 0 never has a lower bound
    if bucketIdx == 0 {
      return false;
    }
    // the equality buckets are odd buckets
    if equalBuckets {
      return bucketIdx % 2 == 0;
    } else {
      return true;
    }
  }
  // things in the bucket are > this
  proc bucketLowerBound(bucketIdx: int) const ref {
    if equalBuckets {
      return sortedSplitter(bucketIdx/2-1);
    } else {
      return sortedSplitter(bucketIdx-1);
    }
  }

  proc bucketHasUpperBound(bucketIdx: int) {
    if equalBuckets {
      if bucketIdx >= 2*myNumBuckets-2 {
        return false;
      }
      return bucketIdx % 2 == 0; // odd buckets are equality buckets
    } else {
      if bucketIdx >= myNumBuckets-1 {
        return false;
      }
      return true;
    }
  }
  // things in the bucket are <= the result of this function
  // (actually, < the result, if equality buckets are in use)
  proc bucketUpperBound(bucketIdx: int) const ref {
    if equalBuckets {
      return sortedSplitter(bucketIdx/2);
    } else {
      return sortedSplitter(bucketIdx);
    }
  }

  proc bucketHasEqualityBound(bucketIdx: int) {
    if equalBuckets {
      return bucketIdx % 2 == 1;
    }
    return false;
  }
  // things in the bucket are < the result of this function
  proc bucketEqualityBound(bucketIdx: int) const ref {
    return sortedSplitter((bucketIdx-1)/2);
  }

  // Build the tree from the sorted splitters
  // logBuckets does not account for equalBuckets.
  proc ref build() {
    // Copy the last element
    sortedStorage[myNumBuckets-1] = sortedStorage[myNumBuckets-2];
    build(0, myNumBuckets-1, 1);
  }

  // Recursively builds the tree
  proc ref build(left: int, right: int, pos: int) {
    var mid = left + (right - left) / 2;
    storage[pos] = sortedStorage[mid];
    if 2*pos < myNumBuckets {
      build(left, mid, 2*pos);
      build(mid, right, 2*pos + 1);
    }
  }

  inline proc splitter(i:int) const ref : eltType {
    return storage[i];
  }
  inline proc sortedSplitter(i:int) const ref : eltType {
    return sortedStorage[i];
  }

  proc bucketForRecord(a, comparator) {
    var bk = 1;
    for lg in 0..<logBuckets {
      bk = 2*bk + (mycompare(splitter(bk), a, comparator) < 0):int;
    }
    if equalBuckets {
      bk = 2*bk + (mycompare(sortedSplitter(bk-myNumBuckets), a, comparator) == 0):int;
    }
    return bk - (if equalBuckets then 2*myNumBuckets else myNumBuckets);
  }
  // yields (value, bucket index) for start_n..end_n
  // gets the elements by calling Input[i] to get element i
  // Input does not have to be an array, but it should have an eltType.
  iter classify(Input, start_n, end_n, comparator) {
    const paramEqualBuckets = equalBuckets;
    const paramLogBuckets = logBuckets;
    const paramNumBuckets = 1 << (paramLogBuckets + paramEqualBuckets:int);
    var b:c_array(int, classifyUnrollFactor);
    var elts:c_array(Input.eltType, classifyUnrollFactor);

    var cur = start_n;
    // Run the main (unrolled) loop
    while cur <= end_n-(classifyUnrollFactor-1) {
      for /*param*/ i in 0..classifyUnrollFactor-1 {
        b[i] = 1;
        elts[i] = Input[cur+i];
      }
      for /*param*/ lg in 0..paramLogBuckets-1 {
        for /*param*/ i in 0..classifyUnrollFactor-1 {
          b[i] = 2*b[i] +
                 (mycompare(splitter(b[i]), elts[i],comparator)<0):int;
        }
      }
      if paramEqualBuckets {
        for /*param*/ i in 0..classifyUnrollFactor-1 {
          b[i] = 2*b[i] +
                 (mycompare(sortedSplitter(b[i] - paramNumBuckets/2),
                            elts[i],
                            comparator)==0):int;
        }
      }
      for /*param*/ i in 0..classifyUnrollFactor-1 {
        yield (elts[i], b[i]-paramNumBuckets);
      }
      cur += classifyUnrollFactor;
    }
    // Handle leftover
    while cur <= end_n {
      elts[0] = Input[cur];
      var bk = 1;
      for lg in 0..<paramLogBuckets {
        bk = 2*bk + (mycompare(splitter(bk), elts[0], comparator)<0):int;
      }
      if paramEqualBuckets {
        bk = 2*bk + (mycompare(sortedSplitter(bk - paramNumBuckets/2),
                               elts[0],
                               comparator)==0):int;
      }
      yield (elts[0], bk - paramNumBuckets);
      cur += 1;
    }
  }
} // end record splitters

class PerTaskState {
  var nBuckets: int;
  var localCounts: [0..<nBuckets] int;
  proc init(nBuckets: int) {
    this.nBuckets = nBuckets;
  }
}

/*
   Stores the elements Input[InputDomain] in a partitioned manner
   into Output[OutputDomain].

   InputDomain and OutputDomain must not be strided. The must be
   local rectangular domains or Block distributed domains.

   Input can be an array over InputDomain or something that simulates
   an array with a 'proc this' and an 'eltType' to generate element i.

   Output is expected to be an array over OutputDomain.
   If Output is 'none', this function will only count,
   and skip the partition step.

   'filterBucket' provides a mechanism to only process certain buckets.
   If 'filterBucket' is provided and not 'none', it will be called as
   'filterBucket(bucketForRecord(Input[i]))' to check if that bucket should
   be processed. Only elements where it returns 'true' will be processed.

   Return an array of counts to indicate how many elements
   ended up in each bucket.

   This is done in parallel.

   'split' is the splitters and it should be either 'record splitters'
   or something else that behaves similarly to it.
   'rsplit' should be the result of calling 'replicate()' on 'split';
    as such it should be 'none' when this code is to run locally.

   If equality buckets are not in use:
     Bucket 0 consists of elts with
       elts <= split.sortedSplitter(0)
     Bucket 1 consists of elts with
       split.sortedSplitter(0) < elts <= split.sortedSplitter(1)
     ...
     Bucket i consists of elts with
       split.sortedSplitter(i-1) < elts <= split.sortedSplitter(i)
     ...
     Bucket nBuckets-1 consits of elt with
       split.sortedSplitter(numBuckets-2) < elts

   If equality buckets are in use:
     Bucket 0 consists of elts with
       elts < split.sortedSplitter(0)
     Bucket 1 consists of elts with
       elts == split.sortedSplitter(0)
     Bucket 2 consists of elts with
       split.sortedSplitter(0) < elts < split.sortedSplitter(1)
     Bucket 3 consists of elts with
       elts == split.sortedSplitter(1)
     Bucket 4 consists of elts with
       split.sortedSplitter(1) < elts < split.sortedSplitter(2)

     Bucket i, with i being even, consists of elts with
       split.sortedSplitter(i/2-1) < elts < split.sortedSplitter(i/2)
     Bucket i, with i being odd, consists of elts with
       elts == split.sortedSplitter((i-1)/2)

     Bucket nBuckets-2 consits of elt with
       elts == split.sortedSplitter((numBuckets-2)/2) < elts
     Bucket nBuckets-1 consits of elt with
       split.sortedSplitter((numBuckets-2)/2) < elts

 */
proc partition(const InputDomain: domain(?),
               const Input,
               const OutputDomain: domain(?),
               ref Output,
               split, rsplit, comparator,
               nTasksPerLocale: int = computeNumTasks(),
               filterBucket: ?t = none) {

  const nBuckets; // set below
  const ref locales =
    if rsplit.type == nothing then none else InputDomain.targetLocales();
  const nLocales =
    if locales.type == nothing then 1 else locales.size;
  const outputStart = OutputDomain.first;

  {
    // access the local replicand to do some checking and get # buckets
    const ref mysplit = getLocalReplicand(split, rsplit);
    nBuckets = mysplit.numBuckets;

    // do some checking / input validation
    if EXTRA_CHECKS {
      // check that the splitters are sorted according to comparator
      if isSubtype(mysplit.type,splitters) {
        assert(isSorted(mysplit.sortedStorage[0..<mysplit.myNumBuckets-1],
                        comparator));
      }
      // check that, if InputDomain is distributed, locales is not none
      if InputDomain.targetLocales().size > 1 {
        assert(locales.type != nothing);
      }
    }
    if filterBucket.type == nothing {
      assert(InputDomain.size == OutputDomain.size);
    }
    if OutputDomain.rank != 1 || OutputDomain.dim(0).strides != strideKind.one {
      compilerError("partition only supports non-strided 1-D OutputDomain");
    }
  }

  // Divide the input into nTasks chunks.
  const nTasks = nLocales * nTasksPerLocale;
  const countsSize = nTasks * nBuckets;

  // create local state arrays to be used by each task for the counting
  const tasksDom = makeBlockDomain(0..<nTasks, locales);
  var localState:[tasksDom] owned PerTaskState?;
  forall (taskId, _) in divideIntoTasks(InputDomain, nTasksPerLocale) {
    localState[taskId] = new PerTaskState(nBuckets);
  }

  // globalCounts stores counts like this:
  //   count for bin 0, task 0
  //   count for bin 0, task 1
  //   ...
  //   count for bin 1, task 0
  //   count for bin 1, task 1
  // i.e. bin*nTasks + taskId
  const globalCountsDom = makeBlockDomain(0..<countsSize, locales);
  var globalCounts:[globalCountsDom] int;

  // Step 1: Count
  forall (taskId, chunk) in divideIntoTasks(InputDomain, nTasksPerLocale) {
    ref counts = localState[taskId]!.localCounts;
    const ref mysplit = getLocalReplicand(split, rsplit);
    const taskStart = chunk.first;
    const taskEnd = chunk.last; // inclusive

    if EXTRA_CHECKS {
      // counts should already be 0 after allocation above
      for x in counts do assert(x==0);
    }

    // this loop must really be serial. it can be run in parallel
    // within the forall because it's updating state local to each task.
    for (_,bin) in mysplit.classify(Input, taskStart, taskEnd, comparator) {
      if filterBucket.type == nothing || filterBucket(bin) {
        counts[bin] += 1;
      }
    }

    // Now store the counts into the global counts array
    foreach bin in 0..<nBuckets {
      globalCounts[bin*nTasks + taskId] = counts[bin];
    }
  }

  if Output.type != nothing {
    // Step 2: Scan
    const globalEnds = + scan globalCounts;

    // Step 3: Distribute
    forall (taskId, chunk) in divideIntoTasks(InputDomain, nTasksPerLocale) {
      ref nextOffsets = localState[taskId]!.localCounts;
      const ref mysplit = getLocalReplicand(split, rsplit);
      const taskStart = chunk.first;
      const taskEnd = chunk.last; // inclusive

      // initialize nextOffsets
      foreach bin in 0..<nBuckets {
        var globalBin = bin*nTasks + taskId;
        nextOffsets[bin] = if globalBin > 0
                           then outputStart + globalEnds[globalBin-1]
                           else outputStart;
      }

      // as above,
      // this loop must really be serial. it can be run in parallel
      // within the forall because it's updating state local to each task.
      for (elt,bin) in mysplit.classify(Input, taskStart, taskEnd, comparator) {
        if filterBucket.type == nothing || filterBucket(bin) {
          // Store it in the right bin
          ref next = nextOffsets[bin];
          Output[next] = elt;
          next += 1;
        }
      }
    }
  }

  // Compute the total counts to return them
  const countsDom = makeBlockDomain(0..<nBuckets, locales);
  var counts:[countsDom] int;
  forall (c,bin) in zip(counts,countsDom) {
    var total = 0;
    for tid in 0..<nTasks {
      total += globalCounts[bin*nTasks + tid];
    }
    counts[bin] = total;
  }

  return counts;
}

/*
  serial insertionSort with a separate array of already-computed keys
 */
/*proc insertionSort(ref elts: [], ref keys: [], region: range) {
  // note: insertionSort should be stable
  const low = region.low,
        high = region.high;

  for i in low..high {
    const keyi = keys[i];
    const elti = elts[i];
    var inserted = false;
    for j in low..i-1 by -1 {
      const keyj = keys[j];
      if keyi < keyj {
        keys[j+1] = keyj;
        elts[j+1] = elts[j];
      } else {
        keys[j+1] = keyi;
        elts[j+1] = elti;
        inserted = true;
        break;
      }
    }
    if (!inserted) {
      keys[low] = keyi;
      elts[low] = elti;
    }
  }
}*/

/** serial shellSort with a separate array of already-computed keys */
/*proc shellSort(ref elts: [], ref keys: [], region: range) {
  // note: shellSort is not stable
  const start = region.low,
        end = region.high;

  // Based on Sedgewick's Shell Sort -- see
  // Analysis of Shellsort and Related Algorithms 1996
  // and see Marcin Ciura - Best Increments for the Average Case of Shellsort
  // for the choice of these increments.
  var js, hs: int;
  var keyi: keys.eltType;
  var elti: elts.eltType;
  const incs = (701, 301, 132, 57, 23, 10, 4, 1);
  for h in incs {
    hs = h + start;
    for is in hs..end {
      keyi = keys[is];
      elti = elts[is];
      js = is;
      while js >= hs && keyi < keys[js-h] {
        keys[js] = keys[js - h];
        elts[js] = elts[js - h];
        js -= h;
      }
      keys[js] = keyi;
      elts[js] = elti;
    }
  }
}*/

/*
  An serial LSB-radix sorter that sorts keys that have been already collected.

  'keys' must be an arrays of unsigned integral type.

  'region' indicates the portion of 'elts' / 'keys' to sort.
 */
/*
proc lsbRadixSort(ref elts: [], ref keys: [], region: range,
                  ref eltsSpace: [], ref keysSpace: [],
                  ref counts: [] int, param bitsPerPass) {
  type t = keys.eltType;
  param nPasses = divCeil(numBits(t), bitsPerPass);
  const bucketsPerPass = 1 << bitsPerPass;
  const maxBucket = nPasses*bucketsPerPass;

  // check that the counts array is big enough
  assert(counts.domain.contains(0));
  assert(counts.domain.contains(maxBucket-1));
  assert(counts.size >= maxBucket);

  if !isUintType(keys.eltType) {
    compilerError("keys.eltType must be an unsigned int type in lsbRadixSort");
  }

  // initialize the counts
  for i in 0..<maxBucket {
    counts = 0;
  }

  // count all of the passes at once
  const mask = bucketsPerPass - 1;
  for i in region {
    const key = keys[i];
    for param pass in 0..<nPasses {
      const startBucket = pass*bucketsPerPass;
      // get the appropriate bitsPerPass from the key
      // since this is an LSB sort, pass 0 should get the bottom bits
      const shift = pass*bitsPerPass;
      const bkt = (key >> shift) & mask;
      counts[(startBucket + bkt):int] += 1;
    }
  }

  // handle the scan + distribute for each pass
  for pass in 0..<nPasses {
    const startBucket = pass*bucketsPerPass;
    // compute the start positions for each bucket
    // this is an exclusive scan, but start from region.low,
    // so that these form the initial output positions for each bucket.
    var total = region.low;
    for bkt in 0..<bucketsPerPass {
      ref x = counts[startBucket + bkt];
      const c = x; // read the current count
      x = total;   // set the current count to the total
      total += c;  // add to total
    }

    // distribute
    // pass 0 reads elts and writes eltsSpace
    // pass 1 reads eltsSpace and writes elts
    // ...
    // data ends up in elts as long as nPasses is even, which is checked above
    const ref inputElts = if pass % 2 == 0 then elts else eltsSpace;
    const ref inputKeys = if pass % 2 == 0 then keys else keysSpace;
    ref outputElts = if pass % 2 == 0 then eltsSpace else elts;
    ref outputKeys = if pass % 2 == 0 then keysSpace else keys;
    for i in region {
      const key = inputKeys[i];
      const elt = inputElts[i];
      const shift = pass*bitsPerPass;
      const bkt = (key >> shift) & mask;
      ref x = counts[(startBucket + bkt):int];
      // store the key into the appropriate bucket
      const outIdx = x;
      outputKeys[outIdx] = key;
      outputElts[outIdx] = elt;
      // increment the bucket counter
      x += 1;
    }
  }

  if nPasses % 2 != 0 {
    elts[region] = eltsSpace[region];
    keys[region] = keysSpace[region];
  }
}*/

// mark the boundaries in boundaries when elt[i-1] != elt[i]
proc markBoundaries(keys, ref boundaries: [], region: range) {
  const start = region.low;
  const end = region.high;
  var cur = start;
  type t = boundaries.eltType;

  // handle bits until the phase becomes aligned
  while cur <= end {
    var phase = cur % numBits(t);
    if phase == 0 {
      break;
    }
    // otherwise, handle index 'start' and increment it
    if cur == start || keys[cur-1] != keys[cur] {
      setBit(boundaries, cur);
    }
    cur += 1;
  }

  // handle setting a word at a time
  while cur + numBits(t) <= end {
    // handle numBits(t) at a time
    var word:t = 0;
    var wordIdx = cur / numBits(t);
    for i in 0..<numBits(t) {
      var bit: t = 0;
      if cur == start {
        bit = 1;
      } else if keys[cur-1] != keys[cur] {
        bit = 1;
      }
      word <<= 1; // make room for the bit
      word |= bit; // add in the bit
      cur += 1;
    }
    boundaries[wordIdx] = word;
  }

  // handle any leftover bits
  while cur <= end {
    if cur == start || keys[cur-1] != keys[cur] {
      setBit(boundaries, cur);
    }
    cur += 1;
  }
}

/*
  A radix sorter that uses a separate keys array and tracks where equal elements
  occur in the sorted output.

  'keys' and 'boundaries' must be an arrays of unsigned integral type.

  'region' indicates the portion of 'elts' / 'keys' to sort.

  Bits will be set in 'boundaries' to track whether elements differed in the
  sorted result. In particular, if the process of computing the sorted result
  revealed that 'elt[i-1] != elt[i]', then bit 'i' will be set in boundaries
  (note that boundaries is storing unsigned ints that record multiple such
  bits).

  The boundary for element 0 will always be marked.

TODO: the standard library sorter is quite a lot faster
      even with the memory allocation. need to shift to just
      using that.

 */
/*proc sortAndTrackEqual(ref elts: [], ref keys: [], ref boundaries: [],
                       region: range,
                       ref eltsSpace: [], ref keysSpace: [],
                       ref counts: [] int) {
  if !isUintType(keys.eltType) {
    compilerError("radixSortAndTrackEqual requires unsigned integer keys");
  }
  if !isUintType(boundaries.eltType) {
    compilerError("radixSortAndTrackEqual requires unsigned integer keys");
  }

  if region.size == 0 {
    return;
  } else if region.size == 1 {
    markBoundaries(keys, boundaries, region);
    return;
  } else if region.size == 2 {
    const i = region.low;
    const j = region.high;
    if keys[i] > keys[j] {
      keys[i] <=> keys[j];
      elts[i] <=> elts[j];
    }
    markBoundaries(keys, boundaries, region);
    return;
  } else if region.size <= 16 {
    insertionSort(elts, keys, region);
    markBoundaries(keys, boundaries, region);
    return;
  } else if region.size <= 2000 {
    shellSort(elts, keys, region);
    markBoundaries(keys, boundaries, region);
    return;
  } else if region.size <= 1 << 16 {
    lsbRadixSort(elts, keys, region, eltsSpace, keysSpace, counts,
                 bitsPerPass=8);
    markBoundaries(keys, boundaries, region);
    return;
  } else {
    lsbRadixSort(elts, keys, region, eltsSpace, keysSpace, counts,
                 bitsPerPass=16);
    markBoundaries(keys, boundaries, region);
    return;
  }
}*/


/* Use a tournament tree (tree of losers) to perform multi-way merging.
   This does P-way merging, assuming that the P ranges in InputRanges
   represent the P sorted regions. OutputRange represents where the
   output should be placed in the Output array and should have a matching size.

   The type readEltType will be used for storing the element for comparison
   in the tournament tree. It might be useful for it to be a different type
   from eltType (e.g. if eltType are offsets into another array or otherwise
   pointers, it might be useful to store full records in the tournament tree).
   If readEltType differs from eltType, this code will cast (with operator : )
   from eltType to readEltType and back again.
   */
proc multiWayMerge(Input: [] ?eltType,
                   InputRanges: [] range,
                   ref Output: [] ?outEltType,
                   outputRange: range,
                   comparator,
                   type readEltType=eltType) {
  const P = InputRanges.size;

  if P <= 1 {
    // Copy the input ranges to the output
    var pos = outputRange.low;
    for r in InputRanges {
      for i in r {
        Output[pos] = Input[i]:outEltType;
        pos += 1;
      }
    }
    return;
  }

  var InternalNodes: [0..<P] int; // integer indices into ExternalNodes
                                   // indicating what the loser was,
                                   // except Losers[0] is the winner of the
                                   // tournament

  // We will store the tree in the order described in Knuth vol.
  // Sorting and Searching:
  //
  // This is the example of the internal nodes of a 12-node tree,
  // followed by the external nodes, which start with e, but continue
  // the numbering:
  //                              1
  //                  2                             3
  //         4                 5               6        7
  //    8        9        10       11       e12 e13  e14 e15
  // e16 e17  e18 e19  e20 e21  e22 e23

  // some observations about this way of numbering nodes:
  //  * for node i, the parent node number can be computed by i / 2
  //  * for node i, the child nodes are 2*i and 2*i + 1
  //  * the leftmost node in each row is a power of 2
  //  * there are always an even number of elements in the bottom row

  // these are numbered P..<2*P to match the external node numbering above
  // the element 2*P is also included to allow the algorithm to consider
  // that the "infinity" element without too much fuss.
  var ExternalNodes: [P..2*P] readEltType; // values that have been read
  var ReadPosition: [P..2*P] int; // index into Input for each sorted list
  var ReadEnd: [P..2*P] int; // end position for each Input list (inclusive)

  // Set up ReadPosition and ReadEnd, and read in the initial records
  for i in 0..<P {
    ReadPosition[P+i] = InputRanges[i].low;
    ReadEnd[P+i] = InputRanges[i].high;
    if ReadPosition[P+i] <= ReadEnd[P+i] {
      ExternalNodes[P+i] = Input[ReadPosition[P+i]]: readEltType;
    }
  }
  // Position/End for 2*P should represent an invalid range, so that
  // checks for end-of-sequence on infinity will say it's end-of-sequence.
  ReadPosition[2*P] = 1;
  ReadEnd[2*P] = 0;

  // compute the regular tournament tree (storing winners)
  var nRows = 2 + log2(P); // e.g. 5 rows for the example tree of 12
                           // Losers[0] is not considered a row

  var inf = 2*P; // how we represent ∞ in internal nodes,
                 // but ExternalNodes[inf] actually exists

  proc doCompare(eltA, eltB, addrA, addrB) {
    //writeln("doCompare ", eltA, " ", eltB, " ", addrA, " ", addrB);
    if addrB == inf {
      return -1; // a is less if b is infinity
    }
    if addrA == inf {
      return 1; // b is less if a is infinity
    }
    return mycompare(eltA, eltB, comparator);
  }

  // consider the rows in reverse order; we will compare elements
  for row in 1..<nRows by -1 {
    //writeln("Working on row ", row);

    const rowStart = 1 << row; // e.g., last row in example starts at 16
    const maxRowSize = 1 << row; // e.g. last row could have up to 16 elts
    const rowSize = min(maxRowSize, 2*P - rowStart);
    for i in rowStart..#rowSize by 2 {
      // compare element i with element i+1

      //writeln("i is ", i);

      // get a reference to the elements to compare
      const ref eltA = if i < P
                       then ExternalNodes[InternalNodes[i]]
                       else ExternalNodes[i];
      const ref eltB = if i+1 < P
                       then ExternalNodes[InternalNodes[i+1]]
                       else ExternalNodes[i+1];
      // what number will we store if the comparison indicates?
      // need to propagate a winner from the current InternalNode
      // if we are working on an internal node.
      const tmpAddrA = if i < P then InternalNodes[i] else i;
      const tmpAddrB = if i+1 < P then InternalNodes[i+1] else i+1;
      //writeln("tmpAddrA ", tmpAddrA);
      //writeln("tmpAddrB ", tmpAddrB);
      const addrA = if ReadPosition[tmpAddrA] <= ReadEnd[tmpAddrA]
                    then tmpAddrA
                    else inf;
      const addrB = if ReadPosition[tmpAddrB] <= ReadEnd[tmpAddrB]
                    then tmpAddrB
                    else inf;
      //writeln("addrA ", addrA);
      //writeln("addrB ", addrB);
      ref eltDst = InternalNodes[i/2];
      //writeln("Comparing ", addrA, " vs ", addrB);
      if doCompare(eltA, eltB, addrA, addrB) < 0 {
        //writeln("Setting node ", i/2, " to ", addrA);
        eltDst = addrA;
      } else {
        //writeln("Setting node ", i/2, " to ", addrB);
        eltDst = addrB;
      }
    }
  }
  // copy the champion to the top of the tree
  InternalNodes[0] = InternalNodes[1];

  //writeln("Winners tree");
  //writeln("InternalNodes ", ExternalNodes[InternalNodes]);

  // change the InternalNodes to store losers rather than winners
  // note that the order in which this loop executes is important
  // (since it reads from 2*i while setting i)
  for i in 1..<P {
    const left = 2*i;
    const right = 2*i + 1;
    const tmpAddrLeft =  if left < P then InternalNodes[left] else left;
    const tmpAddrRight = if right < P then InternalNodes[right] else right;
    const addrLeft =  if ReadPosition[tmpAddrLeft] <= ReadEnd[tmpAddrLeft]
                      then tmpAddrLeft
                      else inf;
    const addrRight = if ReadPosition[tmpAddrRight] <= ReadEnd[tmpAddrRight]
                      then tmpAddrRight
                      else inf;

    if InternalNodes[i] == addrLeft {
      // addrLeft was the winner, so store addrRight
      InternalNodes[i] = addrRight;
    } else if InternalNodes[i] == addrRight {
      // addrRight was the winner, so store addrLeft
      InternalNodes[i] = addrLeft;
    } else {
      assert(false && "problem constructing tournament tree");
    }
  }

  //writeln("Loser's tree");
  //writeln("InternalNodes ", ExternalNodes[InternalNodes]);


  var outPos = outputRange.low;
  while true {
    //writeln("looping");
    //writeln("InternalNodes ", InternalNodes);
    //writeln("ExtarnalNodes[InternalNodes] ", ExternalNodes[InternalNodes]);

    var championAddr = InternalNodes[0]; // index of external node in P..<2*P
    if championAddr == inf {
      break;
    }

    // output the champion
    //writeln("outputting ", ExternalNodes[championAddr]);
    Output[outPos] = ExternalNodes[championAddr] : outEltType;
    outPos += 1;

    // input the new value
    var championAddrOrInf = championAddr;
    ref ChampionPos = ReadPosition[championAddr];
    if ChampionPos+1 <= ReadEnd[championAddr] {
      ChampionPos += 1;
      ExternalNodes[championAddr] = Input[ChampionPos];
      //writeln("Read ", ExternalNodes[championAddr], " into ", championAddr);
    } else {
      championAddrOrInf = inf;
    }

    // move up the tree, adjusting the losers in InternalNodes
    // and updating championAddr based on the comparisons
    var i = championAddr / 2; // parent internal node
    while i >= 1 {
      //writeln("Setting Internal Node ", i);
      // championAddr is an outer variable loop, updated as needed
      const ref championElt = ExternalNodes[championAddrOrInf];

      ref Loser = InternalNodes[i];
      const otherAddr = Loser; // load the current value
      const ref otherElt = ExternalNodes[otherAddr];

      if doCompare(championElt, otherElt, championAddrOrInf, otherAddr) < 0 {
        // newElt has won, nothing to do:
        //  * championAddr is still correct
        //  * Loser is still correct
        //writeln("champion beats ", ExternalNodes[otherAddr]);
      } else {
        // otherElt has won, update the loser and champion
        Loser = championAddrOrInf;
        championAddrOrInf = otherAddr;
        //writeln("champion lost to ", ExternalNodes[otherAddr]);
      }

      i /= 2;
    }
    // store the champion back into the tree
    InternalNodes[0] = championAddrOrInf;
  }
}


} // end module Partitioning
