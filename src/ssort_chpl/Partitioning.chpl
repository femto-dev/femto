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
import Sort;
import Sort.{sort, defaultComparator, keyPartStatus, keyPartComparator};
use Random; // 'use' vs 'import' to workaround an issue
import Math.{log2, divCeil};
import CTypes.c_array;
import BlockDist.blockDist;
import CopyAggregation.{SrcAggregator,DstAggregator};

// These settings control the sample sort and classification process

// how much more should we sample to create splitters?
// 1.0 would be only to sample enough for the splitters
config const sampleRatio = 1.5;
config const seed = 1;

// switch to base case sort if number of elements is < nBuckets * this
config const partitionSortBaseCaseMultiplier = 100.0;

param CLASSIFY_UNROLL_FACTOR = 7;
const SAMPLE_RATIO = min(1.0, sampleRatio);
const SEED = seed;
const PARTITION_SORT_BASE_CASE_MULTIPLIER = partitionSortBaseCaseMultiplier;

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
    const d = new defaultComparator();
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

record integralKeyPartComparator : keyPartComparator {
  inline proc keyPart(elt: integral, i: int): (keyPartStatus, elt.type) {
    var section = if i > 0 then keyPartStatus.pre else keyPartStatus.returned;
    return (section, elt);
  }
}

inline proc myGetBin(a, comparator, startbit:int, radixBits:int) {
  if canResolveMethod(comparator, "keyPart", a, 0) {
    return myGetBinForKeyPart(a, comparator, startbit, radixBits);
  } else if canResolveMethod(comparator, "key", a) {
    return myGetBinForKeyPart(comparator.key(a),
                              new integralKeyPartComparator(),
                              startbit, radixBits);
  } else {
    compilerError("Bad comparator for radix sort ", comparator.type:string,
                  " with eltType ", a.type:string);
  }
}

// Get the bin for a record by calling comparator.keyPart
//
// p = 1 << radixBits
//
// bin 0 is for the end was reached (sort before)
// bins 1..p are for data with next part starting with 0..<p
// bin p+1 is for the end was reached (sort after)
//
// returns bin
inline proc myGetBinForKeyPart(a, comparator, startbit:int, radixBits:int) {
  // We have keyPart(element, start):(keyPartStatus, part which is integral)
  const testRet: comparator.keyPart(a, 0).type;
  const testPart = testRet(1); // get the numeric part
  param bitsPerPart = numBits(testPart.type);
  if EXTRA_CHECKS {
    assert(bitsPerPart >= radixBits);
    assert(bitsPerPart % radixBits == 0);
  }

  // startbit must be a multiple of radixBits because the radix
  // sort operates radixBits at a time.

  // startbit might be partway through a part (e.g. 16 bits into a uint(64))
  const whichpart = startbit / bitsPerPart;
  const bitsinpart = startbit % bitsPerPart;

  const (section, part) = comparator.keyPart(a, whichpart);
  var ubits = part:uint(bitsPerPart);
  // If the number is signed, invert the top bit, so that
  // the negative numbers sort below the positive numbers
  if isInt(part) {
    const one:ubits.type = 1;
    ubits = ubits ^ (one << (bitsPerPart - 1));
  }
  const mask:uint = (1 << radixBits) - 1;
  const ubin = (ubits >> (bitsPerPart - bitsinpart - radixBits)) & mask;

  if section:int == 0 then
    return ubin:int + 1; // a regular bin
  else if section:int < 0 then
    return 0; // the sort-before bin
  else
    return (1 << radixBits) + 1; // the sort-after bin
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
// If equality buckets are not in use, there will be 2**n buckets.
// If they are in use, there will be 2**(n+1) buckets.
// n will be chosen by this function so that the number of buckets
// is <= max(2,requestedNumBuckets).
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
  var SortedSplitters:[0..<myNumBuckets] SortedSample.eltType;

  // gather the sample assuming that SortedSample is sorted
  {
    const perSplitter = SortedSample.size:real / (numSplitters+1):real;
    var start = perSplitter:int;

    for i in 0..<numSplitters {
      var sampleIdx = start + (i*perSplitter):int;
      sampleIdx = min(max(sampleIdx, 0), SortedSample.size-1);
      SortedSplitters[i] = SortedSample[sampleIdx];
    }
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
  // note, when using equality buckets, the number of buckets
  // will be 2 * the number of splitters, so here we
  // are aiming for a smaller number of splitters.

  var oldNumSplitters = numSplitters;
  const nUnique = oldNumSplitters - nDuplicates;
  myNumBuckets = 1 << (1+log2int(nUnique));
  while 2*myNumBuckets > requestedNumBuckets {
    myNumBuckets /= 2;
  }
  myNumBuckets = max(1, myNumBuckets);
  numSplitters = myNumBuckets-1;

  var UniqueSplitters:[0..<myNumBuckets] SortedSample.eltType;

  var next = 0;

  // gather the sample from SortedSplitters
  {
    if nUnique <= myNumBuckets {
      // Gather the unique elements
      UniqueSplitters[0] = SortedSplitters[0];
      next = 1;
      for i in 1..<oldNumSplitters {
        // keep elements that differ from the last splitter added,
        // and discard elements that are the same.
        if mycompare(UniqueSplitters[next-1],
                     SortedSplitters[i], comparator) != 0 {
          UniqueSplitters[next] = SortedSplitters[i];
          next += 1;
        }
      }
    } else {
      // myNumBuckets < nUnique
      const perSplitter = nUnique:real / myNumBuckets:real;
      var start = perSplitter:int;

      next = 0;
      for i in 0..<oldNumSplitters {
        if next == numSplitters then break;
        var sampleIdx = start + (i*perSplitter):int;
        sampleIdx = min(max(sampleIdx, 0), SortedSplitters.size-1);
        if next == 0 ||
           mycompare(UniqueSplitters[next-1],
                     SortedSplitters[sampleIdx], comparator) != 0 {
          UniqueSplitters[next] = SortedSplitters[sampleIdx];
          next += 1;
        }
      }
    }
  }

  if EXTRA_CHECKS {
    for i in 1..<next {
      assert(mycompare(UniqueSplitters[i-1], UniqueSplitters[i], comparator) < 0);
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

  var logSplitters: int;
  var myNumBuckets: int; // number of buckets if no equality buckets
  var equalBuckets: bool;

  // filled from 1..<myNumBuckets
  var storage: [0..<(1<<logSplitters)] eltType;
  // filled from 0..myNumBuckets-2; myNumBuckets-1 is a duplicate of previous
  var sortedStorage: [0..<(1<<logSplitters)] eltType;

  proc init(type eltType) {
    // default init, creates invalid splitters, but useful for replicating
    this.eltType = eltType;
  }
  // creates space for splitters without creating valid splitters
  // numBuckets should be 2**n for some n
  // creates space for splitters assuming that equality bucket will not be used
  // (if they are, a fewer number of splitters will be needed)
  proc init(type eltType, numBuckets: int) {
    this.eltType = eltType;
    this.logSplitters = log2int(numBuckets);
    this.myNumBuckets = 1 << logSplitters;
    init this; // allocate 'storage' and 'sortedStorage'
    myNumBuckets = 0;
  }

  // Create splitters based on some precomputed, already sorted splitters
  // useSplitters needs to be of size 2**n and the last element will
  // not be used. If 'useEqualBuckets=false', there will be 2**n
  // buckets; otherwise there will be 2**(n+1)-1 buckets.
  // Assumes that UseSplitters starts at 0 and is not strided.
  proc init(in UseSplitters: [], useEqualBuckets: bool) {
    assert(UseSplitters.size >= 2);
    this.eltType = UseSplitters.eltType;
    this.logSplitters = log2int(UseSplitters.size);
    this.myNumBuckets = 1 << logSplitters;
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

    if EXTRA_CHECKS then assert(this.numBuckets <= max(2,requestedNumBuckets));
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

    if EXTRA_CHECKS then assert(this.numBuckets <= max(2,requestedNumBuckets));
  }

  /*
  proc init=(const ref rhs: splitters) {
    writeln("in splitters init=");
    this.eltType = rhs.eltType;
    this.logSplitters = rhs.logSplitters;
    this.myNumBuckets = rhs.myNumBuckets;
    this.equalBuckets = rhs.equalBuckets;
    this.storage = rhs.storage;
    this.sortedStorage = rhs.sortedStorage;
  }
  operator =(ref lhs: splitters, const ref rhs: splitters) {
    writeln("in splitters =");
    lhs.logSplitters = rhs.logSplitters;
    lhs.myNumBuckets = rhs.myNumBuckets;
    lhs.equalBuckets = rhs.equalBuckets;
    lhs.storage = rhs.storage;
    lhs.sortedStorage = rhs.sortedStorage;
  }*/

  proc serialize(writer, ref serializer) throws {
    writer.write("splitters(");
    writer.write("\n logSplitters=", logSplitters);
    writer.write("\n myNumBuckets=", myNumBuckets);
    writer.write("\n equalBuckets=", equalBuckets);
    writer.write("\n storage.size=", storage.size);
    writer.write("\n storage=");
    for i in 0..<myNumBuckets {
      writer.writeln(storage[i]);
    }
    writer.write("\n sortedStorage=");
    for i in 0..<myNumBuckets {
      writer.writeln(sortedStorage[i]);
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
  // logSplitters does not account for equalBuckets.
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
    for lg in 0..<logSplitters {
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
    const paramLogBuckets = logSplitters;
    const paramNumBuckets = 1 << (paramLogBuckets + paramEqualBuckets:int);
    var b:c_array(int, CLASSIFY_UNROLL_FACTOR);
    var elts:c_array(Input.eltType, CLASSIFY_UNROLL_FACTOR);

    var cur = start_n;
    // Run the main (unrolled) loop
    while cur <= end_n-(CLASSIFY_UNROLL_FACTOR-1) {
      for /*param*/ i in 0..CLASSIFY_UNROLL_FACTOR-1 {
        b[i] = 1;
        elts[i] = Input[cur+i];
      }
      for /*param*/ lg in 0..paramLogBuckets-1 {
        for /*param*/ i in 0..CLASSIFY_UNROLL_FACTOR-1 {
          b[i] = 2*b[i] +
                 (mycompare(splitter(b[i]), elts[i],comparator)<0):int;
        }
      }
      if paramEqualBuckets {
        for /*param*/ i in 0..CLASSIFY_UNROLL_FACTOR-1 {
          b[i] = 2*b[i] +
                 (mycompare(sortedSplitter(b[i] - paramNumBuckets/2),
                            elts[i],
                            comparator)==0):int;
        }
      }
      for /*param*/ i in 0..CLASSIFY_UNROLL_FACTOR-1 {
        yield (elts[i], b[i]-paramNumBuckets);
      }
      cur += CLASSIFY_UNROLL_FACTOR;
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

proc isSampleSplitters(type splitType) param {
  return isSubtype(splitType, splitters);
}

record radixSplitters : writeSerializable {
  var radixBits: int; // how many bits to sort at once
  var startbit: int;  // start bit position
  var endbit: int;    // when startbit==endbit, everything compares equal

  proc init() {
    // default init, creates invalid splitters, but useful for replicating
  }
  proc init(type eltType, numBuckets: int) {
    radixBits = log2int(numBuckets);
    startbit = 0;
    endbit = max(int);
  }
  // creates a valid radixSplitter
  proc init(radixBits: int, startbit: int, endbit: int) {
    this.radixBits = radixBits;
    this.startbit = startbit;
    this.endbit = endbit;
  }

  proc serialize(writer, ref serializer) throws {
    writer.write("radixSplitters(");
    writer.write("\n radixBits=", radixBits);
    writer.write("\n startbit=", startbit);
    writer.write("\n endbit=", endbit);
    writer.write(")\n");
  }

  proc numBuckets {
    return (1 << radixBits) + 2; // +2 for end-before and end-after bins
  }

  proc bucketHasEqualityBound(bucketIdx: int) {
    return startbit >= endbit - radixBits;
  }

  inline proc bucketForRecord(a, comparator) {
    return myGetBin(a, comparator, startbit, radixBits);
  }

  // yields (value, bucket index) for start_n..end_n
  // gets the elements by calling Input[i] to get element i
  // Input does not have to be an array, but it should have an eltType.
  iter classify(Input, start_n, end_n, comparator) {
    var cur = start_n;
    while cur <= end_n-(CLASSIFY_UNROLL_FACTOR-1) {
      for /*param*/ j in 0..CLASSIFY_UNROLL_FACTOR-1 {
        const elt = Input[cur+j];
        yield (elt, bucketForRecord(elt, comparator));
      }
      cur += CLASSIFY_UNROLL_FACTOR;
    }
    while cur <= end_n {
      const elt = Input[cur];
      yield (elt, bucketForRecord(elt, comparator));
      cur += 1;
    }
  }
} // end record radixSplitters

class PartitionPerTaskState {
  type eltType;

  var numBuckets: int;
  // make sure there is room for equality buckets
  var localCounts: [0..<numBuckets] int;

  // for aggregating the count and element writes
  var countAggregator: DstAggregator(int);
  var eltAggregator: DstAggregator(eltType);

  proc init(type eltType, numBuckets: int) {
    this.eltType = eltType;
    this.numBuckets = numBuckets;
    init this;
  }
}

/*
   Stores the global state needed by a partition operation
   so that it can be reused for many partition operations
   without creating additional per-locale work.

   This technique is an optimization to avoid 'on' statements
   across all locales while inside parallel regions.
 */
record partitioner {
  type eltType;
  type splitterType;
  const numBuckets: int;
  const nTasksPerLocale: int;
  const globalCountsPerBucket: int;
  const globalCountsSize: int;

  // ### splitters storage
  var splitters: splitterType;

  // this uses the full Locales but not all of them are necessarily used.
  var ReplicatedSplitters:
        [blockDist.createDomain(0..<numLocales)]
        owned ReplicatedWrapper(splitterType)?;

  // this tracks which locales are active
  //  * ReplicatedSplitters[i] has a non-nil value
  //  * PerTaskState[i] can have non-zero counts
  var LocaleIsActive:[0..<numLocales] bool;

  // ### per-task state
  //   state for locale 0 tasks 0..<nTasksPerLocale
  //   state for locale 1 tasks 0..<nTasksPerLocale
  //   ...
  // i.e. PerTaskState[here.id*nTasksPerLocale + taskIdInLoc]
  var PerTaskState:
        [blockDist.createDomain(0..<numLocales*nTasksPerLocale)]
        owned PartitionPerTaskState(eltType)?;

  // ### counts and ends storage
  // GlobalCounts stores counts like this:
  //   count for bin 0, locale 0, task 0..<nTasksPerLocale
  //   count for bin 0, locale 1, task 0..<nTasksPerLocale
  //   ...
  //   count for bin 0, locale numLocales-1, task 0..<nTasksPerLocale
  //   count for bin 1, locale 0, task 0..<nTasksPerLocale
  //   count for bin 1, locale 1, task 0..<nTasksPerLocale
  //   ...
  //   count for bin 1, locale numLocales-1, task 0..<nTasksPerLocale
  //   ...
  // i.e. GlobalCounts[bucketIdx*numLocales*nTasksPerLocale
  //                   + here.id*nTasksPerLocale
  //                   + taskIdInLoc]
  // note here that the task indices assume all locales are used
  // (so if fewer are used, there can be extra zeros here)
  // in order for there not to be load imbalance with numLocales/2 etc.
  // TODO:
  //   * these could use Block Cyclic so that per-locale information is local;
  //     or, it could use a custom scan implementation and an array-of-arrays
  //   * partition() could avoid working with elements for inactive locales
  const GlobalCountsDom = blockDist.createDomain(0..<globalCountsSize);
  var GlobalCounts: [GlobalCountsDom] int;
  // GlobalEnds has counts stored in a similar manner
  //var GlobalEnds: [GlobalCountsDom] int;

  // ### to help during the scan
  //var PerLocaleCounts: [blockDist.createDomain(0..<numLocales)] int;
  //var PerLocaleCounts: [0..<numLocales] int;
}


proc partitioner.init(type eltType, type splitterType,
                      numBuckets: int, nTasksPerLocale: int) {
  this.eltType = eltType;
  this.splitterType = splitterType;
  this.numBuckets = numBuckets;
  this.nTasksPerLocale = nTasksPerLocale;
  this.globalCountsPerBucket = nTasksPerLocale * numLocales;
  this.globalCountsSize = numBuckets * globalCountsPerBucket;
  this.splitters = new splitterType(numBuckets=numBuckets);
  init this;

  // create the PerTaskState for each task, assuming we use all Locales
  forall (activeLocIdx, taskIdInLoc, _)
  in divideIntoTasks(PerTaskState.domain, PerTaskState.domain.dim(0),
                     nTasksPerLocale, Locales) {
    const stateIdx = here.id*nTasksPerLocale+taskIdInLoc;
    PerTaskState[stateIdx] = new PartitionPerTaskState(eltType=eltType,
                                                       numBuckets=numBuckets);
  }

  if EXTRA_CHECKS {
    forall state in PerTaskState {
      assert(state != nil && state!.locale == here);
    }
  }
}

proc ref partitioner.reset() {
  const nBuckets = numBuckets;
  sync {
    for i in 0..<numLocales {
      if LocaleIsActive[i] {
        begin {
          on Locales[i] {
            // clear any replicated splitters that were allocated
            ReplicatedSplitters[i] = nil;
            // clear any local counts entries
            coforall taskIdInLoc in 0..<nTasksPerLocale {
              var perTask = getPerTaskState(taskIdInLoc);
              ref counts = perTask.localCounts;
              foreach x in counts do x = 0;
            }

            // clear the GlobalCounts entries
            /*coforall taskIdInLoc in 0..<nTasksPerLocale {
              var perTask = getPerTaskState(taskIdInLoc);
              ref countAgg = perTask.countAggregator;
              for bucketIdx in 0..<nBuckets {
                const countIdx = getGlobalCountIdx(bucketIdx, taskIdInLoc);
                countAgg.copy(GlobalCounts[countIdx], 0);
              }
              countAgg.flush();
            }*/
          }
        }
      }
    }
  }
  // set all locales to inactive
  LocaleIsActive = false;
}

proc ref partitioner.reset(split, activeLocales: [] locale) {
  reset(); // clear any replicated splitters from earlier

  if isSampleSplitters(split.type) {
    assert((1<<split.logSplitters) <= this.splitters.storage.size);
  }

  this.splitters = split;

  //writeln("partitioner.reset with ", split);
  //writeln("this.splitters is ", this.splitters);

  // replicate the splitters to the active locales
  reReplicate(this.splitters, ReplicatedSplitters,
              activeLocales=activeLocales);

  // note also which locales are active to help with freeing
  forall loc in activeLocales {
    LocaleIsActive[loc.id] = true;
  }

  if EXTRA_CHECKS {
    coforall loc in activeLocales {
      on loc {
        assert(LocaleIsActive[here.id]);
        assert(ReplicatedSplitters[here.id] != nil);
        assert(ReplicatedSplitters[here.id]!.locale == here);
        assert(getLocalSplitters() == split);

        //writeln("local splitter on ", loc, " is ", getLocalSplitters());
      }
    }
  }
}

inline proc partitioner.getLocalSplitters() const ref {
  return ReplicatedSplitters[here.id]!.x;
}

inline proc partitioner.getPerTaskState(taskIdInLoc: int) : borrowed class {
  const ret = PerTaskState[here.id*nTasksPerLocale + taskIdInLoc]!;
  if EXTRA_CHECKS {
    assert(ret.locale == here);
  }
  return ret;
}

inline proc partitioner.getGlobalCountIdx(bucketIdx: int,
                                          locIdx: int,
                                          nLocales: int,
                                          taskIdInLoc: int,
                                          nTasksPerLocale: int): int {
  return bucketIdx*nLocales*nTasksPerLocale
         + locIdx*nTasksPerLocale
         + taskIdInLoc;
}
/*
proc partitioner.scanToGlobalEnds(const activeLocales:[] locales) {
  if activeLocales.size >= numLocales / 2 {
    // might as well use the default scan implementation
    // since it's OK to do work on each locale
    GlobalEnds = + scan GlobalCounts;
    return;
  }

  // otherwise, scan in a way that focuses on the active locales
  const nActiveLocales = activeLocales.size;

  // ActiveCounts is a local array storing counts only for active locales
  // accessed like this:
  // ActiveCounts[bucketIdx*nActiveLocales*nTasksPerLocale
  //              + activeLocIdx*nTasksPerLocale
  //              + taskIdInLoc;
  var ActiveCounts:[0..<nBuckets*nActiveLocales*nTasksPerLocale] int;

  // copy the portion for each active locale
  forall activeIdx in ActiveCounts.domain
  with (var agg = new SrcAggregator(int)) {
    const nPerBucket = nTasksPerLocale * nActiveLocales;
    const bucketIdx = activeIdx / nPerBucket;
    const activeLocIdx = (activeIdx - bucketIdx*nPerBucket) / nTasksPerLocale;
    const taskIdInLoc = activeIdx - bucketIdx*nPerBucket - activeLocIdx*nTasksPerLocale;
    const globalIdx = bucketIdx*numLocales*nTasksPerLocale
                      + activeLocales[activeLocIdx].id*nTasksPerLocale
                      + taskIdInLoc;
    agg.copy(ActiveCounts[activeIdx], GlobalCounts[globalIdx]);
  }

  // scan
  const ActiveEnds = + scan ActiveCounts;

  // copy the portion for each active local back to GlobalEnds
  coforall taskIdInLoc in 0..<nTasksPerLocale {
    ref perTask = getPerTaskState(0);
    ref countAgg = perTask.countAggregator;
    for (loc,activeLocIdx) in zip(activeLocales, activeLocales.domain) {
      for bucketIdx in 0..<nBuckets {
        const activeIdx = bucketIdx*nActiveLocales*nTasksPerLocale
                          + activeLocIdx*nTasksPerLocale
                          + taskIdInLoc;
        const globalIdx = bucketIdx*numLocales*nTasksPerLocale
                          + loc.id*nTasksPerLocale
                          + taskIdInLoc;
        countAgg.copy(GlobalEnds[globalIdx], ActiveCounts[activeIdx]);
      }
    }
    countAgg.flush();
  }
}
*/

/*
   Stores the elements Input[InputDomain] in a partitioned manner
   into Output[OutputDomain].

   InputDomain must not be strided. It must be local rectangular domains or
   Block distributed domains.

   Input can be an array over InputDomain or something that simulates
   an array with a 'proc this' and an 'eltType' to generate element i.

   'inputRegion' is the region within InputDomain to consider.

   Output is expected to be an array or something that functions as an array.
   If Output is 'none', this function will only count, and skip the partition
   step.

   OutputStart indicates the start of each bucket. It can be
     * 'none' to do nothing special
     * an integer index to add to all output positions
     * an array of size nBuckets to add bucket start positions

   'filterBucket' provides a mechanism to only process certain buckets.
   If 'filterBucket' is provided and not 'none', it will be called as
   'filterBucket(bucketForRecord(Input[i]))' to check if that bucket should
   be processed. Only elements where it returns 'true' will be processed.

   Return an array of counts to indicate how many elements
   ended up in each bucket. The counts array is never distributed.

   This is done in parallel & distributed (if InputDom is distributed).

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
proc ref partitioner.partition(const InputDomain: domain(?),
                               const inputRegion: range,
                               const Input,
                               const OutputStart,
                               ref Output,
                               comparator,
                               filterBucket: ?t = none) {
  const activeLocs = computeActiveLocales(InputDomain, inputRegion);

  if EXTRA_CHECKS {
    // 'here' should be one of the active locales
    var found = false;
    for loc in activeLocs {
      if loc == here then found = true;
    }
    assert(found);
    // splitters should already exist for the active locales
    coforall loc in activeLocs {
      on loc {
        getLocalSplitters();
      }
    }
  }

  if activeLocs.size <= 2 {
    // allocate local counts as a local array which should go OK
    // when working with 1 or 2 locales and avoid distributed array creation
    // overheads.
    const nBuckets = this.getLocalSplitters().numBuckets;
    //writeln("nBuckets ", nBuckets);
    const nActiveLocales = activeLocs.size;
    //writeln("nActiveLocales ", nActiveLocales);
    const countsPerBucket = nActiveLocales*nTasksPerLocale;
    //writeln("countsPerBucket ", countsPerBucket);
    const countsSize = nBuckets*countsPerBucket;
    const CountsDom = {0..<countsSize};
    //writeln("allocating counts ", CountsDom);
    var Counts: [CountsDom] int;
    return this.doPartition(InputDomain, inputRegion, Input,
                            OutputStart, Output, comparator, filterBucket,
                            activeLocs, Counts, activeLocsOnly=true);
  } else {
    // work with distributed counts, expect to use all locales
    // start by zeroing out GlobalCounts since reusing it
    GlobalCounts = 0;
    return this.doPartition(InputDomain, inputRegion, Input,
                            OutputStart, Output, comparator, filterBucket,
                            activeLocs, GlobalCounts,
                            activeLocsOnly=false);
  }
}

proc partitioner.doPartition(const InputDomain: domain(?),
                             const inputRegion: range,
                             const Input,
                             const OutputStart,
                             ref Output,
                             comparator,
                             filterBucket,
                             const activeLocs: [] locale,
                             ref GlobCounts: [] int,
                             param activeLocsOnly: bool) {
  const ref outersplit = this.getLocalSplitters();
  const nBuckets = outersplit.numBuckets;
  const nActiveLocales = activeLocs.size;
  const nTasksPerLocale = this.nTasksPerLocale;

  //writeln("doPartition with splitters ", outersplit, " active locales ",
  //    activeLocs, " nBuckets ", nBuckets, " nActiveLocales ", nActiveLocales,
  //    " nTasksPerLocale ", nTasksPerLocale);

  {
    // do some checking / input validation
    if EXTRA_CHECKS {
      // check that the splitters are sorted according to comparator
      if isSampleSplitters(outersplit.type) {
        assert(isSorted(outersplit.sortedStorage[0..<outersplit.myNumBuckets-1],
                        comparator));
      }

      /*for loc in activeLocs {
        for bucketIdx in 0..<nBuckets {
          for taskIdInLoc in 0..<nTasksPerLocale {
            assert(GlobCounts[bucketIdx*numLocales*nTasksPerLocale+
                                loc.id*nTasksPerLocale+
                                taskIdInLoc] == 0);
            assert(PerTaskState[loc.id*nTasksPerLocale+itaskIdInLoc]!=nil);a
          }
        }
        assert(ReplicatedSplitters[loc.id]!=nil);
        assert(ReplicatedSplitters[loc.id].x==this.splitters);
      }*/
    }
  }

  // Step 1: Count
  forall (activeLocIdx, taskIdInLoc, chunk)
  in divideIntoTasks(InputDomain, inputRegion, nTasksPerLocale, activeLocs) {
    var perTask = getPerTaskState(taskIdInLoc);
    ref counts = perTask.localCounts;
    const ref mysplit = getLocalSplitters();
    const taskStart = chunk.first;
    const taskEnd = chunk.last; // inclusive

    if EXTRA_CHECKS {
      // counts should be 0 at this point (cleared in 'reset')
      for x in counts do assert(x==0);
    }

    // this loop must really be serial. it can be run in parallel
    // within the forall because it's updating state local to each task.
    for (_,bin) in mysplit.classify(Input, taskStart, taskEnd, comparator) {
      if filterBucket.type == nothing || filterBucket(bin) {
        //writeln("counts[", bin, "] increment");
        counts[bin] += 1;
      }
    }

    // Now store the counts into the global counts array
    ref countAgg = perTask.countAggregator;
    for bucketIdx in 0..<nBuckets {
      var countIdx: int;
      if activeLocsOnly {
        countIdx = getGlobalCountIdx(bucketIdx, activeLocIdx, nActiveLocales,
                                     taskIdInLoc, nTasksPerLocale);
      } else {
        countIdx = getGlobalCountIdx(bucketIdx, here.id, numLocales,
                                     taskIdInLoc, nTasksPerLocale);
      }
      //writeln("countIdx is ", countIdx,
      //        " from ", bucketIdx, " ", activeLocIdx, " ", taskIdInLoc);
      countAgg.copy(GlobCounts[countIdx], counts[bucketIdx]);
    }
    countAgg.flush();
  }


  if Output.type != nothing {
    // Step 2: Scan

    // TODO: this could be adjusted to use only activeLocales
    // if performance on more than 2 and < numLocales is important
    const GlobEnds = + scan GlobCounts;

    //writeln("GlobCounts ", GlobCounts);
    //writeln("GlobEnds ", GlobEnds);

    // Step 3: Distribute
    forall (activeLocIdx, taskIdInLoc, chunk)
    in divideIntoTasks(InputDomain, inputRegion, nTasksPerLocale, activeLocs)
    with (in OutputStart) {
      var perTask = getPerTaskState(taskIdInLoc);
      ref nextOffsets = perTask.localCounts;
      ref eltAgg = perTask.eltAggregator;
      const ref mysplit = getLocalSplitters();
      const taskStart = chunk.first;
      const taskEnd = chunk.last; // inclusive

      // initialize nextOffsets
      foreach bucketIdx in 0..<nBuckets {
        var startForBucket = 0;
        if isArrayType(OutputStart.type) {
          startForBucket = OutputStart[bucketIdx];
        } else if isIntType(OutputStart.type) {
          startForBucket = OutputStart;
        }

        var countIdx: int;
        if activeLocsOnly {
          countIdx = getGlobalCountIdx(bucketIdx, activeLocIdx, nActiveLocales,
                                       taskIdInLoc, nTasksPerLocale);
        } else {
          countIdx = getGlobalCountIdx(bucketIdx, here.id, numLocales,
                                       taskIdInLoc, nTasksPerLocale);
        }
        // this is doing GETs, generally speaking
        nextOffsets[bucketIdx] = if countIdx > 0
                                 then startForBucket + GlobEnds[countIdx-1]
                                 else startForBucket;
      }

      // as above,
      // this loop must really be serial. it can be run in parallel
      // within the forall because it's updating state local to each task.
      for (elt,bin) in mysplit.classify(Input, taskStart, taskEnd, comparator) {
        if filterBucket.type == nothing || filterBucket(bin) {
          // Store it in the right bin
          ref next = nextOffsets[bin];
          //writeln("Output[", next, "] = ", elt, " bin ", bin);
          eltAgg.copy(Output[next], elt);
          next += 1;
        }
      }
      eltAgg.flush();
    }
  }

  // Compute the total counts to return
  var counts:[0..<nBuckets] int;
  forall (c, bucketIdx) in zip(counts, counts.domain) {
    var total = 0;
    for (activeLoc, activeLocIdx) in zip(activeLocs, activeLocs.domain) {
      for taskIdInLoc in 0..<nTasksPerLocale {
        var countIdx: int;
        if activeLocsOnly {
          countIdx = getGlobalCountIdx(bucketIdx, activeLocIdx, nActiveLocales,
                                       taskIdInLoc, nTasksPerLocale);
        } else {
          countIdx = getGlobalCountIdx(bucketIdx, activeLoc.id, numLocales,
                                       taskIdInLoc, nTasksPerLocale);
        }
        // this is doing GETs, generally speaking
        total += GlobCounts[countIdx];
      }
    }
    c = total;
  }

  return counts;
}

/*
private proc partitioningSortCreateSampleSplitters(ref A: [],
                                                   Dom: domain(?),
                                                   comparator,
                                                   const logBuckets: int,
                                                   const nTasksPerLocale: int,
                                                   const baseCaseLimit: int)
 : splitters(A.eltType) {

  const requestBuckets = 1 << logBuckets;
  const nToSample = (SAMPLE_RATIO*requestBuckets):int;
  var SortSamplesSpace:[0..<nToSample] A.eltType;
  const nTasks = A.targetLocales().size * nTasksPerLocale;
  const perTask = divCeil(SortSamplesSpace.size, nTasks);
  const SortSamplesSpaceDomRange = SortSamplesSpace.domain.dim(0);

  // read some random elements from each locale
  // each should set SortSampleSpace[perTask*taskId..#perTask]
  //forall (taskId, chk) in divideIntoTasks(Dom, nTasksPerLocale) {
  forall (activeLocIdx, taskIdInLoc, chunk)
  in divideIntoTasks(Dom, Dom.dim(0), nTasksPerLocale, activeLocs) {
    const dstFullRange = perTask*taskId..#perTask;
    const dstRange = SortSamplesSpaceDomRange[dstFullRange];
    const dstRangeDom = {dstRange};

    // note: it is intentional that this will give different
    // results with the same seed if the number of tasks
    // or the number of locales differs
    var randNums;
    if SEED == 0 {
      randNums = new Random.randomStream(int);
    } else {
      randNums = new Random.randomStream(int, seed=SEED*taskId);
    }

    const low = chk.low;
    const high = chk.high;
    for (dstIdx, randIdx) in zip(dstRangeDom,
                                 randNums.next(dstRangeDom, low, high)) {
      // store the value at randIdx (which should be local) to dstIdx
      SortSamplesSpace[dstIdx] = A[randIdx];
    }
  }

  // sort them using any kind of sort
  /*writeln("before sorting");
  for i in SortSamplesSpace.domain {
    writeln("SortSamplesSpace[", i, "] = ", SortSamplesSpace[i]);
  }*/


  // TODO: this seems to cause it not to compile
  /*
  if SortSamplesSpace.size <= baseCaseLimit {
    sort(SortSamplesSpace, comparator=comparator);
  } else {
    var Scratch: [SortSamplesSpace.domain] A.eltType;
    var BucketBoundaries: [SortSamplesSpace.domain] uint(8);
    parallelPartitioningSort(SortSamplesSpace, Scratch, BucketBoundaries,
                             0..<nToSample, radixSort=false,
                             comparator, logBuckets, nTasksPerLocale,
                             startbit=0, endbit=max(int));
  }*/
  // TODO: using default sort seems to fail due to out of stack space
  // with all-zeros input.
  sort(SortSamplesSpace, comparator=comparator, 0..<nToSample, stable=true);

  /*
  writeln("after sorting");
  for i in SortSamplesSpace.domain {
    writeln("SortSamplesSpace[", i, "] = ", SortSamplesSpace[i]);
  }*/

  if EXTRA_CHECKS {
    //writeln("sorted samples to ", SortSamplesSpace);
    assert(isSorted(SortSamplesSpace, comparator));
  }

  // now form splitters
  //writeln("forming splitters with requestBuckets ", requestBuckets);
  const split = new splitters(SortSamplesSpace, requestBuckets, comparator,
                              howSorted=sortLevel.fully);

  //writeln("splitters are ", split);

  return split;
}*/

param boundaryTypeUnsorted:uint(8) = 0;
param boundaryTypeOrdered:uint(8) = 1;
param boundaryTypeEqual:uint(8) = 2;

/*
private inline proc cmpToBoundaryType(cmp: int) {
  var order: uint(8);
  if cmp == 0 {
    order = boundaryTypeEqual;
  } else {
    order = boundaryTypeOrdered;
  }
  return order;
}

// sets BucketBoundaries[region.low] to ordered
// and sets the subsequent ones according to comparing
// useful after doing a base case sort on A[region] to set bucket boundaries
private proc setBoundariesComparing(const ref A: [], region, comparator,
                                    ref BucketBoundaries: [] uint(8)) {
  // compare the elements to set the bucket boundaries
  const low = region.low;
  const high = region.high;
  BucketBoundaries[low] = boundaryTypeOrdered;
  forall i in low+1..high {
    var cmp = mycompare(A[i-1], A[i], comparator);
    BucketBoundaries[i] = cmpToBoundaryType(cmp);
  }
}

private proc partitionSortBaseCase(ref A: [], region: range, comparator,
                                   ref BucketBoundaries: [] uint(8)) {
  if region.size == 0 {
    return; // nothing to do
  }

  if region.size == 1 {
    // mark the bucket boundary
    BucketBoundaries[region.low] = boundaryTypeOrdered;
    return;
  }

  if region.size == 2 {
    const i = region.low;
    const j = region.low + 1;
    var cmp = mycompare(A[i], A[j], comparator);
    if cmp > 0 {
      A[i] <=> A[j];
    }
    // if we got here, A[i] must differ from previous
    BucketBoundaries[i] = boundaryTypeOrdered;
    BucketBoundaries[j] = cmpToBoundaryType(cmp);
    return;
  }

  if A.domain.localSubdomain().dim(0).contains(region) {
    // sort it with a base case sort
    // sort them using any kind of sort
    /*if region.size < 20 {
      Sort.InsertionSort.insertionSort(A, comparator, region.low, region.high);
    } else */
      sort(A, comparator, region, stable=true);
    // compare the elements again to set the bucket boundaries
    setBoundariesComparing(A, region, comparator, BucketBoundaries);
  } else {
    // copy it locally and sort it with a base case sort
    var LocA:[region] A.eltType;
    LocA[region] = A[region];
    sort(LocA, comparator, region, stable=true);
    // compare the elements again to set the bucket boundaries
    setBoundariesComparing(LocA, region, comparator, BucketBoundaries);
    // copy the sorted data back
    A[region] = LocA[region];
  }
}

// this function partitions from A to Scratch
// forming the outer buckets. Each outer bucket will be processed
// with processOuterBucket.
proc partitionAndProcessOuterBuckets(const Dom: domain(?),
                                     ref A: [],
                                     ref Scratch: [] A.eltType,
                                     ref BucketBoundaries: [] uint(8),
                                     param radixSort,
                                     comparator,
                                     const logBuckets: int,
                                     const nTasksPerLocale: int,
                                     in startbit: int,
                                     const endbit: int,
                                     const baseCaseLimit: int,
                                     const OuterSplit,
                                     const OuterRSplit) {
  const OuterCounts = partition(Dom, A, Dom, Scratch,
                                OuterSplit, OuterRSplit, comparator,
                                nTasksPerLocale);

  /*for i in Dom {
    writeln("after partition1 Scratch[", i, "] = ", Scratch[i], " BucketBoundaries[", i, "] = ", BucketBoundaries[i]);
  }*/

  const OuterEnds = + scan OuterCounts;

  // when radix sorting, the partitioning we just did sorted by
  // an additional logBuckets bits
  startbit += logBuckets;

  forall (outerRegion, outerIdx, outerTaskId)
  in divideByBuckets(Scratch, Dom, OuterCounts, OuterEnds, nTasksPerLocale)
  with (const ref locOutSp = getLocalReplicand(OuterSplit, OuterRSplit)) {
    processOuterBucket(A, Scratch, BucketBoundaries, radixSort, comparator,
                       logBuckets, nTasksPerLocale,
                       startbit, endbit, baseCaseLimit,
                       outerRegion, outerIdx, outerTaskId, locOutSp);
  }
}

// the partitioning sort will partition from A to Scratch
// and this forms the outer buckets. This is called to process each
// outer bucket. Processing each outer bucket will involve
// bringing the data back from Scratch to A (potentially with
// another partitioning step).
proc processOuterBucket(ref A: [],
                        ref Scratch: [] A.eltType,
                        ref BucketBoundaries: [] uint(8),
                        param radixSort,
                        comparator,
                        const logBuckets: int,
                        const nTasksPerLocale: int,
                        const startbit: int,
                        const endbit: int,
                        const baseCaseLimit: int,

                        outerRegion:range,
                        outerIdx:int,
                        outerTaskId:int,
                        const ref outerSplit) {
  // for each bucket, partition from Scratch back into A
  // and mark bucket boundaries indicating what is sorted
  if outerRegion.size == 0 {
    // nothing to do
  } else if outerRegion.size == 1 {
    A[outerRegion.low] = Scratch[outerRegion.low];
    BucketBoundaries[outerRegion.low] = boundaryTypeOrdered;

  } else if outerSplit.bucketHasEqualityBound(outerIdx) {
    A[outerRegion] = Scratch[outerRegion];
    const low = outerRegion.low;
    const high = outerRegion.high;
    BucketBoundaries[low] = boundaryTypeOrdered;
    BucketBoundaries[low+1..high] = boundaryTypeEqual;

  } else if outerRegion.size <= baseCaseLimit {
    // copy it from Scratch back into A
    A[outerRegion] = Scratch[outerRegion];
    // sort it and mark BucketBoundaries
    partitionSortBaseCase(A, outerRegion, comparator, BucketBoundaries);

  } else {
    // do a partition step from Scratch back into A
    // and then process the resulting buckets with processInnerBucket
    // to mark BucketBoundaries
    if Scratch.domain.localSubdomain().dim(0).contains(outerRegion) {
      // do it locally
      const Dom = {outerRegion};
      if !radixSort {
        const InnerSplit =
          partitioningSortCreateSampleSplitters(A, Dom, comparator,
                                                logBuckets, nTasksPerLocale,
                                                baseCaseLimit);
        partitionAndProcessInnerBuckets(Dom, A, Scratch, BucketBoundaries,
                                        radixSort, comparator, logBuckets,
                                        nTasksPerLocale, startbit, endbit,
                                        baseCaseLimit, InnerSplit, none);
      } else {
        const InnerSplit =
          new radixSplitters(radixBits=logBuckets,
                             startbit=startbit, endbit=endbit);
        partitionAndProcessInnerBuckets(Dom, A, Scratch, BucketBoundaries,
                                        radixSort, comparator, logBuckets,
                                        nTasksPerLocale, startbit, endbit,
                                        baseCaseLimit, InnerSplit, none);
      }
    } else {
      // do it distributed
      const Dom = A.domain[outerRegion];
      if !radixSort {
        const InnerSplit =
          partitioningSortCreateSampleSplitters(A, Dom, comparator,
                                                logBuckets, nTasksPerLocale,
                                                baseCaseLimit);
        const InnerRSplit = replicate(InnerSplit, Dom.targetLocales());
        partitionAndProcessInnerBuckets(Dom, A, Scratch, BucketBoundaries,
                                        radixSort, comparator, logBuckets,
                                        nTasksPerLocale, startbit, endbit,
                                        baseCaseLimit, InnerSplit, InnerRSplit);
      } else {
        const InnerSplit =
          new radixSplitters(radixBits=logBuckets,
                             startbit=startbit, endbit=endbit);
        const InnerRSplit = replicate(InnerSplit, Dom.targetLocales());
        partitionAndProcessInnerBuckets(Dom, A, Scratch, BucketBoundaries,
                                        radixSort, comparator, logBuckets,
                                        nTasksPerLocale, startbit, endbit,
                                        baseCaseLimit, InnerSplit, InnerRSplit);
      }
    }
  }
}

// this function partitions from Scratch to A
// forming the inner buckets. Each inner bucket will be
// processed with processInnerBucket.
proc partitionAndProcessInnerBuckets(const Dom: domain(?),
                                     ref A: [],
                                     ref Scratch: [] A.eltType,
                                     ref BucketBoundaries: [] uint(8),
                                     param radixSort,
                                     comparator,
                                     const logBuckets: int,
                                     const nTasksPerLocale: int,
                                     const startbit: int,
                                     const endbit: int,
                                     const baseCaseLimit: int,
                                     const InnerSplit,
                                     const InnerRSplit) {
  const InnerCounts = partition(Dom, Scratch, Dom, A,
                                InnerSplit, InnerRSplit, comparator,
                                nTasksPerLocale);

  /*for i in Dom {
    writeln("after partition2 A[", i, "] = ", A[i], " BucketBoundaries[", i, "] = ", BucketBoundaries[i]);
  }*/

  const InnerEnds = + scan InnerCounts;
  forall (innerRegion, innerBktIdx, innerTask)
  in divideByBuckets(A, Dom, InnerCounts, InnerEnds, nTasksPerLocale)
  with (const ref locInSplit = getLocalReplicand(InnerSplit, InnerRSplit))
  {
    processInnerBucket(A, BucketBoundaries, comparator, baseCaseLimit,
                       innerRegion, innerBktIdx, innerTask, locInSplit);
  }

  /* for i in Dom {
    writeln("after processInnerBuckets A[", i, "] = ", A[i], " BucketBoundaries[", i, "] = ", BucketBoundaries[i]);
  }*/
}

// this processes an inner bucket
// it is primarily concerned with setting BucketBoundaries
proc processInnerBucket(ref A: [],
                        ref BucketBoundaries: [] uint(8),
                        comparator,
                        const baseCaseLimit: int,

                        innerRegion:range,
                        innerBktIdx:int,
                        innerTask:int,
                        const ref innerSplit) {
  //writeln("processInnerBucket ", innerRegion);

  if innerRegion.size == 0 {
    // nothing to do
  } else if innerRegion.size == 1 {
    BucketBoundaries[innerRegion.low] = boundaryTypeOrdered;
    //writeln("processInnerBucket 1 set BucketBoundaries[", innerRegion.low, "] = ", BucketBoundaries[innerRegion.low]);

  } else if innerSplit.bucketHasEqualityBound(innerBktIdx) {
    const low = innerRegion.low;
    const high = innerRegion.high;
    BucketBoundaries[low] = boundaryTypeOrdered;
    BucketBoundaries[low+1..high] = boundaryTypeEqual;

  } else if innerRegion.size <= baseCaseLimit {
    // sort it and mark BucketBoundaries
    partitionSortBaseCase(A, innerRegion, comparator, BucketBoundaries);

  } else {
    // it won't be fully sorted, but we have established (by partitioning)
    // that the element at innerRegion.low differs from the previous
    BucketBoundaries[innerRegion.low] = boundaryTypeOrdered;
  }
}

/* A parallel partitioning sort step.

   When this returns, A will be more sorted, and BucketBoundaries
   will be updated to indicate how A is more sorted.

   Each call to partitioningSortStep will write to 'split' and 'rsplit',
   so make sure each gets its own if running in a parallel context.

   Scratch is temporary space of similar size to the sorted region.

   BucketBoundaries[i] indicates the relationship between A[i] and A[i-1]:
     * unsorted: ordering of A[i] and A[i-1] is not known
     * ordered: A[i] > A[i-1] (i.e. they are in sorted order)
     * equal: A[i] == A[i-1] (i.e. they are in sorted order)

   split is space for some splitters
   rsplit is space for those splitters replicated

   The output will be stored in A.

   A and Scratch can be distributed.
   The others should be local.
 */
proc partitioningSortStep(ref A: [],
                          ref Scratch: [] A.eltType,
                          ref BucketBoundaries: [] uint(8),
                          region: range,
                          param radixSort: bool,
                          comparator,
                          const logBuckets: int,
                          const nTasksPerLocale: int,
                          const startbit: int,
                          const endbit: int,
                          // for testing
                          const noBaseCase: bool) : void {
  if EXTRA_CHECKS {
    assert(A.domain.dim(0).contains(region));
    assert(Scratch.domain.dim(0).contains(region));
    assert(BucketBoundaries.domain.dim(0).contains(region));
  }


  //writeln("partitioningSortStep ", region);

  /*for i in region {
    writeln("starting partitioningSortStep A[", i, "] = ", A[i], " BucketBoundaries[", i, "] = ", BucketBoundaries[i]);
  }*/

  const regularBaseCaseLimit =
    (PARTITION_SORT_BASE_CASE_MULTIPLIER * (1 << logBuckets)):int;
  const baseCaseLimit = if noBaseCase then 1 else regularBaseCaseLimit;

  if region.size <= baseCaseLimit {
    // sort it and mark BucketBoundaries
    partitionSortBaseCase(A, region, comparator, BucketBoundaries);
    return;
  }


  // Partition from A to Scratch, to form outer buckets.
  // Process each outer bucket, which will in
  // turn lead to moving the data back to A
  // (possibly by partitioning again and forming inner buckets).
  if A.domain.localSubdomain().dim(0).contains(region) {
    // process it locally
    const Dom = {region};
    if !radixSort {
      const OuterSplit =
        partitioningSortCreateSampleSplitters(A, Dom, comparator,
                                              logBuckets, nTasksPerLocale,
                                              baseCaseLimit);
      partitionAndProcessOuterBuckets(Dom, A, Scratch, BucketBoundaries,
                                      radixSort, comparator, logBuckets,
                                      nTasksPerLocale, startbit, endbit,
                                      baseCaseLimit, OuterSplit, none);
    } else {
      const OuterSplit = new radixSplitters(radixBits=logBuckets,
                                            startbit=startbit, endbit=endbit);
      partitionAndProcessOuterBuckets(Dom, A, Scratch, BucketBoundaries,
                                      radixSort, comparator, logBuckets,
                                      nTasksPerLocale, startbit, endbit,
                                      baseCaseLimit, OuterSplit, none);
    }
  } else {
    // process it distributed
    const Dom = A.domain[region];
    if !radixSort {
      const OuterSplit =
        partitioningSortCreateSampleSplitters(A, Dom, comparator,
                                              logBuckets, nTasksPerLocale,
                                              baseCaseLimit);
      const OuterRSplit = replicate(OuterSplit, Dom.targetLocales());
      partitionAndProcessOuterBuckets(Dom, A, Scratch, BucketBoundaries,
                                      radixSort, comparator, logBuckets,
                                      nTasksPerLocale, startbit, endbit,
                                      baseCaseLimit, OuterSplit, OuterRSplit);
    } else {
      const OuterSplit = new radixSplitters(radixBits=logBuckets,
                                            startbit=startbit, endbit=endbit);
      const OuterRSplit = replicate(OuterSplit, Dom.targetLocales());
      partitionAndProcessOuterBuckets(Dom, A, Scratch, BucketBoundaries,
                                      radixSort, comparator, logBuckets,
                                      nTasksPerLocale, startbit, endbit,
                                      baseCaseLimit, OuterSplit, OuterRSplit);
    }
  }

  /* writeln("after partitioningSortStep ", region, " startbit=", startbit);
  for i in region {
    writeln("after partitioningSortStep A[", i, "] = ", A[i], " BucketBoundaries[", i, "] = ", BucketBoundaries[i]);
  }*/
}

/* A parallel partitioning sort.

   When this returns, A will be sorted, and BucketBoundaries
   will be updated to indicate how A is more sorted.

   Each call to parallelPartitioningSort will write to 'split' and 'rsplit',
   so make sure each gets its own if running in a parallel context.

   Uses temporary space of similar size
   to the sorted region, as well as BucketBoundaries.

   BucketBoundaries[i] indicates the relationship between A[i] and A[i-1]:
     * unsorted: ordering of A[i] and A[i-1] is not known
     * ordered: A[i] > A[i-1] (i.e. they are in sorted order)
     * equal: A[i] == A[i-1] (i.e. they are in sorted order)

   split is space for some splitters
   rsplit is space for those splitters replicated

   The output will be stored in A.

   A and Scratch can be distributed.
   The others should be local.
 */
proc parallelPartitioningSort(ref A: [],
                              ref Scratch: [] A.eltType,
                              ref BucketBoundaries: [] uint(8),
                              region: range,
                              param radixSort: bool,
                              comparator,
                              const logBuckets: int,
                              const nTasksPerLocale: int,
                              const startbit: int,
                              const endbit: int,
                              // for testing
                              const noBaseCase = false) : void {
  if EXTRA_CHECKS {
    assert(A.domain.dim(0).contains(region));
    assert(Scratch.domain.dim(0).contains(region));
    assert(BucketBoundaries.domain.dim(0).contains(region));
  }

  const regularBaseCaseLimit =
    PARTITION_SORT_BASE_CASE_MULTIPLIER * (1 << logBuckets);
  const baseCaseLimit = if noBaseCase then 1 else regularBaseCaseLimit;

  if region.size <= baseCaseLimit {
    // sort it and mark BucketBoundaries
    partitionSortBaseCase(A, region, comparator, BucketBoundaries);
    return;
  }

  const Dom = A.domain[region];

  var curbit = startbit;

  /* for i in region {
    writeln("starting parallelPartitioningSort A[", i, "] = ", A[i], " BucketBoundaries[", i, "] = ", BucketBoundaries[i]);
  }*/

  // do a partitioning sort step
  partitioningSortStep(A, Scratch, BucketBoundaries, region,
                       radixSort, comparator, logBuckets,
                       nTasksPerLocale,
                       startbit=curbit, endbit=endbit, noBaseCase=noBaseCase);
  if radixSort {
    // when radix sorting, each sortStep sorts by the next 2*logBuckets bits.
    curbit += 2*logBuckets;
  }

  while true {
    /*for i in region {
      writeln("in loop parallelPartitioningSort A[", i, "] = ", A[i], " BucketBoundaries[", i, "] = ", BucketBoundaries[i]);
    }*/

    // scan the BucketBoundaries to determine if A is fully sorted.
    // if it is not, sort within each region updating BucketBoundaries
    // Inner sorts and updates to BucketBoundaries do not race because
    // they update different regions of these arrays.
    var nNotSorted = 0;
    forall (taskId, chunk) in divideIntoTasks(Dom, nTasksPerLocale)
    with (+ reduce nNotSorted) {
      //writeln("task ", taskId, " working on ", chunk);
      // consider buckets that start within chunk
      var cur = chunk.low;
      const end = chunk.high+1;
      const endAll = region.high+1;
      // move 'cur' forward until we find the start of a bucket boundary
      // (such elements would be handled in a previous chunk)
      while cur < end && BucketBoundaries[cur] != boundaryTypeOrdered {
        cur += 1;
      }
      while cur < end {
        if EXTRA_CHECKS {
          /*if BucketBoundaries[cur] != boundaryTypeOrdered {
            writeln("task ", taskId, " error with cur ", cur);
          }*/
          assert(BucketBoundaries[cur] == boundaryTypeOrdered);
        }
        //writeln("task ", taskId, " cur is ", cur);
        // find the start of an unsorted area
        // where the initial bucket boundary is in this task's region
        while cur+1 < endAll && cur < end &&
              BucketBoundaries[cur+1] != boundaryTypeUnsorted {
          cur += 1;
        }
        if cur >= end {
          break; // it's in a different task's region
        }
        var nextOrdered = cur+2; // cur+1 is unordered, so start at cur+2
        if nextOrdered > endAll {
          nextOrdered = endAll;
        }
        // find the end of the unsorted area (perhaps in another task's area)
        while nextOrdered < endAll &&
              BucketBoundaries[nextOrdered] == boundaryTypeUnsorted {
          nextOrdered += 1;
        }
        // now the region of interest is
        const r = cur..<nextOrdered;
        if r.size > 1 {
          /*writeln("task ", taskId, " sorting ", r);
          for i in r {
            writeln("a A[", i, "] = ", A[i], " BucketBoundaries[", i, "] = ", BucketBoundaries[i]);
          }*/

          /*writeln("considering region ", r,
                  " cur=", cur,
                  " nextOrdered=", nextOrdered);*/
          // some elements need to be sorted, so make progress on sorting them
          partitioningSortStep(A, Scratch, BucketBoundaries, r,
                               radixSort, comparator, logBuckets,
                               nTasksPerLocale,
                               startbit=curbit, endbit=endbit,
                               noBaseCase=noBaseCase);

          /*for i in r {
            writeln("b A[", i, "] = ", A[i], " BucketBoundaries[", i, "] = ", BucketBoundaries[i]);
          }*/

          var rIsSorted = true;
          for i in region {
            if BucketBoundaries[i] == boundaryTypeUnsorted {
              rIsSorted = false;
            }
          }

          if !rIsSorted {
            nNotSorted += 1;
          }
        }
        // proceed with searching, starting from 'nextOrdered'
        cur = nextOrdered;
      }
    }

    if radixSort {
      // when radix sorting, the above sorted by the next 2*logBuckets bits
      curbit += 2*logBuckets;
    }

    if nNotSorted == 0 || curbit == endbit {
      //writeln("exiting nNotSorted=", nNotSorted, " curbit=", curbit);
      break;
    }
  }

  /*for i in region {
    writeln("done parallelPartitioningSort A[", i, "] = ", A[i], " BucketBoundaries[", i, "] = ", BucketBoundaries[i]);
  }*/
}*/

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
