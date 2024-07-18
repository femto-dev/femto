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

import Reflection.canResolveMethod;
import Sort.{sort, defaultComparator};
import Math.{log2, divCeil};
import CTypes.c_array;

// These settings control the sample sort and classification process
param classifyUnrollFactor = 7;
const equalBucketThreshold = 5;

// helper function to compute the number of tasks available
proc computeNumTasks(ignoreRunning: bool = dataParIgnoreRunningTasks) {
  if __primitive("task_get_serial") {
    return 1;
  }

  const tasksPerLocale = dataParTasksPerLocale;
  const ignoreRunning = dataParIgnoreRunningTasks;
  var nTasks = if tasksPerLocale > 0 then tasksPerLocale else here.maxTaskPar;
  if !ignoreRunning {
    const otherTasks = here.runningTasks() - 1; // don't include self
    nTasks = if otherTasks < nTasks then (nTasks-otherTasks):int else 1;
  }

  return nTasks;
}

// compute logarithm base 2 rounded down
proc log2int(n: int) {
  if n <= 0 then
    return 0;
  return log2(n);
}

// compare two records according to a comparator, but allow them
// to be different types.
private inline proc mycompare(a, b, comparator) {
  if canResolveMethod(comparator, "key", a) &&
     canResolveMethod(comparator, "key", b) {
    // Use the default comparator to compare the integer keys
    return defaultComparator.compare(comparator.key(a), comparator.key(b));
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
    if sampleIdx >= SortedSample.size then sampleIdx = SortedSample.size-1;
    SortedSplitters[i] = SortedSample[sampleIdx];
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
    if reSort {
      sort(SortedSplitters[0..<numSplitters], comparator);
    }
    if EXTRA_CHECKS {
      assert(isSorted(SortedSplitters[0..<numSplitters], comparator));
    }

    useEqualBuckets = false;
    return SortedSplitters;
  }

  // if there were duplicates, reduce the number of splitters accordingly,
  // activate equality buckets, and return a de-duplicated array.
  const nUnique = numSplitters - nDuplicates;
  myNumBuckets = max(2, 1 << log2int(nUnique));
  numSplitters = myNumBuckets-1;
  var UniqueSplitters:[0..<myNumBuckets] SortedSample.eltType;
  UniqueSplitters[0] = SortedSplitters[0];
  var next = 1;
  for i in 1..<SortedSplitters.size {
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

  if reSort {
    sort(SortedSplitters[0..<numSplitters], comparator);
  }
  if EXTRA_CHECKS {
    assert(isSorted(SortedSplitters[0..<numSplitters], comparator));
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
      bk = 2*bk + (mycompare(a, sortedSplitter(bk-myNumBuckets), comparator) == 0):int;
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

/* Given a way to produce Input
   (which can be an array or something that can generate input element i),

   store the Input elements in a partitioned manner into Output.
   It is assumed that indices start..end (inclusive) exist
   within Input and Output.

   Return an array of counts to indicate how many elements
   ended up in each bucket.

   This is done in parallel.

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
proc partition(const Input, ref Output, split:splitters(?), comparator,
               start: int, end: int,
               nTasks: int = computeNumTasks()) {

  // check that the splitters are sorted according to comparator
  if EXTRA_CHECKS {
    assert(isSorted(split.sortedStorage[0..<split.myNumBuckets-1], comparator));
  }

  const nBuckets = split.numBuckets;
  const n = end - start + 1;

  // Divide the input into nTasks chunks.
  const countsSize = nTasks * nBuckets;
  const blockSize = divCeil(n, nTasks);
  const nBlocks = divCeil(n, blockSize);

  // create the arrays that drive the counting and distributing process
  var localState:[0..<nTasks] owned PerTaskState?;
  coforall i in 0..<nTasks {
    localState[i] = new PerTaskState(nBuckets);
  }

  // globalCounts stores counts like this:
  //   count for bin 0, task 0
  //   count for bin 0, task 1
  //   ...
  //   count for bin 1, task 0
  //   count for bin 1, task 1
  // i.e. bin*nTasks + taskId
  var globalCounts:[0..<countsSize] int;

  // Step 1: Count
  coforall tid in 0..<nTasks {
    var taskStart = start + tid * blockSize;
    var taskEnd = min(taskStart + blockSize - 1, end); // an inclusive bound

    ref counts = localState[tid]!.localCounts;
    for bin in 0..<nBuckets {
      counts[bin] = 0;
    }

    for (_,bin) in split.classify(Input, taskStart, taskEnd, comparator) {
      counts[bin] += 1;
    }
    // Now store the counts into the global counts array
    foreach bin in 0..<nBuckets {
      globalCounts[bin*nTasks + tid] = counts[bin];
    }
  }

  // Step 2: Scan
  const globalEnds = + scan globalCounts;

  // Step 3: Distribute
  coforall tid in 0..<nTasks {
    var taskStart = start + tid * blockSize;
    var taskEnd = min(taskStart + blockSize - 1, end); // an inclusive bound

    ref nextOffsets = localState[tid]!.localCounts;
    // initialize nextOffsets
    for bin in 0..<nBuckets {
      var globalBin = bin*nTasks+tid;
      nextOffsets[bin] = if globalBin > 0
                         then start+globalEnds[globalBin-1]
                         else start;
    }

    for (elt,bin) in split.classify(Input, taskStart, taskEnd, comparator) {
      // Store it in the right bin
      ref next = nextOffsets[bin];
      Output[next] = elt;
      next += 1;
    }
  }

  // Compute the total counts to return them
  var counts:[0..<nBuckets] int;
  forall bin in 0..<nBuckets {
    var total = 0;
    for tid in 0..<nTasks {
      total += globalCounts[bin*nTasks + tid];
    }
    counts[bin] = total;
  }

  return counts;
}


} // end module Partitioning
