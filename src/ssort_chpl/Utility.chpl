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

  femto/src/ssort_chpl/Utility.chpl
*/

module Utility {


import CTypes.{c_int};
import FileSystem.{isFile, isDir, findFiles, getFileSize};
import FileSystem;
import IO;
import List.list;
import OS.EofError;
import Path;
import BitOps;
import Sort.{sort,isSorted};
import Math.divCeil;
import BlockDist.blockDist;
import ChplConfig.CHPL_COMM;
import RangeChunk;
import Version;

import SuffixSort.{EXTRA_CHECKS, INPUT_PADDING, DISTRIBUTE_EVEN_WITH_COMM_NONE};

/* For FASTA files, when reading them, also read in the reverse complement */
config param INCLUDE_REVERSE_COMPLEMENT=true;

/* Compute the number of tasks to be used for a data parallel operation */
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

/* check to see if a domain is of a type that can be distributed */
proc isDistributedDomain(dom) param {
  // this uses unstable / undocumented features. a better way is preferred.
  return !chpl_domainDistIsLayout(dom);
}

/* are we running distributed according to CHPL_COMM ? */
proc maybeDistributed() param {
  return CHPL_COMM!="none" || DISTRIBUTE_EVEN_WITH_COMM_NONE;
}

/*
   Make a BlockDist domain usually, but just return the local 'dom' unmodified
   in some cases:
    * if 'targetLocales' is 'none'
    * if CHPL_COMM=none.
*/
proc makeBlockDomain(dom: domain(?), targetLocales) {
  if maybeDistributed() && targetLocales.type != nothing {
    return blockDist.createDomain(dom, targetLocales=targetLocales);
  } else {
    return dom;
  }
}
/* Helper for the above to accept a range */
proc makeBlockDomain(rng: range(?), targetLocales) {
  return makeBlockDomain({rng}, targetLocales);
}

/* Helper for replicate() */
class ReplicatedWrapper {
  type eltType;
  var x: eltType;
}

/* Returns a distributed array containing replicated copies of 'x',
   or 'none' if replication is not necessary. This array can
   be indexed by 'here.id'.

   targetLocales should be an array of Locales or 'none' if
   replication is not necessary.

   Returns 'none' if 'maybeDistributed()' returns 'false'.
 */
proc replicate(x, targetLocales) {
  if maybeDistributed() && targetLocales.type != nothing {
    var minIdV = max(int);
    var maxIdV = min(int);
    forall loc in targetLocales with (min reduce minIdV, max reduce maxIdV) {
      minIdV reduce= loc.id;
      maxIdV reduce= loc.id;
    }
    const D = blockDist.createDomain(minIdV..maxIdV,
                                     targetLocales=targetLocales);
    var Result: [D] owned ReplicatedWrapper(x.type)?;

    reReplicate(x, Result);

    return Result;
  } else {
    return none;
  }
}

/* Given a distributed array created by 'replicate',
   re-assigns the replicated elements in that array to store x.

   Only replicates to the 'activeLocales'.
   Does not clear old replicands on other locales.
   Assumes that each activeLocales[i].id is contained in Result.domain.
 */
proc reReplicate(x, ref Result: [] owned ReplicatedWrapper(x.type)?,
                 const activeLocales = Result.targetLocales()) {
  //writeln("in reReplicate");

  proc helpReplicate(from: x.type, i: int, start: int, end: int) {
    // should already be on this locale...
    assert(here == activeLocales[i]);

    //writeln("helpReplicate lhs is ", Result[here.id], " x is ", x);

    // create a local copy
    if Result[here.id] == nil {
      Result[here.id] = new ReplicatedWrapper(from.type, from);
    } else {
      Result[here.id]!.x = from;
    }

    // get a reference to the copy we just created
    const ref newFrom = Result[here.id]!.x;

    // if 2*i is in the domain, replicate from Result[targetLocales[i].id]
    // but skip this case for i == 0 to avoid infinite loop
    if start <= 2*i && 2*i <= end && i != 0 {
      begin {
        on activeLocales[2*i] { // note: a GET, generally
          helpReplicate(newFrom, 2*i, start, end);
        }
      }
    }

    // ditto for 2*i+1
    if start <= 2*i+1 && 2*i+1 <= end {
      begin {
        on activeLocales[2*i+1] { // note: a GET, generally
          helpReplicate(newFrom, 2*i+1, start, end);
        }
      }
    }
  }

  sync {
    const start = activeLocales.domain.low;
    const end = activeLocales.domain.high;
    if start <= end {
      on activeLocales[start] {
        helpReplicate(x, start, start, end);
      }
    }
  }

  if EXTRA_CHECKS {
    //writeln("HERE activeLocales is ", activeLocales);
    for loc in activeLocales { 
      //writeln("loc is ", loc, " : ", loc.type:string);
      const ref elt = Result[loc.id];
      //writeln("elt is ", elt, " : ", elt.type:string);
      assert(x == elt!.x);
    }
    //writeln("POST-HERE");
  }
}

proc reReplicate(x, Result:nothing) {
  // nothing to do in this case
}

/* Accesses the result of 'replicate()' to get the local copy.

   'x' should be the same input that was provided to 'replicate()'
 */
proc getLocalReplicand(const ref x, replicated) const ref {
  if maybeDistributed() && replicated.type != nothing {
    return replicated.localAccess[here.id]!.x;
  } else {
    // return the value
    return x;
  }
}

/* Given a Block distributed domain and a range to slice it with,
   computes the locales that have a local subdomain that contains
   region.

   This is done in a communication-free manner.
 */
proc computeActiveLocales(const Dom: domain(?), const region: range) {
  if Dom.rank != 1 then compilerError("activeLocales only supports 1-D");

  //writeln("computeActiveLocales ", Dom, " ", region);

  // if the range is empty, return an empty array
  if region.size == 0 {
    const empty: [1..0] locale;
    //writeln("returning ", empty);
    return empty;
  }

  // if it's the full region or there is only one locale,
  // there isn't much to do here.
  if Dom.dim(0) == region || Dom.targetLocales().size == 1 {
    //writeln("returning ", Dom.targetLocales());
    return Dom.targetLocales();
  }

  // TODO: this could implemented more simply with an assumption
  // that Dom is Block distributed.

  var minIdV = max(int);
  var maxIdV = min(int);
  forall loc in Dom.targetLocales()
  with (min reduce minIdV, max reduce maxIdV) {
    minIdV = min(minIdV, loc.id);
    maxIdV = max(maxIdV, loc.id);
  }
  const minId = minIdV;
  const maxId = maxIdV;

  // count 1 for each locale that is active
  var CountPerLocale:[minId..maxId] int;
  local {
    forall loc in Dom.targetLocales() {
      // note: this should *not* move execution with 'on loc'
      const locRange = Dom.localSubdomain(loc).dim(0);
      const intersect = locRange[region];
      if intersect.size > 0 {
        CountPerLocale[loc.id] = 1;
      }
    }
  }
  //writeln("CountPerLocale ", CountPerLocale);
  // scan to compute packed offsets (to leave out zeros)
  var Ends = + scan CountPerLocale;
  var n = Ends.last;
  var ActiveLocales:[0..<n] locale;
  // store into the packed array
  local {
    forall (locId, count, end) in zip(minId..maxId, CountPerLocale, Ends) {
      if count > 0 {
        var start = end - count;
        ActiveLocales[start] = Locales[locId];
      }
    }
  }
  //writeln("returning ", ActiveLocales);
  return ActiveLocales;
}


/* Given a Block distributed domain or non-distributed domain,
   this iterator divides it into nLocales*nTasksPerLocale chunks
   (where nLocales=Dom.targetLocales().size) to be processed by a
   different task. Each task will only process local elements.

   A forall loop running this iterator will be distributed according to Dom
   and parallel according to nTasksPerLocale. The iteration will traverse
   only those elements in the range 'region' and create work only on
   those locales with elements in 'region'.

   This is different from a regular forall loop because it always divides
   Dom among tasks in the same way, assuming the same 'Dom', 'region', and
   'nTasksPerLocale' arguments. It does not make a different number of tasks
   depending on the number of running tasks.

   Yields (activeLocIdx, taskIdInLoc, chunk) for each chunk.

   activeLocIdx is the index among the active locales 0..

   taskIdInLoc is the task index within the locale

   chunk is a non-strided range that the task should handle

   Calling code that needs a unique task identifier can use
     activeLocIdx*nTasksPerLocale + taskIdInLoc
     (if the locale indices can be packed)
   or
     here.id*nTasksPerLocale + taskIdInLoc
     (if the locale indices need to fit into a global structure)

   to form a global task number in  0..<nLocales*nTasksPerLocale.
 */
iter divideIntoTasks(const Dom: domain(?),
                     const region: range,
                     nTasksPerLocale: int,
                     const ref activeLocales=computeActiveLocales(Dom, region))
{
  if Dom.rank != 1 then compilerError("divideIntoTasks only supports 1-D");
  if Dom.dim(0).strides != strideKind.one then
    compilerError("divideIntoTasks only supports non-strided domains");
  yield (0, 0, 0, Dom.dim(0));
  halt("serial divideIntoTasks should not be called");
}
iter divideIntoTasks(param tag: iterKind,
                     const Dom: domain(?),
                     const region: range,
                     nTasksPerLocale: int,
                     const ref activeLocales=computeActiveLocales(Dom, region))
 where tag == iterKind.standalone {

  if Dom.rank != 1 then compilerError("divideIntoTasks only supports 1-D");
  if Dom.dim(0).strides != strideKind.one then
    compilerError("divideIntoTasks only supports non-strided domains");
  if !Dom.hasSingleLocalSubdomain() {
    compilerError("divideIntoTasks only supports dists " +
                  "with single local subdomain");
    // note: it'd be possible to support; would just need to be written
    // differently, and consider both
    //  # local subdomains < nTasksPerLocale and the inverse.
  }

  coforall (loc, activeLocIdx) in zip(activeLocales, 0..) {
    on loc {
      const ref locDom = Dom.localSubdomain();
      const locRegion = locDom.dim(0)[region];
      coforall (chunk, taskIdInLoc) in
               zip(RangeChunk.chunks(locRegion, nTasksPerLocale), 0..) {
        //yield (nTasksPerLocale*locId + taskId, chunk);
        yield (activeLocIdx, taskIdInLoc, chunk);
      }
    }
  }
}

/**
 This iterator creates distributed parallelism to yield
 a bucket index for each task to process.

 Yields (region of bucket, bucket index, activeLocIdx, taskIdInLoc)

 BucketCounts should be the size of each bucket
 BucketEnds should be the indices (in Arr) just past the end of each bucket
 Arr is a potentially distributed array that drives the parallelism.
 'region' is the region within Arr that was counted.

 The Arr.targetLocales() must be in an increasing order by locale ID.

 Calling code that needs a unique task identifier can use
   activeLocIdx*nTasksPerLocale + taskIdInLoc
   (if the locale indices can be packed)
 or
   here.id*nTasksPerLocale + taskIdInLoc
   (if the locale indices need to fit into a global structure)
 */
iter divideByBuckets(const Arr: [],
                     const region: range,
                     const BucketCounts: [] int,
                     const BucketEnds: [] int,
                     nTasksPerLocale: int,
                     const ref activeLocales
                       = computeActiveLocales(Arr.domain, region)) {
  if Arr.domain.rank != 1 then compilerError("divideByBuckets only supports 1-D");
  if Arr.domain.dim(0).strides != strideKind.one then
    compilerError("divideByBuckets only supports non-strided domains");
  yield (0);
  halt("serial divideByBuckets should not be called");
}
iter divideByBuckets(param tag: iterKind,
                     const Arr: [],
                     const region: range,
                     const BucketCounts: [] int,
                     const BucketEnds: [] int,
                     const nTasksPerLocale: int,
                     const ref activeLocales
                       = computeActiveLocales(Arr.domain, region))
 where tag == iterKind.standalone {

  if Arr.domain.rank != 1 then compilerError("divideByBuckets only supports 1-D");
  if Arr.domain.dim(0).strides != strideKind.one then
    compilerError("divideByBuckets only supports non-strided domains");
  if !Arr.domain.hasSingleLocalSubdomain() {
    compilerError("divideByBuckets only supports dists " +
                  "with single local subdomain");
    // note: it'd be possible to support; would just need to be written
    // differently, and consider both
    //  # local subdomains < nTasksPerLocale and the inverse.
  }

  var minIdV = max(int);
  var maxIdV = min(int);
  forall loc in activeLocales
  with (min reduce minIdV, max reduce maxIdV) {
    minIdV = min(minIdV, loc.id);
    maxIdV = max(maxIdV, loc.id);
  }

  if EXTRA_CHECKS {
    var lastId = -1;
    for loc in activeLocales {
      if loc.id == lastId {
        halt("divideByBuckets requires increasing locales assignment");
      }
    }
  }

  const arrShift = region.low;
  const arrEnd = region.high;
  const bucketsEnd = BucketCounts.domain.high;

  var NBucketsPerLocale: [minIdV..maxIdV] int;
  forall (bucketSize,bucketEnd) in zip(BucketCounts, BucketEnds)
  with (+ reduce NBucketsPerLocale) {
    const bucketStart = bucketEnd - bucketSize;
    // count it towards the locale owning the middle of the bucket
    var checkIdx = bucketStart + bucketSize/2 + arrShift;
    // any 0-size buckets at the end of buckets to the last locale
    if checkIdx > arrEnd then checkIdx = arrEnd;
    const localeId = Arr[checkIdx].locale.id;
    NBucketsPerLocale[localeId] += 1;
  }

  const EndBucketPerLocale = + scan NBucketsPerLocale;

  coforall (loc, locId) in zip(activeLocales, activeLocales.domain) {
    on loc {
      const countBucketsHere = NBucketsPerLocale[loc.id];
      const endBucketHere = EndBucketPerLocale[loc.id];
      const startBucketHere = endBucketHere - countBucketsHere;

      // compute the array offset where work on this locale begins
      const startHere =
        if startBucketHere <= bucketsEnd
        then BucketEnds[startBucketHere] - BucketCounts[startBucketHere]
        else BucketEnds[bucketsEnd-1] - BucketCounts[bucketsEnd-1];

      // compute the total number of elements to be processed on this locale
      var eltsHere = 0;
      forall bucketIdx in startBucketHere..<endBucketHere
      with (+ reduce eltsHere) {
        eltsHere += BucketCounts[bucketIdx];
      }

      const perTask = divCeil(eltsHere, nTasksPerLocale);

      //writeln("locale bucket region ", startBucketHere..<endBucketHere,
      //        " elts ", eltsHere, " perTask ", perTask);

      // compute the number of buckets for each task
      // assuming that we just divide start..end into nTasksPerLocale equally
      var useNTasksPerLocale = nTasksPerLocale;
      if eltsHere == 0 {
        // set it to 0 to create an empty array to do no work on this locale
        useNTasksPerLocale = 0;
      }
      var NBucketsPerTask: [0..<useNTasksPerLocale] int;

      if eltsHere > 0 {
        forall bucketIdx in startBucketHere..<endBucketHere
        with (+ reduce NBucketsPerTask) {
          const bucketEnd = BucketEnds[bucketIdx];
          const bucketSize = BucketCounts[bucketIdx];
          const bucketStart = bucketEnd - bucketSize;
          var checkIdx = bucketStart + bucketSize/2 - startHere;
          // any 0-size buckets at the end of buckets to the last task
          if checkIdx >= eltsHere then checkIdx = eltsHere-1;
          const taskId = checkIdx / perTask;
          NBucketsPerTask[taskId] += 1;
        }
      }

      const EndBucketPerTask = + scan NBucketsPerTask;

      coforall (nBucketsThisTask, endBucketThisTask, taskId)
      in zip(NBucketsPerTask, EndBucketPerTask, 0..)
      {
        const startBucketThisTask = endBucketThisTask - nBucketsThisTask;
        const startBucket = startBucketHere + startBucketThisTask;
        const endBucket = startBucket + nBucketsThisTask;
        for bucketIdx in startBucket..<endBucket {
          const bucketSize = BucketCounts[bucketIdx];
          const bucketStart = BucketEnds[bucketIdx] - bucketSize;
          const start = bucketStart + arrShift;
          const end = start + bucketSize;
          yield (start..<end, bucketIdx, locId, taskId);
        }
      }
    }
  }
}


/* This function gives the size of an array of triangular indices
   for use with flattenTriangular.
 */
inline proc triangleSize(n: int) {
  return (n-1)*n/2;
}

/* This function converts an (i,j) index with i!=j and 0<=i<n and 0<=j<n
   into an index for an array storing the triangular matrix.
   As such it considers (i,j) and (j,i) the same.
   This function assumes i!=j.
 */
inline proc flattenTriangular(in i: int, in j: int) {
  if EXTRA_CHECKS {
    assert(i != j);
  }
  if i < j {
    i <=> j;
  }
  // now i > j
  var ret = triangleSize(i) + j;
  return ret;
}

/* get the i'th bit of 'bits' which should have unsigned int elements */
proc getBit(const bits: [], i: int) : bits.eltType {
  if !isUintType(bits.eltType) {
    compilerError("getBit requires unsigned integer elements");
  }

  type t = bits.eltType;
  param wordBits = numBits(t);
  const wordIdx = i / wordBits;
  const phase = i % wordBits;
  const word = bits[wordIdx];
  const shift = wordBits - 1 - phase;
  return (word >> shift) & 1;
}

/* set the i'th bit of 'bits' which should have unsigned int elements */
proc setBit(ref bits: [], i: int) {
  if !isUintType(bits.eltType) {
    compilerError("getBit requires unsigned integer elements");
  }

  type t = bits.eltType;
  param wordBits = numBits(t);
  const wordIdx = i / wordBits;
  const phase = i % wordBits;
  const shift = wordBits - 1 - phase;
  ref word = bits[wordIdx];
  word = word | (1:t << shift);
}

/*
  Finds and returns the integer index i such that

  arr[i] <= target < arr[i+1]

  May return -1 for i; in that case target < arr[0].
  May return n-1 for i; in that case arr[n-1] <= target.
  Assumes that 'arr' is sorted.
 */
proc bsearch(const arr: [] int, target: int) {
  const n = arr.size;
  var a, b, middle = 0;

  if EXTRA_CHECKS {
    assert(isSorted(arr));
  }
  if target < arr[0] then return -1;
  if arr[n-1] <= target then return n-1;

  a = 0;
  b = n-1;
  // always we have that arr[a] <= target < arr[b].

  // divide the search space in half
  while b - a > 1 {
    middle = (a + b) / 2;
    if target < arr[middle] then b = middle;
    else a = middle; // arr[middle] <= target
  }

  if EXTRA_CHECKS {
    assert(arr[a] <= target && target < arr[a+1]);
  }
  return a;
}

/*
 Gather files; if the passed path is a file, store it in to the list;
 if it is a directory, gather files contained in that directory, recursively,
 into the list.
 */
proc gatherFiles(ref files: list(string), path: string) throws {
  if isFile(path) {
    files.pushBack(path);
  }
  if isDir(path) {
    for found in findFiles(path, recursive=true) {
      files.pushBack(found);
    }
  }
}

private inline proc toUpper(x: uint(8)): uint(8) {
  extern proc toupper(c: c_int): c_int;
  return toupper(x):uint(8);
}

// assumes that the input is already upper case
private inline proc complement(x: uint(8)): uint(8) {
  param A = "A".toByte();
  param T = "T".toByte();
  param G = "G".toByte();
  param C = "C".toByte();

  if x == C then return G;
  if x == G then return C;
  if x == A then return T;
  if x == T then return A;

  // otherwise, assume not DNA
  return x;
}

/* Computes the reverse complement of a region of an input array and stores it
   in a region of the output array. The input and output arrays can be the same
   array provided that the regions are non-overlapping. */
proc reverseComplement(const ref input: [] uint(8),
                       inputRegion: range,
                       ref output: [] uint(8),
                       outputRegion: range) {
  if EXTRA_CHECKS {
    assert(inputRegion.size == outputRegion.size);
  }

  const n = inputRegion.size;
  for i in 0..<n {
    const inputIdx = i + inputRegion.first;
    const outputIdx = n - 1 - i + outputRegion.first;
    output[outputIdx] = complement(input[inputIdx]);
  }
}

private const fastaExtensions = [".fasta", ".fas", ".fa", ".fna",
                                 ".ffn", ".faa", ".mpfa", ".frn"];

/* Returns 'true' if 'path' refers to a fasta file */
proc isFastaFile(path: string): bool throws {
  var foundExt = false;
  for ext in fastaExtensions {
    if path.toLower().endsWith(ext) {
      foundExt = true;
    }
  }

  if foundExt {
    var r = IO.openReader(path, region=0..1);
    return r.readByte() == ">".toByte();
  }

  return false;
}

/* Computes the size of the nucleotide data that will
   be read by readFastaFileSequence */
proc computeFastaFileSize(path: string) throws {
  extern proc isspace(c: c_int): c_int;

  // compute the file size without > lines or whitespace
  var r = IO.openReader(path);
  var inDescLine = false;
  var count = 0;
  while true {
    try {
      var byte = r.readByte();
      if byte == ">".toByte() {
        inDescLine = true;
        count += 1; // we will put > characters to divide sequences
      } else if byte == "\n".toByte() && inDescLine {
        inDescLine = false;
      }
      if isspace(byte) == 0 && !inDescLine {
        count += 1;
      }
    } catch e: EofError {
      break;
    }
  }

  if INCLUDE_REVERSE_COMPLEMENT {
    count = 2*count;
  }

  return count;
}

/* Reads a the sequence portion of a fasta file into a region of an array.
   The resulting array elements will contain a > at the start of each sequence
   followed by the nucleotide data. The whitespace and sequence
   descriptions are removed.
   The region size should match 'computeFastaFileSize'. */
proc readFastaFileSequence(path: string,
                           ref data: [] uint(8),
                           region: range,
                           verbose = true) throws
{
  extern proc isspace(c: c_int): c_int;

  if region.strides != strideKind.one {
    compilerError("Range should be stride one");
  }
  var dataStart = region.low;
  var n = region.size;
  var r = IO.openReader(path);
  var inDescLine = false;
  var count = 0;
  var desc = "";
  while true {
    try {
      var byte = r.readByte();
      if byte == ">".toByte() {
        inDescLine = true;
        if count < n {
          data[dataStart + count] = byte;
        }
        desc = "";
        count += 1;
      } else if byte == "\n".toByte() && inDescLine {
        inDescLine = false;
        if verbose {
          writeln("Reading sequence ", desc);
        }
      }
      if inDescLine {
        desc.appendCodepointValues(byte);
      } else if isspace(byte) == 0 {
        if count < n {
          data[dataStart + count] = toUpper(byte);
        }
        count += 1;
      }
    } catch e: EofError {
      break;
    }
  }

  if INCLUDE_REVERSE_COMPLEMENT {
    // store the reverse complement just after the original sequence;
    // except the initial > would be a trailing >,
    // so emit a separator and don't revcomp the initial >
    data[dataStart + count] = ">".toByte();
    const countLessOne = count - 1; // don't revcomp the initial separator,
                                    // because it would end up at the end
    reverseComplement(data, dataStart+1..#countLessOne,
                      data, dataStart+1+count..#countLessOne);
    count = 2*count;
  }

  if n != count {
    // region does not match the file
    throw new Error("count mismatch in readFastaFileSequence");
  }
}

/* Computes the size of a file. Handles fasta files specially to compute the
   size of the nucleotide data only. */
proc computeFileSize(path: string) throws {
  if isFastaFile(path) {
    return computeFastaFileSize(path);
  } else {
    return getFileSize(path);
  }
}

/* Read the data in a file into a portion of an array. Handles fasta
   files specially to read only the nucleotide data.
   The region should match the file size. */
proc readFileData(path: string,
                  ref data: [] uint(8),
                  region: range,
                  verbose = true) throws
{
  if isFastaFile(path) {
    readFastaFileSequence(path, data, region, verbose);
  } else {
    var r = IO.openReader(path);
    r.readAll(data[region]);
  }
}

/* This function trims away the common portions of the paths
   to make output using the paths more consise. It modifies the 'paths'
   array.
 */
proc trimPaths(ref paths:[] string) {
  var common: string = Path.commonPath(paths);
  if common != "" && !common.endsWith("/") then common += "/";
  for p in paths {
    if p.startsWith(common) {
      p = p[common.size..];
    }
  }
}

/*
 Given a list of files, read in all files into a single array
 and produce several related data items:
   * the array containing all of the data
   * a sorted list of paths
   * a corresponding array of file sizes
   * a corresponding list of offsets where each file starts,
     which, contains an extra entry for the total size

 The resulting arrays will be Block distributed among 'locales'.
 */
proc readAllFiles(const ref files: list(string),
                  locales: [ ] locale,
                  out allData: [] uint(8),
                  out allPaths: [] string,
                  out concisePaths: [] string,
                  out fileSizes: [] int,
                  out fileStarts: [] int,
                  out totalSize: int) throws {
  var locPaths = files.toArray();
  for p in locPaths {
    p = Path.normPath(p);
  }
  sort(locPaths);

  const ByFileDom = makeBlockDomain(0..<locPaths.size, locales);
  const paths:[ByFileDom] string = forall i in ByFileDom do locPaths[i];
  const nFiles = paths.size;

  if nFiles == 0 {
    throw new Error("no input files provided");
  }

  // compute the size for the concatenated input
  var sizes: [paths.domain] int;
  forall (path, sz) in zip(paths, sizes) {
    sz = computeFileSize(path);
    sz += 1; // add a null byte to separate files
  }

  const fileEnds = + scan sizes;
  const total = fileEnds.last;

  const TextDom = makeBlockDomain(0..<total+INPUT_PADDING, locales);
  var thetext:[TextDom] uint(8);

  // read each file
  forall (path, sz, end) in zip(paths, sizes, fileEnds) {
    const start = end - sz;
    const count = sz - 1; // we added a null byte above
    readFileData(path, thetext, start..#count);
  }

  // compute fileStarts
  const StartsDom = makeBlockDomain(0..nFiles, locales);
  var starts:[StartsDom] int;
  starts[0] = 0;
  starts[1..nFiles] = fileEnds;

  // compute trimmed paths
  var tPaths = paths;
  trimPaths(tPaths);

  // return various values
  allData = thetext;
  allPaths = paths;
  concisePaths = tPaths;
  fileSizes = sizes;
  fileStarts = starts;
  totalSize = total;
}

proc offsetToFileIdx(const fileStarts: [] int, offset: int) {
  const fileIdx = bsearch(fileStarts, offset);
  if EXTRA_CHECKS {
    assert(0 <= fileIdx && fileIdx < fileStarts.size);
  }
  return fileIdx;
}

proc printSuffix(offset: int, thetext: [], fileStarts: [] int, lcp: int, amt: int) {
  const end = min(offset + amt, thetext.size);
  for i in offset..<end {
    var ch = thetext[i];
    if 32 <= ch && ch <= 126 { // note: 32 is ' ' and 126 is ~
      // char is OK
    } else {
      ch = 46; // .
    }
    writef("%c", ch);
  }
  const fileIdx = offsetToFileIdx(fileStarts, offset);
  writef(" % 8i f%i lcp%i\n", offset, fileIdx, lcp);
}


proc atomicStoreMinRelaxed(ref dst: atomic int, src: int) {
  if Version.chplVersion >= new Version.versionValue(2,3) {
    dst.min(src, memoryOrder.relaxed);
  } else {
    var t = dst.read(memoryOrder.relaxed);
    while min(src, t) != t {
      // note: dst.compareExchangeWeak updates 't' if it fails
      // to the current value
      if dst.compareExchangeWeak(t, src, memoryOrder.relaxed) {
        return;
      }
    }
  }
}

proc atomicStoreMaxRelaxed(ref dst: atomic int, src: int) {
  if Version.chplVersion >= new Version.versionValue(2,3) {
    dst.max(src, memoryOrder.relaxed);
  } else {
    var t = dst.read(memoryOrder.relaxed);
    while max(src, t) != t {
      // note: dst.compareExchangeWeak updates 't' if it fails
      // to the current value
      if dst.compareExchangeWeak(t, src, memoryOrder.relaxed) {
        return;
      }
    }
  }
}

// helper for computeBitsPerChar / packInput
// returns alphaMap and sets newMaxChar
private proc computeAlphaMap(Input:[],
                             const n: Input.domain.idxType,
                             out newMaxChar: int) {
  // compute the minimum and maximum character in the input
  var minCharacter = max(int);
  var maxCharacter = -1;
  forall (x,i) in zip(Input, Input.domain)
    with (min reduce minCharacter, max reduce maxCharacter) {
    if i < n {
      const asInt = x:int;
      minCharacter reduce= asInt;
      maxCharacter reduce= asInt;
    }
  }

  var alphaMap:[minCharacter..maxCharacter] int;
  forall (x,i) in zip(Input, Input.domain) with (+ reduce alphaMap) {
    if i < n {
      alphaMap[x:int] += 1;
    }
  }

  // set each element to 1 if it is present, 0 otherwise
  // (could be handled with || reduce and an array of bools)
  forall x in alphaMap {
    if x > 0 then x = 1;
  }

  // now count the number of unique characters
  const nUniqueChars = + reduce alphaMap;

  writeln("nUniqueChars is ", nUniqueChars);

  // now set the value of each character
  {
    const tmp = + scan alphaMap;
    alphaMap = tmp - 1;
  }

  newMaxChar = max(1, nUniqueChars-1);
  writeln("newMaxChar is ", newMaxChar);

  return alphaMap;
}


/* Returns a number of bits per character that can be used with packInput */
proc computeBitsPerChar(Input: [], const n: Input.domain.idxType) {
  type characterType = Input.eltType;

  if n <= 0 {
    return numBits(characterType);
  }

  var newMaxChar = 0;
  var ignoredAlphaMap = computeAlphaMap(Input, n, /* out */ newMaxChar);

  const bitsPerChar = numBits(uint) - BitOps.clz(newMaxChar);

  assert(newMaxChar < (1 << bitsPerChar));

  return bitsPerChar: int;
}

// helper for packInput that works with a mapping from
// characters in Input to the packed version, or 'none' if does not
// need to be used.
private proc packInputWithAlphaMap(type wordType,
                                   Input: [],
                                   const n: Input.domain.idxType,
                                   bitsPerChar: int,
                                   alphaMap) {
  // create the packed input array
  param bitsPerWord = numBits(wordType);
  const endBit = n*bitsPerChar;
  const nWords = divCeil(n*bitsPerChar, bitsPerWord);
  const PackedDom = makeBlockDomain(0..<nWords+INPUT_PADDING,
                                    Input.targetLocales());
  var PackedInput:[PackedDom] wordType;

  // now remap the input
  forall (word, wordIdx) in zip(PackedInput, PackedInput.domain)
    with (in alphaMap) {

    // gets the character at Input[charIdx]
    // including checking bounds & applying alphaMap if it is not 'none'
    inline proc getPackedChar(charIdx) : wordType {
      var unpackedChar: Input.eltType = 0;
      if unpackedChar < n {
        unpackedChar = Input[charIdx];
      }

      var packedChar: wordType;
      if alphaMap.type != nothing {
        packedChar = alphaMap[unpackedChar:int]:wordType;
      } else {
        packedChar = unpackedChar:wordType;
      }

      return packedChar;
    }

    // What contributes to wordIdx in PackedInput?
    // It contains the bits bitsPerWord*wordIdx..#bitsPerWord
    const startBit = bitsPerWord*wordIdx;

    // get started
    var w:wordType = 0;
    var charIdx = startBit / bitsPerChar;
    var skip = startBit % bitsPerChar;
    var bitsRead = 0;
    if skip != 0 && startBit < endBit {
      // handle reading only the right part of the 1st character
      // skip the top 'skip' bits and read the rest
      var nBottomBitsToRead = bitsPerChar - skip;
      const char = getPackedChar(charIdx);
      var bottomBits = char & ((1:wordType << nBottomBitsToRead) - 1);
      w |= bottomBits;
      bitsRead += nBottomBitsToRead;
      charIdx += 1;
    }

    while bitsRead + bitsPerChar <= bitsPerWord &&
          startBit + bitsRead + bitsPerChar <= endBit {
      // read a whole character
      const char = getPackedChar(charIdx);
      w <<= bitsPerChar;
      w |= char;
      bitsRead += bitsPerChar;
      charIdx += 1;
    }

    if bitsRead < bitsPerWord && startBit + bitsRead < endBit {
      // handle reading only the left part of the last character
      const nTopBitsToRead = bitsPerWord - bitsRead;
      const nBottomBitsToSkip = bitsPerChar - nTopBitsToRead;
      const char = getPackedChar(charIdx);
      var topBits = char >> nBottomBitsToSkip;
      w <<= nTopBitsToRead;
      w |= topBits;
      bitsRead += nTopBitsToRead;
      charIdx += 1;
    }

    if bitsRead < bitsPerWord {
      // pad with 0 if anything is not yet read
      w <<= bitsPerWord - bitsRead;
    }

    // store the word we computed back to the array
    word = w;
  }

  return PackedInput;
}

/**
  Pack the input. Return an array of words where each word contains packed
  characters, and set bitsPerChar to indicate how many bits each character
  occupies in the packed data.

  bitsPerChar can be computed with computeBitsPerChar.
  */
proc packInput(type wordType,
               Input: [],
               const n: Input.domain.idxType,
               bitsPerChar: int) {
  type characterType = Input.eltType;

  if !isUintType(wordType) {
    compilerError("packInput requires wordType is a uint(w)");
  }
  if !isUintType(characterType) {
    compilerError("packInput requires Input.eltType is a uint(w)");
  }
  if numBits(wordType) < numBits(characterType) {
    compilerError("packInput requires" +
                  " numBits(wordType) >= numBits(Input.eltType)" +
                  " note wordType=" + wordType:string +
                  " has " + numBits(wordType):string + " bits" +
                  " eltType=" + Input.eltType:string +
                  " has " + numBits(characterType):string + " bits");
  }

  if EXTRA_CHECKS {
    assert(bitsPerChar >= computeBitsPerChar(Input, n));
  }

  if n <= 0 {
    const PackedDom = makeBlockDomain(0..<1+INPUT_PADDING,
                                      Input.targetLocales());
    var PackedInput:[PackedDom] wordType;
    return PackedInput;
  }

  if bitsPerChar <= 16 {
    var newMaxChar = 0;
    const alphaMap = computeAlphaMap(Input, n, /* out */ newMaxChar);
    assert(newMaxChar < (1 << bitsPerChar));

    return packInputWithAlphaMap(wordType, Input, n, bitsPerChar, alphaMap);
  }

  // otherwise, pack but don't use alpha map
  return packInputWithAlphaMap(wordType, Input, n, bitsPerChar, none);
}

/* Loads a word full of character data from a PackedInput
   starting at the bit offset startBit */
inline proc loadWord(PackedInput: [], const startBit: int) {
  // load word 1 and word 2
  type wordType = PackedInput.eltType;

  const wordIdx = startBit / numBits(wordType);
  const word0 = PackedInput[wordIdx];
  const word1 = PackedInput[wordIdx+1];
  return loadWordWithWords(word0, word1, startBit);
}
/* Like loadWord, but assumes that the relevant
   potential words that are needed are already loaded. */
inline proc loadWordWithWords(word0: ?wordType, word1: wordType,
                              const startBit: int) {
  const shift = startBit % numBits(wordType);
  const ret =  if shift == 0 then word0
               else word0 << shift | word1 >> (numBits(wordType) - shift);
  return ret;
}

}
