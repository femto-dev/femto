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


import CTypes.{c_int, c_sizeof, c_uintptr, c_ptr, c_ptrConst};
import OS.POSIX.memcpy;
import FileSystem.{isFile, isDir, findFiles, getFileSize};
import FileSystem;
import IO;
import List.list;
import Path;
import BitOps;
import Sort.{sort,isSorted};
import Math.divCeil;
import BlockDist.blockDist;
import ChplConfig.CHPL_COMM;
import RangeChunk;
import Version;
import Time;
import CopyAggregation;
import CopyAggregation.DstAggregator;
import Communication;
import OS.FileNotFoundError;

use SHA256Implementation;

import SuffixSort.{EXTRA_CHECKS, TIMING, TRACE, INPUT_PADDING,
                   DISTRIBUTE_EVEN_WITH_COMM_NONE};

/* For FASTA files, when reading them, also read in the reverse complement */
config const INCLUDE_REVERSE_COMPLEMENT=true;

/* Bulk copy "page" size */
config const bulkCopyPageSz:uint = 8*1024;

/* Yield after this many iterations */
config const yieldPeriod = 2048;
const YIELD_PERIOD = yieldPeriod;


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

proc makeBlockArray(region: range(?), targetLocales, type eltType) {
  const Dom = makeBlockDomain(region, targetLocales);
  var Ret: [Dom] eltType;
  return Ret;
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

  warning("serial divideIntoTasks called");

  for (loc, activeLocIdx) in zip(activeLocales, 0..) {
    on loc {
      const ref locDom = Dom.localSubdomain();
      const locRegion = locDom.dim(0)[region];
      for (chunk, taskIdInLoc) in
           zip(RangeChunk.chunks(locRegion, nTasksPerLocale), 0..) {
        //yield (nTasksPerLocale*locId + taskId, chunk);
        yield (activeLocIdx, taskIdInLoc, chunk);
      }
    }
  }
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

/* Given a Block distributed domain or non-distributed domain,
   this iterator divides it into per-locale chunks and processes
   each on its owning locale.

   yields (activeLocIdx, chunk)
*/
iter divideByLocales(const Dom: domain(?),
                     const region: range,
                     const ref activeLocales=computeActiveLocales(Dom, region))
{
  if Dom.rank != 1 then compilerError("divideByLocales only supports 1-D");
  if Dom.dim(0).strides != strideKind.one then
    compilerError("divideByLocales only supports non-strided domains");
  yield (0, Dom.dim(0));
  halt("serial divideByLocales should not be called");
}
iter divideByLocales(param tag: iterKind,
                     const Dom: domain(?),
                     const region: range,
                     const ref activeLocales=computeActiveLocales(Dom, region))
 where tag == iterKind.standalone {

  if Dom.rank != 1 then compilerError("divideByLocales only supports 1-D");
  if Dom.dim(0).strides != strideKind.one then
    compilerError("divideByLocales only supports non-strided domains");
  if !Dom.hasSingleLocalSubdomain() {
    compilerError("divideByLocales only supports dists " +
                  "with single local subdomain");
    // note: it'd be possible to support; would just need to be written
    // differently, and consider both
    //  # local subdomains < nTasksPerLocale and the inverse.
  }

  coforall (loc, activeLocIdx) in zip(activeLocales, 0..) {
    on loc {
      const ref locDom = Dom.localSubdomain();
      const locRegion = locDom.dim(0)[region];
      yield (activeLocIdx, locRegion);
    }
  }
}

/* Divide up a range into "pages" -- that is, regions that
   have start indices that are aligned (that is, startidx % alignment == 0).
   The first region won't be aligned.

   Parallel standalone or serial, but not distributed.

   Yields non-empty ranges to be processed independently.
 */
iter divideIntoPages(const region: range(?),
                     alignment: region.idxType,
                     nTasksPerLocale: region.idxType = computeNumTasks()) {
  if region.bounds != boundKind.both {
    compilerError("divideIntoPages only supports bounded ranges");
  }
  if region.strides != strideKind.one {
    compilerError("divideIntoPages only supports non-strided ranges");
  }

  if region.size > 0 {
    yield region;
  }
}
iter divideIntoPages(param tag: iterKind,
                     const region: range(?),
                     alignment: region.idxType,
                     nTasksPerLocale: region.idxType = computeNumTasks())
 where tag == iterKind.standalone {
  if region.bounds != boundKind.both {
    compilerError("divideIntoPages only supports bounded ranges");
  }
  if region.strides != strideKind.one {
    compilerError("divideIntoPages only supports non-strided ranges");
  }

  const firstPage = region.low / alignment;
  const lastPage = region.high / alignment;

  if lastPage - firstPage < nTasksPerLocale {
    // just yield the whole range (serially) if the range doesn't
    // have enough "pages" for nTasksPerLocale.
    if region.size > 0 {
      yield region;
    }
    return;
  } else {
    coforall pages in RangeChunk.chunks(firstPage..lastPage, nTasksPerLocale) {
      for whichPage in pages {
        const pageRange = whichPage*alignment..#alignment;
        const toYield = region[pageRange]; // intersect page with input
        if toYield.size > 0 {
          yield toYield;
        }
      }
    }
  }
}


/* Yields the elements in a range but rotated by 'shift',
   that is, the elements yielded start at 'region.low+shift'
   and then wrap around. */
iter rotateRange(const region: range,
                 shift: int,
                 nTasksPerLocale: int = computeNumTasks()) {

  if region.size == 0 {
    return;
  }

  const modShift = mod(shift, region.size);
  const split = region.low + modShift;
  if EXTRA_CHECKS {
    assert(region.contains(split));
  }

  // first do the region starting at 'split' (normally, region.low+shift)
  for i in split..region.high {
    yield i;
  }

  // then do the region ending before 'split'
  for i in region.low..<split {
    yield i;
  }
}
iter rotateRange(param tag: iterKind,
                 const region: range,
                 shift: int,
                 nTasksPerLocale: int = computeNumTasks())
 where tag == iterKind.standalone {

  if region.size == 0 {
    return;
  }

  const modShift = mod(shift, region.size);
  const split = region.low + modShift;
  if EXTRA_CHECKS {
    assert(region.contains(split));
  }

  // first do the region starting at 'split' (normally, region.low+shift)
  coforall r in RangeChunk.chunks(split..region.high, nTasksPerLocale) {
    for i in r {
      yield i;
    }
  }

  // then do the region ending before 'split'
  coforall r in RangeChunk.chunks(region.low..<split, nTasksPerLocale) {
    for i in r {
      yield i;
    }
  }
}

/* Copy a region between a default (local) array and a Block array.
   This code is optimized for the case that the region is relatively
   small and most or all of it is local.
   It assumes that the arrays are 1-D and the ranges are non-strided
   and bounded.
 */
proc bulkCopy(ref dst: [], dstRegion: range,
              const ref src: [], srcRegion: range) : void {
  if EXTRA_CHECKS { // or boundsChecking
    assert(dst.domain.dim(0).contains(dstRegion));
    assert(src.domain.dim(0).contains(srcRegion));
    assert(dstRegion.size == srcRegion.size);
  }

  if dst.eltType != src.eltType {
    compilerError("bulkCopy array element types need to match");
  }

  if isDistributedDomain(dst.domain) &&
     !isSubtype(dst.domain.distribution.type, blockDist) {
    compilerError("bulkCopy only works for blockDist as non-local array");
    // could work for anything with contiguous elements
  }

  if isDistributedDomain(src.domain) &&
     !isSubtype(src.domain.distribution.type, blockDist) {
    compilerError("bulkCopy only works for blockDist as non-local array");
  }

  const eltSize = c_sizeof(dst.eltType);

  // TODO: these are workarounds to avoid
  // error: references to remote data cannot be passed to external routines like 'c_pointer_return_const'
  proc addrOf(const ref p): c_ptr(p.type) {
    return __primitive("_wide_get_addr", p): c_ptr(p.type);
  }
  proc addrOfConst(const ref p): c_ptrConst(p.type) {
    return __primitive("_wide_get_addr", p): c_ptrConst(void) : c_ptrConst(p.type);
  }


  // helper for PUTs
  proc helpPut(dstStart: int, srcStart: int, size: int) {
    if size <= 0 {
      return;
    }

    const startLocale = dst[dstStart].locale.id;
    const endLocale = dst[dstStart+size-1].locale.id;
    if startLocale == endLocale {
      const nBytes = (size * eltSize):uint;
      const dstPtr = addrOf(dst[dstStart]):c_uintptr:uint;
      const srcPtr = addrOf(src[srcStart]):c_uintptr:uint;
      if startLocale == here.id {
        if EXTRA_CHECKS {
          for i in 0..<size {
            assert(dst[dstStart+i].locale == here);
            assert(src[srcStart+i].locale == here);
          }
        }

        forall dstPg in divideIntoPages(dstPtr..#nBytes, bulkCopyPageSz) {
          const dstPartPtr = dstPg.low:c_ptr(void);
          const srcPartPtr = (srcPtr + (dstPg.low - dstPtr)):c_ptr(void);
          if EXTRA_CHECKS {
            assert((dstPtr..#nBytes).contains(dstPartPtr:uint..#dstPg.size));
            assert((srcPtr..#nBytes).contains(srcPartPtr:uint..#dstPg.size));
          }
          memcpy(dstPartPtr, srcPartPtr, dstPg.size);
        }
      } else {
        if EXTRA_CHECKS {
          for i in 0..<size {
            assert(dst[dstStart+i].locale.id == startLocale);
            assert(src[srcStart+i].locale == here);
          }
        }
        forall dstPg in divideIntoPages(dstPtr..#nBytes, bulkCopyPageSz) {
          const dstPartPtr = dstPg.low:c_ptr(void);
          const srcPartPtr = (srcPtr + (dstPg.low - dstPtr)):c_ptr(void);
          if EXTRA_CHECKS {
            assert((dstPtr..#nBytes).contains(dstPartPtr:uint..#dstPg.size));
            assert((srcPtr..#nBytes).contains(srcPartPtr:uint..#dstPg.size));
          }
          Communication.put(dstPartPtr, srcPartPtr, startLocale, dstPg.size);
        }
      }
    } else {
      // do it with bulk transfer since many locales are involved
      if TRACE {
        writeln("warning: unopt bulkCopy PUT");
      }
      dst[dstStart..#size] = src[srcStart..#size];
    }
  }

  // helper for GETs
  proc helpGet(dstStart: int, srcStart: int, size: int) {
    if size <= 0 {
      return;
    }

    const startLocale = src[srcStart].locale.id;
    const endLocale = src[srcStart+size-1].locale.id;
    if startLocale == endLocale {
      const nBytes = (size * eltSize):uint;
      const dstPtr = addrOf(dst[dstStart]):c_uintptr:uint;
      const srcPtr = addrOf(src[srcStart]):c_uintptr:uint;
      if startLocale == here.id {
        if EXTRA_CHECKS {
          for i in 0..<size {
            assert(dst[dstStart+i].locale == here);
            assert(src[srcStart+i].locale == here);
          }
        }
        forall dstPg in divideIntoPages(dstPtr..#nBytes, bulkCopyPageSz) {
          const dstPartPtr = dstPg.low:c_ptr(void);
          const srcPartPtr = (srcPtr + (dstPg.low - dstPtr)):c_ptr(void);
          if EXTRA_CHECKS {
            assert((dstPtr..#nBytes).contains(dstPartPtr:uint..#dstPg.size));
            assert((srcPtr..#nBytes).contains(srcPartPtr:uint..#dstPg.size));
          }
          memcpy(dstPartPtr, srcPartPtr, dstPg.size);
        }
      } else {
        if EXTRA_CHECKS {
          for i in 0..<size {
            assert(dst[dstStart+i].locale == here);
            assert(src[srcStart+i].locale.id == startLocale);
          }
        }
        forall dstPg in divideIntoPages(dstPtr..#nBytes, bulkCopyPageSz) {
          const dstPartPtr = dstPg.low:c_ptr(void);
          const srcPartPtr = (srcPtr + (dstPg.low - dstPtr)):c_ptr(void);
          if EXTRA_CHECKS {
            assert((dstPtr..#nBytes).contains(dstPartPtr:uint..#dstPg.size));
            assert((srcPtr..#nBytes).contains(srcPartPtr:uint..#dstPg.size));
          }
          Communication.get(dstPartPtr, srcPartPtr, startLocale, dstPg.size);
        }
      }
    } else {
      // do it with bulk transfer since many locales are involved
      if TRACE {
        writeln("warning: unopt bulkCopy GET");
      }
      dst[dstStart..#size] = src[srcStart..#size];
    }
  }

  const dstLocal = dst.localSubdomain().dim(0)[dstRegion] == dstRegion;
  const srcLocal = src.localSubdomain().dim(0)[srcRegion] == srcRegion;

  if dstLocal && srcLocal {
    // neither are distributed, so do a memcpy
    helpPut(dstRegion.low, srcRegion.low, dstRegion.size);
    return;
  }

  if !dstLocal && srcLocal {
    // dst is distributed, src is not
    var middlePart = dst.localSubdomain().dim(0)[dstRegion];
    if middlePart.size == 0 {
      // just use the subdomain containing the first dst element
      // not expecting this to come up much
      middlePart =
        dst.localSubdomain(dst[dstRegion.low].locale).dim(0)[dstRegion];
    }
    const nonLocalBefore = dstRegion.low..<middlePart.low;
    const nonLocalAfter = middlePart.high+1..dstRegion.high;
    // now there are 3 regions:
    //  * nonLocalBefore is the dst region before the local part
    //  * localDstPart is the region before the local part
    //  * nonLocalAfter is the dst region after the local part

    helpPut(nonLocalBefore.low,
            srcRegion.low + (nonLocalBefore.low - dstRegion.low),
            nonLocalBefore.size);

    helpPut(middlePart.low,
            srcRegion.low + (middlePart.low - dstRegion.low),
            middlePart.size);

    helpPut(nonLocalAfter.low,
            srcRegion.low + (nonLocalAfter.low - dstRegion.low),
            nonLocalAfter.size);
    return;
  }

  if !srcLocal && dstLocal {
    // src is distributed, dst is not
    var middlePart = src.localSubdomain().dim(0)[srcRegion];
    if middlePart.size == 0 {
      middlePart =
        src.localSubdomain(src[srcRegion.low].locale).dim(0)[srcRegion];
    }
    const nonLocalBefore = srcRegion.low..<middlePart.low;
    const nonLocalAfter = middlePart.high+1..srcRegion.high;

    helpGet(dstRegion.low + (nonLocalBefore.low - srcRegion.low),
            nonLocalBefore.low,
            nonLocalBefore.size);

    helpGet(dstRegion.low + (middlePart.low - srcRegion.low),
            middlePart.low,
            middlePart.size);

    helpGet(dstRegion.low + (nonLocalAfter.low - srcRegion.low),
            nonLocalAfter.low,
            nonLocalAfter.size);
    return;
  }

  // Otherwise, they both have remote elements.
  // Find an element on the source locale and use an 'on' statement
  // to PUT back from there.
  // Use bulk transfer
  if TRACE {
    writeln("warning: unopt bulkCopy (both remote)");
  }
  dst[dstRegion] = src[srcRegion];
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
  } else if isDir(path) {
    for found in findFiles(path, recursive=true) {
      files.pushBack(found);
    }
  } else {
    throw new FileNotFoundError(path);
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

  const nTasksPerLocale = computeNumTasks(ignoreRunning=true);
  const n = inputRegion.size;
  forall (_, _, chunk)
  in divideIntoTasks(input.domain, inputRegion, nTasksPerLocale) {
    var agg = new CopyAggregation.DstAggregator(uint(8));
    for inputIdx in chunk {
      const i = inputIdx - inputRegion.first;
      const outputIdx = n - 1 - i + outputRegion.first;
      const val = complement(input[inputIdx]);
      agg.copy(output[outputIdx], val);
    }
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

/* Reads sequence data that starts within 'taskFileRegion'
   and optionally stores it into data[dstRegion] (if 'data' is not 'none').

   Appends the offsets in dstRegion of > characters to the 'sequencesStarts'
   list (which can be disabled if sequenceStarts=none).
   and similarly the sequence description is stored in the list sequenceDescs
   if it is not 'none'.

   'agg' should be a DstAggregator(uint(8)) or 'none'.
   Returns the count of number of characters read.
 */
proc readFastaSequencesStartingInRegion(path: string,
                                        taskFileRegion: range,
                                        allFileRegion: range,
                                        ref data,
                                        dstRegion: range,
                                        ref agg,
                                        ref sequenceStarts,
                                        ref sequenceDescs) throws {
  extern proc isspace(c: c_int): c_int;

  // skip to > within the task's chunk
  var r = IO.openReader(path, region=taskFileRegion.low..allFileRegion.high);
  try {
    r.advanceTo(">");
  } catch e: IO.EofError {
    return 0;
  } catch e: IO.UnexpectedEofError {
    return 0;
  }

  var dataStart = dstRegion.low;
  var dataSize = dstRegion.size;
  var count = 0;
  var descOffset = r.offset();
  var lastDescCount = 0;
  var inDescLine = false;
  var desc = "";
  // find any sequences that start in this task's chunk
  // (i.e. read sequences starting with > that is within taskFileRegion)
  while true {
    try {
      var byte = r.readByte();
      if byte == ">".toByte() {
        inDescLine = true;
        desc = "";
        descOffset = r.offset() - 1; // the position of the >
        if !taskFileRegion.contains(descOffset) {
          break; // don't read sequences starting outside of task's region
        }
        // store > characters to divide sequences
        if data.type != nothing && count < dataSize {
          agg.copy(data[dataStart + count], byte);
        }
        lastDescCount = count;
        count += 1;
      } else if byte == "\n".toByte() && inDescLine {
        inDescLine = false;
        if sequenceStarts.type != nothing {
          sequenceStarts.pushBack(dataStart + lastDescCount);
        }
        if sequenceDescs.type != nothing {
          sequenceDescs.pushBack(desc);
        }
        /*if TRACE {
          writeln("Reading sequence ", desc);
        }*/
      }
      if inDescLine {
        desc.appendCodepointValues(byte);
      } else if isspace(byte) == 0 {
        // store non-space sequence data
        if data.type != nothing && count < dataSize {
          extern proc toupper(c: c_int): c_int;
          byte = toupper(byte: c_int):uint(8);
          agg.copy(data[dataStart + count], byte);
        }
        count += 1;
      }
    } catch e: IO.EofError {
      break;
    }
  }

  return count;
}

/* Computes the size of the nucleotide data that will
   be read by readFastaFileSequence as well as the
   number of sequences.

   This is a single-locale operation.
   TODO: should it be distributed?

   Returns a tuple consisting of:
    * the number of bytes of FASTA sequence data
    * the number of FASTA sequences
 */
proc computeFastaFileSize(path: string) throws {
  // compute the file size without > lines or whitespace
  const size = IO.open(path, IO.ioMode.r).size;
  const Dom = {0..<size};
  const nTasksPerLocale = computeNumTasks();
  var totalCount = 0;
  var totalSequences = 0;

  // (this is not a distributed loop)
  forall (activeLocIdx, taskIdInLoc, chunk)
  in divideIntoTasks(Dom, 0..<size, nTasksPerLocale)
  with (+ reduce totalCount, + reduce totalSequences) {
    var noneVar = none;
    var mySequenceStarts: list(int);
    var c = readFastaSequencesStartingInRegion(path, chunk, 0..<size,
                                               noneVar, 1..0, noneVar,
                                               mySequenceStarts, noneVar);
    totalCount += c;
    totalSequences += mySequenceStarts.size;
  }

  if INCLUDE_REVERSE_COMPLEMENT {
    totalCount = 2*totalCount;
    totalSequences = 2*totalSequences;
  }

  /*writeln("computeFastaFileSize ", path,
          " INCLUDE_REVERSE_COMPLEMENT=", INCLUDE_REVERSE_COMPLEMENT,
          " totalCount=", totalCount);*/
  return (totalCount, totalSequences);
}

/* Reads a the sequence portion of a fasta file into a region of an array.
   The resulting array elements will contain a > at the start of each sequence
   followed by the nucleotide data. The whitespace and sequence
   descriptions are removed.
   The region size should match 'computeFastaFileSize'. */
proc readFastaFileSequence(path: string,
                           ref data: [] uint(8),
                           region: range,
                           param distributed: bool,
                           seqStartIdx: int, // start position in below arrays
                                             // for this file
                           param skipDescriptions: bool,
                           ref seqDescriptions: [] string,
                           // seqStarts has indices into data
                           ref seqStarts: [] int) throws
{
  const size = IO.open(path, IO.ioMode.r).size;
  //writeln("readFastaFileSequence ", path, " region=", region, " file size=", size);

  // file has spaces and descriptions, data does not, so should be smaller
  if INCLUDE_REVERSE_COMPLEMENT {
    // but with reverse complement it is doubled
    assert(region.size <= 2*size);
  } else {
    assert(region.size <= size);
  }

  const activeLocs = if distributed
                     then computeActiveLocales(data.domain, region)
                     else [here];
  const Dom = if distributed
              then makeBlockDomain(0..<size, activeLocs)
              else {0..<size};
  const nTasksPerLocale = computeNumTasks(ignoreRunning=distributed);
  const nTasks = activeLocs.size * nTasksPerLocale;

  var totalCount = 0;
  var totalSequences = 0;
  const CountsDom = if distributed
                    then makeBlockDomain(0..<nTasks, activeLocs)
                    else {0..<nTasks};
  var Counts:[CountsDom] int;
  var SeqCounts:[CountsDom] int;

  // divide the file size evenly among tasks
  // compute the data position where each task should start
  forall (activeLocIdx, taskIdInLoc, chunk)
  in divideIntoTasks(Dom, 0..<size, nTasksPerLocale, activeLocs)
  with (+ reduce totalCount, + reduce totalSequences) {
    const taskId = activeLocIdx*nTasksPerLocale + taskIdInLoc;
    var noneVar = none;
    var mySequenceStarts: list(int);
    var c = readFastaSequencesStartingInRegion(path, chunk, 0..<size,
                                               noneVar, 1..0, noneVar,
                                               mySequenceStarts, noneVar);
    Counts[taskId] = c;
    SeqCounts[taskId] = mySequenceStarts.size;
    totalCount += c;
    totalSequences += mySequenceStarts.size;
  }

  var checkCount = totalCount;
  if INCLUDE_REVERSE_COMPLEMENT {
    checkCount *= 2;
  }

  if region.size != checkCount {
    // region does not match the file
    throw new Error("count mismatch in readFastaFileSequence");
  }

  // Scan to get the end of each task's region
  const Ends = + scan Counts;
  const SeqEnds = + scan SeqCounts;

  // divide the file size evenly among tasks
  // read in the data for each task
  forall (activeLocIdx, taskIdInLoc, chunk)
  in divideIntoTasks(Dom, 0..<size, nTasksPerLocale, activeLocs)
  with (var agg = new DstAggregator(uint(8))) {
    const taskId = activeLocIdx*nTasksPerLocale + taskIdInLoc;
    const end = Ends[taskId];
    const count = Counts[taskId];
    const start = end - count;

    var dataStart = region.low + start;

    var mySequenceStarts: list(int);
    var mySequenceDescs: list(string);

    // now read in sequences in the task's region
    var c = readFastaSequencesStartingInRegion(path, chunk, 0..<size,
                                               data, dataStart..#count, agg,
                                               mySequenceStarts, mySequenceDescs);
    assert(c == Counts[taskId]);
    /*writeln("Got mySequenceStarts in ", dataStart..#count,
              " mySequenceStarts ", mySequenceStarts);*/

    const seqEnd = SeqEnds[taskId];
    const seqCount = SeqCounts[taskId];
    const seqStart = seqEnd - seqCount;
    assert(mySequenceStarts.size == mySequenceDescs.size);
    assert(mySequenceStarts.size == seqCount);

    // store into seqDescriptions / seqStarts
    for seqIdx in 0..<seqCount {
      const globalSeqIdx = seqStartIdx + seqStart + seqIdx;
      /*writeln("fwd seq ", globalSeqIdx, " starts ", mySequenceStarts[seqIdx] +
          region.low, " desc ", mySequenceDescs[seqIdx]);*/
      if !skipDescriptions {
        seqDescriptions[globalSeqIdx] = mySequenceDescs[seqIdx];
      }
      seqStarts[globalSeqIdx] = mySequenceStarts[seqIdx];
    }
  }

  if INCLUDE_REVERSE_COMPLEMENT && totalCount > 0 {
    var dataStart = region.low;
    // store the reverse complement just after the original sequence;
    // except the initial > would be a trailing >,
    // so emit a separator and don't revcomp the initial >
    const c = totalCount;
    data[dataStart + c] = ">".toByte();
    const cLessOne = c - 1; // don't revcomp the initial separator,
                            // because it would end up at the end
    reverseComplement(data, dataStart+1..#cLessOne,
                      data, dataStart+1+c..#cLessOne);

    // update seqDescriptions / seqStarts for the revcomps
    // from this file

    // first, compute the reverse sequence sizes, so we can scan
    var ReversedSeqSizes:[0..<totalSequences] int;
    forall thisFileSeqIdx in 0..<totalSequences {
      const globalIndex = seqStartIdx + thisFileSeqIdx;
      const nextStart = if thisFileSeqIdx+1 < totalSequences
                        then seqStarts[globalIndex+1]
                        else c + region.low;
      /*writeln("thisFileSeqIdx ", thisFileSeqIdx,
              " globalIndex ", globalIndex,
              " myStart ", seqStarts[globalIndex],
              " nextStart ", nextStart,
              " c ", c);*/
      const seqSize = nextStart - seqStarts[globalIndex];
      ReversedSeqSizes[totalSequences-1-thisFileSeqIdx] = seqSize;
    }

    var ReversedSeqStarts = + scan ReversedSeqSizes;
    /*writeln("ReversedSeqSizes ", ReversedSeqSizes);
    writeln("ReversedSeqStarts ", ReversedSeqStarts);*/

    forall thisFileSeqIdx in 0..<totalSequences {
      const revSeqEnd = ReversedSeqStarts[thisFileSeqIdx];
      const revSeqCount = ReversedSeqSizes[thisFileSeqIdx];
      const revSeqStart = revSeqEnd - revSeqCount;

      const globalRevIndex = seqStartIdx + totalSequences + thisFileSeqIdx;
      const globalFwdIndex = seqStartIdx + totalSequences - 1 - thisFileSeqIdx;
      /*writeln("globalRevIndex ", globalRevIndex, " globalFwdIndex ", globalFwdIndex,
              " region.low ", region.low,
              " c ", c,
              " revSeqStart ", revSeqStart,
              " dststart ", revSeqStart + c + region.low);*/
      if !skipDescriptions {
        var desc = seqDescriptions[globalFwdIndex] + " [revcomp]";
        seqDescriptions[globalRevIndex] = desc;
      }
      seqStarts[globalRevIndex] = revSeqStart + c + region.low;
    }
  }
}

/* similar to file.readAll but it can work in parallel distributed.

   * path is the path of the file to read
   * data is the destination array
   * region is the region in the 'data' array to read into,
     and its size should match the file size
   * distributed indicates if the I/O should be distributed
 */
proc parReadAll(path: string,
                ref data: [] uint(8),
                region: range,
                param distributed: bool = false) throws
{
  const size = IO.open(path, IO.ioMode.r).size;

  if region.size != size {
    // region does not match the file
    throw new Error("size mismatch in parReadAll");
  }

  const activeLocs = if distributed
                     then computeActiveLocales(data.domain, region)
                     else [here];
  const Dom = if distributed
              then makeBlockDomain(0..<size, activeLocs)
              else {0..<size};
  const nTasksPerLocale = computeNumTasks(ignoreRunning=distributed);
  const nTasks = activeLocs.size * nTasksPerLocale;
  const start = region.low;

  // read in parallel
  forall (activeLocIdx, taskIdInLoc, chunk)
  in divideIntoTasks(Dom, 0..<size, nTasksPerLocale, activeLocs) {
    var agg = new DstAggregator(uint(8));
    var r = IO.openReader(path, locking=false, region=chunk);
    for fileOffset in chunk {
      var byte: uint(8);
      r.readByte(byte);
      agg.copy(data[start+fileOffset], byte);
    }
  }
}

/* Computes the size of a file and the number of sequences in the file.
   Handles fasta files specially to compute the size of the nucleotide data only.
   returns a tuple (nBytes, nSequences)
 */
proc computeFileSize(path: string) throws {
  if isFastaFile(path) {
    return computeFastaFileSize(path);
  } else {
    return (getFileSize(path), 1); // non-fasta files always have 1 sequence
  }
}

/* Read all of the data in a file into a portion of an array. Handles fasta
   files specially to read only the nucleotide data.
   The region should match the file size. */
proc readFileData(path: string,
                  ref data: [] uint(8),
                  region: range,
                  seqStartIdx: int,
                  param skipDescriptions: bool,
                  ref seqDescriptions: [] string,
                  ref seqStarts: [] int,
                  verbose = true) throws
{
  if isFastaFile(path) {
    const activeLocs = computeActiveLocales(data.domain, region);
    if activeLocs.size > 1 {
      readFastaFileSequence(path, data, region, distributed=true,
                            seqStartIdx, skipDescriptions,
                            seqDescriptions, seqStarts);
    } else {
      readFastaFileSequence(path, data, region, distributed=false,
                            seqStartIdx, skipDescriptions,
                            seqDescriptions, seqStarts);
    }
  } else {
    const activeLocs = computeActiveLocales(data.domain, region);
    if activeLocs.size > 1 {
      parReadAll(path, data, region, distributed=true);
    } else {
      parReadAll(path, data, region, distributed=false);
    }
    // update the sequence starts. non-fasta files only have one sequence.
    if !skipDescriptions {
      seqDescriptions[seqStartIdx] = path;
    }
    seqStarts[seqStartIdx] = region.low;
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
   * a sorted array of paths
   * a corresponding array of offsets where each file starts,
     which, contains an extra entry for the total size
   * an array of sequences descriptions
   * a corresponding array of sequence start positions
     within allData

 The resulting arrays will be Block distributed among 'locales'.
 */
proc readAllFiles(const ref files: list(string),
                  locales: [ ] locale,
                  out allData: [] uint(8),
                  out allPaths: [] string,
                  out concisePaths: [] string,
                  out fileStarts: [] int,
                  out totalSize: int,
                  out sequenceDescriptions: [] string,
                  out sequenceStarts: [] int,
                  param skipDescriptions=true) throws {
  if TRACE {
    writeln("in readAllFiles, reading ", files.size, " files");
  }

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

  if TRACE {
    writeln("in readAllFiles, computing file sizes");
  }

  // compute the size for the concatenated input
  var sizes: [paths.domain] int;
  var sequencesPerFile: [paths.domain] int;
  forall (path, sz, nSq) in zip(paths, sizes, sequencesPerFile) {
    var (nBytes, nSequences) = computeFileSize(path);
    sz = nBytes + 1; // add a null byte to separate the files
    assert(nBytes >= 0);
    assert(nSequences > 0);
    nSq = nSequences;
  }

  const sequencesPerFileEnds = + scan sequencesPerFile;
  const nSequences = sequencesPerFileEnds.last;
  const fileEnds = + scan sizes;
  const total = fileEnds.last;

  const TextDom = makeBlockDomain(0..<total+INPUT_PADDING, locales);
  var thetext:[TextDom] uint(8);

  var theSequenceDescriptions =
    if skipDescriptions
    then makeBlockArray(0..<locales.size, locales, string) // dummy array
    else makeBlockArray(0..<nSequences, locales, string);

  // and one that includes an extra element at the end
  const SequencesDomInclusive = makeBlockDomain(0..nSequences, locales);
  var theSequenceStarts: [SequencesDomInclusive] int;

  if TRACE {
    writeln("in readAllFiles, reading file contents");
  }

  // read each file
  forall (path, sz, end, fileSqCount, fileSqEnd)
  in zip(paths, sizes, fileEnds, sequencesPerFile, sequencesPerFileEnds)
  with (ref theSequenceDescriptions) {
    const start = end - sz;
    const count = sz - 1; // we added a null byte above
    const sequenceStartIdx = fileSqEnd - fileSqCount;
    readFileData(path, thetext, start..#count,
                 sequenceStartIdx,
                 skipDescriptions, theSequenceDescriptions, theSequenceStarts);
  }
  theSequenceStarts[nSequences] = total;

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
  fileStarts = starts;
  totalSize = total;

  sequenceDescriptions = theSequenceDescriptions;
  sequenceStarts = theSequenceStarts;

  if TRACE {
    writeln("readAllFiles complete");
  }
}

/* Write a Block-distributed array to a file */
proc writeBlockArray(const in path: string, const A: [], region: range) throws {
  if A.domain.rank != 1 {
    compilerError("writeBlockArray only supports 1-D");
  }

  // create the file and write a single byte to get it to the right size
  {
    var f = IO.open(path, IO.ioMode.cw);
    var w = f.writer(region=region.last..region.last);
    w.writeByte(0);
    w.close();
    f.fsync();
    f.close();
  }

  const activeLocs = computeActiveLocales(A.domain, region);
  const nTasksPerLocale = computeNumTasks(ignoreRunning=activeLocs.size>1);

  forall (activeLocIdx, taskIdInLoc, chunk)
  in divideIntoTasks(A.domain, region, nTasksPerLocale, activeLocs) {
    var f = IO.open(path, IO.ioMode.rw);
    var w = f.writer(region=chunk);
    w.writeBinary(A.localSlice(chunk));
    w.close();
    f.close();
  }
}


/* Given a Block-distributed array, return a local copy of that array.
 */
proc localCopyOfBlockArr(const A: []) {
  if A.domain.rank != 1 {
    compilerError("localCopyOfBlock only supports 1-D");
  }
  const region = A.domain.dim(0);
  var LocA:[region] A.eltType;
  bulkCopy(LocA, region, A, region);
  return LocA;
}

/* Compute a SHA-256 checksum for every file, storing the
   checksums in allChecksums */
proc hashAllFiles(const allData: [] uint(8),
                  const fileStarts: [] int,
                  const totalSize: int,
                  out allChecksums: [fileStarts.domain] 8*uint(32)) {

  const activeLocs = allData.targetLocales();
  const nTasksPerLocale = computeNumTasks(ignoreRunning=true);
  const nTasks = activeLocs.size * nTasksPerLocale;
  const nFiles = fileStarts.size;

  // each task considers files starting within its block
  forall (activeLocIdx, taskIdInLoc, chunk)
  in divideIntoTasks(allData.domain, 0..<totalSize, nTasksPerLocale, activeLocs)
  with (const locFileStarts = localCopyOfBlockArr(fileStarts),
        var agg = new DstAggregator(8*uint(32))) {
    // compute the first file starting within the chunk
    var firstFile: int;
    var firstFileStart: int;
    {
      var cur = chunk.low;
      while cur <= chunk.high {
        firstFile = offsetToFileIdx(locFileStarts, cur);
        firstFileStart = locFileStarts[firstFile];
        if chunk.contains(firstFileStart) then break;
        // move on to the next file
        var firstFileEnd = if locFileStarts.domain.contains(firstFile+1)
                           then locFileStarts[firstFile+1]
                           else totalSize;
        cur = firstFileEnd;
      }
    }
    // loop over file starts while the file start is in the chunk
    var curFile = firstFile;
    var curFileStart = firstFileStart;
    while chunk.contains(curFileStart) && curFile < nFiles {
      // compute the file region for curFile
      var curFileEnd = if locFileStarts.domain.contains(curFile+1)
                       then locFileStarts[curFile+1]
                       else totalSize;
      var curFileHashEnd = curFileEnd - 1; // do not count the trailing 0 byte
      if allData[curFileHashEnd] != 0 {
        // workaround to enable this function to work on sequences of FASTA
        // data which never end with a null byte
        curFileHashEnd += 1;
      }

      // process that file
      var cur = curFileStart;
      var s: SHA256State;

      // process the full blocks
      while cur + 16*4 < curFileHashEnd {
        // store the full block
        var block: 16*uint(32);
        for param i in 0..<16 {
          var x:uint(32);
          // read 4 bytes into x
          for param j in 0..<4 {
            x <<= 8;
            x |= allData[cur + i*4 + j];
          }
          // store it into the block
          block[i] = x;
        }
        s.fullblock(block);
        cur += 16*4;
      }

      // process the final block
      {
        var nBitsInBlock = 0;
        var block: 16*uint(32);
        for i in 0..<16 {
          var x:uint(32);
          for j in 0..<4 {
            x <<= 8;
            if cur < curFileHashEnd {
              x |= allData[cur];
              nBitsInBlock += 8;
            }
            cur += 1;
          }
          // store it into the block
          block[i] = x;
        }
        const chksum = s.lastblock(block, nBitsInBlock);
        agg.copy(allChecksums[curFile], chksum);
      }

      // move on to the next file
      curFile += 1;
      curFileStart = curFileEnd;
    }
  }
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

  // now set the value of each character
  {
    const tmp = + scan alphaMap;
    alphaMap = tmp - 1;
  }

  newMaxChar = max(1, nUniqueChars-1);

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


/*
   Help with timing regions of code within a parallel region.
   To use this type:
    * start timing with startTime (which returns this type)
    * stop timing with stopTime
    * accumulate with + reduce in parallel regions
    * report it with reportTime
 */
record subtimer : writeSerializable {
  // skip computations / timer start/stop if disabled
  param enabled: bool;

  var timer: Time.stopwatch;
  var running: bool = false;

  // the below represent data from combining times
  var count: int; // aka how many tasks summarized here
  var totalTime: real;
  var minTime: real;
  var maxTime: real;
};
proc ref subtimer.start() {
  if enabled {
    running = true;
    timer.reset();
    timer.start();
  }
}
proc ref subtimer.stop() {
  if enabled && running {
    timer.stop();
    running = false;

    const t = timer.elapsed();
    if EXTRA_CHECKS {
      assert(!running);
      assert(count == 0 || count == 1);
    }
    if count == 0 {
      count = 1;
      totalTime = t;
      minTime = t;
      maxTime = t;
    } else {
      count = 1;
      totalTime += t;
      minTime += t;
      maxTime += t;
    }
  }
}

// add times within a task
proc ref subtimer.accumulate(ref x: subtimer(?)) {
  // accumulate the timing within a single task
  // (vs + which adds across tasks)
  if enabled {
    x.stop();
    if EXTRA_CHECKS && x.enabled {
      assert(!x.running);
      assert(x.count == 0 || x.count == 1);
      assert(!running);
      assert(count == 0 || count == 1);
    }
    if x.enabled && x.count == 1 {
      count = 1;
      totalTime += x.totalTime;
      minTime += x.minTime;
      maxTime += x.maxTime;
    }
  }
}

// add times from different tasks (for + reduce)
operator subtimer.+(x: subtimer(?), y: subtimer(?)) {
  var ret: subtimer(enabled=(x.enabled || y.enabled));
  if ret.enabled {
    if x.count == 0 && y.count == 0 {
      // leave ret default initialized
    } else if y.count == 0 {
      // use only x
      ret = x;
    } else if x.count == 0 {
      // use only y
      ret = y;
    } else {
      // add them
      ret.count = x.count + y.count;
      ret.totalTime = x.totalTime + y.totalTime;
      ret.minTime = min(x.minTime, y.minTime);
      ret.maxTime = max(x.maxTime, y.maxTime);
    }
  }
  return ret;
}

proc subtimer.serialize(writer, ref serializer) throws {
  writer.write("(count=", count, " totalTime=", totalTime,
               " minTime=", minTime, " maxTime=", maxTime, ")");
}

/* start timing if TIMING, returning something to be used by reportTime */
proc startTime(param doTiming=TIMING) {
  if doTiming {
    var ret: subtimer(enabled=true);
    ret.start();
    return ret;
  } else {
    var ret: subtimer(enabled=false);
    return ret;
  }
}

/* report time started by startTime */
proc reportTime(ref x:subtimer(?), desc:string, n: int = 0, bytesPer: int = 0) {
  if x.enabled {
    x.stop();
    const avgTime = x.totalTime / x.count;
    if x.count <= 1 {
      // in that case, avgTime == minTime == maxTime
      if n == 0 {
        writeln(desc ," in ", avgTime, " s");
      } else if bytesPer == 0 {
        writeln(desc ," in ", avgTime, " s for ",
                n/avgTime/1000.0/1000.0, " M elements/s");
      } else {
        writeln(desc ," in ", avgTime, " s for ",
                n/avgTime/1000.0/1000.0, " M elements/s and ",
                bytesPer*n/avgTime/1024.0/1024.0, " MiB/s");
      }
    } else {
      writeln(desc, " in avg ", avgTime,
                    " min ", x.minTime,
                    " max ", x.maxTime, " s");
    }
  }
}

/* Similar to subtimer; counts something per-task and summarizes
   the min/max/average number per task */
record substat : writeSerializable {
  // skip computations / timer start/stop if disabled
  param enabled: bool;
  type statType;

  // the below represent data from combining times
  var count: int; // aka how many tasks summarized here
  var total: statType;
  var min_: statType;
  var max_: statType;
};

// add stats within a task
proc ref substat.accumulate(v: statType) {
  // accumulate the timing within a single task
  // (vs + which adds across tasks)
  if enabled {
    if EXTRA_CHECKS {
      assert(count == 0 || count == 1);
    }
    count = 1;
    total += v;
    min_ += v;
    max_ += v;
  }
}

// add stats from different tasks (for + reduce)
operator substat.+(x: substat(?), y: substat(?))
where x.statType == y.statType {
  var ret: substat(enabled=(x.enabled || y.enabled), x.statType);
  if ret.enabled {
    if x.count == 0 && y.count == 0 {
      // leave ret default initialized
    } else if y.count == 0 {
      // use only x
      ret = x;
    } else if x.count == 0 {
      // use only y
      ret = y;
    } else {
      // add them
      ret.count = x.count + y.count;
      ret.total = x.total + y.total;
      ret.min_ = min(x.min_, y.min_);
      ret.max_ = max(x.max_, y.max_);
    }
  }
  return ret;
}

proc substat.serialize(writer, ref serializer) throws {
  writer.write("(count=", count, " total=", total,
               " min=", min_, " max=", max_, ")");
}

proc reportStat(const ref x:substat(?), desc:string) {
  if x.enabled {
    const avg = x.total: real / x.count;
    if x.count <= 1 {
      // in that case, avgTime == minTime == maxTime
      writeln(desc ," : ", avg);
    } else {
      writeln(desc, " : avg ", avg,
                    " min ", x.min_,
                    " max ", x.max_);
    }
  }
}

record yieldHelper {
  var itersSinceLastYield: int;
}
proc ref yieldHelper.maybeYield(iters: int = 1) {
  itersSinceLastYield += iters;
  if itersSinceLastYield >= YIELD_PERIOD {
    itersSinceLastYield = 0;
    currentTask.yieldExecution();
  }
}

}
