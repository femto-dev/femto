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

  femto/src/ssort_chpl/CachedAggregators.chpl
*/
module CachedAggregators {


use CopyAggregation;
use AggregationPrimitives;
use ChplConfig;
use CTypes;
use OS.POSIX;
use Set;

private param defaultBuffSize =
  if CHPL_TARGET_PLATFORM == "hpe-cray-ex" then 1024
  else if CHPL_COMM == "ugni" then 4096
  else 8192;

private const yieldFrequency = getEnvInt("CHPL_AGGREGATION_YIELD_FREQUENCY", 1024);
private const dstBuffSize = getEnvInt("CHPL_AGGREGATION_DST_BUFF_SIZE", defaultBuffSize);
private const srcBuffSize = getEnvInt("CHPL_AGGREGATION_SRC_BUFF_SIZE", defaultBuffSize);

private config param aggregate = CHPL_COMM != "none";


// it just has the same interface as DstAggregator but doesn't aggregate
record DummyDstAggregator {
  inline proc ref copy(ref dst: ?t, const in srcVal: t) {
    dst = srcVal;
  }
  inline proc ref flush(freeBuffers=false) { }
}
/* "Aggregator" that uses unordered copy instead of actually aggregating */
record DstUnorderedAggregator {
  type elemType;

  proc deinit() {
    flush();
  }
  proc ref flush(freeBuffers=false) {
    if isPOD(elemType) then unorderedCopyTaskFence();
  }
  inline proc ref copy(ref dst: elemType, const in srcVal: elemType) {
    if isPOD(elemType) then unorderedCopy(dst, srcVal);
                       else dst = srcVal;
  }
}
// it just has the same interface as SrcAggregator but doesn't aggregate
record DummySrcAggregator {
  inline proc ref copy(ref dst: ?t, const ref src: t) {
    if boundsChecking {
      assert(dst.locale.id == here.id);
    }
    dst = src;
  }
  inline proc ref flush(freeBuffers=false) { }
}
/* "Aggregator" that uses unordered copy instead of actually aggregating */
record SrcUnorderedAggregator {
  type elemType;

  proc deinit() {
    flush();
  }
  proc ref flush(freeBuffers=false) {
    if isPOD(elemType) then unorderedCopyTaskFence();
  }
  inline proc ref copy(ref dst: elemType, const ref src: elemType) {
    if boundsChecking {
      assert(dst.locale.id == here.id);
    }
    if isPOD(elemType) then unorderedCopy(dst, src);
                       else dst = src;
  }
}

record CachedDstAggregator {
  var agg: borrowed CachedDstAggregatorClass;
  proc init() {
    var cls = PThreadSupport.getPerPthreadClass(CachedDstAggregatorClass);
    assert(cls != nil);
    this.agg = cls;
  }
  proc ref deinit() {
    flush();
  }
  inline proc ref copy(ref dst: ?t, const ref src: t) {
    this.agg.copy(dst, src);
  }
  inline proc ref flush() {
    this.agg.flush(freeBuffers=false);
  }
}

// create and destroy the per-pthread CachedDstAggregatorClass
var perPthreadAggregators: set(eltType=unmanaged CachedDstAggregatorClass,
                               parSafe=true);
writeln("creating aggregators");
coforall tid in 0..<4*here.maxTaskPar with (ref perPthreadAggregators) {
  var cls = PThreadSupport.getPerPthreadClass(CachedDstAggregatorClass);
  perPthreadAggregators.add(cls:unmanaged);
}
writeln("done creating aggregators");
proc deinit() {
  writeln("destroying aggregators");
  for elt in perPthreadAggregators {
    writeln("destroying ", c_ptrTo(elt));
    delete elt;
  }
}


/* Aggregator that can be shared, works with any type */
class CachedDstAggregatorClass {
  // room for dstBuffSize ints, each with a dst ptr and size
  // (larger items will occupy more of the buffer)
  const bufferSize = (dstBuffSize * (3*c_sizeof(c_ptr(void)))):int;
  const myLocaleSpace = 0..<numLocales;
  var lastLocale: int;
  var opsUntilYield = yieldFrequency;
  var lBuffers: c_ptr(c_ptr(uint(8)));
  var rBuffers: [myLocaleSpace] remoteBuffer(uint(8));
  var bufferIdxs: c_ptr(int);
  var currentlyFlushing = false;

  proc init() {
    init this;
    writeln("Creating CachedDstAggregatorClass ", c_ptrTo(this));
  }

  proc postinit() {
    lBuffers = allocate(c_ptr(uint(8)), numLocales);
    bufferIdxs = bufferIdxAlloc();
    for loc in myLocaleSpace {
      lBuffers[loc] = allocate(uint(8), bufferSize);
      bufferIdxs[loc] = 0;
      rBuffers[loc] = new remoteBuffer(uint(8), bufferSize, loc);
    }
  }

  proc deinit() {
    writeln("Destroying CachedDstAggregatorClass ", c_ptrTo(this));
    flush(freeBuffers=true);
    for loc in myLocaleSpace {
      deallocate(lBuffers[loc]);
    }
    deallocate(lBuffers);
    deallocate(bufferIdxs);
  }

  proc flush(freeBuffers=false) {
    // TODO: try randomized flush
    for offsetLoc in myLocaleSpace + lastLocale {
      const loc = offsetLoc % numLocales;
      flushBuffer(loc, bufferIdxs[loc], freeData=freeBuffers);
    }
  }

  inline proc copy(ref dst:?t, const ref src:t) {
    if boundsChecking {
      assert(src.locale.id == here.id);
    }

    const loc = dst.locale.id;
    const addr_size = c_sizeof(c_ptr(void));
    const size_size = c_sizeof(addr_size.type);
    const data_size = c_sizeof(t);
    const serialize_bytes = addr_size + size_size + data_size;

    // Just do direct assignment if dst is local or src size is large
    if loc == here.id || serialize_bytes > (bufferSize >> 2) {
      dst = src;
      return;
    }

    lastLocale = loc;

    var dstAddr = getAddr(dst);

    // Get our current index into the buffer for dst's locale
    ref bufferIdx = bufferIdxs[loc];

    // Flush our buffer if this entry will exceed capacity
    if bufferIdx + serialize_bytes > bufferSize {
      // note: it can yield inside flushBuffer.
      flushBuffer(loc, bufferIdx, freeData=false);
      opsUntilYield = yieldFrequency;
    }

    const buf = lBuffers[loc];

    // Buffer the address
    memcpy(c_ptrTo(buf[bufferIdx]),
           c_ptrTo(dstAddr),
           addr_size);
    bufferIdx += addr_size:int;
    // Buffer the size
    memcpy(c_ptrTo(buf[bufferIdx]),
           c_ptrToConst(data_size):c_ptr(data_size.type),
           size_size);
    bufferIdx += size_size:int;
    // Buffer the data
    memcpy(c_ptrTo(buf[bufferIdx]),
           c_ptrToConst(src):c_ptr(src.type),
           data_size);
    bufferIdx += data_size:int;

    // If it's been a while since we've let other tasks run, yield so that
    // we're not blocking remote tasks from flushing their buffers.
    if opsUntilYield == 0 {
      currentTask.yieldExecution();
      opsUntilYield = yieldFrequency;
    } else {
      opsUntilYield -= 1;
    }
  }

  proc flushBuffer(loc: int, ref bufferIdx, freeData) {
    // return early if there's no data to flush
    if bufferIdx == 0 then return;

    // wait for some other task if it's currently doing a flush
    while currentlyFlushing {
      currentTask.yieldExecution();
    }

    // return early again if there's no data to flush
    if bufferIdx == 0 then return;

    // now we're the one running and currentlyFlushing was false,
    // so it's up to us to flush! But mark that we're flushing
    // so that if we yield, another task won't also flush.
    currentlyFlushing = true;
    // clear currentlyFlushing on exit from this block
    defer { currentlyFlushing = false; }

    const myBufferIdx = bufferIdx;

    // Allocate a remote buffer
    ref rBuffer = rBuffers[loc];
    const remBufferPtr = rBuffer.cachedAlloc();

    // Copy local buffer to remote buffer
    rBuffer.PUT(lBuffers[loc], myBufferIdx);

    // Process remote buffer
    on Locales[loc] {
      var curBufferIdx = 0;
      while curBufferIdx < myBufferIdx {
        const addr_size = c_sizeof(c_ptr(void));
        const size_size = c_sizeof(addr_size.type);
        var dstAddr: c_ptr(void);
        var dataSize: addr_size.type;

        // Copy the addr out
        memcpy(c_ptrTo(dstAddr),
               c_ptrTo(remBufferPtr[curBufferIdx]),
               addr_size);
        curBufferIdx += addr_size:int;
        // Copy the size out
        memcpy(c_ptrTo(dataSize),
               c_ptrTo(remBufferPtr[curBufferIdx]),
               size_size);
        curBufferIdx += size_size:int;
        // Copy the data out
        memcpy(dstAddr, c_ptrTo(remBufferPtr[curBufferIdx]), dataSize);
        curBufferIdx += dataSize:int;
      }

      if freeData {
        rBuffer.localFree(remBufferPtr);
      }
    }
    if freeData {
      rBuffer.markFreed();
    }
    bufferIdx = 0;
  }
}


module PThreadSupport {
  use CTypes;
  use OS;
  use OS.POSIX;

  extern type pthread_key_t;
  extern proc pthread_key_create(ref key: pthread_key_t,
                                 destr_function: c_fn_ptr): c_int;
  extern proc pthread_setspecific(key: pthread_key_t, ptr: c_ptr(void)): c_int;
  extern proc pthread_getspecific(key: pthread_key_t): c_ptr(void);

  export proc pthread_key_deleter(ptr: c_ptr(void)): void {
    // Problem: the pthread will be destroyed on program exit after
    // the runtime doesn't exist anymore
    /*var obj = ptr:unmanaged RootClass?;
    if obj != nil {
      delete obj;
    }*/
  }

  proc keyCreate() : pthread_key_t throws {
    var key: pthread_key_t;
    var rc: c_int = pthread_key_create(key, c_ptrTo(pthread_key_deleter));
    if rc != 0 {
      throw createSystemError(rc, "error in pthread_key_create");
    }

    return key;
  }

  proc setSpecific(key: pthread_key_t, obj: c_ptr(void)) : void throws {
    var rc: c_int = pthread_setspecific(key, obj);
    if rc != 0 {
      throw createSystemError(rc, "error in pthread_setspecific");
    }
  }
  proc getSpecific(key: pthread_key_t) : c_ptr(void) {
    var ptr: c_ptr(void) = pthread_getspecific(key);
    return ptr;
  }

  proc getPerPthreadClass(type t) where isClassType(t) {
    @functionStatic(sharingKind.computePerLocale)
    var key = try! keyCreate();

    var ptr : c_ptr(void) = getSpecific(key);
    if ptr == nil {
      var obj = new unmanaged t();

      ptr = c_ptrTo(obj);
      try! setSpecific(key, ptr);
      assert(ptr != nil);
    }
    var cls = try! ptr:borrowed t?:borrowed t;
    return cls;
  }
}

/*
pthread_key_create
class Obj;

void* pthread_getspecific(pthread_key_t key)
  pthread_setspecific

// help to implement code using Aggregators sometimes and sometimes not.
// IMO it would be ideal to have a single per-thread aggregator associated
// with the comms cache that allows different data types. Such an aggregator
// would only need to be constructed once (well, maybe once for GET and once
// for PUT) but has additional coordination challenges.
//
// In the meantime, we have aggregators for different types, and if these
// are created ahead of time, we can reuse them.
//
// So, MaybeDstAggregator / MaybeSrcAggregator have two features
// beyond DstAggregator / SrcAggregator:
//
// 1. They can be disabled in a per-instance way.
//    When disabled, `copy` just calls `=`. This allows calling code to avoid
//    lots of conditionals.
// 2. They provide a mechanism for checking that the current pthread
//    being used matches the pthread that created the aggregator.
//    This is only used with EXTRA_CHECKS enabled.

/*extern type pthread_t;
extern proc pthread_self():pthread_t;
extern proc pthread_equal(a: pthread_t, b:pthread_t):c_int;

record MaybeDstAggregator {
  type elemType;
  param active: bool;        // if not, there is no aggregator
  var pthread_id: pthread_t; // pthread owning this aggregator
  var agg: if active then DstAggregator(elemType) else nothing;
  proc init(type elemType, param active: bool) {
    this.elemType = elemType;
    this.active = active;
    this.pthread_id = pthread_self();
    if active {
      this.agg = new DstAggregator(elemType);
    } else {
      this.agg = none;
    }
  }
  inline proc ref copy(ref dst: elemType, const in srcVal: elemType) {
    if active {
      if EXTRA_CHECKS { assert(pthread_equal(pthread_id, pthread_self()) > 0); }
      if EXTRA_CHECKS { assert(src.locale.id == here.id); }
      agg.copy(dst, srcVal);
    } else {
      dst = srcVal;
    }
  }
  inline proc ref flush(freeBuffers=false) {
    if active {
      if EXTRA_CHECKS { assert(pthread_equal(pthread_id, pthread_self()) > 0); }
      agg.flush(freeBuffers=freeBuffers);
    }
  }
}

record MaybeSrcAggregator {
  type elemType;
  param active: bool;        // if not, there is no aggregator
  var pthread_id: pthread_t; // pthread owning this aggregator
  var agg: if active then SrcAggregator(elemType) else nothing;
  proc init(type elemType, param active: bool) {
    this.elemType = elemType;
    this.active = active;
    this.pthread_id = pthread_self();
    if active {
      this.agg = new SrcAggregator(elemType);
    } else {
      this.agg = none;
    }
  }
  inline proc ref copy(ref dst: elemType, const ref src: elemType) {
    if active {
      if EXTRA_CHECKS { assert(pthread_equal(pthread_id, pthread_self()) > 0); }
      if EXTRA_CHECKS { assert(dst.locale.id == here.id); }
      agg.copy(dst, src);
    } else {
      dst = src;
    }
  }
  inline proc ref flush(freeBuffers=false) {
    if active {
      if EXTRA_CHECKS { assert(pthread_equal(pthread_id, pthread_self()) > 0); }
      agg.flush(freeBuffers=freeBuffers);
    }
  }
}
*/

class SharedDstAggregator {
  type elemType;
  var agg: DstAggregator(elemType);
  var inUse: bool;  // is it currently being used
  //var inUseByPthread: pthread_t; // current pthread using it
}

record DstAggregatorGroup {
  type elemType;
  const nTasksPerLocale: int;
  const Dom = blockDist.createDomain(0..<numLocales*nTasksPerLocale);
  var PerTaskAggs: [Dom] unmanaged SharedDstAggregator?;
  proc init(type elemType, nTasksPerLocale: int) {
    this.elemType = elemType;
    this.nTasksPerLocale = nTasksPerLocale;
    init this;
    forall (locIdx, taskIdInLoc, chunk)
    in divideIntoTasks(Dom, Dom.dim(0), nTasksPerLocale) {
      assert(chunk.size == 1);
      const idx = chunk.first;
      PerTaskAggs[idx] = new unmanaged SharedDstAggregator(elemType);
    }
  }
  proc deinit() {
    forall (locIdx, taskIdInLoc, chunk)
    in divideIntoTasks(Dom, Dom.dim(0), nTasksPerLocale) {
      assert(chunk.size == 1);
      const idx = chunk.first;
      delete PerTaskAggs[idx];
    }
  }
}

record FlushingDstAggregator {
  type elemType;
  var sharedAgg: borrowed SharedDstAggregator(elemType);
  proc init(sharedAgg: borrowed SharedDstAggregator(?)) {
    this.elemType = sharedAgg.elemType;
    this.sharedAgg = sharedAgg;
    assert(sharedAgg.inUse == false);
    sharedAgg.inUse = true;
  }
  proc ref agg ref : DstAggregator(elemType) {
    return sharedAgg.agg;
  }
  proc deinit() {
    sharedAgg.agg.flush(freeBuffers=false);
    sharedAgg.inUse = false;
  }
}

proc DstAggregatorGroup.getAggregator(locIdx, taskIdInLoc) {
  var aggCls = PerTaskAggs[locIdx*nTasksPerLocale+taskIdInLoc]!;
  return new FlushingDstAggregator(elemType, aggCls);
}


class SharedSrcAggregator {
  type elemType;
  var agg: SrcAggregator(elemType);
  var inUse: bool;  // is it currently being used
  //var inUseByPthread: pthread_t; // current pthread using it
}

record SrcAggregatorGroup {
  type elemType;
  const nTasksPerLocale: int;
  const Dom = blockDist.createDomain(0..<numLocales*nTasksPerLocale);
  var PerTaskAggs: [Dom] unmanaged SharedSrcAggregator?;
  proc init(type elemType, nTasksPerLocale: int) {
    this.elemType = elemType;
    this.nTasksPerLocale = nTasksPerLocale;
    init this;
    forall (locIdx, taskIdInLoc, chunk)
    in divideIntoTasks(Dom, Dom.dim(0), nTasksPerLocale) {
      assert(chunk.size == 1);
      const idx = chunk.first;
      PerTaskAggs[idx] = new unmanaged SharedSrcAggregator(elemType);
    }
  }
  proc deinit() {
    forall (locIdx, taskIdInLoc, chunk)
    in divideIntoTasks(Dom, Dom.dim(0), nTasksPerLocale) {
      assert(chunk.size == 1);
      const idx = chunk.first;
      delete PerTaskAggs[idx];
    }
  }
}

record FlushingSrcAggregator {
  type elemType;
  var sharedAgg: borrowed SharedSrcAggregator(elemType);
  proc init(sharedAgg: borrowed SharedSrcAggregator(?)) {
    this.elemType = sharedAgg.elemType;
    this.sharedAgg = sharedAgg;
    assert(sharedAgg.inUse == false);
    sharedAgg.inUse = true;
  }
  proc ref agg ref : SrcAggregator(elemType) {
    return sharedAgg.agg;
  }
  proc deinit() {
    sharedAgg.agg.flush(freeBuffers=false);
    sharedAgg.inUse = false;
  }

  proc ref agg {
    return sharedAgg.agg;
  }
  proc deinit() {
    sharedAgg.agg.flush(freeBuffers=false);
  }
}

proc SrcAggregatorGroup.getAggregator(locIdx, taskIdInLoc) {
  var aggCls = PerTaskAggs[locIdx*nTasksPerLocale+taskIdInLoc]!;
  return new FlushingSrcAggregator(elemType, aggCls);
}
*/

}
