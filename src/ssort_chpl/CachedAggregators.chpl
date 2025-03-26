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
use BlockDist;
use Math;

private param TRACE_INIT_DEINIT = false;

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
    if aggregate && isPOD(elemType) {
      unorderedCopyTaskFence();
    }
  }
  inline proc ref copy(ref dst: elemType, const in srcVal: elemType) {
    if aggregate && isPOD(elemType) {
      unorderedCopy(dst, srcVal);
    } else {
      dst = srcVal;
    }
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
    if aggregate && isPOD(elemType) {
      unorderedCopyTaskFence();
    }
  }
  inline proc ref copy(ref dst: elemType, const ref src: elemType) {
    if boundsChecking {
      assert(dst.locale.id == here.id);
    }
    if aggregate && isPOD(elemType) {
      unorderedCopy(dst, src);
    } else {
      dst = src;
    }
  }
}

record CachedDstAggregator {
  type elemType;
  var agg: borrowed CachedDstAggregatorClass?;
  proc init(type elemType) {
    var cls: borrowed CachedDstAggregatorClass? = nil;
    if aggregate {
      cls = PThreadSupport.getPerPthreadClass(CachedDstAggregatorClass);
      assert(cls != nil);
    }
    this.elemType = elemType;
    this.agg = cls;
  }
  proc ref deinit() {
    // flush but don't attempt to free anything since
    // the aggregator is per-pthread
    flush();
  }
  inline proc ref copy(ref dst: elemType, const ref src: elemType) {
    if aggregate {
      this.agg!.copy(dst, src);
    } else {
      dst = src;
    }
  }
  inline proc ref flush() {
    if aggregate {
      this.agg!.flush(freeBuffers=false);
    }
  }
}

record CachedSrcAggregator {
  type elemType;
  var agg: borrowed CachedSrcAggregatorClass?;
  proc init(type elemType) {
    var cls: borrowed CachedSrcAggregatorClass? = nil;
    if aggregate {
      cls = PThreadSupport.getPerPthreadClass(CachedSrcAggregatorClass);
      assert(cls != nil);
    }
    this.elemType = elemType;
    this.agg = cls;
  }
  proc ref deinit() {
    // flush but don't attempt to free anything since
    // the aggregator is per-pthread
    flush();
  }
  inline proc ref copy(ref dst: elemType, const ref src: elemType) {
    if aggregate {
      this.agg!.copy(dst, src);
    } else {
      dst = src;
    }
  }
  inline proc ref flush() {
    if aggregate {
      this.agg!.flush(freeBuffers=false);
    }
  }
}


// create and destroy the per-pthread CachedDstAggregatorClass
type PerPthreadAggsEltType = set(eltType=unmanaged RootClass,
                                 parSafe=true);
private var PerPthreadAggs = blockDist.createArray(0..<numLocales,
                                                   PerPthreadAggsEltType);
if aggregate {
  const testTasksPerLoc = 4*here.maxTaskPar;
  const testN = numLocales*testTasksPerLoc;
  var TestArr1 = blockDist.createArray(0..<testN, int);
  TestArr1 = 0..<testN by -1;
  var TestArr2 = blockDist.createArray(0..<testN, int);
  TestArr2 = 0..<testN by -1;
  var TestIdxs = blockDist.createArray(0..<testN, int);
  TestIdxs = 0..<testN by -1;
  if TRACE_INIT_DEINIT then writeln("creating aggregators");
  coforall (loc, locId) in zip(Locales, 0..) {
    on loc {
      coforall tid in 0..<4*here.maxTaskPar {
        var dsta = PThreadSupport.getPerPthreadClass(CachedDstAggregatorClass);

        if TRACE_INIT_DEINIT {
          extern proc gettid(): c_int;
          extern proc pthread_self(): c_ptr(void);

          writeln("got CachedDstAggregatorClass ",
                  c_ptrTo(dsta),
                  " on thread id ", gettid(), " pthread ", pthread_self());
        }

        PerPthreadAggs[locId].add(dsta:unmanaged);

        var srca = PThreadSupport.getPerPthreadClass(CachedSrcAggregatorClass);
        PerPthreadAggs[locId].add(srca:unmanaged);
      }
    }
  }

  // get the DstAggregators going (allocate remote buffers)
  forall idx in TestArr1.domain with (var agg = new CachedDstAggregator(int)) {
    agg.copy(TestArr1[idx], 0);
  }
  // get the SrcAggregators going (allocate remote buffers)
  forall idx in TestArr2.domain with (var agg = new CachedSrcAggregator(int)) {
    agg.copy(TestArr2[idx], TestArr1[TestIdxs[idx]]);
  }

  if TRACE_INIT_DEINIT then writeln("done creating aggregators");
}

proc deinit() {
  if aggregate {
    if TRACE_INIT_DEINIT then writeln("destroying aggregators");
    coforall (loc, locId) in zip(Locales, 0..) {
      on loc {
        for elt in PerPthreadAggs[locId] {
          if TRACE_INIT_DEINIT then writeln("destroying ", c_ptrTo(elt));
          delete elt;
        }
      }
    }
  }
}

/* CachedDstAggregatorClass / CachedSrcAggregatorClass support
   many tasks working with them within the same thread.
   This helper type is a like a "lock" for many tasks within the same
   thread. It should by no means be used for tasks running on different
   threads.
 */
record perTaskLock {
  // use the task bundle pointer so we know that we can use nil
  // to represent an empty value.
  var reservedByTask: c_ptr(void) = nil;
  proc type getTaskId() {
    extern proc chpl_task_getInfoChapel(): c_ptr(chpl_task_infoChapel_t);
    return chpl_task_getInfoChapel() : c_ptr(void);
  }
  proc const ref isLockedByCurrentTask() : bool {
    return perTaskLock.getTaskId() == reservedByTask;
  }
  proc ref lock() {
    while this.reservedByTask != nil {
      currentTask.yieldExecution();
      chpl_atomic_thread_fence(memory_order_acquire);
    }
    chpl_atomic_thread_fence(memory_order_acquire);
    reservedByTask = perTaskLock.getTaskId();
  }
  proc ref unlock() {
    assert(isLockedByCurrentTask());
    reservedByTask = nil;
    chpl_atomic_thread_fence(memory_order_release);
  }
}

/* This represents the combination of a current buffer offset
   and a PerTaskLock */
record idxAndLock {
  var next: int = 0;     // where to start next operation in the buffer
  var lock: perTaskLock; // lock when using
}

private proc allocateIdxAndLockArray(): c_ptr(idxAndLock) {
  // round up allocation request to 64B & make it 64B aligned
  const nBytesToAlloc = divCeil(numLocales*c_sizeof(idxAndLock), 64) * 64;
  const nToAlloc = divCeil(nBytesToAlloc, c_sizeof(idxAndLock));
  return allocate(idxAndLock, nToAlloc, clear=true, alignment=64);
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
  var idxAndLocks: c_ptr(idxAndLock);

  proc init() {
    init this;

    /*extern proc gettid(): c_int;
    writeln("Creating CachedDstAggregatorClass ", c_ptrTo(this),
            " on thread id ", gettid());*/
  }

  proc postinit() {
    lBuffers = allocate(c_ptr(uint(8)), numLocales);
    idxAndLocks = allocateIdxAndLockArray();
    for loc in myLocaleSpace {
      lBuffers[loc] = allocate(uint(8), bufferSize);
      rBuffers[loc] = new remoteBuffer(uint(8), bufferSize, loc);
    }
  }

  proc deinit() {
    //writeln("Destroying CachedDstAggregatorClass ", c_ptrTo(this));
    flush(freeBuffers=true);
    for loc in myLocaleSpace {
      deallocate(lBuffers[loc]);
    }
    deallocate(idxAndLocks);
    deallocate(lBuffers);
  }

  proc flush(freeBuffers=false) {
    // TODO: try randomized flush
    // TODO: use unordered copy if too few elements to flush
    // TODO: try double buffering
    for offsetLoc in myLocaleSpace + lastLocale {
      const loc = offsetLoc % numLocales;

      ref idxl = idxAndLocks[loc];
      idxl.lock.lock();
      defer { idxl.lock.unlock(); }

      flushBuffer(loc, freeData=freeBuffers);
    }
  }

  inline proc copy(ref dst:?t, const ref src:t) {
    if boundsChecking {
      assert(src.locale.id == here.id);
    }

    const loc = dst.locale.id;
    const addr_size = c_sizeof(c_ptr(void));
    const data_size = c_sizeof(t);
    const size_size = c_sizeof(data_size.type);
    const serialize_bytes = addr_size + size_size + data_size;

    // Just do direct assignment if dst is local or src size is large
    if loc == here.id || serialize_bytes > (bufferSize >> 2) {
      dst = src;
      return;
    }

    {
      // "lock" the current buffer
      ref idxl = idxAndLocks[loc];
      idxl.lock.lock();
      defer { idxl.lock.unlock(); }

      ref bufferIdx = idxl.next;

      lastLocale = loc;
      var dstAddr = getAddr(dst);


      // Flush our buffer if this entry will exceed capacity
      if bufferIdx + serialize_bytes > bufferSize {
        flushBuffer(loc, freeData=false);
        opsUntilYield = yieldFrequency;
      }

      const buf = lBuffers[loc];

      // Buffer the dst address
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
    }

    // If it's been a while since we've let other tasks run, yield so that
    // we're not blocking remote tasks from flushing their buffers.
    // Do this without holding the "lock" since it doesn't really matter
    // how tasks change opsUntilYield when switching between them.
    if opsUntilYield == 0 {
      currentTask.yieldExecution();
      opsUntilYield = yieldFrequency;
    } else {
      opsUntilYield -= 1;
    }
  }

  proc flushBuffer(loc: int, freeData) {
    ref idxl = idxAndLocks[loc];
    assert(idxl.lock.isLockedByCurrentTask()); // should be done by callers
    ref bufferIdx = idxl.next;

    // return early if there's no data to flush
    if bufferIdx == 0 then return;

    const myBufferIdx = bufferIdx;

    // Get or allocate a remote buffer
    ref rBuffer = rBuffers[loc];
    const remBufferPtr = rBuffer.cachedAlloc();

    // Copy local buffer to remote buffer
    rBuffer.PUT(lBuffers[loc], myBufferIdx);
    assert(idxl.lock.isLockedByCurrentTask()); // should not have switched

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
    assert(idxl.lock.isLockedByCurrentTask()); // should not have switched

    if freeData {
      rBuffer.markFreed();
    }
    bufferIdx = 0;
  }
}

class CachedSrcAggregatorClass {
  // request: room for 2 pointers and a size
  // reply: room for 1 pointer, a size, and a data item (assume int size)
  const reqBufferSize = (srcBuffSize * 3*c_sizeof(c_ptr(void))):int;
  const replBufferSize = (srcBuffSize * 3*c_sizeof(c_ptr(void))):int;
  const bufferSize = (srcBuffSize * 4*c_sizeof(c_ptr(void))):int;
  const myLocaleSpace = 0..<numLocales;
  var lastLocale: int;
  var opsUntilYield = yieldFrequency;
  var lReqBuffers: c_ptr(c_ptr(uint(8)));
  var lReplBuffers: [myLocaleSpace][0..#replBufferSize] uint(8);
  var rReqBuffers: [myLocaleSpace] remoteBuffer(uint(8));
  var rReplBuffers: [myLocaleSpace] remoteBuffer(uint(8));
  var idxAndLocks: c_ptr(idxAndLock);

  proc postinit() {
    lReqBuffers = allocate(c_ptr(uint(8)), numLocales);
    idxAndLocks = allocateIdxAndLockArray();
    for loc in myLocaleSpace {
      lReqBuffers[loc] = allocate(uint(8), reqBufferSize);
      rReqBuffers[loc] = new remoteBuffer(uint(8), reqBufferSize, loc);
      rReplBuffers[loc] = new remoteBuffer(uint(8), replBufferSize, loc);
    }
  }

  proc deinit() {
    flush(freeBuffers=true);
    for loc in myLocaleSpace {
      deallocate(lReqBuffers[loc]);
    }
    deallocate(idxAndLocks);
    deallocate(lReqBuffers);
  }

  proc flush(freeBuffers=false) {
    // TODO: try randomized flush
    // TODO: use unordered copy if too few elements to flush
    // TODO: try double buffering
    for offsetLoc in myLocaleSpace + lastLocale {
      const loc = offsetLoc % numLocales;

      ref idxl = idxAndLocks[loc];
      idxl.lock.lock();
      defer { idxl.lock.unlock(); }

      flushBuffer(loc, freeData=freeBuffers);
    }
  }

  inline proc copy(ref dst: ?t, const ref src: t) {
    if boundsChecking {
      assert(dst.locale.id == here.id);
    }

    const loc = src.locale.id;
    const addr_size = c_sizeof(c_ptr(void));
    const data_size = c_sizeof(t);
    const size_size = c_sizeof(data_size.type);
    const req_serialize_bytes = 2*addr_size + size_size;
    const repl_serialize_bytes = addr_size + size_size + data_size;

    // Just do direct assignment if src is local or src size is large
    if loc == here.id ||
       req_serialize_bytes > (reqBufferSize >> 2) ||
       repl_serialize_bytes > (replBufferSize >> 2) {
      dst = src;
      return;
    }

    {
      // "lock" the current buffer
      ref idxl = idxAndLocks[loc];
      idxl.lock.lock();
      defer { idxl.lock.unlock(); }

      ref bufferIdx = idxl.next;

      lastLocale = loc;
      const dstAddr = getAddr(dst);
      const srcAddr = getAddr(src);

      // Flush our buffer if this entry will exceed capacity
      if bufferIdx + req_serialize_bytes > reqBufferSize {
        flushBuffer(loc, freeData=false);
        opsUntilYield = yieldFrequency;
      }

      const buf = lReqBuffers[loc];

      // Buffer the dst address
      memcpy(c_ptrTo(buf[bufferIdx]),
             c_ptrToConst(dstAddr):c_ptr(dstAddr.type),
             addr_size);
      bufferIdx += addr_size:int;
      // Buffer the src address
      memcpy(c_ptrTo(buf[bufferIdx]),
             c_ptrToConst(srcAddr):c_ptr(srcAddr.type),
             addr_size);
      bufferIdx += addr_size:int;
      // Buffer the size
      memcpy(c_ptrTo(buf[bufferIdx]),
             c_ptrToConst(data_size):c_ptr(data_size.type),
             size_size);
      bufferIdx += size_size:int;
    }

    // If it's been a while since we've let other tasks run, yield so that
    // we're not blocking remote tasks from flushing their buffers.
    // Do this without holding the "lock" since it doesn't really matter
    // how tasks change opsUntilYield when switching between them.
    if opsUntilYield == 0 {
      currentTask.yieldExecution();
      opsUntilYield = yieldFrequency;
    } else {
      opsUntilYield -= 1;
    }
  }

  proc flushBuffer(loc: int, freeData) {
    // note: freeData is ignored in this routine at present.
    // Remote buffers will be freed on deinit.

    ref idxl = idxAndLocks[loc];
    assert(idxl.lock.isLockedByCurrentTask()); // should be done by callers
    ref bufferIdx = idxl.next;

    // return early if there's no data to flush
    if bufferIdx == 0 then return;

    const myBufferIdx = bufferIdx;

    ref myRReqBuf = rReqBuffers[loc];
    ref myRReplBuf = rReplBuffers[loc];

    // Get or allocate remote buffers
    const rReqBufPtr = myRReqBuf.cachedAlloc();
    const rReplBufPtr = myRReplBuf.cachedAlloc();

    // Copy request (dst addr, src addr, size) to the remote buffer
    myRReqBuf.PUT(lReqBuffers[loc], myBufferIdx);
    assert(idxl.lock.isLockedByCurrentTask()); // should not have switched

    var bytesValsWritten: 2*int; // reply buffer idx, request buffer idx
    var addrBufferIdx = 0;

    while bytesValsWritten(1) < myBufferIdx {
      // Process remote buffer, copying the value of our addresses into a
      // remote buffer
      const cbytesValsWritten = bytesValsWritten;
      const myReplBufferSize = replBufferSize;
      on Locales[loc] {
        const mycbytesValsWritten = cbytesValsWritten;
        var (replyBufferIdx, reqBufferIdx) = mycbytesValsWritten;
        replyBufferIdx = 0;

        while replyBufferIdx < myReplBufferSize && reqBufferIdx < myBufferIdx {
          const addr_size = c_sizeof(c_ptr(void));
          const size_size = c_sizeof(addr_size.type);
          var dstAddr: c_ptr(void);
          var srcAddr: c_ptr(void);
          var dataSize: addr_size.type;

          var myReqBufferIdx = reqBufferIdx;
          // Read the dst addr from the request
          memcpy(c_ptrTo(dstAddr),
                 c_ptrTo(rReqBufPtr[myReqBufferIdx]),
                 addr_size);
          myReqBufferIdx += addr_size:int;
          // Read the src addr from the request
          memcpy(c_ptrTo(srcAddr),
                 c_ptrTo(rReqBufPtr[myReqBufferIdx]),
                 addr_size);
          myReqBufferIdx += addr_size:int;
          // Read the size from the request
          memcpy(c_ptrTo(dataSize),
                 c_ptrTo(rReqBufPtr[myReqBufferIdx]),
                 size_size);
          myReqBufferIdx += size_size:int;

          const repl_serialize_bytes = addr_size + size_size + dataSize;
          if repl_serialize_bytes > myReplBufferSize {
            // halt if the size is too big for a single buffer --
            // this should have been handled in 'copy'
            halt("size of serialized reply exceeds buffer size");
          }
          if replyBufferIdx + repl_serialize_bytes > myReplBufferSize {
            break;
          }

          // copy into the reply buffer

          //writeln("Loading ", dstAddr, " from ", srcAddr);

          // Write the dst address to the reply
          memcpy(c_ptrTo(rReplBufPtr[replyBufferIdx]),
                 c_ptrTo(dstAddr),
                 addr_size);
          replyBufferIdx += addr_size:int;
          // Write the serialized size to the reply
          memcpy(c_ptrTo(rReplBufPtr[replyBufferIdx]),
                 c_ptrTo(dataSize),
                 size_size);
          replyBufferIdx += size_size:int;
          // Write the serialized data to the reply
          memcpy(c_ptrTo(rReplBufPtr[replyBufferIdx]),
                 srcAddr,
                 dataSize);
          replyBufferIdx += dataSize:int;

          // also update myReqBufferIdx
          reqBufferIdx = myReqBufferIdx;
        }
        // PUT the metadata
        bytesValsWritten = (replyBufferIdx, reqBufferIdx);
      }
      assert(idxl.lock.isLockedByCurrentTask()); // should not have switched

      const replyLen = bytesValsWritten(0);
      // Copy the values loaded remotely into the local buffer
      myRReplBuf.GET(lReplBuffers[loc], replyLen);
      assert(idxl.lock.isLockedByCurrentTask()); // should not have switched

      // Assign the values loaded to the destination addresses
      var replyIdx = 0;
      ref replyBuf = lReplBuffers[loc];
      while replyIdx < replyLen {
        const addr_size = c_sizeof(c_ptr(void));
        const size_size = c_sizeof(addr_size.type);
        var dstAddr: c_ptr(void);
        var dataSize: addr_size.type;

        // Read the dst address from the reply
        memcpy(c_ptrTo(dstAddr),
               c_ptrTo(replyBuf[replyIdx]),
               addr_size);
        replyIdx += addr_size:int;
        // Read the serialized size from the reply
        memcpy(c_ptrTo(dataSize),
               c_ptrTo(replyBuf[replyIdx]),
               size_size);
        replyIdx += size_size:int;

        // Read the data from the reply into the destination addr
        memcpy(dstAddr,
               c_ptrTo(replyBuf[replyIdx]),
               dataSize);
        replyIdx += dataSize:int;
      }
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
    // the runtime doesn't exist anymore & then the on statements
    // can't clean up.
    /*var obj = ptr:unmanaged RootClass?;
    if obj != nil {
      delete obj;
    }*/
  }

  proc keyCreate() : pthread_key_t throws {
    //extern proc gettid(): c_int;
    //writeln("in keyCreate on thread id ", gettid());
    var key: pthread_key_t;
    var rc: c_int = pthread_key_create(key, c_ptrTo(pthread_key_deleter));
    if rc != 0 {
      throw createSystemError(rc, "error in pthread_key_create");
    }

    return key;
  }

  proc setSpecific(key: pthread_key_t, obj: c_ptr(void)) : void throws {
    //extern proc printf(fmt: c_ptrConst(c_char));
    //printf("setSpecific\n");
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
    extern proc gettid(): c_int;
    extern proc pthread_self(): c_ptr(void);

    @functionStatic(sharingKind.computePerLocale)
    var key = try! keyCreate();

    var ptr : c_ptr(void) = getSpecific(key);
    if ptr == nil {
      //writeln("in getPerPthreadClass getSpecific return nil on thread id ", gettid(), " pthread ", pthread_self());

      var obj = new unmanaged t();

      ptr = c_ptrTo(obj);
      //writeln("in getPerPthreadClass calling setSpecific thread id ", gettid(), " pthread ", pthread_self());
      try! setSpecific(key, ptr);
      assert(ptr != nil);
    }
    //writeln("in getPerPthreadClass returning ", ptr, " on thread id ", gettid());
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
