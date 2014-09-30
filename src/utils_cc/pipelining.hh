/*
  (*) 2008-2013 Michael Ferguson <michaelferguson@acm.org>

    * This is a work of the United States Government and is not protected by
      copyright in the United States.

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

  femto/src/utils_cc/pipelining.hh
*/
#ifndef _PIPELINING_HH_
#define _PIPELINING_HH_

/* A pipe has a single read end and a single write end.
   Each pipe is thread-safe and assumes shared memory between
   its ends. Other kinds of communication (e.g. between machines)
   can be added as needed as new nodes in the pipeline graph.

   Pipeline Nodes represent processing that must be done from some
   input pipes to some output pipes. They are individual threads.
   Generally each node will have a "setup" function which will
   set up (but not start) the node. This setup function will create
   the output pipe(s) for that node. There are "compound" nodes which
   represent some larger group of nodes (e.g. sorting nodes which
   are multiple-pass). These compound nodes should start their
   sub-nodes at the start of their main routine.

   */


#include <cstdio>

extern "C" {
#include <pthread.h>
#include <errno.h>
#include "bswap.h"
#include "config.h"
#include "error.h"
#include "timing.h"
#include "page_utils.h"
}

#include <cassert>
#include <cstdlib>
#include <algorithm>
#include <iostream>
#include <vector>
#include <deque>
// error_t exceptions.
#include "error.hh"

// Variable-sized integers
#include "varint.hh"

// Simple utilities
#include "utils.hh"

// RecordTraits and EncodingRecord
#include "record.hh"

// 64 KB tile
#define DEFAULT_TILE_SIZE (64*1024L)
// each IO request is 8 MB
//#define DEFAULT_TILES_PER_IO_GROUP (128)
// each IO request is 2 MB
//#define DEFAULT_TILES_PER_IO_GROUP (32)
// each IO request is 1 MB
//#define DEFAULT_TILES_PER_IO_GROUP (16)
// each IO request is 512KB = 0.5 MB
//#define DEFAULT_TILES_PER_IO_GROUP (8)
// each IO request is 768KB = 0.75 MB
#define DEFAULT_TILES_PER_IO_GROUP (12)
// each IO request is 1.5 MB
//#define DEFAULT_TILES_PER_IO_GROUP (24)

#define DEFAULT_NUM_TILES (2)
#define DEFAULT_NUM_IO_GROUPS (2)

// A tile is the basic structure passed around by pipes
// It's really just a fixed-size amount of data that can be read into 
// memory.
// It should be small enough, generally, to fit into L1 cache.
struct tile {
  // Don't need TR1 shared pointer here because the pipeline should 
  // handle all allocation/deallocation (differently for an mmap file reader)
  //std::tr1::shared_ptr<void> data_ptr; // pointer to the data in memory
  void* data;
  // pos is unnecessary - it can just be a local variable.
  // pos is mutable - you can change the read position without changing
  //mutable size_t pos; // read position
  size_t len; // amount of data currently here
  size_t max; // max size of this buffer
  // Empty tile, also known as "end-of-file"
  tile() : data(NULL), len(0), max(0) { }
  // Full tile with data in it..
  tile(void* data, size_t len, size_t max)
    : data(data),len(len),max(max) { }
  // True if it's the end.
  bool is_end() const {
    return data==NULL;
  }
  bool has_tile() const {
    return data!=NULL;
  }
  void* get_data_void() {
    return static_cast<void*>(data);
  }
};

static inline tile empty_tile() {
  tile t;
  return t;
}
static inline tile allocate_pages_tile(size_t max)
{
  struct allocated_pages mem;
  mem = allocate_pages(max);
  if( ! mem.data ) {
    throw error(ERR_MEM);
  }
  tile ret;
  ret.data = mem.data;
  ret.len = 0;
  ret.max = mem.len;
  return ret;
}
static inline void free_pages_tile(tile t)
{
  struct allocated_pages mem;
  mem.data = t.data;
  mem.len = t.max;
  free_pages(mem);
}

struct tile_pos {
  tile t;
  size_t pos;
  tile_pos() : t(), pos(0) { }
};

// Rounds a tile size to a page-aligned
// multiple of the passed number (which should be small).
size_t round_tile_size_to_page_multiple(size_t tile_size, size_t record_size);

// a class to mark things as uncopyable.
class uncopyable {
  protected:
    uncopyable() {}
    ~uncopyable() {} // not virtual because it's 
                     // got nothing to destruct!
  private:
    uncopyable(const uncopyable&) = delete;
    uncopyable& operator=(const uncopyable&) = delete;
};

// "Resource management" class for a pthreads lock.
struct pthread_mutex : public uncopyable {
  pthread_mutex_t mutex;
  pthread_mutex() {
    int rc;
    // Initialize the pthread lock.
    rc = pthread_mutex_init(&mutex, NULL);
    if( rc ) throw error(ERR_PTHREADS("Could not init lock", rc));
  }
  ~pthread_mutex() {
    int rc;
    // Destroy the mutex.
    rc = pthread_mutex_destroy(&mutex);
    if( rc ) {
      // We're in a destructor, so we don't throw. However, we can warn.
      warn_if_err(ERR_PTHREADS("Could not destroy mutex", rc));
    }
  }
  void lock() {
    int rc;
    //printf("%p locking %p\n", pthread_self(), this);
    rc = pthread_mutex_lock(&mutex);
    //printf("%p locked %p\n", pthread_self(), this);
    if( rc ) throw error(ERR_PTHREADS("Could not lock", rc));
  }
  // Returns true if the lock was acquired, false otherwise.
  bool trylock() {
    int rc;
    rc = pthread_mutex_trylock(&mutex);
    if( rc == EBUSY ) return false;
    else if ( rc ) throw error(ERR_PTHREADS("Could not trylock", rc));
    else return true;
  }
  void unlock() {
    int rc;
    //printf("%p unlocking %p\n", pthread_self(), this);
    rc = pthread_mutex_unlock(&mutex);
    //printf("%p unlocked %p\n", pthread_self(), this);
    if( rc ) throw error(ERR_PTHREADS("Could not unlock", rc));
  }
  void unlock_nothrow() {
    int rc;
    //printf("%p unlocking_ %p\n", pthread_self(), this);
    rc = pthread_mutex_unlock(&mutex);
    //printf("%p unlocked_ %p\n", pthread_self(), this);
    if( rc ) warn_if_err(ERR_PTHREADS("Could not unlock", rc));
  }
};

struct pthread_held_mutex : public uncopyable {
  pthread_mutex* lock;
  pthread_held_mutex(pthread_mutex* lock) : lock(lock) { lock->lock(); }
  ~pthread_held_mutex() { lock->unlock_nothrow(); }
};

struct pthread_rwlock : public uncopyable {
  pthread_rwlock_t lock;
  pthread_rwlock() {
    int rc;
    // Initialize the read-write lock.
    rc = pthread_rwlock_init(&lock, NULL);
    if( rc ) throw error(ERR_PTHREADS("Could not init rwlock", rc));
  }
  ~pthread_rwlock() {
    int rc;
    // Destroy the read-write lock.
    rc = pthread_rwlock_destroy(&lock);
    if( rc ) {
      // We're in a destructor - don't throw.
      warn_if_err(ERR_PTHREADS("Could not init rwlock", rc));
    }
  }
  void lock_read() {
    int rc;
    rc = pthread_rwlock_rdlock(&lock);
    if( rc ) throw error(ERR_PTHREADS("Could not lock read", rc));
  }
  void lock_write() {
    int rc;
    rc = pthread_rwlock_wrlock(&lock);
    if( rc ) throw error(ERR_PTHREADS("Could not lock write", rc));
  }
  void unlock() {
    int rc;
    rc = pthread_rwlock_unlock(&lock);
    if( rc ) throw error(ERR_PTHREADS("Could not unlock", rc));
  }
  void unlock_nothrow() {
    int rc;
    rc = pthread_rwlock_unlock(&lock);
    if( rc ) warn_if_err(ERR_PTHREADS("Could not unlock", rc));
  }
};

struct pthread_held_read_lock : public uncopyable {
  pthread_rwlock* lock;
  pthread_held_read_lock(pthread_rwlock* lock)
    : lock(lock)
  { lock->lock_read(); }
  ~pthread_held_read_lock() { lock->unlock_nothrow(); }
};

struct pthread_held_write_lock : public uncopyable {
  pthread_rwlock* lock;
  pthread_held_write_lock(pthread_rwlock* lock)
    : lock(lock)
  { lock->lock_write(); }
  ~pthread_held_write_lock() { lock->unlock_nothrow(); }
};

// Resource management class for pthread condition variable.
struct pthread_cond : public uncopyable {
  pthread_cond_t cond;
  pthread_cond() {
    int rc;
    rc = pthread_cond_init(&cond, NULL);
    if( rc ) throw error(ERR_PTHREADS("Could not init cond", rc));
  }
  ~pthread_cond() {
    int rc;
    // Destroy the lock.
    rc = pthread_cond_destroy(&cond);
    if( rc ) {
      // We're in a destructor, so we don't throw. However, we can warn.
      warn_if_err(ERR_PTHREADS("Could not destroy mutex", rc));
    }
  }
  void wait(pthread_mutex* mutex) {
    int rc;
    //printf("%p waiting %p\n", pthread_self(), this);
    rc = pthread_cond_wait(&cond, &mutex->mutex);
    if( rc ) throw error(ERR_PTHREADS("Could not wait", rc));
  }
  // return true if we were signalled.
  bool timedwait(pthread_mutex* mutex, const struct timespec* abstime) {
    int rc;
    //printf("%p waiting %p\n", pthread_self(), this);
    rc = pthread_cond_timedwait(&cond, &mutex->mutex, abstime);
    if( rc == ETIMEDOUT ) return false;

    if( rc ) throw error(ERR_PTHREADS("Could not wait", rc));

    return true;
  }

  void signal() {
    int rc;
    rc = pthread_cond_signal(&cond);
    if( rc ) throw error(ERR_PTHREADS("Could not signal", rc));
  }
  void broadcast() {
    int rc;
    rc = pthread_cond_broadcast(&cond);
    if( rc ) throw error(ERR_PTHREADS("Could not broadcast", rc));
  }
};

// A task queue is just a FIFO queue of tiles with support
// for multithreaded access.
// We can push a task into the queue, pop the top task,
// or close the list. A closed list will refuse to wait for 
// new tasks to appear in the list.
// There must be a default constructor for a Task, which means end-of-file.
template<typename Task>
class task_queue : public uncopyable {
  pthread_mutex lock;
  pthread_cond cond;
  bool closed; // if it's closed and empty, it'll never be filled again
  std::deque<Task> tasks;
 public:
  task_queue()
    : lock(), cond(), closed(false), tasks()
  {
  }
  // Pop a Task from the list.
  Task pop()
  {
    Task ret;

    { pthread_held_mutex held_lock(&lock);
      while( tasks.empty() ) {
        // If the the list is closed, we're never going to get more.
        if( closed ) {
          // destructor releases mutex
          return Task(); // end-of-file Task.
        }

        // Otherwise, wait on the condition variable.
        cond.wait(&lock);
      }

      // Tasks is not empty; we have one we can use.
      // Pop the first element.
      ret = tasks.front();
      tasks.pop_front();

    } // destructor releases mutex.

    return ret;
  }
  // Pop a Task from the list without waiting.
  // Returns true if we got one.
  bool pop_nowait(Task* task_out)
  {
    Task ret;

    { pthread_held_mutex held_lock(&lock);
      if( tasks.empty() ) {
        *task_out = ret;
        if( closed ) {
          return true; // EOF is the thing we got.
        } else {
          return false;
        }
      } else {
        // Tasks is not empty; we have one we can use.
        // Pop the first element.
        ret = tasks.front();
        tasks.pop_front();
        *task_out = ret;
        return true;
      }
    } // destructor releases mutex.
  }

  // Wait for a Task to be present in the list.
  void wait()
  {
    { pthread_held_mutex held_lock(&lock); // lock the mutex
      while( tasks.empty() ) {
        // If the the list is closed, we're never going to get more.
        if( closed ) {
          // destructor releases mutex
          return;
        }

        // Otherwise, wait on the condition variable.
        cond.wait(&lock);
      }
    } // destructor unlocks the mutex.
  }

  void push(const Task t)
  {
    { pthread_held_mutex held_lock(&lock); // lock the mutex
      // Append to the queue.
      tasks.push_back(t);

      // Signal anybody waiting that we've got it!
      cond.signal();
    } // destructor unlocks the mutex.
  }

  void close()
  {
    { pthread_held_mutex held_lock(&lock); // lock the mutex
      closed = true;
    
      // Awaken anybody waiting on this mutex.
      cond.broadcast();
    } // destructor unlocks the mutex.
  }

  // Changes the task queue to be open once again, but does nothing about
  // threads that might've finished when it was closed...
  void reopen()
  {
    { pthread_held_mutex held_lock(&lock); // lock the mutex
      closed = false;
    } // destructor unlocks the mutex.
  }

  void wait_close()
  {
    { pthread_held_mutex held_lock(&lock); // lock the mutex
      while( ! closed ) {
        // Otherwise, wait on the condition variable.
        cond.wait(&lock);
      }
    } // destructor unlocks the mutex.
  }
  bool is_closed()
  {
    bool ret;
    { pthread_held_mutex held_lock(&lock); // lock the mutex
      ret = closed;
    } // destructor unlocks the mutex.
    return ret;
  }

  // Gets the size. No gaurantee we'll have this size in a moment!
  size_t size() 
  {
    size_t ret;
    { pthread_held_mutex held_lock(&lock); // lock the mutex
      ret = tasks.size();
    } // destructor unlocks the mutex.

    return ret;
  }
};

typedef task_queue<tile> tile_list;

// interface for any pipe (read or write)
struct pipe {
  virtual ~pipe() = 0;
  // Get the maximum size of each tile
  virtual size_t get_tile_size() = 0;
  virtual size_t get_num_tiles() = 0;
  virtual bool is_thread_safe() = 0;
};

// The interface for the read end of a pipe
struct read_pipe: public pipe {
  virtual ~read_pipe() = 0;
  // get_full_tile will block if the tile isn't available yet.
  virtual const tile get_full_tile() = 0;
  // get_full_tile_will_block returns true if getting a full tile
  // will cause the channel to block. Note that if the pipe is shared
  // among threads, get_full_tile() might still block if another thread got
  // the tile first! (ie. it's not atomic).
  //virtual bool get_full_tile_will_block() { return true; }
  virtual void put_empty_tile(const tile& t) = 0;
  // indicate that no new data will be read -- no new empty tiles added.
  virtual void close_empty() = 0;
  // wait for at least one full tile to be placed into the pipe.
  virtual void wait_full() = 0;

  // Functions that buffered pipe implements but that maybe won't
  // be implemented by the others
  
  // free memory used by tiles, if possible.
  virtual void free_empty_tiles() { }
  // add tiles, if possible.
  virtual void add_tiles(size_t num_new_tiles) { }

  virtual void assert_closed_empty() { }
};

// The interface for the write end of a pipe
struct write_pipe: public pipe {
  // Virtual destructor since we expect subclasses
  virtual ~write_pipe() = 0;
  // get_empty_tile returns a tile (a buffer) that can be filled
  // and then written with a call to put_full_tile or discarded
  // without being written with a call to return_empty_tile().
  // get_empty_tile may block if there isn't enough room in the pipe.
  virtual tile get_empty_tile() = 0;
  // get_empty_tile_will_block returns true if getting an empty tile
  // will cause the channel to block. Note that if the pipe is shared
  // among threads, get_empty_tile_will_block() might still block if another
  // thread got the tile first! (ie. it's not atomic).
  //virtual bool get_empty_tile_will_block() { return true; }
  // Puts a full tile back in the pipe; in other words, writes the
  // data in the tile.
  virtual void put_full_tile(const tile& t) = 0;
  // Return an unused empty tile.
  //virtual void return_empty_tile(const tile& empty_tile) = 0;
  // indicate that no new data will be produced -- no new full tiles.
  virtual void close_full() = 0;

  virtual void assert_closed_full() { }
};

// Pipe just uses a memory region and has a maximum length.
// Does not support both reading and writing at the same time 
// (although it could).
class memory_pipe: public read_pipe, public write_pipe {
  private:
    pthread_mutex lock; // protects everything here.
    void* data;
    size_t len;
    size_t cur;
    size_t tile_size;
  private:
    tile next_tile();
  public:
    // Allocates some number of tiles.
    memory_pipe(void* data, size_t len, size_t tile_size);
    virtual ~memory_pipe();
    // Get the maximum size of each tile
    virtual size_t get_tile_size();
    // Get the number of tiles supported by this pipe.
    // Note that this may change while the pipe exists; this checks it at a moment.
    virtual size_t get_num_tiles();
    virtual bool is_thread_safe();

    // Reader functions
    virtual const tile get_full_tile();
    //virtual bool get_full_tile_will_block();
    virtual void put_empty_tile(const tile& t);
    // indicate that no new data will be read -- no new empty tiles added.
    virtual void close_empty();
    // wait for at least one full tile to be placed into the pipe.
    // That tile isn't necessarily present once it's been added...
    virtual void wait_full();

    
    // Writer functions
    virtual tile get_empty_tile();
    virtual void put_full_tile(const tile& t);
    // indicate that no new data will be produced -- no new full tiles.
    virtual void close_full();

    void reset();
};

// Buffered unidirectional pipe for inter-thread communication
// Uses multiple inheritance for interface, which should be O.K.
// (the alternative would be members implementing each side, but then
// we'd need to return pointers/references to them).
class buffered_pipe: public read_pipe, public write_pipe, public uncopyable {
  private:
    pthread_mutex lock; // protects tile_size and num_tiles
    size_t tile_size;
    size_t num_tiles;
    tile_list full_tiles; // already thread-safe 
    tile_list empty_tiles; // already thread-safe
  public:
    // Allocates some number of tiles.
    buffered_pipe(size_t tile_size=DEFAULT_TILE_SIZE, size_t num_tiles=DEFAULT_NUM_TILES);
    virtual ~buffered_pipe();
    // Get the maximum size of each tile
    virtual size_t get_tile_size();
    // Get the number of tiles supported by this pipe.
    // Note that this may change while the pipe exists; this checks it at a moment.
    virtual size_t get_num_tiles();

    // Reader functions
    virtual const tile get_full_tile();
    //virtual bool get_full_tile_will_block();
    virtual void put_empty_tile(const tile& t);
    // indicate that no new data will be read -- no new empty tiles added.
    virtual void close_empty();
    // wait for at least one full tile to be placed into the pipe.
    // That tile isn't necessarily present once it's been added...
    virtual void wait_full();

    // Writer functions
    virtual tile get_empty_tile();
    //virtual bool get_empty_tile_will_block();
    virtual void put_full_tile(const tile& t);
    // indicate that no new data will be produced -- no new full tiles.
    // also return t as an empty tile.
    //virtual void close_full(const tile& t);
    // indicate that no new data will be produced -- no new full tiles.
    virtual void close_full();

    // Free empty tiles to reclaim space. Only frees those empty at that moment.
    virtual void free_empty_tiles();
    // Add new empty tiles to a pipe..
    virtual void add_tiles(size_t num_new_tiles);
    virtual ssize_t get_num_empty_tiles();
    virtual ssize_t get_num_full_tiles();
    virtual bool is_thread_safe();
};

// Serializing Buffered unidirectional pipe for inter-thread communication
// For testing purposes, this pipe stores all the input in memory before
// providing any output.
class serial_buffered_pipe: public read_pipe, public write_pipe, public uncopyable {
  private:
    pthread_mutex lock; // protects tile_size and num_tiles
    size_t tile_size;
    size_t num_tiles;
    size_t num_made_tiles;
    size_t num_freed_tiles;
    tile_list tiles;
  public:
    // Allocates some number of tiles.
    serial_buffered_pipe(size_t tile_size, size_t num_tiles);
    virtual ~serial_buffered_pipe();
    // Get the maximum size of each tile
    virtual size_t get_tile_size();
    // Get the number of tiles supported by this pipe.
    // Note that this may change while the pipe exists; this checks it at a moment.
    virtual size_t get_num_tiles();

    // Reader functions
    virtual const tile get_full_tile();
    //virtual bool get_full_tile_will_block();
    virtual void put_empty_tile(const tile& t);
    // indicate that no new data will be read -- no new empty tiles added.
    virtual void close_empty();
    // wait for at least one full tile to be placed into the pipe.
    // That tile isn't necessarily present once it's been added...
    virtual void wait_full();

    // Writer functions
    virtual tile get_empty_tile();
    //virtual bool get_empty_tile_will_block();
    virtual void put_full_tile(const tile& t);
    // indicate that no new data will be produced -- no new full tiles.
    // also return t as an empty tile.
    //virtual void close_full(const tile& t);
    // indicate that no new data will be produced -- no new full tiles.
    virtual void close_full();

    // Free empty tiles to reclaim space. Only frees those empty at that moment.
    virtual void free_empty_tiles();
    // Add new empty tiles to a pipe..
    virtual void add_tiles(size_t num_new_tiles);
    virtual bool is_thread_safe();
    tile make_tile();
    void free_tile(tile t);
};

struct single_tile_pipe: public read_pipe {
  read_pipe* put_back;
  tile t;
  bool done;
  single_tile_pipe(tile t, read_pipe* put_back);
  // Get the maximum size of each tile
  virtual size_t get_tile_size();
  // Get the number of tiles supported by this pipe.
  // Note that this may change while the pipe exists; this checks it at a moment.
  virtual size_t get_num_tiles();

  // Reader functions
  virtual const tile get_full_tile();
  //virtual bool get_full_tile_will_block();
  virtual void put_empty_tile(const tile& t);
  // indicate that no new data will be read -- no new empty tiles added.
  virtual void close_empty();
  // wait for at least one full tile to be placed into the pipe.
  // That tile isn't necessarily present once it's been added...
  virtual void wait_full();

  // Free empty tiles to reclaim space. Only frees those empty at that moment.
  virtual void free_empty_tiles();
  // Add new empty tiles to a pipe..
  virtual void add_tiles(size_t num_new_tiles);
  virtual bool is_thread_safe();
};

/* A tile list that instead of blocking
 * calls a "continuation" once data is available.
 * Callback should be easily copy-able and should 
 * implement operator() taking a tile as an argument.
 */
template<typename Callback>
class async_tile_list : public uncopyable {
  pthread_mutex lock;
  pthread_cond cond;
  bool closed;
  std::deque<tile> tiles;
  std::deque<Callback> waiters;
 public:
  async_tile_list()
    : lock(), cond(), closed(false), tiles()
  {
  }
 private:
  // assumes that the lock is held.
  void run_waiters()
  {
    // Now, is there anybody waiting for a tile?
    // Get a tile and give it to a waiter provided that
    //  -- there is a waiter
    //  -- we're done (the pipe is closed) or there's a tile to give.
    while( waiters.size() > 0 && (closed || tiles.size() > 0)) {
      // Get the first tile.
      tile t; // this is eof by default.
      if( tiles.size() > 0 ) {
        t = tiles.front();
        tiles.pop_front();
      }
      // Get the first waiter.
      Callback cb = waiters.front();
      waiters.pop_front();
      lock.unlock();
      try {
        cb(t); // callback must be called when not holding the lock...
      } catch( ... ) {
        lock.lock(); // make sure the lock stays locked... until we return.
        throw;
      }
      lock.lock(); // grab the lock again!
    }
  }
 public:
  // Calls callback cb once the tile is available.
  void pop(Callback cb)
  {
    { pthread_held_mutex held_lock(&lock);
      // Add a waiter.
      waiters.push_back(cb);
      // Run the waiters.
      run_waiters();
    } // destructor releases lock.
  }
  // Only for destructing - pops the top element tile out
  // or returns an empty tile if there isn't one.
  tile pop_destruct()
  {
    tile ret; // empty tile by default.
    { pthread_held_mutex held_lock(&lock);
      if( tiles.size() > 0 ) {
        ret = tiles.front();
        tiles.pop_front();
      }
    }
    return ret;
  }
  // Returns a tile to the queue.
  void push(const tile& t)
  {
    { pthread_held_mutex held_lock(&lock);
      tiles.push_back(t);
      run_waiters();
    } // destructor releases lock.
  }
  void close()
  {
    { pthread_held_mutex held_lock(&lock);
      closed = true;
      run_waiters();
    } // destructor releases lock.
  }
};

template<typename Callback>
struct async_pipe
{
  size_t tile_size;
  size_t num_tiles;
  async_tile_list<Callback> full_tiles;
  async_tile_list<Callback> empty_tiles;

  async_pipe(size_t tile_size, size_t num_tiles)
    : tile_size(tile_size), num_tiles(num_tiles), full_tiles(), empty_tiles()
  {
    // Allocate and put num_tiles in empty tiles.
    for( size_t i = 0; i < num_tiles; i++ ) {
      tile t(allocate_memory(tile_size), 0, tile_size);
      empty_tiles.push(t);
    }
  }
  ~async_pipe()
  {
    while( 1 ) {
      tile t = full_tiles.pop_destruct();
      if( t.is_end() ) break;
      free_memory(t.data);
    }

    while( 1 ) {
      tile t = empty_tiles.pop_destruct();
      if( t.is_end() ) break;
      free_memory(t.data);
    }
  }
  size_t get_tile_size()
  {
   return tile_size; 
  }
  size_t get_num_tiles()
  {
    return num_tiles;
  }
  bool is_thread_safe()
  {
    return true;
  }
  void get_full_tile(Callback cb)
  {
    full_tiles.pop(cb);
  }
  void put_empty_tile(tile& t)
  {
    t.len = 0;
    empty_tiles.push(t);
  }
  void close_empty()
  {
    empty_tiles.close();
  }
  void get_empty_tile(Callback cb)
  {
    empty_tiles.pop(cb);
  }
  void put_full_tile(const tile& t)
  {
    full_tiles.push(t);
  }
  void close_full()
  {
    full_tiles.close();
  }
};

serial_buffered_pipe* create_pipe_for_data(size_t len, const void* data);

// Counts the number of tiles written by a pipe
struct counting_write_pipe: public write_pipe
{
  write_pipe* pipe;
  size_t num_written;

  counting_write_pipe(write_pipe* pipe);

  virtual size_t get_tile_size();
  virtual size_t get_num_tiles();

  virtual tile get_empty_tile();
  //virtual bool get_empty_tile_will_block();
  virtual void put_full_tile(const tile& t);
  virtual void close_full(const tile& t);
  virtual void close_full();
  virtual bool is_thread_safe();

  size_t get_num_written();
};


// An interface into a pipe providing a 
// "push_back" method which appends data to the
// end of the pipe (getting a new buffer if necessary).
// This is also acts as a back-inserter output iterator.
template<typename Record>
class pipe_back_inserter : public std::iterator<std::output_iterator_tag, Record> {
  private:
    typedef RecordTraits<Record> record_traits;
    // Pointer to the pipe we're adding data on to the end of.
    write_pipe* const pipe;
    bool pipe_finished;
    bool dont_close;
    // The current tile we're using.
    tile t;
    // The number of tiles that have been written.
    size_t num_written;
  private:
    void put_tile_impl(tile t_in)
    {
      assert(t_in.len <= t_in.max);
      //printf("pipe_back_inserter putting tile %p in %p\n", t_in.data, pipe);
      pipe->put_full_tile(t_in);
      num_written++;
    }
    void get_tile_impl()
    {
      t = pipe->get_empty_tile();
      //printf("pipe_back_inserter has tile %p from %p\n", t.data, pipe);
      assert(t.len == 0);
      if( t.is_end() ) {
        throw error(ERR_INVALID_STR("pipe_back_inserter could not get empty tile"));
      }
    }
  public:
    pipe_back_inserter(write_pipe* pipe, bool dont_close=false)
      : pipe(pipe), pipe_finished(false), dont_close(dont_close), t(), num_written(0)
    {
      // Get a tile to append to.
      get_tile_impl();
    }

    void flush() {
      if( ! t.is_end() && ! pipe_finished ) {
        if( t.len > 0 ) {
          put_tile_impl(t);
          get_tile_impl();
        }
      }
      // otherwise we have an empty or end tile.
    }

    void finish() {
      // write the tile if we've got one
      if( ! t.is_end() ) {
        put_tile_impl(t);
      }

      // Close the pipe.
      if( ! pipe_finished ) {
        if( ! dont_close ) {
          pipe->close_full();
        }
        pipe_finished = true;
      }
      // Remember that we're at the end.
      tile empty;
      t = empty;
    }

    // Destructor finishes the pipe.
    ~pipe_back_inserter() {
      try {
        if( ! pipe_finished ) {
          warn_if_err(ERR_INVALID_STR("finish must be called before ~pipe_back_inserter"));
        }
        finish();
      } catch(std::exception& e) {
        std::cerr << "Exception in ~pipe_back_inserter: " << e.what() << std::endl;
      }
    }
    // Returns the number of tiles we've written.
    size_t get_num_written() {
      return num_written;
    }
    tile get_tile() {
      return t;
    }
    void put_tile(tile t_in) {
      assert(t.data == t_in.data);
      // First put the tile in question.
      put_tile_impl(t_in);
      // Then get a new empty tile.
      get_tile_impl();
    }
    // push_back -- append a value to the output.
    void push_back(const Record& value) {
      //const size_t r_len = value.get_record_length(ctx);
      const size_t r_len = record_traits::get_record_length(value);
      if( t.len + r_len <= t.max ) {
        // OK - we fit into this tile.
      } else {
        // Get a new tile.
        // First, put the tile we have.
        put_tile_impl(t);
        // Next, get a new empty tile.
        get_tile_impl();
      }
      // Save that output!
      //value.encode(ctx, out_data+t.len);
      record_traits::encode(value, PTR_ADD(t.data,t.len));
      t.len += r_len;
      assert(t.len <= t.max);
    }

    // *iter=value  Writes value to where iterator refers
    // (actually, *iter returns iter, and iter=value does the inserting)
    pipe_back_inserter<Record>&
    operator* () {
      return *this;
    }

    pipe_back_inserter<Record>&
    operator= (const Record value) {
      push_back(value);
      return *this;
    }

    // ++iter       Step forward, returning new position
    // (actually does nothing, returns iter)
    pipe_back_inserter<Record>&
    operator++ () {
      return *this;
    }

    // iter++       Step forward, returning old position
    // (actually does nothing, returns iter)
    pipe_back_inserter<Record>&
    operator++ (int) {
      return *this;
    }

    // TYPE(iter)   Copy an iterator
    // Default copy is O.K.

    write_pipe* get_pipe() {
      return pipe;
    }
};

template<typename Record>
class pipe_iterator : public std::iterator<std::input_iterator_tag, Record> {
  private:
    typedef RecordTraits<Record> record_traits;
    // Pointer to the pipe we're reading records from
    read_pipe* const pipe;
    bool pipe_finished;
    bool dont_close;
    // The current tile we're using.
    // If there's no tile, we're at EOF.
    tile t;
    // The current position within the tile.
    size_t pos;
    // The current record we have
    Record cur;
    // The number of tiles that have been read.
    size_t num_read;

  public:
    // Construct the end-of-pipe iterator
    pipe_iterator()
      : pipe(NULL), pipe_finished(true),
        t(/*no args=no tile*/),
        pos(0), cur(/* default cnstr */), num_read(0)
    { }
    // Make cur point to the next non-empty tile.
    void next_tile() {
      // Get the next tile, skipping any empty ones.
      do {
        if( ! t.is_end() ) pipe->put_empty_tile(t);
        //printf("Waiting for tile from pipe %p\n", pipe); fflush(stdout);
        t = pipe->get_full_tile();
        //printf("Got tile from pipe %p, data=%p len=%li\n", pipe, t.data, t.len); fflush(stdout);
        if( !t.is_end() ) num_read++;
      } while( !t.is_end() && t.len == 0 );
      pos = 0;
    }
    pipe_iterator(read_pipe* pipe, bool dont_close=false)
      : pipe(pipe), pipe_finished(false), dont_close(dont_close),
        t(), pos(0), cur(), num_read(0)
    {
      // Get the first non-empty tile.
      next_tile();

      // Read the first record.
      if( ! t.is_end() ) {
        //cur.decode(ctx, t.data+pos);
        record_traits::decode(cur, PTR_ADD(t.data,pos));
      }
      // Otherwise, has_tile is false and so there's no record.
    }

    void finish() {
      if( pipe ) {
        if( ! t.is_end() ) {
          pipe->put_empty_tile(t);
          tile empty;
          t = empty;
        }
        if( ! pipe_finished ) {
          if( ! dont_close ) {
            pipe->close_empty();
          }
          pipe_finished = true;
        }
      }
      cur = record_traits::get_empty();
    }
    // Destructor puts the tile back (if we have one) and closes the pipe.
    ~pipe_iterator() {
      try {
        if( ! pipe_finished ) {
          warn_if_err(ERR_INVALID_STR("finish must be called before ~pipe_iterator"));
        }
        finish();
      } catch(std::exception& e) {
        std::cerr << "Exception in ~pipe_iterator: " << e.what() << std::endl;
      }
    }

    size_t get_num_read() const {
      return num_read;
    }

    // *iter        Provides read access to the element
    const Record&
    operator* () const {
      // Always just returns the stored record.
      return cur;
    }

    // iter->member Provides read access to a member
    // Should work with *iter

    // ++iter       Steps forward, returning the new position
    pipe_iterator<Record>&
    operator++ () {
      //pos += cur.get_record_length(ctx);
      pos += record_traits::get_record_length(cur);
      // Get the next tile if we're at the end of this one.
      if( pos >= t.len ) {
        next_tile();
      }
      // Read a record if we have one.
      if( ! t.is_end() ) {
        //cur.decode(ctx, t.data+pos);
        record_traits::decode(cur, PTR_ADD(t.data,pos));
      } else {
        // We've reached EOF.
      }
      return *this;
    }

    // iter++       Steps forward, returning old position
    pipe_iterator<Record>
    operator++ (int) {
      // Copy the current iterator
      Record ret(*this);
      // Step forward
      operator++();
      // Return the old position.
      return ret;
    }

    // iter1==iter2 Returns whether two iterators are equal
    // Here we just check that the pipe, tile, and current position
    // are the same. 
    bool operator== (const pipe_iterator<Record>& x) const {
      // True if they're both at the end
      if( t.is_end() && x.t.is_end() ) return true;
      // True if they both have the same pipe
      return pipe == x.pipe;
    }
    // iter1!=iter2 Returns whether two iterators differ
    bool operator!= (const pipe_iterator<Record>& x) const {
      return !(operator==(x));
    }

    // TYPE(iter)   Copies iterator (copy constructor)
    // Default copy constructor should be O.K.

    read_pipe* get_pipe() {
      return pipe;
    }
};

template<typename Record>
class windowed_pipe_iterator
{
  read_pipe* const pipe;
  pipe_iterator<Record> read;
  std::deque<Record> window;
 public:
  // default constructor is empty.
  windowed_pipe_iterator() 
    : pipe(NULL),
      read(),
      window()
  {
  }

  windowed_pipe_iterator(read_pipe* pipe, size_t window_size)
    : pipe(pipe),
      read(pipe),
      window()
  {
    pipe_iterator<Record> end;

    // Get the first window_size characters
    for( size_t i = 0; read != end && i < window_size; i++ ) {
      Record r = *read;
      window.push_back(r);
      ++read;
    }
  }
  // Read an element!
  const Record&
  operator* () const {
    return window[0];
  }
  // Get an element from the window!
  const Record&
  operator[]( size_t i ) {
    if( EXTRA_CHECKS ) assert(0 <= i && i < window.size());
    return window[i];
  }
  // Advance the iterator!
  windowed_pipe_iterator<Record>&
  operator++ () {
    pipe_iterator<Record> end;

    // Move our sliding window forward
    window.pop_front();
    if( read != end ) {
      window.push_back(*read);
      ++read;
    }

    return *this;
  }
  size_t size() const {
    return window.size();
  }

  // iter1==iter2 Returns whether two iterators are equal
  // Can only compare to end -- check that they're both at end.
  bool operator== (const windowed_pipe_iterator<Record>& x) const {
    return size() == x.size();
  }
  // iter1!=iter2 Returns whether two iterators differ
  bool operator!= (const windowed_pipe_iterator<Record>& x) const {
    return size() != x.size();
  }
  void finish() {
    window.clear();
    read.finish();
  }
};

/* A thing with an interface like a pipe_back_inserter but that
   outputs to a whole bunch of pipes. Has the potential to store
   in 2 pipes for each record (in order to handle overlap).

   Note -- it is possible for the bin-getter to modify each record
   on the way.
 */
template<typename Record, typename BinGetter>
class distributing_back_inserter : public std::iterator<std::output_iterator_tag, Record> {
  private:
    BinGetter bg;
    std::vector<write_pipe*> * output;
    std::vector< pipe_back_inserter<Record>* > writers;
  public:
    distributing_back_inserter(BinGetter bg, std::vector<write_pipe*>* output)
    : bg(bg), output(output)
    {
      // Create writers...
      writers.resize(output->size());

      for(size_t i = 0; i < output->size(); i++ ) {
        writers[i] = new pipe_back_inserter<Record>((*output)[i]);
      }
    }
    ~distributing_back_inserter()
    {
      finish();
    }

    void finish() {
      // Finish all of the output pipes.
      for(size_t i = 0; i < output->size(); i++ ) {
        if( writers[i] ) {
          writers[i]->finish();
          delete writers[i];
          writers[i] = NULL;
        }
      }
    }

    void push_back(const Record& r) {
      // Append this value to the output.
      // but which bin?
      size_t bin;
      size_t alt_bin;
      bg.get_bin(r, &bin, &alt_bin);

      if( EXTRA_CHECKS ) {
        assert(0 <= bin && bin < output->size());
        assert(writers[bin]);
      }

      writers[bin]->push_back(r);

      if( bin != alt_bin ) {
        if( EXTRA_CHECKS ) {
          assert(0 <= alt_bin && alt_bin < output->size());
          assert(writers[alt_bin]);
        }
        writers[alt_bin]->push_back(r);
      }
    }
};

template<typename InRecord, typename OutRecord, typename Translator, typename BackInserter=pipe_back_inserter<OutRecord> >
class translating_back_inserter : public std::iterator<std::output_iterator_tag, InRecord> {
 private:
  Translator trans;
  BackInserter* writer;
 public:
  translating_back_inserter(Translator trans, BackInserter* writer)
    : trans(trans), writer(writer)
  {
  }
  // default destructor OK
  void finish() {
    writer->finish();
  }
  void push_back(const InRecord& r) {
    bool keep = true;
    OutRecord out = trans.translate(r, keep);
    if( keep ) writer->push_back( out );
  }
};

/* Append a record to a tile. Return true if there was room,
 * or false if there was no room (and the record was not appended).
 */
template<typename Record>
bool append_record(const Record& r, tile * t)
{
  typedef RecordTraits<Record> record_traits;
  //const size_t r_len = r.get_record_length(ctx);
  const size_t r_len = record_traits::get_record_length(r);
  if( t->len + r_len > t->max ) return false;
  //r.encode(ctx, t->data+t->len);
  record_traits::encode(r,PTR_ADD(t->data,t->len));
  t->len += r_len;
  return true;
}
template<typename Record>
bool append_record(const Record& r, pipe_back_inserter<Record> * writer)
{
  writer->push_back(r);
  return true;
}

class pipeline_node : public uncopyable {
  protected:
    bool is_running;
    pthread_t thread;
    const pipeline_node* parent;
    std::string name;
    bool print_timing;
    tinfo_t timing;
    virtual void run() = 0; // pure virtual run method.
  public:
    pipeline_node(const pipeline_node* parent=NULL, std::string name="", bool print_timing=false); // Creates a node, but doesn't start it.
    static void* pthread_main(void* x);
    void start(); // start the thread.
    void finish(); // wait for thread to complete, if it's running.
    void runhere(); // run in the current thread
                    // this is an optimization to replace {start();finish();}
    std::string get_name() const;
    bool get_print_timing() const;
    io_stats_t* get_io_stats() const;
    void add_current_io_stats() const;
    void print_times() const;
    // Destructor waits for thread to finish if it's running.
    virtual ~pipeline_node();
  private:
    void add_timing_to_parent();
};

// NODES - note that aio_read_pipe should be used instead...
// these remain for testing.
class file_reader_node : public pipeline_node {
  private:
    FILE* fp;
    // sends the data to this pipe..
    write_pipe& output;
    virtual void run();
  public:
    file_reader_node(FILE* fp, write_pipe& output)
      : fp(fp), output(output)
    { }
};

class file_writer_node : public pipeline_node {
  private:
    FILE* fp;
    // reads data from this pipe.
    read_pipe& input;
    virtual void run();
  public:
    file_writer_node(FILE* fp, read_pipe& input)
      : fp(fp), input(input)
    { }
};

class copy_node : public pipeline_node {
 private:
  read_pipe* input;
  write_pipe* output;
 public:
  uint64_t bytes_copied;
 private:
  virtual void run();
 public:
  copy_node(read_pipe* input, write_pipe* output);
};


#endif

