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

  femto/src/utils_cc/pipelining.cc
*/
/* A pipe has a single read end and a single write end.
   There can be several kinds of pipes:
   [same thread same process (memory)]
   different thread same process (memory w/locks)
   different process same machine (unix pipe)
   different machine
   
   All of these pipe implementations will share a common
       parent implementation.
       
   */

#include "pipelining.hh"
#include <cassert>
#include <cerrno>
#include <cstring>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <aio.h>
#include <sys/mman.h>
#include "page_utils.h"
#include "timing.h"
#include "error.h"
}

// Rounds a tile size to a page-aligned
// multiple of the passed number (which should be small).
size_t round_tile_size_to_page_multiple(size_t tile_size, size_t record_size)
{
  size_t page_size = get_page_size();
  size_t chunk = record_size*page_size;
  tile_size = (tile_size+chunk-1)/chunk;
  tile_size *= chunk;
  assert( tile_size % record_size == 0 );
  assert( tile_size % page_size == 0 );
  return tile_size;
}

////////////////////// PIPES /////////////////////////////

pipe::~pipe()
{
}

read_pipe::~read_pipe()
{
}

write_pipe::~write_pipe()
{
}

//////////////////////////////// MEMORY PIPE ///////////////////////////////

tile memory_pipe::next_tile()
{
  {
    pthread_held_mutex held_lock(&lock);

    tile ret;
    if( cur == len ) return ret;

    ret.data = PTR_ADD(data,cur);
    ret.len = tile_size;
    if( cur + ret.len > len ) ret.len = len - cur;
    ret.max = ret.len;
    cur += ret.len;
    return ret;
  }
}

memory_pipe::memory_pipe(void* data, size_t len, size_t tile_size)
  : lock(), data(data), len(len), cur(0), tile_size(tile_size)
{
}

memory_pipe::~memory_pipe()
{
}

size_t memory_pipe::get_tile_size()
{
  return tile_size;
}

size_t memory_pipe::get_num_tiles()
{
  //return (len+len-1) / tile_size;
  size_t max_size_t = (size_t) -1;
  assert( max_size_t > 0 );
  return max_size_t;
}

bool memory_pipe::is_thread_safe()
{
  return true;
}

const tile memory_pipe::get_full_tile()
{
  return next_tile();
}

void memory_pipe::put_empty_tile(const tile& t)
{
  // Do nothing!
}

void memory_pipe::close_empty()
{
  // Do nothing!
}

void memory_pipe::wait_full()
{
  // Do nothing!
}

tile memory_pipe::get_empty_tile()
{
  {
    pthread_held_mutex held_lock(&lock);

    tile ret;
    if( cur == len ) return ret;

    ret.data = PTR_ADD(data,cur);
    ret.len = tile_size;
    if( cur + ret.len > len ) ret.len = len - cur;
    ret.max = ret.len;
    ret.len = 0;
    return ret;
  }
}

void memory_pipe::put_full_tile(const tile& t)
{
  {
    pthread_held_mutex held_lock(&lock);
    cur += t.len;
  }
}

void memory_pipe::close_full()
{
  // Do nothing!
}

void memory_pipe::reset()
{
  cur = 0;
}

//////////////////////////////// BUFFERED PIPE ///////////////////////////////
const tile buffered_pipe::get_full_tile()
{
  //printf("Pipe %p getting full tile num=%zi\n", this, full_tiles.size());
  return full_tiles.pop();
}
void buffered_pipe::put_empty_tile(const tile& t)
{
  empty_tiles.push(t);
}
// indicate that no new data will be read.
void buffered_pipe::close_empty()
{
  empty_tiles.close();
}

// wait for at least one full tile to be placed into the pipe.
// Someone else might steal that..... but we can wait for it initially.
void buffered_pipe::wait_full()
{
  if( full_tiles.size() == 0 ) {
    full_tiles.wait();
  }
}

tile buffered_pipe::get_empty_tile()
{
  tile ret = empty_tiles.pop();
  ret.len = 0;
  return ret;
}

void buffered_pipe::put_full_tile(const tile& t)
{
  full_tiles.push(t);
}

// indicate that no new data will be produced -- no new full tiles.
void buffered_pipe::close_full()
{
  full_tiles.close();
}

// Allocates some number of tiles.
buffered_pipe::buffered_pipe(size_t tile_size, size_t num_tiles)
  : lock(), tile_size(tile_size), num_tiles(0),
    full_tiles(), empty_tiles()
{
  // Allocate and add tiles that were requested...
  add_tiles(num_tiles);
}

// Free the empty tiles.
void buffered_pipe::free_empty_tiles()
{
  // Clear out the empty tiles.
  while( 1 ) {
    tile t = empty_tiles.pop();
    if( t.is_end() ) break;
    // Free the tile.
    free_memory(t.data);
    lock.lock();
    num_tiles--;
    lock.unlock();
  }
}

// Allocate some new empty tiles.
void buffered_pipe::add_tiles(size_t num_new_tiles)
{
  lock.lock();

  try {
    // Now allocate and add num tiles to the empty end of the pipe.
    for( size_t i = 0; i < num_new_tiles; i++ ) {
      tile t(allocate_memory(tile_size), 0, tile_size);
      empty_tiles.push(t);
    }

    num_tiles += num_new_tiles;
  } catch( ... ) {
    lock.unlock_nothrow();
    throw;
  }

  lock.unlock();
}

ssize_t buffered_pipe::get_num_empty_tiles()
{
  return empty_tiles.size();
}

ssize_t buffered_pipe::get_num_full_tiles()
{
  return full_tiles.size();
}

// Destruct the tiles
buffered_pipe::~buffered_pipe() 
{
  try {
    // Close the pipes. This awakens any waiters...
    full_tiles.close();
    empty_tiles.close();

    free_empty_tiles();

    // Clear out the full tiles.
    while( 1 ) {
      tile t = full_tiles.pop();
      if( t.is_end() ) break;
      // Free the tile.
      free_memory(t.data);
    }
  } catch(std::exception& e) {
    std::cerr << "Exception in ~buffered_pipe: " << e.what() << std::endl;
  }
}

size_t buffered_pipe::get_tile_size()
{
  size_t ret;
  lock.lock();
  try {
    ret = tile_size;
  } catch(...) {
    lock.unlock_nothrow();
    throw;
  }
  lock.unlock();
  return ret;
}

size_t buffered_pipe::get_num_tiles()
{
  size_t ret;
  lock.lock();
  try {
    ret = num_tiles;
  } catch(...) {
    lock.unlock_nothrow();
    throw;
  }
  lock.unlock();
  return ret;
}

bool buffered_pipe::is_thread_safe() {
  return true;
}

//////////////////////////////// SERIAL BUFFERED PIPE ///////////////////////
const tile serial_buffered_pipe::get_full_tile()
{
  // Wait until the full tiles are closed.
  tiles.wait_close();
  return tiles.pop();
}
void serial_buffered_pipe::put_empty_tile(const tile& t)
{
  free_tile(t);
}
// indicate that no new data will be read.
void serial_buffered_pipe::close_empty()
{
  // do nothing.
}

// wait for at least one full tile to be placed into the pipe.
// Someone else might steal that..... but we can wait for it initially.
void serial_buffered_pipe::wait_full()
{
  // Wait for the pipe to be closed.
  tiles.wait_close();
}

tile serial_buffered_pipe::get_empty_tile()
{
  return make_tile();
}

void serial_buffered_pipe::put_full_tile(const tile& t)
{
  tiles.push(t);
}

// indicate that no new data will be produced -- no new full tiles.
void serial_buffered_pipe::close_full()
{
  tiles.close();
}

// Allocates some number of tiles.
serial_buffered_pipe::serial_buffered_pipe(size_t tile_size, size_t num_tiles)
  : lock(), tile_size(tile_size), num_tiles(num_tiles),
    num_made_tiles(0), num_freed_tiles(0),
    tiles()
{
}

// Free the empty tiles.
void serial_buffered_pipe::free_empty_tiles()
{
  // no empty tiles..
}

tile serial_buffered_pipe::make_tile()
{
  tile ret;

  lock.lock();

  try {
    tile t(allocate_memory(tile_size), 0, tile_size);
    ret = t;
    num_made_tiles++;

  } catch( ... ) {
    lock.unlock_nothrow();
    throw;
  }

  lock.unlock();

  return ret;
}

void serial_buffered_pipe::free_tile(tile t)
{
  lock.lock();

  try {
    free_memory(t.data);
    num_freed_tiles++;
  } catch( ... ) {
    lock.unlock_nothrow();
    throw;
  }

  lock.unlock();
}

// Allocate some new empty tiles.
void serial_buffered_pipe::add_tiles(size_t num_new_tiles)
{
  lock.lock();

  num_tiles += num_new_tiles;

  lock.unlock();
}

// Destruct the tiles
serial_buffered_pipe::~serial_buffered_pipe() 
{
  try {
    // Close the pipes. This awakens any waiters...
    tiles.close();

    // Clear out the full tiles.
    while( 1 ) {
      tile t = tiles.pop();
      if( t.is_end() ) break;
      // Free the tile.
      free_tile(t);
    }
  } catch(std::exception& e) {
    std::cerr << "Exception in ~serial_buffered_pipe: " << e.what() << std::endl;
  }

  //assert(num_made_tiles == num_freed_tiles);
}

size_t serial_buffered_pipe::get_tile_size()
{
  size_t ret;
  lock.lock();
  try {
    ret = tile_size;
  } catch(...) {
    lock.unlock_nothrow();
    throw;
  }
  lock.unlock();
  return ret;
}

size_t serial_buffered_pipe::get_num_tiles()
{
  size_t ret;
  lock.lock();
  try {
    ret = num_tiles;
  } catch(...) {
    lock.unlock_nothrow();
    throw;
  }
  lock.unlock();
  return ret;
}

bool serial_buffered_pipe::is_thread_safe() {
  return true;
}

serial_buffered_pipe* create_pipe_for_data(size_t len, const void* data)
{
  serial_buffered_pipe* ret = new serial_buffered_pipe(len, 1);
  tile t = ret->get_empty_tile();
  t.len = len;
  memcpy(t.data, data, len);
  ret->put_full_tile(t);
  ret->close_full();
  return ret;
}

/////////////////////////////// SINGLE TILE PIPE ////////////////////////
single_tile_pipe::single_tile_pipe(tile t, read_pipe* put_back)
  : put_back(put_back), t(t), done(false)
{
}

// Get the maximum size of each tile
size_t single_tile_pipe::get_tile_size()
{
  return t.max;
}
// Get the number of tiles supported by this pipe.
// Note that this may change while the pipe exists; this checks it at a moment.
size_t single_tile_pipe::get_num_tiles()
{
  return 1;
}

// Reader functions
const tile single_tile_pipe::get_full_tile()
{
  if( done ) {
    tile empty;
    return empty;
  } else {
    return t;
  }
}
void single_tile_pipe::put_empty_tile(const tile& t)
{
  done = true;
  if( put_back ) put_back->put_empty_tile(t);
}
// indicate that no new data will be read -- no new empty tiles added.
void single_tile_pipe::close_empty()
{
}
// wait for at least one full tile to be placed into the pipe.
// That tile isn't necessarily present once it's been added...
void single_tile_pipe::wait_full()
{
}

// Free empty tiles to reclaim space. Only frees those empty at that moment.
void single_tile_pipe::free_empty_tiles()
{
}

// Add new empty tiles to a pipe..
void single_tile_pipe::add_tiles(size_t num_new_tiles)
{
}

bool single_tile_pipe::is_thread_safe() {
  return false;
}


//////////////////////////// PIPELINE NODE ///////////////////////

pipeline_node::pipeline_node(const pipeline_node* parent, std::string name, bool print_timing)
  : is_running(false), parent(parent), name(name), print_timing(print_timing)
{
  // clear out the timing!
  memset(&timing, 0, sizeof(tinfo_t));
}

void* pipeline_node::pthread_main(void* x)
{
  pipeline_node* node = static_cast<pipeline_node*>(x);
  try {
    if( node->print_timing ) {
      std::cout << "# Starting " << node->get_name() << std::endl;
    }
    start_clock_r(&node->timing);

    node->run();

    stop_clock_r(&node->timing);

    if( node->print_timing ) {
      std::string name = node->get_name();
      std::cout << "# Finished " << node->get_name() << std::endl;
      print_timings_r(&node->timing, name.c_str(), 1, "#");
    }
  } catch (std::exception& e) {
    std::cerr << "EXCEPTION IN PIPELINE NODE " << node->get_name() << ": " << e.what() << std::endl;
    exit(-1);
  }
  return NULL;
}

void pipeline_node::start()
{
  int rc;
  int tries;
  int max_tries = 10;

  for( tries=0; 1; tries++ ) {
    rc = pthread_create(&thread, NULL, pthread_main, this);
    if( rc ) {
      if( rc == EAGAIN && tries < max_tries ) {
        // Try sleeping for a moment.
        warn_if_err(ERR_PTHREADS("Could not create thread -- sleeping", rc));
        sleep(10);
      } else {
        is_running = false;
        throw error(ERR_PTHREADS("Could not create thread", rc));
      }
    } else {
      is_running = true;
      break; // success; no need to try again
    }
  }
}

void pipeline_node::add_timing_to_parent()
{
  // If we have a parent node, add in the timing.
  // Moved this to finish() to prevent the need for locking.
  if( parent ) {
    pipeline_node * non_const_parent = const_cast<pipeline_node*>(parent);
    add_timing_r(&non_const_parent->timing, &timing);
  }
}

void pipeline_node::finish()
{
  int rc;
  void* ret;

  // Do nothing if the thread isn't running.
  if( ! is_running ) return;

  rc = pthread_join(thread, &ret);
  if( rc ) throw error(ERR_PTHREADS("Could not join", rc));

  add_timing_to_parent();

  is_running = false;
}

void pipeline_node::runhere()
{
  is_running = true;

  pthread_main(this);

  add_timing_to_parent();

  is_running = false;
}

std::string pipeline_node::get_name() const
{
  return name;
}

bool pipeline_node::get_print_timing() const
{
  return print_timing;
}

io_stats_t* pipeline_node::get_io_stats() const
{
  // casts away constness.
  return (io_stats_t*) &timing.io_stats;
}

void pipeline_node::add_current_io_stats() const
{
  // casts away constness.
  add_io_stats( get_io_stats(), get_current_io_stats() );
}

void pipeline_node::print_times() const
{
  std::string name = get_name();
  // casts away constness.
  print_timings_r((tinfo_t*) &timing, name.c_str(), 1, "#");
}

pipeline_node::~pipeline_node()
{
  // Wait for the thread to finish,
  // because we can't destroy the thread-management
  // object until it is!
  try {
    finish();
  } catch(std::exception& e) {
    std::cerr << "Exception in ~pipeline_node: " << e.what() << std::endl;
  }
}

void file_reader_node::run()
{
  tile t;

  //fprintf(stderr, "Reader Main\n");

  try {
    while( !feof(fp) ) {
      //fprintf(stderr, "Reader Loop\n");
      // step 1: get a empty buffer to fill from the pipe.
      t = output.get_empty_tile();
      //fprintf(stderr, "Reader Got Tile %p\n", tile);

      // if there's no tile, we're done!
      if( t.is_end() ) break;

      // step 2: fill the buffer by reading from the file.
      t.len = fread(t.data, 1, t.max, fp);

      // step 3: put the full buffer into the pipe
      output.put_full_tile(t);
    }
  } catch ( ... ) {
    // If there's an exception, just record that we're done
    // so that the other threads will continue.
    output.close_full();
    throw;
  }

  // If we finished normally, just close the output.
  output.close_full();
}


void file_writer_node::run()
{
  tile t;

  //fprintf(stderr, "Writer Main\n");
  //fflush(stdout);

  try {
    while( 1 ) {
      //fprintf(stderr, "Writer Loop\n");
      // step 1: get a full buffer to write to the file
      t = input.get_full_tile();
      //fprintf(stderr, "Writer Got Tile %p\n", tile);

      // tile is NULL when we're done!
      if( t.is_end() ) break;

      //fprintf(stderr, "Writer writing %i bytes\n", (int) tile->len);
      // step 2: write the buffer to the file.
      size_t wrote = fwrite(t.data, 1, t.len, fp);
      if( wrote != (size_t) t.len ) {
        throw error(ERR_IO_UNK);
      }

      // step 3: put the empty buffer into the pipe
      input.put_empty_tile(t);
    }
  } catch(...) {
    input.close_empty();
  }

  input.close_empty();
}

copy_node::copy_node(read_pipe* input, write_pipe* output)
    : input(input), output(output), bytes_copied(0)
{
  assert(input->get_tile_size() == output->get_tile_size());
}

void copy_node::run()
{
  while( 1 ) {
    tile t_in = input->get_full_tile();
    if( !t_in.data ) break;
    tile t_out = output->get_empty_tile();
    assert(t_out.data);
    memcpy(t_out.data, t_in.data, t_in.len);
    t_out.len = t_in.len;
    bytes_copied += t_out.len;
    input->put_empty_tile(t_in);
    output->put_full_tile(t_out);
  }
  input->close_empty();
  output->close_full();
}

