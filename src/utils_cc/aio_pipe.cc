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

  femto/src/utils_cc/aio_pipe.cc
*/
#include "aio_pipe.hh"

#include <cerrno>
#include <cstring>

extern "C" {
#include "page_utils.h"
#include <sys/types.h>
#include <fcntl.h>
}


/////////////////////////// AIO PIPES //////////////////////
// This is the portable, POSIX thing to do, but on linux it just ends
// up doing pread by threads...
// The alternative for linux is io_submit() and libaio, which needs O_DIRECT..
// Possibly compiling with -lposix-aio we can have support..


aio_manager::aio_manager(file_pipe_context ctx_in, bool write)
  : ctx(ctx_in),
    allocated(), pending(), outside(), closed(false)
{
  memset(&memory,0,sizeof(memory));
  ctx.create = write;
  ctx.write = write;
  ctx.open_file_if_needed();
  allocate_tiles();
}

void aio_manager::allocate_tiles()
{
  // Allocate memory for num_tiles.
  memory = allocate_pages(ctx.io_group_size*ctx.num_io_groups);
  //memory = static_cast<unsigned char*>(malloc(ctx.io_group_size*ctx.num_io_groups));
  if( ! memory.data ) {
    throw error(ERR_MEM);
  }

  unsigned char* memptr = static_cast<unsigned char*>(memory.data);

  // Allocate num_io_groups control blocks for aio..
  for( size_t i = 0; i < ctx.num_io_groups; i++ ) {
    struct aiocb* cb;
    // Allocate the control block.
    cb = new struct aiocb;
    // Zero it out.
    memset(cb, 0, sizeof(struct aiocb));
    // Put the right buffer.
    cb->aio_buf = static_cast<void*>(memptr + ctx.io_group_size*i);
    // OK, now we're happy.
    allocated.push_back(cb);
  }
}


void aio_manager::flush_and_close()
{
  // Wait for any final IOs.
  while( pending.size() > 0 ) {
    struct aiocb* cb = pending.front();
    pending.pop_front();
    while( 1 ) {
      int rc = aio_suspend(&cb, 1, NULL);
      if( rc == -1 && errno == EAGAIN ) continue;
      if( rc == -1 && errno == EINTR ) continue;
      if( rc ) {
        warn_if_err(ERR_IO_STR_OBJ("Error in aio_suspend", ctx.fname.c_str()));
      } else {
        break;
      }
    }
    // Move it to outside.
    outside.push_back(cb);
  }

  // close the file if possible.
  ctx.close_file_if_needed();

  closed = true;
}

aio_manager::~aio_manager() 
{
  // Wait for pending operations to complete.
  flush_and_close();

  while( outside.size() > 0 ) {
    struct aiocb* cb = outside.back();
    outside.pop_back();
    // Move it to allocated.
    allocated.push_back(cb);
  }

  assert(allocated.size() == ctx.num_io_groups);

  // Free what's allocated.
  while( allocated.size() > 0 ) {
    struct aiocb* cb = allocated.back();
    allocated.pop_back();
    // Free the control block
    delete cb;
  }

  // Free the buffers
  free_pages(memory);
}

aio_read_pipe::aio_read_pipe(file_pipe_context ctx)
  : m(ctx, false)
{
}

void aio_read_pipe::make_requests()
{
  // Don't try to make any requests if there's none in allocated.
  if( m.allocated.empty() ) return;

  size_t nr; // number of requests to make
  size_t start; // where to start in allocated.
  bool not_aligned = false;

  {
    uint64_t cur = m.ctx.cur;
    for( nr = 0;
         nr < m.allocated.size() &&
         (m.ctx.len == PIPE_UNTIL_END || cur < m.ctx.len);
         nr++ ) {
      cur += m.ctx.io_group_size;
    }
    start = m.allocated.size() - nr;
  }

  assert(start+nr <= m.allocated.size());

  // Make the requests in one system call! Weee!
  // (OK... this probably doesn't really use 
  // one system call, but it could..)
  for( nr = 0;
       nr < m.allocated.size() &&
       (m.ctx.len == PIPE_UNTIL_END || m.ctx.cur < m.ctx.len);
       nr++ ) {
    struct aiocb* cb = m.allocated[start + nr];
    // it's volatile just because cb->aio_buf is volatile.
    volatile void* buf = cb->aio_buf;

    //printf("Got from allocated  %p\n", cb);
    // Set up the control buffer appropriately.
    memset(cb, 0, sizeof(struct aiocb));
    // Set the buf.
    cb->aio_buf = buf;
    // Set the length
    cb->aio_nbytes = m.ctx.io_group_size;
    // Set the file descriptor
    cb->aio_fildes = m.ctx.fd;
    // We always read.
    cb->aio_lio_opcode = LIO_READ;
    // Set the offset.
    cb->aio_offset = m.ctx.start + m.ctx.cur;
    uint64_t overflow_check = cb->aio_offset;
    assert( overflow_check == m.ctx.start + m.ctx.cur);
    // We'll wait, so set no signal.
    cb->aio_sigevent.sigev_notify = SIGEV_NONE;
    // Increment the current.
    m.ctx.cur += m.ctx.io_group_size;
    if( m.ctx.o_direct ) {
      // Check the size and alignment for page boundaries.
      if( ((size_t)cb->aio_buf) & m.ctx.page_size_mask ) not_aligned = true;
      if( cb->aio_nbytes & m.ctx.page_size_mask ) not_aligned = true;
      if( cb->aio_offset & m.ctx.page_size_mask ) not_aligned = true;
    }
  }

  if( nr == 0 ) return;

  if( m.ctx.o_direct ) {
    if( not_aligned ) m.ctx.clear_o_direct();
  }

  // Take everything in the vector and start requests
  // If lio_listio is supported, we could use the following call...
  int rc = lio_listio( LIO_NOWAIT, &m.allocated[start], nr, NULL );
  if( rc ) {
    if( errno == EIO ) {
      // Check individual requests.
      for( size_t i = 0; i < nr; i++ ) {
        struct aiocb* cb = m.allocated[start + nr];
        int er = aio_error(cb);
        if( er ) {
          if( er == EINPROGRESS || er == ECANCELED ) {
            // don't care
          } else {
            throw error(ERR_IO_STR_OBJ_NUM("lio_listio failed with EIO; this error for particular request", m.ctx.fname.c_str(),er));
          }
        }
      }
    }
    throw error(ERR_IO_STR_OBJ("lio_listio failed", m.ctx.fname.c_str()));
  }
  /*If lio_listio is not supported:
  for( size_t i = 0; i < nr; i++ ) {
    int rc = aio_read(m.allocated[i]);
    if( rc ) throw error(ERR_IO_STR_OBJ("aio_read failed", m.fname.c_str()));
  }
  */

  // Put all of those into pending!
  for( size_t i = 0; i < nr; i++ ) {
    m.pending.push_back(m.allocated[start + i]);
  }

  // Now remove the last nr things from allocated.
  // Remove everything from allocated now that they're in pending.
  m.allocated.resize(start);
}

const tile aio_read_pipe::get_full_tile()
{
  struct aiocb* cb = NULL;
  {
    pthread_held_mutex held_lock(&m.lock);

    assert( ! (m.pending.empty() && m.allocated.empty()) );

    assert( ! m.closed );

    // Make requests until we've filled our pending queue
    make_requests();

    // Get the first thing out of the pending queue and wait for it
    // ... if there is anything in the pending queue...
    // if not, we return that we're done.
    if( m.pending.empty() ) {
      // Return an empty tile. We're done.
      tile ret;
      return ret;
    }

    //printf("Got from pending: %p\n", cb);
    // Wait for that request to be completed.
    
    cb = m.pending.front();
    m.pending.pop_front();
  }

  while( 1 ) {
    int rc = aio_suspend(&cb, 1, NULL);
    if( rc == -1 && errno == EAGAIN ) continue;
    if( rc == -1 && errno == EINTR ) continue;
    if( rc ) {
      throw error(ERR_IO_STR_OBJ("Error in aio_suspend", m.ctx.fname.c_str()));
    } else {
      break;
    }
  }

  {
    pthread_held_mutex held_lock(&m.lock);

    // Put that request in outside, where we will wait
    // for the empty tiles to come back.
    // We can't queue a new request on this memory region
    // until the empty tile comes back.
    //printf("Putting on outside: %p\n", cb);
    m.outside.push_back(cb);

    // Check the return value of the read operation to get the number
    // of bytes read.
    ssize_t num_read = aio_return(cb);
    if( num_read < 0 ) {
      throw error(ERR_IO_STR_OBJ_NUM("Error from aio read", m.ctx.fname.c_str(), aio_error(cb)));
    }


    // Check that we havn't read past the end.
    // length-to-read - (cur start of this block)
    if( m.ctx.len != PIPE_UNTIL_END ) {
      int64_t max_read = m.ctx.len - (cb->aio_offset - m.ctx.start);
      if( num_read > max_read ) {
        num_read = max_read;
      }
    }


    // Check that we're not at the end of the file.
    // Read returns 0 on EOF.
    // Our computation might also return 0 or negative..
    if( num_read <= 0 ) {
      // Return an empty tile.
      tile ret;
      return ret;
    }
    
    // Update the statistics.
    if( m.ctx.io_stats ) record_io(m.ctx.io_stats, IO_READ, num_read);

    // Set the return tile appropriately.
    // We're casting away volatile - is that a problem?
    tile ret(const_cast<void*>(cb->aio_buf), num_read, m.ctx.io_group_size);

    // Allow the fixer to fix ret.len if appropriate.
    //m.ctx.group_fixer->on_read(ret);
    return ret;
  }
}

void aio_read_pipe::put_empty_tile(const tile& t)
{
  {
    pthread_held_mutex held_lock(&m.lock);
    assert( ! m.closed );
    // We need to put t back into a request.
    // 2- just stomp on what's in the outside queue, replacing the
    //    pointer to memory with what we get from the tile.
    //    Note that if we're changing the tile data pointer
    //    to hide a length... this'll be trouble. So store the lengths
    //    at the end of the tiles.
    struct aiocb* cb = m.outside.back();
    //printf("Got from outside: %p\n", cb);
    m.outside.pop_back();

    // Fix the tile before re-using it.
    //m.ctx.group_fixer->before_empty(const_cast<tile&>(t));

    // Set the control-block buffer to what we get from the tile.
    cb->aio_buf = t.data;

    // Put it back into the allocated queue, where we can make requests with it.
    m.allocated.push_back(cb);

    // Make requests until we've filled our pending queue.
    make_requests();
  }
}

void aio_read_pipe::close_empty()
{
  {
    pthread_held_mutex held_lock(&m.lock);
    assert( ! m.closed );
    // Finish any requests we have..
    m.flush_and_close();
  }
}

void aio_read_pipe::wait_full()
{
  {
    pthread_held_mutex held_lock(&m.lock);
    assert( ! m.closed );
    // Do nothing.
  }
}

size_t aio_read_pipe::get_tile_size()
{
  return m.ctx.io_group_size;
}

size_t aio_read_pipe::get_num_tiles()
{
  return m.ctx.num_io_groups;
}

bool aio_read_pipe::is_thread_safe()
{
  return true;
}

void aio_read_pipe::assert_closed_empty()
{
  assert(m.closed);
}


aio_write_pipe::aio_write_pipe(file_pipe_context ctx)
  : m(ctx, true)
{
}

void aio_write_pipe::complete_write(struct aiocb* cb)
{
  //printf("Got from pending: %p\n", cb);
  // Wait for that write to be completed.
  while( 1 ) {
    int rc = aio_suspend(&cb, 1, NULL);
    if( rc == -1 && errno == EAGAIN ) continue;
    if( rc == -1 && errno == EINTR ) continue;
    if( rc ) {
      throw error(ERR_IO_STR_OBJ("Error in aio_suspend", m.ctx.fname.c_str()));
    } else {
      break;
    }
  }
  // Check the return value of the write operation to get the number
  // of bytes written.
  ssize_t num_written = aio_return(cb);
  if( num_written < 0 ) {
    throw error(ERR_IO_STR_OBJ_NUM("Error from aio write", m.ctx.fname.c_str(), aio_error(cb)));
  }
  if( (size_t) num_written != cb->aio_nbytes ) {
    throw error(ERR_IO_STR_OBJ_NUM("Error from aio write - short write", m.ctx.fname.c_str(), aio_error(cb)));
  }
}

tile aio_write_pipe::get_empty_tile()
{
  struct aiocb* cb = NULL;
  bool suspend;

  {
    pthread_held_mutex held_lock(&m.lock);

    assert( ! m.closed );
    // If we're past that point, just return an empty tile to say EOF..
    if( m.ctx.len != PIPE_UNTIL_END && m.ctx.cur >= m.ctx.len ) {
      tile ret; // empty tile to say EOF
      return ret;
    }

    // If there's a tile in allocated, use that one.
    if( ! m.allocated.empty() ) { 
      // Get an empty tile from the allocated tiles.
      cb = m.allocated.back();
      m.allocated.pop_back();
      suspend = false;
    } else {
      // Reap a pending write.
      assert(! m.pending.empty() );
      cb = m.pending.front();
      m.pending.pop_front();
      suspend = true;
    }
  }

  if( suspend ) {
    complete_write(cb);
  }


  {
    pthread_held_mutex held_lock(&m.lock);

    // Put it in outside, while we wait for the full tile to come back
    m.outside.push_back(cb);

    // Set the return value appropriately.
    // We're casting away volatile here.
    tile ret(const_cast<void*>(cb->aio_buf), 0, m.ctx.io_group_size);
    //printf("Getting empty tile %p outside size is %i\n", ret.data, m.outside.size());

    // Allow fixer to fix ret.max if appropriate.
    //m.ctx.group_fixer->before_fill(ret);

    return ret;
  }
}

void aio_write_pipe::put_full_tile(const tile& t)
{
  {
    pthread_held_mutex held_lock(&m.lock);

    assert( ! m.closed );
    // Allow the fixer to fix the tile to store a length in there..
    // Of course it needs to be able to modify the tile...
    //m.ctx.group_fixer->on_write(const_cast<tile&>(t));

    //printf("Putting full tile %p %zi %zi\n", t.data, t.len, t.max);
    assert(!m.outside.empty());

    // We need to put t back into a request.
    // 2- just stomp on what's in the outside queue, replacing the
    //    pointer to memory with what we get from the tile.
    //    Note that if we're changing the tile data pointer
    //    to hide a length... this'll be trouble. So store the lengths
    //    at the end of the tiles.
    bool not_aligned = false;
    struct aiocb* cb = m.outside.back();
      //printf("Got from outside: %p\n", cb);
    m.outside.pop_back();

    // Zero out the control block.
    memset(cb, 0, sizeof(struct aiocb));
    // Set the buf.
    cb->aio_buf = t.data;
    // Set the length.
    cb->aio_nbytes = t.len;
    // Set the file descriptor
    cb->aio_fildes = m.ctx.fd;
    // We always write.
    cb->aio_lio_opcode = LIO_WRITE;
    // Set the offset.
    cb->aio_offset = m.ctx.start + m.ctx.cur;
    // We'll wait, so set no signal.
    cb->aio_sigevent.sigev_notify = SIGEV_NONE;
    // Add to cur.
    m.ctx.cur += t.len;
    // Check the size and alignment for page boundaries.
    if( m.ctx.o_direct ) {
      if( ((size_t)cb->aio_buf) & m.ctx.page_size_mask ) not_aligned = true;
      if( cb->aio_nbytes & m.ctx.page_size_mask ) not_aligned = true;
      if( cb->aio_offset & m.ctx.page_size_mask ) not_aligned = true;

      if( not_aligned ) m.ctx.clear_o_direct();
    }

    // Write it.
    int rc = aio_write(cb);

    // Put it into the pending queue.
    m.pending.push_back(cb);

    if( rc ) throw error(ERR_IO_STR("Could not aio_write"));
    // Update the statistics.
    if( m.ctx.io_stats ) record_io(m.ctx.io_stats, IO_WRITE, t.len);

  }
}

/*void aio_write_pipe::close_full(const tile& t)
{
  {
    pthread_held_mutex held_lock(&m.lock);
    assert( ! m.closed );
    m.ctx.group_fixer->on_write(const_cast<tile&>(t));

    // Just put the tile back in the allocated queue.
    struct aiocb* cb = m.outside.back();
    m.outside.pop_back();

    // Set the control-block buffer to what we get from the tile.
    cb->aio_buf = t.data;
    cb->aio_nbytes = 0;

    // Put it into the allocated queue.
    m.allocated.push_back(cb);

    close_full();
  }
}
*/

void aio_write_pipe::close_full()
{
  {
    pthread_held_mutex held_lock(&m.lock);
    assert( ! m.closed );

    struct aiocb* cb = NULL;

    // eat up any pending requests.
    while( ! m.pending.empty() ) {
      cb = m.pending.front();
      m.pending.pop_front();

      complete_write(cb);

      m.allocated.push_back(cb);
    }

    // Wait for requests to finish.
    m.flush_and_close();
  }
}

size_t aio_write_pipe::get_tile_size()
{
  return m.ctx.io_group_size;
}

size_t aio_write_pipe::get_num_tiles()
{
  return m.ctx.num_io_groups;
}

bool aio_write_pipe::is_thread_safe()
{
  return true;
}

void aio_write_pipe::assert_closed_full()
{
  assert(m.closed);
}


