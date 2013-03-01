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

  femto/src/utils_cc/kaio_pipe.cc
*/


#include "kaio_pipe.hh"

#include <cerrno>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
//#include <aio.h>
#include <sys/mman.h>
#include "page_utils.h"
#include "error.h"
}


/////////////////////////// KAIO PIPES //////////////////////
// Pipes for read/write a file using io_submit() linux kernel calls.
kaio_manager::kaio_manager(file_pipe_context ctx_in, bool write)
  : ctx(ctx_in),
    req(0), done_req(0),
    allocated(), events(ctx.num_io_groups), outside()
{
  memset(&memory,0,sizeof(memory));
  ctx.create = write;
  ctx.write = write;
  ctx.open_file_if_needed();
  allocate_tiles();
}

void kaio_manager::allocate_tiles()
{
  // Check that number of tiles fits in an unsigned.
  if( ctx.num_io_groups > 0x7fffffff) {
    throw error(ERR_INVALID_STR("too many io_groups"));
  }
  // Check that io_group_size is page-aligned.
  if( ctx.io_group_size & (get_page_size() - 1) ) {
    throw error(ERR_INVALID_STR("io_group_size must be page aligned"));
  }

  // Reserve space for the maximum number of allocated/outside tiles.
  allocated.reserve(ctx.num_io_groups);
  outside.reserve(ctx.num_io_groups);

  // Call io_setup.
  memset(&io_ctx, 0, sizeof(io_context_t));
  int rc = io_setup(ctx.num_io_groups, &io_ctx);
  if( rc != 0 ) throw error(ERR_IO_STR("io_setup failed"));

  // Allocate memory for num_io_groups.
  memory = allocate_pages(ctx.io_group_size*ctx.num_io_groups);
  if( ! memory.data ) {
    throw error(ERR_MEM);
  }

  unsigned char* memptr = static_cast<unsigned char*>(memory.data);

  // Allocate num_io_groups.
  for( size_t i = 0; i < ctx.num_io_groups; i++ ) {
    struct iocb* cb;
    void* buf;
    // Allocate the control block.
    cb = new struct iocb;
    buf = static_cast<void*>(memptr + ctx.io_group_size*i);
    if( ctx.write ) {
      // Prep it for a write -- do the basic preparation.
      io_prep_pwrite(cb, ctx.fd, buf, ctx.io_group_size, 0);
    } else {
      // Prep it for a read -- do the basic preparation.
      io_prep_pread(cb, ctx.fd, buf, ctx.io_group_size, 0);
    }
    // OK, now we're happy.
    allocated.push_back(cb);
  }
}


void kaio_manager::flush_and_close()
{
  // Wait for any final IOs.
  // This should clear out completed.
  while(req < done_req) {
    long ret = io_getevents(io_ctx, 1, events.size(), &events[0], NULL);
    if( ret <= 0 ) {
      warn_if_err(ERR_IO_STR_OBJ("could not io_getevents", ctx.fname.c_str()));
    }

    for( long i = 0; i < ret; i++ ) {
      struct io_event *event = &events[i];
      struct iocb* cb = event->obj;
      // If there was an error in the event, bail out.
      if( event->res2 != 0 ) {
        warn_if_err(ERR_IO_STR_OBJ("Error returned in io_getevents", ctx.fname.c_str()));
      }
      outside.push_back(cb);
    }

    // Move on..
    done_req += ret;
  }

  ctx.close_file_if_needed();
}

kaio_manager::~kaio_manager() 
{
  int rc;

  // Wait for anything pending and close the file if necessary.
  flush_and_close();

  // Clear out outside.
  while( outside.size() > 0 ) {
    struct iocb* cb = outside.back();
    outside.pop_back();
    // Move it to allocated.
    allocated.push_back(cb);
  }

  // Free what's allocated.
  while( allocated.size() > 0 ) {
    struct iocb* cb = allocated.back();
    allocated.pop_back();
    // Free the control block
    delete cb;
  }

  // Destroy the io context.
  rc = io_destroy(io_ctx);
  if( rc ) warn_if_err(ERR_IO_STR("Could not io_destroy"));
  
  // Free the buffers
  free_pages(memory);
}

kaio_read_pipe::kaio_read_pipe(file_pipe_context ctx)
  : m(ctx, false), completed()
{
}

void kaio_read_pipe::make_requests()
{
  // Don't try to make any requests if there's none in allocated.
  if( m.allocated.empty() ) return;

  size_t nr;
  size_t start;

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
  for( nr = 0;
       nr < m.allocated.size() &&
       (m.ctx.len == PIPE_UNTIL_END || m.ctx.cur < m.ctx.len);
       nr++ ) {
    struct iocb* cb = m.allocated[start+nr];
    void* buf = cb->u.c.buf;

    // Set up the control buffer appropriately.
    io_prep_pread(cb, m.ctx.fd, buf, m.ctx.io_group_size, m.ctx.start + m.ctx.cur);
    assert((uint64_t) cb->u.c.offset == m.ctx.start + m.ctx.cur); // no overflow!

    // Save off the "current request number"
    cb->data = (void*) (m.req+nr); // the current request number.
    m.ctx.cur += m.ctx.io_group_size;
    //printf("KAIO read making request %p (%zi)\n", cb, m.req+nr);
  }

  if( nr == 0 ) return;

  // Take everything in the vector and start requests
  int rc = io_submit(m.io_ctx, nr, &m.allocated[start]);
  if( rc != (int) nr ) throw error(ERR_IO_STR_OBJ("io_submit failed", m.ctx.fname.c_str()));

  // Update the current request number.
  m.req += nr;

  // We don't save copies of these requests - we'll get the pointers to them
  // back from io_getevents.

  // Remove everything from allocated now that they've been submitted.
  m.allocated.resize(start);
}

// Assumes lock is already held!
// Waits for some finished IO operations
// Puts the finished IO operations in the right place in 
// the completed queue. Once we have the first outstanding request,
// return it.
struct iocb* kaio_read_pipe::wait_first()
{

  while ( completed.empty() || ! completed[0] ) {
    long ret = io_getevents(m.io_ctx, 1, m.events.size(), &m.events[0], NULL);
    if( ret <= 0 ) {
      throw error(ERR_IO_STR_OBJ("could not io_getevents", m.ctx.fname.c_str()));
    }

    // Ok. we got ret events. 
    for( long i = 0; i < ret; i++ ) {
      struct io_event *event = &m.events[i];
      struct iocb* cb = event->obj;
      size_t x = (size_t) event->data;
      //printf("KAIO read got back request %p (%zi) at (%zi)\n", cb, x, m.done_req);
      // Ok.. This event goes in completed[x-done_req].
      size_t idx = x - m.done_req;
      // If there was an error in the event, bail out.
      if( event->res2 != 0 ) {
        throw error(ERR_IO_STR_OBJ("Error returned in io_getevents", m.ctx.fname.c_str()));
      }
      size_t num_read = event->res;

      // Now if we've read past the end, set nbytes appropriately.
      // Check that we havn't read past the end.
      // length-to-read - (cur start of this block)
      if( m.ctx.len != PIPE_UNTIL_END ) {
        int64_t max_read = m.ctx.len - (x*m.ctx.io_group_size - m.ctx.start);
        if( (int64_t) num_read > max_read ) {
          num_read = max_read;
        }
      }

      // Set the number of bytes read to the result from the event.
      cb->u.c.nbytes = num_read;

      // Make sure that there's room for idx in the deque.
      if( idx >= completed.size() ) {
        completed.resize(idx+1);
      }
      // Save cb in the appropriate spot in the completed queue.
      completed[idx] = cb;
    }
  }

  // Now return the first cb, popping it from the completed queue.
  struct iocb* cb;
  cb = completed.front();
  completed.pop_front();
  m.done_req++;

  return cb;
}



const tile kaio_read_pipe::get_full_tile()
{

  // Make requests until we've filled our pending queue
  make_requests();
  // Are we done? That is, do we have no requests to wait for?
  if( m.req == m.done_req ) {
    // Return an empty tile.
    tile ret;
    return ret;
  }

  // Wait for the first outstanding request we have to come back.
  struct iocb* cb = wait_first();

  // Put that request in outside, where we will wait
  // for the empty tile to come back.
  // We can't queue a new request on this memory region
  // until the empty tile comes back.
  m.outside.push_back(cb);

  // Check that we're not at the end of the file.
  // Read returns 0 on EOF.
  if( cb->u.c.nbytes == 0 ) {
    // Return an empty tile.
    tile ret;
    return ret;
  }

  // Update the statistics.
  if( m.ctx.io_stats ) record_io(m.ctx.io_stats, IO_READ, cb->u.c.nbytes);
  
  // Set the return tile appropriately.
  // We changed the cb->u.c.nbytes to be the length returned by the read
  // in wait_first().
  tile ret(cb->u.c.buf, cb->u.c.nbytes, m.ctx.io_group_size);
  // Allow the fixer to fix ret.len if appropriate.
  //m.ctx.group_fixer->on_read(ret);
  return ret;
}

void kaio_read_pipe::put_empty_tile(const tile& t)
{
  // We need to put t back into a request.
  // 2- just stomp on what's in the outside queue, replacing the
  //    pointer to memory with what we get from the tile.
  //    Note that if we're changing the tile data pointer
  //    to hide a length... this'll be trouble. So store the lengths
  //    at the end of the tiles.
  struct iocb* cb = m.outside.back();
  m.outside.pop_back();

  // Fix the tile before re-using it.
  //m.ctx.group_fixer->before_empty(const_cast<tile&>(t));

  // Set the control-block buffer to what we get from the tile.
  cb->u.c.buf = t.data;

  // Put it back into the allocated queue, where we can make requests with it.
  m.allocated.push_back(cb);

  // Make requests until we've filled our pending queue.
  make_requests();
}

void kaio_read_pipe::close_empty()
{
  // Close the file.
  m.flush_and_close();
}

void kaio_read_pipe::wait_full()
{
  // Do nothing.
}

size_t kaio_read_pipe::get_tile_size()
{
  return m.ctx.io_group_size;
}

size_t kaio_read_pipe::get_num_tiles()
{
  return m.ctx.num_io_groups;
}
bool kaio_read_pipe::is_thread_safe()
{
  return false;
}

kaio_write_pipe::kaio_write_pipe(file_pipe_context ctx)
  : m(ctx, true)
{
}

tile kaio_write_pipe::get_empty_tile()
{
  struct iocb* cb;

  // If we're past that point, just return an empty tile to say EOF..
  if( m.ctx.len != PIPE_UNTIL_END && m.ctx.cur >= m.ctx.len ) {
    tile ret; // empty tile to say EOF
    return ret;
  }

  // If we don't have any tiles in allocated, reap.
  if( m.allocated.empty() ) {
    // Reap one or more pending writes.
    // We just need an empty buffer -- any one will do.
    long ret;
    ret = io_getevents(m.io_ctx, 1, m.events.size(), &m.events[0], NULL);
    if( ret <= 0 ) {
      throw error(ERR_IO_STR_OBJ("could not io_getevents", m.ctx.fname.c_str()));
    }
    // Push these events into completed. The order does not matter.
    for( long i = 0; i < ret; i++ ) {
      struct io_event *event = &m.events[i];
      struct iocb* cb = event->obj;

      //printf("KAIO write got back request %p\n", cb);

      // Check that there was no error
      // If there was an error in the event, bail out.
      if( event->res2 != 0 ) {
        throw error(ERR_IO_STR_OBJ("Error returned in io_getevents", m.ctx.fname.c_str()));
      }
      // Check that the proper number of bytes were written.
      if( event->res != cb->u.c.nbytes ) {
        throw error(ERR_IO_STR_OBJ("Error returned in io_getevents - short write", m.ctx.fname.c_str()));
      }

      // Put it into allocated so we can use it again!.
      m.allocated.push_back(cb);

      // increase the count for done requests.
      m.done_req++;
    }
  }
  // Get an empty tile from the allocated tiles.
  cb = m.allocated.back();
  m.allocated.pop_back();

  // Put it in outside, while we wait for the full tile to come back
  m.outside.push_back(cb);

  // Set the return value appropriately.
  tile ret(cb->u.c.buf, 0, m.ctx.io_group_size);

  // Allow fixer to fix ret.max if appropriate.
  //m.ctx.group_fixer->before_fill(ret);

  return ret;
}

void kaio_write_pipe::put_full_tile(const tile& t)
{
  // Allow the fixer to fix the tile to store a length in there..
  // Of course it needs to be able to modify the tile...
  //m.ctx.group_fixer->on_write(const_cast<tile&>(t));

  // We need to put t back into a request.
  // 2- just stomp on what's in the outside queue, replacing the
  //    pointer to memory with what we get from the tile.
  //    Note that if we're changing the tile data pointer
  //    to hide a length... this'll be trouble. So store the lengths
  //    at the end of the tiles.
  struct iocb* cb = m.outside.back();
  m.outside.pop_back();
  
  // Set the control-block buffer to what we get from the tile.
  io_prep_pwrite(cb, m.ctx.fd, t.data, t.len, m.ctx.start + m.ctx.cur);

  // Add to cur.
  m.ctx.cur += t.len;

  // Write it.
  long ret = io_submit(m.io_ctx, 1, &cb);
  if( ret != 1 ) throw error(ERR_IO_STR("Could not io_submit"));

  // increase the count of requests.
  m.req++;

  // Update statistics.
  if( m.ctx.io_stats ) record_io(m.ctx.io_stats, IO_WRITE, t.len);
}

void kaio_write_pipe::close_full(const tile& t)
{
  // We need to put t back into a request.
  // 2- just stomp on what's in the outside queue, replacing the
  //    pointer to memory with what we get from the tile.
  //    Note that if we're changing the tile data pointer
  //    to hide a length... this'll be trouble. So store the lengths
  //    at the end of the tiles.
  struct iocb* cb = m.outside.back();
  m.outside.pop_back();

  // Set the control-block buffer to what we get from the tile.
  cb->u.c.buf = t.data;

  m.allocated.push_back(cb);

  close_full();
}

void kaio_write_pipe::close_full()
{
  m.flush_and_close();
}

size_t kaio_write_pipe::get_tile_size()
{
  return m.ctx.io_group_size;
}

size_t kaio_write_pipe::get_num_tiles()
{
  return m.ctx.num_io_groups;
}

bool kaio_write_pipe::is_thread_safe()
{
  return false;
}

