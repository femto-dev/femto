/*
  (*) 2011-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils_cc/io_pipe.cc
*/
#include "io_pipe.hh"

#include <cerrno>
#include <cstring>

extern "C" {
#include "page_utils.h"
#include <sys/types.h>
#include <fcntl.h>
}


/////////////////////////// IO PIPES //////////////////////
// Here, we just read/write with the current thread
// and do not attempt to do anything asynchronously.
// aio or kaio are probably more performant, but this
// one is here in case threaded I/O does not make sense,
// or for debugging.

io_manager::io_manager(file_pipe_context ctx_in, bool write)
  : ctx(ctx_in), tiles(), closed(false)
{
  memset(&memory,0,sizeof(memory));
  ctx.create = write;
  ctx.write = write;
  ctx.open_file_if_needed();
  allocate_tiles();
}

void io_manager::allocate_tiles()
{
  // Allocate memory for num_tiles.
  memory = allocate_pages(ctx.io_group_size*ctx.num_io_groups);
  //memory = static_cast<unsigned char*>(malloc(ctx.io_group_size*ctx.num_io_groups));
  if( ! memory.data ) {
    throw error(ERR_MEM);
  }

  unsigned char* memptr = static_cast<unsigned char*>(memory.data);

  tiles.resize(ctx.num_io_groups);

  // Allocate num_io_groups control blocks for aio..
  for( size_t i = 0; i < ctx.num_io_groups; i++ ) {
    tile t(memptr + ctx.io_group_size*i, 0, ctx.io_group_size);
    tiles[i].t = t;
    tiles[i].available = 1;
  }
}

void io_manager::flush_and_close()
{
  // close the file if possible.
  ctx.close_file_if_needed();

  closed = true;

  for( size_t i = 0; i < tiles.size(); i++ ) {
    assert(tiles[i].available == 1);
  }
}

io_manager::~io_manager() 
{
  // Wait for pending operations to complete.
  flush_and_close();

  // Free the buffers
  free_pages(memory);
}

void io_manager::clear_o_direct_if_not_aligned(void* ptr, ssize_t amt, ssize_t offset)
{
  bool not_aligned = false;
  if( ctx.o_direct ) {
    // Check the size and alignment for page boundaries.
    if( ((size_t)ptr) & ctx.page_size_mask ) not_aligned = true;
    if( amt & ctx.page_size_mask ) not_aligned = true;
    if( offset & ctx.page_size_mask ) not_aligned = true;
  }

  if( ctx.o_direct ) {
    if( not_aligned ) ctx.clear_o_direct();
  }
}


io_read_pipe::io_read_pipe(file_pipe_context ctx)
  : m(ctx, false)
{
}

const tile io_read_pipe::get_full_tile()
{
  // Now take the lock...
  pthread_held_mutex held_lock(&m.lock);

  assert( ! m.closed );

  // If we're past that point, just return an empty tile to say EOF..
  if( m.ctx.len != PIPE_UNTIL_END && m.ctx.cur >= m.ctx.len ) {
    tile ret; // empty tile to say EOF
    return ret;
  }

  // Get the tile we should use.
  tile t;
  int id=-1;
  for( size_t i = 0; i < m.tiles.size(); i++ ) {
    if( m.tiles[i].available ) {
      t = m.tiles[i].t;
      m.tiles[i].available = 0;
      id = i;
      break;
    }
  }
  t.max = m.ctx.io_group_size;
  t.len = 0;
  assert(t.data);
  assert(id>=0);

  ssize_t num_read = 0;
  ssize_t got;
  ssize_t toget = m.ctx.io_group_size;
  unsigned char* mem = (unsigned char*) t.data;
  if( m.ctx.len != PIPE_UNTIL_END &&
      m.ctx.len - m.ctx.cur < m.ctx.io_group_size ) {
    toget = m.ctx.len - m.ctx.cur;
  }
  while( num_read < toget ) {
    m.clear_o_direct_if_not_aligned(mem + num_read, toget - num_read, num_read + m.ctx.start + m.ctx.cur);
    got = read(m.ctx.fd, mem + num_read, toget - num_read);
    if( got == 0 ) break; // EOF
    if( got < 0 ) {
      if( errno == EINTR || errno == EAGAIN ) got = 0; // just try again.
      else throw error(ERR_IO_STR_OBJ("Error from aio read", m.ctx.fname.c_str()));
    }
    num_read += got;
  }

  if( num_read <= 0 ) {
    // put it back!
    m.tiles[id].available = 1;

    // Return an empty tile.
    tile ret;
    return ret;
  }
  
  // Update the statistics.
  if( m.ctx.io_stats ) record_io(m.ctx.io_stats, IO_READ, num_read);

  // Set the return tile appropriately.
  t.len = num_read;

  m.ctx.cur += num_read;
  m.tiles[id].available = 0; // for extra clarity...

  return t;
}

void io_read_pipe::put_empty_tile(const tile& t)
{
  pthread_held_mutex held_lock(&m.lock);
  assert( ! m.closed );

  // Find where the tile is in our list.
  int id=-1;
  for( size_t i = 0; i < m.tiles.size(); i++ ) {
    if( m.tiles[i].available == 0 && m.tiles[i].t.data == t.data ) {
      id = i;
      break;
    }
  }
  assert(id>=0);

  m.tiles[id].available = 1;
}

void io_read_pipe::close_empty()
{
  pthread_held_mutex held_lock(&m.lock);
  assert( ! m.closed );
  // Finish any requests we have..
  m.flush_and_close();
}

void io_read_pipe::wait_full()
{
  pthread_held_mutex held_lock(&m.lock);
  assert( ! m.closed );
  // Do nothing.
}

size_t io_read_pipe::get_tile_size()
{
  return m.ctx.io_group_size;
}

size_t io_read_pipe::get_num_tiles()
{
  return m.ctx.num_io_groups;
}

bool io_read_pipe::is_thread_safe()
{
  return true;
}

void io_read_pipe::assert_closed_empty()
{
  assert(m.closed);
}

io_write_pipe::io_write_pipe(file_pipe_context ctx)
  : m(ctx, true)
{
}

tile io_write_pipe::get_empty_tile()
{
  pthread_held_mutex held_lock(&m.lock);

  assert( ! m.closed );

  // If we're past that point, just return an empty tile to say EOF..
  if( m.ctx.len != PIPE_UNTIL_END && m.ctx.cur >= m.ctx.len ) {
    tile ret; // empty tile to say EOF
    return ret;
  }

  tile t;
  int id=-1;
  for( size_t i = 0; i < m.tiles.size(); i++ ) {
    if( m.tiles[i].available ) {
      t = m.tiles[i].t;
      m.tiles[i].available = 0;
      id = i;
      break;
    }
  }
  t.max = m.ctx.io_group_size;
  t.len = 0;
  assert(t.data);
  assert(id>=0);

  return t;
}

void io_write_pipe::put_full_tile(const tile& t)
{
  pthread_held_mutex held_lock(&m.lock);

  assert( ! m.closed );


  ssize_t got;
  ssize_t num_wrote = 0;
  ssize_t towrite = t.len;
  unsigned char* mem = (unsigned char*) t.data;
  if( m.ctx.len != PIPE_UNTIL_END &&
      m.ctx.len - m.ctx.cur < t.len ) {
    towrite = m.ctx.len - m.ctx.cur;
  }
  while( num_wrote < towrite ) {
    m.clear_o_direct_if_not_aligned(mem + num_wrote, towrite - num_wrote, num_wrote + m.ctx.start + m.ctx.cur);
    got = write(m.ctx.fd, mem + num_wrote, towrite - num_wrote);
    if( got < 0 ) {
      if( errno == EINTR || errno == EAGAIN ) got = 0; // just try again.
      else throw error(ERR_IO_STR_OBJ("Error from aio read", m.ctx.fname.c_str()));
    }
    num_wrote += got;
  }
 
  // Find where the tile is in our list.
  int id=-1;
  for( size_t i = 0; i < m.tiles.size(); i++ ) {
    if( m.tiles[i].available == 0 && m.tiles[i].t.data == t.data ) {
      id = i;
      break;
    }
  }
  assert(id>=0);

  m.tiles[id].available = 1;


  // Add to cur.
  m.ctx.cur += t.len;

  // Update the statistics.
  if( m.ctx.io_stats ) record_io(m.ctx.io_stats, IO_WRITE, t.len);
}

void io_write_pipe::close_full()
{
  pthread_held_mutex held_lock(&m.lock);
  assert( ! m.closed );

  // Wait for requests to finish.
  m.flush_and_close();
}

size_t io_write_pipe::get_tile_size()
{
  return m.ctx.io_group_size;
}

size_t io_write_pipe::get_num_tiles()
{
  return m.ctx.num_io_groups;
}

bool io_write_pipe::is_thread_safe()
{
  return true;
}

void io_write_pipe::assert_closed_full()
{
  assert(m.closed);
}


