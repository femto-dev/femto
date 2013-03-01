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

  femto/src/utils_cc/kaio_pipe.hh
*/
#ifndef _KAIO_PIPE_HH_
#define _KAIO_PIPE_HH_

extern "C" {
#include <libaio.h>
#include "page_utils.h"
}

#include "pipelining.hh"
#include "file_pipe_context.hh"
#include <memory> // autoptr

// NOTE - these classes assume single-threaded use - wrap them in 
// a lock protecting these functions if that's not the case!
// Manage kaio buffers...
// This is just shared functionality for kaio_read_pipe and kaio_write_pipe.

struct kaio_manager: private uncopyable {
  file_pipe_context ctx;

  size_t req; // the current request number.
  size_t done_req; // the request number for the first non-completed request,
                   // that is, the first request we'll put in the completed
                   // queue.
  std::vector<struct iocb*> allocated; // what's allocated & available
  std::vector<struct io_event> events; // receive events into here.
  std::vector<struct iocb*> outside; // what's been returned to user
  struct allocated_pages memory;
  io_context_t io_ctx; // The IO context we'll use for reads or writes.

  // Constructors
  kaio_manager(file_pipe_context ctx, bool write);

  // Allocates tiles once the appropriate member vars have been set.
  void allocate_tiles();
  void flush_and_close();

  // Destructor
  ~kaio_manager();
};

class kaio_read_pipe: public read_pipe, private uncopyable {
  private:
    kaio_manager m;
    std::deque<struct iocb*> completed; // Storing early-completed requests (read)
    void make_requests();
    struct iocb* wait_first();
  public:
    kaio_read_pipe(file_pipe_context ctx);

    virtual const tile get_full_tile();
    virtual void put_empty_tile(const tile& t);
    // indicate that no new data will be read -- no new empty tiles added.
    virtual void close_empty();
    // wait for at least one full tile to be placed into the pipe.
    virtual void wait_full();
    // Get the maximum size of each tile
    virtual size_t get_tile_size();
    // Get the number of tiles supported by this pipe.
    virtual size_t get_num_tiles();
    virtual bool is_thread_safe();
};

class kaio_write_pipe: public write_pipe, private uncopyable {
  private:
    kaio_manager m;
  public:
    kaio_write_pipe(file_pipe_context ctx);

    virtual tile get_empty_tile();
    virtual void put_full_tile(const tile& t);
    // indicate that no new data will be produced -- no new full tiles.
    // and also return a tile as empty
    virtual void close_full(const tile& t);
    // indicate that no new data will be produced -- no new full tiles.
    virtual void close_full();
    // Get the maximum size of each tile
    virtual size_t get_tile_size();
    // Get the number of tiles supported by this pipe.
    virtual size_t get_num_tiles();
    virtual bool is_thread_safe();
};


#endif
