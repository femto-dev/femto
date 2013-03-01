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

  femto/src/utils_cc/file_pipe.hh
*/
#ifndef _FILE_PIPE_HH_
#define _FILE_PIPE_HH_

#include "pipelining.hh"

#include "aio_pipe.hh"

/*
typedef aio_read_pipe file_read_pipe_t;
typedef aio_write_pipe file_write_pipe_t;
*/

#include "io_pipe.hh"
typedef io_read_pipe file_read_pipe_t;
typedef io_write_pipe file_write_pipe_t;



// Caller must free these!!
template<typename WritePipe>
std::vector<write_pipe*> get_fileset_writers(fileset set)
{
  std::vector<write_pipe*> ret;
  ret.reserve(set.files.size());
  for( size_t i = 0; i < set.files.size(); i++ ) {
    ret.push_back(new WritePipe(set.files[i]));
  }
  return ret;
}

template< typename f_read_pipe = file_read_pipe_t >
struct fileset_read_pipe: public read_pipe {

  fileset set;
  ssize_t cur;
  f_read_pipe* file_pipe;

  fileset_read_pipe(fileset set) : set(set), cur(-1), file_pipe(NULL) {
    // Start out with a pipe for the front file.
    next_pipe();
  }

  virtual ~fileset_read_pipe() { }

  void close_pipe()
  {
    if( file_pipe ) {
      file_pipe->close_empty();
      delete(file_pipe);
      file_pipe = NULL;
    }
  }
  void next_pipe()
  {
    close_pipe();
  
    cur++;

    if( cur < (ssize_t) set.files.size() ) {
      file_pipe = new f_read_pipe(set.files[cur]);
    }
  }

  virtual const tile get_full_tile() {
    tile t;

    while(1) {
      t = file_pipe->get_full_tile();
      if( t.has_tile() ) break; // return the tile we got.

      // Try the next pipe..
      next_pipe();
      if( ! file_pipe ) break; // We have no more tiles!
    }
    return t;
  }
  virtual void put_empty_tile(const tile& t) {
    file_pipe->put_empty_tile(t);
  }
  // indicate that no new data will be read -- no new empty tiles added.
  virtual void close_empty() {
    close_pipe();
    cur = set.files.size();
  }

  // wait for at least one full tile to be placed into the pipe.
  virtual void wait_full() { file_pipe->wait_full(); }

  virtual size_t get_tile_size() { return file_pipe->get_tile_size(); }
  virtual size_t get_num_tiles() { return file_pipe->get_num_tiles(); }
  virtual bool is_thread_safe() { return file_pipe->is_thread_safe(); }
};

typedef fileset_read_pipe<file_read_pipe_t> fileset_read_pipe_t;

#endif
