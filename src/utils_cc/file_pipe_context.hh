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

  femto/src/utils_cc/file_pipe_context.hh
*/
#ifndef _FILE_PIPE_CONTEXT_HH_
#define _FILE_PIPE_CONTEXT_HH_

extern "C" {
#include "iostats.h"
}

#include <string>

#include "pipelining.hh"

// To indicate that a pipe reads or writes until there is no more data.
#define PIPE_UNTIL_END ((uint64_t) -1)

// The point of a file_pipe_context is (mostly) to hold all of the
// parameters we pass to every file pipe.
struct file_pipe_context {
  static const size_t default_tile_size = DEFAULT_TILE_SIZE;
  static const size_t default_tiles_per_io_group = DEFAULT_TILES_PER_IO_GROUP;
  static const size_t default_num_io_groups = DEFAULT_NUM_IO_GROUPS;

  // io_stats... which has its own lock.
  io_stats_t* io_stats;

  std::string fname; // the path of the file.
  int fd; // the file descriptor for the file.
  size_t tile_size; // the size in bytes of each tile
  size_t tiles_per_io_group; // the number of tiles to use in each I/O.
  size_t io_group_size; // = tile_size*tiles_per_io_group
  size_t num_io_groups; // The number of I/O groups to allocate
                        // (maximum number in flight, but generally only
                        //  expect half of these to be in flight at once).
  bool we_openned; // Did we open the file when creating it?
  bool create; // create a file if not already there
  bool write; // allow writing?
  bool read; // allow reading?
  bool o_direct; // should we use O_DIRECT for the IO?
  uint64_t start; // The byte offset of the start
  uint64_t len; // The number of bytes to read/write.
  uint64_t cur; // The current byte offset (starting from 0, not start)
  //tile_fixer* t_fixer; // how to adjust the tiles... shared ptr.
                          // Just pass e.g. noop_tile_fixer
  //tile_fixer* group_fixer; // how to adjust the io group tiles
                           // we just set this to noop_tile_fixer.

  size_t page_size_mask;

  // Default constructor..
  file_pipe_context();
  // Creates a new file_pipe_context which refers to the named file
  // If fd==-1, we open the file named fname; if fd!=-1, fname is for debugging only
  // tile_size is the size of each tile to write.
  // tiles_per_io_group is the number of tiles to read/write in each I/O.
  // num_io_groups is the number of I/O groups to allocate.
  // write is true if we're going to be writing to the file.
  // o_direct is true if we want to use O_DIRECT - zero copy I/O
  // start is the start offset in bytes to read/write in the file
  // len is the number of bytes to read or write, or PIPE_UNTIL_END
  // if fixed_size_records is set, tile_size must be a multiple of the record
  // size, and the length of each tile except for the last one must be
  // the maximum length... in other words, noop_tile_fixer is used.
  // Otherwise, a len_tile_fixer is used to store the length along with
  // the data in each tile. Note that this flag must be the same when
  // the file is later read...
  file_pipe_context(io_stats_t* io_stats,
                    std::string fname,
                    uint64_t start=0,
                    uint64_t len=PIPE_UNTIL_END,
                    size_t tile_size=default_tile_size,
                    size_t tiles_per_io_group=default_tiles_per_io_group,
                    size_t num_io_groups=default_num_io_groups,
                    bool fixed_size_records=false,
                    bool o_direct=true );
  file_pipe_context(io_stats_t* io_stats,
                    int fd,
                    uint64_t start=0,
                    uint64_t len=PIPE_UNTIL_END,
                    size_t tile_size=default_tile_size,
                    size_t tiles_per_io_group=default_tiles_per_io_group,
                    size_t num_io_groups=default_num_io_groups,
                    bool fixed_size_records=false,
                    bool o_direct=true );
  ~file_pipe_context();

  // is file open?
  bool is_open();

  // Open the file
  void open_file_if_needed();
  // Close the file.
  void close_file_if_needed();

  // Clear the file (close if needed, create 0 length file).
  void create_zero_length();

  void set_o_direct();
  void clear_o_direct();
  void warn_and_clear_o_direct();
};

file_pipe_context fctx_fixed_cached(std::string fname, io_stats_t* stats=NULL);
file_pipe_context fctx_fixed_cached(int fd, io_stats_t* stats=NULL);



struct fileset {
  std::vector<file_pipe_context> files;
  fileset(size_t num, file_pipe_context base);
  fileset(std::vector<std::string> fnames, file_pipe_context base);
  std::vector<std::string> get_names();
  void unlink(); // deletes the files.
  void create_zero_length(); // creates the files.
};

#endif
