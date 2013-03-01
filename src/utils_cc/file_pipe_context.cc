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

  femto/src/utils_cc/file_pipe_context.cc
*/
#include "file_pipe_context.hh"


#include <cerrno>
#include <cstring>

extern "C" {
#include "page_utils.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <alloca.h>
}

#include "utils.hh"

//noop_tile_fixer the_noop_tile_fixer;
//len_tile_fixer the_len_tile_fixer;

file_pipe_context::file_pipe_context()
  : io_stats(NULL), fd(-1), tile_size(0), tiles_per_io_group(0), io_group_size(0), num_io_groups(0), we_openned(false), create(false), write(false), read(false), o_direct(false), start(0), len(0), cur(0), /*t_fixer((tile_fixer*)&the_noop_tile_fixer), group_fixer(&the_noop_tile_fixer),*/ page_size_mask(get_page_size()-1)
{
}

file_pipe_context::file_pipe_context(io_stats_t* io_stats, std::string fname, uint64_t start, uint64_t len, size_t tile_size, size_t tiles_per_io_group, size_t num_io_groups, bool fixed_size_records, bool o_direct)
  : io_stats(io_stats),
    fname(fname), fd(-1),
    tile_size(tile_size),
    tiles_per_io_group(tiles_per_io_group),
    io_group_size(tile_size*tiles_per_io_group),
    num_io_groups(num_io_groups),
    we_openned(false), // havn't openned the file yet.
    create(false),
    write(false),
    read(true),
    o_direct(o_direct),
    start(start),
    len(len),
    cur(0),
    //t_fixer((fixed_size_records)?((tile_fixer*)&the_noop_tile_fixer):((tile_fixer*)&the_len_tile_fixer)),
    //group_fixer(&the_noop_tile_fixer),
    page_size_mask(get_page_size()-1)
{
  // Does not open the file.
  // Why? the file_pipe_context can be passed as a copy;
  // we only want to actually open once it's in the final destination.
  assert(0 <= (ssize_t) tile_size);
  assert(0 <= (ssize_t) tiles_per_io_group);
  assert(0 <= (ssize_t) num_io_groups);
}

file_pipe_context::file_pipe_context(io_stats_t* io_stats, int fd, uint64_t start, uint64_t len, size_t tile_size, size_t tiles_per_io_group, size_t num_io_groups, bool fixed_size_records, bool o_direct)
  : io_stats(io_stats),
    fname("unknown file"), fd(fd),
    tile_size(tile_size),
    tiles_per_io_group(tiles_per_io_group),
    io_group_size(tile_size*tiles_per_io_group),
    num_io_groups(num_io_groups),
    we_openned(false), // we won't open the file.
    create(false),
    write(false),
    read(true),
    o_direct(o_direct),
    start(start),
    len(len),
    cur(0),
    //t_fixer((fixed_size_records)?((tile_fixer*)&the_noop_tile_fixer):((tile_fixer*)&the_len_tile_fixer)),
    //group_fixer(&the_noop_tile_fixer),
    page_size_mask(get_page_size()-1)
{
  assert(0 <= fd);
  assert(0 <= (ssize_t) tile_size);
  assert(0 <= (ssize_t) tiles_per_io_group);
  assert(0 <= (ssize_t) num_io_groups);
}

file_pipe_context::~file_pipe_context()
{
  // Does not close the file - file must be closed explicitly.
  // Why? Then the file_pipe_context can be passed as a copy;
  // there's less memory allocation/deletion to worry about..
}

bool file_pipe_context::is_open()
{
  return (fd != -1);
}

void file_pipe_context::open_file_if_needed()
{
  // If we already have an open file, do nothing.
  if( fd != -1 ) {
    // Just make sure o_direct is set appropriately.
    if( o_direct ) set_o_direct();
    return;
  }

  // Open the file with the filename... O_DIRECT needed for -lposix-aio
  int flags = 0;
  if( create ) {
    flags |= O_CREAT;
    if( write ) {
      // Truncate if the maximal length was -1
      // and there was no start specified.
      if( len == PIPE_UNTIL_END && start == 0 ) 
        flags |= O_TRUNC;
    }
  }
  if( read && write ) {
    flags |= O_RDWR;
  } else {
    if( read ) flags |= O_RDONLY;
    if( write ) flags |= O_WRONLY;
  }

  fd = open(fname.c_str(), flags, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
  if( fd < 0 ) {
    throw error(ERR_IO_STR_OBJ_NUM("Could not open", fname.c_str(),errno));
  }

  we_openned = true;

  if( o_direct ) set_o_direct();
}

void file_pipe_context::close_file_if_needed()
{
  if( we_openned && fd != -1 ) {
    int rc = close(fd);
    if( rc != 0 ) {
      throw error(ERR_IO_STR_OBJ_NUM("Could not close", fname.c_str(), errno));
    }
    // Save that the file is closed so we can handle another
    // close if it comes up..
    fd = -1;
    we_openned = false;
  }
}

void file_pipe_context::create_zero_length()
{
  int rc;

  close_file_if_needed();

  fd = open(fname.c_str(), O_WRONLY | O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
  if( fd < 0 ) {
    throw error(ERR_IO_STR_OBJ_NUM("Could not open", fname.c_str(),errno));
  }

  rc = ftruncate(fd, 0);
  if( rc != 0 ) {
    throw error(ERR_IO_STR_OBJ_NUM("Could not truncate", fname.c_str(), errno));
  }

  rc = close(fd);
  if( rc != 0 ) {
    throw error(ERR_IO_STR_OBJ_NUM("Could not close", fname.c_str(), errno));
  }

  fd = -1;

  if( file_len(fname) != 0 ) throw error(ERR_IO_STR_OBJ("cleared file does not have zero length", fname.c_str()));
}

void file_pipe_context::set_o_direct()
{
  o_direct = set_o_direct_fd(fd, tile_size, fname.c_str());
}

void file_pipe_context::clear_o_direct()
{
  o_direct = false;
  clear_o_direct_fd(fd, fname.c_str());
}

void file_pipe_context::warn_and_clear_o_direct()
{
  std::cerr << "WARNING - clearing O_DIRECT" << std::endl;
  clear_o_direct();
}

file_pipe_context fctx_fixed_cached(std::string fname, io_stats_t* stats)
{
  return file_pipe_context(stats, fname, 
                           0, PIPE_UNTIL_END,
                           file_pipe_context::default_tile_size, file_pipe_context::default_tiles_per_io_group,
                           file_pipe_context::default_num_io_groups,
                           true,
                           false);
}
file_pipe_context fctx_fixed_cached(int fd, io_stats_t* stats)
{
  return file_pipe_context(stats, fd, 
                           0, PIPE_UNTIL_END,
                           file_pipe_context::default_tile_size, file_pipe_context::default_tiles_per_io_group,
                           file_pipe_context::default_num_io_groups,
                           true,
                           false);
}

std::string fileset_subname(std::string filename, size_t i)
{
  size_t sz = 100+filename.length();
  char* buf = (char*) alloca(sz);

  snprintf(buf, sz,
           "%s..%03lx", 
           filename.c_str(),
           (long) i);

  std::string ret(buf);
  return ret;
}

fileset::fileset(size_t num, file_pipe_context base) : files()
{
  // File should not be used yet!
  assert(base.fd == -1);

  files.reserve(num);

  for( size_t i = 0; i < num; i++ ) {
    file_pipe_context ctx = base;
    ctx.fname = fileset_subname(base.fname, i);
    ctx.fd = -1;
    ctx.we_openned = false;

    files.push_back(ctx);
  }

}

std::vector<std::string> fileset::get_names()
{
  std::vector<std::string> ret;
  for( size_t i = 0; i < files.size(); i++ ) {
    ret.push_back(files[i].fname);
  }
  return ret;
}

void fileset::unlink()
{
  for( size_t i = 0; i < files.size(); i++ ) {
    unlink_ifneeded(files[i].fname);
  }
}

void fileset::create_zero_length()
{
  for( size_t i = 0; i < files.size(); i++ ) {
    files[i].create_zero_length();
  }
}

