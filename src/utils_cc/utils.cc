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

  femto/src/utils_cc/utils.cc
*/
#include "utils.hh"

#include "error.hh"

extern "C" {

#include <stdio.h>
// for stat
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
// If O_DIRECT isn't defined, just set it to 0.
#ifndef O_DIRECT
#define O_DIRECT 0
#endif

#include "page_utils.h"

}

#include <cstring>

#include "file_pipe_context.hh"
#include "file_pipe.hh"


off_t file_len(int fd)
{
  off_t orig_pos;
  off_t ret;
  off_t orig_pos2;

  assert(sizeof(off_t) >= 8);

  orig_pos = lseek(fd, 0, SEEK_CUR);
  if( orig_pos == (off_t) -1 ) throw(ERR_IO_STR("Could not lseek"));

  ret = lseek(fd, 0, SEEK_END);
  if( ret == (off_t) -1 ) throw(ERR_IO_STR("Could not lseek"));
  
  orig_pos2 = lseek(fd, orig_pos, SEEK_SET);
  if( orig_pos2 == (off_t) -1 ) throw(ERR_IO_STR("Could not lseek"));

  return ret;
}

size_t file_len(FILE* f)
{
  long ret;
  int rc;
  long orig_pos = ftell(f);

  assert(sizeof(long) >= 8);
  assert(sizeof(size_t) >= 8);

  if( orig_pos < 0 ) throw error(ERR_IO_STR("Could not ftell"));
  rc = fseek(f, 0L, SEEK_END);
  if( rc ) throw error(ERR_IO_STR("Could not fseek"));
  ret = ftell(f);
  if( ret < 0 ) throw error(ERR_IO_STR("Could not ftell"));
  rc = fseek(f, orig_pos, SEEK_SET);
  if( rc ) throw error(ERR_IO_STR("Could not fseek"));
  return ret;
}



void stat_file(const std::string & filename,
               bool& exists,
               bool& regular,
               off_t& len)
{
  int rc;
  struct stat stats;

  assert(sizeof(stats.st_size) >= 8);
  assert(sizeof(off_t) >= 8);

  rc = stat(filename.c_str(), &stats);
  if( rc ) {
    if( errno == ENOENT ) {
      regular = false;
      exists = false;
      len = 0;
      return;
    }
    fprintf(stderr, "Could not stat %s\n", filename.c_str());
    throw error(ERR_IO_STR("Could not stat"));
  }

  exists = true;
  regular = S_ISREG(stats.st_mode);
  len = stats.st_size;
}

off_t file_len(const std::string & filename)
{
  bool exists = false;
  bool regular = false;
  off_t len = 0;

  stat_file(filename, exists, regular, len);

  if( ! exists ) {
    throw error(ERR_IO_STR_OBJ("file does not exist in file_len()", filename.c_str()));
  }
  if( ! regular ) {
    throw error(ERR_IO_STR_OBJ("file is not a regular file in file_len()", filename.c_str()));
  }
  return len;
}

FILE* tmp_file(std::string tmp_dir)
{
  char buf[1024];
  int rc;
  int fd;
  FILE* ret;
  
  snprintf(buf, 1024, "%s/tfXXXXXX", tmp_dir.c_str());

  // Now get the temp file.
  fd = mkstemp(buf);
  if( fd < 0 ) throw error(ERR_IO_STR("Could not mkstemp"));

  ret = fdopen(fd, "w+");

  // Unlink the temporary file.
  rc = unlink(buf);
  if( rc != 0 ) throw error(ERR_IO_STR("Could not unlink"));

  return ret;
}


void print_character(std::ostringstream& os, unsigned long int ch)
{
  char tmp_buf[64];
  unsigned int c = ch;

  if( ch && (ch<256) && isascii(c) && isalnum(c) ) {
    snprintf(tmp_buf, sizeof(tmp_buf), "%c", c);
  } else {
    snprintf(tmp_buf, sizeof(tmp_buf), "\\x%lx", ch);
  }
  os << tmp_buf;
}

bool set_o_direct_fd(int fd, size_t transfer_size, const char* fname)
{
  size_t page_size_mask = get_page_size()-1;
  if( transfer_size & page_size_mask ) {
    static char n_prints=0; // This isn't actually thread safe,
                           // but it's just throttling back the printing
                           // so we shouldn't care.
    if( n_prints < 4 ) {
      std::cerr << "WARNING - transfer size is not page aligned - not setting O_DIRECT" << std::endl;
      n_prints++;
    }
    return false;
  } else {
    int flags, rc;
    // Set O_DIRECT on the file.
    // Open the file with the filename... O_DIRECT needed for -lposix-aio
    if( fd != -1 ) {
      rc = fcntl(fd, F_GETFL);
      if( rc==-1 ) {
        throw error(ERR_IO_STR_OBJ("Could not fcntl F_GETFL", fname));
      }
      flags = rc | O_DIRECT;
      rc = fcntl(fd, F_SETFL, flags);
      if( rc ) {
        throw error(ERR_IO_STR_OBJ("Could not fcntl +O_DIRECT", fname));
      }
    }
    return true;
  }
}

void clear_o_direct_fd(int fd, const char* fname)
{
  int flags, rc;
  // Clear O_DIRECT on the file.
  if( fd != -1 ) {
    rc = fcntl(fd, F_GETFL);
    if( rc==-1 ) {
      throw error(ERR_IO_STR_OBJ("Could not fcntl F_GETFL", fname));
    }
    flags = rc & ~O_DIRECT;
    rc = fcntl(fd, F_SETFL, flags);
    if( rc ) {
      throw error(ERR_IO_STR_OBJ("Could not fcntl -O_DIRECT", fname));
    }
  }
}

void copy_file(const std::string & from_fname, const std::string & to_fname, bool move_not_copy)
{
  if( from_fname == to_fname ) return;

  file_pipe_context from_ctx = fctx_fixed_cached(from_fname);
  file_pipe_context to_ctx = fctx_fixed_cached(to_fname);


  file_read_pipe_t read_pipe(from_ctx);
  file_write_pipe_t write_pipe(to_ctx);

  assert( read_pipe.get_tile_size() == write_pipe.get_tile_size() );

  copy_node copy(&read_pipe, &write_pipe);
  copy.start();
  copy.finish();
  // copy node closes the pipes.

  // now, if we're moving, unlink from_fname.
  if( move_not_copy ) {
    unlink_ifneeded(from_fname);
  }
}

void move_file(const std::string & from_fname, const std::string & to_fname)
{
  copy_file(from_fname, to_fname, true);
}

void unlink_ifneeded(const std::string & fname)
{
  const char* path = fname.c_str();
  int rc = unlink(path);
  if( rc ) {
    if( errno == ENOENT ) {
      // OK.
    } else {
      throw error(ERR_IO_STR_OBJ_NUM("Could not unlink", path,errno));
    }
  }
}

void unlink_ifneeded(std::vector<std::string> fnames)
{
  for( size_t i = 0; i < fnames.size(); i++ ) {
    unlink_ifneeded(fnames[i]);
  }
}

