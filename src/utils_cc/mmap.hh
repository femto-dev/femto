/*
  (*) 2009-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils_cc/mmap.hh
*/
#ifndef _MMAP_HH_
#define _MMAP_HH_

extern "C" {
  #include <sys/mman.h>
  #include <errno.h>
  #include <unistd.h>
  #include "page_utils.h"
  #include "bit_funcs.h"
}
#include <cerrno>
#include <cassert>
#include <cstdio>
#include "error.hh"
#include "utils.hh" // file_len

// Just calls mmap and frees the resource in the destructor
// for RAII purposes.
//
struct FileMMap {
  void* data;
  size_t len;
  off_t offset;
  int prot;
  FileMMap()
    : data(MAP_FAILED), len(0), offset(0), prot(0)
  {
  }
  void map(void* addr, size_t in_len, int in_prot, int flags, int fd, off_t offset_in)
  {
    // Can't mmap something already mapped!
    assert(data == MAP_FAILED);

    data = mmap(addr,in_len,in_prot,flags,fd,offset_in);
    if( data == MAP_FAILED ) {
      throw error(ERR_IO_STR_NUM("mmap failed",errno));
    }
    //printf("map(len=%li,off=%li) = %p\n", (long) in_len, (long) offset_in, data);
    len = in_len;
    offset = offset_in;
    prot = in_prot;
  }
  void unmap()
  {
    if( data != MAP_FAILED ) {
      // mmap over NFS will not write data until msync is called.
      if( prot & PROT_WRITE ) {
        int rc = msync(data, len, MS_ASYNC);
        if( rc ) printf("error in mysnc %i %i\n", rc, errno);
      }
      //printf("unmap(%p,len=%li,off=%li)\n", data, (long) len, (long) offset);
      munmap(data,len);
      data = MAP_FAILED;
      len = 0;
      offset = 0;
    }
  }
  bool is_mapped()
  {
    return data != MAP_FAILED;
  }
  FileMMap(void* addr, size_t in_len, int prot, int flags, int fd, off_t offset)
    : data(MAP_FAILED), len(0), offset(0), prot(0)
  {
    map(addr, in_len, prot, flags, fd, offset);
  }

  ~FileMMap()
  {
    unmap();
  }

  void swap(FileMMap& x)
  {
    void* x_data = x.data;
    size_t x_len = x.len;
    off_t x_offset = x.offset;
    x.data = data;
    x.len = len;
    x.offset = offset;
    data = x_data;
    len = x_len;
    offset = x_offset;
  }

  void advise_willneed()
  {
    error_t err;
    err = advise_need_pages(data, len);
    if( err ) throw error(err);
  }

  // Do not allow it to be copied.
  private:
    FileMMap(const FileMMap&);
    FileMMap& operator=(const FileMMap&);
};

template<typename Record>
struct RecordFileMMap {
  FileMMap map;
  Record* data;
  size_t n;

  // Create a mapping for a new file.
  RecordFileMMap(int fd, size_t num_records)
  {
    int rc;
    size_t sz = num_records*sizeof(Record);

    size_t file_page_size = get_file_page_size(fd);
    //printf("page size is %li\n", (long) file_page_size);

    sz = round_to_mask(sz, file_page_size-1);

    rc = ftruncate(fd, sz );
    if( rc ) throw error(ERR_IO_STR_NUM("ftruncate failed", errno));

    // Map the file.
    map.map(NULL, sz, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);

    data = (Record*) map.data;
    n = num_records;
  }

  // create a read-write mapping for an existing file.
  RecordFileMMap(int fd)
   : map(NULL, file_len(fd), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0)
  {
    data = (Record*) map.data;
    n = map.len/sizeof(Record);
  }

  void advise_sequential() {
    error_t err;
    err = advise_sequential_pages(map.data, map.len);
    if( err ) throw error(err);
  }
};

static inline void advise_file_records_throwing(int fd, size_t page_size, off_t start_n, off_t end_n, advise_t need, size_t record_size)
{
  error_t err = advise_file_records(fd,page_size,start_n,end_n,need,record_size);
  if( err ) throw error(err);
}
#endif

