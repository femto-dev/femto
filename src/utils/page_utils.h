/*
  (*) 2007-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/page_utils.h
*/
#ifndef _PAGE_UTILS_H_
#define _PAGE_UTILS_H_

#include "config.h"

// needed to get MAP_ANON on Mac OS X builds.
#undef _POSIX_C_SOURCE

#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif
#if !defined(MAP_ANONYMOUS) && defined(MAP_ANON)
# define MAP_ANONYMOUS MAP_ANON
#endif

#include "error.h"
#include "iostats.h"

// Drop some pages from in-core memory.
// Their values will be destroyed, but the
// address range will still be valid.
// Calls madvise(addr, len, MADV_DONTNEED)
// or madvise(addr, len, MADV_FREE)
// Returns an error
error_t drop_pages(void* addr, size_t len);

error_t advise_sequential_pages(void* addr, size_t len);

error_t advise_need_pages(void* addr, size_t len);


struct allocated_pages {
  void* data;
  size_t len;
};

extern struct allocated_pages null_pages;

// Allocate memory on a page boundary.
struct allocated_pages allocate_pages_mmap(size_t len);
struct allocated_pages allocate_pages(size_t len);

// Free memory allocated with allocate_pages.
void free_pages(struct allocated_pages pgs);
void free_pages_mmap(struct allocated_pages pgs);

// returns the page size.
// If there's an error, prints a warning and returns 4096.
size_t get_page_size(void);

size_t get_file_block_size(int fd);

size_t get_file_page_size(int fd);


struct Pages {
  // all are in bytes
  off_t outer_start;  // outside page start; outer_start <= start
  off_t start;        // real start
  off_t inner_start;  // inner_start; start <= inner_start
  off_t inner_length;
  off_t length;
  off_t outer_length;
  off_t inner_end;
  off_t end;
  off_t outer_end;
};

struct Pages get_pages(size_t page_size, off_t start_byte, off_t end_byte);
struct Pages get_pages_for_records(size_t page_size, off_t start_n, off_t end_n, size_t record_size);

typedef enum {
  ADVISE_NORMAL = 0,
  ADVISE_SEQUENTIAL,
  ADVISE_WILLNEED,
  ADVISE_DONTNEED,
  ADVISE_FLUSH,
} advise_t;

error_t advise_file_records(int fd, size_t page_size, off_t start_n, off_t end_n, advise_t need, size_t record_size);

error_t blocking_readahead(int fd, size_t offset, size_t count);

// Returns 1 in *out if we believe we're on a shared filesystem.
// -1 if we don't know
// 0 if we're not.
error_t is_netfs(const char* path, int* out);

error_t get_fsid(const char* path, unsigned long* out);


error_t read_file(int fd, void* buf, size_t count, io_stats_t* stats);
error_t write_file(int fd, void* buf, size_t count, io_stats_t* stats);

#endif
