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

  femto/src/utils/page_utils.c
*/
// get posix_fadvise
#define _XOPEN_SOURCE 600
#define _GNU_SOURCE 600
// this one needed to compile on Sun.
#define __EXTENSIONS__
#include <fcntl.h>

#include "page_utils.h"

#include "bit_funcs.h"

#include "config.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/mman.h>

#include "bswap.h" // for __LINUX__

#ifdef __LINUX__
#include <sys/vfs.h>
#endif

#ifdef sun
// workaround dopey problems with sun header..
extern int madvise(caddr_t addr, size_t len, int advice);
#endif

// We normally allocate pages with MMap so that they
// can be dropped with drop_pages, and because these
// don't have gaps on linux (whereas posix_memalign
// will not use contigous memory between allocations,
// which wastes memory).
// Turn this off mostly to check for memory leaks with valgrind.
#define ALLOCATE_PAGES_MMAP

error_t drop_pages(void* addr, size_t len)
{
  int rc;

#ifdef MS_KILLPAGES
  // Mac OS X always returns an error with MADV_FREE,
  // so we'll use msync(MS_KILLPAGES) instead.
  rc = msync(addr, len, MS_KILLPAGES);
  if( rc ) return ERR_MEM_STR_NUM("drop_pages(MS_KILLPAGES) failed", rc);
  return ERR_NOERR;
#else
# ifdef MADV_FREE
  rc = madvise(addr, len, MADV_FREE);
  if( rc ) return ERR_MEM_STR_NUM("drop_pages(MADV_FREE) failed", rc);
  return ERR_NOERR;
# else
#  ifdef POSIX_MADV_DONTNEED
    // we just hope that DONTNEED will actually free
    // the pages...
    rc = posix_madvise(addr, len, POSIX_MADV_DONTNEED);
    if( rc ) return ERR_MEM_STR_NUM("drop_pages(POSIX_MADV_DONTNEED) failed", rc);
    return ERR_NOERR;
#  else
     rc = madvise(addr, len, MADV_DONTNEED);
     if( rc ) return ERR_MEM_STR_NUM("drop_pages(MADV_DONTNEED) failed", rc);
     return ERR_NOERR;
   // end if POSIX_MADV_DONTNEED
#  endif
  // end if MADV_FREE
# endif
// end if MS_KILLPAGES
#endif
}


error_t advise_sequential_pages(void* addr, size_t len)
{
#ifdef POSIX_MADV_SEQUENTIAL
  int rc;
  rc = posix_madvise(addr, len, POSIX_MADV_SEQUENTIAL);
  if( rc ) return ERR_MEM_STR_NUM("posix_madvise(POSIX_MADV_SEQUENTIAL) failed", rc);
  return ERR_NOERR;
#else
  int rc;
  rc = madvise(addr, len, MADV_SEQUENTIAL);
  if( rc ) return ERR_MEM_STR_NUM("madvise(MADV_SEQUENTIAL) failed", rc);
  return ERR_NOERR;
#endif
}

error_t advise_need_pages(void* addr, size_t len)
{
#ifdef POSIX_MADV_WILLNEED
  int rc;
  rc = posix_madvise(addr, len, POSIX_MADV_WILLNEED);
  if( rc ) return ERR_MEM_STR_NUM("posix_madvise(POSIX_MADV_WILLNEED) failed", rc);
  return ERR_NOERR;
#else
  int rc;
  rc = madvise(addr, len, MADV_WILLNEED);
  if( rc ) return ERR_MEM_STR_NUM("madvise(MADV_WILLNEED) failed", rc);
  return ERR_NOERR;
#endif
}


struct allocated_pages null_pages = {NULL, 0};

struct allocated_pages allocate_pages_mmap(size_t len)
{
  struct allocated_pages ret;
  ret.len = len;
  ret.data = NULL;

  if( len > 0 ) {
    ret.data = mmap(NULL, len, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    if( ret.data == MAP_FAILED ) {
      error_t err = ERR_MEM_STR_NUM("mmap anonymous failed", errno);
      warn_if_err(err);
      ret.data = NULL;
    }
  }
  return ret;
}

struct allocated_pages allocate_pages(size_t len)
{
#ifdef ALLOCATE_PAGES_MMAP
  return allocate_pages_mmap(len);
#else
  {
    struct allocated_pages ret;
    ret.len = len;
    ret.data = NULL;
    int rc = posix_memalign(&ret.data, get_page_size(), len);
    if( rc ) {
      error_t err = ERR_MEM_STR_NUM("posix_memalignn failed", rc);
      warn_if_err(err);
      ret.data = NULL; // to be sure.
    }
    return ret;
  }
#endif
}

void free_pages_mmap(struct allocated_pages pgs)
{
  if( pgs.data != NULL ) {
    munmap(pgs.data, pgs.len);
  }
}

void free_pages(struct allocated_pages pgs)
{
#ifdef ALLOCATE_PAGES_MMAP
  free_pages_mmap(pgs);
#else
  if( pgs.data ) free(pgs.data);
#endif
}

size_t get_page_size(void)
{
  long ret = sysconf(_SC_PAGESIZE);
  // note -- getpagesize() is an option too.
  if( ret <= 0 ) {
    error_t err = ERR_IO_STR_NUM("sysconf(_SC_PAGESIZE) fails -- assuming 4k pages",errno);
    warn_if_err(err);
    return 4096;
  }
  return ret;
}

size_t get_file_block_size(int fd)
{
#ifdef HAVE_STATVFS
  struct statvfs fs;
  int ret;

  do {
    ret = fstatvfs(fd, &fs);
  } while( ret != 0 && errno == EINTR );

  if( ret ) {
    perror("fstatfs");
    return 0;
  }

  return fs.f_bsize;
#else
  return get_page_size();
#endif
}

size_t get_file_page_size(int fd)
{
  size_t page_size = get_page_size();
  size_t block_size = get_file_block_size(fd);
  size_t ret = (block_size>page_size)?(block_size):(page_size);
  //printf("file page size is %li\n", (long) ret);
  return ret;
}



struct Pages get_pages(size_t page_size, off_t start_byte, off_t end_byte)
{
  struct Pages ret;
  ret.start = start_byte;
  ret.end = end_byte;
  // outer rounds start down and end up.
  ret.outer_start = round_down_to_multiple(ret.start,page_size);
  ret.outer_end = round_up_to_multiple(ret.end,page_size);
  // inner rounds start up and end down.
  ret.inner_start = round_up_to_multiple(ret.start,page_size);
  ret.inner_end = round_down_to_multiple(ret.end,page_size);
  if( ret.inner_end < ret.inner_start ) ret.inner_end = ret.inner_start;
  // compute lengths.
  ret.inner_length = ret.inner_end - ret.inner_start;
  ret.outer_length = ret.outer_end - ret.outer_start;
  ret.length = ret.end - ret.start;
  return ret;
}

struct Pages get_pages_for_records(size_t page_size, off_t start_n, off_t end_n, size_t record_size)
{
  return get_pages(page_size, start_n*record_size, end_n*record_size);
}


// Flush some file data. Wait if wait==1.
// May always call fsync()..
error_t flush_file_range(int fd, off_t offset, off_t len, int wait)
{
#ifdef HAVE_SYNC_FILE_RANGE
  unsigned int flags = SYNC_FILE_RANGE_WRITE;
  int rc;
  if( len == 0 ) return ERR_NOERR;
  if( wait ) {
    flags |= SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WAIT_AFTER;
  }
  rc = sync_file_range(fd, offset, len, flags);
  if( rc ) return ERR_IO_STR_NUM("sync_file_range failed", errno);
  return ERR_NOERR;
#else
  int rc;
  if( len == 0 ) return ERR_NOERR;
  rc = fsync(fd);
  if( rc ) return ERR_IO_STR_NUM("fsync failed", errno);
  return ERR_NOERR;
#endif
}

error_t advise_file_records(int fd, size_t page_size, off_t start_n, off_t end_n, advise_t need, size_t record_size)
{
#ifdef HAVE_POSIX_FADVISE
  error_t err;
  int rc = 0;
  int advice = POSIX_FADV_NORMAL;
  struct Pages pgs = get_pages_for_records(page_size, start_n, end_n, record_size);
  off_t offset, len;
  switch( need ) {
    case ADVISE_NORMAL: advice = POSIX_FADV_NORMAL; break;
    case ADVISE_SEQUENTIAL: advice = POSIX_FADV_SEQUENTIAL; break;
    case ADVISE_WILLNEED: advice = POSIX_FADV_WILLNEED; break;
    case ADVISE_DONTNEED: advice = POSIX_FADV_DONTNEED; break;
    case ADVISE_FLUSH: advice = POSIX_FADV_NORMAL; break;
    default: return ERR_INVALID_STR("bad need in advise_file");
  }

  // Now.. if we're doing DONTNEED, we do the inside area.
  // if we're doing WILLNEED we do the outside.
  if( need == ADVISE_DONTNEED ) {
    // inside area
    offset = pgs.inner_start;
    len = pgs.inner_length;
    // When doing DONTNEED, consider calling sync_file_range().
    err = flush_file_range(fd, offset, len, 1);
    if( err ) return err;
  } else {
    // outside area
    offset = pgs.outer_start;
    len = pgs.outer_length;
  }
  if( len > 0 ) {
    if( need == ADVISE_FLUSH ) {
      err = flush_file_range(fd, offset, len, 0);
      if( err ) return err;
    } else {
      //if( need != ADVISE_DONTNEED ) {
      rc = posix_fadvise(fd, offset, len, advice);
      if( rc ) return ERR_IO_STR_NUM("posix_fadvise failed", rc);
    }
  }
#endif
  return ERR_NOERR;
}

error_t blocking_readahead(int fd, size_t offset, size_t count)
{
#ifdef HAVE_READAHEAD
  ssize_t rc;
  rc = readahead(fd, offset, count);
  if( rc != 0 ) return ERR_IO_STR("readahead failed");
#else
  // do nothing!
#endif
  return ERR_NOERR;
}

#ifdef __LINUX__

enum {

              ADFS_SUPER_MAGIC      = 0xadf5,
              AFFS_SUPER_MAGIC      = 0xADFF,
              BEFS_SUPER_MAGIC      = 0x42465331,
              BFS_MAGIC             = 0x1BADFACE,
              CIFS_MAGIC_NUMBER     = 0xFF534D42,
              CODA_SUPER_MAGIC      = 0x73757245,
              COH_SUPER_MAGIC       = 0x012FF7B7,
              CRAMFS_MAGIC          = 0x28cd3d45,
              DEVFS_SUPER_MAGIC     = 0x1373,
              EFS_SUPER_MAGIC       = 0x00414A53,
              EXT_SUPER_MAGIC       = 0x137D,
              EXT2_OLD_SUPER_MAGIC  = 0xEF51,
              EXT2_SUPER_MAGIC      = 0xEF52,
              EXT3_SUPER_MAGIC      = 0xEF53,
              HFS_SUPER_MAGIC       = 0x4244,
              HPFS_SUPER_MAGIC      = 0xF995E849,
              HUGETLBFS_MAGIC       = 0x958458f6,
              ISOFS_SUPER_MAGIC     = 0x9660,
              JFFS2_SUPER_MAGIC     = 0x72b6,
              JFS_SUPER_MAGIC       = 0x3153464a,
              MINIX_SUPER_MAGIC     = 0x137F, /* orig. minix */
              MINIX_SUPER_MAGIC2    = 0x138F, /* 30 char minix */
              MINIX2_SUPER_MAGIC    = 0x2468, /* minix V2 */
              MINIX2_SUPER_MAGIC2   = 0x2478, /* minix V2, 30 char names */
              MSDOS_SUPER_MAGIC     = 0x4d44,
              NCP_SUPER_MAGIC       = 0x564c,
              NFS_SUPER_MAGIC       = 0x6969,
              NTFS_SB_MAGIC         = 0x5346544e,
              OPENPROM_SUPER_MAGIC  = 0x9fa1,
              PROC_SUPER_MAGIC      = 0x9fa0,
              QNX4_SUPER_MAGIC      = 0x002f,
              REISERFS_SUPER_MAGIC  = 0x52654973,
              ROMFS_MAGIC           = 0x7275,
              SMB_SUPER_MAGIC       = 0x517B,
              SYSV2_SUPER_MAGIC     = 0x012FF7B6,
              SYSV4_SUPER_MAGIC     = 0x012FF7B5,
              TMPFS_MAGIC           = 0x01021994,
              UDF_SUPER_MAGIC       = 0x15013346,
              UFS_MAGIC             = 0x00011954,
              USBDEVICE_SUPER_MAGIC = 0x9fa2,
              VXFS_SUPER_MAGIC      = 0xa501FCF5,
              XENIX_SUPER_MAGIC     = 0x012FF7B4,
              XFS_SUPER_MAGIC       = 0x58465342,
              _XIAFS_SUPER_MAGIC    = 0x012FD16D,
};

#endif

error_t is_netfs(const char* path, int* out)
{
#ifdef __LINUX__
  struct statfs s;
  int rc = statfs(path, &s);
  if( rc ) return ERR_IO_STR("statfs failed");

  switch (s.f_type) {
    case CIFS_MAGIC_NUMBER:
    case NFS_SUPER_MAGIC:
      *out = 1;
      break;
    case EXT_SUPER_MAGIC:
    case EXT2_OLD_SUPER_MAGIC:
    case EXT2_SUPER_MAGIC:
    case EXT3_SUPER_MAGIC:
    case XFS_SUPER_MAGIC:
    case REISERFS_SUPER_MAGIC:
    case JFS_SUPER_MAGIC:
    case _XIAFS_SUPER_MAGIC:
    case TMPFS_MAGIC:
      *out = 0;
      break;
    default:
      fprintf(stderr, "Unknown filesystem type %#lx in is_netfs(%s)\n", s.f_type, path);
      *out = -1;
  }
#else
  fprintf(stderr, "Warning: is_netfs not supported on this platform\n");
  *out = -1;
#endif

  return ERR_NOERR;
}


error_t get_fsid(const char* path, unsigned long* out)
{
  int rc;
  struct statvfs s;

  rc = statvfs(path, &s);
  if( rc ) return ERR_IO_STR("statvfs failed");

  *out = s.f_fsid;

  return ERR_NOERR;
}

error_t read_file(int fd, void* buf, size_t count, io_stats_t* stats)
{
  size_t cur;
  ssize_t amt;

  cur = 0;

  while( cur < count ) {
    size_t len = count - cur;
    amt = pread(fd, buf+cur, len, cur);
    if( amt == 0 ) {
      return ERR_IO_STR("Premature end-of-file in read_file");
    } else if( amt < 0 ) {
      if( errno == EAGAIN || errno == EINTR ) {
        // OK.
      } else {
        return ERR_IO_STR_NUM("Error in pread in read_file", errno);
      }
    } else {
      cur += amt;
    }
  }

  if( stats ) record_io(stats, IO_READ, count);

  return ERR_NOERR;
}

error_t write_file(int fd, void* buf, size_t count, io_stats_t* stats)
{
  size_t cur;
  ssize_t amt;

  cur = 0;

  while( cur < count ) {
    size_t len = count - cur;
    amt = pwrite(fd, buf+cur, len, cur);
    if( amt == 0 ) {
      warn_if_err(ERR_IO_STR("pwrite wrote 0 bytes in write_file"));
      // Try again.
    } else if( amt < 0 ) {
      if( errno == EAGAIN || errno == EINTR ) {
        // OK.
      } else {
        return ERR_IO_STR_NUM("Error in pwrite in write_file", errno);
      }
    } else {
      cur += amt;
    }
  }

  if( stats ) record_io(stats, IO_WRITE, count);

  return ERR_NOERR;
}

