/*
  (*) 2006-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/buffer.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <errno.h>

#include "buffer.h"
#include "error.h"

#include "buffer_funcs.h"

error_t mmap_buffer(buffer_t* buf, FILE* f, int writeable, long minlen)
{
  unsigned char zero = 0;
  int ret;
  long file_len;

  memset(buf, 0, sizeof(buffer_t));

  ret = fseek(f, 0, SEEK_END);
  if( ret != 0 ) return ERR_IO_UNK;
  file_len = ftell(f);
  if( file_len == -1 ) return ERR_IO_UNK;
  
  // If we're creating a file, make sure that
  // we actually map something...
  if( writeable && minlen == 0 && file_len == 0 ) return ERR_PARAM;

  // Just don't map a zero-length file when we're reading.
  if( !writeable && file_len == 0 ) return ERR_NOERR;

  if( writeable && minlen > file_len ) {
    // write a 0 at the end of the file.
    fseek(f, minlen, SEEK_SET);
    ret = fwrite(&zero, 1, 1, f);
    if( ret != 1 ) return ERR_IO_UNK;
    // get the file length again.
    ret = fseek(f, 0, SEEK_END);
    if( ret != 0 ) return ERR_IO_UNK;
    file_len = ftell(f);
    if( file_len == -1 ) return ERR_IO_UNK;
  }
  // rewind the file
  ret = fseek(f, 0, SEEK_SET);
  if( ret != 0 ) return ERR_IO_UNK;

  // flush the file
  if( writeable ) {
    ret = fflush(f);
    if( ret != 0 ) return ERR_IO_UNK;
  }


  if( writeable ) {
    buf->data = (unsigned char*) mmap(NULL, file_len, PROT_READ|PROT_WRITE, MAP_SHARED, fileno(f), 0);
  } else {
    buf->data = (unsigned char*) mmap(NULL, file_len, PROT_READ, MAP_SHARED, fileno(f), 0);
  }
  //printf("mmap'ing %p %li bytes (fail is %p, eq %i)\n", buf->data, file_len, MAP_FAILED, buf->data == MAP_FAILED);
  if( ! buf->data || buf->data == MAP_FAILED ) {
    return ERR_IO_UNK;
  }
  buf->len = buf->max = file_len;
  return ERR_NOERR;
}

error_t munmap_buffer(buffer_t* buf)
{
  int merr;

  // Just don't unmap if it's NULL.
  if( buf->data == NULL ) return ERR_NOERR;

  // guarantee that the data is sync'd.
  // Don't believe this is necessary!
  // In any case, can only do if writeable!
  //merr = msync(buf->data, buf->max, MS_ASYNC);
  //if( merr ) return ERR_IO_STR_NUM("msync failed", errno);

  //printf("munmap'ing %p %li bytes\n", buf->data, buf->max);

  merr = munmap(buf->data, buf->max);
  if( merr != 0 ) {
    return ERR_IO_STR_NUM("munmap failed", errno);
  }
  return ERR_NOERR;
}

error_t munmap_buffer_truncate(buffer_t* buf, FILE* f)
{
  error_t err;
  int ret;

  // munmap
  err = munmap_buffer(buf);
  if( err ) return err;

  // truncate the file to the buf->len.
  // if buf->len != buf->max.
  // (which would've happened with buffer_extend_mmap)
  if( buf->len < buf->max ) {
    ret = ftruncate(fileno(f), buf->len);
    if( ret ) return ERR_IO_UNK;
  }
  return ERR_NOERR;
}

error_t buffer_extend_mmap(buffer_t* buf, FILE* f, int64_t extra)
{
  if( buf->len + extra < buf->max ) {
    return ERR_NOERR;
  } else {
    error_t err;
    int64_t newmax;
    buffer_t save_buf;

    save_buf = *buf;

    // otherwise, we have to do some re-allocating.
    if( buf->len < BUFFER_CHUNK ) {
      newmax = 2 * buf->max + extra;
    } else {
      newmax = buf->max + BUFFER_CHUNK * CEILDIV(extra, BUFFER_CHUNK);
    }
    assert( extra + buf->len <= newmax);
    //printf("BUFFER EXTENDING TO %l bytes", newmax);
    err = munmap_buffer(buf);
    if( err ) return err;
    err = mmap_buffer(buf, f, 1, newmax);
    
    // restore the buffer vars.
    buf->len = save_buf.len;
    buf->pos = save_buf.pos;
    buf->bsLive = save_buf.bsLive;
    buf->bsBuff = save_buf.bsBuff;

    return err;
  }
}

error_t write_buffer(buffer_t* b, FILE* f)
{
  int wrote;

  wrote = fwrite(b->data, b->len, 1, f);
  if( wrote != 1 ) return ERR_IO_UNK;

  return ERR_NOERR;
}

