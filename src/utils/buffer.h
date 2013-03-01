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

  femto/src/utils/buffer.h
*/
#ifndef _BUFFER_H_
#define _BUFFER_H_

#include <inttypes.h>
#include <stdio.h>
#include "error.h"

#define CEILDIV(top,bottom) ( ((top) + (bottom) - 1) / (bottom) )

typedef uintptr_t bsbuff_t;

// a buffer...
typedef struct {
  // All elements are "long int"
  // or the right size so that the full size of
  // the address space could be in a buffer.
  uintptr_t max;
  uintptr_t len;
  unsigned char* data;
  uintptr_t pos; // for reading
  bsbuff_t bsBuff;
  intptr_t bsLive;
} buffer_t;

error_t mmap_buffer(buffer_t* buf, FILE* f, int writeable, long minlen);
error_t munmap_buffer(buffer_t* buf);
error_t munmap_buffer_truncate(buffer_t* buf, FILE* f);
error_t buffer_extend_mmap(buffer_t* buf, FILE* f, int64_t extra);

error_t write_buffer(buffer_t* b, FILE* f);

// Growing buffers uses a double-in-size allocation method
// until we get to a buffer larger than buffer_chunk, at
// which point a buffer_chunk is simply added.
#define BUFFER_CHUNK (64*1024*1024) // 64 Meg

#endif
