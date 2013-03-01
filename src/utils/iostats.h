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

  femto/src/utils/iostats.h
*/
#ifndef _IOSTATS_H_
#define _IOSTATS_H_

#include <stdlib.h>
#include <inttypes.h>

#include "error.h"


typedef struct io_counters {
  // Handles up to 64-bit requests... 1,2,4,8,16,...
  size_t log_histogram[64];
  size_t num_bytes_total;
  size_t num_requests_total;
} io_counters_t;

typedef enum {
  IO_READ,
  IO_WRITE,
  NUM_IO_TYPES
} io_type_t;

typedef struct io_stats {
  io_counters_t counters[NUM_IO_TYPES];
} io_stats_t;

void clear_io_stats(io_stats_t* stats);

void record_io(io_stats_t* stats, io_type_t io_type, size_t io_size);

void add_io_stats(io_stats_t* dst, io_stats_t* src);
size_t get_io_total(io_stats_t* stats, io_type_t io_type);


typedef struct sys_stats {
  char* stat_str;
} sys_stats_t;

error_t device_name_for_file(const char* fname, char** ret);
error_t save_sys_stats(const char* fname, sys_stats_t* stats);
error_t read_sys_stats(const char* fname, sys_stats_t* stats);
error_t get_sys_stats(sys_stats_t* stats);
void free_sys_stats(sys_stats_t* s);
error_t diff_sys_stats(const char* device_name, const sys_stats_t* before, const sys_stats_t* after, uint64_t* bytes_read, uint64_t* bytes_written);
error_t start_io_measurement(const char* fname);
error_t stop_io_measurement(const char* fname, uint64_t* bytes_read, uint64_t* bytes_written);


#endif
