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

  femto/src/utils/timing.h
*/
#ifndef _TIMING_H_
#define _TIMING_H_

// for clock_gettime
#include <sys/times.h>
#include <time.h>
// for gettimeofday/getrusage
#include <sys/time.h>
#include <sys/resource.h>

#include "iostats.h"

struct timing {
  // The raw data.
  struct rusage times;
  struct timeval real;
  struct timespec thread;
};

typedef struct {
  struct timing start;
  struct timing end;
  // Data we add into.
  double add_thread_time;
  double add_user_time;
  double add_sys_time;
  double max_real_time;
  io_stats_t io_stats;
} tinfo_t;


void start_clock(void);
void stop_clock(void);
void print_timings(const char* thing, double number);
void record_io_timing(io_type_t io_type, size_t io_size);

void record_io_timing_r(tinfo_t* t, io_type_t io_type, size_t io_size);
void start_clock_r(tinfo_t* t);
void stop_clock_r(tinfo_t* t);
void print_timings_r(tinfo_t* t, const char* thing, double number, const char* prefix);

// Can be called by a sub-thread to account for the total
// user/system time taken. real time is the maximum.
void add_timing_r(tinfo_t* dst, tinfo_t* src);

// Returns a pointer to the current timing structure.
io_stats_t* get_current_io_stats(void);

// Adds timing from start_clock()/stop_clock() to a particular structure.
void add_current_timing(tinfo_t* dst);

#endif

