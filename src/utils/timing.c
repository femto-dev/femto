/*
  (*) 2007-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/timing.c
*/

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>

#include "timing.h"
#include "iostats.h"
#include "util.h"

#include "config.h" // for HAVE_CLOCK_GETTIME

#define MAX_DEPTH 1000
static int depth;
static tinfo_t ti[MAX_DEPTH];

#define MAX(a,b) ((a>b)?(a):(b))

void read_time(struct timing* t) 
{
  int err;

  // Get resource usage.
  err = getrusage(RUSAGE_SELF, &t->times);
  assert( ! err );

  // Get real timing.
  err = gettimeofday(&t->real, NULL);
  assert ( ! err );

  // Get the per-thread timing.
#ifdef HAVE_CLOCK_GETTIME
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &t->thread);
#else
  memset(&t->thread, 0, sizeof(struct timespec));
#endif

}

void start_clock_r(tinfo_t* t)
{
  memset(t, 0, sizeof(tinfo_t));
  clear_io_stats(&t->io_stats);
  read_time(&t->start);
}

void start_clock(void)
{
  assert(depth<MAX_DEPTH);
  start_clock_r(&ti[depth]);
  depth++;
}

void stop_clock_r(tinfo_t* t)
{
  read_time(&t->end);
}

void stop_clock(void)
{
  depth--;
  assert(depth>=0);
  stop_clock_r(&ti[depth]);
}

double ttosec(struct timeval* t)
{
  double ret;
  ret = t->tv_usec;
  ret /= 1.0e6;
  ret += t->tv_sec;
  return ret;
}

double tstosec(struct timespec* t)
{
  double ret;
  ret = t->tv_nsec;
  ret /= 1.0e9;
  ret += t->tv_sec;
  return ret;
}

void print_timings_r(tinfo_t* t, const char* thing, double number, const char* prefix)
{
  double rdiff;
  struct val_unit read_v, write_v;
  struct val_unit read_v_s, write_v_s;
  // First, add in the timing info to ourselves 
  // (this accumulates it all in the add_ area).

  add_timing_r(t, t);

  printf("%sDid %f %s in:\n", prefix, number, thing);
  printf("%s   Thread time: %f\n", prefix, t->add_thread_time);
  printf("%s   User time: %f\n", prefix, t->add_user_time);
  printf("%s   Sys  time: %f\n", prefix, t->add_sys_time);
  //printf("%s   Child User time: %f\n", prefix, ctosec(t->end.times.tms_cutime) - ctosec(t->start.times.tms_cutime));
  //printf("%s   Child Sys  time: %f\n", prefix, ctosec(t->end.times.tms_cstime) - ctosec(t->start.times.tms_cstime));
  rdiff = t->max_real_time;
  printf("%s   Real time: %f\n", prefix, rdiff);
  if( t->io_stats.counters[IO_READ].num_bytes_total > 0 ||
      t->io_stats.counters[IO_WRITE].num_bytes_total ) {
    read_v = get_unit(t->io_stats.counters[IO_READ].num_bytes_total);
    write_v = get_unit(t->io_stats.counters[IO_WRITE].num_bytes_total);
    read_v_s = get_unit(t->io_stats.counters[IO_READ].num_bytes_total/rdiff);
    write_v_s = get_unit(t->io_stats.counters[IO_WRITE].num_bytes_total/rdiff);
    printf("%s   I/O read volume: %g %sB %g %sB/s\n", prefix, read_v.value, get_unit_prefix(read_v), read_v_s.value, get_unit_prefix(read_v_s));
    printf("%s   I/O write volume: %g %sB %g %sB/s\n", prefix, write_v.value, get_unit_prefix(write_v), write_v_s.value, get_unit_prefix(write_v_s));
  }

  printf("%s That's %g %s/sec or %g sec average\n", prefix, number / rdiff, thing, rdiff / number);
}

void print_timings(const char* thing, double number)
{
  print_timings_r(&ti[depth], thing, number, "");
}

void record_io_timing_r(tinfo_t* t, io_type_t io_type, size_t io_size)
{
  record_io(&t->io_stats, io_type, io_size);
}

void record_io_timing(io_type_t io_type, size_t io_size)
{
  assert(depth>0);
  record_io_timing_r(&ti[depth-1], io_type, io_size);
}

io_stats_t* get_current_io_stats(void)
{
  if( depth == 0 ) return NULL;
  else return &ti[depth-1].io_stats;
}

void add_timing_r(tinfo_t* dst, tinfo_t* t)
{
  dst->add_thread_time += tstosec(&t->end.thread) - tstosec(&t->start.thread);
  dst->add_user_time += ttosec(&t->end.times.ru_utime) - ttosec(&t->start.times.ru_utime);
  dst->add_sys_time += ttosec(&t->end.times.ru_stime) - ttosec(&t->start.times.ru_stime);
  dst->max_real_time = MAX(dst->max_real_time, ttosec(&t->end.real) - ttosec(&t->start.real));
  if( dst != t ) add_io_stats(&dst->io_stats, &t->io_stats);
}

// After call to stop_clock.
void add_current_timing(tinfo_t* dst)
{
  add_timing_r(dst, &ti[depth]);
}


