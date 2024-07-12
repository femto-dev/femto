/*
  (*) 2008-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/processors.c
*/

#define _BSD_SOURCE
#define _DARWIN_C_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h> // for sysconf
#include <sys/types.h>
#include <sys/param.h>

#include "processors.h"
#include "bswap.h" // get right __LINUX__

// Get the number of processors available in the machine.
error_t get_num_processors(long* num_procs)
{
  long ret=0;
  char* str;
  // First, try to get the env var NUM_PROCS
  str = getenv("NUM_PROCS");
  if( str ) {
    ret = atoi(str);
  }
  // If ret isn't reasonable, use a default.
  if( ret < 1 ) {
    ret = sysconf(_SC_NPROCESSORS_ONLN);
    if( ret < 0 ) {
      return ERR_INVALID_STR("Could not get number of processors");
    }
    if( ret == 0 ) {
      return ERR_INVALID_STR("Number of processors is zero!");
    }
  }
  *num_procs = ret;
  return ERR_NOERR;
}

// Get the number of processors to use for a new task.
error_t get_use_processors(long* use_procs)
{
  error_t err;
  long ret = 0;
  char* str;

  // First, try to get the env var USE_PROCS
  str = getenv("USE_PROCS");
  if( str ) {
    ret = atoi(str);
  }

  if( ret < 1 ) {
    // Otherwise, use a default.
    err = get_num_processors(&ret);
    if( err ) return err;

    ret = (3 * ret)/4;
    if( ret == 0 ) ret = 1;
  }

  *use_procs = ret;
  return ERR_NOERR; 
}

error_t get_phys_mem(long long* bytes)
{
  long long mem_bytes = 0;
#ifdef _SC_PHYS_PAGES  
  long long num_pages;
  long long page_size;
  long ret;

  ret = sysconf(_SC_PHYS_PAGES);
  if( ret <= 0 ) return ERR_INVALID_STR("Could not get number of physical pages");
  num_pages = ret;

  ret = sysconf (_SC_PAGESIZE);
  if( ret <= 0 ) return ERR_INVALID_STR("Could not get page size");
  page_size = ret;
  mem_bytes = num_pages * page_size;
#else
# ifdef __APPLE__
  size_t len = sizeof(mem_bytes);
  int ret = -1;

  /* Note hw.memsize is in bytes, so no need to multiply by page size. */
  ret = sysctlbyname("hw.memsize", &mem_bytes, &len, NULL, 0);
  if (ret == -1) mem_bytes = 0;
#endif
#endif

  *bytes = mem_bytes;

  if( mem_bytes == 0 ) return ERR_INVALID;
  return ERR_NOERR;
}

error_t get_max_open_files(long long* max_fds)
{
  long ret;

  ret = sysconf(_SC_OPEN_MAX);
  if( ret <= 0 ) return ERR_INVALID_STR("Could not get max open files");

  *max_fds = ret;

  return ERR_NOERR;
}

// How many simultaneous mappings?
error_t get_max_mappings(long long* max_mappings)
{
#ifdef __LINUX__
  long long ret;
  int rc;
  FILE* f;
  f = fopen("/proc/sys/vm/max_map_count", "r");
  if( f ) {
    ret = 0;
    rc = fscanf(f, "%lli", &ret);
    fclose(f);
    if( rc == 1 ) {
      *max_mappings = ret;
      return ERR_NOERR;
    } else {
      warn_if_err(ERR_IO_STR("Could not read max_map_count"));
    }
  } else {
    warn_if_err(ERR_IO_STR("Could not open max_map_count"));
  }
#endif
  // max open files is a good standin...
  return get_max_open_files(max_mappings);
}

