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

  femto/src/utils/iostats.c
*/
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <ctype.h>

#include "bswap.h" // for __LINUX__

#ifdef __LINUX__
#include <mntent.h>
#endif

#include "iostats.h"
#include "bit_funcs.h"

void clear_io_stats(io_stats_t* stats)
{
  memset(stats, 0, sizeof(io_stats_t));
}

void record_io_in_counters(io_counters_t* counters, size_t io_size)
{
  int log;

  if( io_size == 0 ) log = 0;
  else log = log2lli(io_size);

  //counters->log_histogram[log]++;
  try_sync_add_size_t(&counters->log_histogram[log], 1);
  //counters->num_bytes_total += io_size;
  try_sync_add_size_t(&counters->num_bytes_total, io_size);
  //counters->num_requests_total++;
  try_sync_add_size_t(&counters->num_requests_total, 1);
}

void record_io(io_stats_t* stats, io_type_t io_type, size_t io_size)
{
  assert(io_type >=0 && io_type < NUM_IO_TYPES);
  record_io_in_counters(&stats->counters[io_type], io_size);
}

void add_io_stats(io_stats_t* dst, io_stats_t* src)
{
  if( ! src || ! dst ) return;

  for( int type = 0; type < NUM_IO_TYPES; type++ ) {
    for( int log = 0; log < 64; log++ ) {
      dst->counters[type].log_histogram[log] +=
        src->counters[type].log_histogram[log];
    }
    dst->counters[type].num_bytes_total += src->counters[type].num_bytes_total;
    dst->counters[type].num_requests_total += src->counters[type].num_requests_total;
  }
}

size_t get_io_total(io_stats_t* stats, io_type_t io_type)
{
  return stats->counters[io_type].num_bytes_total;
}


error_t device_name_for_file(const char* fname, char** ret)
{
#ifdef __LINUX__
  FILE* f;
  struct mntent* ent;
  size_t fname_len = strlen(fname);
  int max_mount_len = 0;

  f = setmntent("/etc/mtab", "r");
  if( ! f ) {
    return ERR_IO_STR_OBJ("Could not setmntent /etc/mntab", fname);
  }

  *ret = NULL;

  while((ent = getmntent(f))) {
    // check that fsname is a prefix of fname...
    size_t fsdir_len = strlen(ent->mnt_dir);
    if( fsdir_len < fname_len &&
        0 == memcmp(ent->mnt_dir, fname, fsdir_len) &&
        fsdir_len >= max_mount_len ) {
      char* str = ent->mnt_fsname;
      char* buf = strdup(str);
      char* a = buf;
      int len;
      int i;
      if( 0 == memcmp(str, "/dev/", strlen("/dev/")) ) {
        a += strlen("/dev/");
      }
      len = strlen(a);
      for( i = len - 1; i >= 0 && isdigit(a[i]); i-- ) {
        // Remove digits at the end.
        a[i] = '\0';
      }
      if( *ret ) free(*ret);
      *ret = strdup(a);
      free(buf);
      max_mount_len = fsdir_len;
    }
  }

  endmntent(f);

  return ERR_NOERR;
#else
  return ERR_INVALID_STR("device_name_for_file not supported on this platform");
#endif
}

error_t save_sys_stats(const char* fname, sys_stats_t* stats)
{
#ifdef __LINUX__
  FILE* f = fopen(fname, "w");
  size_t wrote;

  if( ! f ) {
    fprintf(stderr, "Error opening %s\n", fname);
    return ERR_IO_STR_OBJ("Could not open", fname);
  }

  wrote = fwrite(stats->stat_str, strlen(stats->stat_str), 1, f);
  if( wrote != 1 ) {
    return ERR_IO_FILE_MSG(f, fname, "Could not write");
  }
#else
  return ERR_INVALID_STR("save_sys_stats not supported on this platform");
#endif
  return ERR_NOERR;
}

error_t read_sys_stats(const char* fname, sys_stats_t* stats)
{
  memset(stats, 0, sizeof(sys_stats_t));
#ifdef __LINUX__
  size_t max = 1024*1024;
  char* buf;
  size_t read;
  FILE* f;
  buf = calloc(max,1); // 1MB should be plenty!
  if( ! buf ) return ERR_MEM;
  f = fopen(fname, "r");
  if( ! f ) {
    return ERR_IO_STR_OBJ("Could not open", fname);
  }
  read = fread(buf, 1, max-1, f);
  if( ! feof(f) ) {
    return ERR_IO_FILE_MSG(f, fname, "stats file too big");
  }
  fclose(f);

  stats->stat_str = strdup(buf);
  free(buf);
  return ERR_NOERR;
#else
  return ERR_INVALID_STR("read_sys_stats not supported on this platform");
#endif
}

error_t get_sys_stats(sys_stats_t* stats)
{
#ifdef __LINUX__
  return read_sys_stats("/proc/diskstats", stats);
#else
  return ERR_INVALID_STR("get_sys_stats not supported on this platform");
#endif
}

void free_sys_stats(sys_stats_t* s)
{
#ifdef __LINUX__
  free(s->stat_str);
#endif
  memset(s, 0, sizeof(sys_stats_t));
}


error_t diff_sys_stats(const char* device_name, const sys_stats_t* before, const sys_stats_t* after, uint64_t* bytes_read, uint64_t* bytes_written)
{
  *bytes_read = 0;
  *bytes_written = 0;

#ifdef __LINUX__
  char* pre = before->stat_str;
  char* post = after->stat_str;
  uint32_t dev_maj_pre, dev_maj_post;
  uint32_t dev_min_pre, dev_min_post;
  uint32_t read_sectors_pre, read_sectors_post;
  uint32_t wrote_sectors_pre, wrote_sectors_post;
  uint32_t read_sectors, wrote_sectors;
  uint64_t read_sectors_total, wrote_sectors_total;
  int pre_used, post_used;
  int pre_name_offset, post_name_offset;
  int rc;
  char* pre_name_str;

  read_sectors_total = 0;
  wrote_sectors_total = 0;

  // OK, now go through both at the same time...
  while( pre[0] && post[0] ) {
    rc = sscanf(pre, "%u %u %n %*s %*u %*u %u %*u %*u %*u %u %*u %*u %*u %*u %n",
           &dev_maj_pre, &dev_min_pre, 
           &pre_name_offset,
           &read_sectors_pre, &wrote_sectors_pre,
           &pre_used);
    if( rc < 4 ) {
      return ERR_INVALID_STR("diff_sys_stats sscanf failed");
    }
    pre_name_str = pre + pre_name_offset;
    pre += pre_used;
    rc = sscanf(post, "%u %u %n %*s %*u %*u %u %*u %*u %*u %u %*u %*u %*u %*u %n",
           &dev_maj_post, &dev_min_post,
           &post_name_offset,
           &read_sectors_post, &wrote_sectors_post,
           &post_used);
    if( rc < 4 ) {
      return ERR_INVALID_STR("diff_sys_stats sscanf failed");
    }
    post += post_used;
    if( ! (dev_maj_pre == dev_maj_post && dev_min_pre == dev_min_post) ) {
      return ERR_INVALID_STR("Devices changed in diff_sys_stats");
    }
    if( !device_name ||
        0 == memcmp(pre_name_str, device_name, strlen(device_name)) ) {

      read_sectors = read_sectors_post - read_sectors_pre;
      wrote_sectors = wrote_sectors_post - wrote_sectors_pre;
      read_sectors_total += read_sectors;
      wrote_sectors_total +=  wrote_sectors;
    }
  }

  *bytes_read = read_sectors_total * 512;
  *bytes_written = wrote_sectors_total * 512;
#endif

  return ERR_NOERR;
}

static sys_stats_t saved_io_measurement;

error_t start_io_measurement(const char* fname)
{
  error_t err;

  err = get_sys_stats(&saved_io_measurement);
  if( err ) return err;

  return ERR_NOERR;
}


error_t stop_io_measurement(const char* fname, uint64_t* bytes_read, uint64_t* bytes_written)
{
  char* device_name = NULL;
  error_t err;
  sys_stats_t post;

  err = device_name_for_file(fname, &device_name);
  if( err ) return err;

  err = get_sys_stats(&post);
  if( err ) return err;

  err = diff_sys_stats(device_name, &saved_io_measurement, &post, bytes_read, bytes_written);
  if( err ) return err;

  free(device_name);
  free_sys_stats(&post);
  free_sys_stats(&saved_io_measurement);

  return ERR_NOERR;
}


