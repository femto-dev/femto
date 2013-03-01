/*
  (*) 2010-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/stats_util.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "iostats.h"

int main(int argc, char** argv)
{
  int i;
  int saving = 0;
  int comparing = 0;
  int device = 0;
  char* file_fname = NULL;
  char* save_fname = NULL;
  char* pre_fname = NULL;
  char* post_fname = NULL;
  char* device_name = NULL;
  error_t err;

  i = 1;
  while( i < argc ) {
    if( 0 == strcmp(argv[i], "--save") ) {
      saving = 1;
      i++;
      save_fname = argv[i];
      i++;
    } else if( 0 == strcmp(argv[i], "--compare") ) {
      comparing = 1;
      i++;
      pre_fname = argv[i];
      i++;
      if( i < argc ) {
        post_fname = argv[i];
        i++;
      }
    } else if( 0 == strcmp(argv[i], "--device") ) {
      device = 1;
      i++;
      file_fname = argv[i];
      i++;
    }
  }

  if( ! (saving || comparing || device) ) {
    fprintf(stderr, "Usage: \n"
                    "  %s --device filename\n"
                    "  %s --save filename\n"
                    "  %s --compare pre_filename [post_filename]\n",
                    argv[0], argv[0], argv[0]);
    return -1;
  }

  if( device ) {
    err = device_name_for_file(file_fname, &device_name);
    die_if_err(err);
    printf("Device for %s is %s\n", file_fname, device_name);
  }

  if( saving ) {
    sys_stats_t stats;
    err = get_sys_stats(&stats);
    die_if_err(err);
    err = save_sys_stats(save_fname, &stats);
    die_if_err(err);
    free_sys_stats(&stats);
  }

  if( comparing ) {
    sys_stats_t pre;
    sys_stats_t post;
    uint64_t bytes_read, bytes_written;
    err = read_sys_stats(pre_fname, &pre);
    die_if_err(err);
    if( post_fname ) {
      err = read_sys_stats(post_fname, &post);
      die_if_err(err);
    } else {
      err = get_sys_stats(&post);
      die_if_err(err);
    }
    err = diff_sys_stats(device_name, &pre, &post, &bytes_read, &bytes_written);
    die_if_err(err);

    printf("read %lli bytes\n"
           "wrote %lli bytes\n",
           (long long int) bytes_read,
           (long long int) bytes_written);


    free_sys_stats(&pre);
    free_sys_stats(&post);
  }

  return 0;
}
