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

  femto/src/main/merge_tool.c
*/
#include "index.h"


#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <assert.h>

#include "timing.h"
#include "server.h"

void usage(char* name)
{
  printf("Usage: %s <storage_area> <dst_index_name> <src_index_name> <output_index_name> \"[parameters]\"\n", name);
  printf(" where [parameters] is:\n");
  print_block_param_usage("     ");
  exit(-1);
}

int main( int argc, char** argv )
{
#if SUPPORT_INDEX_MERGE
  char* storage;
  char* dst_name;
  char* src_name;
  char* output_name;
  char** params;
  int nparams;
  int i;

  if( argc < 5 ) usage(argv[0]);

  i = 1;
  storage = argv[i++];
  dst_name = argv[i++];
  src_name = argv[i++];
  output_name = argv[i++];
  if( argc > i ) {
    params = & argv[i];
    nparams = argc - i;
  }
  else {
    params = NULL;
    nparams = 0;
  }


  // first, start up the server.
  {
    server_state_t* sst = NULL;
    index_block_param_t param;
    construct_statistics_t stats;
    index_locator_t dst_loc, src_loc, out_loc;
    error_t err;

    {
      server_settings_t settings;
      set_default_server_settings(&settings);
      settings.storage_path = storage;

      err = start_server(&sst, &settings);
      die_if_err(err);
    }

    dst_loc.id = dst_name;
    src_loc.id = src_name;
    out_loc.id = output_name;

    set_default_param(&param);
    for( int i = 0; i < nparams; i++ ) {
      parse_param(&param, params[i]);
    }

    // now we've got a server going.
    memset(&stats, 0, sizeof(construct_statistics_t));

    {
      index_merge_query_t q;
      setup_index_merge_query(&q, NULL, src_loc, dst_loc, out_loc, 
                              &param, 2.0/sizeof(update_block_entry_t),
                              1024, 512*1024); 

      err = schedule_query(sst, (query_entry_t*) &q);
      die_if_err(err);

      start_clock();
      work_until_done(sst);

      stop_clock();

      assert(!q.proc.entry.err_code);
    }

    stop_server(sst);
    print_timings("merged", 1);

  }

  printf("Created index %s\n", output_name);
  return 0;
#else
  printf("Merge not supported\n");
  return -1;
#endif
}

