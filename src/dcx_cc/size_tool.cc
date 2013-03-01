/*
  (*) 2011-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/dcx_cc/size_tool.cc
*/

#include <cstdio>
#include <cstdlib>
#include <cassert>

#include <vector>
#include <string>
#include <cstring>

extern "C" {
#include "error.h"
#include "index_tool_support.h"
}

char* cmd_name;

void index_tool_usage(void)
{
  printf("Usage: %s [options] ", cmd_name);
  its_usage(stdout);
  printf(" where options include:\n");
  printf(" -v or --verbose print extra verbose output\n");
  printf(" --output <filename> output query results to file instead of stdout\n");
  printf(" --null seperate output lines with 0 bytes instead of newlines\n");
  exit(-1);
}

int main( int argc, char** argv )
{
  std::vector<std::string> input_paths;
  int rc;

  int verbose = 0;

  char* output_fname = NULL;
  FILE* out = stdout;
  char sep = '\n';

  int64_t total_bytes = 0;

  cmd_name = argv[0];

  for( int i = 1; i < argc; i++ ) {
    if( 0 == strcmp(argv[i], "-v" ) ||
        0 == strcmp(argv[i], "--verbose") ) {
      verbose++;
    } else if( 0 == strcmp(argv[i], "--output") ) {
      i++; // pass --output
      output_fname = argv[i];
    } else if( 0 == strcmp(argv[i], "--null") ) {
      sep = '\0';
    } else if( argv[i][0] == '-' ) {
      printf("Unknown option %s\n", argv[i]);
      index_tool_usage();
    } else {
      //printf("adding input %s\n", argv[i]);
      input_paths.push_back(argv[i]);
    }
  }

  if( input_paths.size() == 0 ) index_tool_usage();

  if( output_fname ) {
    out = fopen(output_fname, "w");
    if( ! out ) {
      perror("Could not fopen");
      fprintf(stderr, "Could not open output filename '%s' for writing\n", output_fname);
      exit(-1);
    }
  }

  // Give any arguments to the its tool.
  {
    const char** args = (const char**) malloc(sizeof(char*)*input_paths.size());
    for( size_t i = 0; i < input_paths.size(); i++ ) {
      args[i] = input_paths[i].c_str();
    }

    rc = its_use_arguments(input_paths.size(), args);
    if( rc < 0 ) {
      printf("its_use_arguments returned %i\n", rc);
      die_if_err(ERR_IO_STR("its_use_arguments failed"));
    }

    free(args);
  }

  // We only do a pass 0.

  // Go through the documents
  for( int64_t doc_num = 0; 1; doc_num++ ) {
    int64_t doc_len = 0;
    int64_t doc_info_len = 0;
    unsigned char* doc_info = NULL;
    int64_t num_doc_headers = 0;
    int64_t* doc_header_lens = NULL;
    rc = its_get_doc_info(doc_num,
                     &doc_len,
                     &doc_info_len,
                     &doc_info,
                     &num_doc_headers,
                     &doc_header_lens);

    if( rc == 0 ) {
      break; // all done!
    } else if ( rc == 1 ) {
      // We have a document.
      total_bytes += doc_len;

      // Now free what we got.
      rc = its_free_doc(doc_num, doc_len,
                        doc_info_len, doc_info,
                        num_doc_headers, doc_header_lens, NULL,
                        NULL);
      if( rc < 0 ) {
        printf("its_free_doc returned %i\n", rc);
        die_if_err(ERR_IO_STR("its_free_doc failed"));
      }
    } else {
      // An error occured.
      printf("its_get_doc returned %i\n", rc);
      die_if_err(ERR_IO_STR("its_get_doc_info failed"));
    }
  }

  its_cleanup();

  fprintf(out, "%lli total bytes%c", (long long int) total_bytes, sep); 

  if(out != stdout) fclose(out);

  return 0;
}

