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

  femto/src/main/bwt_tool.c
*/
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "bwt.h"
#include "index.h"
#include "file_find.h"

#include "buffer_funcs.h"
#include "bwt_writer.h"
#include "bwt_creator.h"
#include "timing.h"

typedef struct  {
  file_find_state_t ffs;
  prepared_text_t p;
} bwt_state_t;

bwt_state_t bwt_globals;

error_t size_file(char* path, void* state)
{
  bwt_state_t* s = (bwt_state_t*) state;
  long len;
  FILE* f;
  struct stat st;
  int rc;

  rc = stat(path, &st);
  if( rc != 0 ) return ERR_IO_STR_OBJ("Could not stat", path);

  // Double-check: open the file.
  f = fopen(path, "r");
  if( ! f ) return ERR_IO_STR_OBJ("Could not open file", path);

  // get the length of the file.
  rc = fseek(f, 0L, SEEK_END);
  if( rc != 0 ) return ERR_IO_STR_OBJ("Could not fseek", path);
  len = ftell(f);
  if( len < 0 ) return ERR_IO_STR_OBJ("Could not ftell", path);
  rewind(f);

  if( len != st.st_size ) return ERR_INVALID_STR("seek and stat return different lengths");

  fclose(f);

  return count_file(&s->p, len, 0, NULL, strlen(path), (unsigned char*) path);
}

error_t read_file(char* path, void* state)
{
  FILE* f;
  bwt_state_t* s = (bwt_state_t*) state;
  prepared_text_t* p = &s->p;
  error_t err;

  // First thing, open the file
  f = fopen(path, "r");
  if( ! f ) return ERR_IO_STR("Could not open file");

  printf("%s\n", path);

  // now append the file.
  err = append_file(p, f, 0, NULL, NULL, strlen(path), (unsigned char*) path);
  fclose(f);
  if( err ) return err;

  return ERR_NOERR;
}

int main( int argc, char** argv ) 
{
  int mark_period;
  FILE* output;
  char* info_path = "/tmp/info";
  FILE* aux;
  int chunk_size = 4096; //2048;//1024;
  const char* path;
  error_t err;

  if( argc < 4 ) {
    printf("Usage: %s <mark_period> <file_to_index> <output_file> <document_info_output> <chunk_size> <document_map_output>\n", argv[0]);
    printf(" <mark_period> -- set the marking period. This program will marks every character with offset %% mark_period = 0. Marked characters store full offset information; locating is faster for smaller mark_periods, but the index will be larger. If <mark_period> is 0, no characters are marked. 20 is a reasonable value here.\n");
    printf(" <file_to_index> -- a file or directory containing the document(s) to be indexed; if it is a directory, every file in that directory will be indexed.\n");
    printf(" <output_file> -- where to store the Burrows-Wheeler Transform of the prepared text\n");
    printf(" <document_info_output> -- where to store a file containing document information (e.g. the URL of each document)\n");
    printf(" <chunk_size> -- the number of rows in the Burrows-Wheeler transform to group together and save a list of matching documents. Larger chunks mean that queries for very common terms will report faster and the index will be smaller. Smaller chunks allow the chunks to be used for less common terms. 4096 is a reasonable value here.\n");
    printf(" <document_map_output> -- where to store the list of matching documents for each <chunk_size> group of rows\n");
    exit(-1);
  }

  {
    int i = 1;
    char* name;
    sscanf(argv[i++], "%i", &mark_period);
    path = argv[i++];
    printf("Will save bwt for %s marking %i\n", path, mark_period);
    name = argv[i++];
    printf("Saving bwt to %s\n", name);
    output = fopen(name, "w+");
    if( ! output ) {
      printf("Could not open output file %s\n", name);
      die_if_err(ERR_IO_UNK);
    }
    name = argv[i++];
    printf("Saving document infos to %s\n", name);
    info_path = name;
    if( argc >= 6 ) {
      if( argc < 7 ) {
        printf("Missing <chunk_size> <doc_output_file>\n");
        exit(-1);
      }
      sscanf(argv[i++], "%i", &chunk_size);
      name = argv[i++];
      printf("Saving document map to %s chunking %i\n", name, chunk_size);
      aux = fopen(name, "w+");
      if( ! aux ) {
        printf("Could not open aux. output file %s\n", name);
        die_if_err(ERR_IO_UNK);
      }
    } else aux = NULL;
  }

  start_clock();
  // step 1: prepare the text.
  // initialize the structure
  printf("Reading and preparing text\n"); 
  start_clock();
  err = init_prepared_text(&bwt_globals.p, info_path);
  die_if_err(err);
  
  // read in any documents
  err = init_file_find(&bwt_globals.ffs, 1, &path);
  die_if_err(err);
  err = file_find(&bwt_globals.ffs, size_file, &bwt_globals);
  die_if_err(err);
  free_file_find(&bwt_globals.ffs);

  // For each file... 
  err = init_file_find(&bwt_globals.ffs, 1, &path);
  die_if_err(err);
  err = file_find(&bwt_globals.ffs, read_file, &bwt_globals);
  die_if_err(err);
  free_file_find(&bwt_globals.ffs);


  stop_clock();

  {
    int64_t num_chars, num_docs; 
    err = prepared_num_docs( &bwt_globals.p, &num_docs);
    die_if_err(err);
    err = prepared_num_chars( &bwt_globals.p, &num_chars);
    die_if_err(err);


    printf("Read %"PRIi64 " chars %" PRIi64 " documents \n",
           num_chars, num_docs);

    print_timings("preparing bytes", num_chars );
  }


  printf("Creating output\n");
  err = save_prepared_bwt(&bwt_globals.p, mark_period,
                          output, chunk_size, aux, 1);
  die_if_err(err);
  
  free_prepared_text(&bwt_globals.p);

  return 0;
}
