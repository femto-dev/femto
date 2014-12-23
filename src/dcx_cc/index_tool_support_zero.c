/*
  (*) 2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/dcx_cc/index_tool_support_zero.c
*/



// See this header file for an explanation
// of each of these functions.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <ctype.h>
#include <assert.h>

#include "error.h"
#include "page_utils.h"
#include "index_tool_support.h"
#include "file_find.h"

file_find_state_t file_find_state;
char* path;
unsigned char* data;
size_t data_len;
size_t cur_offset; // offset of the current

int do_munmap(void)
{
  int rc;
  if( data ) {
    rc = munmap(data, data_len);
    if( rc ) {
      error_t err = ERR_IO_STR("Could not munmap");
      warn_if_err(err);
      return rc;
    }
  }
  if( path ) {
    free(path);
  }
  path = NULL;
  data = NULL;
  data_len = 0;
  cur_offset = 0;

  return 0;
}

// Takes over path_in ptr; we will free it in do_munmap.
int do_mmap(char* path_in)
{
  struct stat st;
  int rc;
  int fd;
  error_t err;

  printf("%s\n", path_in);

  if( path ) do_munmap();

  path = path_in;

  cur_offset = 0;
  data = NULL;
  data_len = 0;

  // Get the document length, etc.
  rc = stat(path, &st);
  if( rc != 0 ) {
   warn_if_err(ERR_IO_STR_OBJ("Could not stat", path));
   return -1;
  }
  
  if( ! S_ISREG(st.st_mode) ) {
    warn_if_err(ERR_IO_STR_OBJ("Not regular file", path));
    return -1;
  }

  fd = open(path, O_RDONLY);
  if( fd < 0 ) {
    warn_if_err(ERR_IO_STR_OBJ("Could not open", path));
    return -1;
  }

  data_len = st.st_size;

  if( data_len > 0 ) {
    data = mmap(NULL, data_len, PROT_READ, MAP_SHARED, fd, 0);

    if( data == NULL || data == MAP_FAILED ) {
      warn_if_err(ERR_IO_STR_OBJ("Could not mmap", path));
      return -1;
    }

    // madvise sequential.
    err = advise_sequential_pages(data, data_len);
    warn_if_err(err);
    // failed madvise does not cause total failure.

    //err = advise_need_pages(data, data_len);
    //warn_if_err(err);
    
  } else {
    // A size 0 file!
    data = NULL;
  }

  rc = close(fd);
  if( rc ) {
    warn_if_err(ERR_IO_STR_OBJ("Could not close", path));
    return -1;
  }

  return 0;
}

// Returns 0 for no error.
int parse_data(const unsigned char* data, size_t data_len,
               size_t cur_offset,
               size_t* info_len, unsigned char** info_data,
               size_t* doc_len, unsigned char** doc_data,
               size_t* next_offset)
{
  size_t start, end;


  *info_len = 0;
  *info_data = NULL;
  *doc_len = 0;
  if( doc_data ) *doc_data = 0;
  *next_offset = cur_offset;

  if( cur_offset >= data_len ) {
    return -20; // no more data!
  }

  // Read the line - it will be both the document and the content 
  if( cur_offset >= data_len ) {
    return -200; // no more data!
  }

  start = cur_offset;

  // Find the end of the header string.
  for( end = start; end < data_len; end++ ) {
    if( data[end] == 0 ) break;
  }
  
  //printf("Got Document %s\n", &data[start]);
  // Doc info == doc data
  *info_len = end - start;
  *info_data = (unsigned char*) &data[start];
  *doc_len = end - start;
  if( doc_data ) *doc_data = (unsigned char*) &data[start];
  *next_offset = end + 1;

  return 0;
}

void its_usage(FILE* out)
{
  fprintf(out, "[lines-file] [lines-file] ...\n");
}


// Initialize our file find state with the passed paths.
int its_use_arguments(int num_args, const char** args)
{
  if( num_args == 0 ) {
    index_tool_usage();
    printf("Additional arguments should be file or directory names to index.\n");
    return -1;
  }

  init_file_find(&file_find_state, num_args, args);

  return 0;
}


int its_get_doc_info(int64_t doc_num,
                     int64_t* doc_len_out,
                     int64_t* doc_info_len_out,
                     unsigned char** doc_info_out,
                     int64_t* num_doc_headers_out, 
                     int64_t** doc_header_lens_out)
{
  char* got_path;
  error_t err;
  int rc;
  size_t info_len, doc_len;
  unsigned char* info_data;
  size_t next_offset;

  // First, move on to the next file if we need to.
  while( !data || cur_offset >= data_len ) {
    got_path = NULL;
    err = file_find_get_next(&file_find_state, &got_path);
    if( err ) {
      warn_if_err(err);
      return -1;
    }

    // No more documents!
    if( !got_path ) return 0;

    rc = do_mmap(got_path);
    if( rc ) return rc;
  }

  // Now, get some data from the current file.
  rc = parse_data(data, data_len, cur_offset,
                  &info_len, &info_data,
                  &doc_len, NULL,
                  &next_offset);
  if( rc ) return rc;

  cur_offset = next_offset;

  *doc_len_out = doc_len;
  *doc_info_len_out = info_len;
  *doc_info_out = info_data;
  *num_doc_headers_out = 0;
  *doc_header_lens_out = NULL;

  return 1;
}

int its_switch_passes(void)
{
  error_t err;

  do_munmap();

  err = reset_file_find(&file_find_state);
  if( err ) {
    warn_if_err(err);
    return -1;
  }

  return 0;
}

int its_get_doc(int64_t doc_num,
                int64_t* doc_len_out,
                int64_t* doc_info_len_out,
                unsigned char** doc_info_out,
                int64_t* num_doc_headers_out,
                int64_t** doc_header_lens_out,
                unsigned char*** doc_headers_out,
                unsigned char** doc_contents_out)
{
  char* got_path;
  error_t err;
  int rc;
  size_t info_len, doc_len;
  unsigned char* info_data;
  unsigned char* doc_data;
  size_t next_offset;

  // First, move on to the next file if we need to.
  while( !data || cur_offset >= data_len ) {
    got_path = NULL;
    err = file_find_get_next(&file_find_state, &got_path);
    if( err ) {
      warn_if_err(err);
      return -1;
    }

    // No more documents!
    if( !got_path ) return 0;

    rc = do_mmap(got_path);
    if( rc ) return rc;
  }

  // Now, get some data from the current file.
  rc = parse_data(data, data_len, cur_offset,
                  &info_len, &info_data,
                  &doc_len, &doc_data,
                  &next_offset);
  if( rc ) return rc;

  cur_offset = next_offset;

  *doc_len_out = doc_len;
  *doc_info_len_out = info_len;
  *doc_info_out = info_data;
  *num_doc_headers_out = 0;
  *doc_header_lens_out = NULL;

  *doc_headers_out = NULL;
  *doc_contents_out = doc_data;
  return 1;
}

int its_free_doc(int64_t doc_num,
                 int64_t doc_len,
                 int64_t doc_info_len,
                 unsigned char* doc_info,
                 int64_t num_doc_headers,
                 int64_t* doc_header_lens,
                 unsigned char** doc_headers,
                 unsigned char* doc_contents)
{
  int rc;

  assert(doc_header_lens == NULL);
  assert(doc_headers == NULL);

  // doc_info and doc_contents point into mmap'd region
  // which we will free when the time comes...
  if( cur_offset == data_len ) {
    // we're at the end of a file.
    rc = do_munmap();
    if( rc ) return rc;
  }

  return 0;
}

void its_cleanup(void)
{
  free_file_find(&file_find_state);
  do_munmap();
}
