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

  femto/src/dcx_cc/index_tool_support_files.c
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
#include <assert.h>
#include <limits.h>

#include "error.h"
#include "util.h"
#include "page_utils.h"
#include "index_tool_support.h"
#include "file_find.h"

file_find_state_t file_find_state;

void its_usage(FILE* out)
{
  fprintf(out, "[--from-list file] [--from-list0 file] [file] [file] ...\n");
  fprintf(out, " --from-list gets filenames from a listing file, one name per line\n");
  fprintf(out, " --from-list0 gets filenames from a listing file, filenames are separated by zero bytes\n");
}

// Initialize our file find state with the passed paths.
int its_use_arguments(int num_args, const char** args)
{
  const char** roots = NULL;
  int roots_count = 0;
  int i;
  error_t err = 0;
  char* tmp;

  for( i = 0; i < num_args; i++ ) {
    if( 0 == strcmp(args[i], "--from-list") || 0 == strcmp(args[i], "--from-list0") ) {
      int delim = '\n';
      if( 0 == strcmp(args[i], "--from-list0") ) delim = '\0';

      i++;
      if( i == num_args ) break;

      // Read all of the lines of the file and append them to
      // our array.
      {
        FILE* f;
        char* line = NULL;
        int line_count = 0;
        int c;
        char ch;

        f = fopen(args[i], "r");
        while( 1 ) {
          c = fgetc(f);
          if( c == delim || c == EOF ) {
            if( line_count > 0 ) {
              tmp = malloc(line_count + 1);
              if( ! tmp ) return -103;
              memcpy(tmp, line, line_count);
              tmp[line_count] = '\0';
              err = append_array(&roots_count, &roots, sizeof(char*), &tmp);
              free(line);
              line = NULL;
              line_count = 0;
            }
            if( c == EOF ) break;
          } else {
            ch = c;
            err = append_array(&line_count, &line, sizeof(char), &ch);
          }
        }
        fclose(f);
      }
    } else {
      tmp = strdup(args[i]);
      if( ! tmp ) return -100;
      err = append_array(&roots_count, &roots, sizeof(char*), &tmp);
    }
    if( err ) return -200;
  }


  if( roots_count == 0 ) {
    index_tool_usage();
    printf("Additional arguments should be file or directory names to index.\n");
    return -1;
  }

  // Make sure that all of the files exist.
  for( i = 0; i < roots_count; i++ ) {
    struct stat s;
    int rc;
    rc = stat(roots[i], &s);
    if( rc != 0 ) {
      printf("Could not stat '%s'\n", roots[i]);
      return -402;
    }
  }

  err = init_file_find(&file_find_state, roots_count, roots);
  if( err ) {
    printf("Could not initialize directory traversal\n");
    return -300;
  }

  for( i = 0; i < roots_count; i++ ) {
    free((char*) roots[i]);
  }
  free(roots);

  return 0;
}


int its_get_doc_info(int64_t doc_num,
                     int64_t* doc_len_out,
                     int64_t* doc_info_len_out,
                     unsigned char** doc_info_out,
                     int64_t* num_doc_headers_out, 
                     int64_t** doc_header_lens_out)
{
  struct stat st;
  char* path;
  error_t err;
  int rc;

  path = NULL;
  err = file_find_get_next(&file_find_state, &path);
  if( err ) {
    warn_if_err(err);
    return -1;
  }

  // No more documents!
  if( !path ) return 0;

  // Otherwise, get the document length, etc.
  rc = stat(path, &st);
  if( rc != 0 ) {
   warn_if_err(ERR_IO_STR_OBJ("Could not stat", path));
   return -1;
  }

  if( ! S_ISREG(st.st_mode) ) {
    warn_if_err(ERR_IO_STR_OBJ("Not regular file", path));
    return -1;
  }

  *doc_len_out = st.st_size;

  *doc_info_len_out = strlen(path);
  *doc_info_out = (unsigned char*) path;
  *num_doc_headers_out = 0;
  *doc_header_lens_out = NULL;

  return 1;
}

int its_switch_passes(void)
{
  error_t err;
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
  struct stat st;
  char* path;
  error_t err;
  int rc;
  void* data;
  int fd;

  path = NULL;
  err = file_find_get_next(&file_find_state, &path);
  if( err ) {
    warn_if_err(err);
    return -1;
  }

  // No more documents!
  if( !path ) return 0;

  // Otherwise, get the document length, etc.
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
    err = ERR_IO_STR_OBJ("Could not open", path);
    warn_if_err(err);
    return -1;
  }

  if( st.st_size > 0 ) {
    data = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);

    if( data == NULL || data == MAP_FAILED ) {
      err = ERR_IO_STR_OBJ("Could not mmap", path);
      warn_if_err(err);
      return -1;
    }

    // madvise sequential.
    err = advise_sequential_pages(data, st.st_size);
    warn_if_err(err);
    // failed madvise does not cause total failure.
 
  } else {
    // A size 0 file!
    data = NULL;
  }

  rc = close(fd);
  if( rc ) {
    err = ERR_IO_STR_OBJ("Could not close", path);
    warn_if_err(err);
    return -1;
  }

  *doc_len_out = st.st_size;
  *doc_info_len_out = strlen(path);
  *doc_info_out = (unsigned char*) path;
  *num_doc_headers_out = 0;
  *doc_header_lens_out = NULL;
  *doc_headers_out = NULL;
  *doc_contents_out = data;

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

  if( doc_info ) {
    free(doc_info);
  }

  if( doc_contents ) {
    rc = munmap(doc_contents, doc_len);
    if( rc ) {
      error_t err = ERR_IO_STR("Could not munmap");
      warn_if_err(err);
      return -1;
    }
  }

  return 0;
}

void its_cleanup(void)
{
  free_file_find(&file_find_state);
}
