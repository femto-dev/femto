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
#include "hashmap.h"

#ifdef HAVE_LIBSSL
#include <openssl/sha.h>
#endif

int dedup;

typedef struct MYHASH_value_s {
  int npaths;
  const char** paths;
} MYHASH_value;


#ifdef HAVE_LIBSSL
typedef struct MYHASH_key_s {
  unsigned char h[SHA_DIGEST_LENGTH];
} MYHASH_key;

static
void MYHASH_print(const MYHASH_key * h, const char* path)
{
  int i;
  for( i = 0; i < SHA_DIGEST_LENGTH; i++ ) {
    printf("%02x", h->h[i]);
  }
  printf("  %s\n", path);
}

static
hash_t MYHASH_hash(const void* tohash_v)
{
  const MYHASH_key* tohash = (const MYHASH_key*) tohash_v;
  hash_t ret;
  memcpy(&ret, &tohash->h[0], sizeof(hash_t));
  return ret;
}
static
int MYHASH_cmp(const void* a_v, const void* b_v)
{
  const MYHASH_key* a = (const MYHASH_key*) a_v;
  const MYHASH_key* b = (const MYHASH_key*) b_v;
  return memcmp(a, b, sizeof(MYHASH_key));
}

hashmap_t dedup_table;

// For the canonical version of a name, goes to the MYHASH_value record.
// For a duplicate, goes to NULL.
hashmap_t dups;
#endif

#define GLOM_CHAR '|'

file_find_state_t file_find_state;

void its_usage(FILE* out)
{
  fprintf(out, "[--from-list file] [--from-list0 file] [file] [file] ...\n");
  fprintf(out, " --from-list gets filenames from a listing file, one name per line\n");
  fprintf(out, " --from-list0 gets filenames from a listing file, filenames are separated by zero bytes\n");
  fprintf(out, " --dedup activate file hash-based deduplication. \n"
               "         When duplicates occur, the index will return documents\n"
               "         like some/path/to/file|some/path/to/copy\n");
}

#ifdef HAVE_LIBSSL
static
error_t do_dedup_file(const char* path)
{
  struct stat st;
  error_t err;
  int rc;
  void* data;
  int fd;
  MYHASH_key h;
  hm_entry_t entry;

  if( NULL != strchr(path, GLOM_CHAR) ) return ERR_IO_STR_OBJ("Path contains glom character ", path);

  // Otherwise, get the document length, etc.
  rc = stat(path, &st);
  if( rc != 0 ) {
   return ERR_IO_STR_OBJ("Could not stat", path);
  }

  if( ! S_ISREG(st.st_mode) ) {
    return ERR_IO_STR_OBJ("Not regular file", path);
  }

  fd = open(path, O_RDONLY);
  if( fd < 0 ) {
    return ERR_IO_STR_OBJ("Could not open", path);
  }

  if( st.st_size > 0 ) {
    data = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);

    if( data == NULL || data == MAP_FAILED ) {
      return ERR_IO_STR_OBJ("Could not mmap", path);
    }

    // madvise sequential.
    err = advise_sequential_pages(data, st.st_size);
    warn_if_err(err);
    // failed madvise does not cause total failure.
 
  } else {
    // A size 0 file!
    data = NULL;
  }

  // Deduplicate this file!
  memset(&h, 0, sizeof(MYHASH_key));
  SHA1(data, st.st_size, &h.h[0]);
  //printf("chk "); MYHASH_print(&h, path);
  // Populate the hashtable.
  entry.key = &h;
  entry.value = NULL;
  if( hashmap_retrieve(&dedup_table, &entry) ) {
   MYHASH_key* k = (MYHASH_key*) entry.key;
    MYHASH_value* v = (MYHASH_value*) entry.value;
    // Got an entry
    // Glom path into the existing hash entry.
    // foo\0 -> foo|bar\0
    err = append_array(&v->npaths, &v->paths, sizeof(char*), &path);
    if( err ) return err;
    // No need to reinsert since we just updated the value.

    // Add this path to the dups hashtable.
    entry.key = (void*) path;
    entry.value = NULL;
    err = hashmap_resize(&dups);
    if( err ) return err;
    err = hashmap_insert(&dups, &entry);
    if( err ) return err;

    printf("dup "); MYHASH_print(k, path);
    entry.key = (void*) path;
    entry.value = NULL;
    assert( hashmap_retrieve(&dups, &entry) );
    assert(entry.value == NULL);

  } else {
    MYHASH_key* k = malloc(sizeof(MYHASH_key));
    MYHASH_value* v = malloc(sizeof(MYHASH_value));
    if( !k ) return ERR_MEM;
    if( !v ) return ERR_MEM;
    *k = h;
    v->npaths = 0;
    v->paths = NULL;
    err = append_array(&v->npaths, &v->paths, sizeof(char*), &path);
    if( err ) return err;
    // Add this hash to the dedup table.
    entry.key = k;
    entry.value = v;
    err = hashmap_resize(&dedup_table);
    if( err ) return err;
    err = hashmap_insert(&dedup_table, &entry);
    if( err ) return err;

    entry.key = k;
    entry.value = NULL;
    assert( hashmap_retrieve(&dedup_table, &entry) );
    assert(entry.value == v);

    // Add this path to the dups hashtable.
    entry.key = (void*) path;
    entry.value = v;
    err = hashmap_resize(&dups);
    if( err ) return err;
    err = hashmap_insert(&dups, &entry);
    if( err ) return err;

    printf("new "); MYHASH_print(k, path);
    entry.key = (void*) path;
    entry.value = NULL;
    assert( hashmap_retrieve(&dups, &entry) );
    assert(entry.value == v);
  }

  if( data ) {
    rc = munmap(data, st.st_size);
    if( rc ) {
      return ERR_IO_STR("Could not munmap");
    }
  }

  rc = close(fd);
  if( rc ) {
    return ERR_IO_STR_OBJ("Could not close", path);
  }

  return ERR_NOERR;
}

static
error_t do_dedup(file_find_state_t* s)
{
  error_t err;
  char* path;

  if( ! dedup ) return 1;
 
  // For each file, compute its MD5-sum
  // populate the MYHASH table
  while( 1 ) {
    path = NULL;
    err = file_find_get_next(s, &path);
    if( err ) return err;

    if( path == NULL ) break;

    // Now dedup this file!
    err = do_dedup_file(path);
    if( err ) return err;

    // Normally we would free path, but don't
    // because we will leave hashtable entries pointing to it!
    //free(path);
  }

  return ERR_NOERR;
}  
#endif

// Initialize our file find state with the passed paths.
int its_use_arguments(int num_args, const char** args)
{
  const char** roots = NULL;
  int roots_count = 0;
  int i;
  error_t err = 0;
  char* tmp;

  for( i = 0; i < num_args; i++ ) {
    if( 0 == strcmp(args[i], "--dedup") ) {
#ifdef HAVE_LIBSSL
      dedup = 1;
#else
      printf("Not compiled with -lssh (openssl); can't dedup\n");
      return -150;
#endif
    } else if( 0 == strcmp(args[i], "--from-list") || 0 == strcmp(args[i], "--from-list0") ) {
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

  if( dedup ) {
#ifdef HAVE_LIBSSL
    printf("Deduping....\n");
    // Create the hashtables.
    err = hashmap_create(&dups, 4*1024, hash_string_fn, hash_string_cmp);
    if( err ) {
      warn_if_err(err);
      printf("Could not create dups hashtable\n");
      return -649;
    }
    err = hashmap_create(&dedup_table, 32*1024, MYHASH_hash, MYHASH_cmp);
    if( err ) {
      warn_if_err(err);
      printf("Could not create dedup_table hashtable\n");
      return -648;
    }
  
    err = do_dedup(&file_find_state);
    if( err ) {
      warn_if_err(err);
      printf("Could not dedup\n");
      return -241;
    }
    printf("Done Deduping.\n");

    err = reset_file_find(&file_find_state);
    if( err ) {
      warn_if_err(err);
      return -1;
    }
#endif
  }

  return 0;
}


static
error_t get_next_non_dup_path(char** path_out, MYHASH_value** dups_out)
{
  error_t err = ERR_NOERR;
  char* path;
  MYHASH_value* d;
  hm_entry_t entry;
  while( 1 ) {
    path = NULL;
    d = NULL;
    err = file_find_get_next(&file_find_state, &path);
    if( err ) break;
    if( !path ) break;
    if( !dedup ) break;
#ifdef HAVE_LIBSSL
    entry.key = path;
    entry.value = NULL;
    if( !hashmap_retrieve(&dups, &entry) ) {
      printf("Missing from dups hashmap? Data added? %s\n", path);
    } else {
      if( entry.value ) {
        // Canonical version!
        d = (MYHASH_value*) entry.value;
        break;
      } // otherwise, continue looping.
    }
    free(path);
#else
    break;
#endif
  }
  *path_out = path;
  *dups_out = d;
  return err;
}

// Returns a replacement for path.
// Frees path if it replaces it.
static
char* get_glommed_path(char* path, MYHASH_value* dups)
{
  size_t total_len;
  size_t len;
  int i;
  if( dups && dups->npaths > 1 ) {
    free(path);
    total_len = 0;
    for( i = 0; i < dups->npaths; i++ ) {
      len = strlen(dups->paths[i]);
      total_len += len; // room for string
      total_len ++; // and room for | or '\0'.
    }
    path = malloc(total_len);
    total_len = 0;
    for( i = 0; i < dups->npaths; i++ ) {
      len = strlen(dups->paths[i]);
      memcpy(&path[total_len], dups->paths[i], len);
      total_len += len;
      path[total_len] = GLOM_CHAR;
      total_len ++;
    }
    // Replace the last GLOM_CHAR with an end-of-string.
    path[total_len-1] = '\0';
  }
  return path;
}

int its_get_doc_info(int64_t doc_num,
                     int64_t* doc_len_out,
                     int64_t* doc_info_len_out,
                     unsigned char** doc_info_out,
                     int64_t* num_doc_headers_out, 
                     int64_t** doc_header_lens_out)
{
  struct stat st;
  char* path = NULL;
  MYHASH_value* dups = NULL;
  error_t err;
  int rc;

  err = get_next_non_dup_path(&path, &dups);
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

  // Replace the path with the glommed path (doc info)
  path = get_glommed_path(path, dups);

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
  char* path = NULL;
  MYHASH_value* dups = NULL;
  error_t err;
  int rc;
  void* data;
  int fd;

  err = get_next_non_dup_path(&path, &dups);
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

  // Replace the path with the glommed path (doc info)
  path = get_glommed_path(path, dups);

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
