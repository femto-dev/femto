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

  femto/src/main/block_storage.h
*/

#ifndef _BLOCK_STORAGE_H_
#define _BLOCK_STORAGE_H_ 1

#include <inttypes.h>
#include <stdio.h>
#include <pthread.h>
#include "error.h"
#include "hashmap.h"
#include "buffer.h"

typedef struct {
  uint32_t start;
  uint32_t vers;
  int64_t num_blocks;
} flattened_header_t;

typedef struct {
  const char* path;
  // these are used if it's a file:
  int64_t num_blocks;
  const void* hdr_map; // set if it's a file.
  int64_t hdr_map_len; // in bytes
} id_to_path_t;

typedef struct {
  pthread_rwlock_t rwlock;
  hashmap_t path_to_id;
  int next_id;
  id_to_path_t* id_to_path; // numbers start with 1, so slot 0 is NULL
} path_translator_t;


typedef struct {
  intptr_t id; // the index identifier. (uniquely identifies this index). Key for hashtable-lookups. Use path_translator_t to get back path.
} index_locator_t;

error_t path_translator_init(path_translator_t* t);
void path_translator_destroy(path_translator_t* t);
error_t path_translator_id_for_path(path_translator_t* t, const char* path, index_locator_t* out);
error_t path_translator_path_for_id(path_translator_t* t, index_locator_t id, const char** out, int* isfile);

#define PRlocid PRIiPTR

hash_t hash_index_locator(index_locator_t a);

static inline index_locator_t null_index_locator(void)
{
  index_locator_t ret = {0};
  return ret;
}
static inline int index_locator_is_valid(index_locator_t a)
{
  return (a.id > 0);
}
static inline int index_locator_cmp(index_locator_t a, index_locator_t b)
{
  if( a.id == b.id ) return 0;
  else if (a.id < b.id ) return -1;
  else return 1;
}
static inline int index_locator_equals(index_locator_t a, index_locator_t b)
{
  return 0 == index_locator_cmp(a,b);
}


typedef struct {
  int64_t block_number; // the block number
  const char* location;
  int mode; // 0 for reading, 1 for writing.
  int isnew; // 1 if this block was created when it was opened
  FILE* f;
} stored_block_t;

typedef enum {
  STORED_BLOCK_MODE_NONE,
  STORED_BLOCK_MODE_READ,
  STORED_BLOCK_MODE_WRITE,
  STORED_BLOCK_MODE_CREATE_WRITE,
} stored_block_mode_t;

// The returned filename must be freed by the caller.
char* create_filename_for_block(int64_t block_number, const char* index_path);

// Creates a stored block - opens the FILE* f 
// The passed stored_block structure will be overwritten.
// If mode is CREATE_WRITE, a file representing the block will
// be created and opened for writing, and the block will
// be stored in the repository when it is closed.
// if the mode is STORED_BLOCK_MODE_READ, the block is 
// pulled from the repository and opened for reading.
error_t open_stored_block(stored_block_t* stored_block,
                          int64_t block_number,
                          const char* index_path,
                          stored_block_mode_t mode);
error_t open_stored_block_byloc(stored_block_t* stored_block,
                                path_translator_t* t,
                                int64_t block_number,
                                index_locator_t location,
                                buffer_t* mmap_out);


#define FLATTENED_START 0xb1497dea
#define FLATTENED_VERSION 0x00000006

// The block changes mode from STORED_BLOCK_...WRITE
// to new_mode. Changes are saved in the repository.
// Error if mode wasn't writeable.
error_t commit_stored_block(stored_block_t* block, stored_block_mode_t new_mode);

// Closes a stored block.
// the FILE* f will be closed
void close_stored_block(stored_block_t* block);

int isnew_stored_block(stored_block_t* block);

error_t foreach_stored_block(void* ctx,
    error_t(*doit)(index_locator_t loc, int64_t block_number, void* ctx));

error_t remove_stored_block(path_translator_t* t,
                            int64_t block_number,
                            index_locator_t location);

error_t delete_storage_dir(char* index_path);

#endif
