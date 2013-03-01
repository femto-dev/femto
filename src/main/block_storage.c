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

  femto/src/main/block_storage.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>

#include <limits.h>

#include "block_storage.h"
#include "util.h"
#include "buffer.h"
#include "bswap.h"

#include "index_types.h" // for DEBUG


error_t path_translator_init(path_translator_t* t)
{
  error_t err;
  int rc;
  id_to_path_t none;

  none.path = NULL;
  none.num_blocks = 0;
  none.hdr_map = NULL;
  none.hdr_map_len = 0;

  memset(t, 0, sizeof(path_translator_t));

  rc = pthread_rwlock_init(&t->rwlock, NULL);
  if( rc ) {
    err = ERR_PTHREADS("Could not create rw lock", rc);
    goto error;
  }

  err = hashmap_create(&t->path_to_id, 128, hash_string_fn, hash_string_cmp);
  if( err ) goto error;

  err = append_array(&t->next_id, &t->id_to_path, sizeof(id_to_path_t), &none);
  if( err ) goto error;

  assert(t->next_id == 1);

  return ERR_NOERR;

error:
  path_translator_destroy(t);
  return err;
}

void path_translator_destroy(path_translator_t* t)
{
  int rc;

  hashmap_destroy(&t->path_to_id);

  for( int i = 0; i < t->next_id; i++ ) {
    id_to_path_t* x = & t->id_to_path[i];
    if( x->hdr_map ) {
      rc = munmap((void*) x->hdr_map, x->hdr_map_len);
      if( rc ) {
        warn_if_err(
          ERR_IO_STR_OBJ("error in munmap index table", x->path));
      }
    }
    free((void*) x->path);
  }
  free(t->id_to_path);

  rc = pthread_rwlock_destroy(&t->rwlock);
  if( rc ) {
    warn_if_err(ERR_PTHREADS("Could not destroy rw lock", rc));
  }
}

error_t path_translator_id_for_path(path_translator_t* t, const char* path, index_locator_t* out)
{
  // Is it in the hashtable?
  int found;
  hm_entry_t entry;
  intptr_t id;
  error_t err;
  id_to_path_t x;
  int rc;
  struct stat stats;
  int fd = -1;
  flattened_header_t hdr;
  ssize_t got;
  void* data;
  int isfile;

  x.path = NULL;
  x.num_blocks = 0;
  x.hdr_map = NULL;
  x.hdr_map_len = 0;

  entry.key = (void*) path;
  entry.value = (void*) 0;

  rc = pthread_rwlock_rdlock(&t->rwlock);
  assert(!rc);

  found = hashmap_retrieve(&t->path_to_id, &entry);
  if( found ) {
    id = (intptr_t) entry.value; 
    assert( id > 0 );
    assert( id < (intptr_t) t->next_id );
  } else {
    x.path = strdup(path);
    if( ! x.path ) {
      err = ERR_MEM;
      goto error;
    }

    // Decide if it's a file or a directory.
    rc = stat(x.path, &stats);
    if( rc != 0 ) {
      err = ERR_IO_STR_OBJ("stat failed", x.path);
      goto error;
    }

    if( S_ISDIR(stats.st_mode) ) isfile = 0;
    else if( S_ISREG(stats.st_mode) ) isfile = 1;
    else {
      err = ERR_IO_STR_OBJ("index not file or directory", x.path);
      goto error;
    }

    if( isfile ) {
      // mmap the offsets table
      fd = open(x.path, O_RDONLY);
      if( fd == -1 ) {
        err = ERR_IO_STR_OBJ("failed to open index file", x.path);
        goto error;
      }

      //read the header.
      got = read(fd, &hdr, sizeof(flattened_header_t));
      if( got != sizeof(flattened_header_t) ) {
        err = ERR_IO_STR_OBJ("failed to read flattened header", x.path);
        goto error;
      }

      // convert to host byte order.
      hdr.start = ntoh_32(hdr.start);
      hdr.vers = ntoh_32(hdr.vers);
      hdr.num_blocks = ntoh_64(hdr.num_blocks);

      if( hdr.start != FLATTENED_START ||
          hdr.vers != FLATTENED_VERSION ||
          hdr.num_blocks <= 0 ) {
        err = ERR_FORMAT;
        goto error;
      }

      x.num_blocks = hdr.num_blocks;
      x.hdr_map_len = sizeof(flattened_header_t) + (x.num_blocks+1)*sizeof(int64_t);

      // OK, now mmap the offsets table.
      data = mmap(NULL, x.hdr_map_len, PROT_READ, MAP_SHARED, fd, 0);
      if( ! data || data == MAP_FAILED ) {
        err = ERR_IO_STR_OBJ("failed to mmap index file", x.path);
        goto error;
      }

      x.hdr_map = data;
    }

    // Re-acquire the lock as a writing lock.
    rc = pthread_rwlock_unlock(&t->rwlock);
    assert(!rc);
    rc = pthread_rwlock_wrlock(&t->rwlock);
    assert(!rc);

    id = t->next_id;

    err = append_array(&t->next_id, &t->id_to_path, sizeof(id_to_path_t), &x);
    if( err ) goto error;

    entry.key = (void*) x.path;
    entry.value = (void*) id;
    err = hashmap_insert(&t->path_to_id, &entry);
    if( err ) {
      goto error;
    }
  }

  rc = pthread_rwlock_unlock(&t->rwlock);
  assert(!rc);

  if( fd != -1 ) close(fd);

  out->id = id;
  return ERR_NOERR;

error:
  rc = pthread_rwlock_unlock(&t->rwlock);
  assert(!rc);

  if( x.path ) free( (void*) x.path);
  if( x.hdr_map ) munmap((void*) x.hdr_map, x.hdr_map_len);
  if( fd != -1 ) close(fd);

  out->id = 0;
  return err;
}

static
error_t path_translator_path_for_id_unlocked(path_translator_t* t, index_locator_t id, id_to_path_t** out)
{
  error_t err;

  if( id.id <= 0 ) err = ERR_INVALID;
  else if( id.id >= t->next_id ) err = ERR_INVALID;
  else {
    err = ERR_NOERR;
    *out = &t->id_to_path[id.id];
  }

  return err;
}

hash_t hash_index_locator(index_locator_t a)
{
  return hash_data((unsigned char*) &a.id, sizeof(intptr_t));
}

// the returned filename must be freed by the caller
char* create_filename_for_block(int64_t block_number, const char* index_path)
{
  char* fname;

  fname = (char*) malloc(strlen(index_path)+100);
  if( ! fname ) return NULL;
  sprintf(fname, "%s/%02" PRIx64, index_path, block_number);

  return fname;
}

error_t delete_storage_dir(char* index_path)
{
  // go through the directory.
  DIR* dir = NULL;
  struct dirent* ent = NULL;
  error_t err;
  int rc;
  char* fname;
  char* path = NULL;
  int len;
 
  dir = opendir(index_path);
  if( ! dir ) {
    return ERR_IO_STR_OBJ("Could not open block storage directory", index_path);
  }

  while( ( ent = readdir(dir) ) ) {

    fname = ent->d_name;

    if( 0 == strcmp(fname, "..") || 0 == strcmp(fname, ".") ) continue;

    len = strlen(index_path)+strlen(fname)+2;
    path = malloc(len);
    if( ! path ) {
      err = ERR_MEM;
      goto error;
    }
    sprintf(path, "%s/%s", index_path, fname);

    rc = unlink(path);
    if( rc ) {
      err = ERR_IO_STR("Could not delete");
      goto error;
    }

    free(path);
    path = NULL;
  }

  {
    fname = "_femto_index";
    len = strlen(index_path)+strlen(fname)+2;
    path = malloc(len);
    if( ! path ) {
      err = ERR_MEM;
      goto error;
    }
    sprintf(path, "%s/%s", index_path, fname);

    rc = unlink(path);
    if( rc ) {
      if( errno != ENOENT ) {
        err = ERR_IO_STR("Could not delete");
        goto error;
      }
    }

    free(path);
    path = NULL;
  }

  // now delete the directory.
  rc = rmdir(index_path);
  if( rc ) {
    err = ERR_IO_STR("Could not delete");
    goto error;
  }

  err = ERR_NOERR;

error:
  if( path ) free(path);
  closedir(dir);
  return err;
}

error_t setup_stored_block(stored_block_t* stored_block, 
                           int64_t block_number, 
                           const char* location,
                           stored_block_mode_t mode)
{
  memset(stored_block, 0, sizeof(stored_block_t));
 
  stored_block->block_number = block_number;

  stored_block->location = location;
   
  stored_block->mode = mode;

  return ERR_NOERR;
}

// frees any memory allocated by setup
void cleanup_stored_block(stored_block_t* stored_block)
{
  memset(stored_block, 0, sizeof(stored_block_t));
}

// Remove a stored block
error_t remove_stored_block(path_translator_t* t,
                            int64_t block_number,
                            index_locator_t location)
{
  int ret;
  char* fname;
  id_to_path_t* x = NULL;
  error_t err;
  int rc;

  rc = pthread_rwlock_rdlock(&t->rwlock);
  assert(!rc);

  err = path_translator_path_for_id_unlocked(t, location, &x);
  if( err ) goto error;

  fname = create_filename_for_block(block_number, x->path);
  if( ! fname ) {
    err = ERR_MEM;
    goto error;
  }
  ret = remove(fname);
  free(fname);

  err = ERR_NOERR;
  if( ret ) {
    if( errno == ENOENT ) {
      // ignore "didn't exist" errors
      err = ERR_NOERR;
    }
    err = ERR_IO_STR_OBJ("Could not remove block", fname);
  }

error:
  rc = pthread_rwlock_unlock(&t->rwlock);
  assert(!rc);

  return err;
}

// Creates a stored block - opens the FILE* f
// The passed stored_block structure will be overwritten.
// If mode is CREATE_WRITE, a file representing the block will
// be created and opened for writing.
error_t open_stored_block(stored_block_t* stored_block,
                          int64_t block_number,
                          const char* index_path,
                          stored_block_mode_t mode)
{

  char* fname = NULL;
  error_t err;

  err = setup_stored_block(stored_block, block_number, index_path, mode);
  if( err ) return err;

  fname = create_filename_for_block(block_number, index_path);
  if( ! fname ) return ERR_MEM;

  if( DEBUG ) printf("Opening block %s\n", fname);

  switch (stored_block->mode) {
    case STORED_BLOCK_MODE_NONE:
      break;
    case STORED_BLOCK_MODE_READ:
      stored_block->f = fopen(fname, "r");
      if( ! stored_block->f ) {
        err = ERR_IO_STR_OBJ("Could not open block", fname);
      }
      break;
    case STORED_BLOCK_MODE_WRITE:
      stored_block->f = fopen(fname, "r+");
      if( ! stored_block->f ) {
        err = ERR_IO_STR_OBJ("Could not open block", fname);
      }
      break;
    case STORED_BLOCK_MODE_CREATE_WRITE:
      //stored_block->f = fopen(fname, "r+");
      // try opening an existing file
      //if( ! stored_block->f ) {
        stored_block->f = fopen(fname, "w+");
        if( ! stored_block->f ) {
          err = ERR_IO_STR_OBJ("Could not open block", fname);
        }
        // we created this file.
        stored_block->isnew = 1;
      //}
      break;
  }

  free(fname);

  return err;
}

// Read-only mmap'd access.
error_t open_stored_block_byloc(stored_block_t* stored_block,
                                path_translator_t* t,
                                int64_t block_number,
                                index_locator_t location,
                                buffer_t* mmap_out)
{
  error_t err;
  id_to_path_t* x = NULL;
  int rc;
  void* data;
  int fd = -1;
  int64_t start, end;
  off_t start_off, len_off;
  char* fname = NULL;
  int64_t* offsets;


  rc = pthread_rwlock_rdlock(&t->rwlock);
  assert(!rc);

  err = path_translator_path_for_id_unlocked(t, location, &x);
  if( err ) goto error;

  err = setup_stored_block(stored_block, block_number, x->path, STORED_BLOCK_MODE_READ);
  if( err ) return err;

  if( x->hdr_map ) {
    // index_path is a file in the flattened format.
    // mmap the right region of it.

    fd = open(x->path, O_RDONLY);
    if( fd == -1 ) {
      err = ERR_IO_STR_OBJ("failed to open index file", x->path);
      goto error;
    }

    if( ! x->hdr_map ) {
      err = ERR_INVALID;
      goto error;
    }

    if( block_number >= x->num_blocks ) {
      err = ERR_INVALID;
      goto error;
    }

    offsets = (int64_t*) (x->hdr_map + sizeof(flattened_header_t));

    // read start and end.
    start = ntoh_64(offsets[block_number]);
    end = ntoh_64(offsets[block_number + 1]);

    len_off = end - start;
    start_off = start;
    if( len_off > SSIZE_MAX ) {
      err = ERR_IO_STR_OBJ("index file to big to mmap", x->path);
      goto error;
    }

    if( start_off != start ) {
      err = ERR_IO_STR_OBJ("start too big to mmap", x->path);
      goto error;
    }
    // ok - now mmap the size of the file.
    data = mmap(NULL, len_off, PROT_READ, MAP_SHARED, fd, start_off);
    if( ! data || data == MAP_FAILED ) {
      err = ERR_IO_STR_OBJ("failed to mmap index file", x->path);
      goto error;
    }

    mmap_out->data = data;
    mmap_out->len = mmap_out->max = len_off;
    err = ERR_NOERR;
  } else {
    fname = create_filename_for_block(block_number, x->path);
    if( ! fname ) {
      err = ERR_MEM;
      goto error;
    }

    fd = open(fname, O_RDONLY);
    if( fd == -1 ) {
      err = ERR_IO_STR_OBJ("failed to open index file", fname);
      goto error;
    }

    len_off = lseek(fd, 0, SEEK_END);
    if( len_off == (off_t) -1 ) {
      err = ERR_IO_STR_OBJ("failed to lseek index file", fname);
      goto error;
    }

    if( len_off > SSIZE_MAX ) {
      err = ERR_IO_STR_OBJ("index file to big to mmap", fname);
      goto error;
    }

    // ok - now mmap the size of the file.
    data = mmap(NULL, len_off, PROT_READ, MAP_SHARED, fd, 0);
    if( ! data || data == MAP_FAILED ) {
      err = ERR_IO_STR_OBJ("failed to mmap index file", fname);
      goto error;
    }

    mmap_out->data = data;
    mmap_out->len = mmap_out->max = len_off;
    err = ERR_NOERR;
  }

error:
  rc = pthread_rwlock_unlock(&t->rwlock);
  assert(!rc);

  if( fd != -1 ) {
    rc = close(fd);
    if( rc ) {
      if( ! err ) err = ERR_IO_STR_OBJ("error on close", fname);
    }
  }
  if( fname ) free(fname);

  return err;
}



int isnew_stored_block(stored_block_t* block)
{
  return block->isnew;
}

// The block changes mode from STORED_BLOCK_...WRITE
// to STORED_BLOCK_READ.
// The data is saved in the repository.
// Error if mode wasn't writeable.
error_t commit_stored_block(stored_block_t* block, stored_block_mode_t new_mode)
{
  if( block->mode == STORED_BLOCK_MODE_CREATE_WRITE || block->mode == STORED_BLOCK_MODE_WRITE ) {
    // save the block somewhere special...
    fflush(block->f);
  } else {
    return ERR_PARAM;
  }

  block->mode = new_mode;

  // Throw an error if we were supposed to keep the block around -
  // it's just not supported right now.
  if( block->mode != STORED_BLOCK_MODE_NONE ) return ERR_PARAM;

  return ERR_NOERR;
}

// Closes a stored block.
// the FILE* f will be closed
// and memory from open will be released.
void close_stored_block(stored_block_t* block)
{
  if( block->f ) fclose(block->f);

  // free the string we strduped.
  cleanup_stored_block(block);
}

