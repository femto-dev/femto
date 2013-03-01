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

  femto/src/utils_cc/file_find_node.cc
*/
extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "buffer.h"
}

#include <cstdlib>
#include <cstring>
#include <cstdio>

#include "error.hh"
#include "file_find_node.hh"

static
error_t size_file(char* path, void* state)
{
  ss_state* s = (ss_state*) state;
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

  if( s->verbose ) {
    printf("%s %li\n", path, (long) len);
  }

  s->num_bytes += len;
  s->num_docs++;

  return ERR_NOERR;
}

static
error_t read_file(char* path, void* state)
{
  ss_state* s = (ss_state*) state;
  long len;
  FILE* f;
  size_t read;
  error_t err;
  int rc;

  // First thing, open the file
  f = fopen(path, "r");
  if( ! f ) return ERR_IO_STR_OBJ("Could not open file", path);

  // get the length of the file.
  rc = fseek(f, 0L, SEEK_END);
  if( rc != 0 ) return ERR_IO_STR_OBJ("Could not fseek", path);
  len = ftell(f);
  if( len < 0 ) return ERR_IO_STR_OBJ("Could not ftell", path);
  rewind(f);

  // append the length of the file.
  if( s->file_lengths_file ) {
    ssize_t wrote;
    uint64_t len_be = len;
    len_be = hton_64(len_be);
    wrote = fwrite(&len_be, sizeof(uint64_t), 1, s->file_lengths_file);
    if( wrote != 1 ) return ERR_IO_STR("Could not write");
  }

  err = ERR_NOERR;

  if( !s->reverse_data ) {
    while(!feof(f)) {
      // Get a tile from the pipeline.
      tile t = s->output->get_empty_tile();
      if( t.is_end() ) break;
      // Fill the tile by reading data.
      read = fread(t.data, 1, t.max, f);
      if( read == 0 && ferror(f)) {
        // An error occured!
        err = ERR_IO_FILE_NAME(f, path);
        break;
      }
      // Set the length to however much we read.
      t.len = read;
      s->output->put_full_tile(t);
    }
  } else {
    size_t tile_size = s->output->get_tile_size();
    unsigned char* buf = (unsigned char*) malloc(tile_size);
    if(!buf) return ERR_MEM;
    // Read in the tiles of the file starting from the end
    // into buf, and then reverse them into the tile from the output 
    ssize_t n_tiles = CEILDIV(len,tile_size);
    ssize_t i;
    ssize_t j,k;
    ssize_t amt;
    ssize_t start;
    tile t;

    for( i = n_tiles-1; i>=0; i-- ) {
      start = i*tile_size;
      // Decide how much to read.
      if( i != n_tiles - 1 ) {
        amt = tile_size;
      } else {
        amt = len - start;
      }
      // Seek to the start of tile i.
      rc = fseek(f, start, SEEK_SET);
      if( rc != 0 ) {
        err = ERR_IO_STR_OBJ("Could not fseek", path);
        break;
      }
      // Read the amount we decided we'd need.
      read = fread(buf, 1, amt, f);
      if( (ssize_t) read != amt ) {
        err = ERR_IO_STR_OBJ("Could not read", path);
        break;
      }
      // Get a tile.
      t = s->output->get_empty_tile();
      if( t.is_end() ) break;
      // Reverse the data from buf into the tile!
      for( j=amt-1,k=0; k<amt; j--,k++ ) {
        assert(0);
        //t.data[k] = buf[j];
      }
      t.len = amt;
      // Put the full tile.
      s->output->put_full_tile(t);
    }

    free(buf);
  }
  
  s->num_bytes_read += len;
  
  fclose(f);

  return err;
}

file_find_node::file_find_node(
                   std::vector<std::string> path_names,
                   write_pipe* output,
                   //FILE* file_lengths_file,
                   pipeline_node* parent,
                   std::string name,
                   bool print_timing)
    : pipeline_node(parent,name,print_timing),
      path_names(path_names)
{
  memset(&state, 0, sizeof(ss_state));
  state.num_bytes = 0;
  state.output = output;
  state.num_bytes_read = 0;
  state.file_lengths_file = NULL; //file_lengths_file;
  state.num_docs = 0;
  state.verbose = 0;
}


int64_t file_find_node::count_size() {
  error_t err;
  const char** roots = (const char**) malloc(sizeof(char*)*path_names.size());

  for( size_t i = 0; i < path_names.size(); i++ ) {
    roots[i] = path_names[i].c_str();
  }

  // Count up the total size of all of the files.
  err = init_file_find(&state.ffs, path_names.size(), roots);
  if( err ) throw error(err);
  err = file_find(&state.ffs, size_file, &state);
  if( err ) throw error(err);
  free_file_find(&state.ffs);

  free(roots);

  return state.num_bytes;
}

void file_find_node::run()
{
  error_t err;
  const char** roots = (const char**) malloc(sizeof(char*)*path_names.size());

  for( size_t i = 0; i < path_names.size(); i++ ) {
    roots[i] = path_names[i].c_str();
  }


  // Now read in all of the files and feed them to ... whatever is next.
  err = init_file_find(&state.ffs, path_names.size(), roots);
  if( err ) throw error(err);
  err = file_find(&state.ffs, read_file, &state);
  if( err ) throw error(err);
  free_file_find(&state.ffs);

  free(roots);

  state.output->close_full();
}

