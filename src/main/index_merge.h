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

  femto/src/main/index_merge.h
*/
#ifndef _INDEX_MERGE_H_
#define _INDEX_MERGE_H_ 1

#include "index.h"

/*
error_t merge_indexes(index_struct_t* out,
                      index_struct_t* a, index_struct_t* b,
                      construct_statistics_t* stats);
*/


#if SUPPORT_INDEX_MERGE

typedef enum {
  BLOCK_UPDATE_CREATE = 1, // create (and overwrite) the update block,
                           // using the values in ratio/offset_size_bits
  BLOCK_UPDATE_FINISH = 2, // merge updates put in with BLOCK_UPDATE_DATA
                           // to create a new index; creates the 
                           // new data blocks and the new header blocks.
                           // Assumes that they are all on the same machine.
} block_update_manage_type_t;

typedef struct {
  index_locator_t src_loc;
  index_locator_t out_loc;
  index_block_param_t index_params;
  double ratio;
  int start_uncompressed;
  int max_uncompressed;
  int offset_size_bits;
} block_update_manage_t;

typedef enum {
  BLOCK_UPDATE_DATA = 1,

} block_update_type_t;

typedef struct {
  uint32_t dst_row_in_block; // the row we're "updating"
  int32_t ch; // the L-char.
  int64_t src_row; // the row we came from.
  int64_t offset; // logical offset that will be for the merged index
  int64_t doc; // document number in the merged index
} block_update_t;

typedef struct {
  // settings
  int32_t chunk_size;
  int32_t chunk_count;
  int32_t start_uncompressed;
  int32_t max_uncompressed;
  int32_t offset_size_bits;
  float ratio;

  // offsets
  uint32_t settings_offset;
  uint32_t chunk_ends_offset;
  uint32_t uncompressed_updates_offset;
  uint32_t compressed_chunks_new_rows_offset;
  uint32_t compressed_chunks_offset;
  uint32_t compressed_updates_offset;

  // data used in-memory only
  int writeable; // is this block writeable?
  // a "stored block" record
  stored_block_t stored_block; 
  buffer_t zbits; // the block data (the file is MMAP'd here).
} update_block_t;

typedef struct {
  uint32_t dst_row_in_block;
  int32_t ch;
  int64_t src_row;
  int64_t offset; // logical offset in the merged index
  int64_t doc; // document number in the merged index.
} update_block_entry_t;

error_t create_update_block(index_locator_t location, int64_t block_number, double ratio, int start_uncompressed, int max_uncompressed, int offset_size_bits);

error_t open_update_block(update_block_t* block, index_locator_t location, int64_t block_number, int writeable);
error_t close_update_block(update_block_t* block);

error_t block_update_manage_request(index_locator_t loc,
                       int64_t block_number, 
                       block_update_manage_type_t type,
                       block_update_manage_t* req);

error_t block_update_request(update_block_t* block,
                             block_update_type_t type,
                             block_update_t* req);


typedef struct {
  buffer_t buf; // gets its own pos, etc.
  update_block_entry_t ent;
  int64_t last_dst_row_number; // row in a block
  int64_t last_src_row_number;
  int ch;
  unsigned char offset_size_bits;
  int64_t num_updates;
  int64_t index;
} compressed_update_reader_t;

error_t init_compressed_update_reader(compressed_update_reader_t* r, long compressed_updates_offset, int offset_size_bits, buffer_t* input_buffer);
error_t init_compressed_update_reader_block(compressed_update_reader_t* r, update_block_t* block);
int read_compressed_update(compressed_update_reader_t* r);
void free_compressed_update_reader(compressed_update_reader_t* r);
error_t compress_updates(update_block_t* block);

typedef struct {
  int64_t start_block_num; // outputs num_split blocks from start_block_num
  int64_t num_split; // the number of blocks we're creating from this one.
  int64_t total_size; // size including updates of the resulting index.
  int64_t total_num_documents; // size including updates of the resulting index.
  int64_t total_num_blocks; // total number of blocks in the resulting index
} merge_block_info_t;

typedef struct {
  uint32_t num_rows;
  int occs[ALPHA_SIZE];
} new_block_info_t;

// incorporate
// occs will be ALPHA_SIZE for each block that we create.
error_t merge_block_updates(index_locator_t out_loc,
                            data_block_t* block,
                            update_block_t* update,
                            index_block_param_t * param,
                            merge_block_info_t* merge_info,
                            new_block_info_t* new_info);

#endif

#endif

