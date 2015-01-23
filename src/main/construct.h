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

  femto/src/main/construct.h
*/
#ifndef _CONSTRUCT_H_
#define _CONSTRUCT_H_

#include "error.h"
#include "index.h"
#include "bwt_reader.h"

error_t index_documents(bwt_reader_t* bwt_reader,
                        bwt_document_map_reader_t* map,
                        bwt_document_info_reader_t* info,
                        index_block_param_t* block_param,
                        const char* index_path,
                        construct_statistics_t* stats);

struct index_occs {
  int64_t C[ALPHA_SIZE];
  int64_t occs_total[ALPHA_SIZE];
  int occs_since_block[ALPHA_SIZE];
};


typedef struct {
  index_block_param_t params;
  char* index_path;
  bwt_document_info_reader_t* info; // doc ends, doc infos.
  int64_t total_length;
  int64_t number_of_blocks;
  int64_t num_documents;


  int64_t cur_data_block;
  int64_t data_block_start; // row where current data block starts.
  // info about the current data block.
  int64_t size;
  int64_t number_of_buckets;
  int64_t cur_bucket;
  //int64_t cur_bucket_start; // row (within the block) where the current bucket starts.
  int64_t number_of_chunks;
  int64_t cur_chunk;
  int64_t cur_chunk_start; // row (within the block) where the current chunk starts.

  int stored_data_block_open;
  stored_block_t stored_data_block;
  data_block_writer_t data_block_writer;

  struct index_occs* occs;

  construct_statistics_t* stats;
} index_constructor_t;

error_t constructor_create(index_constructor_t* cstr, const index_block_param_t* params, const char* index_path, int64_t number_of_blocks, bwt_document_info_reader_t* info);
void constructor_destroy(index_constructor_t* cstr);

// Returns a filename that must be freed by the caller.
char* constructor_filename_for_data_block(index_constructor_t* cstr, int64_t data_block_index);

error_t constructor_get_block_size(index_constructor_t* cstr, int* block_size);
error_t constructor_get_bucket_size(index_constructor_t* cstr, int* bucket_size);
error_t constructor_get_chunk_size(index_constructor_t* cstr, int* chunk_size);
error_t constructor_get_mark_period(index_constructor_t* cstr, int* mark_period);

error_t constructor_get_doc_range(index_constructor_t* cstr, int64_t doc,
                                  int64_t* doc_start_out,
                                  int64_t* doc_end_out);
error_t constructor_get_doc(index_constructor_t* cstr, int64_t offset,
                            int64_t* doc, int64_t* doc_start, int64_t* doc_end);


error_t constructor_begin_data_block(index_constructor_t* cstr, int32_t size, int64_t cur_block_number, int64_t cur_block_start_row);
// This one can be invoked in parallel. zdata->data must be malloc/realloc'ed/0
error_t constructor_compress_bucket(// shared read-only state
                                    const index_constructor_t* cstr,
                                    // output areas -- should not be shared
                                    buffer_t* zdata,
                                    int* occs_in_bucket, // ALPHA_SIZE
                                    construct_statistics_t* stats,
                                    // read-only input -- should not be shared
                                    int32_t bucket_size, // # rows this bucket
                                    const alpha_t* L, // character before
                                    const int64_t* offsets, // SA[i] offsets
                                    results_t* chunks // array of size
                                                      // num_chunks_per_bucket
                                                      // storing doc of SA[i]
                                                      // for each chunk.
                                     );
// Serial.
error_t constructor_write_bucket(// read and write
                                 index_constructor_t* cstr,
                                 // output of constructor_compress_bucket
                                 const buffer_t* zdata,
                                 int* occs_in_bucket);

error_t constructor_end_data_block(index_constructor_t* cstr);

error_t constructor_construct_header(index_constructor_t* cstr);

error_t constructor_print_statistics(construct_statistics_t* stats);

#endif

