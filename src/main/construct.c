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

  femto/src/main/construct.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <ctype.h>

#include <unistd.h> // for ftruncate
#include <sys/types.h>

#include "index.h"
#include "bzlib_private.h"
#include "buffer_funcs.h"
#include "wtree_funcs.h"
#include "bwt_reader.h"
#include "util.h"
#include "results.h"
#include "construct.h"
#include "config.h"

error_t constructor_create(index_constructor_t* cstr, const index_block_param_t* params, const char* index_path, int64_t number_of_blocks, bwt_document_info_reader_t* info)
{
  error_t err;

  if( ! cstr ) return ERR_PARAM;

  memset(cstr, 0, sizeof(index_constructor_t));

  cstr->index_path = strdup(index_path);
  cstr->info = info;

  cstr->params = *params;
  err = calculate_params(&cstr->params);
  if( err ) return err;

  cstr->number_of_blocks = number_of_blocks;

  cstr->occs = calloc(1, sizeof(struct index_occs));
  if( ! cstr->occs ) {
    return ERR_MEM;
  }

  cstr->num_documents = bwt_document_info_reader_num_docs(cstr->info);
  cstr->total_length = bwt_document_info_reader_num_chars(cstr->info);

  if( params->block_size <= 0 ) return ERR_PARAM;

  if( cstr->num_documents < 1 ) return ERR_PARAM;

  return ERR_NOERR;
}

void constructor_destroy(index_constructor_t* cstr)
{
  // Clean up..
  free(cstr->index_path);
  free( cstr->occs );
}

char* constructor_filename_for_data_block(index_constructor_t* cstr, int64_t data_block_index)
{
  return create_filename_for_block(
            block_id_for_type(INDEX_BLOCK_TYPE_DATA, data_block_index),
            cstr->index_path);
}

error_t constructor_get_block_size(index_constructor_t* cstr, int* block_size)
{
  *block_size = cstr->params.block_size;
  return ERR_NOERR;
}

error_t constructor_get_bucket_size(index_constructor_t* cstr, int* bucket_size)
{
  *bucket_size = cstr->params.b_size;
  return ERR_NOERR;
}

error_t constructor_get_chunk_size(index_constructor_t* cstr, int* chunk_size)
{
  *chunk_size = cstr->params.chunk_size;
  return ERR_NOERR;
}
error_t constructor_get_mark_period(index_constructor_t* cstr, int* mark_period)
{
  *mark_period = cstr->params.mark_period;
  return ERR_NOERR;
}

error_t constructor_get_doc_range(index_constructor_t* cstr, int64_t doc,
                                  int64_t* doc_start_out,
                                  int64_t* doc_end_out)
{
  error_t err;

  *doc_start_out = -1;
  *doc_end_out = -1;

  err = bwt_document_info_reader_doc_start(cstr->info, doc, doc_start_out);
  if( err ) return err;

  err = bwt_document_info_reader_doc_end(cstr->info, doc, doc_end_out);
  if( err ) return err;

  return ERR_NOERR;
}


error_t constructor_get_doc(index_constructor_t* cstr, int64_t offset,
                            int64_t* doc_out,
                            int64_t* doc_start_out,
                            int64_t* doc_end_out)
{
  error_t err;

  *doc_out = -1;

  err = bwt_document_info_reader_find_doc(cstr->info, offset, doc_out);
  if( err ) return err;

  return constructor_get_doc_range(cstr, *doc_out, doc_start_out, doc_end_out);
}


// size is the number of rows in this block!
error_t constructor_begin_data_block(index_constructor_t* cstr, int32_t size, int64_t cur_block_number, int64_t cur_block_start_row)
{
  error_t err;
  int i;

  cstr->data_block_start = cur_block_start_row;
  cstr->cur_data_block = cur_block_number;
  cstr->size = size;

  cstr->number_of_buckets = CEILDIV(size, cstr->params.b_size);
  if( cstr->params.chunk_size > 0 ) {
    cstr->number_of_chunks = CEILDIV(size, cstr->params.chunk_size);
  } else if( cstr->params.chunk_size == -1 ) {
    // No chunks.
    cstr->number_of_chunks = 0;
  } else if( cstr->params.chunk_size == 0 ) {
    // variable size chunks.
    cstr->number_of_chunks = 0;
  }

  /*printf("starting data block block_number %lli/%lli number_of_buckets = %lli number_of_chunks = %lli\n",
          (long long int) cstr->cur_data_block,
          (long long int) cstr->number_of_blocks,
          (long long int) cstr->number_of_buckets, (long long int) cstr->number_of_chunks );
  */

  if( cur_block_number >= cstr->number_of_blocks ) return ERR_INVALID;
  if( cstr->number_of_buckets < 0 ) return ERR_INVALID;
  if( cstr->number_of_chunks < 0 ) return ERR_INVALID;

  // save the block to a file:
  // start with the block header.
  err = open_stored_block(&cstr->stored_data_block,
                          block_id_for_type(INDEX_BLOCK_TYPE_DATA, cstr->cur_data_block),
                          cstr->index_path,
                          STORED_BLOCK_MODE_CREATE_WRITE);
  if( err ) return err;
  cstr->stored_data_block_open++;

  err = begin_data_block(&cstr->data_block_writer, cstr->stats,
                         cstr->stored_data_block.f, cstr->index_path,
                         cstr->number_of_blocks, cstr->total_length,
                         cstr->num_documents, cstr->cur_data_block,
                         cstr->number_of_buckets,
                         cstr->size, & cstr->params);
  if( err ) return err;
  cstr->stored_data_block_open++;

  // clear occs_since_block
  for( i = 0; i < ALPHA_SIZE; i++ ) {
    cstr->occs->occs_since_block[i] = 0;
  }

  cstr->cur_bucket = 0;
  cstr->cur_chunk = 0;
  cstr->cur_chunk_start = 0;

  return err;
}

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
                                     )
{
  return compress_bucket(zdata, occs_in_bucket, stats,
                         cstr->total_length, cstr->num_documents,
                         cstr->params.chunk_size,
                         bucket_size, L, offsets, chunks);
}

// Serial.
error_t constructor_write_bucket(// read and write
                                 index_constructor_t* cstr,
                                 // output of constructor_compress_bucket
                                 const buffer_t* zdata,
                                 int* occs_in_bucket)
{
  error_t err;
  int ch;

  if( cstr->stored_data_block_open != 2 ) return ERR_PARAM;
  if( cstr->cur_bucket < 0 ||
      cstr->cur_bucket >= cstr->number_of_buckets ) return ERR_PARAM;

  /*printf("writing block_number %lli bucket %lli/%lli\n",
         (long long int) cstr->cur_data_block,
         (long long int) cstr->cur_bucket,
         (long long int) cstr->number_of_buckets);
         */

  err = update_data_block(&cstr->data_block_writer,
                          cstr->cur_bucket,
                          zdata->data, zdata->len,
                          cstr->occs->occs_since_block);
  if( err ) return err;

  // update occs_since_block.
  for( ch = 0; ch < ALPHA_SIZE; ch++ ) {
    cstr->occs->occs_since_block[ch] += occs_in_bucket[ch];
  }

  cstr->cur_bucket++;

  return ERR_NOERR;
}

error_t constructor_end_data_block(index_constructor_t* cstr)
{
  error_t err = ERR_NOERR;

  /*printf("finishing block_number %lli\n",
         (long long int) cstr->cur_data_block);
         */

  // Finish writing/close.
  if( cstr->stored_data_block_open > 1 ) {
    finish_data_block(&cstr->data_block_writer);
  }
  if( cstr->stored_data_block_open > 0 ) {
    err = commit_stored_block(&cstr->stored_data_block, STORED_BLOCK_MODE_NONE);
    close_stored_block(&cstr->stored_data_block);
    cstr->stored_data_block_open = 0;
  }

  // Update cstr structure
  cstr->cur_data_block = -1;
  cstr->data_block_start = -1; 
  cstr->size = cstr->number_of_buckets = cstr->number_of_chunks = 0;

  assert( ! cstr->stored_data_block_open );

  return err;
}

error_t constructor_construct_header(index_constructor_t* cstr)
{
  error_t err = ERR_NOERR;
  int i;
  int64_t sum;
  stored_block_t stored_header_block;
  header_block_writer_t header_block_writer;
  int stored_header_block_open = 0;
  data_block_t block;
  int data_block_open = 0;
  block_request_t request;
  buffer_t zdata = build_buffer(0, NULL);
  int64_t block_number;
  int64_t row, doc;
  int32_t block_row;
  int64_t block_start, block_end;
  path_translator_t pt;
  index_locator_t loc;

  //printf("Creating header block\n");

  memset(&pt, 0, sizeof(path_translator_t));

  err = path_translator_init(&pt);
  if( err ) return err;

  err = path_translator_id_for_path(&pt, cstr->index_path, &loc);
  if( err ) return err;

  // zero-th step:
  // find out which characters are in use throughout the ENTIRE TEXT
  // and also the C array (C[i] is the number of occurences of characters less than i)
  // set occs_total, and then compute C from that.

  // occs are already zero from buffer_calloc

  for( i = 0; i < ALPHA_SIZE; i++ ) {
    cstr->occs->occs_total[i] = 0;
  }

  for( block_number = 0; block_number < cstr->number_of_blocks; block_number++ ) {
    assert(!data_block_open);
    err = open_data_block(&block, &pt, loc, block_number, 1);
    if( err ) goto error;
    data_block_open++;

    for( i = 0; i < ALPHA_SIZE; i++ ) {

      // Get the occs at the final position in the block.
      if( block.hdr.size > 0 ) {
        memset(&request, 0, sizeof(block_request_t));

        request.row_in_block = block.hdr.size - 1;
        request.ch = i;

        err = block_request(&block, BLOCK_REQUEST_OCCS, &request);
        if( err ) goto error;

        cstr->occs->occs_total[i] += request.occs_in_block;
      }

      if( block.hdr.size != cstr->params.block_size &&
          block_number < cstr->number_of_blocks - 1 ) {
        assert(0);
        printf("construct_header setting variable block size\n");
        cstr->params.variable_block_size = 1;
      }
    }


    err = close_data_block(&block);
    if( err ) goto error;
    data_block_open--;
  }

  // compute C.
  sum = 0;
  for( i = 0; i < ALPHA_SIZE; i++ ) {
    cstr->occs->C[i] = sum;
    sum += cstr->occs->occs_total[i];
  }

  // clear occs_total; it'll be updated as we go
  for( i = 0; i < ALPHA_SIZE; i++ ) {
    cstr->occs->occs_total[i] = 0;
  }

  // output the header block.
  err = open_stored_block(&stored_header_block,
                          block_id_for_type(INDEX_BLOCK_TYPE_HEADER, 0),
                          cstr->index_path,
                          STORED_BLOCK_MODE_CREATE_WRITE);

  if( err ) goto error;
  stored_header_block_open++;

  // output the header block, except with zeros for all of the block occs.
  err = begin_header_block(&header_block_writer, cstr->stats,
                           stored_header_block.f,
                           cstr->index_path,
                           cstr->number_of_blocks,
                           cstr->total_length,
                           cstr->num_documents,
                           &cstr->params,
                           cstr->occs->C);
  if( err ) goto error;
  stored_header_block_open++;

  // 0.5'th step
  // Compute the doc to eof_row mapping so that the
  // Note that EOF is before all other characters..
  
  block_number = 0;

  assert(!data_block_open);
  err = open_data_block(&block, &pt, loc, block_number, 1);
  if( err ) goto error;
  data_block_open++;


  row = 0;
  block_row = 0;
  while( row < cstr->num_documents ) {

    memset(&request, 0, sizeof(block_request_t));

    // Get the offset at this character in the index.
    request.row_in_block = block_row;
    // Remember that ch is the previous character,
    // so it could be anything.

    err = block_request(&block, BLOCK_REQUEST_LOCATION, &request);
    if( err ) goto error;

    if( request.offset == INVALID_OFFSET ) {
      err = ERR_PARAM_STR("Final EOF character of each document must be marked");
      goto error;
    }

    // Now use binary search to find the document.
    doc = -1;
    err = bwt_document_info_reader_find_doc(cstr->info, request.offset, &doc);
    if( err ) goto error;

    err = update_header_block_with_doc_eof_row(&header_block_writer,
                                               doc, row);
    if( err ) goto error;

    row++;
    block_row++;

    if( block_row >= block.hdr.size ) {
      // Move on to the next block.
      
      err = close_data_block(&block);
      if( err ) goto error;
      data_block_open--;

      block_number++;

      assert(!data_block_open);
      err = open_data_block(&block, &pt, loc, block_number, 1);
      if( err ) goto error;
      data_block_open++;
    }
  }

  err = close_data_block(&block);
  if( err ) goto error;
  data_block_open--;

  // update the header block with all of the doc ends.
  // and with the document info.
  for( doc = 0; doc < cstr->num_documents; doc++ ) {
    int64_t info_len = 0;
    unsigned char* info_data = NULL;
    int64_t doc_end = 0;

    err = bwt_document_info_reader_doc_end(cstr->info, doc, &doc_end);
    if( err ) goto error;

    // update the document end
    err = update_header_block_with_doc_end(
                            &header_block_writer,
                            doc,
                            doc_end);

    if( err ) goto error;

    // update the doc infos
    err = bwt_document_info_reader_get(cstr->info, doc, &info_len, &info_data);
    if( err ) goto error;
    err = update_header_block_with_doc_info(&header_block_writer, doc, info_len, info_data);
    if( err ) goto error;
  }

  row = 0;
  // Update the header block with all of the data block sizes
  // and with the occs in the block.
  for( block_number = 0; block_number < cstr->number_of_blocks; block_number++ ) {
    assert(!data_block_open);
    err = open_data_block(&block, &pt, loc, block_number, 1);
    if( err ) goto error;
    data_block_open++;

    block_start = row;
    block_end = row + block.hdr.size;

    err = update_header_block(&header_block_writer, block_number, cstr->occs->occs_total);
    if( err ) goto error;

    // update occs_total with occs in block.
    for( i = 0; i < ALPHA_SIZE; i++ ) {
      if( block.hdr.size > 0 ) {
        memset(&request, 0, sizeof(block_request_t));

        // Get the occs at the final position in the block.
        request.row_in_block = block.hdr.size - 1;
        request.ch = i;

        err = block_request(&block, BLOCK_REQUEST_OCCS, &request);
        if( err ) goto error;

        cstr->occs->occs_total[i] += request.occs_in_block;
      }
    }

    // Now we move past block.size rows.
    row += block.hdr.size;

    err = close_data_block(&block);
    if( err ) goto error;
    data_block_open--;
  }


  assert(!data_block_open); // not left open on normal path.

  // Create a file indicating that it's a FEMTO index.
  {
    FILE* tmp;
    char* tag = "/_femto_index";
    size_t len = strlen(cstr->index_path) + strlen(tag) + 1;
    char* buf = malloc(len);

    sprintf(buf, "%s%s", cstr->index_path, tag);
    tmp = fopen(buf, "w");
    if( tmp ) {
      fprintf(tmp, "This is a FEMTO index constructed by %s\n", PACKAGE_STRING);  
    }
    fclose(tmp);
    free(buf);
  }

  err = ERR_NOERR;

error:
  if( zdata.data ) free(zdata.data);
  if( stored_header_block_open > 1 ) {
    finish_header_block(&header_block_writer);
  }
  if( stored_header_block_open > 0 ) {
    if( ! err ) {
      err = commit_stored_block(&stored_header_block, STORED_BLOCK_MODE_NONE);
    }
    close_stored_block(&stored_header_block);
  }
  if( data_block_open > 0 ) {
    close_data_block(&block);
  }
  path_translator_destroy(&pt);

  return err;
}



// Constructs an index, using an already-computed BW-transform
// with mark infos, and saves that index to blocks on the disk.
error_t index_documents(bwt_reader_t* bwt,
                        bwt_document_map_reader_t* map,
                        bwt_document_info_reader_t* info,
                        index_block_param_t* params,
                        const char* index_path,
                        construct_statistics_t* stats)
{
  index_constructor_t cstr;
  int64_t total_length, number_of_blocks, block_number;
  int64_t block_size;
  int chunk_size, bucket_size, chunks_per_bucket;
  int64_t cur_block_sz, cur_block_row;
  int64_t cur_bucket_sz, cur_chunk_sz;
  int64_t cur_bucket_row, cur_chunk_row;
  int cur_chunk;
  int64_t bucket_row;
  error_t err;
  error_t err2;
  alpha_t* bucket_L = NULL;
  int64_t* bucket_offsets = NULL;
  results_t* chunks = NULL;
  buffer_t zdata = build_buffer(0, NULL);
  int occs_in_bucket[ALPHA_SIZE];
 
  // check the block parameters
  // update the mark period.
  params->mark_period = bwt_mark_period(bwt);


  if( map == NULL ) params->chunk_size = -1;
  else params->chunk_size = bwt_document_map_reader_chunk_size(map);

  total_length = bwt_document_info_reader_num_chars(info);
  block_size = params->block_size;
  number_of_blocks = CEILDIV(total_length, block_size);
  err = constructor_create(&cstr,
                           params,
                           index_path,
                           number_of_blocks,
                           info);
  if( err ) goto error;

  err = constructor_get_chunk_size(&cstr, &chunk_size);
  if( err ) goto error;
  err = constructor_get_bucket_size(&cstr, &bucket_size);
  if( err ) goto error;

  assert(chunk_size == 0 || bucket_size % chunk_size == 0);
  chunks_per_bucket = bucket_size / chunk_size;

  bucket_L = malloc(sizeof(alpha_t)*bucket_size);
  bucket_offsets = malloc(sizeof(int64_t)*bucket_size);
  if( map ) {
    chunks = malloc(sizeof(results_t)*chunks_per_bucket);
    memset(chunks, 0, sizeof(results_t)*chunks_per_bucket);
  }

  cur_block_row = 0;
  for (block_number = 0; block_number < number_of_blocks; block_number++) {
    cur_block_sz = block_size;
    if( cur_block_row + cur_block_sz > total_length ) {
      cur_block_sz = total_length - cur_block_row;
    }
    err = constructor_begin_data_block(&cstr, cur_block_sz, block_number, cur_block_row);
    if( err ) goto error;

    // Write the bucket data.
    cur_bucket_row = 0;
    while( cur_bucket_row < cur_block_sz ) {
      cur_bucket_sz = bucket_size;
      if( cur_bucket_row + cur_bucket_sz > cur_block_sz ) {
        cur_bucket_sz = cur_block_sz - cur_bucket_row;
      }

      // Clear out the chunks data so we can free correctly even w/error.
      if( chunks ) {
        memset(chunks, 0, sizeof(results_t)*chunks_per_bucket);
      }

      // Fill in the offsets and the L column.
      for( bucket_row = 0; bucket_row < cur_bucket_sz; bucket_row++ ) {
        int64_t ch, offset;
        ch = INVALID_ALPHA;
        offset = -1;
        err = bwt_reader_read_one(bwt, &ch, &offset);
        if( err ) goto blockerror;
        bucket_L[bucket_row] = ch;
        bucket_offsets[bucket_row] = offset;
      }

      // fill the chunk data if we have it.
      if( map && chunks ) {
        // Write the chunks data.
        cur_chunk_row = 0;
        cur_chunk = 0;
        while( cur_chunk_row < cur_block_sz ) {
          cur_chunk_sz = chunk_size;
          if( cur_chunk_row + cur_chunk_sz > cur_block_sz ) {
            cur_chunk_sz = cur_block_sz - cur_chunk_row;
          }

          // Fill in the chunk_ids.
          err = bwt_document_map_reader_results(map, &chunks[cur_chunk]);
          if( err ) goto bucketerror;

          cur_chunk_row += cur_chunk_sz;
          cur_chunk++;
        }
      }

      zdata.len = 0;
      err = constructor_compress_bucket(&cstr,
                                        &zdata,
                                        occs_in_bucket,
                                        stats,
                                        cur_bucket_sz,
                                        bucket_L,
                                        bucket_offsets,
                                        chunks);
      if( err ) goto bucketerror;

      err = constructor_write_bucket(&cstr, &zdata, occs_in_bucket);
      if( err ) goto bucketerror;

      cur_bucket_row += cur_bucket_sz;

      err = ERR_NOERR;
bucketerror:
      if( chunks ) {
        for( cur_chunk = 0; cur_chunk < chunks_per_bucket; cur_chunk++ ) {
          results_destroy(&chunks[cur_chunk]);
        }
      }
      if( err ) goto blockerror;
    }

    err = ERR_NOERR;
blockerror:
    err2 = constructor_end_data_block(&cstr);
    if( err ) goto error;
    if( err2 ) {
      err = err2;
      goto error;
    }

    cur_block_row += cur_block_sz;
  }

  err = constructor_construct_header(&cstr);
  if( err ) goto error;


  err = ERR_NOERR;

error:
  if( bucket_L ) free( bucket_L );
  if( bucket_offsets ) free( bucket_offsets );
  if( chunks ) free( chunks );
  if( zdata.data ) free( zdata.data );

  constructor_destroy(&cstr);

  return err;
}


