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

  femto/src/main/index.h
*/
#ifndef _INDEX_H_
#define _INDEX_H_ 1

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
// request the format macros.
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string.h>

#include "error.h"
#include "block_storage.h"
#include "cache.h"
#include "buffer.h"
#include "results.h"

#include "wtree.h"
#include "bwt_reader.h"

#include "index_types.h"

// Turning this to 1 activates a bunch of old rubbish
// that doesn't work..
#define SUPPORT_INDEX_MERGE 0

#if SUPPORT_INDEX_MERGE
  // this old way used too many block numbers... to support 
  // what we don't use.
  
  typedef enum {
    INDEX_BLOCK_TYPE_HEADER = 0, // see block_format.txt for format
    INDEX_BLOCK_TYPE_DATA = 1, // see block_format.txt for format
    INDEX_BLOCK_TYPE_DATA_UPDATE = 2 // see update_format.txt for format
  } index_block_type_t;

#define INDEX_BLOCK_TYPE_NUMBITS 2
#define INDEX_BLOCK_TYPE_MASK 0x3

  static inline int64_t block_id_for_type(index_block_type_t type,
                                              int64_t number)
  {
    return (number << INDEX_BLOCK_TYPE_NUMBITS) | type;
  }

  static inline int64_t block_number_from_id(int64_t id)
  {
    return id >> INDEX_BLOCK_TYPE_NUMBITS;
  }

  static inline index_block_type_t block_type_from_id(int64_t id)
  {
    return (index_block_type_t) (id & INDEX_BLOCK_TYPE_MASK);
  }
#else
  typedef enum {
    INDEX_BLOCK_TYPE_HEADER = 0, // see block_format.txt for format
    INDEX_BLOCK_TYPE_DATA = 1, // see block_format.txt for format
  } index_block_type_t;

  static inline int64_t block_id_for_type(index_block_type_t type,
                                          int64_t number)
  {
    if( type == INDEX_BLOCK_TYPE_HEADER ) return 0;
    else return number + 1;
  }

  static inline int64_t block_number_from_id(int64_t id)
  {
    if( id == 0 ) return 0;
    else return id - 1;
  }

  static inline index_block_type_t block_type_from_id(int64_t id)
  {
    if( id == 0 ) return INDEX_BLOCK_TYPE_HEADER;
    else return INDEX_BLOCK_TYPE_DATA;
  }
#endif

typedef struct {
  // block storage parameters:
  // Saved data
  int variable_block_size;
  int block_size; // Index blocks are this size (uncompressed size of L per block).
  int b_size; // the size of a bucket (FMI bucket); contains selectors and occurence information since superbucket.
  int mark_period;
  int mark_type;
  int variable_chunk_size;
  int chunk_size; // the size of each chunk, stored within the blocks,
                  // which records the documents in that range of rows
                  // (gamma-encoded document# differences)
                  // 0,-1 indicates no chunks.
  // Computed data
  int block_size_bits; // number of bits 
  int b_size_bits;
  int num_chunks_per_bucket; // = CEILDIV(bucket_size/chunk_size)
  int num_buckets_per_block; // = CEILDIV(block_size/bucket_size)
} index_block_param_t;

typedef struct {
  uint32_t block_start;
  uint32_t block_version;
  int64_t block_number; // the block number
  int64_t number_of_blocks; // the number of blocks in the index.
  int64_t total_length; // the number of indexed bytes (uncompressed size of L).
                        // total in the entire index
  int64_t number_of_documents; // the total num docs in the entire index
  int32_t num_buckets; // number of buckets in this block
  int32_t size; // number of rows in this block
  index_block_param_t param;
} block_header_t;

typedef struct {
  int preparedOffset;
  int docNum;
  uint32_t docOffset;
  int row; // the row number in the sorted list.
} mark_location_t;


typedef struct {
  block_header_t hdr;
    
  int text_size_bits;
  int chunk_num_docs_bits;

  // a "stored block" record.
  stored_block_t stored_block;
  buffer_t zbits; // the block data (the file is MMAP'd here).
  // cached data:
  cache_t bucket_cache;
} data_block_t;

typedef struct {
  // statistics 
  long bytes_in_block_info;
  long bytes_in_buckets;
  long bytes_in_bucket_occs;
  long bytes_in_bucket_map;
  long bytes_in_bucket_wtree;
  long bytes_in_bucket_mark_table;
  long bytes_in_bucket_mark_arrays;
  long bytes_in_bucket_chunks;
  wtree_stats_t wtree_stats;
} construct_statistics_t;

typedef struct {
  long num_block_queries;
} query_statistics_t;

typedef struct {
  block_header_t hdr;

  // a "stored block" record
  stored_block_t stored_block; 
  buffer_t zbits; // the block data (the file is MMAP'd here).
} header_block_t; 

#define HEADER_BLOCK_START   0xb1177dea
#define DATA_BLOCK_START   0xb1501dea
#define DATA_BLOCK_VERSION 0x00000006
#define HEADER_BLOCK_VERSION 0x00000006
#define END_OF_HEADER 0xe0ffff4d

#define BUCKET_START 0xb140bcc7


typedef enum {
  /** Given row,
       return block_num and make row the row number for the first row 
       in that block */
  HDR_BSEARCH_BLOCK_ROWS = 1,
  /** Given ch, return C[ch] in occs. */
  HDR_REQUEST_C = 2,

  /** Given occs, return ch so that C[ch] <= occs < C[ch+1].
      Updates occs so that occs = C[ch]
   */
  HDR_BSEARCH_C = 4,

  /** Given ch, block_num, return # occurences of ch before
       that block in L in occs
       */
  HDR_REQUEST_BLOCK_OCCS = 8,

  /** Given ch, occs, return the block number that contains the occ
      occurence of ch. Updates occs to be the value for that block #.
   */
  HDR_BSEARCH_BLOCK_OCCS = 16,
  /** Given block number, return the row number for its first row */
  HDR_REQUEST_BLOCK_ROWS = 32,

  /** When set with HDR_BSEARCH_C and HDR_BSEARCH_BLOCK_OCCS, 
      takes in a row and returns the block_num, character, and
      occurence value within that block for searching with
      BLOCK_REQUEST_ROW. Replaces row with the first row of
      the block numbered block_num.
      */
  HDR_FORWARD = 64,
  /** When set with HDR_BSEARCH_BLOCK_ROWS
      HDR_REQUEST_C and HDR_REQUEST_BLOCK_OCCS
      takes in:
        row <- the row number we're interested in
        ch <- the character
      returns:
        occs <- C[ch] + OccsBeforeBlock(ch, block_num)
        block_num <- the block number
        row <- the row at which the block_num block starts
      */
  HDR_BACK = 128,

  /** Returns the number of blocks in the index in block_num.
    */
  HDR_REQUEST_NUM_BLOCKS = 256,

  MAX_HDR_OCCS_REQUEST = (HDR_BSEARCH_BLOCK_ROWS |
                          HDR_REQUEST_BLOCK_ROWS |
                          HDR_REQUEST_C |
                          HDR_BSEARCH_C |
                          HDR_REQUEST_BLOCK_OCCS |
                          HDR_BSEARCH_BLOCK_OCCS |
                          HDR_FORWARD |
                          HDR_BACK |
                          HDR_REQUEST_NUM_BLOCKS),
} header_occs_request_type_t;

typedef struct {
  int64_t row;
  int64_t occs;
  int64_t block_num;
  int ch;
} header_occs_request_t;

typedef enum {
  /** Given, in offset, a logical offset, return in loc the 
      document and within-document offset.
      */
  HDR_LOC_RESOLVE_LOCATION = 1,

  /** Given, in loc.document, a document number, return in
      document_length the length of that document.
      */
  HDR_LOC_REQUEST_DOC_LEN = 2,

  /** Returns, in doc_len, the size of the L column for
    * the entire index, or, in other words, the sum of the
    * lengths of all the documents in the index.
    */
  HDR_LOC_REQUEST_L_SIZE = 4,

  /** Return the document info for the document passed in loc.doc
      in doc_info and sets doc_info_len.
      */
  HDR_LOC_REQUEST_DOC_INFO = 8,

  /** Return in doc_len the total number of documents */
  HDR_LOC_REQUEST_NUM_DOCS = 16,

  /* Given loc.doc, return in offset the row number of its EOF char. */
  HDR_LOC_REQUEST_DOC_EOF_ROW = 32,

  MAX_HDR_LOC_REQUEST = (HDR_LOC_RESOLVE_LOCATION|
                         HDR_LOC_REQUEST_DOC_LEN|
                         HDR_LOC_REQUEST_L_SIZE|
                         HDR_LOC_REQUEST_DOC_INFO|
                         HDR_LOC_REQUEST_NUM_DOCS|
                         HDR_LOC_REQUEST_DOC_EOF_ROW),
} header_loc_request_type_t; // meant to be OR'ed together.

typedef struct {
  int64_t offset;
  location_info_t loc;
  int64_t doc_len;
  int64_t doc_info_len;
  unsigned char* doc_info; // must be freed!
} header_loc_request_t;


typedef enum {
  /** Given row, returns the character L[row] in ch */
  BLOCK_REQUEST_CHAR = 1,

  /** Given row and ch (possibly from BLOCK_REQUEST_CHAR), 
      find Occs( ch, row ) and return it in occs. */
  BLOCK_REQUEST_OCCS = 2,

  /** Given row, return the location if it is a marked row. */
  BLOCK_REQUEST_LOCATION = 4,

  /** Given ch and occs, find row such that Occs( ch, row ) == occs
      and return it in row. Cannot be set with BLOCK_REQUEST_CHAR
      or BLOCK_REQUEST_OCCS.
   */
  BLOCK_REQUEST_ROW = 8, 


  MAX_BLOCK_REQUEST = 15
} block_request_type_t; // meant to be OR'ed together.

typedef struct {
  // Response info
  int64_t offset; // the location - logical offset

  // Request/Response info
  int occs_in_block;
  int row_in_block;
  int ch;
} block_request_t;

typedef enum {
  /** Given first, returns the chunk containing that row.
      Sets [first,last] to the range of rows in the chunk.
    */
  BLOCK_CHUNK_FIND_NUMBER = 1,
  /** Given a chunk number,return the
      documents for the part of this range contained by that chunk.
    */
  BLOCK_CHUNK_REQUEST_DOCUMENTS = 2,
  MAX_BLOCK_CHUNK_REQUEST = 3,
} block_chunk_request_type_t;

typedef struct {
  int first; // input starting row number within block, output first row starting there.
  int last; // output - last row in the chunk.
  int chunk_number; // input
  results_t results; // storing the matching documents. Must be freed with results_destroy
} block_chunk_request_t;

#define MALLOC_DEBUG 0
//#define EXTRA_CHECKS 0
#define SAFETY_CHECK \
{ \
  if(MALLOC_DEBUG) { \
    free(malloc(55768)); \
    free(malloc(8));\
    free(malloc(1));\
    free(malloc(3));\
    free(malloc(17));\
    free(malloc(4));\
  } \
}

void set_default_param(index_block_param_t* p);
error_t calculate_params(index_block_param_t* p);
error_t open_data_block(data_block_t* block,
                   path_translator_t* t,
                   index_locator_t location,
                   int64_t block_number,
                   int buckets_cached // number of buckets to cache.
                  );
error_t close_data_block(data_block_t* block);

error_t block_request(data_block_t* block,
                      block_request_type_t type,
                      block_request_t* req);

error_t block_chunk_request(data_block_t* block,
                      block_chunk_request_type_t type,
                      block_chunk_request_t* req);


int is_bucket_loaded(data_block_t* block, int row_in_block);

error_t open_header_block(header_block_t* block, path_translator_t* t, index_locator_t location);
error_t close_header_block(header_block_t* block);
error_t header_occs_request(header_block_t* block,
                       header_occs_request_type_t type,
                       header_occs_request_t* req);
error_t header_loc_request(header_block_t* block,
                       header_loc_request_type_t type,
                       header_loc_request_t* req);

void print_block_param_usage(const char* prefix);
error_t parse_param(index_block_param_t* p, const char* string);

typedef struct {
  int header_bytes;
  int wavelet_tree_bytes;
  int leaf_mark_table_bytes;
  int mark_array_bytes;
  int chunk_bytes;
  int total_bucket_size;
} bucket_info_t; 

error_t get_bucket_info(bucket_info_t* return_info, data_block_t* block, int bucket_number);

// misc. functions
void read_occs(buffer_t * buf, uchar inUse[ALPHA_SIZE],
               int maxbits, unsigned int occs[ALPHA_SIZE]);
void write_occs(buffer_t* buf, uchar inUse[ALPHA_SIZE],
                int maxbits, unsigned int occs[ALPHA_SIZE]);


typedef struct {
  FILE* f;
  buffer_t mmap;
  block_header_t hdr;
} data_block_writer_t;



error_t update_data_block(data_block_writer_t* writer, int bucket_number, unsigned char* zbucket, int zlen, const int bucket_occs[ALPHA_SIZE]);
error_t finish_data_block(data_block_writer_t* writer);
error_t begin_data_block(data_block_writer_t* writer,
                         construct_statistics_t* stats,
	                 // input data
	                 FILE* f, const char* index_path,
			 int64_t number_of_blocks,
                         int64_t total_length,
                         int64_t total_num_documents,
                         int64_t block_number,
			 uint32_t number_of_buckets,
                         int32_t block_size,
                         index_block_param_t* param);

typedef struct {
  FILE* f;
  buffer_t mmap;
  block_header_t hdr;
} header_block_writer_t;

error_t update_header_block(header_block_writer_t* writer, int64_t block_num, int64_t occs_total[ALPHA_SIZE]);
error_t update_header_block_with_doc_info(header_block_writer_t* writer, int64_t doc_num, int64_t len, unsigned char* data);
error_t update_header_block_with_doc_end(header_block_writer_t* writer, int64_t doc_num, int64_t doc_end);
error_t update_header_block_with_doc_eof_row(header_block_writer_t* writer, int64_t doc_num, int64_t eof_row);

error_t finish_header_block(header_block_writer_t* writer);
error_t begin_header_block(header_block_writer_t* writer,
                           construct_statistics_t* stats,
	                   // input arguments:
	                   FILE* f,
	                   const char* index_path,
	                   int64_t number_of_blocks,
	                   int64_t total_length,
	                   int64_t number_of_documents,
	                   const index_block_param_t* param,
                           const int64_t C[ALPHA_SIZE]);

// compress a bucket.
// L is the L column ie L[i]. offsets are SA[i], or -1 if not known.
error_t compress_bucket( // output areas
                         buffer_t* dst,
                         int* occs_in_bucket, // ALPHA_SIZE
                         construct_statistics_t* stats,
                         // index configuration
                         int64_t total_length,
                         int64_t total_num_documents,
                         int chunk_size,
                         // input areas
                         int bucket_len, // number of rows in this bucket
                         const alpha_t* L,
                         const int64_t* offsets,
                         results_t* chunks // in chunk_size chunks...
                        );


error_t flatten_index(const char* index_dir_path, const char* out_path);

#endif
