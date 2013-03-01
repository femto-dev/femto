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

  femto/src/main/index.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <ctype.h>

#include <unistd.h> // for ftruncate
#include <sys/types.h>
#include <errno.h>

#include "index.h"
#include "bzlib_private.h"
#include "buffer_funcs.h"
#include "wtree_funcs.h"
#include "bwt_writer.h"
#include "util.h"
#include "results.h"
#include "page_utils.h"

enum {
  BLOCK_HEADER_SIZE = 88
};

enum {
  BUCKET_OFFSET_START = 0,
  BUCKET_OFFSET_MAP,
  BUCKET_OFFSET_WTREE,
  BUCKET_OFFSET_MARK_TABLES,
  BUCKET_OFFSET_MARK_ARRAYS,
  BUCKET_OFFSET_NUM_CHUNKS,
  BUCKET_NUM_OFFSETS
};
static inline
int32_t bucket_chunk_dir_offset(void)
{
  return BUCKET_NUM_OFFSETS * 4;
}
static inline
int32_t bucket_read_num_chunks(void * bucket_data)
{
  int* hdr = bucket_data;
  return ntoh_32(hdr[BUCKET_OFFSET_NUM_CHUNKS]);
}

// some structures.
typedef struct {
  int count; // number of markRecords
  int pos; // current offset into markRecords
  int table_bits; // number of bits in table.
  buffer_t table; // recording 0 or 1 to indicate whether that
                  // character in the L column is marked.
  buffer_t markRecords;
} mark_by_char;

typedef struct {
  int len; // the bucket length
  int* L; // the L data we're compressing with this bucket; unseqToSeq[ch]
  alpha_t unseqToSeq[ALPHA_SIZE];
  int occsSeq[ALPHA_SIZE];
  unsigned char huff_len[ALPHA_SIZE];
  int32_t huff_code[ALPHA_SIZE];
  int32_t huff_rfreq[ALPHA_SIZE];
  int nInUse;
  int alphaSize;
  unsigned char inUse[ALPHA_SIZE];
  mark_by_char mark[ALPHA_SIZE]; // by actual character of them
} bucket_compress_info_t;


typedef struct {
 // Information stored in the Index Block Header:
  // Information for the containing index
  index_locator_t index_location;
  int64_t text_size; // the number of indexed bytes (uncompressed size of L).
  int64_t n_blocks; // the number of blocks in the index.
  int64_t C[ALPHA_SIZE]; // C[b] the number of characters in the entire 
                        // index which are alphabetically smaller than b
  // Storage parameters
  index_block_param_t block_param;
  // Block storage information - what's stored in this block?
  // Information about what's contained in this block:
  int64_t number; // the number of this block in the index.
  int32_t size; // the number of indexed bytes (uncompressed size of our L section); <= index_info.block_size
  int64_t bucket_start;
  int32_t bucket_count; // the number of buckets in the block
  int64_t occs_total[ALPHA_SIZE]; // # occs for each character
  // stuff we gather
  int occs_since_block[ALPHA_SIZE]; // # occurences since block
  int occs[ALPHA_SIZE]; // # occurences inside a bucket
  bucket_compress_info_t bucket;

  unsigned char* zdata; // the compressed data section.
} index_block_builder_t;

// a few prototypes
void write_occs(buffer_t* buf, unsigned char inUse[ALPHA_SIZE],
                int maxbits, unsigned int occs[ALPHA_SIZE]);
error_t write_index_block(index_block_builder_t* block, 
                          construct_statistics_t* out);


void set_default_param(index_block_param_t* p)
{
 p->b_size=1024*1024; // 1 MB buckets
 p->variable_block_size = 0;
 //p->block_size=(p->b_size*16); // => 16 MB blocks
 
 // When compression doesn't go well, we get 2x expansion,
 // and 1GB blocks are too big on a machine with 2GB virtual address space.
 // So we make these 256 MB -> 3*512MB == 1.5 GB OK on 2GB machine.
 // 1.5GB is in fact too small for a 2GB machine. Therefore we instead
 // make this 128MB -> 3*256MB == 768 MB OK on 2GB machine.
 p->block_size=(p->b_size*128); // => 128MB blocks

 p->mark_period = 20;
 p->mark_type = 1;

 p->variable_chunk_size = 0;
 p->chunk_size = 2048;

 calculate_params(p);
}

// returns the end of the token.
int parse_param_set(const char* s, int ntok, char* tok, int nval, char* value)
{
  int i = 0; 
  int j = 0;
  int k = 0;

  // pass over whitespace or commas.
  while( s[i] && (s[i] == ' ' || s[i] == ',') ) {
    i++;
  }

  // pass over anything alpha_numeric.
  while( s[i] && (isalnum( (unsigned char) s[i]) || s[i] == '_' || s[i] == '-') ) {
    if( j < ntok-1 ) tok[j++] = s[i];
    i++;
  }

  // pass over an equals sign.
  if( s[i] == '=' ) {
   i++;
   while( s[i] && (isalnum( (unsigned char) s[i]) || s[i] == '_' || s[i] == '-') ) {
     if( k < nval-1 ) value[k++] = s[i];
     i++;
   }
  }

  // add a null for tok.
  tok[j] = 0;

  // add a null for value.
  value[k] = 0;

  // pass over whitespace or commas.
  while( s[i] && (s[i] == ' ' || s[i] == ',') ) {
    i++;
  }
  // i is a space or a punctuation character.
  return i;
}

error_t parse_param(index_block_param_t* p, const char* string)
{
  int slen = 256;
  char s[256];
  int vlen = 256;
  char v[256];
  int i;
  error_t err = ERR_NOERR;

  i = 0;

  while( string && string[i] ) {

    i += parse_param_set(&string[i], slen, s, vlen, v);

    if( 0 == strcmp(s, "block_size") ) {
      sscanf(v, "%i", & p->block_size );
    } else if( 0 == strcmp(s, "bucket_size") ) {
      sscanf(v, "%i", & p->b_size );
    } else if( 0 == strcmp(s, "chunk_size") ) {
      sscanf(v, "%i", & p->chunk_size );
    } else if( 0 == strcmp(s, "mark_period") ) {
      sscanf(v, "%i", & p->mark_period );
    } else {
      printf("Invalid parameter %s\n", s);
      err = ERR_INVALID;
      break;
    }

  }

  if( err ) return err;

  return calculate_params(p);
}

void print_block_param_usage(const char* prefix)
{
  printf("%sblock_size=<num>\n", prefix);
  printf("%sbucket_size=<num>\n", prefix);
  printf("%schunk_size=<num>\n", prefix);
  printf("%smark_period=<num>\n", prefix);
}

void BZ2_bz__AssertH__fail ( int errcode )
{
  fprintf(stderr, "bz assert failure: %i\n", errcode);
  assert(0);
}

static inline
void save_int(buffer_t* buf, int base, int at, int value)
{
  int net = hton_32(value);
  memcpy(&buf->data[base+at*sizeof(int)], &net, sizeof(int));
}

static inline
uint32_t read_int(buffer_t* buf)
{
  return bsDerefUInt32(buf);
}

static inline
uint64_t read_int64(buffer_t* buf)
{
  return bsDerefUInt64(buf);
}

bucket_compress_info_t* create_bucket_compress_info(int bucket_len)
{
  bucket_compress_info_t* b = malloc(sizeof(bucket_compress_info_t));
  if( ! b ) return NULL;
  memset(b, 0, sizeof(bucket_compress_info_t));

  b->L = malloc((bucket_len+1) * sizeof(int)); // room for end-of-bucket
  if( ! b->L ) {
    free(b);
    return NULL;
  }

  return b;
}

void free_bucket_compress_info(bucket_compress_info_t* b)
{
  free(b->L);
  b->L = NULL;
  if( b->mark ) {
    for(int i = 0; i < ALPHA_SIZE; i++ ) {
      if( b->mark[i].table.data ) free(b->mark[i].table.data);
      b->mark[i].table.data = NULL;
      if( b->mark[i].markRecords.data ) free(b->mark[i].markRecords.data);
      b->mark[i].markRecords.data = NULL;
    }
  }
  free(b);
}


/* Given the huffman code lengths, the min len, the max code len, the
   number of symbols coded, assign the "wavelet tree" variant of the
   Huffman code - this is just the normal huffman code with 1 prepended
   to every codeword. That gives the leaves in the wavelet tree.
   */
void wtree_huff_assign_codes( int32_t huff_code[ALPHA_SIZE],
                              unsigned char huff_len[ALPHA_SIZE],
                              int minLen, int maxLen, int alphaSize)
{
    BZ2_hbAssignCodes ( huff_code, huff_len,
                        minLen, maxLen, alphaSize );
    // now adjust all of the codes by prepending 1.
    for( int i = 0; i < alphaSize; i++ ) {
      huff_code[i] |= (1 << huff_len[i]);
    }
}

/* Compresses the passed bucket (with offsets[] and text[])
   and appends the compressed representation to the end of 
   the dst. The bucket conists of offsets[bucket_start]... with bucket_len
   entries.

   Statistics will be added to stats.
   */
error_t compress_bucket( // output areas
                         buffer_t* dst,
                         int* occs_in_bucket, // ALPHA_SIZE
                         construct_statistics_t* stats,
                         // index configuration
                         int64_t total_length,
                         int64_t total_num_documents,
                         int chunk_size, // number of rows per chunk
                         // input areas
                         int bucket_len, // number of rows in this bucket
                         const alpha_t* L,
                         const int64_t* offsets,
                         results_t* chunks // in chunk_size chunks..
                        )
{
  error_t err;
  bucket_compress_info_t* b = create_bucket_compress_info(bucket_len);
  int end_of_bucket;
  int offset_bits = num_bits64(total_length);
  int chunk_num_docs_bits = 0;
  
  if( !b ) return ERR_MEM;

  if( chunk_size > 0 ) chunk_num_docs_bits = num_bits64(chunk_size);

  if( bucket_len < 0 ) {
    err = ERR_PARAM;
    goto error;
  }

  b->len = bucket_len;

  // clear occs_in_bucket
  memset(occs_in_bucket, 0, ALPHA_SIZE*sizeof(int));

  // init the mark table buffers and the record buffers.
  for( int ch = 0; ch < ALPHA_SIZE; ch++ ) {
    bsInitWrite(&b->mark[ch].table);
    bsInitWrite(&b->mark[ch].markRecords);
  }

  // next step - create L and get occs for characters.
  for( int i = 0; i < bucket_len; i++ ) {
    mark_by_char* m;
    int64_t ch, offset;
   
    ch = L[i];
    offset = offsets[i];

    assert( ch >= 0 );
    assert( ch < ALPHA_SIZE );

    b->L[i] = ch;
    occs_in_bucket[ch]++; // count occurences.

    // also get marking information for marking characters..
    m = &b->mark[ch];

    // make sure we have room for 16 bytes more...
    err = buffer_extend(&m->table, 16);
    if( err ) goto error;

    m->table_bits++;
    if( offset != -1 ) {
      m->count++;
      // append the 1 bit to the table
      bsW24(&m->table, 1, 1);
      err = buffer_extend(&m->markRecords, 16*offset_bits);
      if( err ) goto error;
      // add the mark record.
      bsW64( &m->markRecords, offset_bits, offset);
    } else {
      // append the 0 bit to the table
      bsW24(&m->table, 1, 0);
    }

  }

  // compute inUse and unseqToSeq
  b->nInUse = 0;
  for( int k = 0; k < ALPHA_SIZE; k++ ) {
    if( occs_in_bucket[k] > 0 ) {
      b->inUse[k] = 1;
      b->unseqToSeq[k] = b->nInUse;
      b->huff_rfreq[b->nInUse] = occs_in_bucket[k];
      b->nInUse++;
      // finish writing the table
      bsFinishWrite(&b->mark[k].table);
      // finish writing the records
      bsFinishWrite(&b->mark[k].markRecords);
    } else {
      b->inUse[k] = 0;
    }
  }

  end_of_bucket = b->nInUse;
  b->alphaSize = b->nInUse+1;
  b->huff_rfreq[end_of_bucket] = 1;

  { // compute huffman codes
    int minLen, maxLen;

    // compute huffman coding.
    BZ2_hbMakeCodeLengths ( b->huff_len, b->huff_rfreq, b->alphaSize, 20 );

    // compute minlen and maxlen.
    minLen = 32;
    maxLen = 0;

    for( int k = 0; k < b->alphaSize; k++ ) {
      if( b->huff_len[k] > maxLen ) maxLen = b->huff_len[k];
      if( b->huff_len[k] < minLen ) minLen = b->huff_len[k];
    }
    AssertH ( !(maxLen > 20), 3004 );
    AssertH ( !(minLen < 1),  3005 );

    // assign the actual codes
    wtree_huff_assign_codes( b->huff_code, b->huff_len,
                             minLen, maxLen, b->alphaSize);
  }

  // Translate all of the characters to be sequence (no more gaps)
  for( int i = 0; i < bucket_len; i++ ) {
    b->L[i] = b->unseqToSeq[b->L[i]];
  }
  // add the end-of-bucket character
  b->L[bucket_len] = end_of_bucket;

  // Now do the compressing and the storing.
  {
    int wtree_len;
    unsigned char* wtree;
    int bucket_base;
    int mark;
    int num_chunks;
    int chunk_dir;
    int chunk_total_data_bytes = 0;
    int total_mark_table_and_records = 0;

    //printf("Saving bucket to %#x\n", dst->len);

    bucket_base = dst->len; // the start of the bucket.

    if( chunk_size > 0 ) {
      num_chunks = CEILDIV(bucket_len, chunk_size);
      chunk_total_data_bytes = 0;

      // get size of the chunks.
      for( int chunk_start = 0, chunk_num = 0;
           chunk_start < bucket_len;
           chunk_start += chunk_size, chunk_num++ )
      {
        results_t* chunk = &chunks[chunk_num];
        // Now write the chunk data
        chunk_total_data_bytes += results_data_size(chunk);
      }
      assert( chunk_total_data_bytes >= 0 );
    } else {
      num_chunks = 0;
      chunk_total_data_bytes = 0;
    }
    for( int k = 0; k < ALPHA_SIZE; k++ ) {
      if( b->inUse[k] ) {
        total_mark_table_and_records += b->mark[k].table.len;
        total_mark_table_and_records += b->mark[k].markRecords.len;
      }
    }
    assert(total_mark_table_and_records >= 0);

    // Try to allocate enough so we don't have to realloc()
    err = buffer_extend(dst, BUCKET_NUM_OFFSETS*sizeof(int) // room for offsets
                             + num_chunks*sizeof(int) // room for chunk directory
                             + chunk_total_data_bytes // room for chunk data
                             + 2*ALPHA_SIZE // Mapping table
                             + 3*CEILDIV(ALPHA_SIZE*ALPHA_SIZE, 8) // worst-case Huffman table
                             + 2048 // slack space for misc. #s.
                             + 2*bucket_len
                             + total_mark_table_and_records);

    if( err ) goto error;

    // leave room for the offsets.
    for( int i = 0; i < BUCKET_NUM_OFFSETS; i++ ) {
      bsAssignUInt32(dst, 0);
    }

    // save BUCKET_START
    save_int(dst, bucket_base, BUCKET_OFFSET_START, BUCKET_START);

    // Save the number of chunks in this bucket.
    save_int(dst, bucket_base, BUCKET_OFFSET_NUM_CHUNKS, num_chunks);
    assert( num_chunks == bucket_read_num_chunks( &dst->data[bucket_base] ) );

    mark = dst->len;

    // Save the chunk directory.
    chunk_dir = dst->len;
    assert( dst->len - bucket_base == bucket_chunk_dir_offset() );

    if( chunk_size > 0 ) {
      err = buffer_extend(dst, 16 + (num_chunks+1)*sizeof(int) );
      if( err ) goto error;
      for( int i = 0; i < num_chunks+1; i++ ) {
        bsAssignUInt32(dst, 0);
      }

      // save the chunks.
      for( int chunk_start = 0, chunk_num = 0;
           chunk_start < bucket_len;
           chunk_start += chunk_size, chunk_num++ )
      {
        int len = chunk_size;
        int chunk_bytes;
        results_t* chunk = &chunks[chunk_num];

        assert( chunk->type == RESULT_TYPE_DOCUMENTS );

        if( chunk_start + len > bucket_len ) len = bucket_len - chunk_start;

        assert( len > 0 );
        assert( chunk_num < num_chunks );

        // now save the offset in chunks..
        save_int(dst, chunk_dir, chunk_num, dst->len - bucket_base);

        // Now write the chunk data
        chunk_bytes = results_data_size(chunk);

        err = buffer_extend(dst, 16 +  // room for # documents
                                 chunk_bytes);
        if( err ) goto error;

        // encode the number of results.
        bsInitWrite(dst);
        bsW64( dst, chunk_num_docs_bits, chunk->num_documents );
        bsFinishWrite(dst);

        // write the compressed data
        memcpy(&dst->data[dst->len], chunk->data, chunk_bytes);
        dst->len += chunk_bytes;

        // now save the next offset in chunks..
        save_int(dst, chunk_dir, chunk_num+1, dst->len - bucket_base);
      }
    }
    
    if( stats ) stats->bytes_in_bucket_chunks += dst->len - mark;

    // Make sure we have room for mapping and huffman at this point.
    err = buffer_extend(dst, 2*ALPHA_SIZE // Mapping table
                             + 3*CEILDIV(ALPHA_SIZE*ALPHA_SIZE, 8) // worst-case Huffman table
                             + 2048);
    if( err ) goto error;


    mark = dst->len;
    // save the mapping and coding table.
    save_int(dst, bucket_base, BUCKET_OFFSET_MAP, dst->len - bucket_base);
    { 
      Bool inUse16[ALPHA_SIZE_DIV16];
      int nBytes;

      bsInitWrite ( dst );

      // -------------- mapping for characters inside this bucket
      // save the mapping table - inUse
      for (int i = 0; i < ALPHA_SIZE_DIV16; i++) {
          inUse16[i] = False;
          for (int j = 0; j < 16; j++)
             if (b->inUse[i * 16 + j]) inUse16[i] = True;
      }
     
      nBytes = dst->len;
      for (int i = 0; i < ALPHA_SIZE_DIV16; i++)
         if (inUse16[i]) bsW24(dst,1,1); else bsW24(dst,1,0);

      for (int i = 0; i < ALPHA_SIZE_DIV16; i++)
         if (inUse16[i])
            for (int j = 0; j < 16; j++) {
               if (b->inUse[i * 16 + j]) bsW24(dst,1,1); else bsW24(dst,1,0);
            }

      //VPrintf1( "      bytes: mapping %d, ", dst->len-nBytes );

      // send the coding table
      nBytes = b->len;

      // -------------- Huffman coding table
      {
        int32_t curr;

        curr = b->huff_len[0];
        bsW24 ( dst, 5, curr );
        for (int i = 0; i < b->alphaSize; i++) {
           if( DEBUG > 2) printf(" %i", b->huff_len[i]);
           while (curr < b->huff_len[i]) { bsW24(dst,2,2); curr++; /* 10 */ };
           while (curr > b->huff_len[i]) { bsW24(dst,2,3); curr--; /* 11 */ };
           bsW24 ( dst, 1, 0 );
        }
        assert( dst->len <= dst->max );
        if( DEBUG > 2) printf("\n");
      }

      // finish the occs/mapping/coding area.
      bsFinishWrite ( dst );

      err = buffer_extend(dst, 16);
      if( err ) goto error;
      buffer_align( dst, ALIGN_MASK );
    }
    if( stats ) stats->bytes_in_bucket_map += dst->len - mark;

    // -------------- Relative offset of the wavelet tree
    save_int(dst, bucket_base, BUCKET_OFFSET_WTREE, dst->len - bucket_base);
    
    // Construct the wavelet tree for the L column.
    wtree_construct(&wtree_len, &wtree, b->alphaSize, b->huff_code,
                    bucket_len, b->L,
                    (stats)?(&stats->wtree_stats):(NULL));

    // store the result.
    // append to dst.
    mark = dst->len;
    err = buffer_extend(dst, wtree_len + 16);
    if( err ) goto error;
    memcpy(&dst->data[dst->len], wtree, wtree_len);
    dst->len += wtree_len;

    free(wtree);

    err = buffer_extend(dst, 16);
    if( err ) goto error;
    buffer_align( dst, ALIGN_MASK );
    if( stats ) stats->bytes_in_bucket_wtree += dst->len - mark;

    // -------------- Relative offset of the mark tables
    save_int(dst, bucket_base, BUCKET_OFFSET_MARK_TABLES, dst->len - bucket_base);
    mark = dst->len;
    {
      mark_by_char* m;
      int base = dst->len;
      int zlen;
      unsigned char* zdata;

      // leave room for nInUse offsets.
      err = buffer_extend(dst, b->nInUse*sizeof(int) + 16);
      if( err ) goto error;
      dst->len += b->nInUse*sizeof(int);
      buffer_align( dst, ALIGN_MASK );

      // save the mark tables. First save the offsets.
      for( int ch = 0,k=0; ch < ALPHA_SIZE; ch++ ) {
        if( occs_in_bucket[ch] > 0 ) {
          m = &b->mark[ch];
          // store the offset of this one
          save_int(dst, base, k, dst->len - base);
          // compute the RLE-encoded mark table
          err = bseq_construct(&zlen, &zdata, m->table_bits, m->table.data, NULL);
          if( err ) goto error;
          // store the table.
          err = buffer_extend(dst, zlen + 16);
          if( err ) goto error;
          memcpy(&dst->data[dst->len], zdata, zlen);
          dst->len += zlen;
          // make sure that each bseq is aligned.
          buffer_align( dst, ALIGN_MASK );
          // free the constructed value
          free(zdata);

          k++;
        }
      }
      err = buffer_extend(dst, 16);
      if( err ) goto error;
      buffer_align( dst, ALIGN_MASK );
    }
    if( stats ) stats->bytes_in_bucket_mark_table += dst->len - mark;

    // -------------- Relative offset of the mark arrays
    save_int(dst, bucket_base, BUCKET_OFFSET_MARK_ARRAYS, dst->len - bucket_base);
    mark = dst->len;
    {
      mark_by_char* m;
      int base = dst->len;
      int size;

      // leave room for nInUse offsets.
      err = buffer_extend(dst, 16+b->nInUse*sizeof(int));
      if( err ) goto error;
      dst->len += b->nInUse*sizeof(int);
      buffer_align( dst, ALIGN_MASK );

      // save the mark arrays. First save the offsets.
      for( int ch = 0,k=0; ch < ALPHA_SIZE; ch++ ) {
        if( occs_in_bucket[ch] > 0 ) {
          m = &b->mark[ch];
          // store the offset of this one
          save_int(dst, base, k, dst->len - base);
          size = CEILDIV(m->count * offset_bits, 8); //ceildiv8(m->count * offset_bits);
          err = buffer_extend(dst, size + 16 );
          if( err ) goto error;
          // save the markRecords which are already in network order
          memcpy(&dst->data[dst->len], m->markRecords.data, 
                 size );
          dst->len += size;

          k++;
        }
      }
    }
    if( stats ) stats->bytes_in_bucket_mark_arrays += dst->len - mark;

    err = buffer_extend(dst, 16 );
    if( err ) goto error;
    buffer_align( dst, ALIGN_MASK );

    if( stats ) stats->bytes_in_buckets += dst->len - bucket_base;

    if( dst->len > dst->max ) {
      err = ERR_PARAM;
      goto error;
    }
  }

  err = ERR_NOERR;
error:
  free_bucket_compress_info(b);
  return err;
}

// write occs using some kind of encoding method.
void write_occs(buffer_t* buf, unsigned char inUse[ALPHA_SIZE],
                int maxbits, unsigned int occs[ALPHA_SIZE])
{
  int i;

  bsInitWrite( buf );
  for( i = 0; i < ALPHA_SIZE; i++ ) {
    if( !inUse || inUse[i] ) {
      bsW(buf, maxbits, occs[i]);
    }
  }
  bsFinishWrite( buf );
}

// read the occs - inverse for write_occs
// Probably it'd be more efficient to just get the occ you're looking
// for.. but this is easier to write and will make a good check
// for those later routines.
void read_occs(buffer_t * buf, unsigned char inUse[ALPHA_SIZE],
               int maxbits, unsigned int occs[ALPHA_SIZE])
{
  int i;

  bsInitRead( buf );

  for( i = 0; i < ALPHA_SIZE; i++ ) {
    if( !inUse || inUse[i] ) {
      occs[i] = bsR( buf, maxbits );
    } else {
      occs[i] = 0;
    }
  }

  bsFinishRead( buf );
}

void read_occs_add(buffer_t* buf, unsigned char inUse[ALPHA_SIZE],
                   int maxbits, unsigned int occs[ALPHA_SIZE])
{
  int i;

  bsInitRead( buf );

  for( i = 0; i < ALPHA_SIZE; i++ ) {
    if( !inUse || inUse[i] ) {
      occs[i] += bsR( buf, maxbits );
    }
  }

  bsFinishRead( buf );
}

error_t calculate_params(index_block_param_t* p)
{
  if( p->b_size <= 0 || 
      p->block_size <= 0 ) {
    return ERR_PARAM;
  }

  p->b_size_bits = num_bits32(p->b_size);
  p->block_size_bits = num_bits32(p->block_size);

  if( p->block_size % p->b_size != 0 ) return ERR_PARAM;

  p->num_buckets_per_block = CEILDIV(p->block_size,p->b_size);
  if( p->chunk_size > 0 ) {
    if( p->b_size % p->chunk_size != 0 ) return ERR_PARAM;

    p->num_chunks_per_bucket = CEILDIV(p->b_size,p->chunk_size);
  } else {
    p->num_chunks_per_bucket = 0;
  }

  return ERR_NOERR;
}

error_t write_block_header(FILE* f, block_header_t* hdr)
{
  error_t err;
  long pos;

  pos = ftell(f);
  if( pos != 0 ) return ERR_INVALID;

  err = write_int32(f, hdr->block_start);
  if( err ) return err;
  err = write_int32(f, hdr->block_version);
  if( err ) return err;
  err = write_int64(f, hdr->block_number);
  if( err ) return err;
  err = write_int64(f, hdr->number_of_blocks);
  if( err ) return err;
  err = write_int64(f, hdr->total_length);
  if( err ) return err;
  err = write_int64(f, hdr->number_of_documents);
  if( err ) return err;
  err = write_int32(f, hdr->num_buckets);
  if( err ) return err;
  err = write_int32(f, hdr->size);
  if( err ) return err;

  // write data from the parameters.
  err = write_int32(f, hdr->param.variable_block_size);
  if( err ) return err;
  err = write_int32(f, hdr->param.block_size);
  if( err ) return err;
  err = write_int32(f, hdr->param.b_size);
  if( err ) return err;
  err = write_int32(f, hdr->param.mark_period);
  if( err ) return err;
  err = write_int32(f, hdr->param.mark_type);
  if( err ) return err;
  err = write_int32(f, hdr->param.variable_chunk_size);
  if( err ) return err;
  err = write_int32(f, hdr->param.chunk_size);
  if( err ) return err;
  err = write_int32(f, wtree_settings_number() );
  if( err ) return err;
  err = write_int32(f, ALPHA_SIZE );
  if( err ) return err;
  err = write_int32(f, END_OF_HEADER );
  if( err ) return err;

  pos = ftell(f);
  if( pos != BLOCK_HEADER_SIZE ) return ERR_INVALID;

  return ERR_NOERR;
}

static inline
int64_t header_block_c_offset(void)
{
  return BLOCK_HEADER_SIZE;
}

static inline
int64_t header_block_block_occs_offset(void)
{
  return header_block_c_offset() + 8*ALPHA_SIZE;
}

static inline
int64_t header_block_doc_ends_offset(block_header_t* hdr)
{
  return header_block_block_occs_offset() + 8*ALPHA_SIZE*hdr->number_of_blocks;
}

static inline
int64_t header_block_doc_eof_rows_offset(block_header_t* hdr)
{
  return header_block_doc_ends_offset(hdr) + 8*hdr->number_of_documents;
}

static inline
int64_t header_block_doc_info_offset(block_header_t* hdr)
{
  return header_block_doc_eof_rows_offset(hdr) + 8*hdr->number_of_documents;
}

static inline
int32_t data_block_bucket_dir_offset(void)
{
  return BLOCK_HEADER_SIZE;
}

static inline
int32_t data_block_bucket_occs_offset(block_header_t* hdr)
{
  return BLOCK_HEADER_SIZE + 4*(hdr->param.num_buckets_per_block+1);
}

/* Update a header block writer. 
   block_num the block we've just added
   occs_total the number of occurences before this block started
   */
error_t update_header_block(header_block_writer_t* w, int64_t block_num, int64_t occs_total[ALPHA_SIZE])
{
  long pos;
  int64_t* arr;
  int64_t occs;

  // Update the occs.
  arr = (int64_t*) &w->mmap.data[header_block_block_occs_offset()];

  // these are stored by character - each character has
  // num_blocks entries.
  for( int ch = 0; ch < ALPHA_SIZE; ch++ ) {
    occs = occs_total[ch];
    pos = ch * w->hdr.number_of_blocks + block_num;
    arr[pos] = hton_64(occs);
  }

  return ERR_NOERR;
}

error_t update_header_block_with_doc_info(header_block_writer_t* w, int64_t doc_num, int64_t len, unsigned char* data)
{
  error_t err;
  int64_t* arr;

  // Extend to include the doc info size.
  err = buffer_extend_mmap(&w->mmap, w->f, len);
  if( err ) return err;

  arr = (int64_t*) &w->mmap.data[header_block_doc_info_offset(&w->hdr)];
  arr[doc_num] = hton_64(w->mmap.len);
  // now.. write the info.
  memcpy(&w->mmap.data[w->mmap.len], data, len);
  w->mmap.len += len;

  // Update the next one so that 
  // arr[doc_num+1]-arr[doc_num] == len
  arr[doc_num+1] = hton_64(w->mmap.len);

  return ERR_NOERR;
}

error_t update_header_block_with_doc_end(header_block_writer_t* w, int64_t doc_num, int64_t doc_end)
{
  int64_t* arr;

  arr = (int64_t*) &w->mmap.data[header_block_doc_ends_offset(&w->hdr)];
  arr[doc_num] = hton_64(doc_end);

  return ERR_NOERR;
}
error_t update_header_block_with_doc_eof_row(header_block_writer_t* w, int64_t doc_num, int64_t eof_row)
{
  int64_t* arr;

  arr = (int64_t*) &w->mmap.data[header_block_doc_eof_rows_offset(&w->hdr)];
  arr[doc_num] = hton_64(eof_row);

  return ERR_NOERR;
}
error_t finish_header_block(header_block_writer_t* writer)
{
  return munmap_buffer_truncate(&writer->mmap, writer->f);
}

error_t begin_header_block(header_block_writer_t* writer,
                           construct_statistics_t* stats,
	                   // input arguments:
	                   FILE* f,
	                   const char* index_path,
	                   int64_t number_of_blocks,
	                   int64_t total_length,
	                   int64_t number_of_documents,
	                   const index_block_param_t* param,
                           const int64_t C[ALPHA_SIZE])
{
  long pos;
  error_t err;


  if( ! writer ) return ERR_PARAM;
  if( ! f ) return ERR_IO_UNK;

  writer->f = f;
  writer->mmap = build_buffer(0, NULL);

  memset(&writer->hdr, 0, sizeof(block_header_t));

  // Fill out the header.
  writer->hdr.block_start = HEADER_BLOCK_START;
  writer->hdr.block_version = HEADER_BLOCK_VERSION;
  writer->hdr.block_number = -1;
  writer->hdr.number_of_blocks = number_of_blocks;
  writer->hdr.total_length = total_length;
  writer->hdr.number_of_documents = number_of_documents;
  writer->hdr.num_buckets = 0;
  writer->hdr.size = 0;
  writer->hdr.param = *param;

  err = write_block_header(f, &writer->hdr);
  if( err ) return err;

  // write the C array.
  pos = ftell(f);
  assert( pos == header_block_c_offset() );

  for( int i = 0; i < ALPHA_SIZE; i++ ) {
    err = write_int64(f, C[i]);
    if( err ) return err;
  }

  // write block_occs
  pos = ftell(f);
  assert( pos == header_block_block_occs_offset() );
  
  // write number_of_blocks * ALPHA_SIZE 8-byte zeros.
  for( int ch = 0; ch < ALPHA_SIZE; ch++ ) {
    for( int64_t i = 0; i < number_of_blocks; i++ ) {
      err = write_int64(f, 0);
      if( err ) return err;
    }
  }

  // write doc ends
  pos = ftell(f);
  assert( pos == header_block_doc_ends_offset(&writer->hdr) );

  for( int64_t doc = 0; doc < number_of_documents; doc++ ) {
    err = write_int64(f, 0 );
    if( err ) return err;
  }

  // write doc eof rows
  pos = ftell(f);
  assert( pos == header_block_doc_eof_rows_offset(&writer->hdr) );

  for( int64_t doc = 0; doc < number_of_documents; doc++ ) {
    err = write_int64(f, 0 );
    if( err ) return err;
  }

  pos = ftell(f);
  assert( pos == header_block_doc_info_offset(&writer->hdr) );

  // write the doc infos.
  for( int64_t doc = 0; doc < number_of_documents+1; doc++ ) {
    err = write_int64(f, 0);
  }

  fflush(f); // REQUIRED before the mmap.
  err = mmap_buffer(&writer->mmap, f, 1, 0);
  if( err ) return err;

  return ERR_NOERR;
  
}

error_t update_data_block(data_block_writer_t* w, int bucket_number, unsigned char* zbucket, int zlen, const int bucket_occs[ALPHA_SIZE])
{
  error_t err;
  uint32_t* b_dir;
  uint32_t* stored_occs;
  FILE* f = w->f;
  int number_of_buckets = w->hdr.num_buckets;
  //int wrote;

  if( bucket_number > number_of_buckets ) return ERR_PARAM;

  // update the bucket_occs.
  stored_occs = (uint32_t*) &w->mmap.data[data_block_bucket_occs_offset(&w->hdr)];
  for( int i = 0; i < ALPHA_SIZE; i++ ) {
    // store the bucket_occs value.
    stored_occs[i*number_of_buckets + bucket_number] = hton_32(bucket_occs[i]);
  }

  // extend to include the compressed bucket size
  err = buffer_extend_mmap(&w->mmap, f, zlen+2*ALIGN_MASK); // includes room for padding
  if( err ) return err;

  buffer_align(&w->mmap, ALIGN_MASK);

  // update the pointer.
  b_dir = (uint32_t*) &w->mmap.data[data_block_bucket_dir_offset()];
  b_dir[bucket_number] = hton_32(w->mmap.len);

  // now write the compressed bucket data.
  memcpy(&w->mmap.data[w->mmap.len], zbucket, zlen);
  w->mmap.len += zlen;
  buffer_align(&w->mmap, ALIGN_MASK);

  // update the start of the next bucket.
  b_dir[bucket_number+1] = hton_32(w->mmap.len);

  return ERR_NOERR;
}

error_t finish_data_block(data_block_writer_t* writer)
{
  error_t err;

  err = munmap_buffer_truncate(&writer->mmap, writer->f);
  if( err ) return err;

  return ERR_NOERR;
}

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
                         index_block_param_t* param)

{ // write the block.
  error_t err;
  long pos;

  if( ! writer ) return ERR_PARAM;
  if( ! f ) return ERR_IO_UNK;

  writer->f = f;
  writer->mmap = build_buffer(0, NULL);
  memset(&writer->hdr, 0, sizeof(block_header_t));

  // Fill out the header.
  writer->hdr.block_start = DATA_BLOCK_START;
  writer->hdr.block_version = DATA_BLOCK_VERSION;
  writer->hdr.block_number = block_number;
  writer->hdr.number_of_blocks = number_of_blocks;
  writer->hdr.total_length = total_length;
  writer->hdr.number_of_documents = total_num_documents;
  writer->hdr.num_buckets = number_of_buckets;
  writer->hdr.size = block_size;
  writer->hdr.param = *param;

  err = write_block_header(f, &writer->hdr);
  if( err ) return err;

  // update the bucket directory
  pos = ftell(f);
  assert( pos == data_block_bucket_dir_offset() );

  // write out 0s for the directory for now.
  for( int i = 0; i < writer->hdr.param.num_buckets_per_block+1; i++ ) {
    err = write_int32(f, 0);
    if( err ) return err;
  }

  // update the bucket occs position.
  pos = ftell(f);
  assert( pos == data_block_bucket_occs_offset(&writer->hdr) );

  // now write out room for the bucket occs.
  for( int ch = 0; ch < ALPHA_SIZE; ch++ ) {
    for( int i = 0; i < number_of_buckets; i++ ) {
      err = write_int32(f, 0);
      if( err ) return err;
    }
  }

  err = pad_to_align(f);
  if( err ) return err;

  pos = ftell(f);

  // end of block header.
  if( stats ) {
    pos = ftell(f);
    stats->bytes_in_block_info += pos;
  }
  
  fflush(f); // REQUIRED before the mmap.
  err = mmap_buffer(&writer->mmap, f, 1, 0);
  if( err ) return err;

  return ERR_NOERR;
}


// the bucket_cache_entry
// contains mostly just the mapping from 
// normal characters to Huffman codes.
typedef struct {
  unsigned char inUse[ALPHA_SIZE]; // not strictly necessary here.
  int nInUse;
  int32_t huff_code[ALPHA_SIZE]; // for mapping from seq to code
  alpha_t seqToUnseq[ALPHA_SIZE];
  alpha_t unseqToSeq[ALPHA_SIZE];
  int32_t huff_limit[ALPHA_SIZE]; // not actually used
  int32_t huff_base[ALPHA_SIZE]; // base and perm are for mapping
  int32_t huff_perm[ALPHA_SIZE]; // from code -> character
  int32_t huff_min_len;
  // all of these are offsets within the block.
  int bucket_offset; // where the bucket starts...
  int wtree_offset; // these ones are here pretty much for convienience.
  int mark_tables_offset;
  int mark_arrays_offset;

  int alphaSize; // computed from nInUse..
} cached_bucket_t;

error_t b_fault(cache_id_t cid, int data_size, void* data, void* context)
{
  data_block_t* block = (data_block_t*) context;
  cached_bucket_t* ent = (cached_bucket_t*) data;
  buffer_t b = block->zbits;
  int id = numeric_cache_num_for_id(cid);
  uint32_t b_offset;
  uint32_t map_offset;
  unsigned char inUse16[ALPHA_SIZE_DIV16];
  unsigned char len[ALPHA_SIZE];
  int minLen, maxLen;
  int uc, curr;
  int i, j;

  // step 1: seek our bit buffer to the area we're after.
  b.pos = data_block_bucket_dir_offset() + 4 * id;
  b_offset = read_int( &b );
  ent->bucket_offset = b_offset;

  // we've got the bucket offset.
  // Get the map, wtree, leaf_mark, and marking_arrays offsets.
  b.pos = b_offset + sizeof(int) * BUCKET_OFFSET_MAP;
  map_offset = b_offset + read_int( &b );
  b.pos = b_offset + sizeof(int) * BUCKET_OFFSET_WTREE;
  ent->wtree_offset = b_offset + read_int( &b );
  b.pos = b_offset + sizeof(int) * BUCKET_OFFSET_MARK_TABLES;
  ent->mark_tables_offset = b_offset + read_int( &b );
  b.pos = b_offset + sizeof(int) * BUCKET_OFFSET_MARK_ARRAYS;
  ent->mark_arrays_offset = b_offset + read_int( &b );

  if( (ent->bucket_offset & ALIGN_MASK) ||
      (ent->wtree_offset & ALIGN_MASK) ||
      (ent->mark_tables_offset & ALIGN_MASK) ||
      (ent->mark_arrays_offset & ALIGN_MASK) ) {
    return ERR_FORMAT;
  }

  b.pos = map_offset;

  // step 2: read the mapping table
  /*--- Receive the mapping table ---*/

  bsInitRead( &b );
  for (i = 0; i < ALPHA_SIZE_DIV16; i++) {
     uc = bsR24( &b, 1 );
     if (uc == 1)
        inUse16[i] = 1;
     else
        inUse16[i] = 0;
  }

  for (i = 0; i < ALPHA_SIZE; i++) ent->inUse[i] = 0;
                                                                           
  for (i = 0; i < ALPHA_SIZE_DIV16; i++) {
     if (inUse16[i]) {
        for (j = 0; j < 16; j++) {
           uc = bsR24( &b, 1 );
           if (uc == 1) ent->inUse[i * 16 + j] = 1;
        }
     }
  }


  // compute seqToUnseq, nInUse, and unseqToSeq.
  ent->nInUse = 0;
  for (i = 0; i < ALPHA_SIZE; i++) {
    if (ent->inUse[i]) {
      ent->seqToUnseq[ent->nInUse] = i;
      ent->unseqToSeq[i] = ent->nInUse;
      ent->nInUse++;
    } else {
      ent->unseqToSeq[i] = INVALID_SEQ;
    }
  }

  ent->alphaSize = ent->nInUse + 1; // the end-of-bucket character
  if( DEBUG > 2 ) printf("Received alpha size %i\n", ent->alphaSize);

  // receive the coding table

  /*--- Now the coding table ---*/
  curr = bsR24( &b, 5 );
  for (i = 0; i < ent->alphaSize; i++) {
    while (1) {
       if (curr < 1 || curr > 20) return (ERR_BZ_DATA);
       uc = bsR24( &b, 1 );
       if (uc == 0) break;
       uc = bsR24( &b, 1 );
       if (uc == 0) curr++; else curr--;
    }
    len[i] = curr;
    if( DEBUG > 2 ) printf(" %i", len[i]);
  }
  if( DEBUG > 2 ) printf("\n");
 
  /*--- Create the Huffman decoding tables ---*/
  minLen = 32;
  maxLen = 0;
  for (i = 0; i < ent->alphaSize; i++) {
    if (len[i] > maxLen) maxLen = len[i];
    if (len[i] < minLen) minLen = len[i];
  }
  BZ2_hbCreateDecodeTables (
      ent->huff_limit,
      ent->huff_base,
      ent->huff_perm,
      len,
      minLen, maxLen, ent->alphaSize
  );
  ent->huff_min_len = minLen;


  bsFinishRead( &b );


  // also set the encode table.
  wtree_huff_assign_codes( ent->huff_code, len,
                           minLen, maxLen, ent->alphaSize);

  return ERR_NOERR;
}

void b_evict(cache_id_t cid, int data_size, void* data, void* context)
{
}

error_t read_block_header(const buffer_t* block_data, uint32_t check_start, uint32_t check_version, block_header_t* hdr)
{
  // Make our own copy of buffer so we can change position.
  buffer_t b = *block_data;
  int wtree_settings_number_read;
  int alpha_size_read;
  int end_of_header_read;
  error_t err;

  b.pos = 0; // seek to the start of the file.

  hdr->block_start = bsDerefUInt32 ( &b );
  if( hdr->block_start != check_start ) {
    return ERR_FORMAT_STR("Invalid block start");
  }

  hdr->block_version = bsDerefUInt32 ( &b );
  if( hdr->block_version != check_version ) {
    return ERR_FORMAT_STR("Wrong block version");
  }

  hdr->block_number = bsDerefUInt64( &b );
  hdr->number_of_blocks = bsDerefUInt64( &b );
  hdr->total_length = bsDerefUInt64( &b );
  hdr->number_of_documents = bsDerefUInt64( &b );
  hdr->num_buckets = bsDerefUInt32( &b );
  hdr->size = bsDerefUInt32( &b );

  hdr->param.variable_block_size = bsDerefUInt32( &b );
  hdr->param.block_size = bsDerefUInt32( &b );
  hdr->param.b_size = bsDerefUInt32( &b );
  hdr->param.mark_period = bsDerefUInt32( &b );
  hdr->param.mark_type = bsDerefUInt32( &b );
  hdr->param.variable_chunk_size = bsDerefUInt32( &b );
  hdr->param.chunk_size = bsDerefUInt32( &b );
  // calculate the rest of the block parameters.
  err = calculate_params(&hdr->param);
  if( err ) return err;

  wtree_settings_number_read = bsDerefUInt32( &b );
  alpha_size_read = bsDerefUInt32( &b );
  end_of_header_read = bsDerefUInt32( &b );

  if( wtree_settings_number_read != wtree_settings_number() ) {
    return ERR_FORMAT_STR("Wrong wavelet tree settings; perhaps the index was constructed with an older, incompatible version");
  }

  if( alpha_size_read != ALPHA_SIZE ) {
    return ERR_FORMAT_STR("Wrong alphabet size");
  }

  if( end_of_header_read != END_OF_HEADER ) {
    return ERR_FORMAT_STR("Bad end of header");
  }

  return ERR_NOERR;
}

error_t close_data_block(data_block_t* block)
{
  // free any caching memory used by the block...
  cache_destroy(&block->bucket_cache);
  if( block->zbits.data ) munmap_buffer(&block->zbits);

  close_stored_block(& block->stored_block );

  memset( block, 0, sizeof(data_block_t) );

  return ERR_NOERR;
}

error_t open_data_block(data_block_t* block,
                        path_translator_t* t,
                        index_locator_t location,
                        int64_t block_number, 
                        int buckets_to_cache)
{
  error_t err;

  memset(block, 0, sizeof(data_block_t));

  // open the block.
  err = open_stored_block_byloc(& block->stored_block,
                                t,
                                block_id_for_type(INDEX_BLOCK_TYPE_DATA, block_number),
                                location, &block->zbits);
  if( err ) return err;

  if( ! block->zbits.data ) {
    err = ERR_IO_UNK;
    goto error;
  }

  err = read_block_header(&block->zbits, DATA_BLOCK_START, DATA_BLOCK_VERSION, &block->hdr);
  if( err ) goto error;

  // Compute the numbers we need.
  block->text_size_bits = num_bits64(block->hdr.total_length);
  block->chunk_num_docs_bits = num_bits64(block->hdr.param.chunk_size);

  // create the cache
  if( buckets_to_cache < 1 ) {
    err = ERR_PARAM;
    goto error;
  }
  
  err = numeric_cache_create(&block->bucket_cache,
                     buckets_to_cache,
                     sizeof(cached_bucket_t),
                     block, b_fault, b_evict);
  if( err ) {
    goto error;
  }


  return ERR_NOERR;

error:
  close_data_block(block);
  return err;
}

error_t close_header_block(header_block_t* block)
{
  error_t err = ERR_NOERR;
  if( block->zbits.data ) munmap_buffer(&block->zbits);

  close_stored_block(& block->stored_block );

  memset( block, 0, sizeof(header_block_t) );

  return err;
}

error_t open_header_block(header_block_t* block,
                          path_translator_t* t,
                          index_locator_t location)
{
  error_t err;

  memset(block, 0, sizeof(header_block_t));

  // open the block.
  err = open_stored_block_byloc(& block->stored_block,
                                t,
                                block_id_for_type(INDEX_BLOCK_TYPE_HEADER,0),
                                location,
                                &block->zbits);
  if( err ) return err;

  if ( ! block->zbits.data ) {
    err = ERR_IO_UNK;
    goto error;
  }

  // Read in the block header.
  err = read_block_header(&block->zbits, HEADER_BLOCK_START, HEADER_BLOCK_VERSION, &block->hdr);
  if( err ) goto error;
  
  return ERR_NOERR;

error:
  close_header_block(block);
  return err;
}

int is_bucket_loaded(data_block_t* blk, int row_in_block)
{
  int bucket;
  bucket = row_in_block / blk->hdr.param.b_size;
  return is_cached_numeric(& blk->bucket_cache, bucket);
}


int bsearch_C(header_block_t* block, int64_t row)
{
  unsigned char* data;
  long pos;
  int64_t* C;

  // first, get a pointer to the C array.
  data = block->zbits.data;
  pos = header_block_c_offset();
  C = (int64_t*) & data[pos];

  return bsearch_int64_ntoh_arr(ALPHA_SIZE, C, row);
}

// get the value for Occ(c, q); q == row
// get a C value given a block.
int64_t get_C(header_block_t* block, int ch)
{
  unsigned char* data;
  long pos;
  int64_t* C;
  int64_t ret;

  if( ch >= ALPHA_SIZE ) return block->hdr.total_length;

  data = block->zbits.data;
  pos = header_block_c_offset();
  C = (int64_t*) & data[pos];

  ret = ntoh_64( C[ch] );

  return ret;
}

int64_t get_block_occs(header_block_t* block, int ch, int64_t block_num)
{
  unsigned char* data;
  int64_t* arr;
  long pos;

  data = block->zbits.data;
  pos = header_block_block_occs_offset();
  arr = (int64_t*) &data[pos];

  pos = ch * block->hdr.number_of_blocks + block_num;
  return ntoh_64(arr[pos]);

}

int64_t bsearch_block_occs(header_block_t* block, int ch, int64_t occs)
{
  unsigned char* data;
  int64_t* arr;
  long pos;

  data = block->zbits.data;
  pos = header_block_block_occs_offset();
  arr = (int64_t*) &data[pos];
  arr = &arr[ch * block->hdr.number_of_blocks];

  return bsearch_int64_ntoh_arr(block->hdr.number_of_blocks, arr, occs - 1);
}

// go from a logical offset to a document number and within-document
// offset.
location_info_t resolve_location(header_block_t* block, int64_t offset)
{
  unsigned char* data;
  int64_t* arr;
  long pos;
  location_info_t ret;
  int64_t prev;

  data = block->zbits.data;
  pos = header_block_doc_ends_offset(&block->hdr);
  arr = (int64_t*) &data[pos];

  // get the index of the previous document.
  prev = bsearch_int64_ntoh_arr(block->hdr.number_of_documents, arr, offset);
  if( prev == -1 ) {
    // our document is the first document.
    ret.offset = offset;
    ret.doc = 0;
  } else {
    ret.offset = offset - ntoh_64(arr[prev]);
    ret.doc = prev+1;
  }

  return ret;
}

int64_t bsearch_block_rows(header_block_t* block, int64_t row)
{
  assert( block->hdr.param.variable_block_size == 0 );
  // index has fixed-size blocks.
  return row / (int64_t) block->hdr.param.block_size;
  /* This commented out code gets the block containing a row
   * from a table in the header, in case in the future we
   * need to support variable-sized blocks (if that happened
   * it would be a result of index merging/updating).
    // otherwise, we have to search.
    unsigned char* data;
    int64_t* arr;
    long pos;
    int64_t prev;

    data = block->zbits.data;
    pos = block->block_ends_offset;
    arr = (int64_t*) &data[pos];

    prev = bsearch_int64_ntoh_arr(block->n_blocks, arr, row);
    if( prev == -1 ) {
      // first block.
      return 0;
    } else {
      return prev+1;
    }
  }
  */
}

// returns the row number of the first row of this block.
int64_t get_block_row(header_block_t* block, int64_t num)
{
  assert( block->hdr.param.variable_block_size == 0 );
  // fixed size blocks.
  return num * (int64_t) block->hdr.param.block_size;
  /* This commented out code gets the first row of a block
   * from a table in the header, in case in the future we
   * need to support variable-sized blocks (if that happened
   * it would be a result of index merging/updating).
    // otherwise, look it up.
    unsigned char* data;
    int64_t* arr;
    long pos;

    data = block->zbits.data;
    pos = block->block_ends_offset;
    arr = (int64_t*) &data[pos];

    if( num == 0 ) return 0;
    else if( num < block->n_blocks ) return ntoh_64(arr[num-1]);
    else return -1;
  }*/
}

int64_t document_length(header_block_t* block, int64_t doc_num)
{
  unsigned char* data;
  int64_t* arr;
  long pos;

  data = block->zbits.data;
  pos = header_block_doc_ends_offset(&block->hdr);
  arr = (int64_t*) &data[pos];

  if( doc_num == 0 ) {
    return ntoh_64(arr[0]);
  } else {
    return ntoh_64(arr[doc_num]) - ntoh_64(arr[doc_num-1]);
  }
}

int64_t document_eof_row(header_block_t* block, int64_t doc_num)
{
  unsigned char* data;
  int64_t* arr;
  long pos;

  data = block->zbits.data;
  pos = header_block_doc_eof_rows_offset(&block->hdr);
  arr = (int64_t*) &data[pos];

  return ntoh_64(arr[doc_num]);
}

error_t header_occs_request(header_block_t* block,
                       header_occs_request_type_t type,
                       header_occs_request_t* req)
{
  int64_t temp;
  error_t err;

  err = ERR_NOERR;

  if( type == 0 || type > MAX_HDR_OCCS_REQUEST ) {
    return ERR_PARAM;
  }

  if( (type & HDR_REQUEST_C) && (type & HDR_BSEARCH_C) ) {
    return ERR_PARAM;
  }
  if( (type & HDR_REQUEST_BLOCK_OCCS) && (type & HDR_BSEARCH_BLOCK_OCCS) ) {
    return ERR_PARAM;
  }

  // HDR_REQUEST_C then HDR_REQUEST_BLOCK_OCCS
  // or HDR_BSEARCH_C then HDR_BSEARCH_BLOCK_OCCS

  if( type & HDR_BSEARCH_BLOCK_ROWS ) {
    req->block_num = bsearch_block_rows(block, req->row);
    req->row = get_block_row(block, req->block_num);
    if( req->row < 0 ) return ERR_INVALID;
  }
  if( type & HDR_REQUEST_C ) {
    req->occs = get_C(block, req->ch);
  }
  if( type & HDR_BSEARCH_C ) {
    req->ch = bsearch_C(block, req->occs);
    temp = get_C(block, req->ch);
    // automatically compute occs for stepping forward
    if( type & HDR_FORWARD ) {
      req->occs = (req->occs + 1 - temp); // block occs value
    } else {
      req->occs = temp;
    }
  }
  if( type & HDR_REQUEST_BLOCK_OCCS ) {
    temp = get_block_occs(block, req->ch, req->block_num);
    if( type & HDR_BACK ) {
      req->occs = req->occs + temp;
    } else {
      req->occs = temp;
    }
  }
  if( type & HDR_BSEARCH_BLOCK_OCCS ) {
    req->block_num = bsearch_block_occs(block, req->ch, req->occs);
    temp = get_block_occs(block, req->ch, req->block_num);
    if( type & HDR_FORWARD ) {
      req->occs = req->occs - temp; // compute the number we'll need in the block request
    } else {
      req->occs = temp; 
    }
  }
  if( type & HDR_REQUEST_BLOCK_ROWS ) {
    req->row = get_block_row(block, req->block_num);
    if( req->row < 0 ) return ERR_PARAM;
  }
  if( type & HDR_REQUEST_NUM_BLOCKS ) {
    req->block_num = block->hdr.number_of_blocks;
  }

  return err;
}

// mallocs the returned info
error_t document_info(header_block_t* block, int64_t doc, int64_t* len, unsigned char** info)
{
  int64_t start, end;
  // seek to the right spot
  block->zbits.pos = header_block_doc_info_offset(&block->hdr) + 8*doc;
  start = bsDerefUInt64(&block->zbits);
  end = bsDerefUInt64(&block->zbits);
  *len = end - start;
  if( *len ) {
    *info = malloc(*len);
    if( ! *info ) return ERR_MEM;
    memcpy(*info, &block->zbits.data[start], *len);
  } else {
    *info = NULL;
  }

  return ERR_NOERR;
}

error_t header_loc_request(header_block_t* block,
                       header_loc_request_type_t type,
                       header_loc_request_t* req)
{
  error_t err;


  if( type == 0 || type > MAX_HDR_LOC_REQUEST ) {
    return ERR_PARAM;
  }

  if( (type & HDR_LOC_REQUEST_DOC_LEN) &&
      (type & HDR_LOC_REQUEST_L_SIZE) ) {
    return ERR_PARAM;
  }

  err = ERR_NOERR;

  if( type & HDR_LOC_RESOLVE_LOCATION ) {
    req->loc = resolve_location(block, req->offset);
  }
  if( type & HDR_LOC_REQUEST_DOC_LEN ) {
    req->doc_len = document_length(block, req->loc.doc);
  }
  if( type & HDR_LOC_REQUEST_L_SIZE ) {
    req->doc_len = block->hdr.total_length;
  }
  if( type & HDR_LOC_REQUEST_DOC_INFO ) {
    err = document_info(block, req->loc.doc, &req->doc_info_len, &req->doc_info);
  }
  if( type & HDR_LOC_REQUEST_NUM_DOCS ) {
    req->doc_len = block->hdr.number_of_documents;
  }

  if( type & HDR_LOC_REQUEST_DOC_EOF_ROW ) {
    req->offset = document_eof_row(block, req->loc.doc);
  }

  return err;
}

int get_bucket_occs(data_block_t* blk, int ch, int bucket)
{
  unsigned char* data;
  long pos;
  uint32_t* arr;

  data = blk->zbits.data;
  pos = data_block_bucket_occs_offset(&blk->hdr);

  arr = (uint32_t*) &data[pos];
  // now arr is the occurence for bucket infos.
  // get infos just for the character we're after.
  arr = &arr[ch * blk->hdr.num_buckets];

  return ntoh_32(arr[bucket]);
}

// gives the bucket number where the change occurs
// so that occs[bucket] < target <= occs[bucket+1]
int bsearch_bucket_occs(data_block_t* blk, int ch, int occs)
{
  unsigned char* data;
  long pos;
  uint32_t* arr;

  data = blk->zbits.data;
  pos = data_block_bucket_occs_offset(&blk->hdr);

  arr = (uint32_t*) &data[pos];
  // now arr is the occurence for bucket infos.
  // get infos just for the character we're after.
  arr = &arr[ch * blk->hdr.num_buckets];

  return bsearch_uint32_ntoh_arr(blk->hdr.num_buckets, arr, occs-1);
}

error_t get_bucket_info(bucket_info_t* return_info, data_block_t* block, int bucket_number)
{
  error_t err;
  int total_bucket_size;
  int id;
  cached_bucket_t* cached_bucket = NULL;
  int chunk_directory;
  int first_chunk_offset, last_chunk_offset;
  int next_bucket_offset;
  int num_chunks;
  uint32_t* chunk_dir;

  NUMERIC_CACHE_GET(err, & block->bucket_cache, bucket_number, cached_bucket );
  if( err ) return err;

  num_chunks = bucket_read_num_chunks( &block->zbits.data[cached_bucket->bucket_offset]);

  chunk_directory = cached_bucket->bucket_offset + bucket_chunk_dir_offset();
  chunk_dir = (uint32_t*) & block->zbits.data[chunk_directory];
  first_chunk_offset = ntoh_32(chunk_dir[0]);
  last_chunk_offset = ntoh_32(chunk_dir[num_chunks]);

  return_info->chunk_bytes = last_chunk_offset - first_chunk_offset;

  return_info->header_bytes = cached_bucket->wtree_offset - cached_bucket->bucket_offset;
  return_info->wavelet_tree_bytes = cached_bucket->mark_tables_offset - cached_bucket->wtree_offset;
  return_info->leaf_mark_table_bytes = cached_bucket->mark_arrays_offset - cached_bucket->mark_tables_offset;


  // get the total bucket size by finding the offset of the next bucket.
  // step 1: seek our bit buffer to the area we're after.
  // next bucket number is in id.
  id = bucket_number + 1;
  if( id >= block->hdr.num_buckets ) {
    next_bucket_offset = block->zbits.len;
  } else {
    buffer_t b = block->zbits;
    b.pos = data_block_bucket_dir_offset() + 4 * id;
    next_bucket_offset = read_int( &b );
  }

  return_info->mark_array_bytes = next_bucket_offset - cached_bucket->mark_arrays_offset;

  total_bucket_size = next_bucket_offset - cached_bucket->bucket_offset;

  return_info->total_bucket_size = total_bucket_size;

  return ERR_NOERR;
}


error_t block_request_row(data_block_t* blk, block_request_t* np)
{
  cached_bucket_t* cached_bucket = NULL;
  int bucket;
  int offset;
  unsigned char* wtree;
  buffer_t* b;
  wtree_query_t wtq;
  int seq;
  int row_in_bucket;
  int bucket_occs;
  error_t err;

  if( np->ch == INVALID_ALPHA ) {
    return ERR_PARAM;
  }
  // get the row number based on the occ and ch values!
  // first, we need to get the appropriate bucket.
  // Do binary search through all of the buckets.

  bucket = bsearch_bucket_occs(blk, np->ch, np->occs_in_block);
  bucket_occs = get_bucket_occs(blk, np->ch, bucket);

  // now that we know which bucket to go to, open that bucket
  // and search for the appropriate row within that bucket
  // using the wavelet tree.

  NUMERIC_CACHE_GET(err, & blk->bucket_cache, bucket, cached_bucket );
  if( err ) return err;

  b = &blk->zbits;
  offset = cached_bucket->wtree_offset;
  wtree = & b->data[offset];

  seq = cached_bucket->unseqToSeq[np->ch];
  if( seq == INVALID_SEQ ) {
    return ERR_MISSING;
  }

  wtq.index = 0;
  wtq.leaf = cached_bucket->huff_code[seq];
  wtq.count = np->occs_in_block - bucket_occs;
  err = wtree_select(wtree, &wtq);
  if( err ) return err;

  // wavelet tree counts rows from 1. 
  row_in_bucket = wtq.index - 1;

  np->row_in_block = bucket*blk->hdr.param.b_size + row_in_bucket;

  return ERR_NOERR;
}

// get the value for Occ(c, q); q == row
// also runs getC
// where row is the row number within the block
// type is the binary OR of the different request types in
// block_request_type_t.
error_t block_request(data_block_t* blk,
                      block_request_type_t type,
                      block_request_t* np)
{
  error_t err;
  buffer_t* b;
  int64_t occ;
  int bucket;
  int row_in_bucket;
  cached_bucket_t* cached_bucket = NULL;
  int row;
  int location_seq, location_occ;

  location_seq = -1;
  location_occ = 0;

  if( type == 0 || type > MAX_BLOCK_REQUEST ) {
    return ERR_PARAM;
  }

  if( type & BLOCK_REQUEST_ROW ) {
    if( (type & BLOCK_REQUEST_OCCS) || (type & BLOCK_REQUEST_CHAR)) {
      return ERR_PARAM;
    }

    // otherwise, we'll need to figure out the row number now!
    // set np->row_in_block.
    err = block_request_row(blk, np);
    if( err ) return err;
  }

  // The others must at least decode a bucket.

  row = np->row_in_block;
  if( row >= blk->hdr.size ) return ERR_PARAM; // invalid request!

  b = &blk->zbits;

  bucket = row / blk->hdr.param.b_size;
  row_in_bucket = row % blk->hdr.param.b_size;
  if( DEBUG > 50 ) printf("block_request on bucket %i row %i\n", bucket, row_in_bucket);

  // get the bucket from the cache. This gives us mapping infos.
  NUMERIC_CACHE_GET(err, & blk->bucket_cache, bucket, cached_bucket );
  if( err ) return err;

  // clear occ.
  occ = 0;

  // first, determine the character (if necessary).

  {
    // get a pointer to the wavelet tree for the block.
    uint32_t offset;
    unsigned char* wtree;
    wtree_query_t wtq;
    uint32_t leaf, ch;
    uint32_t zn, code;


    offset = cached_bucket->wtree_offset;

    wtree = & b->data[offset];

    if( (type & BLOCK_REQUEST_CHAR) || 
        (type & BLOCK_REQUEST_LOCATION) ) {
      // now make the rank query.
      wtq.index = row_in_bucket+1; // counted from 1 in wavelet tree.
      wtq.leaf = 0;
      wtq.count = 0;
      err = wtree_rank(wtree, &wtq);
      if( err ) return err;

      leaf = wtq.leaf;

      // decode wtq.leaf using the cached_bucket data.
      // get the top bit 
      // how many bits?
      zn = 32 - leadz32(leaf);
      zn--; // don't count the leading 1.
      code = leaf & ~(1 << zn); // remove the top bit.

      // now zn is the length of the Huffman code without the 1
      // prefix, and code is the Huffman code without the prefix.
      if( zn > 20 ) return ERR_BZ_DATA;
      if( code > cached_bucket->huff_limit[zn] ) return ERR_BZ_DATA;
      ch = cached_bucket->huff_perm[code - cached_bucket->huff_base[zn]];

      location_seq = ch;
      location_occ = wtq.count;

      if( type & BLOCK_REQUEST_CHAR ) {
        np->ch = cached_bucket->seqToUnseq[ch];

        if(type & BLOCK_REQUEST_OCCS) {
          occ += wtq.count;
        }
      }
    }

    if( type & BLOCK_REQUEST_OCCS) {

      // We'll count occs for ch.
      ch = np->ch;

      if( ! ( type & BLOCK_REQUEST_CHAR ) ) { // occs+char was done with rank
        int seq;
        if( cached_bucket->inUse[ch] ) {
          seq = cached_bucket->unseqToSeq[ch];
          wtq.index = row_in_bucket+1;
          wtq.leaf = cached_bucket->huff_code[seq];
          wtq.count = 0;
          err = wtree_occs(wtree, &wtq);
          if( err ) return err;

          occ += wtq.count;
        }  // otherwise this character isn't in this bucket..
      }


      // get the number of occs from previous blocks.
      // and from previous buckets.

      // get the occs from the bucket
      occ += get_bucket_occs(blk, ch, bucket);

      np->occs_in_block = occ;
    }

    if( type & BLOCK_REQUEST_LOCATION ) {
      int table_off, arr_off;
      unsigned char* table;
      bseq_query_t q;
      int mark_offset;

      // we should try to get a location record!

      if( location_seq < 0 ||
          location_seq > cached_bucket->nInUse ) {
        return ERR_INVALID;
      }

      // check the index of the appropriate character..
      b->pos = cached_bucket->mark_tables_offset + sizeof(int) * location_seq;
      table_off = read_int(b);
      if( table_off & ALIGN_MASK ) return ERR_FORMAT;
      table = & b->data[cached_bucket->mark_tables_offset + table_off];

      memset(&q, 0, sizeof(bseq_query_t));
      q.index = location_occ; // this is the loc_occ'th occurence of the char
      bseq_rank( table, &q);

      if( q.bit ) {
        // if the bit is set, we want to get the location info.

        b->pos = cached_bucket->mark_arrays_offset + sizeof(int) * location_seq;
        arr_off = read_int(b);
        mark_offset = q.occs[1]-1; // the number of 1s counting from 0

        b->pos = cached_bucket->mark_arrays_offset + arr_off;

        bsInitReadAt( b, blk->text_size_bits * mark_offset );
        np->offset = bsR64( b, blk->text_size_bits );
      } else {
        np->offset = INVALID_OFFSET;
      }

    }
  }

  return ERR_NOERR;
}


error_t block_get_chunk(data_block_t* block, int chunk_number, results_t* results)
{
  buffer_t b;
  uint32_t start_offset, end_offset, size;
  int bucket_number;
  int in_bucket_chunk_number;
  int b_offset;
  int chunks_in_bucket;

  // figure out the bucket number.
  bucket_number = chunk_number / block->hdr.param.num_chunks_per_bucket;
  in_bucket_chunk_number = chunk_number % block->hdr.param.num_chunks_per_bucket;
  if( bucket_number >= block->hdr.num_buckets ) return ERR_PARAM;

  b = block->zbits;

  // Get the offset of the bucket.
  b.pos = data_block_bucket_dir_offset() + 4 * bucket_number;
  b_offset = read_int( &b );

  // Read the number of chunks in that bucket.
  chunks_in_bucket = bucket_read_num_chunks( &b.data[b_offset] );
  if( in_bucket_chunk_number >= chunks_in_bucket ) return ERR_PARAM;

  b.pos = b_offset + bucket_chunk_dir_offset() + 4*in_bucket_chunk_number;
  start_offset = read_int( &b ) + b_offset;
  end_offset = read_int( &b ) + b_offset;

  // now read the appropriate chunk.
  b.pos = start_offset;

  // just create a results group by copying.
  results->type = RESULT_TYPE_DOCUMENTS;

  bsInitRead(&b);
  results->num_documents = bsR64( &b, block->chunk_num_docs_bits );
  bsFinishRead(&b);

  // now just copy the rest of the bytes.
  size = end_offset - b.pos;
  results->data = malloc(size);
  if( ! results->data ) return ERR_MEM;

  memcpy(results->data, & b.data[b.pos], size);
  // all done!

  return ERR_NOERR;
}

error_t block_chunk_request(data_block_t* block,
                            block_chunk_request_type_t type,
                            block_chunk_request_t* req)
{
  error_t err;

  if( type > MAX_BLOCK_CHUNK_REQUEST ) return ERR_PARAM;


  if( type & BLOCK_CHUNK_FIND_NUMBER ) {
    // set chunk_number appropriately from first.
    req->chunk_number = req->first / block->hdr.param.chunk_size;
    // update first and last.
    req->first = req->chunk_number * block->hdr.param.chunk_size;
    req->last = req->first + block->hdr.param.chunk_size - 1;
    if( req->last > block->hdr.size - 1 ) {
      req->last = block->hdr.size - 1;
    }
  }

  if( type & BLOCK_CHUNK_REQUEST_DOCUMENTS ) {
    if( req->chunk_number < 0 ) {
      req->results.num_documents = 0;
      req->results.data = NULL;
      req->first = -1;
      req->last = -2; 
    }

    err = block_get_chunk( block, req->chunk_number, &req->results);
    if( err ) return err;

  }

  return ERR_NOERR;
}

static
error_t pad_to_page(FILE* f)
{
  int wrote;
  unsigned char zero = 0;

  size_t page_size = get_page_size();

  while( ftell(f) % page_size  ) {
    wrote = fwrite(&zero, 1, 1, f);
    if( wrote != 1 ) return ERR_IO_UNK;
  }
  fflush(f);

  return ERR_NOERR;
}

static
error_t copy_buffer(FILE* out, buffer_t* buf)
{
  size_t wrote;

  wrote = fwrite(buf->data, buf->len, 1, out);
  if( wrote != 1 ) return ERR_IO_STR_NUM("fwrite failed", errno);

  return ERR_NOERR;
}

error_t flatten_index(const char* index_dir_path, const char* out_path)
{
  path_translator_t trans;
  header_block_t header_block;
  data_block_t data_block;
  index_locator_t loc;
  int64_t num_blocks;
  FILE* out;
  uint64_t ii;
  size_t wrote;
  int rc;
  error_t err;
  flattened_header_t hdr;

  err = path_translator_init(&trans);
  if( err ) return err;

  err = path_translator_id_for_path(&trans, index_dir_path, &loc);
  if( err ) return err;

  // First, we open up the header block.
  err = open_header_block(&header_block, &trans, loc);
  if( err ) return err;

  // Now -- how many data blocks are there?
  num_blocks = header_block.hdr.number_of_blocks;

  err = close_header_block(&header_block);
  if( err ) return err;

  out = fopen(out_path, "w+");
  if( ! out ) return ERR_IO_STR_NUM("fopen failed", errno);

  // OK, create index header.
  memset(&hdr, 0, sizeof(flattened_header_t));

  hdr.start = hton_32(FLATTENED_START);
  hdr.vers = hton_32(FLATTENED_VERSION);
  hdr.num_blocks = hton_64(num_blocks + 1);

  wrote = fwrite(&hdr, sizeof(flattened_header_t), 1, out);
  if( wrote != 1 ) return ERR_IO_STR_NUM("fwrite failed", errno);

  // Create room for the start/end offset of each block, including header block.
  for( int64_t b = 0; b < num_blocks + 2; b++ ) {
    ii = 0;
    wrote = fwrite(&ii, sizeof(ii), 1, out);
    if( wrote != 1 ) return ERR_IO_STR_NUM("fwrite failed", errno);
  }

  err = pad_to_page(out);
  if( err ) return err;

  // Great. Start copying to out_path.
  for( int64_t b = 0; b < num_blocks+1; b++ ) {
    int64_t start, end;

    start = ftell(out);

    if( b == 0 ) {
      // copy the header block.
      err = open_header_block(&header_block, &trans, loc);
      if( err ) return err;

      err = copy_buffer(out, &header_block.zbits);
      if( err ) return err;

      err = close_header_block(&header_block);
      if( err ) return err;
    } else {
      // copy a data block.
      err = open_data_block(&data_block, &trans, loc, b-1, 1);
      if( err ) return err;

      err = copy_buffer(out, &data_block.zbits);
      if( err ) return err;

      err = close_data_block(&data_block);
      if( err ) return err;
    }

    err = pad_to_page(out);
    if( err ) return err;

    end = ftell(out);

    // Write the start and end positions.
    fseek(out, sizeof(flattened_header_t) + b*sizeof(int64_t), SEEK_SET);
    ii = start;
    ii = hton_64(ii);
    wrote = fwrite(&ii, sizeof(ii), 1, out);
    if( wrote != 1 ) return ERR_IO_STR_NUM("fwrite failed", errno);
    ii = end;
    ii = hton_64(ii);
    wrote = fwrite(&ii, sizeof(ii), 1, out);
    if( wrote != 1 ) return ERR_IO_STR_NUM("fwrite failed", errno);
    fseek(out, end, SEEK_SET);
  }

  rc = fclose(out);
  if( rc ) return ERR_IO_UNK;

  path_translator_destroy(&trans);

  return ERR_NOERR;
}

