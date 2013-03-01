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

  femto/src/main/index_merge.c
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
#include "bwt_writer.h"
#include "util.h"
#include "results.h"
#include "index_merge.h"
#include "construct.h"

// not currently supporting merging...
#if SUPPORT_INDEX_MERGE

#define MERGE_DEBUG 0

// updating indexes.
//#define UPDATE_BLOCK_APPROX_HEADER_LEN (round_to_mask(8*8, ALIGN_MASK))
#define UPDATE_BLOCK_MAX_ENTRY (256)

error_t close_update_block(update_block_t* block)
{
  error_t err = ERR_NOERR;
  if( block->zbits.data ) munmap_buffer(&block->zbits);

  if( block->writeable ) {
    err = commit_stored_block(&block->stored_block, STORED_BLOCK_MODE_NONE);
  }
  close_stored_block(& block->stored_block);

  memset(block, 0, sizeof(update_block_t));

  return err;
}


// writes an initial update block, with a header on it.
error_t write_empty_update_block(FILE* f, index_locator_t location, int64_t block_number, double ratio, int start_uncompressed, int max_uncompressed, int offset_size_bits)
{
  // create a new update block with a settings section.
  error_t err;
  int ret;
  long settings_offset_pos, chunk_ends_offset_pos, pos;
  long uncompressed_offset_pos;
  long compressed_chunks_new_rows_offset_ptr, compressed_chunks_offset_ptr;
  data_block_t data_block;
  int chunk_size;
  int chunk_count;
  unsigned char data_block_open = 0;
  char temp_string[64];

  // get some infos from the data block.
  err = open_data_block(&data_block, location, block_number, 1);
  if( err ) goto error;
  data_block_open = 1;

  chunk_size = data_block.block_param.chunk_size;
  chunk_count = data_block.chunk_count;

  // first of all, truncate the old block (maybe one already existed)
  // it's OK if this function does or doesn't extend the file.
  ret = ftruncate(fileno(f), 1024);
  if( ret ) {
    err = ERR_IO_UNK;
    goto error;
  }

  // next, write the header parts..
  err = write_int32(f, UPDATE_BLOCK_START);
  if( err ) goto error;
  err = write_int32(f, UPDATE_BLOCK_VERSION);
  if( err ) goto error;

  settings_offset_pos = ftell(f);
  err = write_int32(f, 0); // settings offset
  if( err ) goto error;
  chunk_ends_offset_pos = ftell(f);
  err = write_int32(f, 0); // chunk_ends_offset
  if( err ) goto error;
  uncompressed_offset_pos = ftell(f);
  err = write_int32(f, 0); // uncompressed_update_offset
  if( err ) goto error;
  compressed_chunks_new_rows_offset_ptr = ftell(f);
  err = write_int32(f, 0); // compressed_chunks_new_rows_offset
  if( err ) goto error;
  compressed_chunks_offset_ptr = ftell(f);
  err = write_int32(f, 0); // compressed_chunks_offset
  if( err ) goto error;
  err = write_int32(f, 0); // compressed_update_offset
  if( err ) goto error;
  err = write_int32(f, END_OF_OFFSETS);
  if( err ) goto error;
  err = pad_to_align(f);
  if( err ) goto error;

  // update the settings offset
  pos = ftell(f);
  ret = fseek(f, settings_offset_pos, SEEK_SET);
  if( ret == -1 ) {
    err = ERR_IO_UNK;
    goto error;
  }
  err = write_int32(f, pos);
  if( err ) goto error;

  ret = fseek(f, pos, SEEK_SET);
  if( ret == -1 ) {
    err = ERR_IO_UNK;
    goto error;
  }

  // now write the settings.
  err = write_int32(f, chunk_size);
  if( err ) goto error;
  err = write_int32(f, chunk_count);
  if( err ) goto error;
  err = write_int32(f, start_uncompressed);
  if( err ) goto error;
  err = write_int32(f, max_uncompressed);
  if( err ) goto error;
  err = write_int8(f, offset_size_bits);
  if( err ) goto error;

  snprintf(temp_string, sizeof(temp_string), "%a", ratio);
  err = write_string(f, temp_string);
  if( err ) goto error;

  err = pad_to_align(f);
  if( err ) goto error;


  // update the chunk_ends offset
  {
    unsigned char* buf;
    long size;
    int wrote;

    pos = ftell(f);
    ret = fseek(f, chunk_ends_offset_pos, SEEK_SET);
    if( ret == -1 ) {
      err = ERR_IO_UNK;
      goto error;
    }
    err = write_int32(f, pos);
    if( err ) goto error;

    ret = fseek(f, pos, SEEK_SET);
    if( ret == -1 ) {
      err = ERR_IO_UNK;
      goto error;
    }

    if( data_block.chunk_ends_offset ) {
      // copy the chunk_ends, if it's available in the data block.
      // variable-sized chunks are there when chunk_size == 0
      buf = &data_block.zbits.data[data_block.chunk_ends_offset];
      size = chunk_count * sizeof(int32_t);

      wrote = fwrite(buf, size, 1, f);
      if( wrote != 1 ) {
        err = ERR_IO_UNK;
        goto error;
      }
    } else {
      int32_t chunk_end = 0;
      // just write what it should be with data_block->param.chunk_size
      for( int i = 0; i < data_block.chunk_count; i++ ) {
        chunk_end += data_block.block_param.chunk_size;
        if( chunk_end > data_block.size ) chunk_end = data_block.size;
        err = write_int32(f, chunk_end);
        if( err ) goto error;
      }
    }
  }

  err = pad_to_align(f);
  if( err ) goto error;

  // write the uncompressed offsets position
  pos = ftell(f);
  fseek(f, uncompressed_offset_pos, SEEK_SET);
  err = write_int32(f, pos);
  if( err ) goto error;
  fseek(f, pos, SEEK_SET);

  // write a basic entry for the uncompressed offsets.
  err = write_int32(f, start_uncompressed);
  if( err ) goto error;
  err = write_int32(f, 0);
  if( err ) goto error;

  // reserve space for the uncompressed entries.
  fseek(f, start_uncompressed * sizeof(update_block_entry_t), SEEK_CUR);
  err = write_int32(f, 0);

  err = pad_to_align(f);
  if( err ) goto error;

  // write compressed_chunks_new_rows
  pos = ftell(f);
  fseek(f, compressed_chunks_new_rows_offset_ptr, SEEK_SET);
  err = write_int32(f, pos);
  if( err ) goto error;
  fseek(f, pos, SEEK_SET);

  for( int i = 0; i < data_block.chunk_count; i++ ) {
    err = write_int32(f, 0);
  }

  err = pad_to_align(f);
  if( err ) goto error;

  // write the empty compressed chunks
  pos = ftell(f);
  fseek(f, compressed_chunks_offset_ptr, SEEK_SET);
  err = write_int32(f, pos);
  if( err ) goto error;
  fseek(f, pos, SEEK_SET);

  for( int i = 0; i < data_block.chunk_count + 1; i++ ) {
    err = write_int32(f, pos);
  }

  err = pad_to_align(f);
  if( err ) goto error;

  fflush(f);

  err = ERR_NOERR;
error:
  if( data_block_open ) close_data_block(&data_block);
  return err;
}

error_t create_update_block(index_locator_t location, int64_t block_number, double ratio, int start_uncompressed, int max_uncompressed, int offset_size_bits)
{
  error_t err;
  stored_block_t update_block;
  FILE* f;

  err = remove_stored_block( block_id_for_type(INDEX_BLOCK_TYPE_DATA_UPDATE,
                                               block_number),
                             location );
  if( err ) return err;

  err = open_stored_block( & update_block,
                           block_id_for_type(INDEX_BLOCK_TYPE_DATA_UPDATE,
                                             block_number),
                           location, STORED_BLOCK_MODE_CREATE_WRITE);
  if( err ) return err;

  f = update_block.f;
  if( ! f ) return ERR_IO_UNK;

  err = write_empty_update_block(f, location, block_number,  ratio, start_uncompressed, max_uncompressed, offset_size_bits);
  if( err ) goto error;

  err = ERR_NOERR;

error:
  close_stored_block( &update_block );
  return err;
}

error_t read_update_block_header(update_block_t* block)
{
  error_t err;

  // read in the block data.
  buffer_t* b = &block->zbits;

  b->pos = 0; // seek to the start of the file.

  if( UPDATE_BLOCK_START != bsDerefUInt32 ( b ) ) {
    err = ERR_FORMAT;
    goto error;
  }
  if( UPDATE_BLOCK_VERSION != bsDerefUInt32 ( b ) ) {
    err = ERR_FORMAT;
    goto error;
  }

  block->settings_offset = bsDerefUInt32 ( b );
  block->chunk_ends_offset = bsDerefUInt32 ( b );
  block->uncompressed_updates_offset = bsDerefUInt32 ( b );
  block->compressed_chunks_new_rows_offset = bsDerefUInt32 ( b );
  block->compressed_chunks_offset = bsDerefUInt32 ( b );
  block->compressed_updates_offset = bsDerefUInt32 ( b );

  if( block->settings_offset == 0 ||
      block->uncompressed_updates_offset == 0 ) {
    err = ERR_FORMAT;
    goto error;
  }

  if( block->settings_offset > b->len ||
      block->chunk_ends_offset > b->len ||
      block->uncompressed_updates_offset > b->len ||
      block->compressed_chunks_offset > b->len ||
      block->compressed_updates_offset > b->len ) {
    err = ERR_FORMAT;
    goto error;
  }

  if( (block->settings_offset & ALIGN_MASK) ||
      (block->chunk_ends_offset & ALIGN_MASK) ||
      (block->uncompressed_updates_offset & ALIGN_MASK) ||
      (block->compressed_chunks_offset & ALIGN_MASK) ||
      (block->compressed_updates_offset & ALIGN_MASK) ) {
    err = ERR_FORMAT;
    goto error;
  }

  // read END_OF_OFFSETS
  if( END_OF_OFFSETS != bsDerefUInt32 ( b ) ) {
    err = ERR_FORMAT;
    goto error;
  }

  // read some settings.
  b->pos = block->settings_offset;
  block->chunk_size = bsDerefUInt32( b );
  block->chunk_count = bsDerefUInt32( b );
  block->start_uncompressed = bsDerefUInt32( b );
  block->max_uncompressed = bsDerefUInt32( b );
  block->offset_size_bits = bsDerefUInt8( b );

  block->ratio = 0.25;
  sscanf((char*) &b->data[b->pos], "%a", &block->ratio);
 
  return ERR_NOERR; 
error:
  return err;
}

// opens the update block. Will not create it.
error_t open_update_block(update_block_t* block, index_locator_t location, int64_t block_number, int writeable)
{
  error_t err;
  FILE* f;
  stored_block_mode_t mode;

  memset(block, 0, sizeof(update_block_t));

  block->writeable = writeable;
  if( writeable ) mode = STORED_BLOCK_MODE_WRITE;
  else mode = STORED_BLOCK_MODE_READ;

  err = open_stored_block(& block->stored_block,
                          block_id_for_type(INDEX_BLOCK_TYPE_DATA_UPDATE, block_number),
                          location, mode);
  if( err ) return err;

  f = block->stored_block.f;
  if ( ! f ) {
    err = ERR_IO_UNK;
    goto error;
  }
  err = mmap_buffer(&block->zbits, f, writeable, 0);
  if( err ) goto error;

  err = read_update_block_header(block);
  if( err ) goto error;

  return ERR_NOERR;
error:
  close_update_block(block);
  return err;
}

int update_block_entry_ntoh_cmp(const void* aP, const void * bP)
{
  update_block_entry_t* a = (update_block_entry_t*)aP;
  update_block_entry_t* b = (update_block_entry_t*)bP;
  int64_t temp;

  // first sort by the destination row, then by the src row.
  temp = ntoh_32(a->dst_row_in_block);
  temp -= ntoh_32(b->dst_row_in_block);
  if( 0 == temp ) {
    // sort by src row.
    temp = ntoh_64(a->src_row);
    temp -= ntoh_64(b->src_row);
  }
  if( temp < 0 ) return -1;
  else if ( temp > 0 ) return 1;
  else return 0;
}

int update_block_entry_cmp(const void* aP, const void * bP)
{
  update_block_entry_t* a = (update_block_entry_t*)aP;
  update_block_entry_t* b = (update_block_entry_t*)bP;
  int64_t temp;

  // first sort by the destination row, then by the src row.
  temp = a->dst_row_in_block;
  temp -= b->dst_row_in_block;
  if( 0 == temp ) {
    // sort by src row.
    temp = a->src_row;
    temp -= b->src_row;
  }
  if( temp < 0 ) return -1;
  else if ( temp > 0 ) return 1;
  else return 0;
}


error_t init_compressed_update_reader(compressed_update_reader_t* r, long compressed_updates_offset, int offset_size_bits, buffer_t* input_buffer)
{
  memset(r, 0, sizeof(compressed_update_reader_t));
  if( input_buffer == NULL ) {
    // make a reader that returns nothing.
    r->index = 0;
    r->num_updates = 0;
    return ERR_NOERR;
  }
  r->buf = * input_buffer;
  r->ch = INVALID_ALPHA;

  // seek the block to the compressed updates section.
  r->buf.pos = compressed_updates_offset;
  // read the number of updates
  r->num_updates = bsDerefUInt32 ( &r->buf );
  // read the last_dst_row_number
  r->last_dst_row_number = bsDerefUInt32 ( &r->buf );
  // read the last_src_row_number
  r->last_src_row_number = bsDerefUInt64 ( &r->buf );
  // read the offset size
  r->offset_size_bits = offset_size_bits;

  // now we're set at the start of the RLE entries.
  bsInitRead( &r->buf );

  return ERR_NOERR;
}

error_t init_compressed_update_reader_block(compressed_update_reader_t* r, update_block_t* block)
{
  if( block->compressed_updates_offset == 0 ) {
    // we have no compressed entries. Create an empty reader.
    return init_compressed_update_reader(r, 0, 0, NULL);
  } else {
    return init_compressed_update_reader(r, block->compressed_updates_offset, block->offset_size_bits, &block->zbits);
  }
}

// Puts an update entry into ent and returns the run length.
// returns 1 if we returned one, 0 if we're done
int read_compressed_update(compressed_update_reader_t* r)
{
  int64_t temp;

  // we're going to return the entry at r->index
  // or 0 if we're done.
  if( r->index >= r->num_updates ) return 0;

  // read the bit indicating if we have a character
  temp = bsR24( &r->buf, 1 );
  if( temp ) {
    // read a character
    r->ch = bsR( &r->buf, ALPHA_SIZE_BITS);
  } // otherwise, just use the last character

  // read the dst delta
  if( MERGE_DEBUG ) printf("Read entry decoding dst_delta pos %i", (int) r->buf.pos);
  temp = buffer_decode_gamma( &r->buf );
  if( MERGE_DEBUG ) printf(" %i\n", (int) temp);
  r->last_dst_row_number += temp - 1;

  // read the src delta
  if( MERGE_DEBUG ) printf("Read entry decoding src_delta pos %i", (int) r->buf.pos);
  temp = buffer_decode_gamma( &r->buf );
  if( MERGE_DEBUG ) printf(" %i\n", (int) temp);
  r->last_src_row_number += temp - 1;

  // read the bit indicating if we have an offset
  temp = bsR24( &r->buf, 1 );

  if( temp ) {
    // read the offset
    if( MERGE_DEBUG ) printf("Read entry decoding offset pos %i bits %i", (int) r->buf.pos, (int) r->offset_size_bits);
    temp = bsR( &r->buf, r->offset_size_bits );
    if( MERGE_DEBUG ) printf(" %i\n", (int) temp);
  } else {
    temp = INVALID_OFFSET;
  }

  // update the entry we're to return.
  r->ent.dst_row_in_block = r->last_dst_row_number;
  r->ent.ch = r->ch;
  r->ent.src_row = r->last_src_row_number;
  r->ent.offset = temp;
  if( MERGE_DEBUG ) printf("Read entry dst %x src %x ch %x off %x\n", (int) r->last_dst_row_number, (int) r->last_src_row_number, (int) r->ch, (int) temp);

  r->index++;

  return 1;

}

void free_compressed_update_reader(compressed_update_reader_t* r)
{
  bsFinishRead( &r->buf );
}

static inline
int32_t update_block_uncompressed_entry_offset(int32_t uncompressed_updates_offset, int64_t idx)
{
  return uncompressed_updates_offset + 8 + idx*sizeof(update_block_entry_t);
}

static inline
update_block_entry_t update_block_read_uncompressed_entry(buffer_t* buf, int32_t uncompressed_updates_offset, int64_t idx)
{
  update_block_entry_t c;

  buf->pos = update_block_uncompressed_entry_offset(uncompressed_updates_offset,idx);
  c.dst_row_in_block = bsDerefUInt32( buf );
  c.ch = bsDerefUInt32( buf );
  c.src_row = bsDerefUInt64( buf );
  c.offset = bsDerefUInt64( buf );
  c.doc = bsDerefUInt64( buf );

  return c;
}
static inline
int32_t update_block_read_chunk_end(buffer_t* buf, int32_t chunk_ends_offset, int32_t idx)
{
  assert( chunk_ends_offset != 0 );
  buf->pos = chunk_ends_offset + 4*idx;
  return bsDerefUInt32( buf );
}
static inline
int32_t update_block_read_chunk_size(buffer_t* buf, int32_t chunk_ends_offset, int32_t idx)
{
  if( idx == 0 ) return update_block_read_chunk_end(buf, chunk_ends_offset, idx);
  else return update_block_read_chunk_end(buf, chunk_ends_offset, idx) - update_block_read_chunk_end(buf, chunk_ends_offset, idx-1);
}
static inline
int32_t update_block_read_chunk_new_rows(buffer_t* buf, int32_t compressed_chunks_new_rows_offset, int32_t idx)
{
  buf->pos = compressed_chunks_new_rows_offset + 4*idx;
  return bsDerefUInt32( buf );
}
static inline
void update_block_write_chunk_new_rows(buffer_t* buf, int32_t compressed_chunks_new_rows_offset, int32_t idx, int32_t new_rows)
{
  long savelen = buf->len;
  buf->len = compressed_chunks_new_rows_offset + 4*idx;
  bsAssignUInt32( buf, new_rows );
  buf->len = savelen;
}

error_t update_block_read_chunk(buffer_t* buf, int32_t compressed_chunks_offset, int32_t idx, results_t* out)
{
  int32_t start, end;

  buf->pos = compressed_chunks_offset + 4*idx;
  start = bsDerefUInt32( buf );
  end = bsDerefUInt32( buf );

  *out = empty_results(RESULT_TYPE_DOCUMENTS);
  if( start == end ) {
    return ERR_NOERR; // no results!
  }

  buf->pos = start;
  bsInitRead( buf );
  out->num_documents = buffer_decode_gamma(buf);
  bsFinishRead( buf );

  out->data = malloc( end - start );
  if( ! out->data ) return ERR_MEM;

  // copy over the chunk data
  memcpy(out->data, & buf->data[buf->pos], end - buf->pos);

  return ERR_NOERR;
}


error_t update_block_append_uncompressed( buffer_t* b, int32_t uncompressed_updates_offset, block_update_t* req, int32_t* num_uncompressed_out, int32_t* max_uncompressed_out)
{
  int32_t num_uncompressed, max_uncompressed;

  b->pos = uncompressed_updates_offset;
  max_uncompressed = bsDerefUInt32( b );
  num_uncompressed = bsDerefUInt32( b );
  b->pos = 0;

  if( num_uncompressed >= max_uncompressed ) {
    // the block is already full!
    return ERR_INVALID;
  }

  // append the update
  // save the len
  b->pos = b->len;

  b->len = update_block_uncompressed_entry_offset( uncompressed_updates_offset, num_uncompressed );

  bsInitWrite( b );
  bsPutUInt32( b, req->dst_row_in_block );
  bsPutUInt32( b, req->ch );
  bsPutUInt64( b, req->src_row );
  bsPutUInt64( b, req->offset );
  bsPutUInt64( b, req->doc );
  bsFinishWrite( b );
  if( MERGE_DEBUG ) {
    printf("Writing uncompressed entry dst_row %x src_row %x ch %x off %x doc %x\n", 
         req->dst_row_in_block, (int) req->src_row ,  req->ch , (int) req->offset, (int) req->doc);
  }

  // update num_compressed
  num_uncompressed++;

  b->len = uncompressed_updates_offset;
  b->len += 4; // pass max_updates
  bsInitWrite( b );
  bsPutUInt32( b, num_uncompressed );
  bsFinishWrite( b );

  // restore the len
  b->len = b->pos;

  *num_uncompressed_out = num_uncompressed;
  *max_uncompressed_out = max_uncompressed;

  return ERR_NOERR;
}


/**
  Returns the number of uncompressed and the number of compressed entries
  in an update block.
  */
error_t get_num_updates(buffer_t* b, uint32_t uncompressed_updates_offset, uint32_t compressed_updates_offset, uint32_t* num_uncompressed, uint32_t* num_compressed)
{

  b->pos = uncompressed_updates_offset;
  b->pos += 4; // pass max_uncompressed
  *num_uncompressed = bsDerefUInt32( b );

  if( compressed_updates_offset == 0 ) {
    *num_compressed = 0;
  } else {
    b->pos = compressed_updates_offset;
    *num_compressed = bsDerefUInt32( b );
  }

  return ERR_NOERR;
}

/**
  * Compresses the uncompressed updates in block, merging them
  * in with the other updates. The update_block_t block will be
  * rewritten with the updated block. Linear time.
  */
error_t compress_updates(update_block_t* block)
{
  buffer_t tb;
  error_t err, err2;
  FILE* tf = tmpfile();
  int32_t expect_compressed;
  uint32_t tb_num_compressed;
  int32_t block_max_uncompressed;
  uint32_t tb_num_uncompressed;
  int32_t idx_uncompressed;
  int32_t idx_out;
  update_block_entry_t* ent;
  update_block_entry_t c;
  compressed_update_reader_t r;
  unsigned char have_uncompressed, have_compressed;
  int last_character;
  int32_t last_dst_row_number;
  int64_t last_src_row_number;
  int64_t temp;
  buffer_t* b;
  int32_t uncompressed_updates_ptr_pos;
  int32_t compressed_chunks_ptr_pos, compressed_updates_ptr_pos;
  int32_t chunk_new_rows_ptr_pos;
  int32_t tb_chunk_ends_offset;
  int32_t tb_uncompressed_offset;
  int32_t tb_new_rows_offset;
  int32_t tb_chunks_offset;
  int32_t tb_compressed_offset;

  if( MERGE_DEBUG ) printf("In compress_updates\n");

  if( ! block->writeable ) return ERR_PARAM;
  if( block->ratio < 0.0 ) return ERR_PARAM;
  if( block->start_uncompressed <= 0 ) return ERR_PARAM;
  if( block->max_uncompressed <= 0 ) return ERR_PARAM;

  // 1 - copy block to the tmpfile
  err = mmap_buffer(&tb, tf, 1, block->zbits.max + sizeof(block_update_t));
  if( err ) return err;

  // copy
  memcpy(tb.data, block->zbits.data, block->zbits.max);
  tb_chunk_ends_offset = block->chunk_ends_offset;
  tb_uncompressed_offset = block->uncompressed_updates_offset;
  tb_new_rows_offset = block->compressed_chunks_new_rows_offset;
  tb_chunks_offset = block->compressed_chunks_offset;
  tb_compressed_offset = block->compressed_updates_offset;

  // update the max/num
  err = get_num_updates( &tb, tb_uncompressed_offset, tb_compressed_offset, 
                         &tb_num_uncompressed, & tb_num_compressed );
  if( err ) goto error;

  expect_compressed = tb_num_uncompressed + tb_num_compressed;

  // 2 - sort the uncompressed updates in the tmpfile
  tb.pos = update_block_uncompressed_entry_offset(tb_uncompressed_offset, 0);
  qsort(&tb.data[tb.pos], tb_num_uncompressed, sizeof(update_block_entry_t),
        update_block_entry_ntoh_cmp);

  // 3 - merge in the updates with the compressed data
  // well, we'll need to read the compressed data.

  if( tb_compressed_offset == 0 ) {
    // we have no compressed entries. Create an empty reader.
    err = init_compressed_update_reader(&r, 0, 0, NULL);
    if( err ) goto error;
  } else {
    err = init_compressed_update_reader(&r, tb_compressed_offset, block->offset_size_bits, &tb);
    if( err ) goto error;
  }

  // we're going to rewrite the block's data with the new data.
  // (reading from tb.data)
  b = &block->zbits;

  // adjust the header from the previous guy.
  // We'll drop everything except for the uncompressed updates offset.
  b->len = 0;
  b->len += 4; // pass BLOCK_UPDATE_START
  b->len += 4; // pass BLOCK_UPDATE_VERSION
  b->len += 4; // pass settings_offset (keep this)
  b->len += 4; // pass chunk_ends_offset (keep these)
  uncompressed_updates_ptr_pos = b->len;
  b->len += 4; // pass uncompressed_updates_offset
               // we'll leave this one alone.
  chunk_new_rows_ptr_pos = b->len;
  block->compressed_chunks_new_rows_offset = 0;
  bsInitWrite( b );
  bsPutUInt32( b, 0 ); // zero compressed_chunks_new_rows_offset
  bsFinishWrite( b );
  compressed_chunks_ptr_pos = b->len;
  block->compressed_chunks_offset = 0;
  bsInitWrite( b );
  bsPutUInt32( b, 0 ); // zero compressed_chunks_offset
  bsFinishWrite( b );
  block->compressed_updates_offset = 0;
  compressed_updates_ptr_pos = b->len;
  bsInitWrite( b );
  bsPutUInt32( b, 0 ); // zero compressed_updates_offset
  bsFinishWrite( b );

  // first, make space for the uncompressed updates.
  b->pos = uncompressed_updates_ptr_pos;
  // read the offset into len.
  b->len = bsDerefUInt32( b );
  // write out a header for the uncompressed updates section.
  block_max_uncompressed = block->ratio * expect_compressed + block->start_uncompressed;
  if( block_max_uncompressed >= block->max_uncompressed ) block_max_uncompressed = block->max_uncompressed;
  //printf("block_max_uncompressed is %i/%i\n", block_max_uncompressed, block->max_uncompressed);
  bsInitWrite( b );
  bsPutUInt32( b, block_max_uncompressed ); // max updates
  bsPutUInt32( b, 0 ); // number of updates
  bsFinishWrite( b );

  // make sure the buffer has room for those uncompressed updates!
  err = buffer_extend_mmap(b, block->stored_block.f, 64+block_max_uncompressed*sizeof(update_block_entry_t));
  if( err ) goto error;
  // seek to after all that!
  b->len += block_max_uncompressed*sizeof(update_block_entry_t);

  // write out the chunk new rows records.
  buffer_align( b, ALIGN_MASK );
  block->compressed_chunks_new_rows_offset = b->len;
  b->len = chunk_new_rows_ptr_pos;
  bsInitWrite(b);
  bsPutUInt32( b, block->compressed_chunks_new_rows_offset );
  bsFinishWrite(b);
  b->len = block->compressed_chunks_new_rows_offset;

  bsInitWrite(b);
  for( int i = 0; i < block->chunk_count; i++ ) {
    // we'll update these later - put zeros for now.
    // these are the new rows records
    bsPutUInt32( b, 0 );
  }
  bsFinishWrite(b);

  // write out the compressed chunks
  buffer_align( b, ALIGN_MASK );
  block->compressed_chunks_offset = b->len;
  b->len = compressed_chunks_ptr_pos;
  bsInitWrite( b );
  bsPutUInt32( b, block->compressed_chunks_offset );
  bsFinishWrite( b );
  b->len = block->compressed_chunks_offset;

  // write the compressed chunks table.
  buffer_align( b, ALIGN_MASK );
  bsInitWrite( b );
  for( int i = 0; i < block->chunk_count + 1; i++ ) {
    // we'll update these later - put zeros for now.
    // these are the chunk offsets
    bsPutUInt32( b, 0 );
  }
  bsFinishWrite( b );

  {
    buffer_t docs_buf = build_buffer(0, NULL);
    int ndocs;
    long merged_chunk_bytes;
    int chunk_compressed;
    int chunk_uncompressed;
    int32_t chunk_end;
    long start_offset, end_offset;

    idx_uncompressed = 0;

    // write the compressed chunks..
    for( int chunk_idx = 0; chunk_idx < block->chunk_count; chunk_idx++ ) {
      results_t old_chunk = empty_results(RESULT_TYPE_DOCUMENTS);
      results_t new_chunk = empty_results(RESULT_TYPE_DOCUMENTS);
      results_t merged_chunk = empty_results(RESULT_TYPE_DOCUMENTS);

      chunk_end = update_block_read_chunk_end(&tb, tb_chunk_ends_offset, chunk_idx);
      chunk_compressed = update_block_read_chunk_new_rows(&tb, tb_new_rows_offset, chunk_idx);
      chunk_uncompressed = 0;

      err = update_block_read_chunk(&tb, tb_chunks_offset, chunk_idx, &old_chunk);
      if( err ) goto error;

      docs_buf.len = 0;
      err = buffer_extend( &docs_buf, 1024 );
      if( err ) goto error;

      ndocs = 0;

      // read the uncompressed entries from here and put them in a results.
      while( idx_uncompressed < tb_num_uncompressed ) {
        // read the uncompressed entry
        c = update_block_read_uncompressed_entry(&tb, tb_uncompressed_offset, idx_uncompressed);

        if( c.dst_row_in_block >= chunk_end ) break;

        err = buffer_extend( &docs_buf, 16 );
        if( err ) goto error;

        ((int64_t*) docs_buf.data)[ndocs++] = c.doc;
        if( MERGE_DEBUG ) {
          printf("Appending result %"PRIi64"\n", c.doc);
          if( c.doc > 10 ) {
            printf("FOO\n");
          }
        }
        docs_buf.len += 8;

        chunk_uncompressed++;
        idx_uncompressed++;
      }

      err = results_create_sort(&new_chunk, ndocs, (int64_t*) docs_buf.data);
      if( err ) goto error;

      if( MERGE_DEBUG ) {
        printf("compress_updates unioning results of size %i %i\n", (int) results_data_size(&old_chunk), (int) results_data_size(&new_chunk));
        printf("old chunk results:\n");
        results_print(stdout, &old_chunk);
        printf("new chunk results:\n");
        results_print(stdout, &new_chunk);
      }
      err = unionResults(&old_chunk, &new_chunk, &merged_chunk);
      if( err ) goto error;

      start_offset = b->len;
      if( merged_chunk.num_documents > 0 ) {
        merged_chunk_bytes = results_data_size(&merged_chunk);
        if( MERGE_DEBUG ) { // extra test.
          printf("merged chunk\n");
          results_print(stdout, &merged_chunk);
        }

        // write out the chunk here..
        err = buffer_extend_mmap(b, block->stored_block.f, 16 + merged_chunk_bytes);
        if( err ) goto error;

        bsInitWrite(b);
        buffer_encode_gamma(b, merged_chunk.num_documents);
        bsFinishWrite(b);

        memcpy(&b->data[b->len], merged_chunk.data, merged_chunk_bytes);
        b->len += merged_chunk_bytes;

        //printf("Wrote chunk %i from %i to %i\n", chunk_idx, (int) start_offset, (int) b->len);
      }
      end_offset = b->len;
      // update the chunk offsets
      b->len = block->compressed_chunks_offset + 4 * chunk_idx; 
      bsAssignUInt32( b, start_offset );
      bsAssignUInt32( b, end_offset );
      b->len = end_offset;

      results_destroy(&old_chunk);
      results_destroy(&new_chunk);
      results_destroy(&merged_chunk);

      if( MERGE_DEBUG ) { // extra test.
        err = update_block_read_chunk(b, block->compressed_chunks_offset, chunk_idx, &new_chunk);
        if( err ) goto error;
        printf("After writing\n");
        results_print(stdout, &new_chunk);
        results_destroy(&new_chunk);
      }

      // update the new rows offsets
      update_block_write_chunk_new_rows(b, block->compressed_chunks_new_rows_offset, chunk_idx, chunk_uncompressed+chunk_compressed);
      
    }


    free(docs_buf.data);
  }

  last_src_row_number = 0;
  last_dst_row_number = 0;

  // now we do the merge of uncompressed and compressed.
  last_character = INVALID_ALPHA;

  have_uncompressed = 0;
  have_compressed = 0;
  idx_uncompressed=0;
  for ( idx_out = 0; 1; idx_out++ ) {
    // get the idx'th entry if we don't already have it
    // and there's more entries.
    if( have_uncompressed == 0 && idx_uncompressed < tb_num_uncompressed ) {
      c = update_block_read_uncompressed_entry(&tb, tb_uncompressed_offset, idx_uncompressed);
      if( MERGE_DEBUG ) {
        printf("Merge read uncompressed entry dst_row %x src_row %x ch %x off %x doc %x\n", 
             c.dst_row_in_block, (int) c.src_row ,  c.ch , (int) c.offset, (int) c.doc);
      }
      idx_uncompressed++;
      have_uncompressed = 1;
    }

    if( have_compressed == 0 ) {
      have_compressed = read_compressed_update(&r);
      if( have_compressed ) {
        if( MERGE_DEBUG ) {
          printf("Merge read compressed entry dst_row %x src_row %x ch %x off %x\n", 
         r.ent.dst_row_in_block, (int) r.ent.src_row , r.ent.ch , (int) r.ent.offset);
        }
      }
    }

    ent = NULL;

    if( MERGE_DEBUG ) printf("%i %i -> ", have_uncompressed, have_compressed);
    // now, write out one of them.
    if( !have_uncompressed && !have_compressed ) { 
      break; // we're done!
    } else if ( !have_uncompressed ) {
      ent = &r.ent; // no uncompressed so we must use compressed.
      have_compressed = 0;
    } else if( ! have_compressed ) {
      ent = &c; // no compressed so we must use uncompressed
      have_uncompressed = 0;
    } else {
      // we have both; compare them.
      if( update_block_entry_cmp(&c, &r.ent) < 0 ) {
        // c < r.ent; use c; uncompressed entry.
        ent = &c;
        have_uncompressed = 0;
      } else {
        // c >= r.ent; use r.ent ; use the compressed entry
        ent = &r.ent;
        have_compressed = 0;
      }
    }
    if( MERGE_DEBUG ) printf("%i %i\n", have_uncompressed, have_compressed);

    if( idx_out == 0 ) {
      // update the initial offsets!
      last_src_row_number = ent->src_row;
      last_dst_row_number = ent->dst_row_in_block;

      // fix the compressed_updates_offset
      buffer_align( b, ALIGN_MASK );
      block->compressed_updates_offset = b->len;
      b->len = compressed_updates_ptr_pos;
      bsInitWrite( b );
      bsPutUInt32( b, block->compressed_updates_offset );
      bsFinishWrite( b );
      b->len = block->compressed_updates_offset;

      // write out the compressed_updates header
      // make sure we have room for the header
      err = buffer_extend_mmap(b, block->stored_block.f, 4+4+8);
      if( err ) goto error;

      bsInitWrite(b);
      bsPutUInt32(b, expect_compressed); // # compressed updates
      bsPutUInt32(b, last_dst_row_number); // first dst_row
      bsPutUInt64(b, last_src_row_number); // first src_row
      bsFinishWrite(b);

      bsInitWrite(b);
      // must leave the buffer positioned ready to get the RLE entries
    }

    // now output the entry
    // make sure we have room for an entry-
    // it could be 2+ALPHA_SIZE_BITS+offset_size_bits+2*gamma(row#s)
    // which is < 2+64+64+2*128 < 1024 bits < 256 bytes
    err = buffer_extend_mmap(b, block->stored_block.f, UPDATE_BLOCK_MAX_ENTRY);
    if( err ) goto error;
    if( MERGE_DEBUG ) {
      printf("Merge %i %i Writing compressed entry dst_row %x src_row %x ch %x off %x\n", 
           have_uncompressed, have_compressed,
           ent->dst_row_in_block, (int) ent->src_row ,  ent->ch , (int) ent->offset);
    }
    // now save ent.
    // save the character if needed
    if( ent->ch != last_character ) {
      bsW24(b, 1, 1);
      bsW24(b, ALPHA_SIZE_BITS, ent->ch);
    } else {
      bsW24(b, 1, 0);
    }
    // dst_row_number_delta+1
    temp = 1 + ent->dst_row_in_block - last_dst_row_number;
    if( MERGE_DEBUG ) printf("Encoding row_number_delta+1: len %i %i\n", (int) b->len, (int) temp);
    buffer_encode_gamma(b, temp);
    // src_row_number_delta+1
    if( ent->src_row < last_src_row_number ) return ERR_PARAM;
    temp = 1 + ent->src_row - last_src_row_number;
    //printf("last_row    =%" PRIi64 "\n", last_src_row_number);
    //printf("ent->src_row=%" PRIi64 "\n", ent->src_row);
    if( MERGE_DEBUG ) printf("Encoding src_row_number_delta+1: len %i %i\n", (int) b->len, (int) temp);
    buffer_encode_gamma(b, temp);
    // offset?
    if( ent->offset != INVALID_OFFSET ) {
      bsW24(b, 1, 1);
      if( MERGE_DEBUG ) printf("Encoding offset len %i bits %i", (int) b->len, (int) block->offset_size_bits);
      // write the document offset
      bsW(b, block->offset_size_bits, ent->offset);
      if( MERGE_DEBUG ) printf(" off %i\n", (int) ent->offset);
    } else {
      bsW24(b, 1, 0);
    }
    last_dst_row_number = ent->dst_row_in_block;
    last_src_row_number = ent->src_row;

  }

  if( idx_out > 0 ) bsFinishWrite(b);


  // check that we created the proper number
  assert( idx_out == expect_compressed );

  buffer_align(b, ALIGN_MASK);

  free_compressed_update_reader(&r);

  err = ERR_NOERR;

error:
  err2 = munmap_buffer(&tb);
  fclose(tf);
  if( err ) return err;
  else return err2;
}

error_t block_update_request(update_block_t* block,
                             block_update_type_t type,
                             block_update_t* req)
{
  error_t err;
  int32_t max_uncompressed, num_uncompressed;
  buffer_t* b;

  if( type == BLOCK_UPDATE_DATA ) {
    // grab the appropriate part.
    b = &block->zbits;

    err = update_block_append_uncompressed( b, block->uncompressed_updates_offset, req, &num_uncompressed, &max_uncompressed);

    // if we've hit our maximum number of uncompressed updates, 
    // compress the block
    if( num_uncompressed == max_uncompressed ) {
      err = compress_updates(block);
      if( err ) return err;
    }

  } else {
    return ERR_PARAM;
  }


  return ERR_NOERR;
}

error_t get_merge_block_split(data_block_t* block, update_block_t* update, index_block_param_t* param, int64_t* split_out)
{
  uint32_t num_uncompressed, num_compressed;
  int64_t new_rows;
  int64_t split;
  error_t err;

  // figure out how many updates!
  err = get_num_updates(&update->zbits, update->uncompressed_updates_offset, update->compressed_updates_offset, &num_uncompressed, &num_compressed);
  if( err ) return err;

  // figure out the new number of rows in the block!
  new_rows = num_uncompressed;
  new_rows += num_compressed;
  new_rows += block->size;

  // then, figure out how many times the block is split.
  // first of all, we can only split a maximum of the number of
  // chunks in the destination block.
  // Otherwise, we try to split based on dividing..
  split = new_rows / param->block_size;
  if( block->chunk_count > 0 &&
      split > block->chunk_count ) {
    split = block->chunk_count;
  }
  if( split == 0 ) split = 1;

  if( split_out ) *split_out = split;

  return ERR_NOERR;
}

/**
   Given a data block and the corresponding update block,
   this function merges the block updates to create a new
   block which is output.
   out_loc - the location of the index in which to store the new block
   block, update - openned data and update blocks to merge
   param - the indexing settings to use
   merge_info - information about the index as a whole
   new_info - an array of new_block_info_t with room for
              merge_info->num_split elements. On output, 
              this array will contain num_rows and occs for
              each block.
   */
error_t merge_block_updates(index_locator_t out_loc,
                            data_block_t* block,
                            update_block_t* update,
                            index_block_param_t * param,
                            merge_block_info_t* merge_info,
                            new_block_info_t* new_info)
{
  error_t err = ERR_NOERR;
  FILE* tmp_bwt = NULL;
  int64_t new_size;

  tmp_bwt = tmpfile();
  if( ! tmp_bwt ) {
   return ERR_IO_UNK;
  }


  // Step 1: make a BWT-file for the part of the index we'll be
  // constructing. This gets stored in tmp_bwt.
  {
    int64_t bucket;
    int64_t bucket_start, bucket_end, bucket_size;
    int64_t row_in_bucket, row_in_block;
    block_request_t np;
    compressed_update_reader_t r;
    int have_compressed_update;
    bwt_writer_t bwt_writer;

    // compress the updates to make it easy to go through them
    // and to sort them properly.
    err = compress_updates(update);
    if( err ) goto error3;

    err = init_compressed_update_reader_block(&r, update);
    if ( err ) goto error3;

    new_size = r.num_updates + block->size;

    err = bwt_begin_write(&bwt_writer, tmp_bwt, new_size, 0,
                          ALPHA_SIZE, 0,
                          ALPHA_SIZE_BITS,
                          num_bits64(merge_info->total_size));
    if( err ) goto error1;

    have_compressed_update = 0;

    for( bucket = 0; bucket < block->bucket_count; bucket++ ) {
      bucket_start = bucket * block->block_param.b_size;
      bucket_end = bucket_start + block->block_param.b_size;
      if( bucket_end > block->size ) bucket_end = block->size;
      bucket_size = bucket_end - bucket_start;

      for( row_in_bucket = 0; row_in_bucket < bucket_size; row_in_bucket++ ) {
        row_in_block = bucket_start + row_in_bucket;
        np.row_in_block = row_in_block;
        err = block_request(block,
                            BLOCK_REQUEST_CHAR | BLOCK_REQUEST_LOCATION,
                            &np);
        if( err ) goto error1;

        // output np.ch and np.offset
        if( MERGE_DEBUG > 1 ) {
          printf("merge_block_updates appending (dst) %i %" PRIi64 "\n", np.ch, np.offset);
        }
        err = bwt_append(&bwt_writer, np.ch, np.offset);
        if( err ) goto error1;

        // check for row np.row_in_block in the updates block.
        if( ! have_compressed_update ) {
          have_compressed_update = read_compressed_update(&r);
        }
        while( have_compressed_update &&
               r.ent.dst_row_in_block == row_in_block ) {
          // output the update since it goes after this row.
          if( MERGE_DEBUG > 1 ) {
            printf("merge_block_updates appending (src) %i %" PRIi64 "\n", r.ent.ch, r.ent.offset);
          }
          err = bwt_append(&bwt_writer, r.ent.ch, r.ent.offset);
          if( err ) goto error1;

          // get the next update if we can.
          have_compressed_update = read_compressed_update(&r);
        }

      }
    }

    err = bwt_finish_write(&bwt_writer);

    // clean up!
    error1:
      free_compressed_update_reader(&r);
  }

  if( err ) goto error3;

  // Step 2:
  // build the new data blocks from the bwt we created.
  {
    error_t err2;
    bwt_reader_t bwt_reader;
    stored_block_t stored_data_block;
    int stored_data_block_open = 0;
    int64_t new_number;
    int64_t block_number;
    int32_t dst_row, num_rows;
    int64_t number_of_buckets, bucket_number;
    int32_t total_chunks, num_chunks;
    int32_t chunk_index, chunk_portion;
    int32_t bucket_start, bucket_end, bucket_size;
    buffer_t zdata;
    struct index_occs* occs;
    data_block_writer_t data_block_writer;

    zdata.data = NULL;
    occs = NULL;

    // make a buffer for compressing buckets.
    zdata.max = 4 * param->b_size + 4096;
    zdata = build_buffer(zdata.max, malloc(zdata.max));
    if(! zdata.data ) {
      err = ERR_MEM;
      goto error2;
    }

    occs = malloc(sizeof(struct index_occs));
    if( ! occs ) {
      err = ERR_MEM;
      goto error2;
    };

    err = bwt_reader_open(&bwt_reader, tmp_bwt);
    if( err ) goto error2;

    total_chunks = block->chunk_count;
    chunk_portion = CEILDIV(total_chunks, merge_info->num_split); 
    chunk_index = 0;
    dst_row = 0;
    // go through the blocks we have to add.
    for( new_number = 0; new_number < merge_info->num_split; new_number++) {
      // compute new_block_size

      if( total_chunks == 0 ) {
        num_rows = new_size - dst_row;
        // don't do chunking.
        if( num_rows > param->block_size &&
            new_number != merge_info->num_split - 1 ) {
          // we're not in the last one, and num_rows is bigger than 
          // the block size we're aiming for.
          // decrease the num_rows to be block size
          num_rows = param->block_size;
        }
        num_chunks = 0;
      } else {
        // this is really based on the chunks.
        // We'd like to store in the new index
        num_chunks = total_chunks - chunk_index;
        if( num_chunks > chunk_portion && 
            new_number != merge_info->num_split - 1) {
          // we're not in the last block, and we've got more than
          // our allotment of chunks.
          num_chunks = chunk_portion;
        }

        // compute number of rows.
        num_rows = 0;
        for( int i = 0; i < num_chunks; i++ ) {
          // get the old chunk size
          num_rows += update_block_read_chunk_size(&update->zbits, update->chunk_ends_offset, chunk_index + i);
          // get the size of the new material
          num_rows += update_block_read_chunk_new_rows(&update->zbits, update->compressed_chunks_new_rows_offset, chunk_index + i);
        }
      }


      new_info[new_number].num_rows = num_rows;
      dst_row += num_rows;

      // compute the number of buckets
      number_of_buckets = CEILDIV(num_rows, param->b_size);

      block_number = merge_info->start_block_num + new_number;
     
      // clear occs for this block.
      for( int i = 0; i < ALPHA_SIZE; i++ ) {
        new_info[new_number].occs[i] = 0;
        occs->occs_since_block[i] = 0;
      }
      err = open_stored_block(&stored_data_block,
                              block_id_for_type(INDEX_BLOCK_TYPE_DATA, block_number),
                              out_loc,
                              STORED_BLOCK_MODE_CREATE_WRITE);
      if( err ) goto blockerror;
      stored_data_block_open++;

      err = begin_data_block(&data_block_writer, NULL, 
                             stored_data_block.f, out_loc,
                             merge_info->total_num_blocks,
                             merge_info->total_size, 
                             block_number, number_of_buckets,
                             num_chunks,
                             num_rows, param);
      if( err ) goto blockerror;
      stored_data_block_open++;

      for( bucket_number = 0;
           bucket_number < number_of_buckets;
           bucket_number++ ) {
        // compute some start/end/len
        bucket_start = param->b_size * bucket_number;
        bucket_end = bucket_start + param->b_size;
        if( bucket_end > num_rows ) bucket_end = num_rows;
        bucket_size = bucket_end - bucket_start;
        
        // Find the bucket data in question, and write it right now!
        zdata.len = 0;
        err = compress_bucket(&zdata,
                              bucket_size,
                              &bwt_reader,
                              occs->occs_in_bucket, occs->occs_since_block,
                              NULL);

        if( err ) goto blockerror;
        
        update_data_block(&data_block_writer,
                     bucket_number,
                     zdata.data, zdata.len,
                     occs->occs_since_block);

        // update occs_since_block.
        for( int i = 0; i < ALPHA_SIZE; i++ ) {
          occs->occs_since_block[i] += occs->occs_in_bucket[i];
        }
      }

      dst_row = 0;
      for( int i = 0; i < num_chunks; i++ ) {
        results_t new_chunk = empty_results(RESULT_TYPE_DOCUMENTS);
        results_t block_chunk = empty_results(RESULT_TYPE_DOCUMENTS);
        results_t update_chunk = empty_results(RESULT_TYPE_DOCUMENTS);
        int chunk_rows = 0;

        // get the old chunk size
        chunk_rows += update_block_read_chunk_size(&update->zbits, update->chunk_ends_offset, chunk_index + i);

        // get the size of the new material
        chunk_rows += update_block_read_chunk_new_rows(&update->zbits, update->compressed_chunks_new_rows_offset, chunk_index + i);

        err = block_get_chunk(block, chunk_index + i, &block_chunk);
        if( err ) goto blockerror;

        err = update_block_read_chunk(&update->zbits, update->compressed_chunks_offset, chunk_index + i, &update_chunk);
        if( err ) goto blockerror;

        if( MERGE_DEBUG ) {
          printf("merge block unioning results of size %i %i\n", (int) results_data_size(&block_chunk), (int) results_data_size(&update_chunk));
          printf("block chunk results:\n");
          results_print(stdout, &block_chunk);
          printf("update chunk results:\n");
          results_print(stdout, &update_chunk);
        }
        err = unionResults(&block_chunk, &update_chunk,
                           &new_chunk);
        if( err ) goto blockerror;

        err = update_data_block_chunk(&data_block_writer, i, 
                                      dst_row + chunk_rows,
                                      &new_chunk);

        results_destroy(&new_chunk);
        results_destroy(&update_chunk);
        results_destroy(&block_chunk);

        dst_row += chunk_rows;
      }
      chunk_index += num_chunks;

      // update the new_info occs in the block.
      for( int i = 0; i < ALPHA_SIZE; i++ ) {
        new_info[new_number].occs[i] = occs->occs_since_block[i];
      }

      blockerror:
        if( stored_data_block_open ) {
          finish_data_block(&data_block_writer);
          if( ! err ) {
            err = commit_stored_block(&stored_data_block, STORED_BLOCK_MODE_NONE);
          }
          close_stored_block(&stored_data_block);
          stored_data_block_open = 0;
        }
    }

    error2:
      err2 = bwt_reader_close(&bwt_reader);
      if( err2 && ! err) err = err2;
      free(zdata.data);
      free(occs);

  }

error3:
  if( tmp_bwt ) fclose(tmp_bwt);
  return err;
}

/* Assuming that the documents in the src_loc index have already been
   "added" to dst_loc (in the form of "update" blocks), merge the update
   blocks with the other blocks to create a new index. This function
   will have to be broken apart and rewritten to work when the blocks
   are distributed.
*/
error_t finish_merge_indexes(index_locator_t dst_loc, index_locator_t src_loc, index_locator_t out_loc, index_block_param_t* param )
{
  error_t err, err2;
  header_block_t src_h, dst_h;
  stored_block_t stored_header_block;
  header_block_writer_t header_block_writer;
  int64_t dst_block_num;
  int64_t out_block_num;
  int src_h_open = 0;
  int dst_h_open = 0;
  int out_h_open = 0;
  int update_open = 0;
  int block_open = 0;
  int64_t split = 0;
  int64_t number_of_blocks, total_length, number_of_documents;
  struct index_occs* occs;
  data_block_t block;
  update_block_t update;

  // set variable block lengths and chunks.
  param->variable_block_size = 1;
  param->variable_chunk_size = 1;

  occs = malloc(sizeof(struct index_occs));
  if( ! occs ) return ERR_MEM;

  // open the src and dst header blocks.
  err = open_header_block(&src_h, src_loc, 0);
  if( err ) goto error;
  src_h_open++;

  err = open_header_block(&dst_h, dst_loc, 0);
  if( err ) goto error;
  dst_h_open++;

  // create a new header block
  err = open_stored_block(&stored_header_block,
                          block_id_for_type(INDEX_BLOCK_TYPE_HEADER, 0),
                          out_loc,
                          STORED_BLOCK_MODE_CREATE_WRITE);
  if( err ) goto error;
  out_h_open++;

  // set number_of_blocks, total_length, number_of_documents

  // figure out how many blocks we'll be making.
  number_of_blocks = 0;
  for( dst_block_num = 0; dst_block_num < dst_h.n_blocks; dst_block_num++ ) {
    err = open_update_block(&update, dst_loc, dst_block_num, 0);
    if( err ) goto error;
    update_open++;

    err = open_data_block(&block, dst_loc, dst_block_num, 1);
    if( err ) goto error;
    block_open++;

    // compute the number of blocks we'll get from merging
    split = 0;
    err = get_merge_block_split(&block, &update, param, &split);
    if( err ) goto error;
    assert( split > 0 );

    // add it to the total
    number_of_blocks += split;

    close_update_block(&update);
    update_open--;

    close_data_block(&block);
    block_open--;

  }

  total_length = dst_h.text_size + src_h.text_size;
  number_of_documents = dst_h.num_documents + src_h.num_documents;

  // compute the new occs->C array by summing from the the two header blocks.
  for( int i = 0; i < ALPHA_SIZE; i++ ) {
    int src_c, dst_c;
    dst_c = get_C(&dst_h, i);
    src_c = get_C(&src_h, i);
    occs->C[i] = dst_c + src_c;
  }
  err = begin_header_block(&header_block_writer, NULL, stored_header_block.f,
                           out_loc, number_of_blocks,
                           total_length, number_of_documents, 
                           param, occs->C);
  if( err ) goto error;
  out_h_open++;

  // start filling out the new header block.
  // first of all, we need to update with new index infos and doc ends.
  {
    int64_t out_doc;
    int64_t doc_end;
    int64_t len;
    unsigned char* data;
    header_block_t* blocks[2] = {&dst_h, &src_h};
    header_block_t* block;
    int nblocks = 2;

    out_doc = 0;
    doc_end = 0;
    for( int block_num = 0; block_num < nblocks; block_num++ ) {
      block = blocks[block_num];
      for( int64_t doc = 0; doc < block->num_documents; doc++ ) {
        doc_end += document_length(block, doc);
        err = update_header_block_with_doc_end(&header_block_writer, out_doc, doc_end);
        if( err ) goto error;
        // get the doc info
        err = document_info(block, doc, &len, &data);
        if( err ) goto error;
        err = update_header_block_with_doc_info(&header_block_writer, out_doc, len, data);
        if( err ) goto error;
        out_doc++;
      } 
    }
  }

  // go through the blocks, merging them
  {
    new_block_info_t* new_info;
    merge_block_info_t merge_info;
    int64_t block_end;

    // clear the merge info structure.
    memset(&merge_info, 0, sizeof(merge_block_info_t));

    // clear occs_total
    for( int ch = 0; ch < ALPHA_SIZE; ch++ ) {
      occs->occs_total[ch] = 0;
    }

    merge_info.total_size = total_length;
    merge_info.total_num_documents = number_of_documents;
    merge_info.total_num_blocks = number_of_blocks;
    out_block_num = 0;
    block_end = 0;

    for( dst_block_num = 0; dst_block_num < dst_h.n_blocks; dst_block_num++ ) {
      // merge the block numbered block_num - start by opening block and update
      err = open_data_block(&block, dst_loc, dst_block_num, 1);
      if( err ) goto error;
      block_open++;

      err = open_update_block(&update, dst_loc, dst_block_num, 1);
      if( err ) goto error;
      update_open++;

      // setup merge info
      // call that "get the number of blocks" function again.
      split = 0;
      err = get_merge_block_split(&block, &update, param, &split);
      if( err ) goto error;

      merge_info.start_block_num = out_block_num;
      merge_info.num_split = split;

      new_info = malloc(sizeof(new_block_info_t)*split);

      // next, call the merge function
      err = merge_block_updates(out_loc, &block, &update, param, &merge_info, new_info);
      if( err ) goto error;

      // for each of the created blocks, update the header block.
      for( split = 0; split < merge_info.num_split; split++ ) {
        block_end += new_info[split].num_rows;
        err = update_header_block(&header_block_writer, out_block_num, block_end, occs->occs_total);
        if( err ) goto error;

        // increase occs
        for( int ch = 0; ch < ALPHA_SIZE; ch++ ) {
          occs->occs_total[ch] += new_info[split].occs[ch];
        }

        out_block_num++;
      }

      free(new_info);

      close_data_block(&block);
      block_open--;

      close_update_block(&update);
      update_open--;

    }
  }

  err = ERR_NOERR;

error:
  free(occs);
  if( src_h_open ) {
    err2 = close_header_block(&src_h);
    if( err2 && !err ) err = err2;
  }
  if( dst_h_open ) {
    err2 = close_header_block(&dst_h);
    if( err2 && !err ) err = err2;
  }
  if( out_h_open ) {
    err2 = ERR_NOERR;
    if( out_h_open > 1 ) {
      err2 = finish_header_block(&header_block_writer);
    }
    close_stored_block(&stored_header_block);
    if( err2 && !err ) err = err2;
  }
  if( block_open ) {
    close_data_block(&block);
  }
  if( update_open ) {
    close_update_block(&update);
  }
  return err;

  /*
     Get the number of documents in src
     Get the logical length of dst
     For each document in src, in parallel, do a merge_query, using src_off+dst_len as the offset
     When the merge queries are done, merge:
       - compute the number of out blocks
          - figure out how many destination blocks there will be.
            -- record, for each block, the number of blocks output before it
       - create a new header block
          - set C to the sum of the src&dst Cs
          - append the documents to the end of the "logical" area
          - zero out the block_ends and the block_occs
       - for each dst block (in parallel)
          - get start_out from somewhere
          - merge that block to create blocks start_out ...
          - update the block_ends in the out header block
            -- temporarily, these will just be the size of each block
          - update the occs in the out header block
            -- temporarily, the occs in the header block will be
               just occurences within each block
       - In the header block:
          - add up the occs in the header block to convert it to
            occs since the start instead of occs within.
          - add up the block_ends likewise

  */

}

error_t block_update_manage_request(index_locator_t loc,
                       int64_t block_number, 
                       block_update_manage_type_t type,
                       block_update_manage_t* req)
{
  switch ( type ) {
    case BLOCK_UPDATE_CREATE:
      return create_update_block(loc, block_number, req->ratio, req->start_uncompressed, req->max_uncompressed, req->offset_size_bits);
    case BLOCK_UPDATE_FINISH:
      return finish_merge_indexes(loc, req->src_loc, req->out_loc, &req->index_params);
    default:
      return ERR_PARAM;
  }
}

#endif

