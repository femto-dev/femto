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

  femto/src/main/bwt_writer.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>

#include "bwt.h"
#include "bwt_writer.h"
#include "buffer_funcs.h"
#include "util.h"

static
error_t bwt_init(bwt_writer_t* bwt)
{
  unsigned char* data;
  memset(bwt, 0, sizeof(bwt_writer_t));

  data = (unsigned char*) malloc(BWT_BUF_BYTES);
  if( ! data ) return ERR_MEM;

  bwt->buf = build_buffer(BWT_BUF_BYTES, data);

  return ERR_NOERR;
}

static
void bwt_destroy(bwt_writer_t* bwt)
{
  free(bwt->buf.data);
  bwt->buf.data = NULL;
}


error_t bwt_begin_write(bwt_writer_t* bwt, FILE* f, int64_t number_of_characters, int64_t number_of_documents, int64_t alphabet_size, int64_t mark_period, unsigned char alphabet_size_bits, unsigned char offset_size_bits)
{
  uint64_t big_endian;
  size_t wrote;
  uint32_t temp;
  unsigned char byte;
  error_t err;


  err = bwt_init(bwt);
  if( err ) return err;

  bwt->f = f;
  bwt->alphabet_size_bits = alphabet_size_bits;
  bwt->offset_size_bits = offset_size_bits;

  // write out the beginning of the file.
  temp = hton_32(BWT_START);
  wrote = fwrite(&temp, 4, 1, bwt->f);
  if( wrote != 1 ) return ERR_IO_FILE(bwt->f);

  temp = hton_32(BWT_VERSION);
  wrote = fwrite(&temp, 4, 1, bwt->f);
  if( wrote != 1 ) return ERR_IO_FILE(bwt->f);

  big_endian = hton_64(number_of_characters);
  wrote = fwrite(&big_endian, 8, 1, bwt->f);
  if( wrote != 1 ) return ERR_IO_FILE(bwt->f);

  big_endian = hton_64(number_of_documents);
  wrote = fwrite(&big_endian, 8, 1, bwt->f);
  if( wrote != 1 ) return ERR_IO_FILE(bwt->f);

  big_endian = hton_64(alphabet_size);
  wrote = fwrite(&big_endian, 8, 1, bwt->f);
  if( wrote != 1 ) return ERR_IO_FILE(bwt->f);

  big_endian = hton_64(mark_period);
  wrote = fwrite(&big_endian, 8, 1, bwt->f);
  if( wrote != 1 ) return ERR_IO_FILE(bwt->f);

  byte = alphabet_size_bits;
  fwrite(&byte, 1, 1, bwt->f);
  if( wrote != 1 ) return ERR_IO_FILE(bwt->f);

  byte = offset_size_bits;
  fwrite(&byte, 1, 1, bwt->f);
  if( wrote != 1 ) return ERR_IO_FILE(bwt->f);

  bsInitWrite(&bwt->buf);

  // give us some room in the buffer.
  {
    error_t err;
    err = buffer_extend( & bwt->buf, BWT_BUF_BYTES );
    if( err ) return err;
  }
  bwt->last_char = INVALID_ALPHA;
  bwt->run_length = 0;

  return ERR_NOERR;

}

error_t bwt_flush(bwt_writer_t* bwt)
{
  size_t wrote;

  wrote = fwrite(bwt->buf.data, bwt->buf.len, 1, bwt->f);
  if( wrote != 1 ) return ERR_IO_FILE(bwt->f);

  bwt->buf.len = 0;

  return ERR_NOERR;
}

error_t bwt_maybe_flush(bwt_writer_t* bwt)
{
  if( bwt->buf.len > bwt->buf.max - MAX_ENTRY_BYTES ) {
    return bwt_flush(bwt);
  }

  return ERR_NOERR;
}

void bwt_encode_run(bwt_writer_t* bwt, alpha_t the_char, int64_t run_length)
{
  //printf("Encoding character %i run %" PRIi64 "\n", the_char, run_length);
  // write the encoded run
  // first, encode the character.
  bsW24(&bwt->buf, bwt->alphabet_size_bits, the_char );
  // write a "0" to indicate no mark infos.
  bsW24(&bwt->buf, 1, 0);
  // next, encode the # repeats.
  buffer_encode_gamma(& bwt->buf, run_length );
}

void bwt_encode_mark(bwt_writer_t* bwt, alpha_t the_char, int64_t mark_logical_offset)
{

  //printf("Encoding character %i with mark %" PRIi64 "\n", the_char, mark_logical_offset);
  // first, encode the character.
  bsW24(&bwt->buf, bwt->alphabet_size_bits, the_char );
  // write a "1" to indicate mark infos.
  bsW24(&bwt->buf, 1, 1);
  // write the mark offset
  bsW64(&bwt->buf, bwt->offset_size_bits, mark_logical_offset);
}

/* 
   bwt has been initialized with bwt_init.
   the_char is the character to add. At the end, call once with INVALID_ALPHA
   mark_logical_offset is -1 if it's not a marked character, or a logical offset if it is.
 */
error_t bwt_append(bwt_writer_t* bwt, alpha_t the_char, int64_t mark_logical_offset)
{

  if( bwt->last_char == the_char && mark_logical_offset == -1 ) {
    bwt->run_length++;
  } else {
    if( bwt->last_char != INVALID_ALPHA ) {
      // encode the entry for the last run!
      bwt_encode_run(bwt, bwt->last_char, bwt->run_length);
    }

    if( mark_logical_offset != -1 ) {
      // record the mark entry for the current character.

      bwt_encode_mark(bwt, the_char, mark_logical_offset );

      bwt->last_char = INVALID_ALPHA;
      bwt->run_length = 0;

    } else {
      // different char.
      bwt->last_char = the_char;
      bwt->run_length = 1;
    }

  }

  return bwt_maybe_flush(bwt);
}

error_t bwt_finish_write(bwt_writer_t* bwt)
{
  // get the run we're holding on to written.
  bwt_append(bwt, INVALID_ALPHA, -1);
  // Finally, encode an "INVALID_ALPHA" character to end the run.
  bwt_encode_run(bwt, INVALID_ALPHA, 1);
  // round off to bytes.
  bsFinishWrite(&bwt->buf);
  // write out to the file
  bwt_flush(bwt);
  if( fflush(bwt->f) ) {
    return ERR_IO_FILE(bwt->f);
  }

  bwt_destroy(bwt);

  return ERR_NOERR;
}

error_t bwt_document_map_start_write(bwt_document_map_writer_t* w,
 FILE* aux, int64_t chunk_size, int64_t num_chars, int64_t num_docs)
{
  int64_t big_endian;
  uint32_t temp;
  int wrote;

  memset(w, 0, sizeof(bwt_document_map_writer_t));

  w->f = aux;
  w->chunk_size = chunk_size;
  w->chunk_size_num_bits = num_bits64(chunk_size);
  
  w->chunk = malloc(chunk_size*sizeof(int64_t));
  if( ! w->chunk ) return ERR_MEM;

  w->buf = build_buffer(16*w->chunk_size, malloc(16*w->chunk_size));
  if( ! w->buf.data ) return ERR_MEM;

  w->in_chunk = 0;

  temp = hton_32(DOC_MAP_START);
  wrote = fwrite(&temp, 4, 1, w->f);
  if( wrote != 1 ) return ERR_IO_FILE(w->f);

  temp = hton_32(DOC_MAP_VERSION);
  wrote = fwrite(&temp, 4, 1, w->f);
  if( wrote != 1 ) return ERR_IO_FILE(w->f);

  big_endian = hton_64(num_chars);
  wrote = fwrite(&big_endian, 8, 1, w->f);
  if( wrote != 1 ) return ERR_IO_FILE(w->f);

  big_endian = hton_64(num_docs);
  wrote = fwrite(&big_endian, 8, 1, w->f);
  if( wrote != 1 ) return ERR_IO_FILE(w->f);

  big_endian = hton_64(chunk_size);
  wrote = fwrite(&big_endian, 8, 1, w->f);
  if( wrote != 1 ) return ERR_IO_FILE(w->f);

  // write the number of chunks
  big_endian = hton_64(CEILDIV(num_chars, chunk_size));
  wrote = fwrite(&big_endian, 8, 1, w->f);
  if( wrote != 1 ) return ERR_IO_FILE(w->f);

  return ERR_NOERR;
}

error_t save_document_map(bwt_document_map_writer_t* w)
{
  int64_t i;
  int wrote;
  int64_t doc;
  int64_t last = 0;
  int64_t enc;
  int unique; 

  // sort the document numbers.
  unique = sort_dedup(w->chunk, w->in_chunk, sizeof(int64_t), compare_int64);

  // encode them!
  w->buf.len = 0;
  w->buf.pos = 0;
  bsInitWrite( &w->buf );
  // write the number of records in this chunk.
  bsW( &w->buf, w->chunk_size_num_bits, unique );
  for( i = 0; i < unique; i++ ) {
    doc = w->chunk[i];
    doc++; // document numbers count from 1.
    assert( doc != last );
    // encode anything that's different from the last.
    enc = doc-last;
    assert(enc>0);
    buffer_encode_gamma( &w->buf, enc );
    last = doc;
  }
  bsFinishWrite( &w->buf );
  // output the result.

  wrote = fwrite(w->buf.data, w->buf.len, 1, w->f);
  if( wrote != 1 ) return ERR_IO_FILE(w->f);

  w->in_chunk = 0;

  return ERR_NOERR;
}

error_t bwt_document_map_append(bwt_document_map_writer_t* w, int64_t doc)
{

  assert(w->in_chunk < w->chunk_size );

  w->chunk[w->in_chunk++] = doc;

  // add document numbers to the chunk.
  if( w->in_chunk == w->chunk_size ) {
    return save_document_map(w);
  }
  return ERR_NOERR;
}

error_t bwt_document_map_finish_write(bwt_document_map_writer_t* w)
{
  error_t err;

  err = save_document_map(w);
  bsInitWrite( &w->buf );
  bsW( &w->buf, w->chunk_size_num_bits, 0 );
  bsFinishWrite( &w->buf );

  free(w->chunk);
  free(w->buf.data);

  return err;
}


error_t bwt_document_info_start_write(bwt_document_info_writer_t* w, const char* path)
{
  memset(w, 0, sizeof(bwt_document_info_writer_t));

  size_t len = strlen(path);

  w->fname = strdup(path);

  w->doc_lens_fname = malloc(len + 20);
  w->info_lens_fname = malloc(len + 20);
  w->infos_fname = malloc(len + 20);

  if( ! w->doc_lens_fname ||
      ! w->info_lens_fname ||
      ! w->infos_fname ) {
    return ERR_MEM;
  }

  sprintf(w->doc_lens_fname, "%s.doc_lens", w->fname);
  sprintf(w->info_lens_fname, "%s.info_lens", w->fname);
  sprintf(w->infos_fname, "%s.infos", w->fname);

  w->doc_lens_f = fopen(w->doc_lens_fname, "w+");
  if( ! w->doc_lens_f ) return ERR_IO_STR_OBJ("Could not open", w->doc_lens_fname);
  w->info_lens_f = fopen(w->info_lens_fname, "w+");
  if( ! w->info_lens_f ) return ERR_IO_STR_OBJ("Could not open", w->info_lens_fname);
  w->infos_f = fopen(w->infos_fname, "w+");
  if( ! w->infos_f ) return ERR_IO_STR_OBJ("Could not open", w->infos_fname);

  w->num_chars = 0;
  w->num_docs = 0;
  w->sum_info_lens = 0;
  w->last_doc_num = -1;
  return ERR_NOERR;
}

error_t bwt_document_info_write(bwt_document_info_writer_t* w, int64_t doc_num, int64_t doc_size, int64_t info_len, unsigned char* info)
{
  int64_t endian;
  size_t wrote;

  if( doc_num != w->last_doc_num + 1 ) return ERR_PARAM;

  // Write the document length.
  endian = hton_64(doc_size);
  wrote = fwrite(&endian, 8, 1, w->doc_lens_f);
  if( wrote != 1 ) return ERR_IO_FILE(w->doc_lens_f);

  // Write the length of the info string.
  endian = hton_64(info_len);
  wrote = fwrite(&endian, 8, 1, w->info_lens_f);
  if( wrote != 1 ) return ERR_IO_FILE(w->info_lens_f);

  // Write the info.
  if( info_len > 0 ) {
    wrote = fwrite(info, info_len, 1, w->infos_f);
    if( wrote != 1 ) return ERR_IO_FILE(w->infos_f);
  }

  w->last_doc_num = doc_num;
  w->sum_info_lens += info_len;
  w->num_chars += doc_size;
  w->num_docs++;

  return ERR_NOERR;
}

error_t bwt_document_info_finish_write(bwt_document_info_writer_t* w)
{
  error_t err;
  FILE* output;
  buffer_t buf = build_buffer(0, NULL);
  size_t rc;
  int uerr;
  int64_t endian;
  int64_t cur_end;
  int64_t info_len;
  int64_t info_table_start;
  int64_t info_data_start;

  // Now combine the three files we created.
  rewind(w->doc_lens_f);
  rewind(w->info_lens_f);
  rewind(w->infos_f);

  output = fopen(w->fname, "w+");
  if( ! output ) return ERR_IO_STR_OBJ("Could not open", w->fname);

  err = mmap_buffer(&buf, output, 1, 8+8+8+8*w->num_docs+8*(w->num_docs+1)+w->sum_info_lens+1);
  if( err ) return err;

  buf.len = 0;

  bsInitWrite( &buf );
  bsPutUInt32( &buf, DOC_INFO_START);
  bsPutUInt32( &buf, DOC_INFO_VERSION);
  bsPutUInt64( &buf, w->num_chars );
  bsPutUInt64( &buf, w->num_docs );

  // write the table of doc ends.
  cur_end = 0;
  for( int64_t i = 0; i < w->num_docs; i++ ) {
    rc = fread(&endian, 8, 1, w->doc_lens_f);
    if( rc != 1 ) return ERR_IO_FILE(w->doc_lens_f);
    cur_end += ntoh_64(endian);
    bsPutUInt64( &buf, cur_end );
  }

  bsFinishWrite( &buf );

  info_table_start = buf.len;
  info_data_start = buf.len + 8*(w->num_docs+1);

  bsInitWrite( &buf );

  // write the doc infos.
  for( int64_t i = 0; i < w->num_docs; i++ ) {
    rc = fread(&endian, 8, 1, w->info_lens_f);
    if( rc != 1 ) return ERR_IO_FILE(w->info_lens_f);
    info_len = ntoh_64(endian);

    // Write the offset of the info data.
    bsPutUInt64( &buf, info_data_start );
    
    // Check we're not overflowing.
    assert( info_data_start + info_len <= buf.max );

    // Write the info data.
    if( info_len > 0 ) {
      rc = fread(&buf.data[info_data_start], info_len, 1, w->infos_f);
      if( rc != 1 ) return ERR_IO_FILE(w->infos_f);
    }

    // Add to info_data_start.
    info_data_start += info_len;
  }

  // Set the last offset (end of last infos)
  bsPutUInt64( &buf, info_data_start );
  bsFinishWrite( &buf );

  // Set an extra NULL character.
  buf.data[info_data_start++] = '\0';
  buf.len = info_data_start;

  err = munmap_buffer_truncate(&buf, output);
  if( err ) return err;

  fclose(output);

  fclose(w->doc_lens_f);
  w->doc_lens_f = NULL;
  fclose(w->info_lens_f);
  w->info_lens_f = NULL;
  fclose(w->infos_f);
  w->infos_f = NULL;


  uerr = unlink(w->doc_lens_fname);
  if( uerr ) return ERR_IO_STR_OBJ("Could not unlink", w->doc_lens_fname);
  uerr = unlink(w->info_lens_fname);
  if( uerr ) return ERR_IO_STR_OBJ("Could not unlink", w->info_lens_fname);
  uerr = unlink(w->infos_fname);
  if( uerr ) return ERR_IO_STR_OBJ("Could not unlink", w->infos_fname);

  return ERR_NOERR;
} 
error_t bwt_document_info_get_totals(bwt_document_info_writer_t* w,
                                     int64_t* total_chars,
                                     int64_t* total_docs)
{

  *total_chars = w->num_chars;
  *total_docs = w->num_docs;

  return ERR_NOERR;
}

