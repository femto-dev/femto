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

  femto/src/main/bwt_reader.c
*/
#include <stdio.h>
#include <sys/mman.h>
#include <errno.h>

#include "error.h"
#include "index_types.h"
#include "buffer.h"
#include "buffer_funcs.h"
#include "bwt_reader.h"

error_t multifile_reader_open_next(multifile_reader_t* m)
{
  assert( m->is_multifile );
  if( m->fp ) {
    fclose(m->fp);
    m->fp = NULL;
  }

  m->cur_file_num = m->next_file_num;
  sprintf(m->name, "%s_%i", m->prefix, m->cur_file_num);

  m->fp = fopen(m->name, "r");
  if( m->fp == NULL && errno == ENOENT ) {
    m->at_end = 1;
    return ERR_NOERR;
  }

  m->at_end = 0;

  if( ! m->fp ) return ERR_IO_STR_OBJ_NUM("Could not open", m->name, errno);

  m->next_file_num++;

  return ERR_NOERR;
}

error_t multifile_reader_open(multifile_reader_t* m, const char* path_prefix)
{
  memset(m, 0, sizeof(multifile_reader_t));
  m->is_multifile = 1;
  m->prefix = strdup(path_prefix);
  if( ! m->prefix ) return ERR_MEM;
  m->name = calloc(strlen(path_prefix) + 100, 1);
  if( ! m->name ) return ERR_MEM;
  m->cur_file_num = -1;
  m->next_file_num = 0;
  m->at_end = 0;

  return multifile_reader_open_next(m);
}

error_t multifile_reader_close(multifile_reader_t* m)
{
  int rc;
  error_t err = ERR_NOERR;

  if( ! m->is_multifile ) return ERR_NOERR;

  if( m->fp ) {
    rc = fclose(m->fp);
    if( rc ) err = ERR_IO_STR_OBJ_NUM("Could not close", m->name, errno); 
    m->fp = NULL;
  }

  free( m->prefix );
  free( m->name );

  return err;
}

error_t bwt_reader_core_open(bwt_reader_core_t* bwt, FILE* f)
{
  error_t err;

  memset(bwt, 0, sizeof(bwt_reader_core_t));

  // mmap the file into our buffer.
  err = mmap_buffer(&bwt->buf, f, 0, 0);
  if( err ) return err;

  // read the first two numbers to save them.
  bwt->buf.pos = 0;
  if( BWT_START != bsDerefUInt32( &bwt->buf ) ) return ERR_FORMAT;
  if( BWT_VERSION != bsDerefUInt32( &bwt->buf ) ) return ERR_FORMAT;

  bwt->num_chars = bsDerefUInt64( & bwt->buf );
  bwt->num_docs = bsDerefUInt64( & bwt->buf);

  // now get the alphabet and offset sizes
  bwt->alphabet_size = bsDerefUInt64( & bwt->buf );
  bwt->mark_period = bsDerefUInt64( & bwt->buf );
  
  bwt->alphabet_size_bits = bsDerefUInt8( & bwt->buf );
  bwt->offset_size_bits = bsDerefUInt8( & bwt->buf );
  
  assert( bwt->buf.pos == bwt_rle_offset_core(bwt) );

  if( bwt->alphabet_size_bits == 0 || bwt->offset_size_bits == 0 ) return ERR_FORMAT;

  return bwt_reader_core_rewind(bwt);
}

error_t bwt_reader_open(bwt_reader_t* bwt, FILE* f)
{
  memset(bwt, 0, sizeof(bwt_reader_t));
  bwt->m.is_multifile = 0;

  return bwt_reader_core_open(&bwt->r, f);
}

error_t bwt_reader_open_multifile(bwt_reader_t* bwt, const char* path_prefix)
{
  error_t err;

  memset(bwt, 0, sizeof(bwt_reader_t));

  err = multifile_reader_open(&bwt->m, path_prefix);
  if( err ) return err;

  err = bwt_reader_core_open(&bwt->r, bwt->m.fp);
  if( err ) return err;

  return ERR_NOERR;
}

error_t bwt_reader_core_close(bwt_reader_core_t* bwt)
{
  error_t err;

  err = munmap_buffer(&bwt->buf);
  if( err ) return err;

  bwt->at_end = 1;

  return ERR_NOERR;
}

error_t bwt_reader_close(bwt_reader_t* bwt)
{
  error_t err;
  err = bwt_reader_core_close(&bwt->r);
  if( err ) return err;
  err = multifile_reader_close(&bwt->m);
  if( err ) return err;

  return ERR_NOERR;
}

error_t bwt_reader_open_next(bwt_reader_t* bwt)
{
  error_t err;

  assert(bwt->m.is_multifile);
  assert(! bwt->m.at_end);

  // Close was was open
  err = bwt_reader_core_close(&bwt->r);
  if( err ) return err;

  err = multifile_reader_open_next(&bwt->m);
  if( err ) return err;

  // Are we done?
  if( bwt->m.at_end ) return ERR_NOERR;

  // Not at end... so open up.
  err = bwt_reader_core_open(&bwt->r, bwt->m.fp);
  if( err ) return err;

  return ERR_NOERR;
}

error_t bwt_reader_core_rewind(bwt_reader_core_t* bwt)
{
  bwt->buf.pos = bwt_rle_offset_core(bwt);
  bsInitRead( &bwt->buf );
  bwt->read_one_idx = 0;
  bwt->read_one_ent.run = 0;
  bwt->read_one_ent.ch = INVALID_ALPHA;
  bwt->read_one_ent.offset = -1;
  return ERR_NOERR;
}

error_t bwt_reader_rewind(bwt_reader_t* bwt)
{
  if( bwt->m.is_multifile ) {
    //error_t err;
    bwt->m.next_file_num = 0;
    bwt->m.at_end = 0;
    return bwt_reader_open_next(bwt);
    //err = multifile_reader_open_next(&bwt->m);p
    //if( err ) return err;
    //return bwt_reader_core_open(&bwt->r, bwt->m.fp);
  } else {
    return bwt_reader_core_rewind(&bwt->r);
  }
}


// returns INVALID_ALPHA at the end of the file,
// also sets at_end.
error_t bwt_reader_core_read(bwt_reader_core_t* bwt, bwt_entry_t* ent)
{
  unsigned char bit;

  // read an entry!
  ent->ch = bsR24(&bwt->buf, bwt->alphabet_size_bits);
  ent->offset = -1;
  ent->run = 1;
  bit = bsR24( &bwt->buf, 1 );
  if( bit ) {
    ent->offset = bsR64(&bwt->buf, bwt->offset_size_bits);
  } else {
    ent->run = buffer_decode_gamma(&bwt->buf);
  }
  if( ent->ch >= bwt->alphabet_size ) {
    ent->ch = INVALID_ALPHA;
  }

  if( ent->ch == INVALID_ALPHA ) {
    bwt->at_end = 1;
  } else {
    bwt->at_end = 0;
  }

  return ERR_NOERR;
}

// returns INVALID_ALPHA at the end of the file.
error_t bwt_reader_read(bwt_reader_t* bwt, bwt_entry_t* ent)
{
  error_t err;

  while( 1 ) {
    err = bwt_reader_core_read(&bwt->r, ent);
    if( err ) return err;

    if( ! bwt->r.at_end ) break; // OK - got a result.

    // Otherwise, try the next file if we're multifile.
    if( bwt->m.is_multifile ) {
      err = bwt_reader_open_next(bwt);
      if( err ) return err;

      if( bwt->m.at_end ) {
        ent->ch = INVALID_ALPHA;
        ent->offset = -1;
        ent->run = 1;
        return ERR_NOERR; // end of files!
      }
    } else break;
  }

  return ERR_NOERR;
}

error_t bwt_reader_read_one(bwt_reader_t* bwt, int64_t* ch, int64_t* off)
{
  error_t err;

  if( bwt->r.read_one_idx >= bwt->r.read_one_ent.run ) {
    // we're at the end of our run; read a new one.
    err = bwt_reader_read(bwt, &bwt->r.read_one_ent);
    if( err ) return err;
    bwt->r.read_one_idx = 0;
  }

  // return our character.
  *ch = bwt->r.read_one_ent.ch;
  *off = bwt->r.read_one_ent.offset;

  bwt->r.read_one_idx++;

  return ERR_NOERR;
}


error_t bwt_document_map_reader_core_rewind(bwt_document_map_reader_core_t* r)
{
  r->buf.pos = DOC_MAP_HDR_LEN;
  bsInitRead( &r->buf );
  r->chunk_docs = 0;
  r->last = 0;
  return ERR_NOERR;
}

error_t bwt_document_map_reader_core_open(bwt_document_map_reader_core_t* r, FILE* f)
{
  error_t err;

  memset(r, 0, sizeof(bwt_document_map_reader_core_t));
  
  // mmap the file into our buffer.
  err = mmap_buffer(&r->buf, f, 0, 0);
  if( err ) return err;

  // read the first two numbers to save them.
  r->buf.pos = 0;
  if( DOC_MAP_START != bsDerefUInt32( &r->buf ) ) return ERR_FORMAT;
  if( DOC_MAP_VERSION != bsDerefUInt32( &r->buf ) ) return ERR_FORMAT;
  r->num_chars = bsDerefUInt64( & r->buf );
  r->num_docs = bsDerefUInt64( & r->buf );
  r->chunk_size = bsDerefUInt64( & r->buf );
  r->num_chunks = bsDerefUInt64( & r->buf );
  r->chunk_size_num_bits = num_bits64(r->chunk_size);

  if( r->chunk_size == 0 ) return ERR_FORMAT;

  return bwt_document_map_reader_core_rewind(r);
}

error_t bwt_document_map_reader_open(bwt_document_map_reader_t* r, FILE* f)
{
  memset(r, 0, sizeof(bwt_document_map_reader_t));
  r->m.is_multifile = 0;

  return bwt_document_map_reader_core_open(&r->r, f);
}

error_t bwt_document_map_reader_open_multifile(bwt_document_map_reader_t* r, const char* path_prefix)
{
  error_t err;

  memset(r, 0, sizeof(bwt_document_map_reader_t));

  err = multifile_reader_open(&r->m, path_prefix);
  if( err ) return err;

  err = bwt_document_map_reader_core_open(&r->r, r->m.fp);
  if( err ) return err;

  return ERR_NOERR;
}


error_t bwt_document_map_reader_core_close(bwt_document_map_reader_core_t* r)
{
  r->chunk_docs = 0;
  free(r->docs);
  r->docs = NULL;
  return munmap_buffer(&r->buf);
}
error_t bwt_document_map_reader_close(bwt_document_map_reader_t* r)
{
  error_t err;
  err = bwt_document_map_reader_core_close(&r->r);
  if( err ) return err;
  err = multifile_reader_close(&r->m);
  if( err ) return err;

  return ERR_NOERR;

}

error_t bwt_document_map_reader_open_next(bwt_document_map_reader_t* r)
{
  error_t err;

  assert( r->m.is_multifile);
  assert( ! r->m.at_end );

  err = bwt_document_map_reader_core_close(&r->r);
  if( err ) return err;
  err = multifile_reader_open_next(&r->m);
  if( err ) return err;

  // Are we done?
  if( r->m.at_end ) return ERR_NOERR;

  err = bwt_document_map_reader_core_open(&r->r, r->m.fp);
  if( err ) return err;

  return ERR_NOERR;
}

error_t bwt_document_map_reader_core_read_chunk(bwt_document_map_reader_core_t* r)
{
  int64_t read;

  // read a chunk

  if( r->buf.pos > r->buf.len ) return ERR_INVALID;

  bsInitRead( & r->buf );
  r->chunk_docs = bsR(&r->buf, r->chunk_size_num_bits);
  if( r->chunk_docs == 0 ) {
    // At end.
    r->at_end = 1;
    return ERR_NOERR;
  }

  r->docs = realloc(r->docs, r->chunk_docs*sizeof(int64_t));
  if( ! r->docs ) return ERR_MEM;

  r->last = 0;

  for( long i = 0; i < r->chunk_docs; i++ ) {
    read = buffer_decode_gamma(&r->buf); // un-gamma encode
    r->last = read + r->last; // un-delta encode
    r->docs[i] = r->last - 1; // output document numbers count from 0
  }

  bsFinishRead( & r->buf );

  return ERR_NOERR;
}

error_t bwt_document_map_reader_read_chunk(bwt_document_map_reader_t* r)
{
  error_t err;

  while( 1 ) {
    err = bwt_document_map_reader_core_read_chunk(&r->r);
    if( err ) return err;

    if( ! r->r.at_end ) break; // OK - got a result.

    // Otherwise, try the next file if we're multifile.
    if( r->m.is_multifile ) {
      err = bwt_document_map_reader_open_next(r);
      if( err ) return err;

      if( r->m.at_end ) {
        r->r.chunk_docs = 0; // marking end record..
        return ERR_NOERR;
      }
    } else break;
  }

  return ERR_NOERR;
}

error_t bwt_document_info_reader_open(bwt_document_info_reader_t* r, FILE* f)
{
  error_t err;
  err = mmap_buffer(&r->buf, f, 0, 0);
  if( err ) return err;
  r->buf.pos = 0;
  if( DOC_INFO_START != bsDerefUInt32( &r->buf ) ) return ERR_FORMAT;
  if( DOC_INFO_VERSION != bsDerefUInt32( &r->buf ) ) return ERR_FORMAT;
  r->num_chars = bsDerefUInt64( &r->buf );
  r->num_docs = bsDerefUInt64( &r->buf );
  return ERR_NOERR;
}

error_t bwt_document_info_reader_close(bwt_document_info_reader_t* r )
{
  return munmap_buffer(&r->buf);
}

error_t bwt_document_info_reader_doc_end(bwt_document_info_reader_t* r, int64_t doc, int64_t* doc_end)
{
  // go to the proper location.
  r->buf.pos = bwt_document_info_reader_doc_ends_offset(r) + 8*doc;

  *doc_end = bsDerefUInt64( &r->buf );
  return ERR_NOERR;
}

error_t bwt_document_info_reader_doc_start(bwt_document_info_reader_t* r, int64_t doc, int64_t* doc_start)
{
  if( doc == 0 ) {
    *doc_start = 0;
    return ERR_NOERR;
  } else {
    return bwt_document_info_reader_doc_end(r, doc-1, doc_start);
  }
}

error_t bwt_document_info_reader_doc_size(bwt_document_info_reader_t* r, int64_t doc, int64_t* doc_size)
{
  int64_t doc_end, prev_end;

  if( doc == 0 ) {
    prev_end = 0;
  } else {
    r->buf.pos = bwt_document_info_reader_doc_ends_offset(r) + 8*(doc-1);
    prev_end = bsDerefUInt64( &r->buf );
  }

  r->buf.pos = bwt_document_info_reader_doc_ends_offset(r) + 8*doc;
  doc_end = bsDerefUInt64( &r->buf );

  *doc_size = doc_end - prev_end;

  return ERR_NOERR;
}

error_t bwt_document_info_reader_find_doc(bwt_document_info_reader_t* r, int64_t offset, int64_t* doc)
{
  unsigned char* data = r->buf.data;
  // go to the proper location.
  intptr_t pos = bwt_document_info_reader_doc_ends_offset(r);
  int64_t * arr = (int64_t*) &data[pos];
  int64_t prev = bsearch_int64_ntoh_arr(r->num_docs, arr, offset);
  *doc = prev + 1;

  return ERR_NOERR;
}

error_t bwt_document_info_reader_get(bwt_document_info_reader_t* r, int64_t doc, int64_t* info_len, unsigned char** info)
{
  int64_t start, end;
  // go to the proper location.
  r->buf.pos = bwt_document_info_reader_doc_info_offset(r) + 8*doc;
  // read an offset.
  start = bsDerefUInt64( &r->buf );
  end = bsDerefUInt64( &r->buf );

  // seek to the info.
  r->buf.pos = start;
  *info_len = end - start;
  *info = & r->buf.data[r->buf.pos];
  return ERR_NOERR;
}

