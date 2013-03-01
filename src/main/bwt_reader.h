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

  femto/src/main/bwt_reader.h
*/
#ifndef _BWT_READER_H_
#define _BWT_READER_H_ 1

#include "buffer.h"
#include "buffer_funcs.h"
#include "bwt.h"
#include "results.h"

typedef struct {
  int64_t ch;
  int64_t run;
  int64_t offset;
} bwt_entry_t;

typedef struct {
  int is_multifile; // if not multifile, below are 0.
  FILE* fp;
  char* prefix; // path prefix
  char* name; // current file name
  int cur_file_num;
  int next_file_num; // next file number
  int at_end; // are we done?
} multifile_reader_t;

// this could be rewritten to support streaming.
typedef struct {
  // We'll just mmap the file (for convienence)
  buffer_t buf;
  // Save some data to be handy.
  int64_t num_chars; // total
  int64_t num_docs;
  int64_t alphabet_size;
  int64_t mark_period;
  unsigned char alphabet_size_bits;
  unsigned char offset_size_bits;
  bwt_entry_t read_one_ent; // for bwt_reader_read_one.
  int64_t read_one_idx;
  int at_end; // are we done?
} bwt_reader_core_t;

typedef struct {
  multifile_reader_t m;
  bwt_reader_core_t r;
} bwt_reader_t;

error_t bwt_reader_open(bwt_reader_t* bwt, FILE* f);
error_t bwt_reader_open_multifile(bwt_reader_t* bwt, const char* path_prefix);
error_t bwt_reader_close(bwt_reader_t* bwt);
error_t bwt_reader_core_rewind(bwt_reader_core_t* bwt);
error_t bwt_reader_read(bwt_reader_t* bwt, bwt_entry_t* ent);
error_t bwt_reader_read_one(bwt_reader_t* bwt, int64_t* ch, int64_t* off);
error_t bwt_reader_rewind(bwt_reader_t* bwt);

static inline
int64_t bwt_num_chars(bwt_reader_t* bwt)
{
  return bwt->r.num_chars;
}

static inline
int64_t bwt_num_docs(bwt_reader_t* bwt)
{
  return bwt->r.num_docs;
}

static inline
int64_t bwt_alphabet_size(bwt_reader_t* bwt)
{
  return bwt->r.alphabet_size;
}

static inline
int64_t bwt_mark_period(bwt_reader_t* bwt)
{
  return bwt->r.mark_period;
}

static inline
unsigned char bwt_alphabet_size_bits(bwt_reader_t* bwt)
{
  return bwt->r.alphabet_size_bits;
}

static inline
unsigned char bwt_offset_size_bits(bwt_reader_t* bwt)
{
  return bwt->r.offset_size_bits;
}

static inline
intptr_t bwt_rle_offset_core(bwt_reader_core_t* bwt)
{
  return BWT_RLE_OFFSET;
}

static inline
int bwt_at_end(bwt_reader_t* r)
{
  if( r->m.is_multifile ) return r->m.at_end;
  else return r->r.at_end;
}

typedef struct {
  int multifile; // if not multifile, below are 0.
   FILE* fp;
   char* prefix; // path prefix
   char* name; // current file name
   int file_num; // current file number

  // We'll just mmap the file (for convienence)
  buffer_t buf;
  // convienently stored values
  int64_t num_chars; // total
  int64_t num_docs;
  int64_t chunk_size;
  int64_t num_chunks;
  int chunk_size_num_bits;
  // saved document numbers
  int64_t last;
  int64_t chunk_docs; // the # of unique doc #s in this chunk
  int64_t* docs; // the doc #s
  int at_end; // are we done?
} bwt_document_map_reader_core_t;

typedef struct {
  multifile_reader_t m;
  bwt_document_map_reader_core_t r;
} bwt_document_map_reader_t;

static inline
int64_t bwt_document_map_reader_chunk_size(bwt_document_map_reader_t* r) 
{
  return r->r.chunk_size;
}
static inline
int64_t bwt_document_map_reader_num_chunks(bwt_document_map_reader_t* r) 
{
  return r->r.num_chunks;
}

static inline
error_t bwt_document_map_reader_renumber(bwt_document_map_reader_t* r, int64_t* old_doc_to_new_doc)
{
  // Renumber them if necessary.
  for( int64_t i = 0; i < r->r.chunk_docs; i++ ) {
    int64_t old_doc = r->r.docs[i];
    int64_t new_doc = old_doc_to_new_doc[old_doc];
    r->r.docs[i] = new_doc;
  }
  return ERR_NOERR;
}

// Calls results_create_sort on the read document numbers.
static inline
error_t bwt_document_map_reader_results(bwt_document_map_reader_t* r, results_t* results)
{
  return results_create_sort(results, r->r.chunk_docs, r->r.docs);
}

static inline
int bwt_document_map_reader_at_end(bwt_document_map_reader_t* r)
{
  if( r->m.is_multifile ) return r->m.at_end;
  else return r->r.at_end;
}


error_t bwt_document_map_reader_open(bwt_document_map_reader_t* r, FILE* f);
error_t bwt_document_map_reader_open_multifile(bwt_document_map_reader_t* r, const char* path_prefix);
error_t bwt_document_map_reader_close(bwt_document_map_reader_t* r);
error_t bwt_document_map_reader_core_rewind(bwt_document_map_reader_core_t* r);
error_t bwt_document_map_reader_read_chunk(bwt_document_map_reader_t* r);

typedef struct {
  buffer_t buf;
  int64_t num_chars;
  int64_t num_docs;
} bwt_document_info_reader_t;

static inline
intptr_t bwt_document_info_reader_doc_ends_offset(bwt_document_info_reader_t* r)
{
  return DOC_INFO_HDR_LEN;
}

static inline
intptr_t bwt_document_info_reader_doc_info_offset(bwt_document_info_reader_t* r)
{
  return DOC_INFO_HDR_LEN + 8*r->num_docs;
}

static inline
int64_t bwt_document_info_reader_num_chars(bwt_document_info_reader_t* bwt)
{
  return bwt->num_chars;
}

static inline
int64_t bwt_document_info_reader_num_docs(bwt_document_info_reader_t* bwt)
{
  return bwt->num_docs;
}

error_t bwt_document_info_reader_open(bwt_document_info_reader_t* r, FILE* f);
error_t bwt_document_info_reader_close(bwt_document_info_reader_t* r);
error_t bwt_document_info_reader_doc_end(bwt_document_info_reader_t* r, int64_t doc, int64_t* doc_end);
error_t bwt_document_info_reader_doc_start(bwt_document_info_reader_t* r, int64_t doc, int64_t* doc_start);
error_t bwt_document_info_reader_find_doc(bwt_document_info_reader_t* r, int64_t offset, int64_t* doc);
error_t bwt_document_info_reader_doc_size(bwt_document_info_reader_t* r, int64_t doc, int64_t* doc_size);
error_t bwt_document_info_reader_get(bwt_document_info_reader_t* r, int64_t doc, int64_t* info_len, unsigned char** info);

#endif
