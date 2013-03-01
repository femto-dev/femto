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

  femto/src/main/bwt_prepare.c
*/

#include "bwt.h"
#include "index.h"
#include "file_find.h"

#include "buffer_funcs.h"

#include "bwt_prepare.h"
#include "bwt_writer.h"
#include "timing.h"

error_t bwt_begin_write_ssort(bwt_writer_t* bwt, FILE* bwt_f, bwt_document_info_writer_t* info, char* info_path, int64_t number_of_characters_ssort, int64_t number_of_documents, int64_t* doc_ends_ssort, int64_t alphabet_size, int64_t mark_period, unsigned char alphabet_size_bits, unsigned char offset_size_bits, int64_t* doc_info_lens, unsigned char** doc_infos)
{
  error_t err;
  int64_t total_num_chars;
  int64_t total_num_docs;

  err = bwt_document_info_start_write(info, info_path);
  if( err ) return err;

  // Write the document infos.
  for( int64_t doc = 0; doc < number_of_documents; doc++ ) {
    char info_buf[65];
    unsigned char* the_info = NULL;
    int64_t info_len = 0;
    int64_t doc_start, doc_end, doc_size;
   
    if( doc == 0 ) doc_start = 0;
    else doc_start = doc_ends_ssort[doc-1];
    doc_end = doc_ends_ssort[doc];
    doc_size = doc_end - doc_start;

    if( ! doc_info_lens && ! doc_infos ) {
      info_len = snprintf(info_buf, 65, "%lli", (long long int) doc);
      the_info = (unsigned char*) info_buf;
      if( err ) return err;
    } else {
      info_len = doc_info_lens[doc];
      the_info = doc_infos[doc];
    }

    err = bwt_document_info_write(info, doc, doc_size, info_len, the_info);
    if( err ) return err;
  }

  err = bwt_document_info_finish_write(info);
  if( err ) return err;

  err = bwt_document_info_get_totals(info, &total_num_chars, &total_num_docs);
  if( err ) return err;

  assert( total_num_docs == number_of_documents );

  // Now open up a reader.
  err = bwt_begin_write(bwt, bwt_f, total_num_chars, total_num_docs, alphabet_size, mark_period, alphabet_size_bits, offset_size_bits);
  if( err ) return err;

  return ERR_NOERR;
}

error_t bwt_begin_write_easy(bwt_writer_t* bwt, FILE* bwt_f, prepared_text_t* p, int64_t mark_period)
{
  int64_t total_num_chars = -1;
  int64_t total_num_docs = -1;
  error_t err;

  err = bwt_document_info_get_totals(& p->info_writer, &total_num_chars, &total_num_docs);
  if( err ) return err;

  err = bwt_begin_write(bwt, bwt_f,
                        total_num_chars, total_num_docs,
                        ALPHA_SIZE, mark_period,
                        ALPHA_SIZE_BITS,
                        num_bits64(total_num_chars));
  if( err ) return err;

  return ERR_NOERR;
}

static inline
error_t put_spec_char(prepared_text_t* p, uchar spec)
{
  alpha_t ch = ESCAPE_OFFSET+spec;
  return prepare_put_char(p, ch);
}

static inline
error_t put_char(prepared_text_t* p, uchar c)
{
  alpha_t ch = CHARACTER_OFFSET+c;
  return prepare_put_char(p, ch);
}

error_t init_prepared_text( prepared_text_t* p, const char* info_path )
{
  memset(p, 0, sizeof(prepared_text_t));

  p->info_path = strdup(info_path);
  if( ! p->info_path ) return ERR_MEM;

  p->info_f = NULL;
  p->extra = NULL;
  p->stage = 0;
  p->cur_doc_count = 0;
  p->cur_doc_append = 0;

  return bwt_document_info_start_write(& p->info_writer, p->info_path);
}

static
error_t finish_count_if_needed( prepared_text_t* p )
{
  if( p->stage == 0 ) {
    error_t err;
    unsigned char* data;

    p->stage = -1; // in case of error.

    err = bwt_document_info_finish_write(& p->info_writer);
    if( err ) return err;

    assert( p->buf.data == NULL );
    data = (unsigned char*) malloc(BWT_BUF_BYTES);
    if( ! data ) return ERR_MEM;

    p->buf = build_buffer(BWT_BUF_BYTES, data);

    p->info_f = fopen(p->info_path, "r");
    if( ! p->info_f ) return ERR_IO_STR_OBJ("Could not open", p->info_path);

    err = bwt_document_info_reader_open(& p->info_reader, p->info_f);
    if( err ) {
      fclose(p->info_f);
      p->info_f = NULL;
      return err;
    }

    p->stage = 1; // no error

    return ERR_NOERR;
  } else {
    return ERR_NOERR;
  }
}

error_t free_prepared_text( prepared_text_t* p )
{
  error_t err;
  error_t err2;
  err = finish_count_if_needed( p );

  if( p->info_f ) {
    err2 = bwt_document_info_reader_close( & p->info_reader );
    if( err2 && !err ) err = err2;

    fclose(p->info_f);
    p->info_f = NULL;
  }

  free_prepared_text_extra(p);
  if( p->buf.data ) free(p->buf.data);
  p->buf.data = NULL;

  if( p->info_path ) free(p->info_path);

  return err;
}


// Appends the document info for a file.
error_t count_file(prepared_text_t* p,
                   int64_t doc_size,
                   int num_headers, int64_t* header_lens,
                   int64_t info_len, unsigned char* info)
{
  int hdr;
  int64_t plen;
  error_t err;

  if( p->stage != 0 ) return ERR_PARAM;
  if( num_headers > 0xfe ) return ERR_PARAM;
  if( num_headers < 0 ) return ERR_PARAM;

  plen = doc_size;
  // add lengths for all of the headers.
  for( hdr = 0; hdr < num_headers; hdr++ ) {
    plen++; // ESCAPE_CODE_SOH
    plen++; // header #
    plen += header_lens[hdr];
    plen++; // ESCAPE_CODE_EOH
    plen++; // header #
  }
  plen++; // SEOF
  //plen += 8; // document #
  //plen++; // EOF

  err = bwt_document_info_write(& p->info_writer, p->cur_doc_count, plen, info_len, info);
  if( err ) return err;

  p->cur_doc_count++;

  return ERR_NOERR;
}

// The other one just mmaps to get to here.
error_t append_file_mem(prepared_text_t* p,
                        int64_t doc_size, unsigned char* bytes,
                        int num_headers, int64_t* header_lens, unsigned char** headers,
                        int64_t info_len, unsigned char* info)
{
  int hdr;
  int64_t i;
  int64_t plen;
  int64_t saved_size = 0;
  error_t err;
  int64_t saved_info_len = 0;
  unsigned char* saved_info = NULL;

  err = finish_count_if_needed( p );
  if( err ) return err;

  if( p->stage != 1 ) return ERR_PARAM;
  // We counted too few!
  if( p->cur_doc_append >= p->cur_doc_count ) return ERR_PARAM;
  if( num_headers > 0xfe ) return ERR_PARAM;
  if( num_headers < 0 ) return ERR_PARAM;

  plen = doc_size;
  // add lengths for all of the headers.
  for( hdr = 0; hdr < num_headers; hdr++ ) {
    plen++; // ESCAPE_CODE_SOH
    plen++; // header #
    plen += header_lens[hdr];
    plen++; // ESCAPE_CODE_EOH
    plen++; // header #
  }
  plen++; // SEOF
  //plen += 8; // document #
  //plen++; // EOF

  // Check that this is the same size and info as what is recorded in the 
  // info file.
  err = bwt_document_info_reader_get( &p->info_reader, p->cur_doc_append,
                                      &saved_info_len, &saved_info);
  if( err ) return err;
  if( saved_info_len != info_len ||
      0 != memcmp(saved_info, info, saved_info_len) ) {
    return ERR_INVALID_STR("Saved info does not match");
  }

  err = bwt_document_info_reader_doc_size( &p->info_reader, p->cur_doc_append,
                                           &saved_size );
  if( err ) return err;
  if( saved_size != plen ) return ERR_INVALID_STR("Document size does not match");

  // first, add the characters.
  for( i = 0; i < doc_size; i++ ) {
    err = put_char(p, bytes[i]);
    if( err ) return err;
  }

  // now add the trailer.

  // add any header sections that were requested.
  for( hdr = 0; hdr < num_headers; hdr++ ) {
    err = put_spec_char(p, ESCAPE_CODE_SOH);
    if( err ) return err;
    err = put_char(p, hdr);
    if( err ) return err;
    // put the header data.
    for( i = 0; i < header_lens[hdr]; i++ ) {
      err = put_char(p, headers[hdr][i]);
      if( err ) return err;
    }
    err = put_spec_char(p, ESCAPE_CODE_EOH);
    if( err ) return err;
    err = put_char(p, hdr);
    if( err ) return err;
  }

  // now put the SEOF
  err = put_spec_char(p, ESCAPE_CODE_SEOF);
  if( err ) return err;

  err = prepare_end_document(p);
  if( err ) return err;

  p->cur_doc_append++;

  return ERR_NOERR;
}

error_t append_file(prepared_text_t* p,
                    FILE* f,
                    int num_headers, int64_t* header_lens, unsigned char** headers,
                    int64_t info_len, unsigned char* info)
{
  error_t err;
  error_t err2;
  buffer_t buf = build_buffer(0, NULL);
  
  err = mmap_buffer(&buf, f, 0, 0);
  if( err ) return err;

  err = append_file_mem(p, buf.len, buf.data,
                        num_headers, header_lens, headers,
                        info_len, info);


  err2 = munmap_buffer(&buf);
  if( err2 && ! err ) err = err2;

  return err;
}

error_t prepared_finish_count( prepared_text_t* p )
{
  return finish_count_if_needed( p );
}

error_t prepared_num_docs( prepared_text_t* p, int64_t *num_docs)
{
  error_t err;
  err = finish_count_if_needed( p );
  if( err ) return err;

  *num_docs = bwt_document_info_reader_num_docs( &p->info_reader );

  return ERR_NOERR;
}

error_t prepared_num_chars( prepared_text_t* p, int64_t *num_chars)
{
  error_t err;
  err = finish_count_if_needed( p );
  if( err ) return err;
  
  *num_chars = bwt_document_info_reader_num_chars( &p->info_reader );

  return ERR_NOERR;
}

error_t prepared_find_doc( prepared_text_t* p, int64_t offset, int64_t *doc)
{
  error_t err;
  err = finish_count_if_needed( p );
  if( err ) return err;

  err = bwt_document_info_reader_find_doc( &p->info_reader, offset, doc);
  if( err ) return err;

  return ERR_NOERR;
}

error_t prepared_doc_end( prepared_text_t* p, int64_t doc, int64_t *doc_end)
{
  error_t err;

  err = finish_count_if_needed( p );
  if( err ) return err;

  err = bwt_document_info_reader_doc_end( &p->info_reader, doc, doc_end);
  if( err ) return err;

  return ERR_NOERR;
}

error_t prepared_doc_start( prepared_text_t* p, int64_t doc, int64_t *doc_start)
{
  error_t err;

  err = finish_count_if_needed( p );
  if( err ) return err;

  if( doc == 0 ) {
    *doc_start = 0;
  } else {
    // it's the end of the previous doc.
    err = bwt_document_info_reader_doc_end( &p->info_reader, doc-1, doc_start);
    if( err ) return err;
  }

  return ERR_NOERR;
}


error_t bwt_should_mark_doc( prepared_text_t* p, int64_t mark_period, int64_t offset, int64_t doc, unsigned char* yesno)
{
  error_t err;
  int64_t doc_start = -1;
  int64_t doc_end = -1;

  err = finish_count_if_needed( p );
  if( err ) return err;

  err = prepared_doc_start( p, doc, &doc_start );
  if( err ) return err;
  err = prepared_doc_end( p, doc, &doc_end );
  if( err ) return err;

  *yesno = should_mark(mark_period, offset - doc_start, doc_end - doc_start);

  return ERR_NOERR;
}

error_t bwt_should_mark( prepared_text_t* p, int64_t mark_period, int64_t offset, unsigned char* yesno )
{
  error_t err;
  int64_t doc = -1;

  err = finish_count_if_needed( p );
  if( err ) return err;

  err = prepared_find_doc( p, offset, &doc);
  if( err ) return err;

  return bwt_should_mark_doc( p, mark_period, offset, doc, yesno);

}

// Finds and returns the integer index,i
// such that arr[i-1] <= target < arr[i]
// Assumes that target < arr[n-1].
int64_t bsearch_int64_doc(int64_t n, int64_t* arr, int64_t target)
{
  int64_t a, b, middle;

  if( target < arr[0] ) return 0;

  a = 0;
  b = n - 1;
  // always we have that arr[a] <= target < arr[b].

  // divide the search space in half
  while(b - a > 1) {
    middle = (a + b) / 2;
    if( target < arr[middle] ) b = middle;
    else a = middle; // arr[middle] <= target
  }

  return b;
}

