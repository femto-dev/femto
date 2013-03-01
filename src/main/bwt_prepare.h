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

  femto/src/main/bwt_prepare.h
*/
#ifndef _BWT_PREPARE_H_
#define _BWT_PREPARE_H_ 1

#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#include "index_types.h"
#include "error.h"
#include "buffer.h"

#include "bwt_reader.h"
#include "bwt_writer.h"

typedef struct {
  buffer_t buf; // might be null for external memory version.

  char* info_path;
  bwt_document_info_writer_t info_writer;
  FILE* info_f;
  bwt_document_info_reader_t info_reader;

  void* extra; // for the user...

  int stage; // 0 == counting/writing info
             // 1 == info counted/written.

  int64_t cur_doc_count;
  int64_t cur_doc_append;
} prepared_text_t;

// This one is just used for testing.
error_t bwt_begin_write_ssort(bwt_writer_t* bwt, FILE* bwt_f, bwt_document_info_writer_t* info, char* info_path, int64_t number_of_characters_ssort, int64_t number_of_documents, int64_t* doc_ends_ssort, int64_t alphabet_size, int64_t mark_period, unsigned char alphabet_size_bits, unsigned char offset_size_bits, int64_t* doc_info_lens, unsigned char** doc_infos);

// This one is used in testing and elsewhere.
error_t bwt_begin_write_easy(bwt_writer_t* bwt, FILE* f, prepared_text_t* p, int64_t mark_period);

/* PREPARED TEXT INTERFACE:
   call init_prepared_text()
   then some number of count_file()
   then some number of append_file() and/or bwt_should_mark()
   then free_prepared_text
   */
error_t count_file(prepared_text_t* p,
                  int64_t doc_size,
                  int num_headers, int64_t* header_lens,
                  int64_t info_len, unsigned char* info);

error_t append_file_mem(prepared_text_t* p,
                        int64_t doc_size, unsigned char* bytes,
                        int num_headers, int64_t* header_lens, unsigned char** headers,
                        int64_t info_len, unsigned char* info);

error_t append_file(prepared_text_t* p,
                    FILE* f,
                    int num_headers, int64_t* header_lens, unsigned char** headers,
                    int64_t info_len, unsigned char* info);


error_t init_prepared_text( prepared_text_t* p, const char* info_path );
error_t free_prepared_text( prepared_text_t* p );

//int64_t bsearch_int64_doc(int64_t n, int64_t* arr, int64_t target);

// Calling this is optional for other routines here...
error_t prepared_finish_count( prepared_text_t* p );

error_t prepared_num_docs( prepared_text_t* p, int64_t *num_docs);
error_t prepared_num_chars( prepared_text_t* p, int64_t *num_chars);
error_t prepared_find_doc( prepared_text_t* p, int64_t offset, int64_t *doc);
error_t prepared_doc_end( prepared_text_t* p, int64_t doc, int64_t *doc_end);
error_t prepared_doc_start( prepared_text_t* p, int64_t doc, int64_t *doc_start);

error_t bwt_should_mark_doc( prepared_text_t* p, int64_t mark_period, int64_t offset, int64_t doc, unsigned char* yesno);
error_t bwt_should_mark( prepared_text_t* p, int64_t mark_period, int64_t offset, unsigned char* yesno );

//unsigned char bwt_should_output( prepared_text_t* p);

///// Functions that must be implemented elsewehere and linked in!
/*
   Appends the character ch to newtext. j is the "saved offset".
   Returns the new value for j.
   */
extern error_t prepare_put_char(prepared_text_t* p, alpha_t ch);
/*
   Called after the last character of a document has been added.
   */
extern error_t prepare_end_document(prepared_text_t* p);

extern void free_prepared_text_extra(prepared_text_t* p);


#endif
