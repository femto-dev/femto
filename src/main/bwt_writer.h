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

  femto/src/main/bwt_writer.h
*/
#ifndef _BWT_WRITER_H_
#define _BWT_WRITER_H_

#include <stdlib.h>
#include <stdio.h>

#include "buffer.h"
#include "index_types.h"
#include "bwt.h"

typedef struct {
  // storing the actual data.
  FILE* f;
  unsigned char alphabet_size_bits;
  unsigned char offset_size_bits;

 // helping to do the RLE-encoding
  buffer_t buf; // the sequence of RLE entries 
  alpha_t last_char; // can be INVALID_ALPHA
  int64_t run_length; // run of the same, unmarked character.
} bwt_writer_t;

// 9 bits character + 1 bit + 8 bytes of offset
// and 9 bits character + 1 bit + gamma-code up to about 128 bits
// (the bwt_append function might write both before a flush)
// 128 *bytes* is plenty of room
#define MAX_ENTRY_BYTES 128
#define BWT_BUF_BYTES 2048

typedef struct {
  FILE* f;

  int chunk_size;
  int in_chunk;
  int64_t* chunk;
  int chunk_size_num_bits;
  buffer_t buf;
} bwt_document_map_writer_t;

typedef struct {
  char* fname;
  char* doc_lens_fname;
  char* info_lens_fname;
  char* infos_fname;
  FILE* doc_lens_f; // document length of each logical document.
  FILE* info_lens_f; // length of each doc info.
  FILE* infos_f; // concatenated document infos.
  int64_t num_chars;
  int64_t num_docs;
  int64_t sum_info_lens;
  int64_t last_doc_num;
} bwt_document_info_writer_t;

error_t bwt_begin_write(bwt_writer_t* bwt, FILE* f, int64_t number_of_characters, int64_t number_of_documents, int64_t alphabet_size, int64_t mark_period, unsigned char alphabet_size_bits, unsigned char offset_size_bits);

error_t bwt_flush(bwt_writer_t* bwt);

error_t bwt_maybe_flush(bwt_writer_t* bwt);

void bwt_encode_run(bwt_writer_t* bwt, alpha_t the_char, int64_t run_length);

void bwt_encode_mark(bwt_writer_t* bwt, alpha_t the_char, int64_t mark_logical_offset);

error_t bwt_append(bwt_writer_t* bwt, alpha_t the_char, int64_t mark_logical_offset);

error_t bwt_finish_write(bwt_writer_t* bwt);

error_t bwt_document_map_start_write(bwt_document_map_writer_t* w,
 FILE* aux, int64_t chunk_size, int64_t num_chars, int64_t num_docs);

int compar_int64(const void* aP, const void* bP);

error_t save_document_map(bwt_document_map_writer_t* w);

error_t bwt_document_map_append(bwt_document_map_writer_t* w, int64_t doc);

error_t bwt_document_map_finish_write(bwt_document_map_writer_t* w);

error_t bwt_document_info_start_write(bwt_document_info_writer_t* w, const char* path);

error_t bwt_document_info_write(bwt_document_info_writer_t* w, int64_t doc_num, int64_t doc_size, int64_t info_len, unsigned char* info);

error_t bwt_document_info_finish_write(bwt_document_info_writer_t* w);

// Can be called immediately before or after finish_write
error_t bwt_document_info_get_totals(bwt_document_info_writer_t* w,
                                     int64_t* total_chars,
                                     int64_t* total_docs);

#endif
