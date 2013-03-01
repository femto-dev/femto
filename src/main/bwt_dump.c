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

  femto/src/main/bwt_dump.c
*/
#include <stdio.h>
#include <ctype.h>
#include <assert.h>
#include <inttypes.h>

#include "index_types.h"
#include "bwt_reader.h"
#include "buffer.h"
#include "buffer_funcs.h"

void usage_and_exit(int argc, char** argv)
{
  printf("Usage: %s [--multifile] [--bwt <input-file.bwt>] [--map <document_map_file>] [--info <document_info_file>]\n", argv[0]);
  exit(-1);
}

int main(int argc, char** argv)
{

  int arg;
  char* bwt_path = NULL;
  FILE* bwt_f = NULL;
  char* map_path = NULL;
  FILE* map_f = NULL;
  char* info_path = NULL;
  FILE* info_f = NULL;
  bwt_reader_t bwt;
  bwt_document_map_reader_t map;
  bwt_document_info_reader_t info;
  results_t results = empty_results(RESULT_TYPE_DOCUMENTS);
  //bwt_entry_t ent;
  error_t err;

  int64_t number_of_characters, number_of_documents, alphabet_size;
  int64_t i; 
  unsigned char alphabet_size_bits, offset_size_bits;
  int64_t mark_period;
  int64_t read_chars = 0;
  int64_t ch, off;
  int multifile = 0;

  if( argc < 2 ) {
    usage_and_exit(argc, argv);
  }

  for( arg = 1; arg < argc; arg++ ) {
    if( 0 == strcmp(argv[arg], "--multifile") ) {
      multifile = 1;
    } else if( 0 == strcmp(argv[arg], "--bwt") ) {
      arg++;
      bwt_path = argv[arg];
    } else if( 0 == strcmp(argv[arg], "--map") ) {
      arg++;
      map_path = argv[arg];
    } else if( 0 == strcmp(argv[arg], "--info") ) {
      arg++;
      info_path = argv[arg];
    } else {
      usage_and_exit(argc, argv);
    }
  }

  if( bwt_path ) {
    // hand it off to the bwt-reader
    if( multifile ) {
      err = bwt_reader_open_multifile(&bwt, bwt_path);
      die_if_err(err);
    } else {
      bwt_f = fopen(bwt_path, "r");
      assert(bwt_f);
      err = bwt_reader_open(&bwt, bwt_f);
      die_if_err(err);
    }

    number_of_characters = bwt_num_chars(&bwt);
    printf("Number of characters = %" PRIi64 "\n", number_of_characters);
    number_of_documents = bwt_num_docs(&bwt);
    printf("Number of documents = %" PRIi64 "\n", number_of_documents);

    alphabet_size = bwt_alphabet_size(&bwt);
    printf("Alphabet size = %" PRIi64 "\n", alphabet_size);

    mark_period = bwt_mark_period(&bwt);
    printf("Mark period = %" PRIi64 "\n", mark_period);

    alphabet_size_bits = bwt_alphabet_size_bits(&bwt);
    printf("Alphabet size bits = %i\n", alphabet_size_bits);
    offset_size_bits = bwt_offset_size_bits(&bwt);
    printf("Offset size bits = %i\n", offset_size_bits);

    printf("Begin L column data\n");
    printf("character       offset\n");
    // now read the RLE entries.
    while( ! bwt_at_end(&bwt) ) {
      /*err = bwt_reader_read(&bwt, &ent);
      die_if_err(err);
      ch = ent.ch;
      off = ent.offset;
      if( ch != INVALID_ALPHA ) {
        read_chars += ent.run;
        printf("% -8" PRIi64 " % -16" PRIi64 " (%" PRIi64 " repeats)\n", ent.ch, ent.offset, ent.run);
      }*/
      err = bwt_reader_read_one(&bwt, &ch, &off);
      die_if_err(err);

      if( bwt_at_end(&bwt) ) break;

      assert( ch != INVALID_ALPHA );

      //if( ch != INVALID_ALPHA ) {
        read_chars++;
        char toprint = ' ';
        if( ch >= CHARACTER_OFFSET )
        {
          toprint = ch - CHARACTER_OFFSET;
          if( ! isprint( toprint ) ) toprint = '.';
        }
        printf("%c %-16" PRIi64 " % -16" PRIi64 " \n", toprint, ch, off);
      //}
    } //while(ch != INVALID_ALPHA);

    printf("Read %i encoded characters\n", (int) read_chars);

    bwt_reader_close(&bwt);

    if( bwt_f ) fclose(bwt_f);
  }

  // handle the map info
  if( map_path ) {
    if( multifile ) {
      err = bwt_document_map_reader_open_multifile(&map, map_path);
      die_if_err(err);
    } else {
      map_f = fopen(map_path, "r");
      assert(map_f);
      err = bwt_document_map_reader_open(&map, map_f);
      die_if_err(err);
    }

    printf("Document map; chunk size = %" PRIi64 "\n",
           bwt_document_map_reader_chunk_size(&map));

    for( i = 0; 1; i++ ) {
      err = bwt_document_map_reader_read_chunk(&map);
      die_if_err(err);

      if( bwt_document_map_reader_at_end(&map) ) break;

      err = bwt_document_map_reader_results(&map, &results);
      die_if_err(err);

      printf("document chunk %" PRIi64 "\n", i);
      results_print(stdout, &results);

      results_destroy(&results);
    }

    err = bwt_document_map_reader_close(&map);
    die_if_err(err);

    if( map_f ) fclose(map_f);
  }

  if( info_path ) {
    info_f = fopen(info_path, "r");

    err = bwt_document_info_reader_open(&info, info_f);
    die_if_err(err);

    number_of_characters = bwt_document_info_reader_num_chars(&info);
    printf("Number of characters = %" PRIi64 "\n", number_of_characters);
    number_of_documents = bwt_document_info_reader_num_docs(&info);
    printf("Number of documents = %" PRIi64 "\n", number_of_documents);
    for(  i = 0; i < number_of_documents; i++ ) {
      int64_t doc_end = 0;
      int64_t info_len = 0;
      unsigned char* info_buf = NULL;
      char* mybuf;

      err = bwt_document_info_reader_doc_end(&info, i, &doc_end);
      die_if_err(err);

      printf("End[%" PRIi64 "] = %" PRIi64 "\n", i, doc_end);

      err = bwt_document_info_reader_get(&info, i, &info_len, &info_buf);
      die_if_err(err);

      mybuf = malloc(info_len+1);
      assert(mybuf);
      memcpy(mybuf, info_buf, info_len);
      mybuf[info_len] = '\0';

      printf("Info[%" PRIi64 "] = len %" PRIi64 " '%s'\n", i, info_len, (char*) mybuf);

      free(mybuf);

    }
 
    err = bwt_document_info_reader_close(&info);
    die_if_err(err);

    if( info_f ) fclose(info_f);
  }

}
