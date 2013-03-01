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

  femto/src/main/index_test_funcs.c
*/
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>

#include "bwt.h"
#include "bwt_writer.h"
#include "bwt_creator.h"

#include "timing.h"

#include "server.h"
#include "femto_internal.h"
#include "construct.h"

//#include "index.c"
// in bwt_qsufsort.c
extern alpha_t prepared_char_at(prepared_text_t* p, int64_t idx);

char* TEST_PATH = "./test_indexes";
char* TEST_PATH_INFO = "./test_indexes/info";
int TEST_INDEX_NUM = -1;

#define NUM_TEST_PARAMS 4
void set_test_params(index_block_param_t* param, char** desc, int i)
{
  if( i == 0 ) {
    set_default_param(param);
    calculate_params(param);

    *desc = "default params";
  } else if( i == 1 ) {
    // try first with easy-to-deal-with big blocks&buckets
    set_default_param(param);
    param->block_size = 10000;
    param->b_size = 1000;
    param->chunk_size = 1000;

    calculate_params(param);

    *desc = "big buckets";
  } else if( i == 2 ) {
    // now try with artificially small buckets
    set_default_param(param);
    param->block_size = 10000;
    param->b_size = 4;
    param->chunk_size = 2;

    calculate_params(param);

    *desc = "small buckets";
  } else if( i == 3 ) {
    // now try with tiny blocks and buckets.
    set_default_param(param);
    param->block_size = 16;
    param->b_size = 4;
    param->chunk_size = 8;

    calculate_params(param);

    *desc = "small blocks";
  } else {
    assert(0);
  }
}

int ez_print_char(int ch)
{
  if( ch == -1 ) return printf("-");
  else {
    if( ch < CHARACTER_OFFSET ) return printf("%x", ch);
    else {
      ch = ch - CHARACTER_OFFSET;
      if( isprint(ch) ) return printf("%c", ch);
      else return printf("%x", ch);
    }
  }
}

int ez_print_alpha(int ch)
{
  if( ch == -1 ) return printf("-");
  else {
    if( ch < CHARACTER_OFFSET ) return printf("<%x>", ch);
    else {
      ch = ch - CHARACTER_OFFSET;
      if( isprint(ch) ) return printf("%c", ch);
      else return printf("\\x%x", ch);
    }
  }
}

int int_sort_ascend(const void* ap, const void* bp)
{
  int a = * (int*) ap;
  int b = * (int*) bp;

  // a < b -> return -1
  return a - b;
}

int chunk_size = 4;

// After do_bwt is called, must close the readers.
// bwt can be passed to index_documents
// after that it should be passed to bwt_reader_close(bwt);
// L and F must have allocated length == p.number_of_characters
void do_bwt(// input:
           prepared_text_t* p, // because of headers.
           int64_t mark_period,
           // output:
           int* L, int* F,
           bwt_reader_t* bwt,
           bwt_document_map_reader_t* map)
{
  // step 1 - block sort the input.
  error_t err;
  FILE* f;
  FILE* auxf = NULL;
  suffix_array_t sa;
  bwt_writer_t writer;
  bwt_document_map_writer_t aux_writer;
  sa_reader_t reader;
  int64_t use_offset;
  int printed = 0;
  int64_t i, j;
  int64_t cur_len, max_len;
  int64_t doc;
  int64_t num_chars, num_docs;
  int64_t doc_start, doc_end;
  int print;
 

  err = prepared_num_docs( p, &num_docs);
  die_if_err(err);
  err = prepared_num_chars( p, &num_chars);
  die_if_err(err);

  print = DEBUG && (DEBUG > 2 || num_chars < 60 );

  f = tmpfile();
  assert(f);

  if( map ) {
    auxf = tmpfile();
    assert(auxf);
  }

  memset(&sa, 0, sizeof(suffix_array_t));

  err = suffix_sort(p, &sa);
  assert(!err);
  
  memset(&reader, 0, sizeof(sa_reader_t));
  reader.mark_period = mark_period;
  reader.p = p;
  reader.sa = &sa;
  reader.state = NULL;
  reader.offset = -1;
  reader.L = INVALID_ALPHA;
  err = init_sa_reader(&reader);
  assert(!err);

  if( map ) {
    err = bwt_document_map_start_write(&aux_writer, auxf, chunk_size,
                                       num_chars, num_docs);
    die_if_err(err);
  }

  err = bwt_begin_write_easy(&writer, f, p, mark_period);
  die_if_err(err);

  max_len = 10;
  for( doc = 0; doc < num_docs; doc++ ) {
    err = prepared_doc_start( p, doc, &doc_start );
    die_if_err(err);
    
    err = prepared_doc_end( p, doc, &doc_end );
    die_if_err(err);

    cur_len = doc_end - doc_start;
    if( cur_len > max_len ) {
      max_len = cur_len;
    }
  }
  max_len += 20;

  i = 0;
  while(read_sa_bwt(&reader)){
    if( reader.mark ) {
      use_offset = reader.offset;
    } else {
      use_offset = -1;
    }

    err = bwt_append(&writer, reader.L, use_offset);
    assert(!err);

    if( map ) {
      err = bwt_document_map_append(&aux_writer, reader.doc);
      assert(!err);
    }

    L[i] = reader.L;

    // print out this row.
    if( print ) {
      int z;
      int ch;
      printed = 0;
      printed += printf("%03i %03i  ", (int) i, (int) reader.offset);
      // print the F char.
      ch = prepared_char_at(p, reader.offset_ssort);
      z = ez_print_alpha(ch);
      while( z < 5 ) {
        printf(" ");
        z++;
      }
      printed += z;

      printed += printf("  ");
    }

    i++;

    // which document has SA[i]?
    err = prepared_find_doc( p, reader.offset_ssort, &doc );
    die_if_err(err);

    err = prepared_doc_start( p, doc, &doc_start );
    die_if_err(err);
    
    err = prepared_doc_end( p, doc, &doc_end );
    die_if_err(err);

    cur_len = doc_end - doc_start;

    // now print out the characters in this row (based on that doc).
    if( print ) {
      for( j = 0; j < cur_len; j++ ) {
        int doc_offset;
        int ch;
        doc_offset = (reader.offset_ssort + j - doc_start) % cur_len;
        if( doc_start+doc_offset < doc_end ) {
          ch = prepared_char_at(p, doc_offset+doc_start);
          printed += ez_print_char(ch);
        }
      }
      while( printed < max_len ) {
        printed += printf(" ");
      }
    }

    if( print ) {
      printf("  ");
      ez_print_alpha(reader.L);
      printf("  ");
      printf("%i", reader.L);
      printf("\n");
    }
  }

  err = bwt_finish_write(&writer);
  assert(!err);

  if( map ) {
    err = bwt_document_map_finish_write(&aux_writer);
    die_if_err(err);
  }

  free_sa_reader(&reader);
  free_suffix_array(&sa);

  // If we have an F pointer, compute it.
  if( F ) {
    memcpy(F, L, i * sizeof(int));
    qsort(F, i, sizeof(int), int_sort_ascend);
  }

  // set up the bwt reader.
  err = bwt_reader_open(bwt, f);
  assert(!err);

  if( map ) {
    // set up the map reader.
    err = bwt_document_map_reader_open(map, auxf );
    assert(!err);
  }

  SAFETY_CHECK;
}


// generate a text that's interesting to search for
void generate_text(int len, uchar* text)
{
  int i,j,k;
  int idx;

  memset(text, 'x', len);
  i = 0;
  j = 0; // the counter
  while ( i < len ) {
    // output our string for j
    k = j;
    while( k > 0 ) {
      // output k & 0xf
      idx = (k - 1) % 6;
      text[i++] = 'a' + idx;
      if( i == len ) return;
      k /= 6;
    }
    j++;
  }
  SAFETY_CHECK;
}

void generate_text_random(int len, unsigned char* text)
{
  for( int i = 0; i < len; i++ ) {
    text[i] = 'a' + (rand() % 6);
  }
}

int count_num_occs(int tlen, uchar* text, int plen, alpha_t* pat)
{
  int noccs;
  int i;
  int j;
  int match;

  noccs = 0;

  for( i = 0; i <= tlen - plen; i++ ) {
    match = 1;
    for( j = 0; j < plen; j++ ) {
      if( CHARACTER_OFFSET + text[i+j] != pat[j] ) {
        match = 0;
        break;
      }
    }
    if( match ) noccs++;
  }

  SAFETY_CHECK;
  return noccs;
}

/** Create an index with particular content.
  */
void create_index(int ndocs, char** urls,
                  int* doc_lens, unsigned char** docs,
                  char* index_path,
                  index_block_param_t* param,
                  char* desc /* the name of the test case */)
{
  prepared_text_t prepared;
  int64_t len;
  bwt_reader_t bwt;
  bwt_document_map_reader_t map;
  error_t err;
  int* realL;
  int* realF;
  index_block_param_t default_param;

  if( DEBUG ) {
    printf("Creating index %s\n", desc);
  }
  if( param == NULL ) {
    param = &default_param;
    set_default_param(param);
  }
  memset(&prepared, 0, sizeof(prepared_text_t));

  err = init_prepared_text(&prepared, TEST_PATH_INFO);
  die_if_err(err);

  // count our files.
  for( int i = 0; i < ndocs; i++ ) {
    char* url = "";
    if( urls && urls[i] ) url = urls[i];
    err = count_file(&prepared, doc_lens[i],
                      0, NULL,
                      strlen(url), (unsigned char*) url);
    die_if_err(err);
  }

  // append our files.
  for( int i = 0; i < ndocs; i++ ) {
    char* url = "";
    if( urls && urls[i] ) url = urls[i];
    err = append_file_mem(&prepared, doc_lens[i], docs[i],
                      0, NULL, NULL, // no headers
                      strlen(url), (unsigned char*) url);
    die_if_err(err);
  }

  err = prepared_num_chars( &prepared, &len);
  die_if_err(err);

  realL = malloc(sizeof(int)*len);
  assert(realL);
  memset(realL, -1, sizeof(int)*len);

  realF = malloc(sizeof(int)*len);
  assert(realF);
  memset(realF, -1, sizeof(int)*len);

  chunk_size = param->chunk_size;
  do_bwt(&prepared, param->mark_period, realL, realF, &bwt, &map);

  if( index_path == NULL ) {
    index_path = TEST_PATH;
  }

  err = index_documents( &bwt, &map, &prepared.info_reader, param, index_path, NULL );
  die_if_err(err);

  bwt_reader_close(&bwt);
  bwt_document_map_reader_close(&map);

  free_prepared_text(&prepared);
  free(realL);
  free(realF);

  // now... we have an index!
}

void check_results(results_t* got, 
                   int num_expect,
                   location_info_t* expect)
{
  results_reader_t r;
  error_t err;
  int64_t document, offset;
  int num_got;

  err = results_reader_create(&r, got);
  die_if_err(err);

  num_got = 0;
  document = 0; offset = 0;
  while( results_reader_next(&r, &document, &offset ) ) {
    assert( num_got < num_expect );
    assert( document == expect[num_got].doc );
    if( results_type( got ) == RESULT_TYPE_DOC_OFFSETS ) {
      assert( offset == expect[num_got].offset );
    }
    num_got++;
  }

  assert(num_got == num_expect);

  results_reader_destroy(&r);
}

void create_random_documents(int len, int ndocs, int** doc_lens_out, unsigned char*** docs_out)
{
  int* doc_ends = malloc(ndocs*sizeof(int));
  unsigned char** docs = malloc(ndocs*sizeof(unsigned char*));
  int total;

  assert(doc_ends);
  assert( len > 2*ndocs );

  // set the document ends.
  for( int i = 0; i < ndocs - 1; i++ ) {
    doc_ends[i] = (i+1) * len / ndocs - 1;
    // random bit twiddle below..
    if( rand() % 2 ) {
      doc_ends[i]++;
    }
    if(doc_ends[i] >= len ) {
      assert(i == ndocs - 1 );
      doc_ends[i] = len - 1;
    }
  }
  doc_ends[ndocs-1] = len;

  // convert doc ends to doc lens.
  for( int i = ndocs - 1; i >= 1; i-- ) {
    doc_ends[i] -= doc_ends[i-1];
  }

  // generate the docs.
  total = 0;
  for( int i = 0; i < ndocs; i++ ) {
    docs[i] = malloc(doc_ends[i]);
    assert(docs[i]);
    if( rand() % 1 ) {
      generate_text(doc_ends[i], docs[i]);
    } else {
      generate_text_random(doc_ends[i], docs[i]);
    }
    total += doc_ends[i];
  }

  assert( total == len );

  *doc_lens_out = doc_ends;
  *docs_out = docs;
}

void free_documents(int ndocs, int* doc_lens, unsigned char** docs)
{
  for( int i = 0; i < ndocs; i++ ) {
    free(docs[i]);
  }
  free(doc_lens);
  free(docs);
}

void print_index_internal(char* index_path, int verbose)
{
  header_loc_query_t hlq;
  header_occs_query_t hoq;
  block_query_t bq;
  int64_t row, block_num, block_start, len;
  femto_server_t srv;
  index_locator_t index_loc;
  error_t err;

  err = femto_start_server_err(&srv, verbose);
  die_if_err(err);

  err = femto_loc_for_path_err(&srv, index_path, &index_loc);
  die_if_err(err);

  // first, get the length of the L column of the index.
  setup_header_loc_query(&hlq, index_loc, 0, NULL,
                         HDR_LOC_REQUEST_L_SIZE, 0, 0);
  err = femto_run_query(&srv, (query_entry_t*) &hlq);
  die_if_err(err);

  len = hlq.r.doc_len;

  cleanup_header_loc_query(&hlq);

  if( verbose ) printf("Index length: %" PRIi64 "\n", len);

  // now go through the rows.
  for( row = 0; row < len; row++ ) {
    setup_header_occs_query(&hoq, index_loc, 0, NULL, 
                           HDR_BSEARCH_BLOCK_ROWS,
                           0, 0, row, 0);
    err = femto_run_query(&srv, (query_entry_t*) &hoq);
    die_if_err(err);
    block_num = hoq.r.block_num;
    assert( block_num >= 0 );
    block_start = hoq.r.row;
    assert( block_start >= 0 );
    assert( block_start < len );
    setup_block_query(&bq, index_loc, 0, NULL,
                      BLOCK_REQUEST_CHAR|BLOCK_REQUEST_LOCATION,
                      block_num, 0, row - block_start, 0);
    err = femto_run_query(&srv, (query_entry_t*) &bq);
    die_if_err(err);

    if( verbose ) {
      printf("row %" PRIi64 "   % 4i  " , row, bq.r.ch); 
      ez_print_alpha(bq.r.ch);
      printf("\n");
    }
  }

  femto_stop_server(&srv);
}

void print_index(char* index_path)
{
  print_index_internal(index_path, 1);
}


