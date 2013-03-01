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

  femto/src/main/index_test.c
*/
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>

#include "bwt.h"
#include "bwt_writer.h"

#include "timing.h"

#include "server.h"
#include "femto_internal.h"

#include "index_test_funcs.c"
//#include "index.c"
// in bwt_qsufsort.c
extern alpha_t prepared_char_at(prepared_text_t* p, int64_t idx);

#define BUCKET_CACHE_SIZE 6


// stack variables that were made global.
int tg_C[ALPHA_SIZE];
unsigned int tg_occs[ALPHA_SIZE];
unsigned int tg_block_occs[ALPHA_SIZE];
unsigned int tg_occsRead[ALPHA_SIZE];
#define TG_DATA_SIZE 4096
uchar tg_data[TG_DATA_SIZE];
uchar tg_inUse[ALPHA_SIZE];
int tg_offsets[1024];
#define TG_MTFV_SIZE 1024
uint16_t tg_mtfv[TG_MTFV_SIZE];
uchar tg_charsInUse[ALPHA_SIZE];
int tg_realL[1024]; 
uchar tg_text[1024];
alpha_t tg_unseqToSeq[ALPHA_SIZE];
alpha_t tg_yy[ALPHA_SIZE];

void make_and_test_index(int len, uchar* text, index_block_param_t* param, char* desc)
{
  prepared_text_t prepared;
  bwt_reader_t bwt;
  char* index_path;
  index_locator_t index_loc;
  int plen;
  int dlen;
  int* realL = NULL;
  int* realF = NULL;
  int block_num;
  int nblocks;
  error_t err;
  int i,j;
  int64_t got;
  char* url = "url";
  int alpha_len;
  header_block_t header_block;
  header_occs_request_t header_req;
  header_loc_request_t header_loc_req;
  int64_t num_chars, num_docs;
  path_translator_t trans;

  index_path = TEST_PATH;

  err = path_translator_init(&trans);
  die_if_err(err);

  err = path_translator_id_for_path(&trans, index_path, &index_loc);
  die_if_err(err);

  printf("Testing index %s %i\n", desc, len);
  if( DEBUG ) printf("Testing index %s %i: creating\n", desc, len);

  // First, prepare the text
  memset(&prepared, 0, sizeof(prepared_text_t));

  err = init_prepared_text(&prepared, TEST_PATH_INFO);
  die_if_err(err);

  // just one file to append... append it.
  err = count_file(&prepared, len, 0, NULL, strlen(url), (unsigned char*) url);
  die_if_err(err);

  err = append_file_mem(&prepared, len, text,
                    0, NULL, NULL, // no headers
                    strlen(url), (unsigned char*) url);
  die_if_err(err);

  err = prepared_num_docs( &prepared, &num_docs);
  die_if_err(err);
  err = prepared_num_chars( &prepared, &num_chars);
  die_if_err(err);

  alpha_len = dlen = plen = num_chars;

  SAFETY_CHECK;
  realL = malloc(sizeof(int)*plen);
  assert(realL);
  memset(realL, -1, sizeof(int)*plen);

  realF = malloc(sizeof(int)*plen);
  assert(realF);
  memset(realF, -1, sizeof(int)*plen);

  // Now compute the BWT
  do_bwt(&prepared, param->mark_period, realL, realF, &bwt, NULL);

  // Next, create the actual index.

  if( DEBUG ) printf("Testing index %s %i: prepared length is %i\n", desc, len, plen);
  SAFETY_CHECK;

  SAFETY_CHECK;
  nblocks = ceildiv(alpha_len, param->block_size);
  SAFETY_CHECK;

  SAFETY_CHECK;
  err = index_documents( &bwt, NULL, &prepared.info_reader, param, index_path, NULL);
  die_if_err ( err );
  SAFETY_CHECK;

  // now we're done with the bwt_reader.
  bwt_reader_close(&bwt);

  // Get the C values.
  for( i = 0; i < ALPHA_SIZE; i++ ) {
    tg_occs[i] = 0;
    tg_block_occs[i] = 0;
    tg_C[i] = 0;
  }

  // accumulate the C values.
  for( i = 0; i < alpha_len; i++ ) {
    tg_occs[realF[i]]++;
  }

  free_prepared_text(&prepared);
  SAFETY_CHECK;

  if( DEBUG ) printf("Testing index %s %i: getting buckets and occs\n", desc, len);

  {
    j = 0;
    for ( i = 0; i < ALPHA_SIZE; i++ ) {
      tg_C[i] = j;
      j += tg_occs[i];
    }
  }

  for( i = 0; i < ALPHA_SIZE; i++ ) tg_occs[i] = 0;

  // test out the header block.

  err = open_header_block(&header_block, &trans, index_loc);
  die_if_err(err);

  // try getting all C values: HDR_REQUEST_C
  for( i = 0; i < ALPHA_SIZE+1; i++ ) {
    memset(&header_req, 0, sizeof(header_occs_request_t));
    header_req.ch = i;
    err = header_occs_request(&header_block, HDR_REQUEST_C, &header_req);
    die_if_err(err);
    if( i == ALPHA_SIZE ) {
      assert( header_req.occs == alpha_len );
    } else {
      assert( header_req.occs == tg_C[i] );
    }
  }

  // try getting the first character of every row: HDR_BSEARCH_C
  for( i = 0; i < alpha_len; i++ ) {
    memset(&header_req, 0, sizeof(header_occs_request_t));
    header_req.occs = i;
    err = header_occs_request(&header_block, HDR_BSEARCH_C, &header_req);
    die_if_err(err);
    assert( header_req.ch == realF[i] );
  }

  // try resolving every location: HDR_RESOLVE_LOCATION
  for( i = 0; i < alpha_len; i++ ) {
    memset(&header_loc_req, 0, sizeof(header_loc_request_t));
    header_loc_req.offset = i;
    err = header_loc_request(&header_block, HDR_LOC_RESOLVE_LOCATION, &header_loc_req);
    die_if_err(err);
    assert( header_loc_req.loc.doc == 0 );
    assert( header_loc_req.loc.offset == i );
  }

  // open up the index we just built.
  for( block_num = 0; block_num < nblocks; block_num ++ ) {
    int block_start;
    int block_end;
    int block_len;
    data_block_t block;

    block_start = block_num * param->block_size;
    block_end = block_start + param->block_size;
    if( block_end > alpha_len ) block_end = alpha_len;
    block_len = block_end - block_start;

    err = open_data_block(&block, &trans, index_loc, block_num, BUCKET_CACHE_SIZE);
    die_if_err ( err );

    // get the # occs at the start of the block.
    for( i = 0; i < ALPHA_SIZE; i++ ) {
      tg_block_occs[i] = tg_occs[i];
    }

    // check occs: HDR_REQUEST_BLOCK_OCCS
    for( i = 0; i < ALPHA_SIZE; i++ ) {
      memset(&header_req, 0, sizeof(header_occs_request_t));
      header_req.ch = i;
      header_req.block_num = block_num;
      err = header_occs_request(&header_block, HDR_REQUEST_BLOCK_OCCS, &header_req);
      die_if_err(err);
      assert( header_req.occs == tg_block_occs[i] );
    }

    for( i = block_start; i < block_end; i++ ) {
      block_request_t np;
      tg_occs[realL[i]]++;

      // check HDR_BSEARCH_BLOCK_OCCS
      memset(&header_req, 0, sizeof(header_occs_request_t));
      header_req.ch = realL[i];
      header_req.occs = tg_occs[realL[i]];
      err = header_occs_request(&header_block, HDR_BSEARCH_BLOCK_OCCS, &header_req);
      die_if_err(err);
      assert( header_req.block_num == block_num );

      // check BLOCK_REQUEST_OCCS
      j = realL[i];
      np.row_in_block = i - block_start;
      np.ch = j;
      err = block_request(&block, BLOCK_REQUEST_OCCS, &np);
      die_if_err ( err );
      got = np.occs_in_block + tg_block_occs[j];
      assert( got == tg_occs[j] );

      for( j = 0; j < ALPHA_SIZE; j++ ) {
        np.occs_in_block = 99;
        np.row_in_block = i - block_start;
        np.ch = j;
        err = block_request(&block, BLOCK_REQUEST_OCCS, &np);
        die_if_err ( err );
        got = np.occs_in_block + tg_block_occs[j];
        assert( got == tg_occs[j] );
        //printf("Tested at i=%i j=%i\n", i, j);
      }

      // check getting the character.
      np.ch = INVALID_ALPHA;
      err = block_request(&block, BLOCK_REQUEST_CHAR, &np);
      die_if_err ( err );
      assert(np.ch == realL[i]);

      // check BLOCK_REQUEST_ROW
      np.ch = realL[i];
      np.occs_in_block = tg_occs[np.ch] - tg_block_occs[np.ch];
      np.row_in_block = 0;
      err = block_request(&block, BLOCK_REQUEST_ROW, &np);
      die_if_err( err );
      assert( np.row_in_block == i - block_start );

    }
    
    close_data_block( & block );
    SAFETY_CHECK;
    
  }

  close_header_block(&header_block);

  if( DEBUG ) printf("Testing index %s %i: server queries\n", desc, len);
  {
    int npats;
    int maxpats;
    int *plens;
    alpha_t** pats;
    int64_t* first;
    int64_t* last;
    int * noccs;
    int64_t** offsets;
    femto_server_t srv;
    int got_count;
    int exp_count;
    char* xs;

    maxpats = 5 + 2 * len;
    plens = malloc(maxpats * sizeof(int));
    pats = malloc(maxpats * sizeof(uchar*));
    first = malloc(maxpats * sizeof(int64_t));
    last = malloc(maxpats * sizeof(int64_t));
    noccs = malloc(maxpats * sizeof(int));
    offsets = malloc(maxpats * sizeof(int64_t*));
    npats = 0;

    // generate some patterns.
    pats[npats] = strtoalpha(3, (unsigned char*) "abc"); plens[npats] = 3; npats++;
    pats[npats] = strtoalpha(2, (unsigned char*) "ab"); plens[npats] = 2; npats++;
    pats[npats] = strtoalpha(2, (unsigned char*) "bc"); plens[npats] = 2; npats++;
    pats[npats] = strtoalpha(5, (unsigned char*) "zzyxe"); plens[npats] = 5; npats++;
    pats[npats] = strtoalpha(1, (unsigned char*) "a"); plens[npats] = 1; npats++;
    pats[npats] = strtoalpha(1, (unsigned char*) "c"); plens[npats] = 1; npats++;
    xs = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    pats[npats] = strtoalpha(strlen(xs), (unsigned char*) xs); plens[npats] = strlen(xs); npats++;
    while( npats < maxpats ) {
      int plen, pstart;
      // select some part of the string
      plen = 1 + (rand() % 100);
      pstart = rand() % (len-1);
      if( pstart + plen > len ) plen = len - pstart;

      pats[npats] = strtoalpha(plen, &text[pstart]);
      plens[npats] = plen;
      npats++;
    }

    path_translator_destroy(&trans);
    memset(&index_loc, 0, sizeof(index_locator_t));


    if( DEBUG ) printf("Doing queries - starting server\n");
    // do the querying.
    err = femto_start_server_err(&srv, 1);
    die_if_err(err);
    
    err = femto_loc_for_path_err(&srv, index_path, &index_loc);
    die_if_err(err);

    if( DEBUG ) printf("Doing queries - count\n");
    {
      // try doing them one at a time.
      start_clock();
      for( i = 0; i < npats; i++ ) {
        err = parallel_count(&srv, index_loc, 1, &plens[i], &pats[i], &last[i], NULL);
        die_if_err ( err );
      }
      stop_clock();
      if(DEBUG) print_timings("single count queries", npats);

      start_clock();
      err = parallel_count(&srv, index_loc, npats, plens, pats, last, NULL);
      stop_clock();
      die_if_err ( err );
      if(DEBUG) print_timings("parallel count queries", npats);

      // test the results.
      for( i = 0; i < npats; i++ ) {
        got_count = last[i];//1 + last[i] - first[i];
        exp_count = count_num_occs(len, text, plens[i], pats[i]);
        if( DEBUG > 2 ) printf("Expected %i got %i\n", exp_count, got_count);
        assert(got_count == exp_count);
      }

    }
    if( DEBUG ) printf("Doing queries - locate\n");
    {
      int z;
      int off;

      // try doing them one at a time
      start_clock();
      for( i = 0; i < npats; i++ ) {
        offsets[i] = NULL;
        if( DEBUG > 3 ) {
          printf("Single locate on query ");
          for( z = 0; z < plens[i]; z++ ) {
            ez_print_alpha(pats[i][z]);
          }
          printf("\n");
        }
        err = parallel_locate(&srv, index_loc, 1, &plens[i], &pats[i], INT32_MAX, &noccs[i], &offsets[i]);
        die_if_err ( err );

        // test the result.
        for( j = 0; j < noccs[i]; j++ ) {
          off = offsets[i][j];
          for( z = 0; z < plens[i]; z++ ) {
            assert( text[off+z] + CHARACTER_OFFSET == pats[i][z] );
          }
        }
        free(offsets[i]);
        offsets[i] = NULL;
      }
      stop_clock();
      if(DEBUG) print_timings("single locate queries", npats);

      start_clock();
      err = parallel_locate(&srv, index_loc, npats, plens, pats, INT32_MAX, noccs, offsets);
      stop_clock();
      die_if_err ( err );
      if(DEBUG) print_timings("parallel locate queries", npats);

      // test the results.
      for( i = 0; i < npats; i++ ) {
        got_count = noccs[i];
        exp_count = count_num_occs(len, text, plens[i], pats[i]);
        if( DEBUG > 2 ) printf("Expected %i got %i\n", exp_count, got_count);
        assert(got_count == exp_count);

        // now check all the occs.
        for( j = 0; j < noccs[i]; j++ ) {
          off = offsets[i][j];
          for( z = 0; z < plens[i]; z++ ) {
            assert( text[off+z] + CHARACTER_OFFSET == pats[i][z]);
          }
        }
      }

      for( i = 0; i < npats; i++ ) {
        free(offsets[i]);
      }
    }

    if( DEBUG ) printf("Extracting document\n");
    {
      extract_document_query_t q;
      err = setup_extract_document_query(&q, NULL, index_loc, 0);
      die_if_err( err );
      err = femto_run_query(&srv, (query_entry_t*) &q);
      die_if_err( err );
      assert( q.proc.entry.err_code == 0 );
      assert( q.doc_len >= len );
      for( i = 0; i < len; i++ ) {
        int expect = text[i];
        expect += CHARACTER_OFFSET;
        assert( q.content[i] == expect );
      }
      cleanup_extract_document_query(&q);
    }
    if( DEBUG ) printf("Stopping server\n");
    femto_stop_server(&srv);
    SAFETY_CHECK;

    
    for( i = 0; i < npats; i++ ) {
      free(pats[i]);
    }
    free(plens);
    free(pats);
    free(first);
    free(last);
    free(noccs);
    free(offsets);

  }


  if( DEBUG ) printf("Testing index %s %i: done\n", desc, len);

  free(realL);
  free(realF);


}


void test_write_occs(void)
{
  // test that write_occs and read_occs are inverses.
  buffer_t b = build_buffer(TG_DATA_SIZE, tg_data);
  int i;
  int maxbits = 23;

  for( i = 0; i < ALPHA_SIZE; i++ ) {
    tg_inUse[i] = 1;
    if( i == 128 ) tg_inUse[i] = 0;
  }


  for( i = 0; i < ALPHA_SIZE; i++ ) {
    tg_occs[i] = i * i + i;
  }

  write_occs(&b, tg_inUse, maxbits, tg_occs);
  read_occs(&b, tg_inUse, maxbits, tg_occsRead);
  assert(b.len < b.max);

  for( i = 0; i < ALPHA_SIZE; i++ ) {
    if( tg_inUse[i] ) assert( tg_occs[i] == tg_occsRead[i] );
    else assert( 0 == tg_occsRead[i] );
  }

}

void test_construct(void)
{
  error_t err;
  char* index_path = TEST_PATH;
  path_translator_t trans;
  index_locator_t index_loc;
  int num_texts = 2;
  uchar* texts[] = {(unsigned char*) "test_one;", (unsigned char*) "test_two_fun;"};
  int preparedEnds[2];
  prepared_text_t prepared;
  bwt_reader_t bwt;
  int len;
  int alphalen;
  int markPeriod = 100;
  char* urls[] = {"1", "2"};
  index_block_param_t param;
  int64_t num_docs, num_chars;


  printf("Testing index construction\n");
  // try constructing an index.

  set_default_param(&param);
  param.mark_period = markPeriod;

  err = init_prepared_text(&prepared, TEST_PATH_INFO);
  assert(!err);

  for( int i = 0; i < num_texts; i++ ) {
    err = count_file(&prepared, strlen((char*) texts[i]),
                      0, NULL, // no headers
                      strlen(urls[i]), (unsigned char*) urls[i]);
    assert(!err);
  }

  for( int i = 0; i < num_texts; i++ ) {
    err = append_file_mem(&prepared, strlen((char*) texts[i]), texts[i], 
                      0, NULL, NULL, // no headers
                      strlen(urls[i]), (unsigned char*) urls[i]);
    assert(!err);
  }

  err = prepared_num_docs( &prepared, &num_docs);
  die_if_err(err);
  err = prepared_num_chars( &prepared, &num_chars);
  die_if_err(err);
  for( int i = 0; i < num_texts; i++ ) {
    int64_t doc_end;
    err = prepared_doc_end(&prepared, i, &doc_end);
    die_if_err(err);
    preparedEnds[i] = doc_end;
  }

  alphalen = len = num_chars;

  memset(tg_realL, -1, sizeof(tg_realL));
  do_bwt(&prepared, param.mark_period, tg_realL, NULL, &bwt, NULL);

  err = index_documents( &bwt, NULL, &prepared.info_reader, &param, index_path, NULL );
  die_if_err( err );

  bwt_reader_close(&bwt);

  free_prepared_text(&prepared);

  err = path_translator_init(&trans);
  die_if_err(err);

  err = path_translator_id_for_path(&trans, index_path, &index_loc);
  die_if_err(err);

  // open up the index.
  {
    data_block_t block;
    int64_t occ;
    index_block_param_t param;


    set_default_param(&param);
    param.b_size = 1000;
    param.chunk_size = 500;
    param.block_size = 10000;
    err = calculate_params(&param);
    die_if_err ( err );

    err = open_data_block(&block, &trans, index_loc, 0, BUCKET_CACHE_SIZE);
    die_if_err ( err );

    // try to get some occs.
    {
      block_request_t np;

      np.occs_in_block = 0;
      np.ch = -1;
      np.row_in_block = 7;
      err = block_request(&block, BLOCK_REQUEST_CHAR, &np);
      die_if_err ( err );
      assert( np.ch == CHARACTER_OFFSET + 'n');

      np.occs_in_block = 0;
      np.ch = CHARACTER_OFFSET + 'n';
      np.row_in_block = 19;
      err = block_request(&block, BLOCK_REQUEST_OCCS, &np);
      die_if_err ( err );
      occ = np.occs_in_block;
      assert( occ == 2);

      np.occs_in_block = 0;
      np.ch = CHARACTER_OFFSET + 'e';
      np.row_in_block = 1;
      err = block_request(&block, BLOCK_REQUEST_OCCS, &np);
      occ = np.occs_in_block;
      die_if_err ( err );
      assert( occ == 0);

      np.occs_in_block = 0;
      np.ch = -1;
      np.row_in_block = 8;
      err = block_request(&block, BLOCK_REQUEST_CHAR | BLOCK_REQUEST_OCCS, &np);
      occ = np.occs_in_block;
      die_if_err ( err );
      assert( occ == 3 );
      assert( np.ch == CHARACTER_OFFSET + 't' );

      np.occs_in_block = 0;
      np.ch = CHARACTER_OFFSET + 't';
      np.row_in_block = 8;
      err = block_request(&block, BLOCK_REQUEST_OCCS, &np);
      occ = np.occs_in_block;
      die_if_err ( err );
      assert( occ == 3 );

      np.occs_in_block = 0;
      np.ch = CHARACTER_OFFSET + 't';
      np.offset = INVALID_OFFSET;
      np.row_in_block = 19;
      err = block_request(&block,
                          BLOCK_REQUEST_OCCS | BLOCK_REQUEST_LOCATION,
                          &np);
      die_if_err ( err );
      occ = np.occs_in_block;
      assert( occ == 4 );
      assert( np.offset == 0 );

      np.ch = CHARACTER_OFFSET + 't';
      np.row_in_block = 21;
      err = block_request(&block, 
                          BLOCK_REQUEST_OCCS | BLOCK_REQUEST_LOCATION,
                          &np);
      die_if_err ( err ) ;
      assert( np.offset == INVALID_OFFSET );


      np.occs_in_block = 0;
      np.ch = CHARACTER_OFFSET + 't';
      np.offset = INVALID_OFFSET;
      np.row_in_block = 20;
      err = block_request(&block,
                          BLOCK_REQUEST_OCCS | BLOCK_REQUEST_LOCATION,
                          &np);
      die_if_err ( err );
      occ = np.occs_in_block;
      assert( occ == 4 );
      assert( np.offset == preparedEnds[0] ); // start of 2nd document.


      // try resolving this offset.
      {
        header_block_t hb;
        header_loc_request_t hr;
        header_occs_request_t or;

        err = open_header_block(&hb, &trans, index_loc);
        die_if_err ( err );

        memset(&hr, 0, sizeof(header_loc_request_t));
        hr.offset = np.offset;
        err = header_loc_request(&hb,
                             HDR_LOC_RESOLVE_LOCATION | HDR_LOC_REQUEST_DOC_LEN ,
                             &hr);
        die_if_err ( err );
        assert( hr.loc.doc == 1 );
        assert( hr.loc.offset == 0 );
        assert( hr.doc_len == preparedEnds[1] - preparedEnds[0]  );

        free(hr.doc_info);


        // Check that C is correct.
        memset(&or, 0, sizeof(header_occs_request_t));
        or.ch = 255;
        err = header_occs_request(&hb, HDR_REQUEST_C, &or);
        die_if_err( err );
        assert( or.occs == len );

        memset(&or, 0, sizeof(header_occs_request_t));
        or.ch = 'e' + CHARACTER_OFFSET;
        err = header_occs_request(&hb, HDR_REQUEST_C, &or);
        die_if_err( err );
        assert( or.occs == 7 );

        memset(&or, 0, sizeof(header_occs_request_t));
        or.ch = 't' + CHARACTER_OFFSET;
        err = header_occs_request(&hb, HDR_REQUEST_C, &or);
        die_if_err( err );
        assert( or.occs == 17 );

        close_header_block(&hb);
      }

      // check that the sorting is correct.
      for( int i = 0 ; i < preparedEnds[num_docs-1]; i++ ) {

        np.row_in_block = i;
        np.ch = INVALID_ALPHA;
        err = block_request(&block, BLOCK_REQUEST_CHAR, &np);
        die_if_err ( err );
        assert(np.ch == tg_realL[i]);
      }
    }

    close_data_block(&block);
  }

  path_translator_destroy(&trans);


}

void test_indices(void)
{
  // try creating a 2-block index with small block size.
  //uchar text[44];
  //int len = 44;
  char* desc;
  int len = 0;
  index_block_param_t param;


  memset(tg_text, 0, 1024);

  printf("Testing indexing of generated documents\n");

  for( int i = 0; i < NUM_TEST_PARAMS; i++ ) {
    set_test_params(&param, &desc, i);

    len = 3;
    generate_text(len, tg_text);
    tg_text[len] = '\0';
    make_and_test_index(len, tg_text, &param, desc);

    len = 4;
    generate_text(len, tg_text);
    tg_text[len] = '\0';
    make_and_test_index(len, tg_text, &param, desc);

    len = 10;
    generate_text(len, tg_text);
    tg_text[len] = '\0';
    make_and_test_index(len, tg_text, &param, desc);

    len = 13;
    generate_text(len, tg_text);
    tg_text[len] = '\0';
    make_and_test_index(len, tg_text, &param, desc);

    len = 20;
    generate_text(len, tg_text);
    tg_text[len] = '\0';
    make_and_test_index(len, tg_text, &param, desc);

    len = 400;
    generate_text(len, tg_text);
    tg_text[len] = '\0';
    make_and_test_index(len, tg_text, &param, desc);
  }

}

block_query_t makebq(int row)
{
  block_query_t ret;
  memset(&ret, 0, sizeof(block_query_t));
  ret.leaf.entry.type = QUERY_TYPE_BLOCK;
  ret.leaf.entry.loc.id = -1;
  ret.leaf.block_id = 0;
  ret.r.row_in_block = row;
  ret.r.ch = 0;
  return ret;
}

void print_leaf_queries(server_state_t* ss)
{
  int i;
  leaf_entry_t* node;
  i = 0;
  for( node = rb_min_leaf_entry(ss);
       node;
       node = rb_next_leaf_entry(ss, node)
     ) {
    block_query_t* q = (block_query_t*) node;
    printf("%i: query row %i\n", i, q->r.row_in_block);
    i++;
  }
}

void test_rb_block_queries(void)
{
  int i;
  server_state_t ss;
  block_query_t arr[50];
  leaf_entry_t* node;
  leaf_entry_t* next;

  memset(&ss, 0, sizeof(server_state_t));

  arr[0] = makebq(3);
  arr[1] = makebq(4);
  arr[2] = makebq(1);
  arr[3] = makebq(2);
  arr[4] = makebq(5);
  arr[5] = makebq(9);
  arr[6] = makebq(6);
  arr[7] = makebq(7);
  arr[8] = makebq(8);
  arr[9] = makebq(0);
  arr[10] = makebq(7);
  arr[11] = makebq(7);
  arr[12] = makebq(7);
  arr[13] = makebq(7);

  for( i = 0; i < 14; i++ ) {
    block_query_t* q = &arr[i];
    if( DEBUG ) printf("Insert query %i %p row %i\n", i, q, q->r.row_in_block);
    rb_insert_leaf_entry(&ss, &q->leaf);
  }

  if( DEBUG > 3 ) {
    printf(" After insert:\n");
    print_leaf_queries(&ss);
  }

  // go through the block queries.
  if( DEBUG > 2 ) printf("Trying loop\n");
  for( node = rb_min_leaf_entry(&ss);
       node;
       node = rb_next_leaf_entry(&ss, node)
     ) {
    block_query_t* q = (block_query_t*) node;
    if( DEBUG ) printf("Got query row %i\n", q->r.row_in_block);
  }

  if( DEBUG > 2 ) printf("Trying delete loop\n");
  for( node = rb_min_leaf_entry(&ss);
       node;
       node = next
     ) {
    next = rb_next_leaf_entry(&ss, node);
    block_query_t* q = (block_query_t*) node;
    if( DEBUG ) printf("Got query row %i\n", q->r.row_in_block);
    rb_delete_leaf_entry(&ss, & q->leaf);
  }

  assert( ! rb_min_leaf_entry(&ss) );

  arr[0] = makebq(9);
  arr[1] = makebq(8);
  arr[2] = makebq(7);
  arr[3] = makebq(6);
  arr[4] = makebq(5);
  arr[5] = makebq(4);
  arr[6] = makebq(3);
  arr[7] = makebq(2);
  arr[8] = makebq(1);
  arr[9] = makebq(0);
  arr[10] = makebq(2);
  arr[11] = makebq(2);
  arr[12] = makebq(2);
  arr[13] = makebq(2);

  for( i = 0; i < 14; i++ ) {
    block_query_t* q = &arr[i];
    if( DEBUG ) printf("Insert query %i %p row %i\n", i, q, q->r.row_in_block);
    rb_insert_leaf_entry(&ss, &q->leaf);
  }

  if( DEBUG > 2 ) printf("Trying delete-insert loop\n");
  for( node = rb_min_leaf_entry(&ss);
       node;
       node = next
     ) {
    next = rb_next_leaf_entry(&ss, node);
    block_query_t* q = (block_query_t*) node;
    if( DEBUG ) printf("Got query %p row %i\n", q, q->r.row_in_block);
    rb_delete_leaf_entry(&ss, &q->leaf);
    if( DEBUG > 3 ) {
      printf(" After delete:\n");
      print_leaf_queries(&ss);
    }
    if( q->r.row_in_block < 3 ) {
      q->r.row_in_block += 2;
      if( DEBUG > 3 ) printf("  Adding query %p row %i\n", q, q->r.row_in_block);
      rb_insert_leaf_entry(&ss, &q->leaf);
    }
    if( DEBUG > 3 ) {
      printf(" After delete-add:\n");
      print_leaf_queries(&ss);
    }
  }

  assert( ! rb_min_leaf_entry(&ss) );

}

void test_parse_param(void)
{
  index_block_param_t param;
  error_t err;

  memset(&param, 0, sizeof(index_block_param_t));
  set_default_param(&param);
  err = parse_param(&param, "chunk_size=2");
  assert(!err);
  assert(param.chunk_size == 2 );
  memset(&param, 0, sizeof(index_block_param_t));
  set_default_param(&param);
  err = parse_param(&param, "chunk_size=2 bucket_size=4");
  assert(!err);
  assert(param.chunk_size == 2);
  assert(param.b_size == 4);
  memset(&param, 0, sizeof(index_block_param_t));
  set_default_param(&param);
  err = parse_param(&param, "block_size=20 bucket_size=10 chunk_size=5");
  assert(!err);
  assert(param.block_size == 20 );
  assert(param.b_size == 10);
  assert(param.chunk_size == 5);
}

int main(int argc, char** argv)
{
  error_t err;
  err = mkdir_if_needed(TEST_PATH);
  die_if_err(err);

  test_parse_param();
  test_rb_block_queries();
  test_write_occs();
  test_construct();
  test_indices();

  printf("All index tests PASSED\n");
  return 0;
}
