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

  femto/src/main/merge_test.c
*/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "index.h"

#include "index_test_funcs.c"

int compare_update(const void* av, const void * bv)
{
  block_update_t* a = (block_update_t*) av;
  block_update_t* b = (block_update_t*) bv;

  if( a->dst_row_in_block < b->dst_row_in_block ) return -1;
  else if( a->dst_row_in_block > b->dst_row_in_block ) return 1;
  else {
    if( a->src_row < b->src_row ) return -1;
    else if ( a->src_row > b->src_row ) return 1;
    else return 0;
  }
}

void test_update_block(int num)
{
  update_block_t block;
  error_t err;
  block_update_t* updates = NULL;
  int last_char = 1;
  index_locator_t loc;
  int r;
  compressed_update_reader_t reader;

  printf("Testing update block with %i entries\n", num);

  loc.id = "test_index";
  // step one  -- create an index so we'll have a block
  // we can update.
  {

    int* doc_lens;
    unsigned char** docs;
    int ndocs = 10;
    index_block_param_t param;

    set_default_param(&param);

    create_random_documents(10000, ndocs, &doc_lens, &docs);


    create_index(ndocs, NULL, doc_lens, docs, loc.id, &param, "test_update_block");

    free_documents(ndocs, doc_lens, docs);

  }

  if( num > 0 ) {
    updates = malloc(num*sizeof(block_update_t));
    for( int i = 0; i < num; i++ ) {
      memset(&updates[i], 0, sizeof(block_update_t));
      r = rand();
      updates[i].dst_row_in_block = r & 0x1f0;
      if( (rand() & 0x7) == 0x0 ) {
        // 1 out of 8 times, put a character
        updates[i].ch = rand() & 0xff;
        last_char = updates[i].ch;
      } else {
        updates[i].ch = last_char;
      }
      assert(updates[i].dst_row_in_block < 0xffff);
      assert(num < 0xffff);
      updates[i].src_row = (updates[i].dst_row_in_block << 16) | ( (num - i) & 0xffff );
      if( (rand() & 0xf) == 0x0 ) {
        // 1 out of 8 times, put an offset
        updates[i].offset = rand() & 0x1ff;
      } else {
        updates[i].offset = INVALID_OFFSET;
      }
    }
  }

  err = remove_stored_block(block_id_for_type(INDEX_BLOCK_TYPE_DATA_UPDATE,0),
                            loc);
  assert ( ! err );

  err = create_update_block(loc,
                        0, // block number
                        2.0 / sizeof(update_block_entry_t),
                        10, // start_uncompressed
                        1024*1024, // max_uncompressed
                        9 // offset_size_bits
                        );
  assert ( ! err );
  // now try saving the offsets.
  err = open_update_block(&block,
                        loc,
                        0, // block number
                        1 // writeable
                        );

  for( int i = 0; i < num; i++ ) {
    err = block_update_request(&block, BLOCK_UPDATE_DATA, &updates[i]);
    assert ( ! err );
  }

  // sort the updates we have.
  qsort(updates, num, sizeof(block_update_t), compare_update);

  // make sure they're all compressed.
  err = compress_updates(&block);
  assert ( ! err );

  // now try reading them all!
  err = init_compressed_update_reader_block(&reader, &block);
  assert ( ! err );

  for( int i = 0; i < num; i++ ) {
    assert( 1 == read_compressed_update(&reader));
    assert(reader.ent.dst_row_in_block == updates[i].dst_row_in_block);
    assert(reader.ent.ch == updates[i].ch);
    assert(reader.ent.src_row == updates[i].src_row);
    assert(reader.ent.offset == updates[i].offset);
  }
  assert(0 == read_compressed_update(&reader));

  free_compressed_update_reader(&reader);

  err = close_update_block(&block);
  assert ( ! err );

  free(updates);
}

void test_merge_indexes(int src_len, int src_ndocs, int dst_len, int dst_ndocs)
{
  index_block_param_t param;
  int* src_doc_lens;
  int* dst_doc_lens;
  unsigned char** src_docs;
  unsigned char** dst_docs;
  int* out_doc_lens;
  unsigned char** out_docs;
  int i,j;
  int out_ndocs;
  char* desc = NULL;

  printf("test_merge_indexes %i %i %i %i\n", src_len, src_ndocs, dst_len, dst_ndocs);

  for( int p = 0; p < NUM_TEST_PARAMS; p++ ) {
    set_test_params(&param, &desc, p);
    printf("Testing merge with param %s\n", desc);

    create_random_documents(src_len, src_ndocs, &src_doc_lens, &src_docs);
    create_random_documents(dst_len, dst_ndocs, &dst_doc_lens, &dst_docs);

    out_doc_lens = malloc((src_ndocs+dst_ndocs)*sizeof(int));
    assert( out_doc_lens );
    out_docs = malloc((src_ndocs+dst_ndocs)*sizeof(unsigned char*));
    assert( out_docs );

    // merge will put the dst docs first, then the src docs.
    j = 0;
    for( i = 0; i < dst_ndocs; i++ ) {
      out_doc_lens[j] = dst_doc_lens[i];
      out_docs[j] = dst_docs[i];
      j++;
    }
    for( i = 0; i < src_ndocs; i++ ) {
      out_doc_lens[j] = src_doc_lens[i];
      out_docs[j] = src_docs[i];
      j++;
    }
    out_ndocs = j;

    create_index(src_ndocs, NULL, src_doc_lens, src_docs, "src", &param, "merge src");
    create_index(dst_ndocs, NULL, dst_doc_lens, dst_docs, "dst", &param, "merge dst");

    test_index("src", src_ndocs, src_doc_lens, src_docs);
    test_index("dst", dst_ndocs, dst_doc_lens, dst_docs);

    // merge the indexes!
    {
      index_merge_query_t q;
      setup_index_merge_query(&q, NULL, 
                              index_locator_for_id("src"),
                              index_locator_for_id("dst"),
                              index_locator_for_id("out"),
                              &param,
                              0.5,
                              10, 1024*1024);
      run_query((query_entry_t*) &q);
    }

    // test the merged index!
    if( DEBUG ) print_index("out");
    test_index("out", out_ndocs, out_doc_lens, out_docs);


    free_documents(src_ndocs, src_doc_lens, src_docs);
    free_documents(dst_ndocs, dst_doc_lens, dst_docs);
  }

}

int main( int argc, char** argv )
{
  char* TEST_PATH = "./test_indexes";
  set_block_storage_prefix(TEST_PATH);


  test_update_block(0);
  test_update_block(1);
  test_update_block(3);
  test_update_block(10);
  test_update_block(11);
  test_update_block(12);
  test_update_block(15);
  test_update_block(25);
  test_update_block(15);
  test_update_block(30);
  test_update_block(100);
  test_update_block(1000);
  test_update_block(10000);
  test_update_block(30000);

  test_merge_indexes(3,1,3,1);
  test_merge_indexes(3,1,4,1);
  test_merge_indexes(8,1,8,1);
  test_merge_indexes(8,1,8,1);
  test_merge_indexes(8,1,8,1);
  
  test_merge_indexes(10,2,3,1);
 
  test_merge_indexes(3,1,10,2);
  test_merge_indexes(100,8,200,5);
  

  printf("All merge tests PASSED\n");
}
