/*
  (*) 2006-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/wtree_test.c
*/
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "wtree_funcs.h"
#include "buffer_funcs.h"
#include "util.h"

int int_cmp(const void* aIn, const void* bIn)
{
  int* a = (int*) aIn;
  int* b = (int*) bIn;

  return *a - *b;
}

int bit_equals(unsigned char* a, unsigned char* b, int bit_len)
{
  int byte_idx, bit_idx;
  int a_bit, b_bit;

  for( byte_idx = 0; 1; byte_idx++ ) {
    for( bit_idx = 0; bit_idx < 8; bit_idx++ ) {
      if( bit_idx + 8* byte_idx >= bit_len ) return 1;
      a_bit = a[byte_idx] >> (7 - bit_idx);
      b_bit = b[byte_idx] >> (7 - bit_idx);
      if( a_bit != b_bit ) return 0;
    }
  }
}

void do_experiment(int argc, char** argv)
{
  int ndocs = 100000;
  int sz    = 100000000;
  //int sz = 10000*ndocs;
  int pows[] = {1000, 10000, 100000, 1000000, 0};
  int atn, atm;
  int npers[] = {1, 10, 100, 1000, 10000, sz, 0};
  int nper;
  int* map;
  int* data;
  int i, np;
  int left;
  error_t err;
  int zlen;
  unsigned char* wtree;
 
  wtree_stats_t stats;

  for( atn = 0; pows[atn]; atn++ ) {
    sz = pows[atn];
    for( atm = 0; pows[atm]; atm++ ) {
      ndocs = pows[atm];

      if( sz <= ndocs ) continue;

      printf("% 6i documents, %6i rows\n", ndocs, sz);

      map = malloc(sizeof(int)* ndocs);
      data = malloc(sizeof(int)* sz);

      assert(map);
      assert(data);

      for(i = 0; i < ndocs; i++ ) map[i] = 1+i;

      for(i = 0; i < sz; i++ ) data[i] = random() % ndocs;

      /*

    10,000,000 documents, 10,000,000 rows
       640000000 bits (80,000,000 bytes) for documents count table
       240000000 bits for document array of 10000000 entries
      2000478912 bits for wavelet tree of 10000000 entries with 1 long ascending sequences
      2000478784 bits for wavelet tree of 10000000 entries with 10 long ascending sequences
      1993033216 bits for wavelet tree of 10000000 entries with 100 long ascending sequences
      1966111872 bits for wavelet tree of 10000000 entries with 1000 long ascending sequences
      1932646848 bits for wavelet tree of 10000000 entries with 10000 long ascending sequences
      1820092224 bits for wavelet tree of 10000000 entries with 10000000 long ascending sequences

    1,000,000 documents, 10,000,000 rows
        64000000 bits (8,000,000 bytes) for documents count table
       200000000 bits for document array of 10000000 entries
       374476544 bits for wavelet tree of 10000000 entries with 1 long ascending sequences
       374476096 bits for wavelet tree of 10000000 entries with 10 long ascending sequences
       367105408 bits for wavelet tree of 10000000 entries with 100 long ascending sequences
       340811904 bits for wavelet tree of 10000000 entries with 1000 long ascending sequences
       307211136 bits for wavelet tree of 10000000 entries with 10000 long ascending sequences
       197484224 bits for wavelet tree of 10000000 entries with 10000000 long ascending sequences

    100,000 documents, 10,000,000 rows
         6400000 bits (800,000 bytes) for documents count table
       170000000 bits for document array of 10000000 entries
       175665856 bits for wavelet tree of 10000000 entries with 1 long ascending sequences
       175664128 bits for wavelet tree of 10000000 entries with 10 long ascending sequences
       167925952 bits for wavelet tree of 10000000 entries with 100 long ascending sequences
       141523520 bits for wavelet tree of 10000000 entries with 1000 long ascending sequences
       107400256 bits for wavelet tree of 10000000 entries with 10000 long ascending sequences
        20613504 bits for wavelet tree of 10000000 entries with 10000000 long ascending sequences

    10,000 documents, 10,000,000 rows
          640000 bits (80,000 bytes) for documents count table
       140000000 bits for document array of 10000000 entries
       123393344 bits for wavelet tree of 10000000 entries with 1 long ascending sequences
       123393664 bits for wavelet tree of 10000000 entries with 10 long ascending sequences
       115752640 bits for wavelet tree of 10000000 entries with 100 long ascending sequences
        88908160 bits for wavelet tree of 10000000 entries with 1000 long ascending sequences
        55424512 bits for wavelet tree of 10000000 entries with 10000 long ascending sequences
         2204544 bits for wavelet tree of 10000000 entries with 10000000 long ascending sequences

    1000 documents 10,000,000 rows
           64000 bits (8,000 bytes) for documents count table
       100000000 bits for document array of 10000000 entries
        85662080 bits for wavelet tree of 10000000 entries with 1 long ascending sequences
        85662016 bits for wavelet tree of 10000000 entries with 10 long ascending sequences
        78202176 bits for wavelet tree of 10000000 entries with 100 long ascending sequences
        51960448 bits for wavelet tree of 10000000 entries with 1000 long ascending sequences
        18625728 bits for wavelet tree of 10000000 entries with 10000 long ascending sequences
          240768 bits for wavelet tree of 10000000 entries with 10000000 long ascending sequences

    10,000 documents 100,000,000 rows
          640000 bits (80,000 bytes) for documents count table
      1400000000 bits for document array of 100000000 entries
      1218173824 bits for wavelet tree of 100000000 entries with 1 long ascending sequences
      1218174208 bits for wavelet tree of 100000000 entries with 10 long ascending sequences
      1141755456 bits for wavelet tree of 100000000 entries with 100 long ascending sequences
       873337408 bits for wavelet tree of 100000000 entries with 1000 long ascending sequences
       538698368 bits for wavelet tree of 100000000 entries with 10000 long ascending sequences
         2412032 bits for wavelet tree of 100000000 entries with 100000000 long ascending sequences

    100,000 documents 100,000,000 rows
         6400000 bits (800,000 bytes) for documents count table
      1700000000 bits for document array of 100000000 entries
      1590122432 bits for wavelet tree of 100000000 entries with 1 long ascending sequences
      1590112000 bits for wavelet tree of 100000000 entries with 10 long ascending sequences
      1512700992 bits for wavelet tree of 100000000 entries with 100 long ascending sequences
      1248657856 bits for wavelet tree of 100000000 entries with 1000 long ascending sequences
       907609664 bits for wavelet tree of 100000000 entries with 10000 long ascending sequences
        22053248 bits for wavelet tree of 100000000 entries with 100000000 long ascending sequences

    */

      printf("% 12i bits for table of %i document counts\n", ndocs * num_bits32(ndocs), ndocs);
      printf("% 12i bits for document array of %i entries\n", sz * num_bits32(ndocs), sz);

      for( np = 0; npers[np]; np++ ) {
        nper = npers[np];

        if( nper > sz ) continue;

        // Now sort in chunks of predetermined size.
        if( nper != 1 ) {
          for(i = 0; i < sz; i += nper ) {
            left = nper;
            if( i + left > sz ) left = sz - i;
            qsort(&data[i], left, sizeof(int), int_cmp);
          }
        }

        memset(&stats, 0, sizeof(stats));

        err = wtree_construct(&zlen, &wtree, ndocs, map, sz, data, &stats);
        assert(!err);

        printf("% 12li bits for wavelet tree (no nodes) of %i entries with %i long ascending sequences\n", stats.bseqs*8, sz, nper);
        printf("  % 12li rle segments bits and % 12li uncompressed segment bits\n", stats.bseq_stats.num_rle_segments*SEGMENT_BITS, stats.bseq_stats.num_unc_segments*SEGMENT_BITS);
        printf("  % 12li segment bits used; % 12li rle segments bits used and % 12li uncompressed segment bits used\n", stats.bseq_stats.segment_bits_used, stats.bseq_stats.rle_segment_bits_used, stats.bseq_stats.unc_segment_bits_used);

        free(wtree);
      }

      free(map);
      free(data);
    }
  }
}

int main(int argc, char** argv)
{
  error_t err;
  int zlen;
  unsigned int values[] = {1, 2, 3,
                           0x7f, 0x80,
                           10, 1000, 100000,
                           0x7fff, 0x8000,
                           0x7fffffff, 0};
  unsigned char *zdata;
  unsigned char test_buf[100];
  buffer_t tbuf = build_buffer(100, test_buf);

  printf("Sizeof void*=%i int=%i long=%i long long int=%i\n", (int) sizeof(void*), (int) sizeof(int), (int) sizeof(long), (int) sizeof(long long int));
 
 if( argc != 1 ) {
   do_experiment(argc, argv);
   return 0;
 }

  // check build_buffer worked correctly.
  assert(tbuf.max == 100);
  assert(tbuf.len == 0 );
  assert(tbuf.data == test_buf);
  assert(tbuf.pos == 0 );
  assert(tbuf.bsBuff == 0 );
  assert(tbuf.bsLive == 0 );

  assert(sizeof(int) == 4);

  // test bit_equals;
  assert( bit_equals((unsigned char*) "ab", (unsigned char*) "ac", 8));
  assert( bit_equals((unsigned char*) "\xca", (unsigned char*) "\xcf", 4));
  assert( ! bit_equals((unsigned char*) "\xda", (unsigned char*) "\xcf", 4));

  { // test leadz32 and leadz64.
    assert( 64 == leadz64(0) );
    assert( 63 == leadz64(1) );
    assert( 55 == leadz64(0x120) );
    assert( 1 == leadz64(0x7fffffff00000000LL) );
    assert( 0 == leadz64(0xffffffff00000000LL) );
    assert( 32 == leadz32(0) );
    assert( 31 == leadz32(1) );
    assert( 23 == leadz32(0x120) );
    assert( 1 == leadz32(0x7fffffff) );
    assert( 0 == leadz32(0xffffffff) );

    for( int i = 0; i < 63; i++ ) {
      assert( 63 - i == leadz64(1LL << i) );
    }

    assert( 0 == log2i(1) );
    assert( 1 == log2i(2) );
    assert( 1 == log2i(3) );
    assert( 2 == log2i(4) );
    assert( 2 == log2i(5) );
    assert( 2 == log2i(6) );
    assert( 2 == log2i(7) );
    assert( 3 == log2i(8) );

    // test ntoh_seg
    {
      seg_t host, net, check;
      for( int i = 0; i < SEGMENT_WORDS; i++ ) {
        host.words[0] = i+1;
      }
      net = hton_seg(host);
      check = ntoh_seg(net);
      for( int i = 0; i < SEGMENT_WORDS; i++ ) {
        assert( host.words[i] == check.words[i] );
      }
    }
  }

  { // test sort_dedup
    int arr[] = {3,4,1,1,1,0,4,5,13,56,0,15,61,61,15,15,15,2,6,6};
    int n_arr = 20;
    int expect[] = {0,1,2,3,4,5,6,13,15,56,61};
    int n_expect = 11;

    n_arr = sort_dedup(arr, n_arr, sizeof(int), int_cmp);
    assert( n_arr = n_expect );
    for(int k = 0; k < n_expect; k++ ) {
      assert( arr[k] == expect[k] );
    }
  }

  { // test encode_varbyte and decode_varbyte
    unsigned int v;

    for( int i = 0; values[i]; i++ ) {
      tbuf.len = 0;
      tbuf.len += encode_varbyte(&tbuf.data[tbuf.len], values[i]);
      tbuf.pos = 0;
      v = 0;
      tbuf.pos += decode_varbyte(&tbuf.data[tbuf.pos], &v);
      assert( tbuf.pos == tbuf.len );
      assert( v == values[i] );
    }

  }

  {
    // test gamma encode and decode.
    unsigned char bits;
    unsigned char bits2;
    unsigned int v;
    uint64_t segment;

    for( int i = 0; values[i]; i++ ) {
      bits = encode_gamma(&segment, values[i]);
      // move it to the top of segment.
      segment <<= (64 - bits);
      // run the decoder.
      bits2 = decode_gamma(segment, &v); 
      assert( bits == bits2 );
      assert( v == values[i] );
    }

    {
      unsigned int tests[] = {1,2,3,4,5,0};
      uint64_t test_gamma[] = { 0x8000000000000000LL,
                                0x4000000000000000LL,
                                0x6000000000000000LL,
                                0x2000000000000000LL,
                                0x2800000000000000LL
                               };
      for( int i = 0; tests[i]; i++ ) {
        bits = encode_gamma(&segment, tests[i]);
        // move it to the top of segment.
        segment <<= (64-bits);
        // Check that it's right.
        assert( segment == test_gamma[i] );
      }

    }
  }

  {
    // test segs_reader and segs_writer.
    struct segs_writer writer;
    struct segs_reader reader;
    // Try writing and reading bit patterns..
    for( int bit = 1; bit >=0; bit-- ) {
      for( int num_bits = 0; num_bits < SEGMENT_BITS-1; num_bits++ ) {
        start_segment(&writer);
        for( int j = 0; j < num_bits; j++ ) {
          append_segment(&writer, bit, 1);
        }
        finish_segment(&writer);

        init_segs_reader(&reader, writer.seg);
        for( int j = 0; j < num_bits; j++ ) {
          assert( bit == segs_reader_read_bit(&reader) );
        }
      }
    }

    // Try writing and reading gamma-encoded numbers.
    int nums[] = {2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 100, 1000};
    int num_nums = sizeof(nums)/sizeof(int);

    for( int i = 0; i < num_nums; i++ ) {
      int num = nums[i];
      int num_stored = 0;
      start_segment(&writer);
      while(1) {
        uint64_t encoded;
        int encoded_bits;
        encoded_bits = encode_gamma(&encoded, num);
        if( ! has_room(&writer, encoded_bits) ) break;
        append_segment(&writer, encoded, encoded_bits);
        num_stored++;
      }
      finish_segment(&writer);

      init_segs_reader(&reader, writer.seg);
      for( int j = 0; j < num_stored; j++ ) {
        int got = segs_reader_read_gamma(&reader);
        assert( num == got );
      }
    }
  }

  { // test bseq_construct 
    unsigned int test_len[] =  {1,2,3,4,5,0};
   
    printf("testing bseq_construct: single runs\n");
    for( unsigned int bit = 0; bit < 2; bit++ ) {
      for(int i = 0; test_len[i]; i++ ) {
        // test gamma-encoding the section.
        // first, put in test_in a run of that many 0s or 1s.
        tbuf.len = 0;
        bsInitWrite(&tbuf);
        for( unsigned int j = 0; j < test_len[i]; j++) {
          bsW24( &tbuf, 1, bit ); // a run of test_len bits.
        }
        bsFinishWrite(&tbuf);

        zlen = 0;
        zdata = NULL;
        err = bseq_construct(&zlen, &zdata, test_len[i], tbuf.data, NULL );
        assert(!err);

        {
          struct segs_reader reader;
          int use_rle;
          int got_bit;
          int got_value;

          init_segs_reader(&reader, bseq_segment(zdata, 0) );

          use_rle = segs_reader_read_bit(&reader);
          if( use_rle ) {
            got_bit = segs_reader_read_bit(&reader);
            got_value = segs_reader_read_gamma(&reader);
            assert( got_value == test_len[i] );
          }
        }

        {
          // check that the segment sum is right.
          int k;
          unsigned int sum_0, sum_1;
          const unsigned char* S = bseq_S_section(zdata, 0);
          k = 0;
          k += decode_varbyte(&S[k], &sum_0);
          k += decode_varbyte(&S[k], &sum_1);

          if( bit ) {
            assert( sum_1 == test_len[i] );
            assert( sum_0 == 0 );
          } else {
            assert( sum_0 == test_len[i] );
            assert( sum_1 == 0 );
          }
        }
        free(zdata);
      }    
    }

    printf("testing bseq_construct and bseq_rank\n");
    {
      char* str1 = "abracadabradabrabadrafunzobomsemesaoedasamba";
      char* str2 = "1209phrlaocuhk934hdytn;qbjksrathoe09u8f34yhboenith[9348frrtohiu09l83f4598donutbqkbv;qwkh/cgy[938f4y098dintohei0987320cg6doituvawuarlgfpu0[367499g,dyrcg5d609ldenhitdbnetibvwkbxqlrcdo098f34nt5ds,hoeu098f34ynthduk0g3bi0d3ichdeao09ufaoe0u";
      int rand_data_len = 128*1024;
      unsigned char* rand_data = (unsigned char*) malloc(rand_data_len);
      unsigned char* rand_a_z = (unsigned char*) malloc(rand_data_len);
      unsigned char* pat0 = (unsigned char*) malloc(rand_data_len);
      unsigned char* pat1 = (unsigned char*) malloc(rand_data_len);
      unsigned char* pat01 = (unsigned char*) malloc(rand_data_len);
      unsigned char* pat0001 = (unsigned char*) malloc(rand_data_len);
      unsigned char* data[] = {(unsigned char*) "\x61\x7e\x33\x33\x33\x33\x33\x33\x33\x33",
                               (unsigned char*) "\x10\x20\x30\x40",
                               (unsigned char*) "\x11\x11\x21\x13\x31\x14\x64\x11\x5a\xa5\x10",
                               (unsigned char*) str1,
                               (unsigned char*) str2,
                               (unsigned char*) "\x89\xb9\xf2\x8d\x74\xfb\xe3\x0e\x73\xfc\xf5\xee\x21\xc8\x2c\x6e\xe5\xbc\x9c\x5a\xb3\x43\x58\xb9\x9f\xb9\xa1\x4d\x7c\x6d\xab\x05\x26\x9d\x93\x9b\x98\x76\xa9\x0b\x72\x9e\xf9\x93\x66\x25\x01\x4c\xe2\x9d\xa6\x95\xe0\xff\x4e\x7f\xb8\xef\xcc\x34\x5d\x77\x3a\x83\x14\xcd\x1e\xad\x43\xc7\xb8\xb5\x66\xb2\x48\xcc\xd7\x49\x18\xb9\xe6\xbf\x4f\xc7\xbe\x9d\x46\x76\x8d\x13\xaa\xea\x8a\xe4\x6d\x9f\xb1\x8c\x4c\xf4\x53\x04\xa9\xb9\xb6\xf1\x86\x8e\x3a\x9e",
                               rand_data,
                               rand_a_z,
                               pat0,
                               pat1,
                               pat01,
                               pat0001,
                               NULL};
      char* names[] = {"short binary",
                       "short binary 2",
                       "short binary 3",
                       "short ascii 1",
                       "short ascii 2",
                       "longer binary",
                       "random",
                       "random a-z",
                       "all zero",
                       "all one",
                       "all 0x55",
                       "all 0x11",
                       NULL};

      int databits[] = {80,
                        32,
                        84,
                        8*strlen(str1),
                        8*strlen(str2),
                        8*110,
                        8*rand_data_len,
                        8*rand_data_len,
                        8*rand_data_len,
                        8*rand_data_len,
                        8*rand_data_len,
                        8*rand_data_len,
                        0};
      char* typenames[] = {"no rle",
                           "automatic choice",
                           "rle",
                           NULL};
      int occs[2];

      // generate the random data.
      for( int k = 0; k < rand_data_len; k++ ) {
        rand_data[k] = rand() & 0xff;
      }
      for( int k = 0; k < rand_data_len; k++ ) {
        rand_a_z[k] = 'a' + ((rand() & 0xff) % 26);
      }

      for( int k = 0; k < rand_data_len; k++ ) {
        pat0[k] = 0;
        pat1[k] = 0xff;
        pat01[k] = 0x55;
        pat0001[k] = 0x11;
      }

      /*
      for( int k = 0; k < rand_data_len; k++ ) {
        printf("\\x%02x", rand_data[k]);
      }
      printf("\n");
      */

      for( int i = 0; data[i]; i++ ) {
        for( int type = -1; type <= 1; type++ ) {
          bseq_stats_t stats;

          printf("Testing sequence %i '%s' type %s\n", i, names[i], typenames[1+type]);
          zlen = 0;
          zdata = NULL;

          memset(&stats, 0, sizeof(bseq_stats_t));

          err = bseq_construct_forcetype(&zlen, &zdata, databits[i], data[i], &stats, type);
          assert( ! err );
          printf(" len=%i enc_len =%i (%f %%) segs %li seg_bits %li seg_bits_used %li\n", CEILDIV(databits[i],8), zlen, 100.0 * 8.0 * (double) zlen / (double) databits[i], stats.segments, stats.segment_bits, stats.segment_bits_used);

          occs[0] = occs[1] = 0;
          // check that bseq_rank works right.
          for( int byte = 0; byte < ceildiv8(databits[i]); byte++ ) {
            // extract bit j and check that it's right.
            for( int bit = 0; bit < 8; bit++ ) {
              bseq_query_t bq;
              int v;
              int idx = 8*byte + bit;
              if( idx >= databits[i] ) break;

              v = data[i][byte];
              v >>= (7 - bit);
              v &= 0x1;
              occs[v]++;
              // check that v and occs are returned by bseq_rank
              //printf("Checking index %i\n", idx);
              bq.index = idx+1; // counting from 1.
              bseq_rank(zdata, &bq);
              //printf("Checking idx %i bit %i (%i,%i) bseq is %i (%i,%i)\n",
              //       idx, v, occs[0], occs[1], r.bit, r.occs[0], r.occs[1]); 
              if( bq.bit != v || bq.occs[0] != occs[0] || bq.occs[1] != occs[1]) {
                printf("Failed check at index %i\n", idx);
              }
              assert( bq.bit == v );
              assert( bq.occs[0] == occs[0] );
              assert( bq.occs[1] == occs[1] );

              // check bseq_select
              bq.bit = v;
              bq.occs[bq.bit] = occs[v];
              bseq_select(zdata, &bq);
              assert(bq.occs[0] == occs[0]);
              assert(bq.occs[1] == occs[1]);
              assert(bq.index == occs[0] + occs[1]);

            }
          }

          free(zdata);
        }
      }
      free(rand_data);
      free(rand_a_z);
      free(pat0);
      free(pat1);
      free(pat01);
      free(pat0001);

    }
  }

  printf("Testing wtree simple example\n");
  {
    // try constructing a wavelet tree.
    int datalen = 14;
    int data[] = {1,20,1,20,20,20,21,20,20,22,22,21,22,1};
    int alpha_size = 40;
    int leaf[256];
    /*
    unsigned char* expect[] = {(unsigned char*) "\x1a\x89",
                               (unsigned char*) "\x72\xc0",
                               (unsigned char*) "\x56"};
    int expect_bitlens[] = {16, 10, 7};
    */
    int num_internal;
    int num_nodes;
    int i;
    error_t err;
    int zlen;
    unsigned char* wtree;
   
    num_internal = 3;
    i = num_internal + 1;
    memset(leaf, 0, alpha_size*sizeof(int));
    leaf[1] = i++;
    leaf[20] = i++;
    leaf[21] = i++;
    leaf[22] = i++;
    num_nodes = i;

    err = wtree_construct(&zlen, &wtree, 23, leaf, datalen, data, NULL);
    assert( ! err );

    // check that we got the right thing from our wavelet tree!
    /*for( int k = 1; k < num_internal + 1; k++ ) {
      unsigned char* bseq;
      unsigned char* D_data;

      // grab the right bseq for internal node k.
      bseq = wtree_bseq(wtree, k);
      D_data = (unsigned char*) bseq_D_data(bseq);
      
      assert( bit_equals(D_data, expect[k-1], expect_bitlens[k-1]) );

    }*/

    // check some rank/select queries.
    {
      wtree_query_t q;

      q.index = 1;
      q.leaf = -1;
      q.count = -1;
      err = wtree_rank(wtree, &q);
      assert( ! err );
      assert( q.leaf == leaf[1] );
      assert( q.count == 1);

      q.index = 2;
      q.leaf = -1;
      q.count = -1;
      err = wtree_rank(wtree, &q);
      assert( ! err );
      assert( q.leaf == leaf[20] );
      assert( q.count == 1);

      q.index = 3;
      q.leaf = -1;
      q.count = -1;
      err = wtree_rank(wtree, &q);
      assert( ! err );
      assert( q.leaf == leaf[1] );
      assert( q.count == 2);

      q.index = 8;
      q.leaf = -1;
      q.count = -1;
      err = wtree_rank(wtree, &q);
      assert( ! err );
      assert( q.leaf == leaf[20] );
      assert( q.count == 5);

      // select queries
      q.index = -1;
      q.leaf = leaf[1];
      q.count = 1;
      err = wtree_select(wtree, &q);
      assert( ! err );
      assert( q.index == 1 );

      q.index = -1;
      q.leaf = leaf[20];
      q.count = 2;
      err = wtree_select(wtree, &q);
      assert( ! err );
      assert( q.index == 4 );

      q.index = -1;
      q.leaf = leaf[21];
      q.count = 1;
      err = wtree_select(wtree, &q);
      assert( ! err );
      assert( q.index == 7 );

      q.index = -1;
      q.leaf = leaf[21];
      q.count = 2;
      err = wtree_select(wtree, &q);
      assert( ! err );
      assert( q.index == 12 );

      // try a occs queries
      q.index = 6;
      q.leaf = leaf[1];
      q.count = -1;
      err = wtree_occs(wtree, &q);
      assert( ! err );
      assert( q.count == 2 );

      q.index = 6;
      q.leaf = leaf[20];
      q.count = -1;
      err = wtree_occs(wtree, &q);
      assert( ! err );
      assert( q.count == 4 );

      q.index = 7;
      q.leaf = leaf[21];
      q.count = -1;
      err = wtree_occs(wtree, &q);
      assert( ! err );
      assert( q.count == 1 );

      q.index = 1;
      q.leaf = leaf[21];
      q.count = -1;
      err = wtree_occs(wtree, &q);
      assert( ! err );
      assert( q.count == 0 );

      q.index = 1;
      q.leaf = leaf[1];
      q.count = -1;
      err = wtree_occs(wtree, &q);
      assert( ! err );
      assert( q.count == 1 );

    }

    free(wtree);

  }
  printf("Testing wtree many examples\n");
  {
    char* str1 = "abracadabradabrabadrafunzobomsemesaoedasamba";
    char* str2 = "1209phrlaocuhk934hdytn;qbjksrathoe09u8f34yhboenith[9348frrtohiu09l83f4598donutbqkbv;qwkh/cgy[938f4y098dintohei0987320cg6doituvawuarlgfpu0[367499g,dyrcg5d609ldenhitdbnetibvwkbxqlrcdo098f34nt5ds,hoeu098f34ynthduk0g3bi0d3ichdeao09ufaoe0u";
    int rand_data_len = 128*1024;
    unsigned char* rand_data = (unsigned char*) malloc(rand_data_len);
    unsigned char* data[] = { (unsigned char*) str1, (unsigned char*) str2, rand_data, NULL };
    int data_len[] = { strlen(str1), strlen(str2), rand_data_len };
    int leaf[256];
    int occs[256];
    int num_internal;

    num_internal = 255; // this is 2^8 - 1
    for( int j = 0; j < 0x100; j++ ) {
      leaf[j] = 0x100 | j;
    }

    for( int k = 0; k < rand_data_len; k++ ) {
      rand_data[k] = rand() & 0xff;
    }

    for( int i = 0; data[i]; i++ ) {
      unsigned char* d = data[i];
      int* int_data;
      int len = data_len[i];
      unsigned char* wtree;
      int wtree_len;
      wtree_query_t q;
    
      printf("Testing wtree %i\n", i);
      int_data = (int*) malloc(sizeof(int) * len);
      for( int j = 0; j < len; j++ ) {
        int_data[j] = d[j];
      }

      // check d, len
      // construct the wavelet tree.
      err = wtree_construct(&wtree_len, &wtree, 256, leaf, len, int_data, NULL);
      assert( ! err );

      free(int_data);

      memset(occs, 0, sizeof(int) * 256);

      for( int j = 0; j < len; j++ ) {
        occs[d[j]]++;

        q.index = j+1;
        q.leaf = -1;
        q.count = -1;
        err = wtree_rank(wtree, &q);
        assert( ! err );
        assert( q.leaf == leaf[d[j]] );
        assert( q.count == occs[d[j]]);

        // check, for every other character, the rank query.
        if( j < 1000 ) { // only for the first 1000 to keep the test fast
          for( int k = 0; k < 0x100; k++ ) {
            q.index = j+1;
            q.leaf = leaf[k];
            q.count = -1;
            err = wtree_occs(wtree, &q);
            assert( ! err );
            assert( q.count == occs[k] );
          }
        }
        // try a random occs query.
        {
          int k = rand() & 0xff;
          q.index = j+1;
          q.leaf = leaf[k];
          q.count = -1;
          err = wtree_occs(wtree, &q);
          assert( ! err );
          assert( q.count == occs[k] );
        }

        q.index = -1;
        q.leaf = leaf[d[j]];
        q.count = occs[d[j]];
        err = wtree_select(wtree, &q);
        assert( ! err );
        assert( q.index == j+1 );

      }

      free(wtree);
    }
    free(rand_data);
  }

  printf("All wtree tests PASSED\n");
  return 0;
}
