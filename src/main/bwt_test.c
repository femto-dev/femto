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

  femto/src/main/bwt_test.c
*/
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include <sys/stat.h> //mkdir
#include <unistd.h> //unlink,rmdir

#include "timing.h"

#include "server.h"

#include "bwt_writer.h"
#include "bwt_creator.c"
#include "bwt_qsufsort.c"

char* TEST_PATH_DIR = "./test_indexes/";
char* TEST_PATH_INFO = "./test_indexes/info";
char* TEST_PATH_BWT = "./test_indexes/bwt";

void my_memcmp(alpha_t* expect, prepared_alpha_t* b, unsigned int expect_bytes, unsigned int len)
{
  unsigned int i;
  int got;

  assert( expect_bytes / sizeof(prepared_alpha_t) <= len );
  len = expect_bytes / sizeof(prepared_alpha_t);

  for( i = 0; i < len; i++ ) {
    got = ntoh_palpha(b[i]);
    assert(expect[i] == got);
  }
}


void ez_print_char(int ch)
{
  if( ch == -1 ) printf("-");
  else {
    if( ch < CHARACTER_OFFSET ) printf("%x", ch);
    else {
      ch = ch - CHARACTER_OFFSET;
      if( isprint(ch) ) printf("%c", ch);
      else printf("%x", ch);
    }
  }
}

void ez_print_alpha(int ch)
{
  if( ch == -1 ) printf("-");
  else {
    if( ch < CHARACTER_OFFSET ) printf("<%x>", ch);
    else {
      ch = ch - CHARACTER_OFFSET;
      if( isprint(ch) ) printf("%c", ch);
      else printf("\\x%x", ch);
    }
  }
}

int get_L(int len, int* L, int64_t num_docs, int64_t* doc_ends, prepared_alpha_t* prepared, int* F)
{
  int i, j;
  error_t err;
  int doc;
  int ch, fch;
  qsuf_int* offsets;
  int front;
  int ret = 0;
  int64_t doc_num;
  int cur_len;
  int doc_start;
  int print = DEBUG && (DEBUG > 2 || len < 60);


    err = block_sort(&offsets, len, prepared, NULL);
    die_if_err ( err );
    assert( offsets );

    for( i = 0; i < len; i++ ) {
      // print out the M row.
      if( print ) {
        printf("%03i ", ret);
        front = get_F_char_from_offsets(i, offsets, prepared);
        ez_print_alpha(front);
        printf(" ");
      }

      // figure out which document...
      for( doc = 0; doc < num_docs; doc++ ) {
        if( offsets[i] < doc_ends[doc] ) break;
      }
      if( doc == 0 ) doc_start = 0;
      else doc_start = doc_ends[doc-1];
      cur_len = doc_ends[doc] - doc_start;

      for( j = 0; j < cur_len; j++ ) {
        int doc_offset;
        doc_offset = (offsets[i] + j - doc_start) % cur_len;
        ch = ntoh_palpha(prepared[doc_offset + doc_start]);
        if( print ) ez_print_char(ch);

      }
      if( print ) {
        printf(" ");
        doc_num = -1;
        ez_print_alpha(get_L_char_from_offsets(i, offsets, prepared));
        printf(" doc %i", doc);
        printf("\n");
      }
      ch = get_L_char_from_offsets(i, offsets, prepared);
      fch = get_F_char_from_offsets(i, offsets, prepared);

      if( ch >= 0 ) {
        if( F ) F[ret] = fch;
        L[ret++] = ch;
      }
    }

  free(offsets);

  SAFETY_CHECK;
  return ret;
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


// stack variables that were made global.
int tg_C[ALPHA_SIZE];
int64_t tg_occs[ALPHA_SIZE];
unsigned int tg_block_occs[ALPHA_SIZE];
int tg_occsRead[ALPHA_SIZE];
#define TG_DATA_SIZE 4096
uchar tg_data[TG_DATA_SIZE];
uchar tg_inUse[ALPHA_SIZE];
qsuf_int tg_offsets[1024];
#define TG_MTFV_SIZE 1024
uint16_t tg_mtfv[TG_MTFV_SIZE];
uchar tg_charsInUse[ALPHA_SIZE];
int tg_realL[1024]; 
uchar tg_text[1024];
prepared_alpha_t tg_prepared[1024];
alpha_t tg_unseqToSeq[ALPHA_SIZE];
alpha_t tg_yy[ALPHA_SIZE];

void test_prepare_documents(void)
{
  // test prepare_documents
  int i,j;
  char* texts[] = {"abra_", "dabra_", "blarghz_"};
  char* header[3] = {"#", "!!", "?"};
  unsigned char** headers[3];
  int64_t header_lengths[3];
  int text_lengths[3];
  //char* urls[] = {"0", "1", "2"};
  error_t err;
  prepared_text_t p;
  int ends[3];


#define EOF2 ESCAPE_CODE_EOF
#define SEF2 ESCAPE_CODE_SEOF
#define SOH2 ESCAPE_CODE_SOH
#define EOH2 ESCAPE_CODE_EOH
#define DIV2 ESCAPE_CODE_DIVIDER
#define CH(xxx) (CHARACTER_OFFSET+xxx)


  err = init_prepared_text(&p, TEST_PATH_INFO);
  assert(!err);

  for( i = 0; i < 3; i++ ) {
    header_lengths[i] = strlen(header[i]);
    headers[i] = (unsigned char**) &header[i];
    text_lengths[i] = strlen(texts[i]);
  }

  err = count_file(&p, text_lengths[0],
                   0, NULL,
                   0, NULL);
  die_if_err(err);


  err = append_file_mem(&p, text_lengths[0], (unsigned char*) texts[0], 
                    0, NULL, NULL,
                    0, NULL);
  die_if_err(err);

  for( i = 0; i < 1; i++ ) {
    int64_t doc_end;
    err = prepared_doc_end(&p, i, &doc_end);
    ends[i] = doc_end;
  }

  { alpha_t expect[] = 
    { 
        //STX2, // STX and  MARK
        CH('a'), CH('b'), CH('r'), CH('a'), CH('_'), // original text
        SEF2, // EOF
        //CH(0), CH(0), CH(0), CH(0), CH(0), CH(0), CH(0), CH(0), 
        //EOF2, // EOF
    };
    my_memcmp(expect, (prepared_alpha_t*) p.buf.data, sizeof(expect), ends[0]);
  }
  free_prepared_text(&p);

  err = init_prepared_text(&p, TEST_PATH_INFO);
  die_if_err(err);

  err = count_file(&p, text_lengths[0],
                    1, &header_lengths[0],
                    0, NULL);
  die_if_err(err);

  err = append_file_mem(&p, text_lengths[0], (unsigned char*) texts[0], 
                    1, &header_lengths[0], headers[0],
                    0, NULL);
  die_if_err(err);

  for( i = 0; i < 1; i++ ) {
    int64_t doc_end;
    err = prepared_doc_end(&p, i, &doc_end);
    ends[i] = doc_end;
  }

  { alpha_t expect[] = 
    {
        //STX2, // STX MARK
        CH('a'), CH('b'), CH('r'), CH('a'), CH('_'), // original text
        SOH2, // MARK
        CH(0), // SOH custom-header
        CH('#'), // custom header data
        EOH2, CH(0), // EOH custom-header
        SEF2, // EOF // 24
        //CH(0), CH(0), CH(0), CH(0), CH(0), CH(0), CH(0), CH(0), 
        //EOF2, // EOF
    };
    my_memcmp(expect, (prepared_alpha_t*) p.buf.data, sizeof(expect), ends[0]);
  }
  free_prepared_text(&p);

  err = init_prepared_text(&p, TEST_PATH_INFO);
  die_if_err(err);
  for( int doc = 0; doc < 3; doc++ ) {
    err = count_file(&p, text_lengths[doc],
                      1, &header_lengths[doc],
                      0, NULL);
    die_if_err(err);
  }
  for( int doc = 0; doc < 3; doc++ ) {
    err = append_file_mem(&p, text_lengths[doc], (unsigned char*) texts[doc], 
                      1, &header_lengths[doc], headers[doc],
                      0, NULL);
    die_if_err(err);
  }

  for( i = 0; i < 3; i++ ) {
    int64_t doc_end;
    err = prepared_doc_end(&p, i, &doc_end);
    ends[i] = doc_end;
  }

  { prepared_alpha_t expect[] = 
    {
        // document 0:
        //STX2, // STX MARK
        CH('a'), CH('b'), CH('r'), CH('a'), CH('_'), // original text
        SOH2, // MARK
        CH(0), // SOH custom-header
        CH('#'), // custom header data
        EOH2, CH(0), // EOH custom-header
        SEF2, // EOF
        //CH(0), CH(0), CH(0), CH(0), CH(0), CH(0), CH(0), CH(0),
        //EOF2, // EOF
    };
    my_memcmp(expect, (prepared_alpha_t*) p.buf.data, sizeof(expect), ends[0]);
  }
  { alpha_t expect[] =
    {
        // document 1:
        //STX2, // STX MARK
        CH('d'), CH('a'), CH('b'), CH('r'), CH('a'), CH('_'), // original text
        SOH2, CH(0), // SOH custom-header
        CH('!'), CH('!'), // custom header data
        EOH2, CH(0), // EOH custom-header
        SEF2, // EOF
        //CH(0), CH(0), CH(0), CH(0), CH(0), CH(0), CH(0), CH(1),
        //EOF2, // EOF
    };
    my_memcmp(expect, &((prepared_alpha_t*) p.buf.data)[ends[0]],
        sizeof(expect),
        ends[1] - ends[0]);
  }
  {
    alpha_t expect[] = 
    {
        // document 2:
        //STX2, // STX
        CH('b'), CH('l'), CH('a'), CH('r'),
        CH('g'), CH('h'), CH('z'), CH('_'), // original text
        SOH2, CH(0), // SOH custom-header
        CH('?'), // custom header data
        EOH2, CH(0), // EOH custom-header
        SEF2, // EOF
        //CH(0), CH(0), CH(0), CH(0), CH(0), CH(0), CH(0), CH(2),
        //EOF2, // EOF
    };
    my_memcmp(expect, &((prepared_alpha_t*) p.buf.data)[ends[1]],
        sizeof(expect), 
        ends[2] - ends[1]);
  }

  free_prepared_text(&p);

  // test one with all the characters.
  for( i = 0; i < 256; i++ ) {
    tg_text[i] = i;
  }

  texts[0] = (char*) tg_text;
  text_lengths[0] = 256;


  {
    prepared_alpha_t* output = NULL;
    err = init_prepared_text(&p, TEST_PATH_INFO);
    die_if_err(err);
    err = count_file(&p, text_lengths[0],
                      1, &header_lengths[0],
                      0, NULL);
    die_if_err(err);
    err = append_file_mem(&p, text_lengths[0], (unsigned char*) texts[0], 
                      1, &header_lengths[0], headers[0],
                      0, NULL);
    die_if_err(err);

    for( i = 0; i < 1; i++ ) {
      int64_t doc_end;
      err = prepared_doc_end(&p, i, &doc_end);
      ends[i] = doc_end;
    }

    output = (prepared_alpha_t*) p.buf.data;

    j = 0;
    //assert( ntoh_palpha(output[0]) == STX2 ); // start of text
    //j = 1;
    i = 0;
    for( ; i < 256; j++ ) {
      // the output should either be:
      // our input character.
      assert( CH(i) == ntoh_palpha(output[j]) );
      i++;
    }

    free_prepared_text(&p);

  }

}

void test_blocksort(void)
{
  // test blocksort and mtf-rle encode
  int64_t text_length = 16;
  prepared_alpha_t mytext[] = {12,13,13,13, 12,12,12,13, 12,12,13,13, 12,13,12,2};
  int expect[] = {15, 14, 4, 5, 8, 12, 6, 9, 0, 13, 3, 7, 11, 2, 10, 1 };
  prepared_alpha_t L[] =      {12,13,13,12,13,13,12,12,2,12,13,12,13,13,12,12};
//  prepared_alpha_t L8[] =     {2,3,3,2,1,3,2,2,3,2,3,2,3,3,2,2};
//  prepared_alpha_t L4[] =     {2,3,3,2,3,1,2,2,3,2,3,2,3,3,2,2};
//  prepared_alpha_t L2[] =     {2,1,2,2,2,3,3,2,3,2,3,2,3,3,3,2};
  //int64_t doc_ends_8[] = {8,16};
  //int64_t doc_ends_4[] = {4,8,12,16};
  //int64_t doc_ends_2[] = {2,4,6,8,10,12,14,16};
  int i;
  qsuf_int* offsets;
  error_t err;

  printf("Testing blocksort\n");
  for( i = 0; i < text_length; i++ ) {
    tg_prepared[i] = hton_palpha(mytext[i]);
  }
  tg_prepared[text_length] = 0;
  tg_prepared[text_length+1] = 0;
  tg_prepared[text_length+2] = 0;
  offsets = NULL;
  err = block_sort(&offsets, text_length, tg_prepared, NULL);
  assert ( ! err );
  assert(offsets);
  memcpy(tg_offsets, offsets, text_length * sizeof(qsuf_int));
  free(offsets);

  for( i = 0; i < text_length; i++ ) {
    assert( tg_offsets[i] == expect[i] );
  }

  for( i = 0; i < text_length; i++ ) {
    assert( L[i] == get_L_char_from_offsets(i, tg_offsets, tg_prepared));
    tg_mtfv[i] = L[i];
  }

  {
    // check that get_L works right.
    get_L(text_length, tg_realL, 1, &text_length, tg_prepared, NULL);
    for( i = 0; i < text_length; i++ ) {
      assert( L[i] == tg_realL[i]);
    }
  }

  {
    // try another test.
    prepared_alpha_t mytext2[] = {CHARACTER_OFFSET + 'a',
                                  CHARACTER_OFFSET + 'b',
                                  CHARACTER_OFFSET + 'c',
                                  ESCAPE_OFFSET + 3,
                                  CHARACTER_OFFSET + 0,
                                  CHARACTER_OFFSET + 0,
                                  CHARACTER_OFFSET + 0,
                                  CHARACTER_OFFSET + 0,
                                  ESCAPE_OFFSET + 3,
                                  ESCAPE_OFFSET + 2};
    int expect2[] = {9,8,3,7,6,5,4,0,1,2};
    int secondL[] = {ESCAPE_OFFSET + 3,
                     CHARACTER_OFFSET + 0,
                     CHARACTER_OFFSET + 'c',
                     CHARACTER_OFFSET + 0,
                     CHARACTER_OFFSET + 0,
                     CHARACTER_OFFSET + 0,
                     ESCAPE_OFFSET + 3,
                     ESCAPE_OFFSET + 2,
                     CHARACTER_OFFSET + 'a',
                     CHARACTER_OFFSET + 'b'};

    int64_t len2 = 10;

    for( i = 0; i < len2; i++ ) {
      tg_prepared[i] = hton_palpha(mytext2[i]);
    }

    tg_prepared[len2] = 0;
    tg_prepared[len2+1] = 0;
    tg_prepared[len2+2] = 0;
    offsets = NULL;
    err = block_sort(&offsets, len2, tg_prepared, NULL);
    assert ( ! err );
    assert(offsets);
    memset(tg_offsets, 0, sizeof(tg_offsets));
    memcpy(tg_offsets, offsets, len2 * sizeof(qsuf_int));
    free(offsets);


    // now.. check that the offsets are correct.
    for( i = 0; i < len2; i++ ) {
      assert(tg_offsets[i] == expect2[i]);
    }

    // check that the L column we get from getL is correct.
    get_L(len2, tg_realL, 1, &len2, tg_prepared, NULL );
    for( i = 0; i < len2; i++ ) {
      assert(tg_realL[i] == secondL[i]);
    }


  }
}

int64_t fread_int64(FILE* f)
{
  int64_t n;
  int read;
  read = fread(&n, 8, 1, f);
  assert( read == 1 );
  return ntoh_64(n);
}
int32_t fread_int32(FILE* f)
{
  int32_t n;
  int read;
  read = fread(&n, 4, 1, f);
  assert( read == 1 );
  return ntoh_32(n);
}


void check_encoding(int64_t number_of_characters_ssort, int64_t number_of_documents, int64_t document_ends_ssort[], alpha_t L[], int64_t off[], int entries_bytes, unsigned char entries[], int64_t mark_period) 
{
  bwt_writer_t bwt;
  bwt_document_info_writer_t info;
  FILE* f = fopen(TEST_PATH_BWT, "w+");
  error_t err;
  int i;
  int64_t number_of_characters = number_of_characters_ssort;
  unsigned char obits = num_bits64(number_of_characters);

  memset(tg_occs, 0, sizeof(tg_occs));
  for( i = 0; i < number_of_characters; i++ ) {
    tg_occs[L[i]]++; // update # occurences for use in checking.
  }

  err = bwt_begin_write_ssort(&bwt, f, &info, TEST_PATH_INFO, number_of_characters_ssort, number_of_documents, document_ends_ssort, ALPHA_SIZE, mark_period, ALPHA_SIZE_BITS, obits, NULL, NULL);
  assert(!err);

  for( i = 0; i < number_of_characters; i++ ) {
    err = bwt_append(&bwt, L[i], off[i]);
    assert(!err);
  }

  err = bwt_finish_write(&bwt);
  assert(!err);

  rewind(f);

  // what should f contain?
  assert( BWT_START == fread_int32(f) );
  assert( BWT_VERSION == fread_int32(f) );
  assert( number_of_characters == fread_int64(f) );
  assert( number_of_documents == fread_int64(f) );
  assert( ALPHA_SIZE == fread_int64(f) );
  assert( mark_period == fread_int64(f) );
  assert( ALPHA_SIZE_BITS == fgetc(f) );
  assert( obits == fgetc(f) );

  {
    unsigned char* buf = (unsigned char*) malloc(entries_bytes);

    assert(buf);
    assert( 1 == fread( buf, entries_bytes, 1, f ) );

    // now we're ready to check the encoding.
    for( i = 0; i < entries_bytes; i++ ) {
      assert( entries[i] == buf[i]);
    }

    free(buf);
  }

  fclose(f);
}

void test_encoder(void)
{
  int64_t number_of_documents = 2;
  int64_t number_of_characters = 5; //number_of_documents + 5;
  int64_t document_ends[] = {2,5};
  alpha_t L[] = {2, 2, 3, 2, 3};
  int64_t off[] = {-1, -1, -1, -1, -1};
  int64_t off2[] = {3, 2, -1, -1, -1};
  int64_t mark_period = 20;

  unsigned char entries[] = {0x01, 0x10, 0x0d, 0x01, 0x20, 0x37, 0xfe, 0x80};
  unsigned char entries2[] = {0x01, 0x58, 0x0a, 0x80, 0x68, 0x09, 0x01, 0xbf, 0xf4};

  check_encoding(number_of_characters, number_of_documents, document_ends, L, off, sizeof(entries), entries, 0);

  check_encoding(number_of_characters, number_of_documents, document_ends, L, off, sizeof(entries), entries, mark_period);

  check_encoding(number_of_characters, number_of_documents, document_ends, L, off2, sizeof(entries2), entries2, mark_period);

  
}


void test_buffer_gamma(void)
{
  buffer_t buf;
  //int seq[] = {1, 2, 17, 8, 4, 3, 90, 1, 6};
  int seq[] = {1, 2};
  int seqlen = sizeof(seq) / 4;
  int i;
  int read;

  buf = build_buffer(1000, (unsigned char*) malloc(1000));
  assert(buf.data);

  bsInitWrite(&buf);
  // try encoding and decoding some gammas.
  for( i = 0; i < seqlen; i++ ) {
    buffer_encode_gamma( &buf, seq[i] );
  }
  bsFinishWrite(&buf);

  bsInitRead(&buf);
  for( i = 0; i < seqlen; i++ ) {
    read = buffer_decode_gamma( &buf );
    assert( seq[i] == read );
  }
  bsFinishRead(&buf);

  free(buf.data);
}

int main(int argc, char** argv)
{
  // ignore errors (it might exist)
  mkdir(TEST_PATH_DIR, S_IRWXU);

  test_buffer_gamma();
  test_blocksort();
  test_prepare_documents();
  test_encoder();

  // delete the directory
  unlink(TEST_PATH_INFO);
  unlink(TEST_PATH_BWT);
  rmdir(TEST_PATH_DIR);

  printf("All bwt tests PASSED\n");
  return 0;
}
