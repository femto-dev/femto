#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>

extern "C" {
  #include "suffix_sort.h"
  #include "timing.h"
}

#include "dcx_inmem.hh"


int int_shift_cmp(const void* av, const void* bv, const void* usr)
{
  int a = * (const int*) av;
  int b = * (const int*) bv;

  // don't consider the low bit.
  a >>= 1;
  b >>= 1;

  assert(usr==(void*)0x10);
  return a - b;
}

void unit_test_suffix_sort_funs(void)
{
  assert(sizeof(sptr_t) == sizeof(usptr_t));

  assert(1 == pointer_bytes_needed_for(2));
  assert(1 == pointer_bytes_needed_for(12));
  assert(1 == pointer_bytes_needed_for(128));
  assert(2 == pointer_bytes_needed_for(256));
  assert(2 == pointer_bytes_needed_for(128*256));
  assert(3 == pointer_bytes_needed_for(256*255));
  assert(3 == pointer_bytes_needed_for(128*256*256));
  assert(4 == pointer_bytes_needed_for(0x1400000));

  { // test set_be and get_be
    unsigned char* buf = calloc(1,1024);
    sptr_t nums[] = {1, 2, -1, -2, 0, 0, 10, 11,
                     0x1234, 0xffff, 0xffffff, 
                     0x102030, 0x77665544, 1, 1, 2, 2};
    int num_nums = 16;
    sptr_t masks[9];
    sptr_t umasks[9];
    sptr_t got;
   
   
    for( int i = 0; i <= 8; i++ ) {
      usptr_t ones = 0;
      ones = ~ones; // now all ones.
      if( i == 0 ) masks[i] = 0;
      else {
        masks[i] = ones >> (8*(sizeof(sptr_t)-i)+1);
        umasks[i] = ones >> (8*(sizeof(sptr_t)-i));
      }
    }

    for( int size = 1; size <=sizeof(sptr_t); size++ ) {
      memset(buf, 0, 1024);
      //printf("size %i\n", size);
      // store all these numbers...
      for( int i = 0, j = 0; i < num_nums; i++,j+=size ) {
        sptr_t store;
        if( nums[i] >= 0 ) {
          store = nums[i] & masks[size];
        } else {
          store = (-1 ^ masks[size]) | (nums[i] & masks[size]);
        }
        set_be(buf + j, size, store);
        //printf("stored %li for %li\n", store, nums[i]);
        got = get_be(buf + j, size);
        //printf("got    %li\n", got);
        assert((store&umasks[size]) == (got&umasks[size]));
      }
      // test that we can read them all.
      for( int i = 0, j = 0; i < num_nums; i++,j+=size ) {
        sptr_t store;
        if( nums[i] >= 0 ) {
          store = nums[i] & masks[size];
        } else {
          store = (-1 ^ masks[size]) | (nums[i] & masks[size]);
        }
        got = get_be(buf + j, size);
        assert((store&umasks[size]) == (got&umasks[size]));
      }

      // check that if we store them with set_T/get_T
      // we always get positive numbers.
      for( int i = 0, j = 0; i < num_nums; i++,j+=size ) {
        set_T(buf, size, j, nums[i] & umasks[size]);
        got = get_T(buf, size, j);
        assert((nums[i] & umasks[size]) == got);
      }
    }
  }
}

#define PAD_ZEROS "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" \
                  "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" \
                  "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" \
                  "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" \
                  "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" \
                  "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" \
                  "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" \
                  "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" \
                  "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" \
                  "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"

void unit_test_dcx(void)
{
  suffix_sorting_problem_t p;
  unsigned char* texts[] = {
    (unsigned char*) "yabbadabbado" PAD_ZEROS,
    (unsigned char*) "yabbadabbad" PAD_ZEROS,
    (unsigned char*) "yabbadabbadoo" PAD_ZEROS,
    (unsigned char*) "yabbadabba" PAD_ZEROS,
    (unsigned char*) "abcdefghijklmnopqrstuvwxyz" PAD_ZEROS,
    (unsigned char*) "zyxwvutsrqponmlkjihgfedcba" PAD_ZEROS,
    (unsigned char*) "\x0\xFF\x0\x1\x2\xA9\xFF\xA9" PAD_ZEROS,
    NULL
  };
  int expects[][50] = {
    {1, 6, 4, 9, 3, 8, 2, 7, 5, 10, 11, 0},
    {6, 1, 9, 4, 8, 3, 7, 2, 10, 5, 0},
    {1, 6, 4, 9, 3, 8, 2, 7, 5, 10, 12, 11, 0},
    {9, 6, 1, 4, 8, 3, 7, 2, 5, 0},
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
    {25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
    {2, 0, 3, 4, 7, 5, 1, 6},
  };

  for( int period = 3; period < 200; period++ ) {
    error_t err;

    if( ! dcx_inmem_supports_period(period) ) continue;

    for( int csize = 1; csize <= 5; csize++ ) {
      for( int psize = 0; psize <= 5; psize++ ) {
        for( int i = 0; texts[i]; i++ ) {
          // Try DC3 on it..
          unsigned char* T;

          printf("DC%i csize=%i psize=%i i=%i\n", period, csize, psize, i);
          memset(&p, 0, sizeof(p));

          p.n = strlen((char*)texts[i]);
          T = calloc(p.n+period, csize);
          assert(T);

          // copy the characters into T.
          for( int j = 0; j < p.n; j++ ) {
            set_T(T, csize, j*csize, texts[i][j]);
            assert(texts[i][j] ==
                   get_T(T, csize, j*csize));
          }

          p.bytes_per_character = csize;
          p.bytes_per_pointer = psize;
          p.T = T;
          p.S = NULL;
          err = dcx_ssort(&p, &cover);
          die_if_err(err);

          // now we should have, in p.S, the result of the suffix sorting.
          for( int j = 0; j < p.n; j++ ) {
            assert(get_S(p.bytes_per_pointer, p.S, j*p.bytes_per_pointer) == expects[i][j]*p.bytes_per_character);
          }

          free(p.S);
        }
      }
    }
  }
}

int main(int argc, char** argv)
{
  if( argc == 1 ) {
    unit_test_suffix_sort_funs();
    unit_test_dcover();
    unit_test_dcx();

    printf("All tests PASS\n");
  } else if ( argc >= 2 ) {
    // read in a suffix sort a file!
    FILE* f = fopen(argv[1], "r");
    long len, pad;
    unsigned char* data;
    int max_period, min_period;
    error_t err;

    if( argc == 3 ) max_period = min_period = atoi(argv[2]);
    else {
      min_period = 3;
      max_period = 200;
    }

    pad = 1024;
    assert(f);

    fseek(f, 0, SEEK_END);
    len = ftell(f);
    rewind(f);

    data = malloc(len+pad);
    assert(data);

    // read it in.
    assert( 1 == fread(data, len, 1, f));
    // set the pad.
    memset(&data[len], 0, pad);

    // ask for the suffix array.
    for( int period = min_period; period <= max_period; period++ ) {
      suffix_sorting_problem_t p;

      if( ! dcx_inmem_supports_period(period) ) continue;

      p.n = len;
      p.bytes_per_character = 1;
      p.bytes_per_pointer = 0;
      p.T = data;
      p.S = NULL;

      printf( "Suffix sorting with period %i\n", period );
      start_clock();
      err = dcx_ssort(&p, &cover);
      die_if_err(err);
      stop_clock();

      print_timings("DCX", len);

      free(p.S);


    }
  }
}


