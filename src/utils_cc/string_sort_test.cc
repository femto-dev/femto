/*
  (*) 2007-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils_cc/string_sort_test.cc
*/


#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <cstring>

extern "C" {
  #include "string_sort.h"
  #include "timing.h"
  #include "error.h"
}

void test_strings(void)
{
  // test swap_bytes.
  {
    int i;
    for( i = 0; i < 32; i++ ) {
      const unsigned char* a = (const unsigned char*) "0123456789abcdef0123456789abcdef";
      const unsigned char* b = (const unsigned char*) "fedcba9876543210fedcba9876543210";
      unsigned char* as = (unsigned char*) strdup((const char*)a);
      unsigned char* bs = (unsigned char*) strdup((const char*)b);
      swap_bytes(as, bs, i);
      assert(0 == memcmp(as, b, i ) );
      assert(0 == memcmp(as+i, a+i, 32-i ) );
      assert(0 == memcmp(bs, a, i ) );
      assert(0 == memcmp(bs+i, b+i, 32-i ) );
    }
  }
}

int test_str_len = 0;

unsigned char* test_get_string(const void* context, const void* data_ptr) {
  unsigned char** d = (unsigned char**) data_ptr;
  assert(context == (void*) 0x1234);
  return *d;
}
int test_after_compare(const void* context, const void* a, const void* b)
{
  unsigned char** ap = (unsigned char**) a;
  unsigned char** bp = (unsigned char**) b;
  unsigned char* as = *ap;
  unsigned char* bs = *bp;

  int str_len = test_str_len;

  assert(context == (void*) 0x1234);
  return as[str_len-1] - bs[str_len-1];
}


void test_sort(string_sort_fun_t sort,
                 int test_number,
                 int n_strings, 
                 int str_len)
{
  string_sort_params_t params = {0};
  error_t err;
  unsigned char** strings_before;
  unsigned char** strings;
  int i, j;

  //printf("test sort(%p %i %i %i)\n", sort, test_number, n_strings, str_len);
  // generate the data.
  strings = (unsigned char**) malloc(n_strings*sizeof(unsigned char*));
  assert(strings);
  strings_before = (unsigned char**) malloc(n_strings*sizeof(unsigned char*));
  assert(strings_before);
  for( i=0; i < n_strings; i++ ) {
    strings[i] = (unsigned char*) malloc(str_len);
    assert(strings[i]);
    strings_before[i] = strings[i];
    for( j = 0; j < str_len; j++ ) {
      if( (test_number>>1) == 0 ) {
        strings[i][j] = 0;
      } else if ( (test_number>>1) == 1 ) {
        strings[i][j] = n_strings - i - 1;
      } else if ( (test_number>>1) == 2 ) {
        strings[i][j] = i;
      } else if ( (test_number>>1) == 3 ) {
        strings[i][j] = i + j;
      } else if ( (test_number>>1) == 4 ) {
        strings[i][j] = i*j;
      } else if ( (test_number>>1) == 5 ) {
        switch ( rand() % 4) {
          case 0:
            strings[i][j] = 'a' + (rand()%26);
            break;
          case 1:
            strings[i][j] = 'A' + (rand()%26);
            break;
          case 2:
            strings[i][j] = '0' + (rand()%10);
            break;
          default:
            switch ( rand() % 1024 ) {
              case 0:
                strings[i][j] = 0;
                break;
              default:
                strings[i][j] = rand();
                break;
            }
            break;
        }
      } else if( (test_number>>1) == 6 ) {
        if( i < n_strings/2 ) {
          strings[i][j] = 'a' + (rand()%26);
        } else {
          strings[i][j] = rand();
        }
      } else {
        strings[i][j] = rand();
      }
    }
  }

  // declare some comparison functions
  test_str_len = str_len;

  params.context = (void*) 0x1234;
  params.base = (unsigned char*) strings;
  params.n_memb = n_strings;
  params.memb_size = sizeof(unsigned char*);
  params.str_len = str_len;
  params.get_string = test_get_string;
  params.compare = NULL;

  if( test_number & 1 ) {
    params.str_len--;
    params.compare = test_after_compare;
  }

  err = sort(&params);
  die_if_err(err);

  // test that things are appropriately sorted!
  for( i = 1; i < n_strings; i++ ) {
    int r;
    r = memcmp(strings[i-1], strings[i], str_len);
    assert(r <= 0);
  }
  // check that the first 1000 elements in strings_before
  // are present in the output.
  for( i = 0; i < 1000 && i < n_strings; i++ ) {
    unsigned char* s = strings_before[i];
    for( j = 0; j < n_strings; j++ ) {
      if( strings[j] == s ) break;
    }
    assert( j != n_strings ); // not every string present in output!
  }

  for( i = 0; i < n_strings; i++ ) {
    free(strings[i]);
  }
  free(strings);
  free(strings_before);
}


unsigned char* test_two_get_string(const void* context, const void* data_ptr) {
  unsigned char** d = (unsigned char**) data_ptr;
  return *d;
}

              
int main(int argc, char** argv) 
{
  string_sort_fun_t sorters[] = {
    bucket_sort,
    NULL};
  const char* names[] = {
    "bucket sort",
    NULL
  };
  int limits[] = {
    0x7fffffff,
    0
  };

  // Tests we're working on.
  test_sort(bucket_sort, 3, 2, 1);

  srand(0);

  if( argc == 1 ) {
    int i, j, str_len, n_keys;
    assert( 0 == memcmp(argv, argv, 0));
    test_strings();

    for( i = 0; sorters[i]; i++ ) {
      printf("\nTesting sorter %s\n",names[i]);
      // test this sorter!
      // generate some inputs.
      for( n_keys = 1; n_keys < 100; n_keys++ ) {
        if( n_keys > limits[i] ) {
          continue;
        }

        for( str_len = 1; str_len < 64; str_len += 15 ) {
          for( j = 0; j < 14; j++ ) {
            printf("."); fflush(stdout);
            test_sort(sorters[i], j, n_keys, str_len);
          }
        }
      }
      for( n_keys = 1000; n_keys < 800000; n_keys+=400000 ) {
        if( n_keys > limits[i] ) {
          continue;
        }

        for( str_len = 1; str_len < 64; str_len += 15 ) {
          for( j = 0; j < 14; j++ ) {
            printf("."); fflush(stdout);
            test_sort(sorters[i], j, n_keys, str_len);
          }
        }
      }
    }
    printf("\n");
  } else if( argc == 3 ) {
    // test a file.
    FILE* input = fopen(argv[1], "r");
    int len = atoi(argv[2]);
    char* buf;
    int i,j;
    char** lines;
    char** orig_lines;
    int nlines;

    printf("Will sort length %i suffixes from %s\n", len, argv[1]);
    assert(input);

    nlines = 0;

    buf = (char*) calloc(1, len+1);
    assert(buf);
    // each string is just the start of each line..
    while(fgets(buf, len+1, input) ) {
      nlines++;
    }

    lines = (char**) calloc(nlines,sizeof(char*));
    assert(lines);
    orig_lines = (char**) calloc(nlines,sizeof(char*));
    assert(orig_lines);
    rewind(input);
    i = 0;
    while(fgets(buf, len+1, input) ) {
      orig_lines[i] = buf;
      buf = (char*) calloc(1, len+1);
      assert(buf);
      i++;
    }

    // now run the sorters.
    for( i = 0; sorters[i]; i++ ) {
      string_sort_params_t params;
      error_t err;
      if( nlines > limits[i] ) {
        printf("Skipping sorter %s from limit %i\n", names[i], limits[i]);
        continue;
      }

      // set up the input 
      for( j = 0; j < nlines; j++ ) {
        lines[j] = orig_lines[j];
      }
      printf("Testing sorter %s\n",names[i]);
      params.context = NULL;
      params.base = (unsigned char*) lines;
      params.n_memb = nlines;
      params.memb_size = sizeof(char*);
      params.str_len = len;
      params.get_string = test_two_get_string;
      params.compare = NULL;

      start_clock();
      err = sorters[i](&params);
      die_if_err(err);
      stop_clock();
      print_timings(names[i], nlines);

      // test that things are appropriately sorted!
      for( j = 1; j < nlines; j++ ) {
        int r;
        r = memcmp(lines[j-1], lines[j], len);
        assert(r <= 0);
      }
      // check that every element in strings_before
      // is present in the output.
      for( j = 0; j < 1000; j++ ) {
        char* s = orig_lines[i];
        int k;
        for( k = 0; k < nlines; k++ ) {
          if( lines[k] == s ) break;
        }
        assert( k != nlines ); // not every string present in output!
      }

    }


    free(buf);
    for(i = 0; i < nlines; i++ ) {
      free(lines[i]);
    }
    free(lines);
  } else {
    printf("Usage: %s [file len]\n", argv[0]);
    return -10;
  }

  printf("All tests PASS\n");
  return 0;
}


