/*
  (*) 2008-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/util_test.c
*/
#include <assert.h>
#include "util.h"
#include "bswap.h"

void test_rdiv(uint64_t n, uint64_t d)
{
#ifdef HAS_RECIPROCAL_DIVIDE
  uint64_recip_t inv = compute_reciprocal(d);
  uint64_t got_r = reciprocal_divide(n, inv);
  uint64_t got_normal = n / d;
  assert(got_r == got_normal);
#endif
}

int main(int argc, char** argv)
{
  // test units.
  {
    struct val_unit x;
    x = get_unit(0);
    assert(x.value == 0 && 0 == strcmp("", get_unit_prefix(x)));
    x = get_unit(10);
    assert(x.value == 10 && 0 == strcmp("", get_unit_prefix(x)));
    x = get_unit(-10);
    assert(x.value == -10 && 0 == strcmp("", get_unit_prefix(x)));
    x = get_unit(1024);
    assert(x.value == 1 && 0 == strcmp("Ki", get_unit_prefix(x)));
    x = get_unit(-1024);
    assert(x.value == -1 && 0 == strcmp("Ki", get_unit_prefix(x)));
    x = get_unit(2*1024);
    assert(x.value == 2 && 0 == strcmp("Ki", get_unit_prefix(x)));
    x = get_unit(-2*1024);
    assert(x.value == -2 && 0 == strcmp("Ki", get_unit_prefix(x)));
    x = get_unit(1024L*1024L);
    assert(x.value == 1 && 0 == strcmp("Mi", get_unit_prefix(x)));
    x = get_unit(2*1024L*1024L);
    assert(x.value == 2 && 0 == strcmp("Mi", get_unit_prefix(x)));
    x = get_unit(1024LL*1024LL*1024LL);
    assert(x.value == 1 && 0 == strcmp("Gi", get_unit_prefix(x)));
    x = get_unit(2*1024LL*1024LL*1024LL);
    assert(x.value == 2 && 0 == strcmp("Gi", get_unit_prefix(x)));
    x = get_unit(1024LL*1024LL*1024LL*1024LL);
    assert(x.value == 1 && 0 == strcmp("Ti", get_unit_prefix(x)));
    x = get_unit(2*1024LL*1024LL*1024LL*1024LL);
    assert(x.value == 2 && 0 == strcmp("Ti", get_unit_prefix(x)));
  }

  // test gcd and lcm.
  {
    assert( gcd64(2,4) == 2 );
    assert( gcd64(15,27) == 3 );
    assert( gcd64(81,27) == 27 );
    assert( gcd64(10,15) == 5 );
    assert( lcm64(2,4) == 4 );
    assert( lcm64(4,6) == 12 );
    assert( lcm64(10,100) == 100 );
    assert( lcm64(16,10) == 80 );
  }

  // test reciprocal divide
  {
    uint64_t my_nums[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
                          100,
                          0x100,
                          0x10000,
                          0xACABA,
                          0x1000000,
                          0xffffffff,
                          0x1000000000,
                          0x100000000000,
                          0x10000000000000,
                          0x1000000000000000L,
                          0x1234567812345678L,
                          0x1234567812345678L,
                          0x1987123498612394L,
                          0xAB12BD9201C76762L,
                          0xAAAAAAAAAAAAAAAAL,
                          0xBBBBBBBBBBBBBBBBL,
                          0xCCCCCCCCCCCCCCCCL,
                          0xDDDDDDDDDDDDDDDDL,
                          0xEEEEEEEEEEEEEEEEL,
                          0x91A912349928133BL,
                          0x52E0F648A03D0D0AL,
                          0x7083093A89E1CF99L,
                          0xF234567812345678L, 
                          0xffffffffffffffffL };
    int num_nums = sizeof(my_nums) / sizeof(uint64_t);
    int i, j;

    test_rdiv(0,1);
    test_rdiv(0,2);
    test_rdiv(1,1);
    test_rdiv(1,2);
    test_rdiv(100,1);
    test_rdiv(100,2);
    test_rdiv(100,3);

    for( i = 0; i < num_nums; i++ ) {
      for( j = 0; j < num_nums; j++ ) {
        // divide i by j if j is not zero
        uint64_t n = my_nums[i];
        uint64_t d = my_nums[j];
        if( d != 0 ) test_rdiv(n,d);
      }
    }
  }

  // test bsearch
  {
    // test int bsearch_int_doc(int n, int* arr, int target)
    // document ending lengths
    int64_t arr[] = {4, 10, 11, 20};
    int64_t i;
    int64_t got;

    // Turn it into network byte order.
    for( i = 0; i < 4; i++ ) {
      arr[i] = hton_64(arr[i]);
    }

    printf("Testing bsearch 64\n");
    for( i = 0; i < 4; i++ ) assert( -1 == bsearch_int64_ntoh_arr(4, arr, i) );
    for( i = 4; i < 10; i++ ) assert( 0 == bsearch_int64_ntoh_arr(4, arr, i) );
    for( i = 10; i < 11; i++ ) {
      got = bsearch_int64_ntoh_arr(4, arr, i);
      if( 1 != got ) {
        printf("Failed at %i %i\n", (int) i, (int) got);
        assert(0);
      }
    }
    for( i = 11; i < 20; i++ ) {
      got = bsearch_int64_ntoh_arr(4, arr, i);
      if( 2 != got ) {
        printf("Failed at %i %i\n", (int) i, (int) got);
        assert(0);
      }
    }
  }
  {
    // test int bsearch_int_doc(int n, int* arr, int target)
    // document ending lengths
    uint32_t arr[] = {4, 10, 11, 20};
    uint32_t i;
    uint32_t got;

    // Turn it into network byte order.
    for( i = 0; i < 4; i++ ) {
      arr[i] = hton_32(arr[i]);
    }
    
    printf("Testing bsearch 32\n");
    for( i = 0; i < 4; i++ ) assert( -1 == bsearch_uint32_ntoh_arr(4, arr, i) );
    for( i = 4; i < 10; i++ ) assert( 0 == bsearch_uint32_ntoh_arr(4, arr, i) );
    for( i = 10; i < 11; i++ ) {
      got = bsearch_uint32_ntoh_arr(4, arr, i);
      if( 1 != got ) {
        printf("Failed at %i %i\n", (int) i, (int) got);
        assert(0);
      }
    }
    for( i = 11; i < 20; i++ ) {
      got = bsearch_uint32_ntoh_arr(4, arr, i);
      if( 2 != got ) {
        printf("Failed at %i %i\n", (int) i, (int) got);
        assert(0);
      }
    }
  }


  printf("Util test PASS\n");

  return 0;
}
