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

  femto/src/utils_cc/varint_test.cc
*/
#include <iostream>
#include <cassert>
#include <cstring>
#include "varint.hh"

be_uint40::register_type
read_5byte(void* x)
{
  be_uint40::register_type r;
  be_uint40* ptr = static_cast<be_uint40*>(x);
  r = *ptr;
  return r;
}

void test_varint(void)
{
  // check fast uints
  assert(sizeof(fast_uint<1>)==1);
  {
    uint8_t n = 1;
    assert(n == fast_uint<1>(n));
  }
  assert(sizeof(fast_uint<2>)==2);
  {
    uint16_t n = 1;
    assert(n == fast_uint<2>(n));
  }
  assert(sizeof(fast_uint<3>)==4);
  assert(sizeof(fast_uint<4>)==4);
  {
    uint32_t n = 1;
    assert(n == fast_uint<4>(n));
  }
  assert(sizeof(fast_uint<5>)==8);
  assert(sizeof(fast_uint<6>)==8);
  assert(sizeof(fast_uint<7>)==8);
  assert(sizeof(fast_uint<8>)==8);
  {
    uint64_t n = 1;
    assert(n == fast_uint<8>(n));
  }

  // First, check the sizes of the various var-ints.
  assert(sizeof(be_uint64)==8);
  assert(sizeof(be_uint56)==7);
  assert(sizeof(be_uint48)==6);
  assert(sizeof(be_uint40)==5);
  assert(sizeof(be_uint32)==4);
  assert(sizeof(be_uint24)==3);
  assert(sizeof(be_uint16)==2);
  assert(sizeof(be_uint8)==1);

  // Next, try reading some numbers.
  unsigned char arr1[] = {1,2,3,4,5,6,7,8,
                          9,0xa,0xb,0xc,0xd,0xe,0xf,0x10};
  void* p1 = arr1;
  unsigned char arr2[] = {0xff,0xfe,0xfd,0xfc,0xfb,0xfa,0xf9,0xf8,
                          0xf7,0xf6,0xf5,0xf4,0xf3,0xf2,0xf1,0xf0};
  void* p2 = arr2;

  // Test reading 8-bit numbers.
  {
    be_uint8::register_type expect1[] = 
                         {0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,
                          0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,0x10};
    be_uint8::register_type expect2[] = 
                         {0xff,0xfe,0xfd,0xfc,0xfb,0xfa,0xf9,0xf8,
                          0xf7,0xf6,0xf5,0xf4,0xf3,0xf2,0xf1,0xf0};
    be_uint8* ints1 = static_cast<be_uint8*>(p1);
    be_uint8* ints2 = static_cast<be_uint8*>(p2);
    for(int i = 0; i < 16; i++ ) {
      be_uint8::register_type r = ints1[i]; // implicit conversion
      be_uint8 temp = ints1[i];
      // now check 
      assert(ints1[i] == expect1[i]);
      // now create a be object.
      be_uint8 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints1[i],sizeof(be_uint8)));
      temp = 0;
      temp = r;
      be_uint8::register_type r2 = temp;
      assert( r == r2 );
      
    }
    for(int i = 0; i < 16; i++ ) {
      be_uint8::register_type r = ints2[i]; // implicit conversion
      // now check 
      assert(ints2[i] == expect2[i]);
      // now create a be object.
      be_uint8 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints2[i],sizeof(be_uint8)));
    }
  }

  // Test reading 16-bit numbers.
  {
    be_uint16::register_type expect1[] = 
                         {0x0102, 0x0304, 0x0506, 0x0708,
                          0x090a, 0x0b0c, 0x0d0e, 0x0f10 };
    be_uint16::register_type expect2[] = 
                         {0xfffe,0xfdfc,0xfbfa,0xf9f8,
                          0xf7f6,0xf5f4,0xf3f2,0xf1f0};
    be_uint16* ints1 = static_cast<be_uint16*>(p1);
    be_uint16* ints2 = static_cast<be_uint16*>(p2);
    for(int i = 0; i < 8; i++ ) {
      be_uint16::register_type r = ints1[i]; // implicit conversion
      // now check 
      assert(ints1[i] == expect1[i]);
      // now create a be object.
      be_uint16 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints1[i],sizeof(be_uint16)));
    }
    for(int i = 0; i < 8; i++ ) {
      be_uint16::register_type r = ints2[i]; // implicit conversion
      // now check 
      assert(ints2[i] == expect2[i]);
      // now create a be object.
      be_uint16 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints2[i],sizeof(be_uint16)));
    }
  }

  // Test reading 24-bit numbers.
  {
    be_uint24::register_type expect1[] = 
                         {0x010203, 0x040506, 0x070809,
                          0x0a0b0c, 0x0d0e0f };
    be_uint24::register_type expect2[] = 
                         {0xfffefd, 0xfcfbfa, 0xf9f8f7, 
                          0xf6f5f4, 0xf3f2f1};
    be_uint24* ints1 = static_cast<be_uint24*>(p1);
    be_uint24* ints2 = static_cast<be_uint24*>(p2);
    for(int i = 0; i < 5; i++ ) {
      be_uint24::register_type r = ints1[i]; // implicit conversion
      // now check 
      assert(ints1[i] == expect1[i]);
      // now create a be object.
      be_uint24 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints1[i],sizeof(be_uint24)));
    }
    for(int i = 0; i < 5; i++ ) {
      be_uint24::register_type r = ints2[i]; // implicit conversion
      // now check 
      assert(ints2[i] == expect2[i]);
      // now create a be object.
      be_uint24 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints2[i],sizeof(be_uint24)));
    }
  }

  // Test reading 32-bit numbers.
  {
    be_uint32::register_type expect1[] = 
                         {0x01020304, 0x05060708,
                          0x090a0b0c, 0x0d0e0f10 };
    be_uint32::register_type expect2[] = 
                         {0xfffefdfc,0xfbfaf9f8,
                          0xf7f6f5f4,0xf3f2f1f0};
    be_uint32* ints1 = static_cast<be_uint32*>(p1);
    be_uint32* ints2 = static_cast<be_uint32*>(p2);
    for(int i = 0; i < 4; i++ ) {
      be_uint32::register_type r = ints1[i]; // implicit conversion
      // now check 
      assert(ints1[i] == expect1[i]);
      // now create a be object.
      be_uint32 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints1[i],sizeof(be_uint32)));
    }
    for(int i = 0; i < 4; i++ ) {
      be_uint32::register_type r = ints2[i]; // implicit conversion
      // now check 
      assert(ints2[i] == expect2[i]);
      // now create a be object.
      be_uint32 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints2[i],sizeof(be_uint32)));
    }
  }
  // Test reading 40-bit numbers.
  {
    be_uint40::register_type expect1[] = 
                         {0x0102030405LL,
                          0x060708090aLL,
                          0x0b0c0d0e0fLL};
    be_uint40::register_type expect2[] = 
                         {0xfffefdfcfbLL,
                          0xfaf9f8f7f6LL,
                          0xf5f4f3f2f1LL};
    be_uint40* ints1 = static_cast<be_uint40*>(p1);
    be_uint40* ints2 = static_cast<be_uint40*>(p2);
    for(int i = 0; i < 3; i++ ) {
      be_uint40::register_type r = ints1[i]; // implicit conversion
      be_uint40 tmp = ints1[i]; // implicit conversion
      // now check 
      assert(ints1[i] == expect1[i]);
      // now create a be object.
      be_uint40 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints1[i],sizeof(be_uint40)));
    }
    for(int i = 0; i < 3; i++ ) {
      be_uint40::register_type r = ints2[i]; // implicit conversion
      be_uint40 tmp = ints2[i]; // implicit conversion
      // now check 
      assert(ints2[i] == expect2[i]);
      // now create a be object.
      be_uint40 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints2[i],sizeof(be_uint40)));
    }
    assert(read_5byte(p1)==expect1[0]);
    assert(read_5byte(p2)==expect2[0]);
  }

  // Test reading 48-bit numbers.
  {
    be_uint48::register_type expect1[] = 
                         {0x010203040506LL,
                          0x0708090a0b0cLL,
                         };
    be_uint48::register_type expect2[] = 
                         {0xfffefdfcfbfaLL,
                          0xf9f8f7f6f5f4LL
                         };
    be_uint48* ints1 = static_cast<be_uint48*>(p1);
    be_uint48* ints2 = static_cast<be_uint48*>(p2);
    for(int i = 0; i < 2; i++ ) {
      be_uint48::register_type r = ints1[i]; // implicit conversion
      be_uint48 tmp = ints1[i]; // implicit conversion
      // now check 
      assert(ints1[i] == expect1[i]);
      // now create a be object.
      be_uint48 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints1[i],sizeof(be_uint48)));
    }
    for(int i = 0; i < 2; i++ ) {
      be_uint48::register_type r = ints2[i]; // implicit conversion
      be_uint48 tmp = ints2[i]; // implicit conversion
      // now check 
      assert(ints2[i] == expect2[i]);
      // now create a be object.
      be_uint48 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints2[i],sizeof(be_uint48)));
    }
  }
  // Test reading 56-bit numbers.
  {
    be_uint56::register_type expect1[] = 
                         {0x01020304050607LL,
                          0x08090a0b0c0d0eLL
                         };
    be_uint56::register_type expect2[] = 
                         {0xfffefdfcfbfaf9LL,
                          0xf8f7f6f5f4f3f2LL
                         };
    be_uint56* ints1 = static_cast<be_uint56*>(p1);
    be_uint56* ints2 = static_cast<be_uint56*>(p2);
    for(int i = 0; i < 2; i++ ) {
      be_uint56::register_type r = ints1[i]; // implicit conversion
      be_uint56 tmp = ints1[i]; // implicit conversion
      // now check 
      assert(ints1[i] == expect1[i]);
      // now create a be object.
      be_uint56 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints1[i],sizeof(be_uint56)));
    }
    for(int i = 0; i < 2; i++ ) {
      be_uint56::register_type r = ints2[i]; // implicit conversion
      be_uint56 tmp = ints2[i]; // implicit conversion
      // now check 
      assert(ints2[i] == expect2[i]);
      // now create a be object.
      be_uint56 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints2[i],sizeof(be_uint56)));
    }
  }
  // Test reading 64-bit numbers.
  {
    be_uint64::register_type expect1[] = 
                         {0x0102030405060708LL,
                          0x090a0b0c0d0e0f10LL };
    be_uint64::register_type expect2[] = 
                         {0xfffefdfcfbfaf9f8LL,
                          0xf7f6f5f4f3f2f1f0LL};
    be_uint64* ints1 = static_cast<be_uint64*>(p1);
    be_uint64* ints2 = static_cast<be_uint64*>(p2);
    for(int i = 0; i < 2; i++ ) {
      be_uint64::register_type r = ints1[i]; // implicit conversion
      be_uint64 tmp = ints1[i]; // copy constructor
      be_uint64::register_type tmpr = tmp;
      assert(tmpr = r);
      // now check 
      assert(ints1[i] == expect1[i]);
      // now create a be object.
      be_uint64 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints1[i],sizeof(be_uint64)));
    }
    for(int i = 0; i < 2; i++ ) {
      be_uint64::register_type r = ints2[i]; // implicit conversion
      be_uint64 tmp = ints2[i]; // copy constructor
      // now check 
      assert(ints2[i] == expect2[i]);
      // now create a be object.
      be_uint64 b(r);
      // check that it's the same in memory.
      assert(0==memcmp(&b,&ints2[i],sizeof(be_uint64)));
      assert(ints2[i] == expect2[i]);
    }
  }
}


int main(int argc, char** argv)
{
  test_varint();
  std::cout << "All tests PASSED" << std::endl;
}

