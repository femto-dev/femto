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

  femto/src/utils_cc/compare_record_test.cc
*/
#include <iostream>
#include <vector>
#include <cassert>

#include "compare_record.hh"
#include "example_record.hh"

template<typename Record, typename Criterion>
void check_sorted(std::vector<Record>& recs, Criterion crit)
{
  typedef CompareRecord<Record,Criterion> Compare;
  for( size_t i = 1; i < recs.size(); i++ ) {
    Record& a = recs[i-1];
    Record& b = recs[i];
    int cmp;
    // Check comparing with self is zero.
    cmp = Compare::compare(crit, a, a);
    assert( cmp == 0 );
    // Check manually sorted list is sorted.
    cmp = Compare::compare(crit, a, b);
    assert( cmp <= 0 );
    // Check comparison the other way.
    cmp = Compare::compare(crit, b, a);
    assert( cmp >= 0 );
  }
}

template<typename KeyType>
void check_bits_in_common(KeyType a, KeyType b, size_t expect)
{
  size_t got;
  assert( expect == (got = num_bits_in_common_impl<uint8_t,KeyType>(a,b)));
  assert( expect == (got = num_bits_in_common_impl<uint16_t,KeyType>(a,b)));
  assert( expect == (got = num_bits_in_common_impl<uint32_t,KeyType>(a,b)));
  assert( expect == (got = num_bits_in_common_impl<uint64_t,KeyType>(a,b)));
}

void check_string_in_common(const char* a_in, const char* b_in, size_t expect)
{
  std::string a(a_in);
  std::string b(b_in);
  check_bits_in_common(a,b,expect);
}

template<int Size, typename Character>
void check_fixed_string_in_common_impl(const char* a_in, const char* b_in, size_t expect_chars, size_t expect_within)
{
  FixedLengthStringKey<Character,Size> a, b;
  for( int i = 0; i < Size; i++ ) {
    a.characters[i] = a_in[i];
    b.characters[i] = b_in[i];
  }

  size_t expect = expect_chars * 8 * sizeof(Character);
  if( expect_within > 0 ) {
    expect += 8*(sizeof(Character) - sizeof(char)) + expect_within;
  }
  check_bits_in_common(a,b,expect);
}

template<int Size>
void check_fixed_string_in_common(const char* a_in, const char* b_in, size_t expect_chars, size_t expect_within)
{
  check_fixed_string_in_common_impl<Size,uint32_t>(a_in, b_in, expect_chars, expect_within);
  check_fixed_string_in_common_impl<Size,uint8_t>(a_in, b_in, expect_chars, expect_within);
  check_fixed_string_in_common_impl<Size,uint16_t>(a_in, b_in, expect_chars, expect_within);
  check_fixed_string_in_common_impl<Size,uint64_t>(a_in, b_in, expect_chars, expect_within);
}

int main(int argc, char** argv)
{
  {
    // Create a few records and compare them.
    std::vector<StringRecord> recs;
    { StringRecord r(0, "", 80); recs.push_back(r); }
    { StringRecord r(0, "", 80); recs.push_back(r); }
    { StringRecord r(0, "", 83); recs.push_back(r); }
    { StringRecord r(1, "a", 3); recs.push_back(r); }
    { StringRecord r(1, "a", 18); recs.push_back(r); }
    { StringRecord r(3, "abc", 901); recs.push_back(r); }
    { StringRecord r(4, "abcd", 2); recs.push_back(r); }
    { StringRecord r(4, "abcd", 800); recs.push_back(r); }
    { StringRecord r(4, "abce", 700); recs.push_back(r); }
    { StringRecord r(4, "abcf", 600); recs.push_back(r); }
    { StringRecord r(3, "abd", 500); recs.push_back(r); }
    { StringRecord r(1, "z", 83); recs.push_back(r); }
    { StringRecord r(1, "\x7f", 127); recs.push_back(r); }
    { StringRecord r(1, "\x80", 128); recs.push_back(r); }
    { StringRecord r(8, "\xff\xff\xff\xff\xff\xff\xff\xff", 900); recs.push_back(r); }
    // Check that recs is in sorted order.
    {
      StringRecordKeySortingCriterion crit;
      check_sorted<StringRecord,StringRecordKeySortingCriterion>(recs, crit);
    }
    {
      StringRecordCompareSortingCriterion crit;
      check_sorted<StringRecord,StringRecordCompareSortingCriterion>(recs, crit);
    }
  }

  // Check that right_shift works correctly.
  {
    char str[] = {0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09};
    typedef std::string string_t;
    string_t s(str,sizeof(str)/sizeof(char));
    unsigned char ret_char = 0;
    uint32_t ret_int = 0;
    uint64_t ret_long = 0;

    KeyTraits<string_t>::right_shift(s,0*8,ret_char);
    assert( ret_char == 0x09 );
    KeyTraits<string_t>::right_shift(s,0*8,ret_int);
    assert( ret_int == 0x06070809 );
    KeyTraits<string_t>::right_shift(s,0*8,ret_long);
    assert( ret_long == 0x0203040506070809ULL );

    KeyTraits<string_t>::right_shift(s,4,ret_char);
    assert( ret_char == 0x80 );
    KeyTraits<string_t>::right_shift(s,4,ret_int);
    assert( ret_int == 0x50607080 );
    KeyTraits<string_t>::right_shift(s,4,ret_long);
    assert( ret_long == 0x1020304050607080ULL );

    KeyTraits<string_t>::right_shift(s,1*8,ret_char);
    assert( ret_char == 0x08 );
    KeyTraits<string_t>::right_shift(s,1*8,ret_int);
    assert( ret_int == 0x05060708 );
    KeyTraits<string_t>::right_shift(s,1*8,ret_long);
    assert( ret_long == 0x0102030405060708ULL );

    KeyTraits<string_t>::right_shift(s,2*8,ret_char);
    assert( ret_char == 0x07 );
    KeyTraits<string_t>::right_shift(s,2*8,ret_int);
    assert( ret_int == 0x04050607 );
    KeyTraits<string_t>::right_shift(s,2*8,ret_long);
    assert( ret_long == 0x01020304050607ULL );

    KeyTraits<string_t>::right_shift(s,3*8,ret_char);
    assert( ret_char == 0x06 );
    KeyTraits<string_t>::right_shift(s,3*8,ret_int);
    assert( ret_int == 0x03040506 );
    KeyTraits<string_t>::right_shift(s,3*8,ret_long);
    assert( ret_long == 0x010203040506ULL );

    KeyTraits<string_t>::right_shift(s,4*8,ret_char);
    assert( ret_char == 0x05 );
    KeyTraits<string_t>::right_shift(s,4*8,ret_int);
    assert( ret_int == 0x02030405 );
    KeyTraits<string_t>::right_shift(s,4*8,ret_long);
    assert( ret_long == 0x0102030405ULL );

    KeyTraits<string_t>::right_shift(s,5*8,ret_char);
    assert( ret_char == 0x04 );
    KeyTraits<string_t>::right_shift(s,5*8,ret_int);
    assert( ret_int == 0x01020304 );
    KeyTraits<string_t>::right_shift(s,5*8,ret_long);
    assert( ret_long == 0x01020304 );

    KeyTraits<string_t>::right_shift(s,6*8,ret_char);
    assert( ret_char == 0x03 );
    KeyTraits<string_t>::right_shift(s,6*8,ret_int);
    assert( ret_int == 0x010203 );
    KeyTraits<string_t>::right_shift(s,6*8,ret_long);
    assert( ret_long == 0x010203 );

    KeyTraits<string_t>::right_shift(s,7*8,ret_char);
    assert( ret_char == 0x02 );
    KeyTraits<string_t>::right_shift(s,7*8,ret_int);
    assert( ret_int == 0x0102 );
    KeyTraits<string_t>::right_shift(s,7*8,ret_long);
    assert( ret_long == 0x0102 );

    KeyTraits<string_t>::right_shift(s,8*8,ret_char);
    assert( ret_char == 0x01 );
    KeyTraits<string_t>::right_shift(s,8*8,ret_int);
    assert( ret_int == 0x01 );
    KeyTraits<string_t>::right_shift(s,8*8,ret_long);
    assert( ret_long == 0x01 );

    KeyTraits<string_t>::right_shift(s,9*8,ret_char);
    assert( ret_char == 0 );
    KeyTraits<string_t>::right_shift(s,9*8,ret_int);
    assert( ret_int == 0 );
    KeyTraits<string_t>::right_shift(s,9*8,ret_long);
    assert( ret_long == 0 );
  }

  // Check that right_shift works correctly.
  {
    uint16_t str[] = {0x0102, 0x0304, 0x0506, 0x0708, 0x090a};
    typedef std::basic_string<uint16_t> string_t;
    string_t s(str,sizeof(str)/sizeof(uint16_t));
    unsigned char ret_char = 0;
    uint32_t ret_int = 0;
    uint64_t ret_long = 0;

    KeyTraits<string_t>::right_shift(s,0*8,ret_char);
    assert( ret_char == 0x0a );
    KeyTraits<string_t>::right_shift(s,0*8,ret_int);
    assert( ret_int == 0x0708090a );
    KeyTraits<string_t>::right_shift(s,0*8,ret_long);
    assert( ret_long == 0x030405060708090aULL );

    KeyTraits<string_t>::right_shift(s, 4,ret_char);
    assert( ret_char == 0x90 );
    KeyTraits<string_t>::right_shift(s, 4,ret_int);
    assert( ret_int == 0x60708090 );
    KeyTraits<string_t>::right_shift(s, 4,ret_long);
    assert( ret_long == 0x2030405060708090ULL );

    KeyTraits<string_t>::right_shift(s, 8,ret_char);
    assert( ret_char == 0x09 );
    KeyTraits<string_t>::right_shift(s, 8,ret_int);
    assert( ret_int == 0x06070809 );
    KeyTraits<string_t>::right_shift(s, 8,ret_long);
    assert( ret_long == 0x0203040506070809ULL );

    KeyTraits<string_t>::right_shift(s, 12,ret_char);
    assert( ret_char == 0x80 );
    KeyTraits<string_t>::right_shift(s, 12,ret_int);
    assert( ret_int == 0x50607080 );
    KeyTraits<string_t>::right_shift(s, 12,ret_long);
    assert( ret_long == 0x1020304050607080ULL );

    KeyTraits<string_t>::right_shift(s, 16,ret_char);
    assert( ret_char == 0x08 );
    KeyTraits<string_t>::right_shift(s, 16,ret_int);
    assert( ret_int == 0x05060708 );
    KeyTraits<string_t>::right_shift(s, 16,ret_long);
    assert( ret_long == 0x0102030405060708ULL );

    KeyTraits<string_t>::right_shift(s, 20,ret_char);
    assert( ret_char == 0x70 );
    KeyTraits<string_t>::right_shift(s, 20,ret_int);
    assert( ret_int == 0x40506070 );
    KeyTraits<string_t>::right_shift(s, 20,ret_long);
    assert( ret_long == 0x010203040506070ULL );

    KeyTraits<string_t>::right_shift(s, 24,ret_char);
    assert( ret_char == 0x07 );
    KeyTraits<string_t>::right_shift(s, 24,ret_int);
    assert( ret_int == 0x04050607 );
    KeyTraits<string_t>::right_shift(s, 24,ret_long);
    assert( ret_long == 0x01020304050607ULL );

    KeyTraits<string_t>::right_shift(s,9*8,ret_char);
    assert( ret_char == 0x01 );
    KeyTraits<string_t>::right_shift(s,9*8,ret_int);
    assert( ret_int == 0x01 );
    KeyTraits<string_t>::right_shift(s,9*8,ret_long);
    assert( ret_long == 0x01ULL );

    KeyTraits<string_t>::right_shift(s,10*8,ret_char);
    assert( ret_char == 0 );
    KeyTraits<string_t>::right_shift(s,10*8,ret_int);
    assert( ret_int == 0 );
    KeyTraits<string_t>::right_shift(s,10*8,ret_long);
    assert( ret_long == 0 );
  }

  {
    // Test with fixed-length maybe more-characters strings.
    typedef FixedLengthStringKey<unsigned int,3> string_t;
    string_t s;
    s.characters[0] = 'a'; // 0x00000061
    s.characters[1] = '0'; // 0x00000030
    s.characters[2] = '0'; // 0x00000030
    unsigned char ret_char = 0;
    uint32_t ret_int = 0;
    uint64_t ret_long = 0;
    KeyTraits<string_t>::right_shift(s,0,ret_char);
    assert(ret_char == 0x30);
    KeyTraits<string_t>::right_shift(s,0,ret_int);
    assert(ret_int == 0x30);
    KeyTraits<string_t>::right_shift(s,0,ret_long);
    assert(ret_long == 0x0000003000000030ULL);

    KeyTraits<string_t>::right_shift(s,32,ret_char);
    assert(ret_char == 0x30);
    KeyTraits<string_t>::right_shift(s,32,ret_int);
    assert(ret_int == 0x30);
    KeyTraits<string_t>::right_shift(s,32,ret_long);
    assert(ret_long == 0x0000006100000030ULL);

    KeyTraits<string_t>::right_shift(s,64,ret_char);
    assert(ret_char == 0x61);
    KeyTraits<string_t>::right_shift(s,64,ret_int);
    assert(ret_int == 0x61);
    KeyTraits<string_t>::right_shift(s,64,ret_long);
    assert(ret_long == 0x00000061ULL);
  }

  // Check that num_bits_in_common works correctly.
  {
    check_string_in_common("\xff", "\xff", 8);
    check_string_in_common("\xff", "\xf0", 4);
    check_string_in_common("\x7f", "\xff", 0);
    check_string_in_common("uvwxyz", "uvwxyz", 6*8);
    check_string_in_common("uvwxyz", "\xffvwxyz", 0*8);
    check_string_in_common("uvwxyz", "u\xffwxyz", 1*8);
    check_string_in_common("uvwxyz", "uv\xffxyz", 2*8);
    check_string_in_common("uvwxyz", "uvw\xffyz", 3*8);
    check_string_in_common("uvwxyz", "uvwx\xffz", 4*8);
    check_string_in_common("uvwxyz", "uvwxy\xff", 5*8);
    check_string_in_common("aa0", "a00", 9);


    check_fixed_string_in_common<3>("aa0", "a\xff\xff", 1, 0);
    check_fixed_string_in_common<3>("aa0", "a00", 1, 1);
  }


  std::cout << "All tests PASS" << std::endl;
  return 0;
}
