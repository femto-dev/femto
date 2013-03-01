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

  femto/src/utils_cc/example_record.hh
*/
#ifndef _EXAMPLE_RECORD_HH_
#define _EXAMPLE_RECORD_HH_

#include <iostream>
#include <cstddef>
#include <sstream>
#include <cctype>
#include <cstring>

#include "criterion.hh"
#include "compare_record.hh"
#include "pipelining.hh"
#include "file_pipe.hh"
#include "aio_pipe.hh"

// A class to represent an external-memory record.
// When constructed with a void*, it breaks apart the passed
// record.
// In this example, each record has the following format:
// 1-byte length of key
// key-length bytes of key
// 8-bytes of value

// See sorting_inerface.hh for SortingCriterion options.
struct StringRecord {
  std::string key;
  uint64_t value;

  typedef incomplete_record_tag record_category;

  // Default constructor is required.
  StringRecord() 
    : key(), value(0) {
  }

  // This function is not part of the basic interface
  StringRecord(std::string key, uint64_t value)
    : key(key), value(value) {
  }
  StringRecord(int len, const char* data, uint64_t value)
    : key(data, len), value(value) {
  }
  StringRecord(int len, const void* data, uint64_t value)
    : key((const char*) data, len), value(value) {
  }



  // Get the value
  // This function is not part of the basic interface
  uint64_t get_value() const {
    return value;
  }

  // Get a string representation of a record.
  // This function is only used for debugging and can
  // safely return "" 
  std::string to_string() const {
    std::ostringstream os;
    os << "Record: key=\"" << key << "\"";
    os << " value=" << std::dec << value;
    return os.str();
  }

  // Return the length of the (encoded) record in bytes
  size_t get_record_length() const {
    return key.size() + 9;
  }
  // Decode a record from a region of memory.
  void decode(const void* pv) {
    // Read into the private variables.
    const unsigned char * p = (const unsigned char*) pv;
    unsigned char key_length = *p;
    p++;
    key = std::string((const char*) p, key_length);
    p+= key_length;
    value = * (uint64_t*) p;
  }

  // Write the (encoded) record to a memory region; it will occupy
  // get_record_length() bytes.
  void encode(void* outv) const {
    unsigned char* out = (unsigned char*) outv;
    // copy the length of key
    assert(key.size()<256);
    *out = key.size();
    out++;
    key.copy((char*) out, key.size());
    out+=key.size();
    memcpy(out, &value, 8);
  }
};

struct StringRecordValueSortingCriterion {
  typedef return_key_criterion_tag criterion_category;
  typedef uint64_t key_t;

  static key_t get_key(const StringRecord& r) {
    return r.value;
  }
};

struct StringRecordCompareSortingCriterion {
  typedef compare_criterion_tag criterion_category;
  static int compare(const StringRecord& a, const StringRecord& b) {
    return a.key.compare(b.key);
  }
};

struct StringRecordKeySortingCriterion {
  typedef return_key_criterion_tag criterion_category;

  typedef std::string key_t;

  // Return the key.
  key_t get_key(const StringRecord& r) {
    return r.key;
  }
};



struct IntRecord
{
  typedef complete_record_tag record_category;
  unsigned int b;
  std::string to_string() const
  {
    std::ostringstream os;
    os << b;
    return os.str();
  }
  size_t get_record_length() const {
    return sizeof(int);
  }
  void decode(const void* p) {
    memcpy(&b, p, sizeof(b));
  }
  void encode(void* p) const {
    memcpy(p, &b, sizeof(b));
  }
};

struct IntRecordSortingCriterion
{
  typedef return_key_criterion_tag criterion_category;
  typedef unsigned int key_t;

  static key_t get_key(const IntRecord& r)
  {
    return r.b;
  }
};
 
typedef StringRecordKeySortingCriterion ExampleRecordCriterion;
typedef StringRecord ExampleRecord;
static inline int compare_record(const ExampleRecord& a, const ExampleRecord& b)
{
  ExampleRecordCriterion crit;
  return CompareRecord<ExampleRecord,ExampleRecordCriterion>::compare(crit, a, b);
}

typedef IntRecordSortingCriterion IntRecordCriterion;

static inline int compare_record(const IntRecord& a, const IntRecord& b)
{
  IntRecordCriterion crit;
  return CompareRecord<IntRecord,IntRecordCriterion>::compare(crit, a, b);
}

template <typename Character, /* character type */
          int Num /* = num characters */
          >
struct FixedLengthStringKey {
  typedef Character character_t;
  static const int num_characters = Num;
  character_t characters[num_characters];
  FixedLengthStringKey()
  {
    for(int i=0;i<num_characters;i++)
      characters[i] = 0;
  }

  typedef character_t key_part_t;
  key_part_t get_key_part(size_t i) const
  {
    return characters[i];
  }
  size_t get_num_key_parts() const
  {
    return num_characters;
  }
};

template <typename Character, int Num >
struct FixedLengthStringRecord {
  typedef fixed_size_record_tag record_category;
  typedef FixedLengthStringKey<Character,Num> key_t;
  key_t key;
  uint64_t value;
  FixedLengthStringRecord() { };
};

template <typename Character, int Num>
struct FixedLengthStringRecordSortingCriterion {
  typedef return_key_criterion_tag criterion_category;
  typedef FixedLengthStringKey<Character,Num> key_t;
  static key_t get_key(const FixedLengthStringRecord<Character,Num>& r)
  {
    return r.key;
  }
};

#endif
