/*
  (*) 2010-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils_cc/record.hh
*/
#ifndef _RECORD_HH_
#define _RECORD_HH_

#include <iterator>

extern "C" {
#include <stddef.h>
#include <stdint.h>
}
#include "utils.hh" //PTR_ADD

// A record needs to have
//   typedef record_tag record_category;
//                      (one of the tags below)

// Record completely stores the data encoded in it.
// It does not store any pointers back to the original data.
struct fixed_size_record_tag { };
struct fixed_size_decoded_record_tag { };

struct variable_size_record_tag { };

// Record stores data completely, but may be variable-length.
struct complete_record_tag : variable_size_record_tag { };

// Record contains a pointer back to the encoded data.
struct incomplete_record_tag : variable_size_record_tag  { };

template <typename Tag, typename Record>
struct RecordTraitsReally {
  // Error!
};

template <typename Record>
struct RecordTraitsReally<fixed_size_record_tag, Record> {
  enum { record_size = sizeof(Record) };
  static size_t get_record_length(const Record& r)
  {
    return record_size;
  }
  static void decode(Record& r, const void* p)
  {
    const Record* record_ptr = (const Record*) p;
    r = *record_ptr;
  }
  static void encode(const Record& r, void* p)
  {
    Record* record_ptr = (Record*) p;
    *record_ptr = r;
  }
  typedef Record* iterator_t;
  //static const void* getbase(const Record* i) {
  //  return i;
  //}
  static void* getbase(Record* i) {
    return i;
  }
  //static const Record* getiter(const void* v) {
  //  return (const Record*) v;
  //}
  static Record* getiter(void* v) {
    return (Record*) v;
  }
  static Record get_empty()
  {
    Record empty;
    return empty;
  }
};

/* A variable size record needs to implement:

  // The category for this record.
  typedef record_tag record_category;

  // Default constructor is required.
  Record() {
  }

  // Get a string representation of a record.
  // This function is only used for debugging and can
  // safely return "" 
  std::string to_string() const;

  // Return the length of the (encoded) record in bytes
  size_t get_record_length() const;

  // Decode a record from a region of memory.
  void decode(const void* p);

  // Write the (encoded) record to a memory region; it will occupy
  // get_record_length() bytes.
  void encode(void* out) const;
*/

// forward declare record iterator.
template<typename Record>
class RecordIterator;

template<typename Record>
struct RecordTraitsReally<fixed_size_decoded_record_tag, Record> {
  enum { record_size = Record::record_size };
  static size_t get_record_length(const Record& r)
  {
    return record_size;
  }
  static void decode(Record& r, const void* p)
  {
    r.decode(p);
  }
  static void encode(const Record& r, void* p)
  {
    r.encode(p);
  }
  typedef RecordIterator<Record> iterator_t;
  static void* getbase(RecordIterator<Record> i) {
    return i.base();
  }
  static RecordIterator<Record> getiter(void* v) {
    return iterator_t(v);
  }
  static Record get_empty()
  {
    Record empty;
    return empty;
  }
};

template<typename Record>
struct RecordTraitsReally<variable_size_record_tag, Record> {
  static size_t get_record_length(const Record& r)
  {
    return r.get_record_length();
  }
  static void decode(Record& r, const void* p)
  {
    r.decode(p);
  }
  static void encode(const Record& r, void* p)
  {
    r.encode(p);
  }
  static Record get_empty()
  {
    Record empty;
    return empty;
  }
};
template<typename Record>
struct RecordTraitsReally<complete_record_tag, Record> {
  static size_t get_record_length(const Record& r)
  {
    return r.get_record_length();
  }
  static void decode(Record& r, const void* p)
  {
    r.decode(p);
  }
  static void encode(const Record& r, void* p)
  {
    r.encode(p);
  }
  static Record get_empty()
  {
    Record empty;
    return empty;
  }
};
template<typename Record>
struct RecordTraitsReally<incomplete_record_tag, Record> {
  static size_t get_record_length(const Record& r)
  {
    return r.get_record_length();
  }
  static void decode(Record& r, const void* p)
  {
    r.decode(p);
  }
  static void encode(const Record& r, void* p)
  {
    r.encode(p);
  }
  static Record get_empty()
  {
    Record empty;
    return empty;
  }
};
template<typename Record>
struct RecordTraits : RecordTraitsReally<typename Record::record_category, Record>
{
};

template<typename NumType>
struct IntRecordTraits : RecordTraitsReally<fixed_size_record_tag,NumType>
{
  static NumType get_empty()
  {
    return 0;
  }
};

template<> struct RecordTraits<int8_t> : IntRecordTraits<int8_t> {};
template<> struct RecordTraits<uint8_t> : IntRecordTraits<uint8_t> {};
template<> struct RecordTraits<int16_t> : IntRecordTraits<int16_t> {};
template<> struct RecordTraits<uint16_t> : IntRecordTraits<uint16_t> {};
template<> struct RecordTraits<int32_t> : IntRecordTraits<int32_t> {};
template<> struct RecordTraits<uint32_t> : IntRecordTraits<uint32_t> {};
template<> struct RecordTraits<int64_t> : IntRecordTraits<int64_t> {};
template<> struct RecordTraits<uint64_t> : IntRecordTraits<uint64_t> {};

template<typename Tag,typename Record>
struct RecordIteratorReally {
  // error!
};

template<typename Record>
struct RecordStandin
{
  enum { record_size = RecordTraits<Record>::record_size };
  void* ptr;
  RecordStandin() : ptr(NULL) { }
  explicit RecordStandin(void* ptr) : ptr(ptr) { }
  // allow implicit conversion to read.
  operator Record() const {
    Record cur;
    RecordTraits<Record>::decode(cur, (const void*) ptr);
    return cur;
  }
  RecordStandin& operator=(const RecordStandin& value) {
    //Record cur;
    //RecordTraits<Record>::decode(cur, value.ptr);
    //RecordTraits<Record>::encode(cur, ptr);
    memcpy(ptr, value.ptr, record_size);
    return *this;
  }
  // Override the assignment operator for write.
  RecordStandin& operator=(const Record& value) {
    RecordTraits<Record>::encode(value, ptr);
    return *this;
  }
  // For debugging.
  Record get_record() const {
    return *this; // should call implicit conversion above.
  }
};

template<typename Record>
inline void swap(RecordStandin<Record> a, RecordStandin<Record> b)
{
  Record read_a = a;
  Record read_b = b;
  a = read_b;
  b = read_a;
}

template<typename Record>
class RecordIterator
  : public std::iterator<std::random_access_iterator_tag,
                          Record,
                          ptrdiff_t,
                          RecordStandin<Record>,
                          RecordStandin<Record>
                         >
{
 protected:
  void* ptr;
 public:
  typedef ptrdiff_t difference_type_t;
  typedef Record iterator_type_t;
  typedef RecordStandin<Record> standin_t;

  enum { record_size = RecordTraits<Record>::record_size };
  RecordIterator() : ptr(NULL) { }
  RecordIterator(RecordIterator& i) : ptr(i.ptr) { }
  // allow iterator to const iterator conversion
  RecordIterator(const RecordIterator& i) : ptr(i.ptr) { }

  explicit RecordIterator(void* data) : ptr(data) { }
  RecordIterator(void* data, size_t i)
    : ptr(PTR_ADD(data,i*record_size))
  {
  }
  void* base() const
  {
    return ptr;
  }
  const char* base_charptr() const
  {
    return (const char*) ptr;
  }
  // Forward iterator requirements
  standin_t operator*()
  {
    return standin_t(ptr);
  }
  const standin_t operator*() const
  {
    return standin_t(ptr);
  }
 
  standin_t operator->() const
  {
    return standin_t(ptr);
  }

  RecordIterator& operator++()
  {
    ptr = PTR_ADD(ptr,record_size);
    return *this;
  }
  RecordIterator operator++(int)
  {
    RecordIterator tmp = *this;
    ptr = PTR_ADD(ptr,record_size);
    return tmp;
  }
  // Bidirection iterator requirements
  RecordIterator& operator--()
  {
    ptr = PTR_SUB(ptr,record_size);
    return *this;
  }
  RecordIterator operator--(int)
  {
    RecordIterator tmp = *this;
    ptr = PTR_SUB(ptr,record_size);
    return tmp;
  }
  // Random access iterator requirements
  RecordIterator operator+(difference_type_t n) const
  {
    return RecordIterator(PTR_ADD(ptr,n*record_size));
  }
  RecordIterator& operator+=(difference_type_t n)
  {
    ptr = PTR_ADD(ptr,n*record_size);
    return *this;
  }
  RecordIterator operator-(difference_type_t n) const
  {
    return RecordIterator(PTR_SUB(ptr,n*record_size));
  }
  RecordIterator& operator-=(difference_type_t n)
  {
    ptr = PTR_SUB(ptr,n*record_size);
    return *this;
  }
  const standin_t operator[](difference_type_t n) const
  {
    return standin_t(PTR_ADD(ptr,n*record_size));
  }
  standin_t operator[](difference_type_t n)
  {
    return standin_t(PTR_ADD(ptr,n*record_size));
  }

};

template<typename Record>
inline bool operator==(const RecordIterator<Record>& lhs,
                       const RecordIterator<Record>& rhs)
{
  return lhs.base_charptr() == rhs.base_charptr();
}
  
template<typename Record>
inline bool operator!=(const RecordIterator<Record>& lhs,
                       const RecordIterator<Record>& rhs)
{
  return lhs.base_charptr() != rhs.base_charptr();
}

template<typename Record>
inline bool operator<(const RecordIterator<Record>& lhs,
                       const RecordIterator<Record>& rhs)
{
  return lhs.base_charptr() < rhs.base_charptr();
}

template<typename Record>
inline bool operator>(const RecordIterator<Record>& lhs,
                       const RecordIterator<Record>& rhs)
{
  return lhs.base_charptr() > rhs.base_charptr();
}

template<typename Record>
inline bool operator<=(const RecordIterator<Record>& lhs,
                       const RecordIterator<Record>& rhs)
{
  return lhs.base_charptr() <= rhs.base_charptr();
}

template<typename Record>
inline bool operator>=(const RecordIterator<Record>& lhs,
                       const RecordIterator<Record>& rhs)
{
  return lhs.base_charptr() >= rhs.base_charptr();
}

template<typename Record>
inline typename RecordIterator<Record>::difference_type_t
operator-(const RecordIterator<Record>& lhs,
          const RecordIterator<Record>& rhs)
{
  return (lhs.base_charptr() - rhs.base_charptr())/(RecordTraits<Record>::record_size);
}

template<typename Record>
inline RecordIterator<Record>
operator+(typename RecordIterator<Record>::difference_type_t n,
          const RecordIterator<Record>& i)
{
  return RecordIterator<Record>(i.base(), n);
}

#endif
