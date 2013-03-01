/*
  (*) 2009-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils_cc/sort_tests.hh
*/
#ifndef _SORT_TESTS_HH_
#define _SORT_TESTS_HH_

#include <cassert>

#include "criterion.hh"
#include "compare_record.hh"
#include "file_pipe.hh"
#include "sort_context.hh"
#include "pipelining.hh"
#include "varint.hh"
#include "compile_time.hh"

extern "C" {
  #include "buffer.h"
  #include "buffer_funcs.h"
}

struct Context {
  std::string tmp_dir;
  std::string get_tmp_dir() { return tmp_dir; }
  enum { DEBUG = 1 };
  enum { TIMING = 1 };
  size_t default_tile_size;
  size_t default_tiles_per_io_group;
  size_t default_num_io_groups;
  size_t default_num_tiles;
  Context() : tmp_dir("/tmp/"), default_tile_size(DEFAULT_TILE_SIZE), default_tiles_per_io_group(DEFAULT_TILES_PER_IO_GROUP), default_num_io_groups(2), default_num_tiles(2)
  {
  }
};

template<int ExtraBytes=4>
struct Record {
  uint64_t key;
  uint32_t v;
 private:
  unsigned char extra[ExtraBytes];
 public:
  Record()
  {
    key = 0;
    v = 0;
    for( int i = 0; i < ExtraBytes; i++ ) extra[i] = i+1;
  }
  void randomize()
  {
    size_t atatime = CompileTimeNumBytes<RAND_MAX>::num_bytes;
    atatime *= 8;
    atatime--;
    /*static int printed = 0;
    if( ! printed ) {
      printf("atatime is %i\n", (int) atatime);
      printed = 1;
    }*/

    key = 0;
    for( size_t i = 0; i < 8*sizeof(key); i+=atatime ) {
      key <<= atatime;
      key ^= rand();
    }
    v = 0;
    for( int i = 0; i < ExtraBytes; i++ ) extra[i] = i+1;
  }
  typedef fixed_size_record_tag record_category;

  std::string to_string(const Context* ctx) const
  {
    std::ostringstream os;
    os << "Record(" << key << ";" << v << ";";
    for( int i = 0; i < ExtraBytes; i++ ) {
      os << (int) extra[i] << ",";
      if( i != ExtraBytes-1 ) os << ",";
    }
    os << ")";
    return os.str();
  }
  void check()
  {
    for( int i = 0; i < ExtraBytes; i++ ) assert(extra[i] == i+1);
  }
};

template<int ExtraBytes>
static inline
bool operator<(const Record<ExtraBytes>& a, const Record<ExtraBytes>& b)
{
  return a.key < b.key;
}

template<int store_bits>
struct PackedRecordBitBuf {
  uint64_t key;
  uint64_t v;

  typedef fixed_size_decoded_record_tag record_category;
  enum { record_size = (store_bits + store_bits + 7)/8 };

  void decode(const void* p)
  {
    buffer_t bitbuf = build_buffer(record_size, (unsigned char*) p);
    bsInitRead(&bitbuf);
    key = bsR64( &bitbuf, store_bits);
    v = bsR64( &bitbuf, store_bits);
    bsFinishRead(&bitbuf);
  }
  void encode(void* p) const
  {
    buffer_t bitbuf = build_buffer(record_size, (unsigned char*) p);
    bsInitWrite(&bitbuf);
    bsW64(&bitbuf, store_bits, key);
    bsW64(&bitbuf, store_bits, v);
    bsFinishWrite(&bitbuf);
  }

};

template<int store_bits>
struct PackedRecordDecode {
  uint64_t key;
  uint64_t v;

  typedef fixed_size_decoded_record_tag record_category;
  enum { store_bytes = (store_bits + 7)/8 };
  enum { record_size = 2*store_bytes };

  void decode(const void* p)
  {
    be_uint<store_bytes>* ptr = (be_uint<store_bytes>*)p;
    key = *ptr;
    ptr++;
    v = *ptr;
  }
  void encode(void* p) const
  {
    be_uint<store_bytes>* ptr = (be_uint<store_bytes>*)p;
    *ptr = key;
    ptr++;
    *ptr = v;
  }

};

template<int store_bits>
struct PackedRecord {
  typedef fixed_size_record_tag record_category;
  enum { store_bytes = (store_bits + 7)/8 };
  enum { record_size = 2*store_bytes };
  be_uint<store_bytes> key;
  be_uint<store_bytes> v;
};

template<int ExtraBytes=0>
struct RecordPtr {
  Record<ExtraBytes>* x;
  RecordPtr(Record<ExtraBytes>* x) : x(x)
  {
  }
};

template<int ExtraBytes=0>
struct Criterion
{
  typedef return_key_criterion_tag criterion_category;
  typedef uint64_t key_t;
  //static const bool sort_keys_ascending = true;
  const key_t& get_key(const Record<ExtraBytes>& r)
  {
    return r.key;
  }
  const key_t& get_key(const RecordPtr<ExtraBytes>& r)
  {
    return r.x->key;
  }
};

template<int ExtraBytes=0>
struct ComparisonCriterion
{
  typedef compare_criterion_tag criterion_category;
  int compare(const Record<ExtraBytes> & a, const Record<ExtraBytes> & b) const
  {
    if( a.key < b.key ) return -1;
    if( a.key > b.key ) return 1;
    return 0;
  }
};

template<int store_bits>
struct PackedRecordCriterion
{
  typedef return_key_criterion_tag criterion_category;
  typedef uint64_t key_t;

  const key_t get_key(const PackedRecord<store_bits>& r)
  {
    return r.key;
  }
};


template<int ExtraBytes=0>
struct PartCriterion
{
  typedef return_key_criterion_tag criterion_category;

  struct PartKey {
    uint64_t key;
    typedef uint64_t key_part_t;

    PartKey() : key(0) { }
    PartKey(uint64_t key) : key(key) { }

    key_part_t get_key_part(size_t i) const
    {
      key_part_t ret;
      if( i == 0 ) {
        // return top half
        ret = key;
        ret >>= 8*sizeof(key)/2;
        return ret;
      } else if( i == 1 ) {
        // return bottom half
        ret = key;
        ret <<= 8*sizeof(key)/2;
        ret >>= 8*sizeof(key)/2;
        return ret;
      } else {
       assert(0);
      } 
    }
    size_t get_num_key_parts() const
    {
     return 2;
    }
  };

  typedef PartKey key_t;

  key_t get_key(const Record<ExtraBytes>& r)
  {
    return PartKey(r.key);
  }
  key_t get_key(const RecordPtr<ExtraBytes>& r)
  {
    return PartKey(r.x->key);
  }

};


template<int ExtraBytes>
static inline
bool operator<(const RecordPtr<ExtraBytes>& a, const RecordPtr<ExtraBytes>& b)
{
  return *a.x < *b.x;
}

template< typename RecordType >
struct sort_test_producer : public pipeline_node {
  write_pipe* output;
  uint64_t num;
  uint64_t num_zeros;

  virtual void run()
  {
    pipe_back_inserter<RecordType> writer(output);
    RecordType last;

    for( uint64_t i = 0; i < num; i++ ) {
      RecordType x;
      x.key = last.key + i;
      if( (i & 3) == 0 ) x.randomize();
      if( x.key == 0 ) num_zeros++;
      writer.push_back(x);

      last = x;
    }

    writer.finish();
  }

  sort_test_producer(write_pipe* output, uint64_t num)
    : output(output), num(num), num_zeros(0)
  {
  }
};

template< typename RecordType >
struct sort_test_consumer : public pipeline_node {
  read_pipe* input;
  uint64_t num_zeros;
  sort_test_producer<RecordType>* producer;

  virtual void run()
  {
    pipe_iterator<RecordType> read(input);
    pipe_iterator<RecordType> end;
    RecordType last;
    uint64_t count;

    count = 0;
    while( read != end ) {
      if( count > 0 ) {
        if(last.key <= (*read).key) {
          // OK
        } else {
          printf("At record %lli; last.key=%llx cur.key=%llx\n",
                 (long long int) (count),
                 (long long int) last.key,
                 (long long int) (*read).key);
          assert(last.key <= (*read).key);
        }
      }
      if( (*read).key == 0 ) num_zeros++;

      last = *read;
      ++read;
      ++count;
    }

    read.finish();

    // Check against producer.
    assert( producer->num == count );
    assert( producer->num_zeros == num_zeros );
  }

  sort_test_consumer(read_pipe* input, sort_test_producer<RecordType>* p)
    : input(input), num_zeros(0), producer(p)
  {
  }
};


#endif

