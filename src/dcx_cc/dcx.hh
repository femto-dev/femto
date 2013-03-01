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

  femto/src/dcx_cc/dcx.hh
*/
#ifndef _DCX_HH_
#define _DCX_HH_
/*
 * Note - this program depends critically on 
 * in-order delivery of MPI messages (from a given src to a given dst).
 */

// some basic infrastructure
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <sstream>
#include <string>
#include <cassert>
#include <limits>

extern "C" {
// POSIX aio interface.
//#include <aio.h>
#include <alloca.h>
#include "config.h"
#include "util.h"
#include "bit_funcs.h"
#include "buffer.h"
#include "buffer_funcs.h"
}

#ifdef HAVE_MPI_H
#include <mpi.h>
#endif

#include "compile_time.hh" // min_word_getter.
#include "pipelining.hh"
#include "criterion.hh"
#include "utils.hh"
#include "dcover.hh"
#include "file_pipe.hh"
#include "merge_node.hh"
#include "sort.hh"
#include "compare_record.hh" // right_shift_key_parts
#include "mpi_utils.hh" // copy_file, DEFAULT_COMM, My_comm, MPI_handler

#define NUM_RANDOM_DOCTORS 1

#define DEFAULT_BINS_PER_NODE 8

// default 7; options include 3 7 13 21 31 39 57 73 91 95 133 1024 2048 4096 8192
#define DEFAULT_PERIOD 7

/*
 Here are measured in-memory timings for sort/permute/distribute
 sort                  200 MB/s
 permute               285 MB/s
 distribute-sort       300 MB/s
 distribute-permute    700 MB/s

 We target 400 MB/s and therefore set the numbers below.
*/

// hopefully 570 MB/s
// hopefully 850 MB/s
// 3 for more || but believe only 2 needed.
#define PERMUTE_PROCS_PER_BIN 2

// hopefully 400 MB/s
// hopefully 600 MB/s
// 3 for more || but believe only 2 needed.
#define SORT_PROCS_PER_BIN 2

// hopefully 700 MB/s
#define SAMPLE_SPLIT_PROCS_PER_BIN 2

// hopefully 700 MB/s
#define EVEN_SPLIT_PROCS_PER_BIN 1

// Should be > than all the numbers above..
#define NUM_MPI_SEND_TILES 4

// indexer threads do not send tiles (they are local
// operations, then we copy index blocks).
#define NUM_INDEXER_THREADS 4

// So at most 2 sorters + 2 splitters -> 
// 4 x USE_MEMORY_PER_BIN + file cache ->
// 8 x USE_MEMORY_PER_BIN -> 1 GB OK.

extern unsigned long dcx_g_mem_per_bin;
extern void setup_mem_per_bin(long sort_memory);

// We use up to 2 threads working on 2 input files of up to 2 GB = 8 GB
#define USE_MEMORY_PER_BIN (dcx_g_mem_per_bin)

// how many records should we use when computing a sample?
// n/USE_MEMORY times this number.
// At this rate, for 1TB input, we have 1024*2304=2.25 M records
// times 25 bytes for largest records -> 56 MB
// so even for 100TB input, still only 5 GB which is pretty reasonable. 
#define NUM_SPLITTER_RECORDS_PER_FILE (800000L)
#define NUM_SPLITTER_RECORDS_PER_BIN_ADD (100000L)

#define DEBUG_DCX 0

// Setting DONT_DELETE != 0 causes intermediate files not to be deleted
//#pragma warning dont_delete_on
//#define DONT_DELETE 10
#define DONT_DELETE 0


#define REPORT_DISK_USAGE 1
#define PRINT_TIMING_DCX (1024*1024)
#define PRINT_PROGRESS_DCX 1
#define TIME_FINISH 0
#define TIME_PWRITE 0
#define RENUMBER_DOCUMENTS 0
// How many 'next documents' to keep with priority queue
#define RENUMBER_NUM_NEXT 4
// How many 'next documents' to create and sort?
#define RENUMBER_NUM_FOLLOWING 1

#define SHOULD_PRINT_TIMING (((PRINT_TIMING_DCX > 0) && (n >= PRINT_TIMING_DCX)) && (iproc == 0))
#define SHOULD_PRINT_PROGRESS (iproc == 0)


#define BWT_SUPPORT
#ifdef BWT_SUPPORT
extern "C" {
#include "bwt_prepare.h"
#include "bwt_writer.h"
#include "construct.h"
}
#endif

// These things are really in dcx.cc
extern int64_t dcx_g_cur_disk_usage;
extern int64_t dcx_g_max_disk_usage;
extern MPI_handler* dcx_g_handler;
extern void add_dcx_disk_usage(int64_t sz);
extern void sub_dcx_disk_usage(int64_t sz);

template <typename NumType>
struct MaybeUniqueName {
  NumType num;
  bool is_unique() const {
    return (num & 1);
  }
  NumType get_name() const {
    return num >> 1;
  }
  void set(bool is_unique, NumType name_in)
  {
    NumType unique = (is_unique)?(1):(0);
    num = (name_in << 1) | unique;
  }
  MaybeUniqueName() : num(0) {}
  MaybeUniqueName(bool is_unique, NumType name_in)
  {
    set(is_unique, name_in);
  }
};

// Input/output.
template <typename Offset, /* offset type */
          int nbits_offset,
          typename Rank, /* rank/character type */
          int nbits_rank
          >
struct OffsetRankRecord {
  typedef Offset offset_t;
  typedef Rank rank_t;
  offset_t offset;
  rank_t rank;

  OffsetRankRecord() : offset(0), rank(0) { }
  std::string to_string() const {
    std::ostringstream os;
    os << "OffsetRankRecord(" << offset << ";" << rank << ")";
    return os.str();
  }

  //typedef fixed_size_record_tag record_category;
  typedef fixed_size_decoded_record_tag record_category;

  enum { offset_bytes = (nbits_offset + 7)/8 };
  enum { rank_bytes = (nbits_rank + 7)/8 };
  enum { record_size = offset_bytes + rank_bytes };

  void decode(const void* p)
  {
    be_uint<offset_bytes>* o_ptr = (be_uint<offset_bytes>*) p;
    be_uint<rank_bytes>* r_ptr = (be_uint<rank_bytes>*) PTR_ADD(p,offset_bytes);
    offset = *o_ptr;
    rank = *r_ptr;
  }
  void encode(void* p) const
  {
    be_uint<offset_bytes>* o_ptr = (be_uint<offset_bytes>*) p;
    be_uint<rank_bytes>* r_ptr = (be_uint<rank_bytes>*) PTR_ADD(p,offset_bytes);
    *o_ptr = offset;
    *r_ptr = rank;
    if( EXTRA_CHECKS ) {
      OffsetRankRecord tmp;
      tmp.decode(p);
      assert(tmp.offset == offset);
      assert(tmp.rank == rank);
    }
  }
};

template<typename Document,
         int nbits_doc>
struct DocRenumberRecord {
  typedef Document document_t;
  document_t doc;
  document_t next_doc;

  DocRenumberRecord() : doc(0), next_doc(0) { }
  std::string to_string() const {
    std::ostringstream os;
    os << "DocRenumberRecord(" << doc <<  ";" << next_doc << ")";
    return os.str();
  }

  //typedef fixed_size_record_tag record_category;
  typedef fixed_size_decoded_record_tag record_category;

  enum { doc_bytes = (nbits_doc + 7)/8 };
  enum { record_size = doc_bytes + doc_bytes };

  void decode(const void* p)
  {
    be_uint<doc_bytes>* d_ptr = (be_uint<doc_bytes>*) p;
    be_uint<doc_bytes>* n_ptr = (be_uint<doc_bytes>*) PTR_ADD(p,doc_bytes);
    doc = *d_ptr;
    next_doc = *n_ptr;
  }
  void encode(void* p) const
  {
    be_uint<doc_bytes>* d_ptr = (be_uint<doc_bytes>*) p;
    be_uint<doc_bytes>* n_ptr = (be_uint<doc_bytes>*) PTR_ADD(p,doc_bytes);
    *d_ptr = doc;
    *n_ptr = next_doc;
    if( EXTRA_CHECKS ) {
      DocRenumberRecord tmp;
      tmp.decode(p);
      assert(tmp.doc == doc);
      assert(tmp.next_doc == next_doc);
    }
  }

  // So it can be used as a key.
  typedef document_t key_part_t;

  key_part_t get_key_part(size_t i) const
  {
    if( i == 0 ) return doc;
    else return next_doc;
  }

  size_t get_num_key_parts() const
  {
    return 2;
  }
  
};

template <typename Offset, /* offset type */
          int nbits_offset,
          typename Rank, /* rank type */
          int nbits_rank,
          typename Character, /* character type */
          int nbits_char,
          typename Document,
          int nbits_doc
          >
struct BwtRecord {
  typedef Offset offset_t;
  typedef Rank rank_t;
  typedef Character character_t;
  typedef Document document_t;
  offset_t doc_offset;
  rank_t rank;
  character_t ch;
  document_t doc;

  BwtRecord() : doc_offset(0), rank(0), ch(0), doc(0) { }
  std::string to_string() const {
    std::ostringstream os;
    os << "BwtRecord(" << doc <<  ";" << doc_offset << ";" << rank << ";" << ch << ")";
    return os.str();
  }

  //typedef fixed_size_record_tag record_category;
  typedef fixed_size_decoded_record_tag record_category;

  enum { offset_bytes = (nbits_offset + 7)/8 };
  enum { rank_bytes = (nbits_rank + 7)/8 };
  enum { char_bytes = (nbits_char + 7)/8 };
  enum { doc_bytes = (nbits_doc + 7)/8 };
  enum { record_size = offset_bytes + rank_bytes + char_bytes + doc_bytes};

  void decode(const void* p)
  {
    be_uint<offset_bytes>* o_ptr = (be_uint<offset_bytes>*) p;
    be_uint<rank_bytes>* r_ptr = (be_uint<rank_bytes>*) PTR_ADD(p,offset_bytes);
    be_uint<char_bytes>* c_ptr = (be_uint<char_bytes>*) PTR_ADD(p,offset_bytes+rank_bytes);
    be_uint<doc_bytes>* d_ptr = (be_uint<doc_bytes>*) PTR_ADD(p,offset_bytes+rank_bytes+char_bytes);
    doc_offset = *o_ptr;
    rank = *r_ptr;
    ch = *c_ptr;
    doc = *d_ptr;
  }
  void encode(void* p) const
  {
    be_uint<offset_bytes>* o_ptr = (be_uint<offset_bytes>*) p;
    be_uint<rank_bytes>* r_ptr = (be_uint<rank_bytes>*) PTR_ADD(p,offset_bytes);
    be_uint<char_bytes>* c_ptr = (be_uint<char_bytes>*) PTR_ADD(p,offset_bytes+rank_bytes);
    be_uint<doc_bytes>* d_ptr = (be_uint<doc_bytes>*) PTR_ADD(p,offset_bytes+rank_bytes+char_bytes);
    *o_ptr = doc_offset;
    *r_ptr = rank;
    *c_ptr = ch;
    *d_ptr = doc;
    if( EXTRA_CHECKS ) {
      BwtRecord tmp;
      tmp.decode(p);
      assert(tmp.doc_offset == doc_offset);
      assert(tmp.rank == rank);
      assert(tmp.ch == ch);
      assert(tmp.doc == doc);
    }
  }
};

// create offset_rank_record_by_offset() comparator
// create offset_rank_record_by_rank() comparator

// After naming.
// Names might not be unique!
template <typename Offset, /* offset type */
          int nbits_offset,
          typename Name, /* rank/character type */
          int nbits_name
          >
struct OffsetMaybeUniqueNameRecord {
  typedef Offset offset_t;
  typedef Name name_t;
  offset_t offset;
  MaybeUniqueName<name_t> name;

  OffsetMaybeUniqueNameRecord() : offset(0), name(false, 0) { }
  std::string to_string() const {
    std::ostringstream os;
    os << "OffsetMaybeUniqueNameRecord(" << offset << ";" <<
       ((name.is_unique())?("unique"):("nonunique")) << "," << name.get_name() << ")";
    return os.str();
  }

  //typedef fixed_size_record_tag record_category;
  typedef fixed_size_decoded_record_tag record_category;
  enum { offset_bytes = (nbits_offset + 7)/8 };
  enum { name_bytes = (nbits_name + 1 + 7)/8 };
  enum { record_size = name_bytes + offset_bytes };

  void decode(const void* p)
  {
    be_uint<offset_bytes>* o_ptr = (be_uint<offset_bytes>*) p;
    be_uint<name_bytes>* n_ptr = (be_uint<name_bytes>*) PTR_ADD(p,offset_bytes);
    offset = *o_ptr;
    name.num = *n_ptr;
  }
  void encode(void* p) const
  {
    be_uint<offset_bytes>* o_ptr = (be_uint<offset_bytes>*) p;
    be_uint<name_bytes>* n_ptr = (be_uint<name_bytes>*) PTR_ADD(p,offset_bytes);
    *o_ptr = offset;
    *n_ptr = name.num;
    if( EXTRA_CHECKS ) {
      OffsetMaybeUniqueNameRecord tmp;
      tmp.decode(p);
      assert(tmp.offset == offset);
      assert(tmp.name.num == name.num);
    }
  }
};

// After naming.
template <typename Offset, /* offset type */
          int nbits_offset,
          typename Name, /* rank/character type */
          int nbits_name
          >
struct OffsetNameRecord {
  typedef Offset offset_t;
  typedef Name name_t;
  offset_t offset;
  name_t name;

  OffsetNameRecord() : offset(0), name(0) { }

  std::string to_string() const {
    std::ostringstream os;
    os << "OffsetNameRecord(" << offset << ";" << name << ")";
    return os.str();
  }

  //typedef fixed_size_record_tag record_category;

  typedef fixed_size_decoded_record_tag record_category;
  enum { offset_bytes = (nbits_offset + 7)/8 };
  enum { name_bytes = (nbits_name + 7)/8 };
  enum { record_size = offset_bytes + name_bytes };

  void decode(const void* p)
  {
    be_uint<offset_bytes>* o_ptr = (be_uint<offset_bytes>*) p;
    be_uint<name_bytes>* n_ptr = (be_uint<name_bytes>*) PTR_ADD(p,offset_bytes);
    offset = *o_ptr;
    name = *n_ptr;
  }
  void encode(void* p) const
  {
    be_uint<offset_bytes>* o_ptr = (be_uint<offset_bytes>*) p;
    be_uint<name_bytes>* n_ptr = (be_uint<name_bytes>*) PTR_ADD(p,offset_bytes);
    *o_ptr = offset;
    *n_ptr = name;
    if( EXTRA_CHECKS ) {
      OffsetNameRecord tmp;
      tmp.decode(p);
      assert(tmp.offset == offset);
      assert(tmp.name == name);
    }
  }
};

template<typename Character, int nbits_character, typename Offset, int Period>
struct Characters {
  typedef Offset key_part_t;
  enum {
    part_bytes = sizeof(key_part_t),
    part_bits = 8*part_bytes,
    num_characters = Period,
    num_bits = nbits_character*num_characters,
    num_parts = (num_bits + part_bits - 1)/part_bits,
    record_size = num_parts*part_bytes
  };

  key_part_t parts[num_parts];

  key_part_t get_key_part(size_t i) const
  {
    return parts[i];
  }
  size_t get_num_key_parts() const
  {
    return num_parts;
  }
 
  bool is_zero() const
  {
    for( size_t i = 0; i < get_num_key_parts(); i++ ) {
      key_part_t kp = get_key_part(i);
      if( kp != 0 ) return false;
    }
    return true;
  }

  Characters()
  {
    for(int i = 0; i < num_parts; i++ ) {
      parts[i] = 0;
    }
    // Verify that offset and Character are unsigned.
    {
      key_part_t neg_one = -1;
      assert(neg_one > 0);
    }
    {
      Character neg_one = -1;
      assert(neg_one > 0);
    }
    if( nbits_character == part_bits ) {
      assert(num_parts == num_characters);
    }
    assert(nbits_character <= part_bits);
  }

  void set_characters(Character* chars)
  {
    // There are really two cases:
    // case 1 -- don't bother shifting
    if( num_parts == num_characters &&
        nbits_character <= part_bits ) {
      if( EXTRA_CHECKS ) assert(num_parts == num_characters);
      for( int i = 0; i < num_parts; i++ ) {
        parts[i] = chars[i];
      }
    } else {
      // case 2 -- it's worth shifting.
      // first, clear the parts.
      for( int i = 0; i < num_parts; i++ ) {
        parts[i] = 0;
      }
      int cur_part = 0;
      int part_bits_left = part_bits;
      for( int i = 0; i < num_characters; i++ ) {
        // A character need only be divided among 2 parts.
        size_t part1_bits = std::min(part_bits_left,nbits_character);
        size_t part2_bits = nbits_character - part1_bits;

        if( EXTRA_CHECKS ) assert(part_bits_left > 0);

        // put top part1_bits of char into part1
        key_part_t char_part = chars[i];
        char_part >>= nbits_character - part1_bits;
        parts[cur_part] <<= part1_bits;
        parts[cur_part] |= char_part;
        part_bits_left -= part1_bits;

        if( part_bits_left == 0 ) {
          cur_part++;
          part_bits_left = part_bits;
        }

        if( part2_bits > 0 ) {
          // put bottom part2_bits of char int part2.
          char_part = chars[i];
          // clear out the top bits
          // or don't bother - these will be shifted away
          // by the rest of the operation.
          //char_part <<= nbits_character - part2_bits;
          //char_part >>= nbits_character - part2_bits;
          // or it in.
          parts[cur_part] <<= part2_bits;
          parts[cur_part] |= char_part;
          part_bits_left -= part2_bits;
        }
      }
      parts[cur_part] <<= part_bits_left;
    }

    // Check that we can decode these..
    if( EXTRA_CHECKS ) {
      for( int  i = 0; i < num_characters; i++ ) {
        assert(get_character(i) == chars[i]);
      }
    }
  }

  Character get_character(int i) const
  {
    if( num_parts == num_characters &&
        nbits_character <= part_bits ) {
      return parts[i];
    } else {
      // Compute the shift amount
      size_t leftover = part_bits*num_parts - nbits_character*num_characters;
      size_t shift_amt = (num_characters-1-i)*nbits_character + leftover;
      Character ret = right_shift_key_parts<Character,Characters>(*this, shift_amt);
      // Clear the top bits of ret.
      ret <<= 8*sizeof(Character) - nbits_character;
      ret >>= 8*sizeof(Character) - nbits_character;

      return ret;
    }
  }
};

template <typename Character, /* character type */
          int nbits_character,
          typename Offset, /* offset type */
          int nbits_offset,
          int Period /* difference cover period */
          >
struct OffsetTupleRecord {
  typedef Character character_t;
  typedef Offset offset_t;
  typedef Characters<Character,nbits_character,Offset,Period> characters_t;
  // Length is not needed since last is 0-filled
  // and we can just assign rank 0 to the ranks near the end.
  offset_t offset;
  characters_t characters;
  OffsetTupleRecord() : offset(0) { }
  std::string to_string() const {
    std::ostringstream os;
    os << "OffsetTupleRecord(" << offset << ";";
    for(int i = 0; i < characters.num_characters; i++ ) {
      if( i > 0 ) os << ",";
      os << characters.get_character(i);
    }
    os << ")";
    return os.str();
  }

  typedef fixed_size_record_tag record_category;

  // So it can be used as a key; sorts by characters then offset.

  // So it can be used as a key.
  typedef offset_t key_part_t;

  key_part_t get_key_part(size_t i) const
  {
    if( i < characters_t::num_parts ) return characters.get_key_part(i);
    else return offset;
  }

  size_t get_num_key_parts() const
  {
    return 1 + characters.get_num_key_parts();
  }

};

// need offset_tuple_records_by_tuple() comparator

template <typename Offset, /* offset type */
          int nbits_offset,
          typename Name, /* name type */
          int nbits_name,
          typename Rank, /* rank type */
          int nbits_rank,
          int Period
          >
struct MergeRecord {
  typedef Dcover<Period> cover_t;
  static const int period = Period;
  static const int sample_size = cover_t::sample_size;
  typedef Offset offset_t;
  typedef Name name_t;
  typedef Rank rank_t;
  // Length is not needed since last is 0-filled
  // and we can just assign rank 0 to the ranks near the end.
  offset_t offset;
  name_t name; // rank of this tuple character (never unique).
  rank_t ranks[sample_size];
  MergeRecord()
    : offset(0), name(0)
  {
    for(int i = 0; i < sample_size; i++ ) {
      ranks[i] = 0;
    }
  }
  // Allow implicit conversion from an OffsetNameRecord.
  MergeRecord(const OffsetNameRecord<Offset,nbits_offset,Name,nbits_name> &r)
    : offset(r.offset), name(r.name)
  {
    // These ranks should never be used... so leaving them 0 is fine.
    for(int i = 0; i < sample_size; i++ ) {
      ranks[i] = 0;
    }
  }

  std::string to_string() const {
    std::ostringstream os;
    os << "MergeRecord(" << offset << ";" << name << ";";
    for(int i = 0; i < sample_size; i++ ) {
      if( i > 0 ) os << ",";
      os << ranks[i];
    }
    os << ")";
    return os.str();
  }

  //typedef fixed_size_record_tag record_category;
  typedef fixed_size_decoded_record_tag record_category;
  enum { offset_bytes = (nbits_offset + 7)/8 };
  enum { name_bytes = (nbits_name + 7)/8 };
  enum { rank_bytes = (nbits_rank + 7)/8 };
  enum { record_size = offset_bytes + name_bytes + sample_size*rank_bytes };
  void decode(const void* p)
  {
    be_uint<offset_bytes>* o_ptr = (be_uint<offset_bytes>*) p;
    be_uint<rank_bytes>* n_ptr = (be_uint<name_bytes>*) PTR_ADD(p,offset_bytes);
    be_uint<rank_bytes>* r_ptr = (be_uint<rank_bytes>*) PTR_ADD(p,offset_bytes+name_bytes);

    offset = *o_ptr;
    name = *n_ptr;
    for( int i = 0; i < sample_size; i++ ) {
      ranks[i] = *r_ptr;
      r_ptr++;
    }
  }
  void encode(void* p) const
  {
    be_uint<offset_bytes>* o_ptr = (be_uint<offset_bytes>*) p;
    be_uint<rank_bytes>* n_ptr = (be_uint<name_bytes>*) PTR_ADD(p,offset_bytes);
    be_uint<rank_bytes>* r_ptr = (be_uint<rank_bytes>*) PTR_ADD(p,offset_bytes+name_bytes);

    *o_ptr = offset;
    *n_ptr = name;
    for( int i = 0; i < sample_size; i++ ) {
      *r_ptr = ranks[i];
      r_ptr++;
    }
    if( EXTRA_CHECKS ) {
      MergeRecord tmp;
      tmp.decode(p);
      assert(tmp.offset == offset);
      assert(tmp.name == name);
      for( int i = 0; i < sample_size; i++ ) {
        assert(tmp.ranks[i] == ranks[i]);
      }
    }
  }
};

template <typename Character, /* character type */
          typename Rank /* rank type */
          >
struct CheckRecord {
  typedef fixed_size_record_tag record_category;
  typedef Character character_t;
  typedef Rank rank_t;
  rank_t rank;
  character_t character;
  rank_t rank_prime;

  CheckRecord() : rank(0), character(0), rank_prime(0) { }
  std::string to_string() const {
    std::ostringstream os;
    os << "CheckRecord(" << rank << ";" << character << ";" << rank_prime << ")";
    return os.str();
  }
};

typedef enum {
  DONT_SORT,
  PERMUTE,
  SORT
} sort_type_t;

typedef enum {
  MODE_NONE,
  MODE_READING,
  MODE_WRITING,
} bins_mode_t;


// need merge_record_comparator_nonsample to do individual sorts for merge record data.
// merge_record_general_comparator to merge record data at the end..

template <int nbits_character,
          int nbits_offset,
          int nbits_document,
          int Period>
class Dcx : public pipeline_node /* for timing */ {
public:
  // Always use these read/write pipes.
  typedef ssize_t bin_idx_t; // bin_idx_t must be signed


  // type of var to represent input alphabet
  typedef typename min_word_getter<(nbits_character+7)/8>::register_type character_t;

  // type of var to represent pointers into input
  typedef typename min_word_getter<(nbits_offset+7)/8>::register_type offset_t;

  // type of var to represent a document number.
  typedef typename min_word_getter<(nbits_document+7)/8>::register_type document_t;

      //typedef N new_offset_t; // type of var to represent pointers to sampled elements
    /* this would shrink offset_t as we recurse.
    //	if you add a static int num_bytes = X to be_uint<X> and #include "compile_time.hh" then we can use
            static const sptr_t new_max_pointer = (CompileTimePow<2,8*offset_t::num_bytes>-1)*Dcover::sample_size/Dcover::period;
            typedef be_uint<CompileTimeNumBytes<new_max_pointer>::num_bytes> new_offset_t;
    */
  typedef offset_t rank_t; // type of var to hold (compressed) names = ranks of sampled elements
  typedef offset_t name_t; // type of var to hold (compressed) names = ranks of sampled elements
  enum {
    nbits_rank = nbits_offset,
    nbits_name = nbits_offset
  };

  typedef MaybeUniqueName<rank_t> maybe_unique_name_t;

  typedef Dcover<Period> cover_t;

  typedef OffsetRankRecord<offset_t,nbits_offset,rank_t,nbits_rank> offset_rank_record_t;
  typedef OffsetNameRecord<offset_t,nbits_offset,name_t,nbits_name> offset_name_record_t;
  typedef OffsetMaybeUniqueNameRecord<offset_t,nbits_offset,name_t,nbits_name> offset_m_name_record_t;
  typedef OffsetTupleRecord<character_t,nbits_character,offset_t,nbits_offset,Period> offset_tuple_record_t;
  typedef MergeRecord<offset_t,nbits_offset,name_t,nbits_name,rank_t,nbits_rank,Period> merge_record_t;
  typedef BwtRecord<offset_t,nbits_offset,rank_t,nbits_rank,character_t,nbits_character,document_t,nbits_document> bwt_record_t;
  typedef DocRenumberRecord<document_t,nbits_document> doc_renumber_record_t;

  typedef Dcx<nbits_name, nbits_offset, nbits_document, Period> SubDcx;
  typedef typename SubDcx::offset_m_name_record_t recursion_input_record_t;

  
  // Uses current io stats!
  template<typename Record>
  static
  file_pipe_context get_file_pipe_context(std::string fname)
  {
    size_t record_size = RecordTraits<Record>::record_size;
    file_pipe_context ret = file_pipe_context(get_current_io_stats(),
                                              fname,
                                              0,
                                              PIPE_UNTIL_END,
                                              round_up_to_multiple(
                                                DEFAULT_TILE_SIZE,
                                                record_size),
                                              DEFAULT_TILES_PER_IO_GROUP,
                                              DEFAULT_NUM_IO_GROUPS,
                                              true, /* fixed size records */
                                              false /* no direct IO */);
    return ret;
  }



  static
  std::string filename_for_bin(std::string tmp_dir, size_t depth, std::string type, bin_idx_t bin)
  {
    size_t sz = 100+tmp_dir.length()+type.length();
    char* buf = (char*) alloca(sz);

    snprintf(buf, sz,
             "%s/d%li_%s_%lx",
             tmp_dir.c_str(),
             (long) depth,
             type.c_str(),
             (long) bin);

    std::string ret(buf);
    return ret;
  }

  template<typename Context, typename Record>
  struct IdentityFilterTranslator 
  {
    const Context* dcx;
    static Record translate(const Context* dcx, const Record& in, bool& keep) {
      keep = true;
      return in;
    }
  };

  /* Bins are not multithreaded - they must be used by exactly
   * one thread at a time.
   */
  template<typename Context, /* Dcx */
           typename Record, /* Record type */
           typename Criterion, /* for min/max */
           typename Splitters, /* Split to get bin # and to files at dest */
           sort_type_t SortType,
           typename FilterTranslator = IdentityFilterTranslator<Context,Record>,
           typename SplitterTranslator = IdentitySplitterTranslator<Record,Criterion,typename Splitters::splitter_record_t,typename Splitters::splitter_criterion_t>
          >
  struct Bins : private uncopyable {
    typedef Context context_t;
    typedef Record record_t;
    typedef Criterion criterion_t;
    typedef Splitters splitters_t;
    typedef FilterTranslator filter_translator_t;
    typedef typename splitters_t::splitter_record_t splitter_record_t;
    typedef typename splitters_t::splitter_criterion_t splitter_criterion_t;
    typedef typename splitters_t::splitter_key_t splitter_key_t;
    typedef typename RecordTraits<splitter_record_t>::iterator_t splitter_record_iterator_t;
    typedef RecordTraits<record_t> record_traits_t;
    typedef RecordSortingCriterion<record_t,criterion_t> comparator_t;
    typedef TotalMinMaxCount<splitter_record_t,splitter_criterion_t> totals_t;
    typedef TotalMinMaxCountByBin<splitter_record_t,splitter_criterion_t> totals_by_bin_t;
    typedef KeyCritTraits<Record,Criterion> kc_traits_t;
    typedef typename kc_traits_t::key_t key_t;
    typedef typename RecordTraits<Record>::iterator_t iterator_t;
    typedef parallel_distributor_node<record_t,criterion_t,splitters_t,SplitterTranslator> bin_distributor_node_t;
    typedef Sorter<Record,Criterion,Splitters,SplitterTranslator> sorter_t;
    typedef typename sorter_t::first_pass first_pass_t;
    typedef typename sorter_t::second_pass second_pass_t;
    typedef typename sorter_t::sort_status status_t;
    typedef pipe_back_inserter<Record> pipe_back_inserter_t;


    static const sort_type_t sort_type = SortType;
    static const bool permute = (sort_type==PERMUTE);

    struct Bin {
      // valid on all nodes:
      bin_idx_t bin_number; // index of this bin
      std::string fname;
      //int mpi_owner; // use owner() or group.owner()

      // valid on all nodes... at the right time
      bool total_computed; // only if this is true!
      offset_t num_records; // number of records in this bin (not counting those repeated) 
      offset_t num_before; // total number of records in bins before this one.
      splitter_key_t min; // (min and max include repeaded records)
      splitter_key_t max;

      MPI_send_pipe* to_bin; // Always goes to MPI comms...

      // valid only on bin owner:
      bool owns_splitters;
      Splitters* file_splitters;
      MPI_recv_pipe* recv_pipe; // Normally recv_pipe is connected to
                                // first_pass. However, if Binz<CanSort>
                                // is false, it is instead connected to
                                // copy_node, which writes it to a file.
      // If we're sorting:
      status_t* sort_status;
      first_pass_t* first_pass;
      second_pass_t* second_pass;
      bool started_reading; 
      // If not sorting, these are used in pass 1
      copy_node* copy;
      pipe_back_inserter<record_t>* dontsort_writer;
      file_write_pipe_t* file_write_pipe;

      read_pipe* from_bin; // Reads from second pass on bin owner only.
                           // if Binz<CanSort> is false, this is just a file
                           // reader.

      offset_t num_records_with_overlap; // number of records in this bin -- including those for repeated 
      //uint64_t file_offset;

      // copying is a bad idea because destructor deletes the pipe.
      Bin() : bin_number(0), fname(),
              total_computed(false), num_records(0),
              num_before(0), 
              //min(RecordTraits<Record>::get_empty()),
              //max(RecordTraits<Record>::get_empty()),
              to_bin(NULL),
              owns_splitters(false),
              file_splitters(NULL),
              recv_pipe(NULL),
              //fctx(),
              sort_status(NULL),
              first_pass(NULL), second_pass(NULL),
              started_reading(false),
              copy(NULL), dontsort_writer(NULL), file_write_pipe(NULL),
              from_bin(NULL),
              num_records_with_overlap(0)
      {
      }

      ~Bin()
      {
        // to_bin and recv_pipe are owned by the MPI group
        // so they are not freed here.
        if( first_pass ) delete first_pass;
        if( second_pass) delete second_pass;
        if( from_bin ) delete from_bin;
        if( copy ) delete copy;
        if( owns_splitters && file_splitters ) delete file_splitters;
        if( sort_status ) delete sort_status;
        if( file_write_pipe ) delete file_write_pipe;
      }

    };

    typedef Bin bin_t;


    const Context* ctx;
    criterion_t crit;
    splitter_criterion_t splitter_crit;
    SplitterTranslator trans;
    comparator_t comparator;
    std::string type;
    std::vector< bin_t > bins;
    bin_idx_t n_bins;

    // For the bin distributor
    bool owns_bin_splitters;
    splitters_t* bin_splitters;
    buffered_pipe* to_bins_distributor;
    pipe_back_inserter_t* to_bins;
    std::vector< pipe_back_inserter<record_t>* > dont_sort_writers;
    std::vector<write_pipe*> from_bins_distributor;
    bin_distributor_node_t* bins_distributor;
    totals_by_bin_t totals_by_bin;

    MPI_pipe_group group;

    int nproc; // number of processors.
    int iproc; // current processor rank.
    int thread_multiplicity_first_pass; // if > 1, we might use fewer threads
                                        // in order to keep the memory requirements down.
    int thread_multiplicity_second_pass;

    bins_mode_t mode;
    bool finished;
    bool cleared;

    static std::vector<int> get_bin_owners(bin_idx_t n_bins, int nproc, bool all_on_zero)
    {
      std::vector<int> ret;

      ret.reserve(n_bins);

      // set bin owners.
      for( bin_idx_t b = 0; b < n_bins; b++ ) {
        if( all_on_zero ) ret.push_back(0);
        //else ret.push_back(b / bins_per_proc);
        else ret.push_back( b % nproc );
      }

      return ret;
    }

    Bins(const Context* ctx, const char* type, bool all_on_zero=false, std::vector<std::string>* names=NULL )
      : ctx(ctx), crit(ctx), splitter_crit(ctx), trans(), comparator(crit), type(type),
        bins(), 
        n_bins(ctx->n_bins),
        owns_bin_splitters(false),
        bin_splitters(NULL),
        to_bins_distributor(NULL),
        to_bins(NULL),
        from_bins_distributor(),
        bins_distributor(NULL),
        totals_by_bin(splitter_crit, n_bins),
        //bins_per_proc(ceildiv(n_bins,ctx->nproc)),
        group(dcx_g_handler, type, get_bin_owners(n_bins, ctx->nproc, all_on_zero)),
        nproc(group.nproc),
        iproc(group.iproc),
        thread_multiplicity_first_pass(1),
        thread_multiplicity_second_pass(1),
        mode(MODE_NONE), finished(false), cleared(false)
    {
      assert( group.nproc == ctx->nproc );
      assert( group.bins.size() == (size_t) n_bins );
      setup_files(names);
    }
    void set_thread_multiplicity(int num)
    {
      thread_multiplicity_first_pass = num;
      thread_multiplicity_second_pass = num;
    }
 
    ~Bins() {
      if( ! finished ) {
        fprintf(stderr,
               "WARNING: node %i:%s never finshed\n",
               iproc, type.c_str());
        finish();
      }
      if( ! cleared ) {
        // Don't warn here, since it could be output.
        //fprintf(stderr,
        //       "WARNING: node %i:%s never cleared\n",
        //       iproc, type.c_str());
        clear(false);
      }
    }

    void setup_files(std::vector<std::string>* names)
    {
      // Put empty bins into the vector.
      bins.resize(n_bins);

      {
        error_t err;
        err = mkdir_if_needed(ctx->tmp_dir.c_str());
        if( err ) throw error(err);
      }

      if( names ) assert(names->size() == (size_t) n_bins);

      // Setup bin file names.
      for( bin_idx_t b = 0; b < n_bins; b++ ) {
        if( names ) {
          bins[b].fname = (*names)[b];
        } else {
          // set the filename
          bins[b].fname = filename_for_bin(ctx->tmp_dir, ctx->depth, type, b);
        }
      }

      for( bin_idx_t b = 0; b < n_bins; b++ ) {
        bin_t * bin = &bins[b];

        bin->bin_number = b;

        // These are not setup
        //bin->file_offset = 0;
        bin->num_records = 0;
        bin->num_before = 0;
        bin->num_records_with_overlap = 0;
        bin->total_computed = false;
        bin->recv_pipe = NULL;
        bin->first_pass = NULL;
        bin->second_pass = NULL;
        bin->from_bin = NULL;
      }
    }


    // Reclaim any space used by these bins.
    void clear(bool remove_files=true)
    {
      assert( mode ==  MODE_NONE );

      if( cleared ) assert(0);
      // Report the disk usage.
      if( REPORT_DISK_USAGE && remove_files ) {
        size_t record_size = record_traits_t::record_size;
        for( bin_idx_t b = 0; b < (bin_idx_t) bins.size(); b++ ) {
          if( is_local(b) ) { // local bin.
            sub_dcx_disk_usage(bins[b].num_records_with_overlap * record_size);
          }
        }
      }

      for( bin_idx_t b = 0; b < (bin_idx_t) bins.size(); b++ ) {
        if( is_local(b) ) { // local bin.
          int rc;
          if( remove_files && ! DONT_DELETE ) {
            rc = unlink(bins[b].fname.c_str());
            if( rc && errno != ENOENT ) {
              //printf("errno %i noent %i\n", errno, ENOENT);
              throw error(ERR_IO_STR_OBJ_NUM("unlink failed", bins[b].fname.c_str(), errno));
            }
            // Try deleting any filenames in the bin file set.
            if( bins[b].sort_status ) {
              bins[b].sort_status->set.unlink();
            }
          }
        }
      }

      cleared = true;
    }

    bool is_local(bin_idx_t b)
    {
      return group.is_local(b);
    }
    int owner(bin_idx_t b)
    {
      return group.owner(b);
    }

    // Calls get_current_io_stats to get the stats to write to.
    void begin_reading(bool can_delete=true, uint64_t max=((uint64_t)-1)) {
      assert(mode == MODE_NONE);
      mode = MODE_READING;
      assert( finished );
      assert( ! cleared );

      if( DONT_DELETE ) can_delete = false;

      for( bin_idx_t b = 0; b < n_bins; b++ ) {
        if( is_local(b) ) {
          bin_t* bin = &bins[b];
          assert(!bin->second_pass);
          if( SortType != DONT_SORT ) {
            assert(bin->sort_status);
            size_t record_size = RecordTraits<Record>::record_size;
            buffered_pipe* bpipe = new buffered_pipe(
                round_up_to_multiple(DEFAULT_TILE_SIZE, record_size), DEFAULT_NUM_TILES);
            bin->from_bin = bpipe;

            assert(bin->file_splitters);

            bin->second_pass = new second_pass_t(bin->sort_status, bpipe, bin->file_splitters, max, can_delete, ctx, type + " d=" + ctx->depth_string + " sort pass two", ctx->should_print_timing());
            //bin->second_pass->start();
          } else {
            bin->second_pass = NULL;
            bin->from_bin = new file_read_pipe_t(get_file_pipe_context<Record>(bin->fname));
          }
          bin->started_reading = false;
        }
      }
    }
    void begin_reading_bin(bin_idx_t b)
    {
      bin_t* bin = &bins[b];
      if( bin->second_pass ) {
        //std::cout << "node " << iproc << " starting second pass for "
        //          << type << " bin " << b << std::endl;
        bin->second_pass->start();
      }
      bin->started_reading = true;
    }
    void end_reading() {
      assert( mode == MODE_READING );
      mode = MODE_NONE;

      for( bin_idx_t b = 0; b < n_bins; b++ ) {
        if( is_local(b) ) {
          bin_t* bin = &bins[b];

          if( SortType != DONT_SORT ) {
            assert(bin->second_pass);
          }
          if( bin->second_pass ) {
            if( bin->started_reading ) bin->second_pass->finish();
            delete bin->second_pass;
            bin->second_pass = NULL;
          }
          // Note -- merge_records_node will delete these from_bin pipes.
          delete bin->from_bin;
          bin->from_bin = NULL;

          bin->started_reading = false;
        }
      }
    }

    // Uses current io stats!
    void begin_writing() {
      assert(mode == MODE_NONE);
      mode = MODE_WRITING;
      assert( ! finished );
      assert( ! cleared );

      // These must be set up in e.g. begin_writing_even
      if( SortType != DONT_SORT ) {
        assert(bin_splitters);
      }

      // activate
      for( bin_idx_t b = 0; b < n_bins; b++ ) {
        bin_t* bin = &bins[b];
        bin->to_bin = group.bins[b].send_pipe;
        if( is_local(b) ) {
          bin->recv_pipe = group.bins[b].recv_pipe;
          assert(! bin->first_pass );

          if( SortType != DONT_SORT ) {
            // These are set in e.g. begin_writing_even.
            assert(bin->sort_status);
            assert(bin->file_splitters);

            // first_pass constructor will create the files.
            bin->first_pass = new first_pass_t(bin->sort_status, bin->recv_pipe, bin->file_splitters, ctx, type + " d=" + ctx->depth_string + " sort pass one", ctx->should_print_timing());
            bin->first_pass->start();
          } else {
            file_pipe_context ctx = get_file_pipe_context<record_t>(bin->fname);
            ctx.tile_size = dcx_g_handler->tile_size;
            ctx.tiles_per_io_group = 1;
            ctx.io_group_size = dcx_g_handler->tile_size;

            // Make sure to create the file.
            ctx.create_zero_length();

            bin->file_write_pipe = new file_write_pipe_t(ctx);
            bin->copy = new copy_node(bin->recv_pipe, bin->file_write_pipe);
            bin->copy->start();
          }
        }
         
        assert(bin->to_bin);

        if( SortType == DONT_SORT ) {
          // Also create a writer..
          bin->dontsort_writer = new pipe_back_inserter<record_t>(bin->to_bin);
        }
      }

      if( bin_splitters ) {
        // Set up the to-bin distributor.
        long use_procs = 0;
        if( SortType == DONT_SORT ) use_procs = 1;
        else if( SortType == PERMUTE ) use_procs = EVEN_SPLIT_PROCS_PER_BIN;
        else if( SortType == SORT ) use_procs = SAMPLE_SPLIT_PROCS_PER_BIN;
        use_procs = ceildiv(use_procs, thread_multiplicity_first_pass);

        size_t record_size = RecordTraits<Record>::record_size;
        to_bins_distributor = new buffered_pipe(
                  round_up_to_multiple(DEFAULT_TILE_SIZE, record_size),
                  DEFAULT_NUM_TILES*use_procs);
        to_bins = new pipe_back_inserter_t(to_bins_distributor);
        from_bins_distributor.reserve(n_bins);
        for( bin_idx_t b = 0; b < n_bins; b++ ) {
          from_bins_distributor.push_back(bins[b].to_bin);
        }
        assert(from_bins_distributor.size() == (size_t) n_bins);
        // Written this way to get saner compiler errors.
        typename bin_distributor_node_t::criterion_t dcrit = crit;
        typename bin_distributor_node_t::splitters_t* dsplit = bin_splitters;
        typename bin_distributor_node_t::translator_t dtrans = trans;
        bins_distributor = new bin_distributor_node_t(dcrit,
                                                     to_bins_distributor,
                                                     &from_bins_distributor,
                                                     dsplit,
                                                     dtrans,
                                                     use_procs);
        bins_distributor->start();
      }
       
      dcx_g_handler->add_group(&group);
    }

    void end_writing() {
      assert( mode == MODE_WRITING );
      mode = MODE_NONE;

      assert( finished );

      // Join with the bins distributor thread.
      if( bins_distributor ) {
        bins_distributor->finish();
        totals_by_bin.update(& bins_distributor->totals);
      }

      // Update min, max, and count for bins we own.
      // Delete pipeline nodes.
      for( bin_idx_t b = 0; b < n_bins; b++ ) {
        bin_t* bin = &bins[b];

        // We add up all the num_records from the different sources
        // in update_counts, so we put it in the bin whether or
        // not it was a local bin.
        bin->num_records = totals_by_bin.t[b].count;

        if( is_local(b) ) {
          bin->recv_pipe = NULL; // was aliasing group.bins[b].recv_pipe
          bin->to_bin = NULL; // was aliasing group.bins[b].send_pipe
          if( SortType != DONT_SORT ) {
            bin->first_pass->finish();

            totals_t* totals = bin->sort_status->totals;
            bin->num_records_with_overlap = totals->total_num;
            bin->min = totals->total_min;
            bin->max = totals->total_max;
            delete bin->first_pass;
            bin->first_pass = NULL;
          } else {
            size_t record_size = record_traits_t::record_size;

            bin->copy->finish();
            bin->num_records_with_overlap = bin->copy->bytes_copied / record_size;
            delete bin->file_write_pipe;
            bin->file_write_pipe = NULL;
            delete bin->copy;
            bin->copy = NULL;
          }
        }
      }

      // Delete bin splitters if we own them.
      if( owns_bin_splitters ) {
        delete bin_splitters;
      }
      bin_splitters = NULL;
      delete to_bins;
      to_bins = NULL;
      delete to_bins_distributor;
      to_bins_distributor = NULL;
      delete bins_distributor;
      bins_distributor = NULL;
      // Clear out aliases to MPI send pipes..
      from_bins_distributor.resize(0);


      dcx_g_handler->remove_group(&group);

      group.barrier();

      update_counts();
    }

    void update_counts() {

      // Report the disk usage.
      if( REPORT_DISK_USAGE ) {
        size_t record_size = record_traits_t::record_size;
        for( bin_idx_t b = 0; b < (bin_idx_t) bins.size(); b++ ) {
          if( is_local(b) ) { // local bin.
            add_dcx_disk_usage(bins[b].num_records_with_overlap * record_size);
          }
        }
      }
      
#ifdef HAVE_MPI_H
      // share num_records with all other processes
      // share min and max with all other processes
      // This should probably all be done with an
      // "MPI_Alltoallv" but measurements indicate
      // it currently takes ~0.08s per finish which is not too bad.
      {
        int rc;
        long long int in, out, inout;
        for( bin_idx_t b = 0; b < (bin_idx_t) bins.size(); b++ ) {
          // communicate num records (difficult since not
          // all records in bin go to this total).
          int root = owner(b);
          in = bins[b].num_records;
          rc = MPI_Allreduce( &in, &out, 1, MPI_LONG_LONG_INT, MPI_SUM, group.comm );
          if( rc ) throw error(ERR_IO_STR("MPI_Allreduce failed"));
          bins[b].num_records = out;
          // broadcast num_records_with_overlap
          inout = bins[b].num_records_with_overlap;
          rc = MPI_Bcast( &inout, 1, MPI_LONG_LONG_INT, root, group.comm);
          if( rc ) throw error(ERR_IO_STR("MPI_Bcast failed"));
          bins[b].num_records_with_overlap = inout;

          rc = MPI_Bcast( &bins[b].min, sizeof(splitter_key_t), MPI_BYTE, root, group.comm );
          if( rc ) throw error(ERR_IO_STR("MPI_Bcast failed"));

          rc = MPI_Bcast( &bins[b].max, sizeof(splitter_key_t), MPI_BYTE, root, group.comm );
          if( rc ) throw error(ERR_IO_STR("MPI_Bcast failed"));
        }
      }
#endif

      {
        // compute num_before for each bin.
        offset_t total_num = 0;
        for( bin_idx_t b = 0; b < (bin_idx_t) bins.size(); b++ ) {
          bins[b].num_before = total_num;
          total_num += bins[b].num_records;
          bins[b].total_computed = true;
        }
      }


      if( iproc == 0 ) {
        for( bin_idx_t b = 0; b < (bin_idx_t) bins.size(); b++ ) {
          if( PRINT_PROGRESS_DCX ) { //if( DEBUG_DCX ) {
            printf("bin %i %s num_records=%lli num_records_with_overlap=%lli\n", 
                   (int) b,
                   type.c_str(),
                   (long long int) bins[b].num_records,
                   (long long int) bins[b].num_records_with_overlap);
          }
          // Either we're using overlap or we're not.
          // This check is here because of previous bug
          // (finding putting all records in bin 0 in bin 1 as well).
          assert( bins[b].num_records == bins[b].num_records_with_overlap ||
                  bins[b].num_records + ctx->bin_overlap >= bins[b].num_records_with_overlap );
        }
      }
    }

    // "pipe_back_inserter" functions.
    void finish() {
      assert( ! finished );
      assert( mode != MODE_NONE );
    
      finished = true;

      if( to_bins ) {
       // normal case - finishing the to_bin distributor writer will
       // cause the MPI pipes to be closed.
       to_bins->finish();
      } else {
        assert( SortType == DONT_SORT );
        for( bin_idx_t b = 0; b < n_bins; b++ ) {
          bins[b].dontsort_writer->finish();
          delete bins[b].dontsort_writer;
          bins[b].dontsort_writer = NULL;
        }
      }
    }

    template<typename InRecord>
    void push_back(const InRecord& in_r)
    {
      bool keep = true;
      Record r = FilterTranslator::translate(ctx, in_r, keep);

      if( keep ) {
        to_bins->push_back(r);
      }
    }


    template<typename InRecord>
    void push_back_bins(const InRecord& in_r, bin_idx_t bin, bin_idx_t alt_bin)
    {
      assert( SortType == DONT_SORT );

      bool keep = true;
      Record r = FilterTranslator::translate(ctx, in_r, keep);

      if( keep ) {
        bins[bin].dontsort_writer->push_back(r);
        totals_by_bin.update(bin, r);

        if( alt_bin != bin ) {
          bins[alt_bin].dontsort_writer->push_back(r);
        }
      }
    }
  };

  // Uses current io stats.
  template<typename InBins>
  void begin_writing_split(InBins* bins, typename InBins::splitters_t* bin_splitters, std::vector<typename InBins::splitters_t*> * file_splitters, long use_mem_per_bin = USE_MEMORY_PER_BIN)
  {
    typedef typename InBins::splitter_record_t record_t;
    typedef typename InBins::splitter_criterion_t criterion_t;
    typedef SampleSplitters<record_t,criterion_t> splitters_t;

    assert( file_splitters->size() == (size_t ) bins->n_bins );

    assert( InBins::sort_type == SORT );

    // Set the infos for the bin splitters.
    bins->owns_bin_splitters = false;
    bins->bin_splitters = bin_splitters;

    for( bin_idx_t b = 0; b < bins->n_bins; b++ ) {
      if( bins->is_local(b) ) {
        typename InBins::bin_t * bin = &bins->bins[b];

        int first_pass_threads = ceildiv(SAMPLE_SPLIT_PROCS_PER_BIN,
                                         bins->thread_multiplicity_first_pass);
        int second_pass_threads = ceildiv(
            (InBins::permute)?(PERMUTE_PROCS_PER_BIN):(SORT_PROCS_PER_BIN),
            bins->thread_multiplicity_second_pass);

        bin->sort_status = new typename InBins::sorter_t::sort_status(
            bins->crit, bins->trans, bin->fname,
            first_pass_threads, second_pass_threads,
            InBins::permute,
            (*file_splitters)[b]->num_bins,
            use_mem_per_bin);

        bin->owns_splitters = false;
        bin->file_splitters = (*file_splitters)[b];
      }
    }
    // Now call begin_writing on the bins.
    bins->begin_writing();
  }

  template<typename InBins>
  void begin_writing_even_estimate(InBins* bins, offset_t n_per_bin, offset_t bin_overlap, offset_t n_per_bin_estimate, long use_mem_per_bin = USE_MEMORY_PER_BIN )
  {
    typedef typename InBins::splitter_record_t record_t;
    typedef typename InBins::splitter_criterion_t criterion_t;
    typedef DividingSplitters<record_t,criterion_t> splitters_t;
    typedef typename InBins::sorter_t sorter_t;

    //assert( InBins::sort_type == PERMUTE || InBins::sort_type == DONT_SORT );

    // Set the infos for the bin splitters.
    bins->owns_bin_splitters = true;
    bins->bin_splitters = new splitters_t(bins->splitter_crit, bins->n_bins);
    bins->bin_splitters->use_sample_overlap(n_per_bin, bin_overlap);

    size_t n_files = sorter_t::get_n_bins(n_per_bin_estimate, USE_MEMORY_PER_BIN);

    printf("writing even files per bin: %lli\n", (long long int) n_files);

    // Then, for each bin that is ours, set up splitters.
    for( bin_idx_t b = 0; b < bins->n_bins; b++ ) {
      if( bins->is_local(b) ) {
        typename InBins::bin_t * bin = &bins->bins[b];

        int first_pass_threads = ceildiv(EVEN_SPLIT_PROCS_PER_BIN,
                                         bins->thread_multiplicity_first_pass);
        int second_pass_threads = ceildiv(
            (InBins::permute)?(PERMUTE_PROCS_PER_BIN):(SORT_PROCS_PER_BIN),
            bins->thread_multiplicity_second_pass);

        bin->sort_status = new typename InBins::sorter_t::sort_status(
            bins->crit, bins->trans, bin->fname,
            first_pass_threads, second_pass_threads,
            InBins::permute,
            n_files,
            use_mem_per_bin);

        splitters_t* file_splitters = new DividingSplitters<record_t,criterion_t>(
            bins->splitter_crit, bin->sort_status->n_bins);
        file_splitters->use_sample_minmax( b*n_per_bin,
                                          (b+1)*n_per_bin - 1 + bin_overlap);
        bin->owns_splitters = true;
        bin->file_splitters = file_splitters;
      }
    }

    // Now call begin_writing on the bins.
    bins->begin_writing();
  }
  template<typename InBins>
  void begin_writing_even(InBins* bins, offset_t n_per_bin, offset_t bin_overlap, long use_mem_per_bin = USE_MEMORY_PER_BIN )
  {
    begin_writing_even_estimate(bins, n_per_bin, bin_overlap, n_per_bin, use_mem_per_bin);
  }

  template<typename InBins>
  void begin_writing_fixed(InBins* bins,
                          typename InBins::splitter_record_t min,
                          typename InBins::splitter_record_t max,
                          ssize_t target_bin)
  {
    typedef typename InBins::splitter_record_t record_t;
    typedef typename InBins::splitter_criterion_t criterion_t;
    typedef FixedSplitters<record_t,criterion_t> splitters_t;
    typedef KeyCritTraits<record_t,criterion_t> kc_traits_t;
    typedef typename kc_traits_t::key_t key_t;

    key_t min_key, max_key;
    min_key = kc_traits_t::get_key(bins->splitter_crit, min);
    max_key = kc_traits_t::get_key(bins->splitter_crit, max);

    // Set the infos for the bin splitters.
    bins->owns_bin_splitters = true;
    bins->bin_splitters = new splitters_t(bins->crit, bins->n_bins, target_bin);
    bins->bin_splitters->use_sample_minmax(min_key, max_key);

    // Then, for each bin that is ours, set up splitters.
    for( bin_idx_t b = 0; b < bins->n_bins; b++ ) {
      if( bins->is_local(b) ) {
        typename InBins::bin_t * bin = &bins->bins[b];
 
        int first_pass_threads = 1; // zero splitter only needs 1 thread.
        int second_pass_threads = ceildiv(
            (InBins::permute)?(PERMUTE_PROCS_PER_BIN):(SORT_PROCS_PER_BIN),
            bins->thread_multiplicity_second_pass);

        bin->sort_status = new typename InBins::sorter_t::sort_status(
            bins->crit, bins->trans, bin->fname,
            first_pass_threads, second_pass_threads,
            InBins::permute, 1,
            USE_MEMORY_PER_BIN );

        splitters_t* file_splitters = new FixedSplitters<record_t,criterion_t>(
            bins->crit, 1, 0); /* always store to file 0 */
        file_splitters->use_sample_minmax(min_key, max_key);
        bin->owns_splitters = true;
        bin->file_splitters = file_splitters;
      }
    }

    // Now call begin_writing on the bins.
    bins->begin_writing();
  }


  void barrier() {
    mpi_barrier(comm);
  }
  
  // Sorting criterion
  // comparators!
  struct ZeroCriterion {
    ZeroCriterion(const Dcx* ctx) { }
    typedef return_key_criterion_tag criterion_category;
    typedef int key_t;
    template<typename Record>
    static
    key_t get_key(const Record& r)
    {
      return 0;
    }
  };


  template <typename Record>
  struct ByOffset {
    ByOffset(const Dcx* ctx) { }
    typedef return_key_criterion_tag criterion_category;
    typedef typename Record::offset_t key_t;
    static
    key_t get_key(const Record& r)
    {
      return r.offset;
    }
  };

  struct ByName {
    ByName(const Dcx* ctx) { }
    typedef return_key_criterion_tag criterion_category;
    typedef name_t key_t;
    static
    name_t get_key(const offset_name_record_t& r)
    {
      return r.name;
    }
  };

  template <typename Record>
  struct ByMaybeUniqueName {
    explicit ByMaybeUniqueName(const Dcx* ctx) { }
    typedef return_key_criterion_tag criterion_category;
    typedef typename Record::rank_t key_t;
    static
    key_t get_key(const Record& r)
    {
      return r.name.get_name();
    }
  };

  template <typename Record>
  struct ByRank {
    explicit ByRank(const Dcx* ctx) { }
    typedef return_key_criterion_tag criterion_category;
    typedef typename Record::rank_t key_t;
    key_t get_key(const Record& r) const
    {
      return r.rank;
    }
  };

  struct NameAndRank {
    // So it can be used as a record.
    typedef fixed_size_record_tag record_category;

    // So it can be used as a key.
    typedef rank_t key_part_t;
    key_part_t name;
    key_part_t rank;
    NameAndRank() : name(0), rank(0)
    {
    }
    // Build one from a merge record.
    explicit NameAndRank(merge_record_t mr)
      : name(mr.name), rank(mr.ranks[0])
    {
    }
    // Build one from an offset_name record.
    // This is used for unique records, so
    // the rank 0 is OK.
    explicit NameAndRank(offset_name_record_t on)
      : name(on.name), rank(0)
    {
    }

    key_part_t get_key_part(size_t i) const
    {
      return (i==0)?(name):(rank);
    }
    size_t get_num_key_parts() const {
      return 2;
    }
  };

  struct ByNameAndFirstRank {
    explicit ByNameAndFirstRank(const Dcx* ctx) { }
    typedef return_key_criterion_tag criterion_category;
    typedef NameAndRank key_t;
    static
    key_t get_key(const merge_record_t& r)
    {
      NameAndRank ret(r); // this constructor always uses ranks[0]
      return ret;
    }
    static
    const key_t& get_key(const NameAndRank& r)
    {
      return r;
    }
  };

  struct ByCharacters {
    explicit ByCharacters(const Dcx* ctx) { }
    typedef return_key_criterion_tag criterion_category;
    typedef typename offset_tuple_record_t::characters_t key_t;
    static
    const key_t& get_key(const offset_tuple_record_t& r)
    {
      return r.characters;
    }
  };


  struct ByCharactersAndOffset {

    explicit ByCharactersAndOffset(const Dcx* x) { }


    typedef return_key_criterion_tag criterion_category;
    typedef offset_tuple_record_t key_t;
    static
    const key_t& get_key(const offset_tuple_record_t& r)
    {
      return r; // note -- OffsetTupleRecord includes key get_key_part etc. for sorting by chars then offset.
    }
  };

  struct CharactersSplitterTranslator {
    // Convert from a splitter key to a sorting key.
    static
    typename offset_tuple_record_t::characters_t
    splitter_key_to_key(ByCharactersAndOffset crit, const offset_tuple_record_t& in, bool is_for_min)
    {
      return in.characters;
    }

    // Convert from a record to a splitter key.
    static
    const offset_tuple_record_t&
    record_to_splitter_key(ByCharactersAndOffset crit, const offset_tuple_record_t& in)
    {
      return in;
    }
  };

  struct ByNameAndDifferenceCover {
    const Dcx* ctx;
    ByNameAndDifferenceCover(const Dcx* ctx) : ctx(ctx) { }

    typedef compare_criterion_tag criterion_category;
    int compare(const merge_record_t& a, const merge_record_t& b) const
    {

      // first, compare by name.
      if( a.name < b.name ) return -1;
      if( a.name > b.name ) return 1;
     
      int a_amt, b_amt;
      ctx->cover.which_samples_to_use(a.offset % cover_t::period,
                                      b.offset % cover_t::period,
                                      &a_amt, &b_amt);

      // return the comparison of S[i+L] and S[j+L]
      rank_t ar = a.ranks[a_amt];
      rank_t br = b.ranks[b_amt];
      int ans;

      if( ar < br ) ans = -1;
      else if( ar > br ) ans = 1;
      else ans = 0;

      if (DEBUG_DCX>4) {
        printf("comparing: %lli / %lli \t a_amt=%i; b_amt=%i; ans=%i\n",
               (long long int) a.offset, (long long int) b.offset, a_amt, b_amt, ans);
      }

      // because of padding, we have the rare occasion of a tie. that is, both records have 0 rank (we padded with 0)
      //  these ties are simple to break by looking at the offsets
      if( ans == 0 ) {
        if( a.offset < b.offset ) ans = 1;
        else if( a.offset > b.offset ) ans = -1;
      }

      return ans;
    }
  };

  struct ByDoc {
    const Dcx* ctx;
    explicit ByDoc(const Dcx* ctx) : ctx(ctx) { }
    typedef return_key_criterion_tag criterion_category;
    typedef document_t key_t;
    key_t get_key(const doc_renumber_record_t& r) const
    {
      return r.doc;
    }
  };

  struct ByDocNextDoc {
    const Dcx* ctx;
    ByDocNextDoc(const Dcx* ctx) : ctx(ctx) { }

    // So it can be used as a record.
    typedef fixed_size_record_tag record_category;

    typedef return_key_criterion_tag criterion_category;

    typedef doc_renumber_record_t key_t;
    key_t get_key(const doc_renumber_record_t& r) const
    {
      return r;
    }
  };

  struct DocToNextDoc {
    // Convert from a splitter key to a sorting key.
    static
    doc_renumber_record_t
    splitter_key_to_key(ByDoc d, const document_t& in, bool is_for_min)
    {
      doc_renumber_record_t ret;
      ret.doc = in;
      if( is_for_min ) {
        ret.next_doc = 0;
      } else {
        ret.next_doc = d.ctx->n_docs;
      }
      return ret;
    }

    // Convert from a record to a splitter key.
    static
    document_t
    record_to_splitter_key(ByDoc d, const doc_renumber_record_t& in)
    {
      return in.doc;
    }
  };



  // Divide num/denom, but round up.
  // Do it in an overflow-safe way.
  template<typename Number>
  static
  Number ceildiv_safe(Number num, Number denom)
  {
    Number ret;
    ret = num/denom;
    if( ret*denom < num ) {
      ret++;
    }
    return ret;
  }

  // Call this one to set an offset to the appropriate value for recursion.
  offset_t down_offset( offset_t offset ) const
  {
    int cover_idx = cover.get_sample(offset % cover_t::period);
    if( EXTRA_CHECKS ) assert(0 <= cover_idx && cover_idx < cover_t::period);
    return characters_per_mod * cover_idx + offset/cover_t::period;
  }

  // Call this to restore an offset from recursion to the usual one
  offset_t up_offset( offset_t offset ) const
  {
    if( EXTRA_CHECKS ) assert( offset < characters_per_mod * cover_t::sample_size );
    int cover_idx = offset / characters_per_mod;
    if( EXTRA_CHECKS ) assert(0 <= cover_idx && cover_idx < cover_t::period);
    offset_t added = offset - characters_per_mod * cover_idx;
    return added*cover.period + cover.get_cover(cover_idx);
    /*} else {
      // it's padding at the end, we don't care about it.
      return n + (offset - sample_n);
    }*/
  }

  // Call this to go from an offset for recursion for a sample suffix
  // to a packed sample suffix offset outside of recursion.
  // These packed offsets exist to support fast permutation; the sample
  // offsets will be 0...sample_n. These will be in the same order as
  // they would in the original problem.
  offset_t up_to_packed_offset( offset_t offset ) const
  {
    if( EXTRA_CHECKS ) assert( offset < characters_per_mod * cover_t::sample_size );
    int cover_idx = offset / characters_per_mod;
    if( EXTRA_CHECKS ) assert(0 <= cover_idx && cover_idx < cover_t::period);
    offset_t added = offset - characters_per_mod * cover_idx;
    return added*cover.sample_size + cover_idx;
  }

  // Unpack a previously packed offset.
  offset_t unpack_offset( offset_t offset ) const
  {
    if( EXTRA_CHECKS ) assert( offset < characters_per_mod * cover_t::sample_size );
    int sample_idx = offset % cover_t::sample_size;
    offset_t before_chunks = offset / cover_t::sample_size;

    return before_chunks*cover_t::period + cover.get_cover(sample_idx);
  }


  // The types of the bins for the different suffix sorting stages.
  // These typedefs are making life much simpler...
  // since we don't have to think about the comparison routine each time.
  typedef Bins<Dcx,
               offset_rank_record_t,
               ByRank<offset_rank_record_t>,
               DividingSplitters<offset_rank_record_t,
                                 ByRank<offset_rank_record_t> >,
               PERMUTE > offset_rank_by_rank_t;
  typedef Bins<Dcx,
               offset_rank_record_t,
               ByOffset<offset_rank_record_t>,
               DividingSplitters<offset_rank_record_t,
                                 ByOffset<offset_rank_record_t> >,
               PERMUTE> offset_rank_by_offset_t;
  typedef Bins<Dcx,
               bwt_record_t,
               ByRank<bwt_record_t>,
               DividingSplitters<bwt_record_t,
                                 ByRank<bwt_record_t> >,
               PERMUTE> bwt_by_rank_t;

  typedef Bins<Dcx,
               doc_renumber_record_t,
               ByDoc,
               DividingSplitters<doc_renumber_record_t,
                                 ByDoc >,
               SORT > doc_next_by_doc_t;

  typedef Bins<Dcx,
               doc_renumber_record_t,
               ByDocNextDoc,
               DividingSplitters<doc_renumber_record_t,
                                 ByDoc >,
               SORT,
               IdentityFilterTranslator<Dcx,doc_renumber_record_t>,
               DocToNextDoc > doc_next_by_doc_next_t;



// For forming splitters on tuples.


  typedef SampleSplitters<offset_tuple_record_t,
                          ByCharactersAndOffset
                          > tuple_splitters_t;

  typedef Bins<Dcx,
               typename tuple_splitters_t::splitter_record_t,
               typename tuple_splitters_t::splitter_criterion_t,
               FixedSplitters<typename tuple_splitters_t::splitter_record_t,
                              typename tuple_splitters_t::splitter_criterion_t>,
               DONT_SORT > tuples_sample_t;

  typedef Bins<Dcx,
               offset_tuple_record_t,
               ByCharacters,
               tuple_splitters_t,
               SORT,
               IdentityFilterTranslator<Dcx,offset_tuple_record_t>,
               CharactersSplitterTranslator
               > tuples_by_rank_t;

  typedef Bins<Dcx,
               offset_m_name_record_t,
               ByOffset<offset_m_name_record_t>,
               DividingSplitters<offset_m_name_record_t,
                                 ByOffset<offset_m_name_record_t> >,
               PERMUTE> names_by_offset_t;

  struct UpToPackedOffsetFilterTranslator 
  {
    static offset_rank_record_t translate(const Dcx* dcx, const offset_rank_record_t& in, bool& keep) {
      offset_rank_record_t ret = in;
      offset_t offset = in.offset;
      offset_t packed = dcx->up_to_packed_offset(offset);
      offset_t up = dcx->up_offset(offset);
      if( EXTRA_CHECKS ) {
        offset_t unpacked = dcx->unpack_offset(packed);
        assert( up == unpacked );
      }
      ret.offset = packed;
      if( up >= dcx->n ) keep = false;
      else keep = true;
      if( DEBUG_DCX > 10 ) {
        printf("node %i up_to_packed %s gives %s\n",
               (int) dcx->iproc, in.to_string().c_str(), ret.to_string().c_str());
      }
      return ret;
    }
  };


  typedef Bins<Dcx,
               offset_rank_record_t,
               ByOffset<offset_rank_record_t>,
               DividingSplitters<offset_rank_record_t,
                                 ByOffset<offset_rank_record_t> >,
               PERMUTE,
               UpToPackedOffsetFilterTranslator> sample_ranks_by_offset_t;
  // NOTE -- sample_ranks_by_offset_t stores packed offsets.


  // For forming splitters on merge records.
  typedef SampleSplitters<merge_record_t,
                          ByNameAndDifferenceCover
                          > merge_splitters_t;

  struct UniqueToMerges {
    static
    merge_record_t translate(const Dcx* dcx, const merge_record_t& in, bool& keep)
    {
      keep = true;
      return in;
    }
    static
    merge_record_t translate(const Dcx* dcx, const offset_name_record_t& in, bool& keep)
    {
      merge_record_t r(in);
      keep = true;
      return r;
    }
  };

  typedef Bins<Dcx,
               typename merge_splitters_t::splitter_record_t,
               typename merge_splitters_t::splitter_criterion_t,
               FixedSplitters<typename merge_splitters_t::splitter_record_t,
                              typename merge_splitters_t::splitter_criterion_t>,
               DONT_SORT,
               UniqueToMerges> merges_sample_t;

  struct UniqueSplitterTranslator {
    static
    name_t splitter_key_to_key(ByNameAndDifferenceCover crit, const merge_record_t& in, bool is_for_min)
    {
      return in.name;
    }

    static
    merge_record_t record_to_splitter_key(ByNameAndDifferenceCover crit, const offset_name_record_t& in)
    {
      merge_record_t ret(in);
      return in;
    }
  };

  typedef Bins<Dcx,
               offset_name_record_t,
               ByName,
               //ByNameAndFirstRank, // technically ByName would do,
                                     // but we use this to make the 
                                     // types work out more easily
                                     // (since merge splitters can
                                     //  always be of type NameAndFirstRank).
               merge_splitters_t,
               SORT,
               IdentityFilterTranslator<Dcx,offset_name_record_t>,
               UniqueSplitterTranslator> unique_by_rank_t;
  // unique_by_rank and merge_by_rank must sort since they've got 
  //  different (sample/nonsample/unique) files for different types
  //  of ranks; plus we might have duplicate 0 ranks, and so permuting
  //  is not an option.


  typedef Bins<Dcx,
               merge_record_t,
               ByNameAndFirstRank,
               merge_splitters_t,
               SORT
               //IdentityFilterTranslator<Dcx,merge_record_t>,
               //MergeRecordToNameAndRank -- not needed
               // since record is the same.
               > merge_by_rank_t;

  template<typename Record>
  struct IdentityTranslator {
    static Record translate(const Record& r)
    {
      return r;
    }
  };

  struct OffsetMaybeNameToCharacter {
    static character_t translate(const offset_m_name_record_t& r)
    {
      return r.name.get_name();
    }
  };

  template<typename InCharacter>
  struct AddOneTranslator {
    static character_t translate(InCharacter in)
    {
      character_t in_char = in;
      character_t ret = in_char + 1;
      if( EXTRA_CHECKS ) {
        assert( ret > 0 );
        assert( in_char < ret ); // no overflow!
      }
      return ret;
    }
  };

  template<typename OutOffset>
  struct OffsetRankToOffset {
    static OutOffset translate(offset_rank_record_t r)
    {
      return r.offset;
    }
  };

  // MEMBER VARIABLES:
 

  const cover_t cover; // holds difference cover tables.

  // Stuff used here.
  std::string tmp_dir;

  // Totally key stuff.
  offset_t bin_overlap;
  offset_t n; // length of input
  offset_t characters_per_mod;
  offset_t sample_n; // number of sample suffixes (n for recursion)
  bin_idx_t n_bins;
  character_t max_char; 
  rank_t rank_bin_size;
  // These are for index construction.
  rank_t block_bin_size_rows; // how many rows per block bin?
  offset_t offset_bin_size;
  offset_t packed_offset_bin_size;
  document_t n_docs; // how many documents?
  document_t doc_bin_size_docs; // how many document numbers per doc bin?
  std::vector<document_t>* doc_renumber;

  int nproc; // number of processors.
  int iproc; // current processor rank.
  //size_t bins_per_proc;
  size_t depth;
  std::string depth_string;
  My_comm comm;

  tuple_splitters_t* tuple_whichbin_splitters;
  std::vector< tuple_splitters_t* > tuple_whichfile_splitters; // [n_bins];
  merge_splitters_t* merge_whichbin_splitters;
  std::vector< merge_splitters_t* > merge_unique_whichfile_splitters; // [n_bins];
  std::vector< merge_splitters_t* > merge_sample_whichfile_splitters; // [n_bins];
  std::vector< merge_splitters_t* > merge_nonsample_whichfile_splitters; // [n_bins];
  offset_t num_splitter_records_per_bin;

  //for compatibility with other code
  static const int TIMING = 0;
  //static const int DEBUG = 0; 
          // >0 = notes for recursion added
          // >1 = each node prints brief records
          // >2 = print output at each recursion level
          // >3 = each node prints verbose records instead
          // >4 = comparisons used by sort nodes are made explicit, 
 
  virtual void run()
  {
    assert(0); // should call suffix_sort_impl, etc. not run pipeline node.
  }
  bool should_print_timing() const
  {
    return false; // it's just too noisy!
    //return SHOULD_PRINT_TIMING;
  }

  // CONSTRUCTOR: Dcx constructor dcx constructor
  Dcx(std::string tmp_dir, offset_t n_in, bin_idx_t n_bins, My_comm in_comm, size_t depth, const pipeline_node* parent)
    : pipeline_node(parent, "dcx d="+num_to_string(depth), (PRINT_TIMING_DCX>0 && n_in>=PRINT_TIMING_DCX)),
      tmp_dir(tmp_dir),
      bin_overlap(cover.period),
      n(0), // set below
      characters_per_mod(0), // set below
      sample_n(0), // set below
      n_bins(n_bins), // set below
      rank_bin_size(0), // set below
      block_bin_size_rows(0),
      offset_bin_size(0), // set below
      packed_offset_bin_size(0), // set below
      n_docs(0), // set in bwt
      doc_bin_size_docs(0), // set in bwt
      doc_renumber(NULL), // set in bwt
      depth(depth),
      depth_string(num_to_string(depth)),
      comm(NULL_COMM),
      tuple_whichbin_splitters(NULL),
      tuple_whichfile_splitters(),
      merge_whichbin_splitters(NULL),
      merge_unique_whichfile_splitters(),
      merge_sample_whichfile_splitters(),
      merge_nonsample_whichfile_splitters(),
      num_splitter_records_per_bin(0)
  {
    if( ! dcx_g_handler ) {
      size_t lcm_sizes = lcm_record_sizes(1);
      lcm_sizes = SubDcx::lcm_record_sizes(lcm_sizes);
      size_t handler_tile_size = round_up_to_multiple(
           DEFAULT_TILE_SIZE*DEFAULT_TILES_PER_IO_GROUP,
           lcm_sizes);

      printf("Using a handler tile size of %li bytes\n", (long int) handler_tile_size);

      dcx_g_handler = new MPI_handler(handler_tile_size, NUM_MPI_SEND_TILES, in_comm);
    }


    assert(sizeof(character_t)<=sizeof(offset_t));
    // Make sure that there is room to store ranks.
    assert((size_t)(1+num_bits64(n))<=8*sizeof(offset_t));
    assert((size_t)(1+num_bits64(n))<=nbits_offset);

    assert( nbits_character <= 8*sizeof(character_t));
    assert( nbits_offset <= 8*sizeof(offset_t));

    iproc = 0; // index of this proc.
    nproc = 1;

#ifdef HAVE_MPI_H
    int rc;

    // this is a collective operation...
    rc = MPI_Comm_dup(in_comm, &comm);
    if( rc ) throw error(ERR_IO_STR("MPI_Comm_dup failed"));

    //rc = MPI_Comm_set_errhandler(comm, MPI_ERRORS_RETURN);
    //if( rc ) throw error(ERR_IO_STR("MPI_Comm_set_errhandler failed"));

    rc = MPI_Comm_size(comm,&nproc);
    if( rc ) throw error(ERR_IO_STR("MPI_Comm_size failed"));
    rc = MPI_Comm_rank(comm,&iproc);
    if( rc ) throw error(ERR_IO_STR("MPI_Comm_rank failed"));
#endif

    assert( n_bins >= nproc );

    set_n(n_in);


    //bins_per_proc = ceildiv(n_bins,nproc);
    //begin_bin = bins_per_proc * iproc;
    //end_bin = bins_per_proc * (iproc+1);
    //if( end_bin > n_bins ) end_bin = n_bins;

    //if( DEBUG_DCX ) {
    //  std::cout << "node " << iproc << " begin_bin=" << begin_bin << " end_bin=" << end_bin << " bins_per_proc=" << bins_per_proc << std::endl;
    //}
  }
  ~Dcx()
  {
#ifdef HAVE_MPI_H
    if( comm != MPI_COMM_NULL ) {
      MPI_Comm_free(&comm);
    }
#endif
    if( tuple_whichbin_splitters ) {
      delete tuple_whichbin_splitters;
      tuple_whichbin_splitters = NULL;
    }
    for( size_t i = 0; i < tuple_whichfile_splitters.size(); i++ ) {
      if(tuple_whichfile_splitters[i]) {
        delete tuple_whichfile_splitters[i];
        tuple_whichfile_splitters[i] = NULL;
      }
    }
    if( merge_whichbin_splitters ) {
      delete merge_whichbin_splitters;
      merge_whichbin_splitters = NULL;
    }
    for( size_t i = 0; i < merge_unique_whichfile_splitters.size(); i++ ) {
      delete merge_unique_whichfile_splitters[i];
      merge_unique_whichfile_splitters[i] = NULL;
    }
    for( size_t i = 0; i < merge_sample_whichfile_splitters.size(); i++ ) {
      delete merge_sample_whichfile_splitters[i];
      merge_sample_whichfile_splitters[i] = NULL;
    }
    for( size_t i = 0; i < merge_nonsample_whichfile_splitters.size(); i++ ) {
      delete merge_nonsample_whichfile_splitters[i];
      merge_nonsample_whichfile_splitters[i] = NULL;
    }
  }

  static
  size_t lcm_record_sizes(size_t s)
  {
    uint64_t ret = s;
    ret = lcm64(ret, RecordTraits<offset_rank_record_t>::record_size);
    ret = lcm64(ret, RecordTraits<merge_record_t>::record_size);
    ret = lcm64(ret, RecordTraits<offset_name_record_t>::record_size);
    ret = lcm64(ret, RecordTraits<offset_m_name_record_t>::record_size);
    ret = lcm64(ret, RecordTraits<recursion_input_record_t>::record_size);
    ret = lcm64(ret, RecordTraits<offset_tuple_record_t>::record_size);
    ret = lcm64(ret, RecordTraits<bwt_record_t>::record_size);
    assert( ret < 10*DEFAULT_TILE_SIZE );
    return ret;
  }

  void set_n(offset_t n_in)
  {
    n = n_in;

    // characters_per_mod includes at least one NULL character after each set;
    // in other words, we pretend that there are cover.period NULLs at the
    // end of the input string, and we also add a character to make sure that
    // we have a 0 character between each subproblem in recursion.
    characters_per_mod = 1 + ceildiv(n,cover.period);
        //  (n+cover.period)/cover.period + 1;

    {
      offset_t plus_period = n_in + cover.period;
      offset_t n_bins_r = n_bins;
      //printf("n_bins %i n_bins_r %i\n", (int) n_bins, (int) n_bins_r);
      offset_bin_size = ceildiv_safe(plus_period, n_bins_r);
      // Bad things happen if the overlap would
      // require us to put records in more than 2 bins.
      if( offset_bin_size < bin_overlap ) {
        offset_bin_size = bin_overlap;
      }
      // Round offset_bin_size to a multiple of cover.period.
      // This is important to coordinate the sample ranks bins,
      // which have a different numbering system (packed offsets)
      // with the rest of the algorithm.
      offset_t cov_period = cover.period;
      offset_t period_multiples = ceildiv_safe(offset_bin_size, cov_period);
      offset_bin_size = period_multiples * cov_period;

      packed_offset_bin_size = period_multiples * cover.sample_size;

      if( n > 0 ) {
        bin_idx_t last_bin = (n-1)/offset_bin_size;
        assert( last_bin < n_bins );
      }
    }


    rank_bin_size = offset_bin_size;

    // set sample_n
    {
      // set sample_n to a default so it can be used below
      sample_n = characters_per_mod * cover.sample_size;

      // now compute the proper sample_n by using the maximum value
      // in the sub-problems. We're doing this so that we can avoid
      // having zeros padding the end of the string, since repeated
      // zeros at the end become nonunique and then the algorithm
      // doesn't terminate.
      //printf("sample_n %i\n", (int) sample_n);
      offset_t biggest_sub = 0;
      offset_t start = 0;
      if( n > cover.period ) start = n - cover.period;

      for( offset_t i = start; i < n; i++ ) {
        if( cover.in_cover(i % cover.period) ) {
          offset_t sub_offset = down_offset(i);
          if( sub_offset > biggest_sub ) biggest_sub = sub_offset;
        }
      }

      //printf("biggest_sub %i\n", (int) biggest_sub);
      // this should just be making a tighter bound.
      assert( biggest_sub < sample_n );
      sample_n = biggest_sub + 1;

      if( n == 0 ) sample_n = 0;
    }

    // set num_splitter_records_per_bin too.
    uint64_t mem = USE_MEMORY_PER_BIN;
    uint64_t n_u = n;
    uint64_t num_splitter = ceildiv_safe(n_u,mem);
    num_splitter *= NUM_SPLITTER_RECORDS_PER_FILE;
    num_splitter /= n_bins; // since we do it per bin.
    num_splitter += NUM_SPLITTER_RECORDS_PER_BIN_ADD;
    if( num_splitter > n_u ) num_splitter = n_u;
    num_splitter_records_per_bin = num_splitter;
    
    if( DEBUG_DCX && iproc == 0) {
      printf("at depth %i set_n(%lli) offset_bin_size=%lli rank_bin_size=%lli characters_per_mod=%lli sample_n=%lli num_splitter=%lli\n",
              (int) depth, (long long int) n_in, (long long int) offset_bin_size, (long long int) rank_bin_size,
              (long long int) characters_per_mod, (long long int) sample_n, (long long int) num_splitter_records_per_bin);
    }
  }

  void set_maxchar(character_t max_char_in)
  {
    assert(sizeof(character_t) <= sizeof(rank_t));

    max_char = max_char_in;

    if( DEBUG_DCX && iproc == 0 ) {
      printf("at depth %i set_maxchar(%i)\n", (int) depth, (int) max_char);
    }
  }

  offset_tuple_record_t min_offset_tuple_record()
  {
    typedef typename offset_tuple_record_t::character_t char_t;
    char_t chars[cover_t::period];

    memset(chars, 0x00, sizeof(chars));

    for( int i = 0; i < cover_t::period; i++ ) {
      assert(chars[i] == 0);
    }

    offset_tuple_record_t ret;
    ret.characters.set_characters(chars);
    ret.offset = 0;

    return ret;
  }

  offset_tuple_record_t max_offset_tuple_record()
  {
    typedef typename offset_tuple_record_t::character_t char_t;
    char_t chars[cover_t::period];

    for( int i = 0; i < cover_t::period; i++ ) {
      chars[i] = max_char;
    }

    offset_tuple_record_t ret;
    ret.characters.set_characters(chars);
    ret.offset = n;

    return ret;
  }

  merge_record_t min_merge_record()
  {
    merge_record_t ret;
    ret.offset = n+1; 
    ret.name = 0;
    for( int i = 0; i < cover_t::sample_size; i++ ) {
      ret.ranks[i] = 0;
    }
    return ret;
  }

  merge_record_t max_merge_record()
  {
    merge_record_t ret;
    ret.offset = 0;
    ret.name = n+1;
    for( int i = 0; i < cover_t::sample_size; i++ ) {
      ret.ranks[i] = n+1;
    }
    return ret;
  }
  /* Given (offset, character) records already distributed into offset bins
     finish permuting them into place. Then create tuple records and 
     distribute them to the rank bins. Note that we are storing the
     tuple records for *the entire input* and not just sample suffixes.
     This is an improvement over the original algorithm since we can
     just use a rank instead of a tuple in the merge records.
    
     trans.translate(r) must return a character_t
   */
  template<typename InputBins,
           typename InputToCharacterTranslator >
  struct form_tuples_node : public pipeline_node {
    const Dcx* dcx;
    // INPUT
    InputBins* input_bins;
    size_t max_records;
    // OUTPUT
    tuples_by_rank_t* tuples_by_rank;
    tuples_sample_t* sample_output;

    form_tuples_node(const Dcx* dcx, InputBins* input_bins, size_t max_records, tuples_by_rank_t* tuples_by_rank, tuples_sample_t* sample_output)
      : pipeline_node(dcx, "form_tuples d="+dcx->depth_string, dcx->should_print_timing()),
        dcx(dcx), input_bins(input_bins), max_records(max_records), tuples_by_rank(tuples_by_rank), sample_output(sample_output)
    {
      // One, but not both, of tuples_by_rank and sample_output must be set.
      assert(sample_output || tuples_by_rank);
      assert( ! (sample_output && tuples_by_rank) );
    }

    virtual void run()
    {
      for( bin_idx_t b = 0; b < input_bins->n_bins; b++ ) {
        if( input_bins->is_local(b) ) {
          input_bins->begin_reading_bin(b);
          form_tuples(b);
        }
      }
      if( tuples_by_rank ) tuples_by_rank->finish();
      if( sample_output ) sample_output->finish();
    }

    void form_tuples( bin_idx_t bin )
    {
      typename InputBins::bin_t* input_by_offset = &input_bins->bins[bin];
      typedef typename InputBins::record_t input_record_t;

      if( DEBUG_DCX ) {
        printf("node %i form_tuples bin=%i fname is %s num_records is %lli num_before is %lli\n",
               (int) dcx->iproc, (int) bin, input_by_offset->fname.c_str(), 
               (long long int) input_by_offset->num_records,
               (long long int) input_by_offset->num_before);
      }

      // permute the bins into place...  must have already been done!
      
      windowed_pipe_iterator<input_record_t> read(input_by_offset->from_bin, cover_t::period);
      windowed_pipe_iterator<input_record_t> end;

      // now create tuple records.
      // We create the tuple records by going through the (offset, character) records.
      // Read the file.
    
      // do work for loop
      assert(input_by_offset->total_computed);

      offset_t offset = input_by_offset->num_before;

      if( bin == input_bins->n_bins - 1 ) assert(dcx->n == input_by_offset->num_before + input_by_offset->num_records);

      for( offset_t i = 0;
          i < input_by_offset->num_records &&
          (max_records == 0 || i < max_records); i++) {
        /*if( DEBUG_DCX > 10 ) {
          for( size_t d = 0; d < read.size(); d++ ) {
            std::cout << "node " << iproc << " in " << d << ": " << read[d].to_string() << std::endl;
          }
        }*/

        if( EXTRA_CHECKS ) {
          assert(read != end);
          assert(offset < dcx->n);
        }

        offset_tuple_record_t tuple;

        tuple.offset = offset;

        {
          character_t chars[cover_t::period];
          for( int k = 0; k < cover_t::period; k++ ) {
            // we should have space from the end-of-bin padding...
            if( k < (int) read.size() ) {
              chars[k] = InputToCharacterTranslator::translate(read[k]);
            } else {
              chars[k] = 0; // 0 is reserved for end-of-string.
            }
          }
          tuple.characters.set_characters(chars);
        }

        if( DEBUG_DCX > 10 ) {
          printf("node %i out: %s\n", dcx->iproc, tuple.to_string().c_str());
        }

        // Output the record we made.
        if( tuples_by_rank ) tuples_by_rank->push_back(tuple);
        if( sample_output ) {
          sample_output->push_back_bins(tuple, bin, bin);
          // set the offset to a random offset.
          for( int i = 0; i < NUM_RANDOM_DOCTORS; i++ ) {
            tuple.offset = dcx->rand_offset();
            sample_output->push_back_bins(tuple, bin, bin);
          }
        }

        // Move our sliding window forward
        ++read;
        offset++;
      }

      read.finish();

      // Now the data is in the rank bins (partly sorted by tuple values).
    }
  };



  /*
  Sort the tuple records in each rank bin to get (tuple,offset) records.
  Set the offset bin size for the recursion.
  Check to see if the tuples are unique. Distribute newly named
  (offset,rank) records into sample and nonsample offset bins.

  Outputs to 2 sets of bins: sample suffix (by offset) prepared for recursion 
   (ie down_offset has been called) and nonsample suffix (by offset) that
   we'll use when 
   returns 1 if the tuple values are unique.
   */
  struct sort_and_name_node : public pipeline_node {
    const Dcx* dcx;
    // IN
    tuples_by_rank_t* tuples_by_rank_bins;
    // OUT
    typename SubDcx::names_by_offset_t* recursion_input_bins;
    names_by_offset_t* all_out_bins;
    name_t max_name;
    bool unique;
    offset_t num_unique;


    sort_and_name_node( const Dcx* dcx,
                        tuples_by_rank_t* tuples_by_rank_bins,
                        typename SubDcx::names_by_offset_t* recursion_input_bins,
                        names_by_offset_t* all_out_bins)
      : pipeline_node(dcx, "sort_and_name d="+dcx->depth_string, dcx->should_print_timing()),
        dcx(dcx),
        tuples_by_rank_bins(tuples_by_rank_bins),
        recursion_input_bins(recursion_input_bins),
        all_out_bins(all_out_bins),
        max_name(0),
        unique(true),
        num_unique(0)
    {
    }

    virtual void run()
    {
      for( bin_idx_t b = 0; b < tuples_by_rank_bins->n_bins; b++ ) {
        if( tuples_by_rank_bins->is_local(b) ) {
          tuples_by_rank_bins->begin_reading_bin(b);

          name_t bin_max_name = 0;
          bool bin_unique = true;
          sort_and_name(b, &bin_max_name, &bin_unique);
          max_name = std::max(max_name, bin_max_name);
          unique &= bin_unique;
        }
      }
      recursion_input_bins->finish();
      all_out_bins->finish();
    }

    /* Decides based on an offset if it is destined for the recursion
     * or just all_out.
     * If it goes into the recursion bin, we call down_offset on it.
     */
    void store_offset_name( offset_m_name_record_t & offset_name)
    {
      if( DEBUG_DCX > 10 ) {
        printf("node %i all_out: %s\n", (int) dcx->iproc, offset_name.to_string().c_str());
      }

      all_out_bins->push_back(offset_name);

      // Now decide if we need to store in recursion_input
      if( dcx->cover.in_cover(offset_name.offset % cover_t::period) ) {
        offset_m_name_record_t for_recursion = offset_name;
        for_recursion.offset = dcx->down_offset(for_recursion.offset);
        if( DEBUG_DCX > 10 ) {
          printf("node %i recursion_out: %s\n", (int) dcx->iproc, for_recursion.to_string().c_str());
        }
        if( EXTRA_CHECKS ) {
          // Check that up_offset(down_offset(x)) == x.
          offset_t down_up = dcx->up_offset(for_recursion.offset);
          if( down_up != offset_name.offset ) {
            printf("node %i down_up=%lli offset=%lli cover.period=%i characters_per_mod=%lli sample_n=%lli\n",
                   (int) dcx->iproc, (long long int) down_up, (long long int) offset_name.offset,
                   (int) cover_t::period, (long long int) dcx->characters_per_mod,
                   (long long int) dcx->sample_n);
          }
          // Check that unpack_offset(up_to_packed_offset(down_offset(x))) == x
          offset_t packed = dcx->up_to_packed_offset(for_recursion.offset);
          offset_t unpacked = dcx->unpack_offset(packed);
          assert( unpacked == offset_name.offset );
        }

        recursion_input_bins->push_back(for_recursion);
      }
    }


    void sort_and_name(bin_idx_t bin, name_t* bin_max_name, bool* bin_unique)
    {
      typename tuples_by_rank_t::bin_t* tuple_by_rank = &tuples_by_rank_bins->bins[bin];

      if( DEBUG_DCX ) {
        printf("node %i sort_and_name bin=%i num_records_with_overlap=%lli\n",
               (int) dcx->iproc, (int) bin, (long long int) tuple_by_rank->num_records_with_overlap);
      }

      typename tuples_by_rank_t::comparator_t & comparator = tuples_by_rank_bins->comparator;

      // Add the NULL padding between the characters_per_mod sets
      // in the recursion input that we're preparing. 
      // We also pad out the last bin in order to make the input
      // size exactly cover.sample_size * characters_per_mod.
      // Do this only for bin 0.
      if( bin == 0 ) {
        offset_t end;
        
        end = dcx->up_offset(dcx->characters_per_mod * cover_t::sample_size - 1);

        for( offset_t i = dcx->n; i <= end; i++ ) {
          if( dcx->cover.in_cover(i % cover_t::period) ) {
            //offset_m_name_t offset_name;
            //offset_name.offset = i;
            //offset_name.name.set(false, 0);
            recursion_input_record_t for_recursion;
            for_recursion.offset = dcx->down_offset(i);
            for_recursion.name.set(false, 0);
            if( for_recursion.offset < dcx->sample_n ) {
              if( DEBUG_DCX > 10 ) {
                printf("inr_pad: i=%lli %s\n", (long long int) i, for_recursion.to_string().c_str());
              }
              recursion_input_bins->push_back(for_recursion);
              //sample_out_bins->push_back(offset_name);
            }
          }
        }
      }



      bool all_are_unique = true;
      assert(tuple_by_rank->total_computed);
      rank_t start_name = 1; // always name from 1.
      offset_tuple_record_t min_record = tuple_by_rank->min;
      offset_tuple_record_t max_record = tuple_by_rank->max;
      rank_t min_record_name = 0;
      rank_t max_record_name = 0;
      bool min_record_not_unique;
      bool max_record_not_unique;

      // Set min_record_name and max_record name based
      // on min,max records of this and previous bins.
      {
        // Find the first previous bin that starts with a max
        // equal to our min_record.
        // Here we rely on bin_idx_t being signed.
        {
          bin_idx_t test = -1;
          assert( test < 0 );
        }

        bin_idx_t equal_bin = -1;
        for( bin_idx_t prev_bin = bin-1;
             prev_bin >= 0;
             prev_bin-- ) {
          if( tuples_by_rank_bins->bins[prev_bin].num_records > 0 ) {
            if( comparator.equals(min_record,
                                  tuples_by_rank_bins->bins[prev_bin].max) ) {
              equal_bin = prev_bin;
            } else {
              break;
            }
          }
        }

        // Did we find a previous equal bin?
        if( equal_bin >= 0 ) {
          // not unique if there's a previous bin with that record.
          min_record_not_unique = true;

          // OK. Now the name for our min record is the name for the
          // maximum record in that bin.
          // Was it the minimum record in that bin?
          if( comparator.equals(min_record, tuples_by_rank_bins->bins[equal_bin].min) ) {
            min_record_name = start_name +
                              tuples_by_rank_bins->bins[equal_bin].num_before;
          } else {
            min_record_name = start_name +
                              tuples_by_rank_bins->bins[equal_bin].num_before +
                              tuples_by_rank_bins->bins[equal_bin].num_records - 1;
          }
        } else {
          min_record_not_unique = false; // maybe it's unique.
          min_record_name = start_name +
                            tuples_by_rank_bins->bins[bin].num_before;
        }

        // Our maximum record name is either:
        //  ... equal to min_record_name if min == max
        //  ... start_name + [bin]->n_before + [bin]->num_records - 1
        if( comparator.equals(min_record, max_record) ) {
          max_record_name = min_record_name;
        } else {
          max_record_name = start_name +
                            tuples_by_rank_bins->bins[bin].num_before +
                            tuples_by_rank_bins->bins[bin].num_records - 1;
        }

        // To decide max_record_not_unique, is there a later
        // bin containing that record?
        
        max_record_not_unique = false; // maybe it's unique.
        for( bin_idx_t next_bin = bin + 1;
             next_bin < tuples_by_rank_bins->n_bins;
             next_bin++ ) {
          if( tuples_by_rank_bins->bins[next_bin].num_records > 0 ) {
            if( comparator.equals(max_record,
                                  tuples_by_rank_bins->bins[next_bin].min) ) {
                max_record_not_unique = true;
            }
            break; // we have set it appropriately.
          }
        }
      }

      rank_t current_name = min_record_name;
      rank_t next_name_gaps = current_name; // next name with gaps
      rank_t next_name_nogaps = current_name; // next name without gaps
      windowed_pipe_iterator<offset_tuple_record_t> read(tuple_by_rank->from_bin, 2);
   
#define USE_NEXT next_name_nogaps

      for( offset_t i = 0; i < tuple_by_rank->num_records; ) {

        if( EXTRA_CHECKS ) {
          assert((*read).offset < dcx->n);
          assert(! comparator.less(*read, min_record) );
          assert(! comparator.less(max_record, *read) );
        }

        bool is_zero = read[0].characters.is_zero();
        // Force all-zero-characters to count as 'non-unique'
        // so that we use merge records to break the ties,
        // since zero is used for end-of-string padding
        // and also might be an input character.
        bool equals_next = (read.size() >= 2) && comparator.equals(read[0],read[1]);
        bool equals_min = comparator.equals(min_record, *read);
        bool equals_max = comparator.equals(max_record, *read);
        bool equals = equals_next ||
                      (equals_min && min_record_not_unique) ||
                      (equals_max && max_record_not_unique);

        if( is_zero || equals ) {
          offset_tuple_record_t start_equal = read[0];

          // Output some non-unique records.
          if( equals ) all_are_unique = false;

          current_name = USE_NEXT;
          do {
            offset_m_name_record_t offset_name;
            offset_name.offset = (*read).offset;
            if( equals_min ) {
              offset_name.name.set(false, min_record_name);
            } else if( equals_max ) {
              offset_name.name.set(false, max_record_name);
            } else {
              offset_name.name.set(false, current_name);
            }

            if( DEBUG_DCX > 10 ) {
              printf("node %i in (dup): %s\n",
                     (int) dcx->iproc, (*read).to_string().c_str());
              printf("node %i out: %s\n",
                     (int) dcx->iproc, offset_name.to_string().c_str());
            }

            // Now place the offset_name record.
            store_offset_name(offset_name);

            next_name_gaps++;
            // move on to the next record!
            ++read;
            i++;
          } while( i < tuple_by_rank->num_records && 
                   comparator.equals(start_equal, *read ));
          next_name_nogaps++;
        } else {
          // output a unique record
          offset_m_name_record_t offset_name;

          num_unique++;

          current_name = USE_NEXT;

          offset_name.offset = (*read).offset;
          if( equals_min ) {
            offset_name.name.set(true, min_record_name);
          } else if( equals_max ) {
            offset_name.name.set(true, max_record_name);
          } else {
            offset_name.name.set(true, current_name);
          }

          if( DEBUG_DCX > 10 ) {
            printf("node %i in (unique): %s\n",
                   (int) dcx->iproc, (*read).to_string().c_str());
            printf("node %i out: %s\n",
                   (int) dcx->iproc, offset_name.to_string().c_str());
          }

          // Now place the offset_rank record.
          store_offset_name(offset_name);

          next_name_gaps++;
          next_name_nogaps++;
          // move on to the next record!
          ++read;
          i++;
        }
      }

      read.finish();

      *bin_unique = all_are_unique;
      // not current_name in order to keep room for all ranks in recursion..
      //if( USE_NEXT > 0 ) *bin_max_name = USE_NEXT - 1;
      //else *bin_max_name = 0;
      *bin_max_name = max_record_name;
#undef USE_NEXT
    }
  };
   
  /* If all of the names are unique, we need to translate the
   * names and return them as ranks.
   */
  template<typename OutputBins> // some kind of Bins
  struct output_unique_node : public pipeline_node {
    const Dcx* dcx;
    names_by_offset_t* all_names_by_offset_bins;
    OutputBins* output;

    output_unique_node(const Dcx* dcx,
                       // IN
                       names_by_offset_t* all_names_by_offset_bins,
                       // OUT
                       OutputBins* output)
      : pipeline_node(dcx, "output_unique d="+dcx->depth_string, dcx->should_print_timing()),
        dcx(dcx),
        all_names_by_offset_bins(all_names_by_offset_bins),
        output(output)
    {
    }
    virtual void run()
    {
      for(bin_idx_t b=0;b < all_names_by_offset_bins->n_bins; b++ ) {
        if( all_names_by_offset_bins->is_local(b) ) {
          all_names_by_offset_bins->begin_reading_bin(b);

          output_unique(b);
        }
      }
      output->finish();
    }

    void output_unique(bin_idx_t bin)
    {
      typename names_by_offset_t::bin_t* name_by_offset = &all_names_by_offset_bins->bins[bin];

      if( DEBUG_DCX ) {
        printf("node %i output_unique bin=%i\n", (int) dcx->iproc, (int) bin);
      }

      // permute the bins into place.
      pipe_iterator<offset_m_name_record_t> read(name_by_offset->from_bin);
      pipe_iterator<offset_m_name_record_t> end;

      for( offset_t i = 0; i < name_by_offset->num_records; ) {
        // sample suffix.
        if( EXTRA_CHECKS ) assert(read != end);

        if( DEBUG_DCX > 10 ) {
          printf("node %i name_by_offset_in: %s\n", (int) dcx->iproc, (*read).to_string().c_str());
        }

        // output it!
        offset_rank_record_t r;
        name_t name = (*read).name.get_name();
        r.offset = (*read).offset;
        r.rank = name - 1;

        if( EXTRA_CHECKS ) {
          assert(name > 0);
          assert(r.offset < dcx->n);
          assert(r.rank < dcx->n);
        }

        if( DEBUG_DCX > 10 ) {
          printf("node %i out: %s\n", (int) dcx->iproc, r.to_string().c_str());
        }

        output->push_back(r);
        // Move past this sample one
        ++read;
        i++;
      }
      read.finish();
    }
  };

 
  /* Convert offset_by_rank into Offset records,
   * and store these in the output "bins".
   */
  template< typename OutOffset >
  struct output_offsets_node : public pipeline_node 
  {
    const Dcx* dcx;
    offset_rank_by_rank_t* offset_rank_by_rank_bins;
    std::vector<std::string>* output_fnames;

    output_offsets_node(Dcx* dcx,
                        // IN
                        offset_rank_by_rank_t* offset_rank_by_rank_bins,
                        // OUT
                        std::vector<std::string>* output_fnames
                       )
      : pipeline_node(dcx, "output_offsets d="+dcx->depth_string, dcx->should_print_timing()),
        dcx(dcx),
        offset_rank_by_rank_bins(offset_rank_by_rank_bins),
        output_fnames(output_fnames)
    {
      assert(output_fnames->size()==(size_t) offset_rank_by_rank_bins->n_bins);
    }

    virtual void run()
    {
      for( bin_idx_t b = 0; b < offset_rank_by_rank_bins->n_bins; b++ ) {
        if( offset_rank_by_rank_bins->is_local(b) ) {
          offset_rank_by_rank_bins->begin_reading_bin(b);

          output_offsets(b);
        }
      }
    }
    void output_offsets( bin_idx_t bin)
    {
      typename offset_rank_by_rank_t::bin_t* offset_rank_by_rank = &offset_rank_by_rank_bins->bins[bin];

      if( DEBUG_DCX ) {
        printf("node %i output_offsets bin=%i\n", (int) dcx->iproc, (int) bin);
      }

      file_pipe_context out_ctx = get_file_pipe_context<OutOffset>((*output_fnames)[bin]);
      file_write_pipe_t write_pipe(out_ctx);
      pipe_back_inserter<OutOffset> writer(&write_pipe);

      pipe_iterator<offset_rank_record_t> read(offset_rank_by_rank->from_bin);
      pipe_iterator<offset_rank_record_t> end;

      while( read != end ) {
        if( DEBUG_DCX > 10 ) {
          printf("node %i in : %s\n", (int) dcx->iproc, (*read).to_string().c_str());
          printf("node %i out : %lli\n", (int) dcx->iproc, (long long int) (*read).offset);
        }
        offset_rank_record_t r = *read;

        if( EXTRA_CHECKS ) {
          assert(r.offset < dcx->n);
        }

        writer.push_back( r.offset );

        ++read;
      }

      writer.finish();
      read.finish();
    }
  };

  /*
  Given (rank,offset) records in offset bins, finish permuting them in
  to place. Then, use the input in this region to form merge records.
  Form tuples for the merge records that also include the ranks. Distribute
  the merge records by the tuple value into rank bins.

  */
  struct form_merge_records_node : public pipeline_node
  {
    const Dcx* dcx;
    // IN
    sample_ranks_by_offset_t* sample_by_offset_bins;
    names_by_offset_t* name_by_offset_bins;
    offset_t max_records; // used to make splitters,0 means as many as there are.
    // OUT
    // unique includes unique sample.
    unique_by_rank_t* unique_by_rank;
    merge_by_rank_t* sample_by_rank;
    merge_by_rank_t** nonsample_by_rank;
    merges_sample_t* splitter_output;

    form_merge_records_node(
                       const Dcx* dcx,
                       // IN
                       sample_ranks_by_offset_t* sample_by_offset_bins,
                       names_by_offset_t* name_by_offset_bins,
                       offset_t max_records, // used to make splitters,0 means as many as there are.
                       // OUT
                       // unique includes unique sample.
                       unique_by_rank_t* unique_by_rank,
                       merge_by_rank_t* sample_by_rank,
                       merge_by_rank_t** nonsample_by_rank, //[cover_t::nonsample_size] 
                       merges_sample_t* splitter_output
                       )
      : pipeline_node(dcx, "form_merge d="+dcx->depth_string, dcx->should_print_timing()),
        dcx(dcx),
        sample_by_offset_bins(sample_by_offset_bins),
        name_by_offset_bins(name_by_offset_bins),
        max_records(max_records),
        unique_by_rank(unique_by_rank),
        sample_by_rank(sample_by_rank),
        nonsample_by_rank(nonsample_by_rank),
        splitter_output(splitter_output)
    {
      // We should have either splitter_output or the others.
      if( splitter_output ) {
        assert(!unique_by_rank);
        assert(!sample_by_rank);
        assert(!nonsample_by_rank);
      } else {
        assert(unique_by_rank);
        assert(sample_by_rank);
        assert(nonsample_by_rank);
        for( int j = 0; j < cover_t::nonsample_size; j++ ) {
          assert( nonsample_by_rank[j] );
        }
      }
    }

    virtual void run()
    {
      for( bin_idx_t b = 0; b < sample_by_offset_bins->n_bins; b++ ) {
        if( sample_by_offset_bins->is_local(b) ) {
          sample_by_offset_bins->begin_reading_bin(b);
          assert(name_by_offset_bins->is_local(b));
          name_by_offset_bins->begin_reading_bin(b);

          form_merge_records(b);
        } else {
          assert(! name_by_offset_bins->is_local(b));
        }
      }
      if( ! splitter_output ) {
        unique_by_rank->finish();
        sample_by_rank->finish();
        for( int j = 0; j < cover_t::nonsample_size; j++ ) {
            nonsample_by_rank[j]->finish();
        }
      } else {
        splitter_output->finish();
      }
    }

    void form_merge_records( bin_idx_t bin )
    {
      typename sample_ranks_by_offset_t::bin_t* sample_by_offset = &sample_by_offset_bins->bins[bin];
      typename names_by_offset_t::bin_t* name_by_offset = &name_by_offset_bins->bins[bin];

      if( DEBUG_DCX ) {
        printf("node %i form_merge_records bin %i\n", (int) dcx->iproc, (int) bin);
      }

      // assumes permuting already done!
      
      windowed_pipe_iterator<offset_rank_record_t> read_sample(sample_by_offset->from_bin, cover_t::sample_size);
      windowed_pipe_iterator<offset_rank_record_t> read_sample_end;
      pipe_iterator<offset_m_name_record_t> read_name(name_by_offset->from_bin);
      pipe_iterator<offset_m_name_record_t> read_name_end;

      offset_t min_offset = bin * dcx->offset_bin_size;
      offset_t max_offset = min_offset +
                            name_by_offset->num_records;

      for( offset_t offset = min_offset, i = 0;
           offset < max_offset && (max_records == 0 || i < max_records);
           offset++,i++ ) {

        // Are we on a sample or nonsample suffix?
        bool is_sample_offset = dcx->cover.in_cover(offset % cover_t::period);

        if( EXTRA_CHECKS ) {
          if( is_sample_offset ) { 
            assert(read_sample != read_sample_end);
            assert(read_name != read_name_end);

            offset_t packed = (*read_sample).offset;
            offset_t unpacked = dcx->unpack_offset(packed);

            assert( unpacked == offset );
          }
          assert((*read_name).offset == offset);
          assert(offset < dcx->n);
        }

        if( DEBUG_DCX > 10 && is_sample_offset ) {
          printf("node %i sample_in: %s\n", (int) dcx->iproc, (*read_sample).to_string().c_str());
        }
        if( DEBUG_DCX > 10 ) {
          printf("node %i name_in: %s\n", (int) dcx->iproc, (*read_name).to_string().c_str());
        }

        if( ! splitter_output ) {
          // Output to either sample_by_rank or unique_by_rank,
          // depending on how unique the name is.
          // Was the name unique?
          if( (*read_name).name.is_unique() ) {
            offset_name_record_t r;
            r.offset = (*read_name).offset;
            r.name = (*read_name).name.get_name();
            if( DEBUG_DCX > 10 ) {
              printf("node %i unique_out: %s\n", (int) dcx->iproc, r.to_string().c_str());
            }
            // Output to unique bins.
            unique_by_rank->push_back(r);
          } else {
            // Output a merge record.
            merge_record_t r;
            r.offset = (*read_name).offset;
            r.name = (*read_name).name.get_name();
            for( int d = 0; d < cover_t::sample_size; d++ ) {
              // Just get the ranks of the next several cover elements.
              if( (size_t) d < read_sample.size() ) r.ranks[d] = read_sample[d].rank;
              else r.ranks[d] = 0;
            }

            if( is_sample_offset ) {
              if( DEBUG_DCX > 10 ) {
                printf("node %i sample_out: %s\n", (int) dcx->iproc, r.to_string().c_str());
              }
              sample_by_rank->push_back(r);
            } else {
              int d = dcx->cover.get_nonsample(offset % cover_t::period); 

              if( DEBUG_DCX > 10 ) {
                printf("node %i nonsample_out[%i]: %s\n", (int) dcx->iproc, (int) d, r.to_string().c_str());
              }
              // Save the output.
              nonsample_by_rank[d]->push_back(r);
            }
          }
        } else {
          merge_record_t r;
          r.offset = (*read_name).offset;
          r.name = (*read_name).name.get_name();
          for( int d = 0; d < cover_t::sample_size; d++ ) {
            // Just get the ranks of the next several cover elements.
            if( (size_t) d < read_sample.size() ) r.ranks[d] = read_sample[d].rank;
            else r.ranks[d] = 0;
          }

          splitter_output->push_back_bins(r, bin, bin);
          /*

             The code below really helps distribution
             of data with all-zeros input, but it also
             causes unit tests to fail.
             Have not tracked down the problem.

          r.offset -= r.offset % cover_t::period;
          for( int d = 0; d < cover_t::sample_size; d++ ) {
            // all same ranks -> transitive comparison function.
            r.ranks[d] = r.ranks[0];
          }
          splitter_output->push_back_bins(r, bin, bin);

          for( int i = 0; i < NUM_RANDOM_DOCTORS; i++ ) {
            // Here, as above, we add this record, but 
            // we make the ranks all the same so that
            // it doesn't screw us up (all same ranks->
            // transitive comparison function).
            // set the offset to a random offset.
            r.offset = dcx->rand_sample_offset();
            rank_t rank = dcx->rand_rank();
            for( int d = 0; d < cover_t::sample_size; d++ ) {
              r.ranks[d] = rank;
            }
            splitter_output->push_back_bins(r, bin, bin);
          }*/
          
        }

        // Advance to the next set of sample ranks if we were on a sample offset.
        if( is_sample_offset ) {
          // Move on to the next sample suffix.
          ++read_sample;
        }
       
        // Move on to the next name
        ++read_name;
      }

      read_sample.finish();
      read_name.finish();
    }
  };

  /*
   * Given bins of merge records, one for each period offset,
   * we sort that bin individually. Then, we merge them all together.
   *
   * Note that the sample suffixes and the nonsample suffixes require
   * the difference cover (ie. must be merge_records) in order to be
   * sorted together.
   * 
  */
  template<typename OutputBackInserter /* a distributing_back_inserter */>
  struct sort_and_merge_node : public pipeline_node
  {
    const Dcx* dcx;
    // IN
    unique_by_rank_t* unique_bins;
    merge_by_rank_t* sample_bins;
    merge_by_rank_t** nonsample_bins;
    // OUT
    OutputBackInserter* output;

    sort_and_merge_node(
                       Dcx* dcx,
                       // IN
                       unique_by_rank_t* unique_bins,
                       merge_by_rank_t* sample_bins,
                       merge_by_rank_t** nonsample_bins,
                       // OUT
                       OutputBackInserter* output )
      : pipeline_node(dcx, "merge d="+dcx->depth_string, dcx->should_print_timing()),
        dcx(dcx),
        unique_bins(unique_bins),
        sample_bins(sample_bins),
        nonsample_bins(nonsample_bins),
        output(output)
    {
    }
    void run()
    {
      for( bin_idx_t b = 0; b < unique_bins->n_bins; b++ ) {
        if( unique_bins->is_local(b) ) {
          unique_bins->begin_reading_bin(b);
          assert( sample_bins->is_local(b) );
          sample_bins->begin_reading_bin(b);
          for( int j = 0; j < cover_t::nonsample_size; j++ ) {
            assert( nonsample_bins[j]->is_local(b) );
            nonsample_bins[j]->begin_reading_bin(b);
          }

          sort_and_merge(b);
        } else {
          assert( ! sample_bins->is_local(b) );
          for( int j = 0; j < cover_t::nonsample_size; j++ ) {
            assert( ! nonsample_bins[j]->is_local(b) );
          }
        }
      }
      output->finish();
    }
    void sort_and_merge( bin_idx_t bin )
    {
      typename unique_by_rank_t::bin_t* unique_in = &unique_bins->bins[bin];
      typename merge_by_rank_t::bin_t* sample_in = &sample_bins->bins[bin];
      typename merge_by_rank_t::bin_t* nonsample_in[cover_t::nonsample_size];

      for( int p = 0; p < cover_t::nonsample_size; p++ ) {
        nonsample_in[p] = &nonsample_bins[p]->bins[bin];
      }

      if( DEBUG_DCX ) {
        printf("node %i sort_and_merge bin=%i\n", (int) dcx->iproc, (int) bin);
      }

      // Set up a merger reading merge_record_t sorted files.
      size_t merge_record_size = RecordTraits<merge_record_t>::record_size;
      buffered_pipe big_merge_output(
          round_up_to_multiple(DEFAULT_TILE_SIZE, merge_record_size),
          DEFAULT_NUM_TILES);
      std::vector<read_pipe*> big_merge_input;

      for( int p = 0; p < cover_t::nonsample_size; p++ ) {
        big_merge_input.push_back( nonsample_in[p]->from_bin );
      }
      big_merge_input.push_back( sample_in->from_bin );

      // Start the merger.
      typedef ByNameAndDifferenceCover crit_t;
      merge_records_node<merge_record_t,crit_t >
        merger(crit_t(dcx), &big_merge_input, &big_merge_output);

      merger.start();

      // Now we have:
      //   - unique records
      //   - merged records
      // We can just merge using the original name,
      //   because a unique record's name can only appear in 
      //   unique records, and so there will never be a need
      //   to break a tie.

      // Process output from the merger.
      {
        // Now we need to consume the data from the merged nonsample records
        // and combine them with the unique records and the sample
        // records.. This is a 2-way merge using different data types.
        // Process output from the merger..
        pipe_iterator<merge_record_t> read_merged(&big_merge_output);
        pipe_iterator<merge_record_t> end_merged;
        pipe_iterator<offset_name_record_t> read_unique(unique_in->from_bin);
        pipe_iterator<offset_name_record_t> end_unique;

        offset_rank_record_t out;
        rank_t rank;

        rank_t start_name = 0; // always count ranks from 0.
        assert(unique_in->total_computed);
        start_name += unique_in->num_before;
        assert(sample_in->total_computed);
        start_name += sample_in->num_before;
        for( int p = 0; p < cover_t::nonsample_size; p++ ) {
          assert(nonsample_in[p]->total_computed);
          start_name += nonsample_in[p]->num_before;
        }

        rank = start_name;

        while( read_unique != end_unique && read_merged != end_merged ) {
          // Output the lesser of the two.
          if( (*read_unique).name < (*read_merged).name ) {
            // Output unique.
            out.offset = (*read_unique).offset;
            if( (*read_unique).name != 0 ) {
              out.rank = rank++;
            } else {
              out.rank = 0;
            }
            if( EXTRA_CHECKS ) {
              assert(out.offset < dcx->n);
            }

            if( DEBUG_DCX > 10 ) {
              printf("node %i in_unique: %s\n", (int) dcx->iproc, (*read_unique).to_string().c_str());
              printf("node %i out: %s\n", (int) dcx->iproc, out.to_string().c_str());
            }
            output->push_back(out);
            // Move on to the next unique.
            ++read_unique;
          } else {
            // Output merged.
            out.offset = (*read_merged).offset;
            out.rank = rank;
            if( EXTRA_CHECKS ) {
              assert(out.offset < dcx->n);
            }


            if( DEBUG_DCX > 10 ) {
              printf("node %i in_merged: %s\n", (int) dcx->iproc, (*read_merged).to_string().c_str());
              printf("node %i out: %s\n", (int) dcx->iproc, out.to_string().c_str());
            }
            output->push_back(out);
            rank++;
            // Move on to the next merged.
            ++read_merged;
          }
        }

        // finish up if either one had more.
        while( read_unique != end_unique ) {
          // Output unique.
          out.offset = (*read_unique).offset;
          if( (*read_unique).name != 0 ) {
            out.rank = rank++;
          } else {
            out.rank = 0;
          }
          if( EXTRA_CHECKS ) {
            assert(out.offset < dcx->n);
          }

          if( DEBUG_DCX > 10 ) {
            printf("node %i in_unique: %s\n", (int) dcx->iproc, (*read_unique).to_string().c_str());
            printf("node %i out: %s\n", (int) dcx->iproc, out.to_string().c_str());
          }
          output->push_back(out);
          // Move on to the next unique.
          ++read_unique;
        }

        while( read_merged != end_merged ) {
          // Output merged.
          out.offset = (*read_merged).offset;
          out.rank = rank;
          if( EXTRA_CHECKS ) {
            assert(out.offset < dcx->n);
          }
          if( DEBUG_DCX > 10 ) {
            printf("node %i in_merged: %s\n", (int) dcx->iproc, (*read_merged).to_string().c_str());
            printf("node %i out: %s\n", (int) dcx->iproc, out.to_string().c_str());
          }
          output->push_back(out);
          rank++;
          // Move on to the next merged.
          ++read_merged;
        }

        read_merged.finish();
        read_unique.finish();
      }

      merger.finish();
    }
  };


  // We assume that the data is distributed to bins with
  // whichbin_splitters_out->getbin()
  template<typename Splitters, typename InputBins>
  void read_and_share_splitter_material(
                  typename InputBins::criterion_t crit,
                  typename InputBins::key_t min,
                  typename InputBins::key_t max,
                  bin_idx_t n_bins,
                  // n_estimate and whichfile_splitters_out are vectors
                  // so that we can create different numbers of files 
                  // for different n_estimates for sample/nonsample/unique
                  // merge bins.
                  std::vector<offset_t>* n_estimate,
                  std::vector<size_t>* record_size,
                  size_t use_memory_per_subbin,
                  InputBins* input_bins,
                  Splitters** whichbin_splitters_out,
                  std::vector<std::vector<Splitters*>*>* whichfile_splitters_out
                  )
  {
    typedef Splitters splitters_t;
    typedef typename InputBins::record_t record_t;
    typedef typename InputBins::criterion_t criterion_t;
    typedef typename InputBins::key_t key_t;
    typedef Sorter<record_t,criterion_t,splitters_t> sorter_t;
    typedef typename sorter_t::sort_status sort_status_t;
    typedef typename RecordTraits<record_t>::iterator_t iterator_t;

    // PLAN:
    //
    // Forming splitters:
    //    previously, form records into local bins (done before this call)
    //    -- then sort each local bin
    //    -- then send that data to node 0. (mpi copy file, or usual thing)
    //    -- node 0 gets sorted data for each respective
    //       bin in its own different bins.
    //    -- node 0 will merge the sorted data, and then form the splitters.

    // Input bins are all distributed. Sort them all!

    for( bin_idx_t b = 0; b < input_bins->n_bins; b++ ) {
      SorterSettings settings;
      settings.Parallel = true;
      settings.Permute = false;

      if( input_bins->is_local(b) ) {
        //std::cout << "node " << iproc << " sorting file " << input_bins->bins[b].fname << std::endl;
        GeneralSorter<record_t, criterion_t>::general_sort_file(
            crit, min, max,
            input_bins->bins[b].fname,
            0,
            input_bins->bins[b].num_records_with_overlap,
            &settings);
      }
    }

    std::string sorted_name = input_bins->type;
    sorted_name = sorted_name + "_sorted";
    InputBins sorted_on_zero(this, sorted_name.c_str(), true /* all on 0 */);

    // Don't give us trouble about not really using these bins..
    sorted_on_zero.begin_writing();
    sorted_on_zero.finish();
    dcx_g_handler->work();
    sorted_on_zero.end_writing();

    barrier();

    // Copy all of the bins to node 0.
    for( bin_idx_t b = 0; b < input_bins->n_bins; b++ ) {
      mpi_move_file( comm,
                     input_bins->bins[b].fname, input_bins->owner(b),
                     sorted_on_zero.bins[b].fname, 0 );
      sorted_on_zero.bins[b].num_records = input_bins->bins[b].num_records_with_overlap;
      sorted_on_zero.bins[b].num_records_with_overlap = input_bins->bins[b].num_records_with_overlap;
    }

    barrier();

    std::vector< record_t > whichbin_splitter_records;
    std::vector<std::vector< std::vector<record_t> > > whichfile_splitter_records;
    whichfile_splitter_records.resize(n_estimate->size());
    for( size_t k = 0; k < n_estimate->size(); k++ ) {
      whichfile_splitter_records[k].resize(n_bins);
    }

    if( iproc == 0 ) { 
      // Now, all of the data is local to node 0. If we're node 0,
      // merge the sorted bins into a single file.
      file_pipe_context out_ctx = get_file_pipe_context<record_t>(filename_for_bin(tmp_dir, depth, input_bins->type + "merged", 0));
      {
        file_write_pipe_t write_pipe(out_ctx);

        std::vector<read_pipe*> merge_input;
        for( bin_idx_t b = 0; b < input_bins->n_bins; b++ ) {
          merge_input.push_back(new file_read_pipe_t(get_file_pipe_context<record_t>( sorted_on_zero.bins[b].fname)));
        }

        merge_records_node<record_t, criterion_t> merger(crit,
                                                    &merge_input,
                                                    &write_pipe);


        merger.start();
        merger.finish();

        write_pipe.assert_closed_full();

        for( bin_idx_t b = 0; b < input_bins->n_bins; b++ ) {
          delete merge_input[b];
          merge_input[b] = NULL;
        }
      }

      // OK, now in the out_ctx file, we have the sorted splitter records.
      // MMap that sucker and start using it!
      int fd = open(out_ctx.fname.c_str(), O_RDONLY);
      size_t len = file_len(fd);
      size_t splitter_record_size = RecordTraits<record_t>::record_size;
      size_t num_records = len / splitter_record_size;
      FileMMap memory;
      Pages pgs = get_pages_for_records(get_file_page_size(fd),0,len,splitter_record_size);
      if( pgs.outer_length > 0 ) {
        memory.map(NULL, pgs.outer_length, PROT_READ, MAP_SHARED, fd, pgs.outer_start);
      }

      iterator_t records = RecordTraits<record_t>::getiter(memory.data);

      // OK -- now get the bin splitters..
      // Setup as "whichbin" splitters

      splitters_t whichbin_splitters(crit, n_bins);
      whichbin_splitters.use_sample(min, max, records, num_records);
      whichbin_splitter_records = whichbin_splitters.splitters;

      if( DEBUG_DCX && iproc == 0 ) {
        printf("of %lli sampled records using bin splitters: \n", (long long int) num_records);
        for( size_t i = 0; i < whichbin_splitter_records.size(); i++ ) {
          printf("%s\n", whichbin_splitter_records[i].to_string().c_str());
        }
      }


      // Now create file splitters. Do this by making one pass
      // through the mmap'd file of splitters.
      // Then, for each bin, use the splitter material as sample data.
      // We just make a single pass through looking for the current
      // splitter since everything is sorted. Also keep in mind that
      // the bin for a splitter is always i+1.
      //
      // We do this once for this outer loop... so that we can
      // make file splitters of different sizes.
      for( size_t k = 0; k < n_estimate->size(); k++ ) {
        offset_t n_bins_offset = n_bins;
        offset_t n_estimate_per_bin = ceildiv_safe((*n_estimate)[k], n_bins_offset);
        size_t n_files = sorter_t::get_n_bins(n_estimate_per_bin, use_memory_per_subbin,
                                              (*record_size)[k]);

        assert( k < whichfile_splitter_records.size());

        size_t cur_splitter_start = 0;
        size_t cur_splitter_end = 0;
        typedef typename KeyCritTraits<record_t,criterion_t>::ComparisonCriterion comp_t;
        comp_t comp(crit);

        for( size_t b = 0; b < 1+whichbin_splitter_records.size(); b++ ) {

          // Find cur_splitter_end, the first record in the next bin.
          // Remember the bin for a splitter is always i+1
          if( b < whichbin_splitter_records.size() ) {
            for( cur_splitter_end = cur_splitter_start;
                 cur_splitter_end < num_records &&
                 comp.compare(records[cur_splitter_end],
                              whichbin_splitter_records[b]) < 0;
                 cur_splitter_end++ ) {
            }
          } else {
            cur_splitter_end = num_records;
          }

          splitters_t splitters( crit, n_files );

          splitters.use_sample(min, max,
                               records + cur_splitter_start,
                               cur_splitter_end - cur_splitter_start);

          assert( (size_t) b < whichfile_splitter_records[k].size());
          whichfile_splitter_records[k][b] = splitters.splitters;

          cur_splitter_start = cur_splitter_end;

          if( DEBUG_DCX && iproc == 0 ) {
            printf("bin %i using file splitters: \n", (int) b);
            for( size_t i = 0; i < whichfile_splitter_records[k][b].size(); i++ ) {
              printf("%s\n", whichfile_splitter_records[k][b][i].to_string().c_str());
            }
          }
        }
      }

      // Free resources.
      memory.unmap();

      int rc;

      rc = close(fd);
      if( rc ) throw error(ERR_IO_STR_OBJ("close failed", out_ctx.fname.c_str()));

      rc = unlink(out_ctx.fname.c_str());
      if( rc ) throw error(ERR_IO_STR_OBJ("unlink failed", out_ctx.fname.c_str()));
    }

    sorted_on_zero.clear();

    barrier(); // just to be sure the broadcasts do not get confused.

    // send whichbins splitters everywhere.
    mpi_share_vector(comm, &whichbin_splitter_records, 0);
    // send whichfile splitters everywhere.
    for( size_t k = 0; k < n_estimate->size(); k++ ) {
      for( bin_idx_t b = 0; b < n_bins; b++ ) {
        mpi_share_vector(comm, &whichfile_splitter_records[k][b], 0);
      }
    }

    // Save it all in the right output structures.
    *whichbin_splitters_out = new splitters_t(crit, min, max, whichbin_splitter_records);

    assert( whichfile_splitters_out->size() == n_estimate->size() );
    for( size_t k = 0; k < n_estimate->size(); k++ ) {
      std::vector< Splitters * > * whichfile = (*whichfile_splitters_out)[k];
      assert( whichfile );
      whichfile->resize(n_bins);
      for( bin_idx_t b = 0; b < n_bins; b++ ) {
        (*whichfile)[b] = new splitters_t(crit, min, max,
                                          whichfile_splitter_records[k][b]);
      }
    }
  }

  long int rand_offset() const
  {
    if( n == 0 ) return 0;

    long int offset = random();
    if( offset < 0 ) offset = - offset;
    offset = offset % n;

    return offset;
  }

  offset_t rand_sample_offset() const
  {
    if( n == 0 ) return 0;

    long int offset = rand_offset();

    // Now round down to make it 0 mod period.
    long int amt = offset % cover_t::period;
    offset -= amt;
    assert( offset >= 0 && offset < (long int) n && 
            offset % cover_t::period == 0 );

    assert( sizeof( long int ) >= sizeof( offset_t ));

    return offset;
  }

  rank_t rand_rank() const
  {
    if( sample_n == 0 ) return 0;

    long int rank = random();
    if( rank < 0 ) rank = - rank;
    rank = rank % sample_n;
    rank++; // from 1 to sample_n.

    assert( sizeof( long int ) >= sizeof( offset_t ));

    return rank;
  }

  /*
   * Each suffix_sort call starts out with (offset,non-unique-rank) records in offset bins,
   * and ends with (offset, unique-rank) records in offset bins.
   *
   * the arguments are vectors of size n_bins.
   * In the input_bins, the used values are:
   *   num_before -- offset start of this bin
   *   num_records -- num records in this bin
   *   fctx -- ready to read at that start position.
   * In the output_bins, the used values are:
   *   num_before -- offset start of this bin
   *   num_records -- num records in this bin
   *   fctx -- ready to write at that start position.
   */
  template<typename InputBins,
           typename InputToCharacterTranslator,
           typename OutputBins>
  void suffix_sort_impl(InputBins* input_bins,
                        bool delete_input,
                        InputToCharacterTranslator input_trans,
                        OutputBins* output_bins,
                        offset_t output_bin_size,
                        offset_t output_bin_overlap)
  {
    if( DEBUG_DCX ) {
      printf("node %i begin suffix_sort_impl depth=%i\n", (int) iproc, (int) depth);
    }

    barrier(); // we should all be here!

    {
      if( SHOULD_PRINT_PROGRESS ) {
        printf("permute and form splitters depth=%i n=%lli\n", (int) depth, (long long int) n);
      }

      // Always do start_clock since we use iostats from current timing
      start_clock();

      // barrier in here.
      // We put the data in local bins, sort it, and then send it to
      // node 0 where it will be merged.
      tuples_sample_t tuple_splitter_bins( this, "tuples_by_rank_split" );

      // form_tuples_node will be using InputToCharacterTranslator
      // to convert the input into characters (since the input might
      // be {name, offset} in recursion or {character} at 1st level.)
      form_tuples_node<InputBins,InputToCharacterTranslator> form_tuples_splitters(this, input_bins, num_splitter_records_per_bin, NULL, &tuple_splitter_bins);

      input_bins->begin_reading(false, num_splitter_records_per_bin+2*cover_t::period);
      tuple_splitter_bins.begin_writing();
      form_tuples_splitters.start();
      dcx_g_handler->work();
      form_tuples_splitters.finish();
      tuple_splitter_bins.end_writing();
      input_bins->end_reading();

      // Send the sample data to all the nodes.
      {
        std::vector<offset_t> n_estimate;
        std::vector<size_t> record_size;
        std::vector<std::vector<tuple_splitters_t*>*> whichfile;

        n_estimate.push_back(n);
        record_size.push_back(RecordTraits<offset_tuple_record_t>::record_size);
        whichfile.push_back(&tuple_whichfile_splitters);

        read_and_share_splitter_material<tuple_splitters_t,
                                         tuples_sample_t> (
          typename tuple_splitters_t::splitter_criterion_t(this),
          min_offset_tuple_record(),
          max_offset_tuple_record(),
          n_bins,
          &n_estimate,
          &record_size,
          USE_MEMORY_PER_BIN,
          &tuple_splitter_bins,
          &tuple_whichbin_splitters,
          &whichfile);
      }
      tuple_splitter_bins.clear();

      this->add_current_io_stats();
      stop_clock();
      if( SHOULD_PRINT_TIMING ) {
        print_timings("permute and form splitters", n);
      }
    }

    if( SHOULD_PRINT_PROGRESS ) {
      printf("form tuples depth=%i n=%lli\n", (int) depth, (long long int) n);
    }

    start_clock();

    // Form the tuples (using the splitters to decide where they go).
    tuples_by_rank_t tuples_by_rank( this, "tuples_by_rank" );

    form_tuples_node<InputBins,InputToCharacterTranslator>
      form_tuples(this, input_bins, 0, &tuples_by_rank, NULL);

    input_bins->begin_reading(delete_input);
    begin_writing_split(&tuples_by_rank, tuple_whichbin_splitters, &tuple_whichfile_splitters);
    form_tuples.start();
    dcx_g_handler->work();
    form_tuples.finish();
    tuples_by_rank.end_writing();
    input_bins->end_reading();

    {
      // Check that the sum of the number of records in tuples_by_rank
      // is n.
      offset_t total = 0;
      for( bin_idx_t b=0; b<n_bins; b++ ) {
        total += tuples_by_rank.bins[b].num_records;
      }
      assert(total == n);
    }

    this->add_current_io_stats();
    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("form tuples", n);
    }

    if( SHOULD_PRINT_PROGRESS ) {
      printf("sort and name depth=%i n=%lli\n", (int) depth, (long long int) n);
    }

    start_clock();

    // Setup the subproblem for the recursion
    // Set the offset bin size for the recursion.
    // This is just based on the number of sample suffixes
    SubDcx sub(tmp_dir, sample_n, n_bins, comm, depth+1, this);

    names_by_offset_t all_names_by_offset( this, "all_names_by_offset" );
    typename SubDcx::names_by_offset_t recursion_input( &sub, "input" );

    // Sort the distributed rank bins.
    // Now, we expect them not to be unique,
    // so we assume that we will recurse. Therefore,
    // we distribute (rank, offset) to the offset bins.
    sort_and_name_node sort_and_name( this,
                                      &tuples_by_rank,
                                      &recursion_input,
                                      &all_names_by_offset);

    tuples_by_rank.begin_reading();
    begin_writing_even(&recursion_input, sub.offset_bin_size, sub.bin_overlap);
    begin_writing_even(&all_names_by_offset, offset_bin_size, bin_overlap);
    sort_and_name.start();
    dcx_g_handler->work();
    sort_and_name.finish();
    all_names_by_offset.end_writing();
    recursion_input.end_writing();
    tuples_by_rank.end_reading();

    name_t max_name = sort_and_name.max_name;
    bool unique = sort_and_name.unique;
    offset_t num_unique = sort_and_name.num_unique;

    this->add_current_io_stats();
    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("sort and name", n);
    }

    {
      // Check that the sum of the number of records in recursion input
      // is sample_n.
      offset_t total = 0;
      for( bin_idx_t b=0; b<n_bins; b++ ) {
        total += recursion_input.bins[b].num_records;
      }
      assert(total == sample_n);
    }

    if( SHOULD_PRINT_PROGRESS ) {
      printf("recursion/output depth=%i n=%lli\n", (int) depth, (long long int) n);
    }

    start_clock();

#ifdef HAVE_MPI_H
    if(nproc > 1) {
      int unique_in = unique;
      int unique_out = 0;
      int rc;
      // Reduce unique.
      rc = MPI_Allreduce( &unique_in, &unique_out,
          1, MPI_INT, MPI_LAND, comm );
      if( rc ) throw error(ERR_IO_STR("MPI_Allreduce failed"));

      unique = unique_out;

      long long int num_unique_in = num_unique;
      long long int num_unique_out = 0;
      // Reduce total ranks.
      rc = MPI_Allreduce( &num_unique_in, &num_unique_out,
          1, MPI_LONG_LONG_INT, MPI_SUM, comm );
      if( rc ) throw error(ERR_IO_STR("MPI_Allreduce failed"));
      num_unique = num_unique_out;
 
      long long int max_ranks_in = max_name;
      long long int max_ranks_out = 0;
      // no overflow!
      assert(max_ranks_in >= 0);
      assert((rank_t)max_ranks_in == max_name);

      // Reduce total ranks.
      rc = MPI_Allreduce( &max_ranks_in, &max_ranks_out,
          1, MPI_LONG_LONG_INT, MPI_MAX, comm );
      if( rc ) throw error(ERR_IO_STR("MPI_Allreduce failed"));
      max_name = max_ranks_out;
    }
#endif

    if( SHOULD_PRINT_PROGRESS ) {
      printf("depth=%i num_unique=%lli (%lf%%)\n", (int) depth, (long long int) num_unique,
             (100.0 * num_unique)/n);
    }

    // Setup the characters per bin for the subbin.
    sub.set_maxchar(max_name);

    // Now we are done with tuples_by_rank.
    tuples_by_rank.clear();

    // maybe_recurse

    // Assuming we do recurse..
    // This Bins also calls UpToPackedOffset on the result before storing it.
    // (this is because it has a Translator to call up_offset)
    sample_ranks_by_offset_t sample_ranks_by_offset( this, "sample_ranks_by_offset" );

    if( ! unique ) {
      // need distributor that calls up_offset
      // before finding the target bin. That can't be done in 
      // form_merge_records because the data is already distributed
      // to the offset bins!

      sub.suffix_sort_impl(&recursion_input, true,
          typename SubDcx::OffsetMaybeNameToCharacter(),
          &sample_ranks_by_offset,
          packed_offset_bin_size,
          bin_overlap);
      // calls finish on sample_ranks_up_distributor.

      // don't need recursion input after recursion completes!
      recursion_input.clear();
    } else {
      // go from sample_names_by_offset and nonsample_names_by_offset
      // to output distributor.

      // we don't need sample_ranks_by_offset after all.
      begin_writing_even(&sample_ranks_by_offset, packed_offset_bin_size, bin_overlap);
      sample_ranks_by_offset.finish();
      dcx_g_handler->work();
      sample_ranks_by_offset.end_writing();
      sample_ranks_by_offset.clear();

      output_unique_node<OutputBins> output_unique(
            this,
            &all_names_by_offset,
            output_bins);

      all_names_by_offset.begin_reading();
      begin_writing_even(output_bins, output_bin_size, output_bin_overlap);
      output_unique.start();
      dcx_g_handler->work();
      output_unique.finish();
      output_bins->end_writing();
      all_names_by_offset.end_reading();

      // we are done with sample_names_by_offset and nonsample too
      recursion_input.clear();
      all_names_by_offset.clear();
      if( DEBUG_DCX ) {
        printf("node %i end suffix_sort_impl %i\n", (int) iproc, (int) depth);
      }

      this->add_current_io_stats();
      stop_clock();
      if( SHOULD_PRINT_TIMING ) {
        print_timings("final output", n);
      }
      return;
    }

    this->add_current_io_stats();
    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("recursion", sample_n);
    }

    // At this point, we should have (offset,rank) distributed into offset bins.

    if( SHOULD_PRINT_PROGRESS ) {
      printf("form merge splitters depth=%i n=%lli\n", (int) depth, (long long int) n);
    }

    start_clock();

    long use_mem_per_merge_bin = (USE_MEMORY_PER_BIN * SORT_PROCS_PER_BIN) / (SORT_PROCS_PER_BIN*2L + cover_t::nonsample_size);
    {
      // Splitter material is stored locally, sorted, then sent
      // to node 0 and merged.
      merges_sample_t merge_splitter_bins( this, "merge_by_rank_split" );


      // Choose splitters for merge records.
      form_merge_records_node
        form_merge_record_splitters(
            this,
            &sample_ranks_by_offset,
            &all_names_by_offset,
            num_splitter_records_per_bin,
            NULL,
            NULL,
            NULL,
            &merge_splitter_bins);

      sample_ranks_by_offset.begin_reading(false, ceildiv(cover_t::sample_size * num_splitter_records_per_bin, cover_t::period) + cover_t::period);
      all_names_by_offset.begin_reading(false, num_splitter_records_per_bin+cover_t::period);
      merge_splitter_bins.begin_writing();
      form_merge_record_splitters.start();
      dcx_g_handler->work();
      form_merge_record_splitters.finish();
      merge_splitter_bins.end_writing();
      all_names_by_offset.end_reading();
      sample_ranks_by_offset.end_reading();

      {
        std::vector<offset_t> n_estimate;
        std::vector<size_t> record_size;
        std::vector< std::vector<merge_splitters_t*> * > whichfile;

        // unique
        n_estimate.push_back(num_unique);
        record_size.push_back(RecordTraits<offset_name_record_t>::record_size);
        whichfile.push_back(&merge_unique_whichfile_splitters);
        // sample
        n_estimate.push_back(sample_n);
        record_size.push_back(RecordTraits<merge_record_t>::record_size);
        whichfile.push_back(&merge_sample_whichfile_splitters);
        // nonsample
        n_estimate.push_back(ceildiv(n, cover_t::period));
        record_size.push_back(RecordTraits<merge_record_t>::record_size);
        whichfile.push_back(&merge_nonsample_whichfile_splitters);

        read_and_share_splitter_material<merge_splitters_t,merges_sample_t> (
          typename merge_splitters_t::splitter_criterion_t(this),
          min_merge_record(),
          max_merge_record(),
          n_bins,
          &n_estimate,
          &record_size,
          // Assume we have only memory for 2*SORT_PROCS_PER_BIN input bins on each,
          //USE_MEMORY_PER_BIN * 2L / (2 + cover_t::nonsample_size),
          use_mem_per_merge_bin,
          &merge_splitter_bins,
          &merge_whichbin_splitters,
          &whichfile);
      }
      merge_splitter_bins.clear();
      
      this->add_current_io_stats();
      stop_clock();
      if( SHOULD_PRINT_TIMING ) {
        print_timings("form merge splitters", n);
      }
    }


    if( SHOULD_PRINT_PROGRESS ) {
      printf("form merge records depth=%i n=%lli\n", (int) depth, (long long int) n);
    }
    start_clock();

    // setup actual output bins.
    unique_by_rank_t unique_by_rank( this, "unique_by_rank" );

    merge_by_rank_t sample_merge_by_rank( this, "sample_merge_by_rank" );
    merge_by_rank_t* nonsample_merge_by_rank[cover_t::nonsample_size];

    for( int d = 0; d < cover_t::nonsample_size; d++ ) {
      static const int max_buf = 2040;
      char buf[max_buf];
      snprintf(buf, max_buf, "nonsample_%i_merge_by_rank", d);
      nonsample_merge_by_rank[d] = new merge_by_rank_t( this, buf );
      // Use fewer threads on the nonsample merge records... this should
      // really just give us 1 thread for each nonsample area.
      nonsample_merge_by_rank[d]->set_thread_multiplicity(cover_t::nonsample_size);
    }

    // Form the merge records.
    // distribute the merge records by rank for all sample and nonsample suffixes.
    // Note that for this step, we will need rank bins
    // for each nonsample suffix offset.
    form_merge_records_node
      form_merge_records(
                             this,
                             &sample_ranks_by_offset,
                             &all_names_by_offset,
                             0,
                             &unique_by_rank,
                             &sample_merge_by_rank,
                             nonsample_merge_by_rank,
                             NULL);
 
    sample_ranks_by_offset.begin_reading();
    all_names_by_offset.begin_reading();
    begin_writing_split(&unique_by_rank, merge_whichbin_splitters, &merge_unique_whichfile_splitters, use_mem_per_merge_bin);
    begin_writing_split(&sample_merge_by_rank, merge_whichbin_splitters, &merge_sample_whichfile_splitters, use_mem_per_merge_bin);
    for( int d = 0; d < cover_t::nonsample_size; d++ ) {
      begin_writing_split(nonsample_merge_by_rank[d], merge_whichbin_splitters, &merge_nonsample_whichfile_splitters, use_mem_per_merge_bin);
    }
    form_merge_records.start();
    dcx_g_handler->work();
    form_merge_records.finish();
    for( int d = 0; d < cover_t::nonsample_size; d++ ) {
      nonsample_merge_by_rank[d]->end_writing();
    }
    sample_merge_by_rank.end_writing();
    unique_by_rank.end_writing();
    all_names_by_offset.end_reading();
    sample_ranks_by_offset.end_reading();

    this->add_current_io_stats();
    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("form merge records", n);
    }

    if( SHOULD_PRINT_PROGRESS ) {
      printf("merge depth=%i n=%lli\n", (int) depth, (long long int) n);
    }

    start_clock();

    // Now we are done with all_names_by_offset
    all_names_by_offset.clear();
    // Also done with sample_ranks_by_offset
    sample_ranks_by_offset.clear();

    // Now we are done with sample_by_ranks

    // sort each nonsample suffix set individually
    // (full radix sort). then merge them all together
    // along with the sample suffixes (fancy comparison routine).
    //
    // Finally, place (offset,rank) records
    // in the parent's offset bins (for previous recursion levels).

    sort_and_merge_node<OutputBins>
      sort_and_merge(
                        this,
                        &unique_by_rank,
                        &sample_merge_by_rank,
                        nonsample_merge_by_rank,
                        output_bins );

    unique_by_rank.begin_reading();
    sample_merge_by_rank.begin_reading();
    for( int d = 0; d < cover_t::nonsample_size; d++ ) {
      nonsample_merge_by_rank[d]->begin_reading();
    }
    begin_writing_even(output_bins, output_bin_size, output_bin_overlap);
    sort_and_merge.start();
    dcx_g_handler->work();
    sort_and_merge.finish();
    output_bins->end_writing();
    for( int d = 0; d < cover_t::nonsample_size; d++ ) {
      nonsample_merge_by_rank[d]->end_reading();
    }
    sample_merge_by_rank.end_reading();
    unique_by_rank.end_reading();

    // Finish up with the bins we used here.
    unique_by_rank.clear();
    sample_merge_by_rank.clear();
    for( int d = 0; d < cover_t::nonsample_size; d++ ) {
      nonsample_merge_by_rank[d]->clear();
      delete nonsample_merge_by_rank[d];
    }

    this->add_current_io_stats();
    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("merge", n);
    }

    if( DEBUG_DCX ) {
      printf("node %i end suffix_sort_impl %i\n", (int) iproc, (int) depth);
    }
  }


  template<typename InCharacter>
  struct read_input_node : public pipeline_node
  {
    typedef Bins<Dcx,
                 InCharacter,
                 ZeroCriterion,
                 FixedSplitters<InCharacter, ZeroCriterion>,
                 DONT_SORT> input_by_offset_t;

    const Dcx* dcx;

    read_pipe* input;
    input_by_offset_t* in_bins;
    read_input_node(const Dcx* dcx, read_pipe* input, input_by_offset_t* in_bins)
      : pipeline_node(dcx, "read_input d="+dcx->depth_string, dcx->should_print_timing()),
        dcx(dcx), input(input), in_bins(in_bins)
    {
    }

    virtual void run() {
      if( dcx->iproc == 0 ) {
        if( DEBUG_DCX ) {
          printf("node %i read_input\n", (int) dcx->iproc);
        }

        typedef ByOffset<offset_name_record_t> crit_t;
        typedef DividingSplitters<offset_name_record_t, crit_t> splitters_t;

        crit_t crit(dcx);
        splitters_t splitters(crit, in_bins->n_bins);
        splitters.use_sample_overlap(dcx->offset_bin_size, dcx->bin_overlap);

        // Read from the input pipe to distribute the input
        // by offset among the different bins.
        pipe_iterator<InCharacter> read_in(input);
        pipe_iterator<InCharacter> end;

        offset_t i;
        for( i = 0; read_in != end; ++read_in, i++ ) {
          InCharacter in_ch = *read_in;

          offset_name_record_t r;
          r.name = in_ch;
          r.offset = i;

          ssize_t bin, alt_bin;
          bin = alt_bin = 0;
          splitters.get_bins(i, bin, alt_bin);

          if( DEBUG_DCX > 10 ) {
            printf("node %i storing ch=%lli to bins %i,%i\n",
                   (int) dcx->iproc, (long long int) in_ch, (int) bin, (int) alt_bin);
          }
 
          in_bins->push_back_bins(in_ch, bin, alt_bin);
        }
        assert(i==dcx->n);
        read_in.finish();
      }

      in_bins->finish(); // closes the write pipes.
    }
  };

  /* Suffix sort data from an input pipe available to node 0,
   * storing the data in an output pipe also available to node 0.
   * While the input and output are only on node 0, each node
   * must call this routine (passing NULL for the pipes if they
   * are not node 0).
   */ 
  template<typename InCharacter,typename OutOffset>
  void suffix_sort(OutOffset len,
                   InCharacter max_char,
                   read_pipe* input,
                   std::vector<std::string>* output_fnames)
  {
    if( DEBUG_DCX ) {
      printf("node %i suffix_sort (outer) output '%s' ..\n",
             (int) iproc, (*output_fnames)[0].c_str());
    }

    start_clock();

    assert(output_fnames->size() == (size_t) n_bins);

    // make sure max_char + 1 can be stored.
    character_t max_char_plus_1 = max_char;
    max_char_plus_1++;
    assert(max_char_plus_1>0); // no overflow!

    set_maxchar(max_char_plus_1);

    assert(len == n);


    typedef read_input_node<InCharacter> read_input_node_t;

    start_clock();

    // Create the in_bins
    typename read_input_node_t::input_by_offset_t in_bins(this, "input");
    read_input_node_t read_input(this, input, &in_bins);

    in_bins.begin_writing();
    read_input.start();
    dcx_g_handler->work();
    read_input.finish();
    in_bins.end_writing();

    this->add_current_io_stats();
    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("read input", n);
    }

    // OK. Now output the data to the output bins.
    offset_rank_by_rank_t out_bins( this, "output");

    // Call suffix_sort
    suffix_sort_impl(&in_bins, true, AddOneTranslator<InCharacter>(), &out_bins,
                     rank_bin_size, 0);
    // There is a barrier in there!

    if( SHOULD_PRINT_PROGRESS ) {
      printf("output offsets n=%lli\n", (long long int) n);
    }

    start_clock();

    // Note that at this point, we have the (offset,rank) records
    // distributed by rank. To compute the suffix array we need
    // sort them by rank.
    
    // Sort the output bins by their unique ranks, and output that.
    // output_offset_nodes will use OffsetRankToOffset to output
    // only offsets (and not ranks).
    output_offsets_node< OutOffset >
      output_offsets(this, 
                     &out_bins, output_fnames );

    out_bins.begin_reading();
    output_offsets.start();
    dcx_g_handler->work();
    output_offsets.finish();
    out_bins.end_reading();

    barrier();

    // Now copy the output back to node 0.
    for(bin_idx_t b=0; b<n_bins; b++) {
      std::string in_fname = (*output_fnames)[b];
      int owner = out_bins.owner(b);

      mpi_move_file( comm, 
                     in_fname, owner,
                     (*output_fnames)[b], 0 );
    }

    barrier();

    this->add_current_io_stats();
    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("output offsets", n);
    }

    // Remove temporary files!
    in_bins.clear();
    out_bins.clear();


    this->add_current_io_stats();
    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("outer suffix sort", len);
    }

  }

  template<typename InCharacter,typename OutOffset>
  void suffix_sort(OutOffset len,
                   InCharacter max_char,
                   std::string input_filename,
                   std::string output_filename)
  {
    file_read_pipe_t* input_pipe = NULL;

    start_clock();

    if( iproc == 0 ) {
      file_pipe_context in_ctx = get_file_pipe_context<InCharacter>(input_filename);

      in_ctx.create = false;
      in_ctx.write = false;
      in_ctx.read = true;

      input_pipe = new file_read_pipe_t( in_ctx );
    }

    suffix_sort(len, max_char, input_pipe, output_filename);

    if( input_pipe ) {
      delete input_pipe;
    }

    this->add_current_io_stats();
    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("outer file suffix sort", len);
    }

  }

#ifdef BWT_SUPPORT


  struct form_bwt_node : public pipeline_node {
    typedef typename read_input_node<alpha_t>::input_by_offset_t input_by_offset_t;

    const Dcx* dcx;
    // INPUT
    input_by_offset_t* in_bins;
    offset_rank_by_offset_t* rank_bins;
    alpha_t doc_end_char;
    bwt_document_info_reader_t* info;
    // OUTPUT
    bwt_by_rank_t* out_bwt_bins;

    form_bwt_node(const Dcx* dcx, input_by_offset_t* in_bins, offset_rank_by_offset_t* rank_bins, alpha_t doc_end_char, bwt_document_info_reader_t* info, bwt_by_rank_t* out_bwt_bins)
      : pipeline_node(dcx, "form_bwt d="+dcx->depth_string, dcx->should_print_timing()),
        dcx(dcx), in_bins(in_bins), rank_bins(rank_bins), doc_end_char(doc_end_char), info(info), out_bwt_bins(out_bwt_bins)
    {
    }

    virtual void run() {
      for( bin_idx_t b = 0; b < in_bins->n_bins; b++ ) {
        if( in_bins->is_local(b) ) {
          in_bins->begin_reading_bin(b);
          assert( rank_bins->is_local(b) );
          rank_bins->begin_reading_bin(b);

          form_bwt(b);
        } else {
          assert( ! rank_bins->is_local(b) );
        }
      }
      out_bwt_bins->finish();
    }

    void form_bwt(bin_idx_t b)
    {
      typename offset_rank_by_offset_t::bin_t* offset_rank_by_offset = &rank_bins->bins[b];
      typename input_by_offset_t::bin_t* char_by_offset = &in_bins->bins[b];

      pipe_iterator<offset_rank_record_t> read(offset_rank_by_offset->from_bin);
      pipe_iterator<offset_rank_record_t> end;
      pipe_iterator<alpha_t> read_char( char_by_offset->from_bin );
      pipe_iterator<alpha_t> end_char;

      character_t prev_char;
      offset_t start;
      offset_t stop;
      int64_t cur_doc;

      // offset starts out as the number before..
      
      // figure out the previous character for the initial offset.
      // computes prev_char.
      // We start on the second record of each bin,
      // except for in the first bin.
      if( offset_rank_by_offset->num_before == 0 ) {
        start = 0;
        prev_char = doc_end_char; // last char in every document..
      } else {
        start = 1; // since this record will overlap 
                   // with previous bin, we output it then.
        prev_char = *read_char;
        ++read_char;
        ++read;
      }

      cur_doc = -1;

      stop = offset_rank_by_offset->num_records + 1; // so we get overlap.
      // handle last bin..
      if( stop > offset_rank_by_offset->num_records_with_overlap ) {
        stop = offset_rank_by_offset->num_records_with_overlap;
      }

      for( offset_t i = start; i < stop; i++ )
      {
        bwt_record_t r;
        offset_t offset;
        offset_t doc_offset;

        offset = (*read).offset;
        // which document number?
        {
          error_t err;
          if( cur_doc == -1 ) {
            err = bwt_document_info_reader_find_doc(info, offset, &cur_doc);
            if( err ) throw error(err);
          } else {
            int64_t doc_end = -1;
            err = bwt_document_info_reader_doc_end(info, cur_doc, &doc_end);
            if( err ) throw error(err);
            // Now check to see if we're past the current document.
            if( ((int64_t)offset) >= doc_end ) cur_doc++;
            if( EXTRA_CHECKS ) {
              int64_t check_doc = -1;
              err = bwt_document_info_reader_find_doc(info, offset, &check_doc);
              if( err ) throw error(err);
              assert( check_doc == cur_doc );
            }
          }
        }


        // Translate offset to doc_offset.
        {
          error_t err;
          int64_t doc_start = -1;
          err = bwt_document_info_reader_doc_start(info, cur_doc, &doc_start);
          if( err ) throw error(err);

          if( EXTRA_CHECKS ) assert( (int64_t)offset >= doc_start );

          doc_offset = offset - doc_start;
        }

        r.doc_offset = doc_offset; 
        r.doc = cur_doc;
        r.rank = (*read).rank;
        r.ch = prev_char;
        prev_char = *read_char;

        if( DEBUG_DCX > 10 ) {
          printf("node %i out %s\n", (int) dcx->iproc, r.to_string().c_str());
        }
  
        out_bwt_bins->push_back(r);

        ++read_char;
        ++read;
      }

      read.finish();
      read_char.finish();
    }

  };

  struct write_index_files_node : public pipeline_node {
    const Dcx* dcx;

    // INPUT
    bwt_by_rank_t* bwt_bins;
    alpha_t doc_end_char;
    const std::vector< std::vector< int64_t > >* bins_to_blocks;
    // OUTPUT index constructor (created but we call output funs on it).
    index_constructor_t* cstr;

    write_index_files_node(const Dcx* dcx, bwt_by_rank_t* bwt_bins, alpha_t doc_end_char, const std::vector< std::vector< int64_t > >* bins_to_blocks, index_constructor_t* cstr)
      : pipeline_node(dcx, "write_index_files d="+dcx->depth_string, dcx->should_print_timing()),
        dcx(dcx), bwt_bins(bwt_bins), doc_end_char(doc_end_char), bins_to_blocks(bins_to_blocks), cstr(cstr)
    {
    }

    virtual void run()
    {
      {
        for( bin_idx_t b = 0; b < bwt_bins->n_bins; b++ ) {
          if( bwt_bins->is_local(b) ) {
            bwt_bins->begin_reading_bin(b);

            write_index_files(b);
          }
        }
      }
    }

    struct bucket_compress_job : public pipeline_node {
      const index_constructor_t* cstr;
      int max_bucket_size;
      int max_chunk_size;
      int bucket_number;
      int bucket_size;
      alpha_t* L;
      int64_t* offsets;
      int64_t* docs;
      buffer_t zdata;
      int occs_in_bucket[ALPHA_SIZE];
      error_t err;

      bucket_compress_job(write_index_files_node* parent, int max_bucket_size, int max_chunk_size, int bucket_number, int bucket_size)
        : pipeline_node(parent, "bucket_compress_job", parent->dcx->should_print_timing()),
          cstr(parent->cstr),
          max_bucket_size(max_bucket_size),
          max_chunk_size(max_chunk_size),
          bucket_number(bucket_number),
          bucket_size(bucket_size),
          L(NULL), offsets(NULL), docs(NULL), zdata(build_buffer(0,NULL))
      {
        L = (alpha_t*) malloc(sizeof(alpha_t)*max_bucket_size);
        offsets = (int64_t*) malloc(sizeof(int64_t)*max_bucket_size);
        docs = (int64_t*) malloc(sizeof(int64_t)*max_bucket_size);
        if( ! L || ! offsets || ! docs ) {
          throw error(ERR_MEM);
        }
      }


      ~bucket_compress_job()
      {
        if( L ) {
          free(L);
          L = NULL;
        }
        if( offsets ) {
          free(offsets);
          offsets = NULL;
        }
        if( docs ) {
          free(docs);
          docs = NULL;
        }
        if( zdata.data ) {
          free(zdata.data);
          zdata.data = NULL;
        }
      }

      virtual void run()
      {
        assert( ! zdata.data );

        // construct the chunks for this bucket.
        std::vector<results_t> chunks;

        err = ERR_NOERR;

        if( max_chunk_size > 0 ) {
          int cur_chunk_row, cur_chunk_sz;
          cur_chunk_row = 0;
          while( cur_chunk_row < bucket_size ) {
            cur_chunk_sz = max_chunk_size;
            if( cur_chunk_row + cur_chunk_sz > bucket_size ) {
              cur_chunk_sz = bucket_size - cur_chunk_row;
            }

            results_t r = empty_results(RESULT_TYPE_DOCUMENTS);
            err = results_create_sort(&r, cur_chunk_sz, &docs[cur_chunk_row]);
            if( err ) break;

            chunks.push_back(r);

            cur_chunk_row += cur_chunk_sz;
          }
        }

        if( ! err ) {
          err = constructor_compress_bucket(cstr,
                                            // output
                                            &zdata, occs_in_bucket,
                                            NULL, /* stats */
                                            bucket_size,
                                            L,
                                            offsets,
                                            (max_chunk_size>0)?(&chunks[0]):(NULL) );
        }

        // free what's in chunks.
        for( size_t i = 0; i < chunks.size(); i++ ) {
          results_destroy(&chunks[i]);
        }
      }
    };

    void write_index_files(bin_idx_t b)
    {
      typename bwt_by_rank_t::bin_t* out_bwt = & bwt_bins->bins[b];

      pipe_iterator<bwt_record_t> read(out_bwt->from_bin);
      pipe_iterator<bwt_record_t> end;

      error_t err;

      int block_size, chunk_size, bucket_size, mark_period;

      std::deque<bucket_compress_job*> jobs;

      err = constructor_get_block_size(cstr, &block_size);
      if( err ) throw error(err);
      err = constructor_get_chunk_size(cstr, &chunk_size);
      if( err ) throw error(err);
      err = constructor_get_bucket_size(cstr, &bucket_size);
      if( err ) throw error(err);
      err = constructor_get_mark_period(cstr, &mark_period);
      if( err ) throw error(err);

      assert( out_bwt->total_computed );
      assert( dcx->block_bin_size_rows % block_size == 0 );

      int64_t bin_sz = out_bwt->num_records;
      int64_t bin_row = out_bwt->num_before;
      int64_t bin_end = bin_row + bin_sz;

      for( size_t j = 0; j < (*bins_to_blocks)[b].size(); j++ ) {
        int64_t block_idx = (*bins_to_blocks)[b][j];
        int64_t cur_block_row = block_idx * block_size;
        int64_t cur_block_sz = block_size;
        if( cur_block_row + cur_block_sz > bin_end ) {
          cur_block_sz = bin_end - cur_block_row;
        }

        printf("bin %lli writing data block %lli size %lli at row %lli\n",
               (long long int) b,
               (long long int) block_idx,
               (long long int) cur_block_sz,
               (long long int) bin_row);

        assert( bin_row == cur_block_row );

        err = constructor_begin_data_block(cstr, cur_block_sz, block_idx, cur_block_row);
        if( err ) throw error(err);

        int64_t cur_bucket_row, cur_bucket_sz;
        int64_t cur_bucket;

        cur_bucket = 0;
        cur_bucket_row = 0;
        while( cur_bucket_row < cur_block_sz ) {
          cur_bucket_sz = bucket_size;
          if( cur_bucket_row + cur_bucket_sz > cur_block_sz ) {
            cur_bucket_sz = cur_block_sz - cur_bucket_row;
          }

          if( DEBUG_DCX ) {
            printf("bin %lli reading data block %lli bucket %lli L data\n",
                   (long long int) b,
                   (long long int) block_idx,
                   (long long int) cur_bucket);
          }

          bucket_compress_job* job = new bucket_compress_job(this, bucket_size, chunk_size, cur_bucket, cur_bucket_sz);

          // Fill in bucket_L and bucket_offsets and bucket_docs.
          for( int64_t bucket_row = 0; bucket_row < cur_bucket_sz; bucket_row++ ) {
            if( EXTRA_CHECKS ) assert( read != end );
            int64_t doc_offset = (*read).doc_offset;
            alpha_t the_char = (*read).ch;
            int64_t doc = (*read).doc;
            int64_t offset, doc_start, doc_end;

            // Figure out the new doc # if necessary.
            if( dcx->doc_renumber ) {
              doc = (* dcx->doc_renumber)[doc];
            }

            // info applies to the new document #s.
            err = constructor_get_doc_range(cstr, doc, &doc_start, &doc_end);
            if( err ) throw error(err);

            offset = doc_start + doc_offset;

            if( EXTRA_CHECKS ) {
              int64_t check_doc = -1;
              int64_t check_start, check_end;
              err = constructor_get_doc(cstr, offset, &check_doc, &check_start, &check_end);
              if( err ) throw error(err);
              assert( check_doc == doc );
            }

            if( doc_start == offset ) {
              // Force this to be the last char in every document..
              the_char = doc_end_char;
            }

            if( EXTRA_CHECKS ) {
              assert(doc_start <= offset);
              assert(offset < doc_end);
            }
            
            int mark = should_mark(mark_period, doc_offset, doc_end-doc_start);
            int64_t use_offset;
            if( mark ) use_offset = offset;
            else use_offset = -1;

            job->L[bucket_row] = the_char;
            job->offsets[bucket_row] = use_offset;
            job->docs[bucket_row] = doc;

            ++read;
            bin_row++;
          }

          job->start();
          jobs.push_back(job);
          job = NULL;

          // are we full of jobs?
          if( jobs.size() >= NUM_INDEXER_THREADS ) {
            // Wait for the first one to finish.
            job = jobs.front();
            jobs.pop_front();
            
            job->finish();
  
            if( job->err ) throw error(err);

            if( DEBUG_DCX ) {
              printf("bin %lli writing data block %lli bucket %lli L data\n",
                   (long long int) b,
                   (long long int) block_idx,
                   (long long int) job->bucket_number);
            }

            err = constructor_write_bucket(cstr, &job->zdata, job->occs_in_bucket);
            if( err ) throw error(err);

            delete job;
            job = NULL;
          }

          cur_bucket_row += cur_bucket_sz;
          cur_bucket++;
        }

        while( jobs.size() > 0 ) {
          // Wait for the first one to finish.
          bucket_compress_job* job = jobs.front();
          jobs.pop_front();
          
          job->finish();

          if( job->err ) throw error(err);

          printf("bin %lli writing data block %lli bucket %lli L data\n",
               (long long int) b,
               (long long int) block_idx,
               (long long int) job->bucket_number);

          err = constructor_write_bucket(cstr, &job->zdata, job->occs_in_bucket);
          if( err ) throw error(err);

          delete job;
          job = NULL;
        }

        err = constructor_end_data_block(cstr);
        if( err ) throw error(err);

        printf("bin %lli finished data block %lli size %lli at row %lli\n",
               (long long int) b,
               (long long int) block_idx,
               (long long int) cur_block_sz,
               (long long int) bin_row);


      }

      read.finish();
    }
  };



  struct bwt_to_docs_node : public pipeline_node {
    const Dcx* dcx;
    // INPUT
    bwt_by_rank_t* bwt_bins;
    // OUTPUT
    doc_next_by_doc_next_t* doc_bins;

    bwt_to_docs_node(const Dcx* dcx, bwt_by_rank_t* bwt_bins, doc_next_by_doc_next_t* doc_bins)
      : pipeline_node(dcx, "bwt_to_docs_node d="+dcx->depth_string, dcx->should_print_timing()),
        dcx(dcx), bwt_bins(bwt_bins), doc_bins(doc_bins)
    {
    }

    virtual void run()
    {
      for( bin_idx_t b = 0; b < bwt_bins->n_bins; b++ ) {
        if( bwt_bins->is_local(b) ) {
          bwt_bins->begin_reading_bin(b);
          to_docs(b);
        }
      }
      doc_bins->finish();
    }

    void to_docs( bin_idx_t bin )
    {
      typename bwt_by_rank_t::bin_t* input = &bwt_bins->bins[bin];

      if( DEBUG_DCX ) {
        printf("node %i to_docs bin=%i fname is %s num_records is %lli\n",
               (int) dcx->iproc, (int) bin,
               input->fname.c_str(), (long long int) input->num_records);
      }

      // sorting bwt bins already done
      
      windowed_pipe_iterator<bwt_record_t> read(input->from_bin, RENUMBER_NUM_FOLLOWING + 1);
      windowed_pipe_iterator<bwt_record_t> end;

      // now create doc_next_doc records
      // Read the file.
    
      // do work for loop
      assert(input->total_computed);

      for( document_t i = 0;
           i < input->num_records;
           i++ ) {
        /*if( DEBUG_DCX > 10 ) {
          for( size_t d = 0; d < read.size(); d++ ) {
            std::cout << "node " << iproc << " in " << d << ": " << read[d].to_string() << std::endl;
          }
        }*/

        if( EXTRA_CHECKS ) {
          assert(read != end);
        }

        if( read.size() > 0 ) {
          if( DEBUG_DCX > 10 ) {
            printf("node %i in:%s\n",
                   (int) dcx->iproc, (*read).to_string().c_str());
          }
        }

        if( read.size() > 1 ) {
          for( int i = 0; i < RENUMBER_NUM_FOLLOWING; i++ ) {
            doc_renumber_record_t renum;

            renum.doc = read[0].doc;
            renum.next_doc = read[i+1].doc;

            if( DEBUG_DCX > 10 ) {
              printf("node %i out:%s\n",
                     (int) dcx->iproc, renum.to_string().c_str());
            }

            // Output the record we made.
            // self-similarity is not useful in document renumbering...
            if( renum.doc != renum.next_doc ) {
              doc_bins->push_back(renum);
            }
          }
        }

        // Move our sliding window forward
        ++read;
      }

      read.finish();

      // Now the data is in the doc bins (partly sorted by doc values).
    }
  };


  struct docs_filter_node : public pipeline_node {
    const Dcx* dcx;
    // INPUT
    doc_next_by_doc_next_t* input;
    // OUTPUT
    doc_next_by_doc_t* output;

    docs_filter_node(const Dcx* dcx, doc_next_by_doc_next_t* input, doc_next_by_doc_t* output)
      : pipeline_node(dcx, "docs_filter_node d="+dcx->depth_string, dcx->should_print_timing()),
        dcx(dcx), input(input), output(output)
    {
    }

    virtual void run()
    {
      for( bin_idx_t b = 0; b < input->n_bins; b++ ) {
        if( input->is_local(b) ) {
          input->begin_reading_bin(b);
          filter(b);
        }
      }
      output->finish();
    }

    struct next_count {
      document_t next_doc;
      offset_t count;
      std::string to_string() const {
        std::ostringstream os;
        os << "next_count(" << next_doc << ";" << count << ")";
        return os.str();
      }
    };
    struct sort_criterion {
      // "less than" function
      bool operator() (const next_count& a, const next_count& b) const
      {
        // switch order because priority queue sorts descending.
        return a.count > b.count;
      }
    };

    typedef std::priority_queue<next_count,
                                std::vector<next_count>,
                                sort_criterion> my_pq_t;

    void pq_add(my_pq_t& pq, next_count cur_next)
    {
      // Store cur_next to priority queue.
      pq.push(cur_next);
      // Are there too many elements in priority queue?
      if( pq.size() > RENUMBER_NUM_NEXT ) {
        pq.pop(); // remove the least element (lowest count).
      }
    }

    void update_cur_next(doc_renumber_record_t* r, next_count& cur_next, my_pq_t& pq)
    {
      if( r ) {
        // Update cur_next and maybe put something in the priority q.
        if( cur_next.count > 0 && cur_next.next_doc != r->next_doc ) {
          // Store cur_next to priority queue.
          pq_add(pq, cur_next);
          // Start with empty cur_next again.
          cur_next.next_doc = dcx->n_docs;
          cur_next.count = 0;
        }

        cur_next.next_doc = r->next_doc;
        cur_next.count++;

      } else {
        pq_add(pq, cur_next);
      }
    }

    void output_pq(document_t cur_doc, my_pq_t& pq, std::vector<next_count>& vec)
    {
      // output the list of top ones..
      vec.clear();
      // save pq elements in vec.
      while( ! pq.empty() ) {
        if( DEBUG_DCX > 10 ) {
          printf("node %i pq top: %s\n",
                 (int) dcx->iproc, pq.top().to_string().c_str());
        }
        vec.push_back(pq.top());
        pq.pop();
      }
      //std::cout << "vec size is " << vec.size() << std::endl;

      // append elements of vector in reverse.
      for( ssize_t i = vec.size() - 1; i >= 0; i-- ) {
        doc_renumber_record_t out;
        out.doc = cur_doc;
        out.next_doc = vec[i].next_doc;

        if( DEBUG_DCX > 10 ) {
          printf("node %i out: %s\n",
                 (int) dcx->iproc, out.to_string().c_str());
        }

        output->push_back(out);
      }
      vec.clear();
    }

    void filter( bin_idx_t bin )
    {
      typename doc_next_by_doc_next_t::bin_t* input_bin = &input->bins[bin];

      if( DEBUG_DCX ) {
        printf("node %i filter_docs bin=%i fname is %s num_records is %lli\n",
               (int) dcx->iproc, (int) bin,
               input_bin->fname.c_str(), (long long int) input_bin->num_records);
      }

      // sorting bwt bins already done
      
      pipe_iterator<doc_renumber_record_t> read(input_bin->from_bin);
      pipe_iterator<doc_renumber_record_t> end;
      my_pq_t pq;
      std::vector<next_count> vec;

      document_t cur_doc = dcx->n_docs;
      next_count cur_next;

      cur_next.next_doc = dcx->n_docs;
      cur_next.count = 0;

      // Read the file.
    
      // do work for loop
      assert(input_bin->total_computed);

      for( document_t i = 0;
           i < input_bin->num_records;
           i++ ) {
        if( DEBUG_DCX > 10 ) {
          printf("node %i in %s\n", (int) dcx->iproc, (*read).to_string().c_str());
        }

        if( EXTRA_CHECKS ) {
          assert(read != end);
        }

        doc_renumber_record_t r = *read;

        if( r.doc != cur_doc ) {
          // Different document; maybe output list of most likely followers
          if( cur_doc != dcx->n_docs ) {  // valid record.
            update_cur_next(&r, cur_next, pq);
            // output the list of top ones..
            output_pq(cur_doc, pq, vec);
          }

          if( EXTRA_CHECKS ) assert( pq.empty() );

          cur_doc = r.doc;
          cur_next.next_doc = dcx->n_docs;
          cur_next.count = 0;
        }

        update_cur_next(&r, cur_next, pq);
        // Move our sliding window forward
        ++read;
      }

      if( cur_next.count > 0 ) {
        update_cur_next(NULL, cur_next, pq);
      }
      if( cur_doc != dcx->n_docs ) {  // valid record.
        // output the list of top ones..
        output_pq(cur_doc, pq, vec);
      }

      read.finish();

      // Now the data is in the doc bins (partly sorted by doc values).
    }
  };



  /* Suffix sort data from an input pipe available to node 0,
   * storing the data in an output pipe also available to node 0.
   * While the input and output are only on node 0, each node
   * must call this routine (passing NULL for the pipes if they
   * are not node 0).
   */
  /* Suffix sorts but also computes the previous character
   * so that it outputs a combined bwt/suffix array:
   * offset, previous-character
   * i, T[i-1]
   */ 
  void bwt(alpha_t min_char,
           alpha_t max_char,
           alpha_t doc_end_char, // all docs must end with this.
           read_pipe* prepared_input,
           std::string info_filename,
           index_block_param_t* params,
           std::string index_path
           )
  {
    if( DEBUG_DCX ) {
      printf("node %i suffix_sort+bwt (outer)\n", (int) iproc);
    }

    start_clock();

    // make sure max_char + 1 can be stored.
    //character_t max_char_plus_1 = max_char;
    //max_char_plus_1++;
    //assert(max_char_plus_1>0); // no overflow!
    //set_maxchar(max_char_plus_1);

    assert(min_char > 0);
    set_maxchar(max_char);

    // adjust rank bin size to properly handle
    // chunk_size/bucket_size
    int64_t block_sz = params->block_size;
    {
      int64_t cur_sz = rank_bin_size;
      int64_t block_bin_size_blocks = ceildiv(cur_sz, block_sz);
      block_bin_size_rows = block_bin_size_blocks * block_sz;

      printf("Block bins are %lli rows with %lli index blocks\n",
             (long long int) block_bin_size_rows,
             (long long int) block_bin_size_blocks );
    }
    // Figure out the mapping between blocks and bins.
    // This is used/checked by write_index per bin
    // and in constructing the header (since we have
    // to copy the right index blocks that were created
    // on each bin).
    std::vector< std::vector<int64_t> > bins_to_blocks;
    bins_to_blocks.resize(n_bins);

    int64_t n_int = n;
    int64_t cur_bin_row = 0;
    int64_t block_idx = 0;
    int64_t n_blocks;

    for( bin_idx_t b = 0; b < n_bins; b++ ) {
      int64_t cur_bin_sz = block_bin_size_rows;
      if( cur_bin_row + cur_bin_sz > n_int ) {
        cur_bin_sz = n_int - cur_bin_row;
      }

      if( cur_bin_sz > 0 ) {
        int64_t cur_block_row = 0;
        while( cur_block_row < cur_bin_sz ) {
          int64_t cur_block_sz = block_sz;
          if( cur_block_row + cur_block_sz > cur_bin_sz ) {
            cur_block_sz = cur_bin_sz - cur_block_row;
          }

          if( cur_block_sz > 0 ) {
            if( iproc == 0 ) {
              printf("bin %lli will make data block %lli size %lli at row %lli\n",
                     (long long int) b,
                     (long long int) block_idx,
                     (long long int) cur_block_sz,
                     (long long int) cur_bin_row + cur_block_row);
            }

            assert( cur_bin_row + cur_block_row == (int64_t) block_sz*block_idx );
            bins_to_blocks[b].push_back(block_idx);
          }

          cur_block_row += cur_block_sz;
          block_idx++;
        }
      }

      cur_bin_row += cur_bin_sz;
    }

    n_blocks = block_idx;
    assert( n_blocks * block_sz >= n_int );

    assert(ALPHA_SIZE_BITS==nbits_character);
    assert(sizeof(character_t) == sizeof(alpha_t));

    //assert(len.offset == n);

    /*if( iproc == 0 ) {
      // make sure that we have the right number of filenames.
      assert(output_bwt_filenames.size() == n_bins);
      assert(output_map_filenames.size() == n_bins);
    }*/

    // Create the in_bins

    typedef read_input_node<alpha_t> read_input_node_t;
    typename read_input_node_t::input_by_offset_t in_bins(this, "input");

    read_input_node_t read_input(this, prepared_input, &in_bins);

    in_bins.begin_writing();
    read_input.start();
    dcx_g_handler->work();
    read_input.finish();
    in_bins.end_writing();

    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("read input", n);
    }

    start_clock();

    // OK. Now output the data to the output bins.
    // Note -- the code would be faster for plain suffix
    // sorts if this was offset_rank_by_rank_t.
    offset_rank_by_offset_t rank_bins( this, "output_ranks");

    // Call suffix_sort
    //suffix_sort_impl(&in_bins, AddOneTranslator<alpha_t>(), &rank_bins);
    // We don't need to add one to our alpha_t because alpha_t is
    // gauranteed never to be zero.
    suffix_sort_impl(&in_bins, false, /* don't delete input */
                     IdentityTranslator<alpha_t>(), &rank_bins,
                     offset_bin_size,
                     bin_overlap);
    // There is a barrier in there!

    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("suffix sort (in bwt)", n);
    }

    if( SHOULD_PRINT_PROGRESS ) {
      printf("form bwt records n=%lli\n", (long long int) n);
    }
    start_clock();

    FILE* info_f = fopen(info_filename.c_str(), "r");
    if( !info_f ) throw error(ERR_IO_STR_OBJ("could not open info file", info_filename.c_str()));
    bwt_document_info_reader_t info;
    error_t err;
    err = bwt_document_info_reader_open(&info, info_f);
    if( err ) throw error(err);

    // Set the number of docs.
    n_docs = bwt_document_info_reader_num_docs(&info);
    doc_bin_size_docs = ceildiv(n_docs, n_bins);

    // Note that at this point, we have the (offset,rank) records
    // distributed by offset.
    
    // Compute the bwt records and distribute them by rank.
    bwt_by_rank_t bwt_bins( this, "output_bwt_r");

    form_bwt_node form_bwt(this, &in_bins, &rank_bins, doc_end_char, &info, &bwt_bins);

    in_bins.begin_reading(true /* now delete input */);
    rank_bins.begin_reading();
    begin_writing_even(&bwt_bins, block_bin_size_rows, 1); 

    form_bwt.start();
    dcx_g_handler->work();
    form_bwt.finish();
    bwt_bins.end_writing();
    rank_bins.end_reading();
    in_bins.end_reading();

    // Remove temporary files!
    in_bins.clear();
    rank_bins.clear();

    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("form_bwt", n);
    }

    if( RENUMBER_DOCUMENTS ) {
      if( SHOULD_PRINT_PROGRESS ) {
        printf("renumber documents n=%lli\n", (long long int) n);
      }
      start_clock();

      doc_next_by_doc_next_t doc_bins( this, "renumber_docs" );

      bwt_to_docs_node bwt_to_docs(this, &bwt_bins, &doc_bins );

      bwt_bins.begin_reading(false); // don't delete bwt bins.
      begin_writing_even_estimate(&doc_bins, doc_bin_size_docs, 0, ceildiv_safe(n, n_docs));
      bwt_to_docs.start();
      dcx_g_handler->work();
      bwt_to_docs.finish();
      doc_bins.end_writing();
      bwt_bins.end_reading();

      // Now filter down to e.g. top 3 next-docs for each doc.
      doc_next_by_doc_t filtered_doc_bins( this, "top_renumber_docs", true /* all on 0 */);

      docs_filter_node filter_docs(this, &doc_bins, &filtered_doc_bins);

      doc_bins.begin_reading();
      begin_writing_even_estimate(&filtered_doc_bins, doc_bin_size_docs, 0, RENUMBER_NUM_NEXT*doc_bin_size_docs);
      filter_docs.start();
      dcx_g_handler->work();
      filter_docs.finish();
      filtered_doc_bins.end_writing();
      doc_bins.end_reading();

      filtered_doc_bins.begin_reading();
      // Now create a vector renumbering the documents and share it.
      {
        doc_renumber = new std::vector<document_t>(n_docs);

        if( iproc == 0 ) {
          document_t largest_doc = n_docs;
          int64_t largest_size = 0;

          // Figure out the largest doc.
          for( document_t d = 0; d < n_docs; d++ ) {
            int64_t sz = 0;
            err = bwt_document_info_reader_doc_size(&info, d, &sz);
            if( err ) throw error(err);
            if( sz > largest_size ) {
              largest_size = sz;
              largest_doc = d;
            }
          }
          assert( largest_doc != n_docs );

          // Now read all of the next_docs into an array.
          std::vector<document_t> next_docs(RENUMBER_NUM_NEXT*n_docs);
          document_t cur_doc = n_docs;
          document_t next_i = 0;

          for( size_t i = 0; i < RENUMBER_NUM_NEXT*n_docs; i++ ) {
            next_docs[i] = n_docs;
          }

          // On node 0, read all of the doc bins.
          for( bin_idx_t b = 0; b < filtered_doc_bins.n_bins; b++ ) {
            assert( filtered_doc_bins.is_local(b) );
            filtered_doc_bins.begin_reading_bin(b);

            typename doc_next_by_doc_t::bin_t* input_bin = &filtered_doc_bins.bins[b];

            // Go through all records in this bin.
            pipe_iterator<doc_renumber_record_t> read(input_bin->from_bin);
            pipe_iterator<doc_renumber_record_t> end;

            while( read != end ) {
              doc_renumber_record_t r = *read;

              if( r.doc != cur_doc ) {
                cur_doc = r.doc;
                next_i = 0;
              }

              next_docs[RENUMBER_NUM_NEXT*cur_doc + next_i] = r.next_doc;
              next_i++;

              ++read;
            }

            read.finish();
          }

          // Set up doc_renumber as n_docs for all of them
          // values==n_docs will mean 'not assigned'.
          for( size_t i = 0; i < n_docs; i++ ) {
            (*doc_renumber)[i] = n_docs;
          }

          // Now assign document numbers.
          cur_doc = largest_doc;
          next_i = 0;

          while( 1 ) {
            // Set cur_doc's position.
            if( (*doc_renumber)[cur_doc] == n_docs ) {
              (*doc_renumber)[cur_doc] = next_i++;
            }

            bool found_next = false;
            for( size_t i = 0; i < RENUMBER_NUM_NEXT; i++ ) {
              document_t next_doc = next_docs[RENUMBER_NUM_NEXT*cur_doc + i];
              if( next_doc != n_docs && (*doc_renumber)[next_doc] == n_docs ) {
                cur_doc = next_doc;
                found_next = true;
                break;
              }
            }

            if( ! found_next ) {
              document_t start_cur_doc = cur_doc;

              cur_doc++;
              if( cur_doc >= n_docs ) cur_doc = 0;

              // Increment cur_doc until we've found an empty one.
              while( (*doc_renumber)[cur_doc] != n_docs &&
                     cur_doc != start_cur_doc ) {
                cur_doc++;
                if( cur_doc >= n_docs ) cur_doc = 0;
              }

              if( cur_doc == start_cur_doc ) break;
            }
          }
        }

        if( EXTRA_CHECKS ) {
          // Verify that doc_renumber is set for all docs.
          for( document_t d = 0; d < n_docs; d++ ) {
            assert((*doc_renumber)[d] != n_docs);
          }
        }

        // Share the vector on node 0 with the other nodes.
        mpi_share_vector(comm, doc_renumber, 0);
      }
      filtered_doc_bins.end_reading();

      // Create a new info file based on doc_renumber.
      std::string new_info = info_filename + "new";
      if( iproc == 0 ) {
        std::vector<offset_t> new_num_to_old_num(n_docs);

        for( document_t d = 0; d < n_docs; d++ ) {
          new_num_to_old_num[d] = n_docs;
        }

        for( document_t d = 0; d < n_docs; d++ ) {
          document_t new_num = (*doc_renumber)[d];
          new_num_to_old_num[new_num] = d;
        }

        if( EXTRA_CHECKS ) {
          // Verify that new_num_to_old_num is set for all docs.
          for( document_t d = 0; d < n_docs; d++ ) {
            assert(new_num_to_old_num[d] != n_docs);
          }
        }

        bwt_document_info_writer_t writer;
        bwt_document_info_start_write(&writer, new_info.c_str());

        for( document_t new_number = 0; new_number < n_docs; new_number++ ) {
          document_t old_num = new_num_to_old_num[new_number];
          int64_t info_len = 0;
          unsigned char* info_str = NULL;
          int64_t doc_sz = 0;

          err = bwt_document_info_reader_get(&info, old_num, &info_len, &info_str);
          if( err ) throw error(err);
          err = bwt_document_info_reader_doc_size(&info, old_num, &doc_sz);
          if( err ) throw error(err);

          err = bwt_document_info_write(&writer, new_number, doc_sz, info_len, info_str);
          if( err ) throw error(err);
        }

        err = bwt_document_info_finish_write(&writer);
        if( err ) throw error(err);

        delete doc_renumber;
      }

      barrier();

      err = bwt_document_info_reader_close(&info);
      if( err ) throw error(err);

      fclose(info_f);

      unlink_ifneeded(info_filename);

      // Copy the file to all mpi guys.
      for( int i = 0; i < nproc; i++ ) {
        // Barrier to:
        // 1) make sure node 0 is done writing the file,
        // 2) serialize the file copy in case we copy over
        //    another file (ie node1 is proc 2,3)
        barrier();
        mpi_copy_file( comm, new_info, 0, info_filename, i );
      }

      // Remove new_info.
      if( iproc == 0 ) {
        unlink_ifneeded(new_info);
      }

      // Reopen the info file for everybody..
      info_f = fopen(info_filename.c_str(), "r");
      if( !info_f ) throw error(ERR_IO_STR_OBJ("could not open info file", info_filename.c_str()));

      err = bwt_document_info_reader_open(&info, info_f);
      if( err ) throw error(err);

      stop_clock();
      if( SHOULD_PRINT_TIMING ) {
        print_timings("renumber_documents", n);
      }
    }

    if( SHOULD_PRINT_PROGRESS ) {
      printf("write index files n=%lli\n", (long long int) n);
    }
    start_clock();

    index_constructor_t cstr;
    err = constructor_create(&cstr, params,
                             index_path.c_str(),
                             n_blocks, &info);
    if( err ) throw error(err);

    // Sort them once again by rank.

    // Turn each bwt into:
    //   -- bwt file
    //   -- document map file
    write_index_files_node write_index_files(this, &bwt_bins, 
                                             doc_end_char,
                                             &bins_to_blocks,
                                             &cstr);

    bwt_bins.begin_reading();
    write_index_files.start();
    dcx_g_handler->work(); // not really necessary, but we're principled.
    write_index_files.finish();
    bwt_bins.end_reading();

    barrier();

    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("write_index_files", n);
    }

    start_clock();

    barrier();

    // Transmit these files back to node 0.
    // Now transmit the file back to node 0.
    // This is an unparallel loop for convenience,
    // and since mpi_copy_file must be reached by all nodes..
    // Either we used NFS, or the data is all going to one 
    // node anyways, so this loop isn't really parallel..
    for(bin_idx_t b=0; b<n_bins; b++) {
      for( size_t j = 0; j < bins_to_blocks[b].size(); j++ ) {
        int64_t block_idx = bins_to_blocks[b][j];
        char* data_block_name = constructor_filename_for_data_block(&cstr, block_idx);
        std::string db_name_str(data_block_name);

        mpi_move_file( comm, 
                       db_name_str, bwt_bins.owner(b),
                       db_name_str, 0 );

        free( data_block_name );
      }
    }

    barrier();

    bwt_bins.clear();

    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("move files to 0", n_bins);
    }


    start_clock();

    if( iproc == 0 ) {
      // Construct the index header block.
      err = constructor_construct_header(&cstr);
      if( err ) throw error(err);
    }

    stop_clock();
    if( SHOULD_PRINT_TIMING ) {
      print_timings("construct header", 1);
    }

    constructor_destroy(&cstr);
    bwt_document_info_reader_close(&info);
    fclose(info_f);

  }
#endif

};

template<typename InCharacter, typename OutOffset, int nbits_char, int nbits_offset>
void suffix_sort(OutOffset len, InCharacter max_char, read_pipe* input_pipe, std::vector<std::string>* output_filenames, std::string tmp_dir)
{
  int started_io_measurement = 0;
  if( REPORT_DISK_USAGE ) {
    error_t err;
    err = start_io_measurement(tmp_dir.c_str());
    if( err ) warn_if_err(err);
    else started_io_measurement = 1;
  }

  Dcx<nbits_char + 1,
      nbits_offset,
      nbits_offset,
      DEFAULT_PERIOD> dcx(tmp_dir, len, output_filenames->size(),
                          DEFAULT_COMM, 0, NULL);

  // We should have at least enough bins for all processors..
  assert( output_filenames->size() >= (size_t) dcx.nproc );

  dcx.suffix_sort(len, max_char, input_pipe, output_filenames);

  if( dcx.should_print_timing() ) {
    dcx.print_times();
  }

  if( REPORT_DISK_USAGE ) {
    error_t err;
    uint64_t bytes_read, bytes_written;

    bytes_read = get_io_total(dcx.get_io_stats(), IO_READ);
    bytes_written = get_io_total(dcx.get_io_stats(), IO_WRITE);

    uint64_t bytes_disk_read = 0;
    uint64_t bytes_disk_written = 0;
    if( started_io_measurement ) { 
      err = stop_io_measurement(tmp_dir.c_str(), &bytes_disk_read, &bytes_disk_written);
      if( err ) throw error(err);
    }

    printf("REPORT DISK_BYTES_READ=%lli\n"
           "REPORT DISK_BYTES_WRITTEN=%lli\n"
           "REPORT DISK_MAX_BYTES_USED=%lli\n"
           "REPORT DISK_BYTES_DISK_READ=%lli\n"
           "REPORT DISK_BYTES_DISK_WRITTEN=%lli\n",
           (long long int) bytes_read,
           (long long int) bytes_written,
           (long long int) dcx_g_max_disk_usage,
           (long long int) bytes_disk_read,
           (long long int) bytes_disk_written);
  }
}

#ifdef BWT_SUPPORT
template<int nbits_offset, int nbits_document>
void do_bwt(int n_bins,
            int64_t len,
            alpha_t min_char,
            alpha_t max_char,
            alpha_t doc_end_char, // all docs must end with this.
            read_pipe* prepared_input, /* alpha_t characters */
            std::string info_filename,
            index_block_param_t* params,
            std::string tmp_dir,
            std::string index_path)
{
  int started_io_measurement = 0;
  if( REPORT_DISK_USAGE ) {
    error_t err;
    err = start_io_measurement(tmp_dir.c_str());
    if( err ) warn_if_err(err);
    else started_io_measurement = 1;
  }

  if( params->chunk_size > 0 ) {
    // bucket size must be multiple of chunk size.
    assert( params->chunk_size <= params->b_size );
    assert( params->b_size % params->chunk_size == 0 );
  }

  Dcx<ALPHA_SIZE_BITS,
      nbits_offset,
      nbits_document,
      DEFAULT_PERIOD> dcx(tmp_dir, len, n_bins, DEFAULT_COMM, 0, NULL);

  dcx.bwt(min_char, max_char, doc_end_char, prepared_input,
          info_filename, params, 
          index_path);

  if( dcx.should_print_timing() ) {
    dcx.print_times();
  }

  if( REPORT_DISK_USAGE ) {
    error_t err;
    uint64_t bytes_read, bytes_written;

    bytes_read = get_io_total(dcx.get_io_stats(), IO_READ);
    bytes_written = get_io_total(dcx.get_io_stats(), IO_WRITE);

    uint64_t bytes_disk_read = 0;
    uint64_t bytes_disk_written = 0;
    if( started_io_measurement ) {
      err = stop_io_measurement(tmp_dir.c_str(), &bytes_disk_read, &bytes_disk_written);
      if( err ) warn_if_err(err);
    }

    printf("REPORT DISK_BYTES_READ=%lli\n"
          "REPORT DISK_BYTES_WRITTEN=%lli\n"
          "REPORT DISK_MAX_BYTES_USED=%lli\n"
          "REPORT DISK_BYTES_DISK_READ=%lli\n"
          "REPORT DISK_BYTES_DISK_WRITTEN=%lli\n",
          (long long int) bytes_read,
          (long long int) bytes_written,
          (long long int) dcx_g_max_disk_usage,
          (long long int) bytes_disk_read,
          (long long int) bytes_disk_written);
  }
}

#endif
// endif BWT_SUPPORT.

#endif

