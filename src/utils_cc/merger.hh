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

  femto/src/utils_cc/merger.hh
*/
#ifndef _MERGER_HH_
#define _MERGER_HH_


#include <cassert>
#include <iostream>
#include <algorithm>
#include <queue>
#include <vector>
#include <deque>
#include <list>
#include <climits>

extern "C" {
#include "processors.h"
}
#include "record.hh"
#include "utils.hh" // PTR_ADD
#include "pipelining.hh"
#include "compare_record.hh"
#include "file_pipe_context.hh"

#define K_WAY 4
//#define K_WAY 9000

/* Performance notes -- on an (8-core Xeon)
 * merging 1024 runs of 1048576 bytes (random ints)
 *
 * heap merger (1 thread)   - 42.45 s
 * merger network 1 thread  - 20.46 s
 * merger network 2 threads - 11.70 s
 * merger network 3 threads -  8.933 s
 * merger network 4 threads -  7.772 s
 * merger network 5 threads -  7.075 s
 * merger network 6 threads -  6.693 s
 * merger network 7 threads -  6.437 s
 * merger network 8 threads -  6.310 s
 */
//  define USE_HEAP_ALWAYS for testing to compare with heap-merger
//#pragma warning unperformant_configuration
//#define USE_HEAP_ALWAYS
#define MERGER_DEBUG 0
#define MERGER_NET_DEBUG 0

template< typename Record,
          typename Criterion >
class heap_merger
{
 public:
  typedef pipe_iterator<Record> reader_t;
  // We'll force merging to be stable by adding an index
  // to each record in the heap.
  struct stable_reader {
    reader_t* iterator;
    size_t idx;
    stable_reader() : iterator(NULL), idx(0) { }
  };
  class ReaderSortingCriterion {
   private:
    RecordSortingCriterion<Record,Criterion> crit;
   public:
    // Constructor saves the context.
    ReaderSortingCriterion(Criterion ctx)
      : crit(ctx)
    {
    }
    int compare(const stable_reader& a, const stable_reader& b) const
    {
      int cmp = crit.compare(**(a.iterator), **(b.iterator));
      if( cmp != 0 ) return cmp;
      if( a.idx < b.idx ) return -1;
      if( a.idx > b.idx ) return 1;
      return 0;
    }
    // Comparison function - "less than"
    bool operator() (const stable_reader& a, 
                     const stable_reader& b)const
    {
      //switch order because priority queue sorts in descending order
      return compare(a,b) > 0;
    }
  };


 private:
  ReaderSortingCriterion criterion;
  std::priority_queue<stable_reader,
                      std::vector<stable_reader>,
                      ReaderSortingCriterion> priority_queue;
  reader_t end;
  size_t cur_idx;
  stable_reader cur_reader;
  bool deletes_pipe;

 public:
  heap_merger(Criterion ctx, size_t num_procs, bool deletes_pipe=false)
    : criterion(ctx), priority_queue(criterion),
      end(), cur_idx(0), cur_reader(),
      deletes_pipe(deletes_pipe)
  {
  }

  // Add a read pipe to be merged. The pipe will be closed when
  // merger is finished with it, and deleted if deletes_pipe is true.
  void add_read_pipe(read_pipe* pipe)
  {
    reader_t* iterator = new reader_t(pipe);

    stable_reader sr;
    sr.iterator = iterator;
    sr.idx = cur_idx++;
    if( *sr.iterator != end ) {
      priority_queue.push(sr);
    } else {
      sr.iterator->finish();
      delete sr.iterator;
      if( deletes_pipe ) {
        delete pipe;
      }
    }
  }

  // Are we done? Is it empty?
  bool empty()
  {
    return priority_queue.empty();
  }

  // Get the smallest record.
  const Record& top()
  {
    cur_reader = priority_queue.top();
    return **cur_reader.iterator;
  }

  void pop()
  {
    // Remove cur_iterator from the priority queue.
    priority_queue.pop();

    // Move on to the next record.
    ++(*cur_reader.iterator);
    if(*cur_reader.iterator != end)
    {
      priority_queue.push(cur_reader);
    } else {
      read_pipe* pipe = NULL;
      // If it's reached the end, destroy it
      //flush the pipe...
      cur_reader.iterator->finish();
      if( deletes_pipe ) {
        pipe = cur_reader.iterator->get_pipe();
      }
      delete cur_reader.iterator;
      cur_reader.iterator = NULL;
      if( deletes_pipe ) {
        delete pipe;
      }
    }

    // Clear out the current iterator.
    stable_reader empty_reader;
    cur_reader = empty_reader;
  }
  size_t size()
  {
    return priority_queue.size();
  }
  void start() 
  {
  }
  void finish()
  {
  }
};


// these are used in do_merge
struct my_input_info {
  tile_pos tp;
  read_pipe* p;
};
typedef std::list<my_input_info> my_input_info_list_t;
typedef my_input_info_list_t::iterator my_input_info_list_iter_t;

template< typename Record,
          typename Criterion >
struct merger {
  typedef pipe_iterator<Record> reader_t;
  typedef pipe_back_inserter<Record> writer_t;
  typedef heap_merger<Record,Criterion> heap_merger_t;
  typedef RecordTraits<Record> record_traits;

  enum {
    MERGE_NEED_OUTPUT=-1,
    MERGE_NEED_INPUT=0 // positive input number needed.
  };

  static int do_merge1(Criterion ctx, tile_pos* input0, tile* output)
  {
    Record r;
    size_t r_len;

    // Check to see if we need input
    if( input0->pos >= input0->t.len ) return MERGE_NEED_INPUT+0;
    // Read the record.
    record_traits::decode(r, PTR_ADD(input0->t.data,input0->pos));
    r_len = record_traits::get_record_length(r);

    while( 1 ) {
      // Check to see if the output fits.
      if( output->len + r_len > output->max ) return MERGE_NEED_OUTPUT;

      // Write the record
      record_traits::encode(r, PTR_ADD(output->data,output->len));
      output->len += r_len;
      // Print out handy debug message.
      //if( MERGER_DEBUG )
      //  std::cout<<"merge1 in/out: "<<r.to_string()<<std::endl;
      //
      // Move on to the next input.
      input0->pos += r_len;
      if( input0->pos >= input0->t.len ) return MERGE_NEED_INPUT+0;
      // Read the record.
      record_traits::decode(r, PTR_ADD(input0->t.data,input0->pos));
      r_len = record_traits::get_record_length(r);
    }
  }

  static int do_merge2(Criterion ctx, tile_pos* input0, tile_pos* input1, tile* output)
  {
    RecordSortingCriterion<Record,Criterion> crit(ctx);
    Record r0;
    Record r1;
    size_t r_len0;
    size_t r_len1;

    // Check to see if we need input
    if( input0->pos >= input0->t.len ) return MERGE_NEED_INPUT+0;
    if( input1->pos >= input1->t.len ) return MERGE_NEED_INPUT+1;
    // Decode the records
    record_traits::decode(r0, PTR_ADD(input0->t.data,input0->pos));
    r_len0 = record_traits::get_record_length(r0);
    record_traits::decode(r1, PTR_ADD(input1->t.data,input1->pos));
    r_len1 = record_traits::get_record_length(r1);
   
#define DEBUG_PRINT_2(out)
/*
    if( MERGER_DEBUG )
          std::cout<<"merge2 in0: " << r0.to_string()
                   <<" in1: " << r1.to_string()
                   <<" out1: " << out.to_string() << std::endl;
*/
    while( 1 ) {
      // Compare record 1 and record 0.
      if(crit(r1,r0)) { // 1 < 0
        // Check to see if the output fits
        if( output->len + r_len1 > output->max ) return MERGE_NEED_OUTPUT;
        // Write the record
        record_traits::encode(r1, PTR_ADD(output->data,output->len));
        output->len += r_len1;
        // Print out handy debug message.
        DEBUG_PRINT_2(r1);
        // Move on to the next input
        input1->pos += r_len1;
        if( input1->pos >= input1->t.len ) return MERGE_NEED_INPUT+1;
        record_traits::decode(r1, PTR_ADD(input1->t.data,input1->pos));
        r_len1 = record_traits::get_record_length(r1);
      } else { // 0 <= 1
        // Check to see if the output fits
        if( output->len + r_len0 > output->max ) return MERGE_NEED_OUTPUT;
        // Write the record
        record_traits::encode(r0, PTR_ADD(output->data,output->len));
        output->len += r_len0;
        // Print out handy debug message.
        DEBUG_PRINT_2(r0);
        // Move on to the next input
        input0->pos += r_len0;
        if( input0->pos >= input0->t.len ) return MERGE_NEED_INPUT+0;
        record_traits::decode(r0, PTR_ADD(input0->t.data,input0->pos));
        r_len0 = record_traits::get_record_length(r0);
      }
    }
  }

  static int do_merge3(Criterion ctx, tile_pos* input0, tile_pos* input1, tile_pos* input2, tile* output)
  {
    RecordSortingCriterion<Record,Criterion> crit(ctx);
    Record r0;
    Record r1;
    Record r2;
    size_t r_len0;
    size_t r_len1;
    size_t r_len2;

    // Check to see if we need input
    if( input0->pos >= input0->t.len ) return MERGE_NEED_INPUT+0;
    if( input1->pos >= input1->t.len ) return MERGE_NEED_INPUT+1;
    if( input2->pos >= input2->t.len ) return MERGE_NEED_INPUT+2;
    // Decode the records
    record_traits::decode(r0, PTR_ADD(input0->t.data,input0->pos));
    r_len0 = record_traits::get_record_length(r0);
    record_traits::decode(r1, PTR_ADD(input1->t.data,input1->pos));
    r_len1 = record_traits::get_record_length(r1);
    record_traits::decode(r2, PTR_ADD(input2->t.data,input2->pos));
    r_len2 = record_traits::get_record_length(r2);

    if (crit.compare(r0,r1) <= 0) // r0 <= r1
    {
      if (crit.compare(r1,r2) <= 0) // r1 <= r2
        goto s012;
      else
        // r0 <= r1 and r2 < r1
        if (crit.compare(r0,r2) <= 0) // r0 <= r2
          goto s021;
        else
          goto s201;
    }
    else
    {
      // r1 < r0
      if (crit.compare(r1,r2) <= 0) // r1 <= r2
      {
        // r1 < r0; r1 <= r2
        if (crit.compare(r0, r2) <= 0) // r0 <= r2
          goto s102;
        else
          goto s120;
      }
      else // r1 < r0; r2 < r1
        goto s210;
    }
#define DEBUG_PRINT_3(a,b,c,c0,c1)
/*
      if( MERGER_DEBUG )                                                      \
        std::cout<<"merge3 in0: "<<(r ## a).to_string()                    \
                 <<" in1: "<<(r ## b).to_string()                          \
                 <<" in2: "<<(r ## c).to_string()                          \
                 <<" out: "<<(r ## a).to_string()<<std::endl;              \
*/ 
#define _MERGE_3_CASE(a,b,c,c0,c1)                                            \
    s ## a ## b ## c :                                                        \
      if( output->len + r_len ## a > output->max ) return MERGE_NEED_OUTPUT;  \
      record_traits::encode((r ## a), PTR_ADD(output->data,output->len));     \
      output->len += r_len ## a;                                              \
      DEBUG_PRINT_3(a,b,c,c0,c1);                                             \
     (input ## a)->pos += r_len ## a;                                         \
      if( (input ## a)->pos >= (input ## a)->t.len  )                         \
         return MERGE_NEED_INPUT+a;                                           \
      record_traits::decode((r ## a),                                         \
                             PTR_ADD((input ## a)->t.data,(input ## a)->pos));\
      r_len ## a = record_traits::get_record_length((r ## a));           \
      if (crit.compare(r ## a, r ## b) c0 0) goto s ## a ## b ## c;           \
      if (crit.compare(r ## a, r ## c) c1 0) goto s ## b ## a ## c;           \
      goto s ## b ## c ## a;

    _MERGE_3_CASE(0, 1, 2, <=, <=);
    _MERGE_3_CASE(1, 2, 0, <=, < );
    _MERGE_3_CASE(2, 0, 1, < , < );
    _MERGE_3_CASE(1, 0, 2, < , <=);
    _MERGE_3_CASE(0, 2, 1, <=, <=);
    _MERGE_3_CASE(2, 1, 0, < , < );

#undef _MERGE_3_CASE
  }

  static int do_merge4(Criterion ctx, tile_pos* input0, tile_pos* input1, tile_pos* input2, tile_pos* input3, tile* output)
  {
    RecordSortingCriterion<Record,Criterion> crit(ctx);
    Record r0;
    Record r1;
    Record r2;
    Record r3;
    size_t r_len0;
    size_t r_len1;
    size_t r_len2;
    size_t r_len3;

    // Check to see if we need input
    if( input0->pos >= input0->t.len ) return MERGE_NEED_INPUT+0;
    if( input1->pos >= input1->t.len ) return MERGE_NEED_INPUT+1;
    if( input2->pos >= input2->t.len ) return MERGE_NEED_INPUT+2;
    if( input3->pos >= input3->t.len ) return MERGE_NEED_INPUT+3;
    // Decode the records
    record_traits::decode(r0, PTR_ADD(input0->t.data,input0->pos));
    r_len0 = record_traits::get_record_length(r0);
    record_traits::decode(r1, PTR_ADD(input1->t.data,input1->pos));
    r_len1 = record_traits::get_record_length(r1);
    record_traits::decode(r2, PTR_ADD(input2->t.data,input2->pos));
    r_len2 = record_traits::get_record_length(r2);
    record_traits::decode(r3, PTR_ADD(input3->t.data,input3->pos));
    r_len3 = record_traits::get_record_length(r3);

#define _DECISION(a,b,c,d) {                            \
      if (crit.compare(r ## d,r ## a) < 0) goto s ## d ## a ## b ## c;	\
      if (crit.compare(r ## d,r ## b) < 0) goto s ## a ## d ## b ## c;	\
      if (crit.compare(r ## d,r ## c) < 0) goto s ## a ## b ## d ## c;	\
      goto s ## a ## b ## c ## d;  }

    if (crit.compare(r0, r1) <= 0)
    {
      if (crit.compare(r1, r2) <= 0)
        _DECISION(0,1,2,3)
      else
        if (crit.compare(r2, r0) < 0)
          _DECISION(2,0,1,3)
        else
          _DECISION(0,2,1,3)
    } else {
        if (crit.compare(r1, r2) <= 0)
        {
          if (crit.compare(r0, r2) <=0)
            _DECISION(1,0,2,3)
          else
            _DECISION(1,2,0,3)
        } else
          _DECISION(2,1,0,3)
    }

#define DEBUG_PRINT_4(a,b,c,d,c0,c1,c2)
/*
    if( MERGER_DEBUG )                                                        \
      std::cout<<"merge4 in0: "<<(r ## a).to_string()                      \
               <<" in1: "<<(r ## b).to_string()                            \
               <<" in2: "<<(r ## c).to_string()                            \
               <<" in3: "<<(r ## d).to_string()                            \
               <<" out: "<<(r ## a).to_string()<<std::endl;                \
*/ 
#define _GLIBCXX_PARALLEL_MERGE_4_CASE(a,b,c,d,c0,c1,c2)                      \
    s ## a ## b ## c ## d:                                                    \
    if( output->len + r_len ## a > output->max ) return MERGE_NEED_OUTPUT;    \
    record_traits::encode((r ## a), PTR_ADD(output->data,output->len));       \
    output->len += r_len ## a;                                                \
    DEBUG_PRINT_4(a,b,c,d,c0,c1,c2)                                           \
    (input ## a)->pos += r_len ## a;                                          \
    if( (input ## a)->pos >= (input ## a)->t.len )                            \
      return MERGE_NEED_INPUT+a;                                              \
    record_traits::decode((r ## a),                                           \
                           PTR_ADD((input ## a)->t.data,(input ## a)->pos));  \
    r_len ## a = record_traits::get_record_length((r ## a));                  \
    if (crit.compare(r ## a, r ## b) c0 0) goto s ## a ## b ## c ## d;        \
    if (crit.compare(r ## a, r ## c) c1 0) goto s ## b ## a ## c ## d;        \
    if (crit.compare(r ## a, r ## d) c2 0) goto s ## b ## c ## a ## d;        \
    goto s ## b ## c ## d ## a;

    _GLIBCXX_PARALLEL_MERGE_4_CASE(0, 1, 2, 3, <=, <=, <=);
    _GLIBCXX_PARALLEL_MERGE_4_CASE(0, 1, 3, 2, <=, <=, <=);
    _GLIBCXX_PARALLEL_MERGE_4_CASE(0, 2, 1, 3, <=, <=, <=);
    _GLIBCXX_PARALLEL_MERGE_4_CASE(0, 2, 3, 1, <=, <=, <=);
    _GLIBCXX_PARALLEL_MERGE_4_CASE(0, 3, 1, 2, <=, <=, <=);
    _GLIBCXX_PARALLEL_MERGE_4_CASE(0, 3, 2, 1, <=, <=, <=);
    _GLIBCXX_PARALLEL_MERGE_4_CASE(1, 0, 2, 3, < , <=, <=);
    _GLIBCXX_PARALLEL_MERGE_4_CASE(1, 0, 3, 2, < , <=, <=);
    _GLIBCXX_PARALLEL_MERGE_4_CASE(1, 2, 0, 3, <=, < , <=);
    _GLIBCXX_PARALLEL_MERGE_4_CASE(1, 2, 3, 0, <=, <=, < );
    _GLIBCXX_PARALLEL_MERGE_4_CASE(1, 3, 0, 2, <=, < , <=);
    _GLIBCXX_PARALLEL_MERGE_4_CASE(1, 3, 2, 0, <=, <=, < );
    _GLIBCXX_PARALLEL_MERGE_4_CASE(2, 0, 1, 3, < , < , <=);
    _GLIBCXX_PARALLEL_MERGE_4_CASE(2, 0, 3, 1, < , <=, < );
    _GLIBCXX_PARALLEL_MERGE_4_CASE(2, 1, 0, 3, < , < , <=);
    _GLIBCXX_PARALLEL_MERGE_4_CASE(2, 1, 3, 0, < , <=, < );
    _GLIBCXX_PARALLEL_MERGE_4_CASE(2, 3, 0, 1, <=, < , < );
    _GLIBCXX_PARALLEL_MERGE_4_CASE(2, 3, 1, 0, <=, < , < );
    _GLIBCXX_PARALLEL_MERGE_4_CASE(3, 0, 1, 2, < , < , < );
    _GLIBCXX_PARALLEL_MERGE_4_CASE(3, 0, 2, 1, < , < , < );
    _GLIBCXX_PARALLEL_MERGE_4_CASE(3, 1, 0, 2, < , < , < );
    _GLIBCXX_PARALLEL_MERGE_4_CASE(3, 1, 2, 0, < , < , < );
    _GLIBCXX_PARALLEL_MERGE_4_CASE(3, 2, 0, 1, < , < , < );
    _GLIBCXX_PARALLEL_MERGE_4_CASE(3, 2, 1, 0, < , < , < );

#undef _MERGE_4_CASE
#undef _DECISION

  }

  // Merger routine. Takes in a list of tile_pos and a tile for output.
  // Returns an iterator pointing to the input needed next, or
  // an EOF iterator if we need more output.
  template<typename ReaderInfo>
  static typename std::list<ReaderInfo>::iterator do_merge(Criterion ctx, std::list<ReaderInfo> *ins, tile* out)
  {
    typedef typename std::list<ReaderInfo>::iterator it_t;
    size_t size = ins->size();
    it_t it;
    it = ins->begin();
    if( size == 1 ) {
      it_t in0 = it;
      int ret = do_merge1(ctx, &(*in0).tp, out);
      if( ret == MERGE_NEED_OUTPUT ) {
        return ins->end();
      } else if( ret == MERGE_NEED_INPUT ) {
        return in0;
      } else assert(0);
    } else if( size == 2 ) {
      it_t in0 = it;
      ++it;
      it_t in1 = it;
      int ret = do_merge2(ctx, &(*in0).tp, &(*in1).tp, out);
      if( ret == MERGE_NEED_OUTPUT ) {
        return ins->end();
      } else if( ret == MERGE_NEED_INPUT ) {
        return in0;
      } else if( ret == MERGE_NEED_INPUT+1 ) {
        return in1;
      } else assert(0);
    } else if( size == 3 ) {
      it_t in0 = it;
      ++it;
      it_t in1 = it;
      ++it;
      it_t in2 = it;
      int ret = do_merge3(ctx, &(*in0).tp, &(*in1).tp, &(*in2).tp, out);
      if( ret == MERGE_NEED_OUTPUT ) {
        return ins->end();
      } else if( ret == MERGE_NEED_INPUT ) {
        return in0;
      } else if( ret == MERGE_NEED_INPUT+1 ) {
        return in1;
      } else if( ret == MERGE_NEED_INPUT+2 ) {
        return in2;
      } else assert(0);
    } else if( size == 4 ) {
      it_t in0 = it;
      ++it;
      it_t in1 = it;
      ++it;
      it_t in2 = it;
      ++it;
      it_t in3 = it;
      int ret = do_merge4(ctx, &(*in0).tp, &(*in1).tp, &(*in2).tp, &(*in3).tp, out);
      if( ret == MERGE_NEED_OUTPUT ) {
        return ins->end();
      } else if( ret == MERGE_NEED_INPUT ) {
        return in0;
      } else if( ret == MERGE_NEED_INPUT+1 ) {
        return in1;
      } else if( ret == MERGE_NEED_INPUT+2 ) {
        return in2;
      } else if( ret == MERGE_NEED_INPUT+3 ) {
        return in3;
      } else assert(0);
    } else {
      assert(0);
      /*
      heap_merger_t merger(ctx, 1, false);

      for( size_t i = 0; i < read_pipes->size(); i++ ) {
        merger.add_read_pipe((*read_pipes)[i]);
      }

      while( ! merger.empty() ) {
        if( MERGER_DEBUG )
          std::cout<<"mergek out: "<<merger.top().to_string(ctx)<<std::endl;
        writer->push_back(merger.top());
        merger.pop();
      }*/
    }
    // avoid no-return error...
    return it;
  }


  /* Closes the output pipe after the merge is completed.
   * This is really pretty much just for testing.
   */
  static void do_merge(Criterion ctx, std::vector<read_pipe*> *ins, write_pipe* out)
  {
    my_input_info_list_t in_list;
    std::vector<my_input_info_list_iter_t> iters;

    // Go through the inputs, getting a tile for in_list.
    for( size_t i = 0; i < ins->size(); i++ ) {
      my_input_info info;
      info.tp.pos = 0;
      info.p = (*ins)[i];
      info.tp.t = info.p->get_full_tile();
      if( info.tp.t.has_tile() ) {
        my_input_info_list_iter_t it = in_list.insert(in_list.end(), info);
        iters.push_back(it);
      }
    }

    // Now get an output tile.
    tile output_tile = out->get_empty_tile();
    assert(output_tile.has_tile());

    do {
      // Do a little merging...
      my_input_info_list_iter_t it = do_merge(ctx, &in_list, &output_tile);
      // now it is the position of the exhausted input, or end() for output
      // is exhausted.
      if( it == in_list.end() ) {
        out->put_full_tile(output_tile);
        output_tile = out->get_empty_tile();
        assert(output_tile.has_tile());
      } else {
        // it is a pointer into in_list which has a pipe in it.
        my_input_info *info = &(*it);
        info->p->put_empty_tile(info->tp.t);
        info->tp.t = info->p->get_full_tile();
        if( info->tp.t.is_end() ) {
          info->p->close_empty();
          in_list.erase(it);
        }
      }
    } while( ! in_list.empty() );

    out->put_full_tile(output_tile);

    out->close_full();
  }
};


// A network of mergers - each is a different thread
// Here, we use k-way mergers.
template< typename Record,
          typename Criterion >
class merger_network
{
  typedef pipe_iterator<Record> reader_t;
  typedef pipe_back_inserter<Record> writer_t;
  typedef std::vector <read_pipe*> read_pipe_ptr_vec;

  // Forward declarations.
  struct merger_tree_node;
  struct Callback;

  struct input_info { 
    read_pipe* leaf_pipe; // for a leaf node, input pipe
    merger_tree_node* child; // intermediate node child (use child[i]->pipe)
    tile_pos tp; // the current full tile for this input.. plus pos
    input_info(read_pipe* p)
      : leaf_pipe(p), child(NULL), tp()
    {
    }
    input_info(merger_tree_node* n)
      : leaf_pipe(NULL), child(n), tp()
    {
    }
  };
  typedef std::list<input_info> inputs_list_t;
  typedef typename inputs_list_t::iterator inputs_iter_t;
  enum {
    STATE_TO_DELETE=-5,
    STATE_AT_EOF=-4,
    STATE_STARTING=-3,
    STATE_WAIT_OUTPUT=-2,
    STATE_RUNNABLE=-1,
    STATE_WAIT_INPUT=0,
    STATE_WAIT_ALL_INPUTS=1,
  };

  struct merger_tree_node
  {
    // The lock is held when any member data of a node is read/written.
    // Also, when locking more than one node, first the parent
    // is locked and then the children (in increasing order).
    // That way the other locks are always acquired in an order;
    // deadlock is not possible.
    
    pthread_mutex running; // set by thread running this node.
    // The node lock - locked whenever the node data is used.
    pthread_rwlock lock;

    int state; // doesn't require write lock to be set (only read lock+running)
    inputs_iter_t cur_input; // when going through the input
                             // adding requests, use this iterator.

    // The rwlock lock protects these data members
    // The write pipe for the final output. closed, but NOT DELETED!
    write_pipe* final_output;
    // the output to the parent of this node; deleted when this node is.
    async_pipe<Callback>* pipe;
    tile output_tile;

    inputs_list_t inputs;

    // Destructor deletes read_pipes, children, and readers, but not output.
    merger_tree_node(size_t tile_size, size_t num_tiles)
      : running(), lock(),
        final_output(NULL), pipe(new async_pipe<Callback>(tile_size, num_tiles)),
        output_tile(),
        inputs()
    {
    }
    merger_tree_node(write_pipe* final_output)
      : running(), lock(),
        final_output(final_output), pipe(NULL),
        output_tile(),
        inputs()
    {
    }
    bool is_intermediate_level()
    {
      return ((!inputs.empty()) && inputs.front().child);
    }
    bool is_leaf_level()
    {
      return ! is_intermediate_level();
    }
    ~merger_tree_node()
    {
      if( MERGER_NET_DEBUG ) printf("Deleting tree node %p\n", this);
      for( inputs_iter_t it = inputs.begin();
           it != inputs.end();
           ++it ) {
        input_info* in = &(*it);
        //delete in->leaf_pipe; Don't delete input pipes.
        delete in->child;
        // !! warning - may not return tile t..
        if( in->tp.t.has_tile() ) {
          warn_if_err(ERR_INVALID_STR("tile not being returned"));
        }
      }
      delete pipe;
    }
  };


  struct Callback
  {
    merger_network* net;
    merger_tree_node* node;

    // Empty callback is a placeholder for EOF..
    Callback()
      : net(NULL), node(NULL)
    {
    }
    Callback(merger_network* net, merger_tree_node* node)
      : net(net), node(node)
    {
    }
    bool is_end() { return net==NULL; }
    // Wait on the i'th input, which is pointed to by info.
    // Assumes that the node read lock is held.
    void request_input(inputs_iter_t it, bool all)
    {
      input_info* info = &(*it);
      if( all ) {
        node->state = STATE_WAIT_ALL_INPUTS;
        if( MERGER_NET_DEBUG ) {
          printf("%p - requesting input (all) %p\n", node, info);
        }
      } else {
        node->state = STATE_WAIT_INPUT;
        if( MERGER_NET_DEBUG ) {
          printf("%p - requesting input %p\n", node, info);
        }
      }
      memset(&info->tp, 0, sizeof(tile_pos));
      node->cur_input = it;
      if( info->leaf_pipe ) {
        tile t = info->leaf_pipe->get_full_tile();
        (*this)(t); // call our "do the next thing" routine
      } else {
        { pthread_held_read_lock rl(&info->child->lock);
          info->child->pipe->get_full_tile(*this);
        }
      }
    }
    void put_input(inputs_iter_t it, tile t)
    {
      input_info* info = &(*it);
      if( MERGER_NET_DEBUG ) {
        printf("%p - returning input %p\n", node, info);
      }
      if( info->leaf_pipe ) {
        info->leaf_pipe->put_empty_tile(t);
      } else {
        { pthread_held_read_lock rl(&info->child->lock);
          info->child->pipe->put_empty_tile(t);
        }
      }
    }
    void close_input(inputs_iter_t it)
    {
      input_info* info = &(*it);
      if( MERGER_NET_DEBUG ) {
        printf("%p - closing input %p\n", node, info);
      }
      if( info->leaf_pipe ) {
        info->leaf_pipe->close_empty();
      } else {
        { pthread_held_read_lock rl(&info->child->lock);
          info->child->pipe->close_empty();
        }
      }
    }
    // Assumes that the node read lock is held.
    void request_output()
    {
      node->state = STATE_WAIT_OUTPUT;
      memset(&node->output_tile, 0, sizeof(tile));

      if( MERGER_NET_DEBUG ) {
        printf("%p - requesting output\n", node);
      }
      // Ask for the output tile.
      if( node->pipe ) {
        node->pipe->get_empty_tile(*this);
      } else {
        tile t = node->final_output->get_empty_tile();
        (*this)(t); // call our "do the next thing" routine
      }
    }
    void close_output()
    {
      if( MERGER_NET_DEBUG ) {
        printf("%p - closing output\n", node);
      }
      if( node->pipe ) {
        node->pipe->close_full();
      } else {
        node->final_output->close_full();
      }
    }
    void put_output(tile t)
    {
      if( MERGER_NET_DEBUG ) {
        printf("%p - returning output %p\n", node, t.data);
      }
      if( node->pipe ) {
        node->pipe->put_full_tile(t);
      } else {
        node->final_output->put_full_tile(t);
      }
    }
 
    void operator()(tile t) {
      { pthread_held_read_lock rl(&node->lock); // take the read lock.
        // Depending on the state we are in... act accordingly.
        if ( node->state == STATE_WAIT_ALL_INPUTS ) {
          inputs_iter_t it = node->cur_input;
          input_info* info = &(*it);
          if( MERGER_NET_DEBUG ) {
            printf("%p - received input   (all) %p %p\n", node, info, t.data);
          }
          // We just got an input tile!
          info->tp.t = t;
          info->tp.pos = 0;
          // Do we need to wait for the next input?
          ++it;
          if( it != node->inputs.end() ) {
            info = &(*it);
            // Ask for the next input tile.
            request_input(it, true);
            return;
          }
          // Otherwise, wait for the output.
          request_output();
          return;
        }
        if ( node->state == STATE_WAIT_INPUT ) {
          inputs_iter_t it = node->cur_input;
          input_info* info = &(*it);
          if( MERGER_NET_DEBUG ) {
            printf("%p - received input %p %p\n", node, info, t.data);
          }
          // We just got an input tile!
          info->tp.t = t;
          info->tp.pos = 0;
          // Add this task to the queue
          node->state=STATE_RUNNABLE;
          if( MERGER_NET_DEBUG ) {
            printf("%p - adding to queue\n", node);
          }
          net->tasks.push(*this);
          return;
        }
        if( node->state == STATE_WAIT_OUTPUT ) {
          if( MERGER_NET_DEBUG ) {
            printf("%p - received output %p\n", node, t.data);
          }
          // we just got an output tile!
          assert(t.len == 0); // length should be zero!
          node->output_tile = t;
          node->state=STATE_RUNNABLE;
          if( MERGER_NET_DEBUG ) {
            printf("%p - adding to queue\n", node);
          }
          // Add this task to the queue
          net->tasks.push(*this);
          return;
        }
      } // destructor releases read lock.
      assert(0); // should never get here. Bad state!
    }

    void run(size_t worker_num) {
      bool to_delete = false;
      { pthread_held_read_lock rl(&node->lock); // take the read lock.
        if( MERGER_NET_DEBUG ) {
          printf("Worker %zi - node %p - state %i\n", worker_num, node, node->state);
        }
        assert(node->state == STATE_STARTING ||
               node->state == STATE_TO_DELETE ||
               node->state == STATE_RUNNABLE);
        if( node->state == STATE_STARTING ) {
          if( node->inputs.empty() ) {
            // If no inputs, we just go to .. STATE_RUNNABLE
            node->state = STATE_RUNNABLE;
          } else {
            // Wait for the first input.
            request_input(node->inputs.begin(), true);
            return;
          }
        }
        if( node->state == STATE_TO_DELETE ) {
          to_delete = true;
        }
        if( node->state == STATE_RUNNABLE ) {
          // Do some merging.
          // First things first, check for EOF in the tiles. Any tile
          // that is at EOF will be removed.
          for(inputs_iter_t it=node->inputs.begin();
              it != node->inputs.end();
              ) {
            input_info *info = &(*it);
            if( info->tp.t.is_end() ) {
              // End of file. Remove this input from the list.
              input_info to_delete = *info;
              if( MERGER_NET_DEBUG ) {
                printf("Worker %zi - node %p - deleting empty input %p\n", worker_num, node, info);
              }
              // Close that input..
              close_input(it);
              it = node->inputs.erase(it);
              //if( to_delete.leaf_pipe ) delete to_delete.leaf_pipe;
              if( to_delete.child ) {
                merger_tree_node* child = to_delete.child;
                // It's at EOF. Tell the node it's OK to delete.
                { pthread_held_write_lock wr(&child->lock);
                  assert(child->state == STATE_AT_EOF);
                  assert(child->inputs.empty());
                  child->state = STATE_TO_DELETE;
                  // Add this task to the queue
                  net->tasks.push(Callback(net,child));
                } // lock is released
              }
            } else {
              if( MERGER_NET_DEBUG ) {
                printf("Worker %zi - node %p - will use input %p (%p)\n", worker_num, node, info, info->child);
              }
              // Do nothing. We'll merge with it..
              ++it;
            }
          }

          if( ! node->inputs.empty() ) {
            inputs_iter_t it = merger<Record,Criterion>::do_merge
              (net->ctx, &node->inputs, &node->output_tile);
            if( it == node->inputs.end() ) {
              // Put the full output tile.
              // We do this in the following order:
              // save the tile we're returning
              // put the tile we're returning
              // request a new tile (zeroing node->output_tile)
              // This order is necessary since request_input could
              //   delete this node if it was finished. 
              tile t = node->output_tile;
              put_output(t);
              request_output();
              return;
            } else {
              // We do this in the following order:
              // save the tile we're returning
              // put the tile we're returning
              // request a new tile (zeroing info->tp)
              // This order is necessary since request_input could
              //   delete this node if it was finished.
              input_info* info = &(*it);
              tile t = info->tp.t;
              put_input(it, t);
              request_input(it, false);
              return;
            }
          } else {
            // There are no more inputs. We don't need to schedule anything.
            // Close the output, but don't do anything else.
            // We'll wait for our parent to remove us from their list
            // before we clear out everything.
            // Note that this does NOT cause the end-of-file node
            // to be added once again to the task queue!
            tile t = node->output_tile;
            put_output(t);
            close_output();
            node->state = STATE_AT_EOF;
            if( MERGER_NET_DEBUG ) {
              printf("Worker %zi - node %p - marking EOF\n", worker_num, node);
            }
            // If this is the root node that's done, we'll close the 
            // task queue.
            { pthread_held_read_lock rl(&net->root_lock);

              if( net->root == node ) {
                net->tasks.close();
              }
            }
            return;
          }
        }
      } // destructor releases read lock.
      if( to_delete ) {
        // Since it's been removed by the parent, and only exists
        // in the task queue, which we got, we have exclusive access to it.
        delete node;
        return;
      }
      assert(0); // bad state? - should never get here
    }
  };

  class merger_network_thread : public pipeline_node
  {
    merger_network* net;
    long num;
    void run()
    {
      net->run(num);
    }
   public:
    merger_network_thread(merger_network* net, int num) : net(net), num(num) { }
  };

 public:
  Criterion ctx;
 private:
  /* The root of the tree. */
  /* Locks are read-write locks.
   * Going through a pointer, we have to (at least) read-lock it;
   * we may write-lock it. So parent locks must be locked first.
   * root_lock must be locked by anyone using the root.
   */
  pthread_rwlock root_lock; // protects access to root.
  merger_tree_node* root;
  pthread_mutex vars_lock; // may not wait on another lock after 
                           // locking vars_lock; it's just to protect
                           // these little vars.
  size_t num_input_pipes;
  size_t num_mergers;
  pthread_cond wait_new_pipes; // waits on vars_lock..
  bool closed;
  std::vector<merger_network_thread*> threads;
  size_t tile_size;

  /* We run the merger network by completing the tasks in this queue.
     Each task will generally add a new task as part of running - once
     we have waited for more data from each pipe.

     When running a task, we get the node's read lock. Since each node has
     only one entry in the queue, we know that we're the only thread
     "working on" this node. Other nodes might:
       - lock the write lock while modifying the node (adding a pipe), or
       - lock the read lock to get child->pipe.
   */

  task_queue<Callback> tasks;

  static const size_t max_k = K_WAY;
  static const size_t num_tiles = 3;

 public:

  merger_network(Criterion ctx, write_pipe* output)
    : ctx(ctx), root_lock(), root(NULL), vars_lock(), 
      num_input_pipes(0), num_mergers(0),
      wait_new_pipes(),
      closed(false), threads(), tile_size(file_pipe_context::default_tile_size),
      tasks()
  {
    root = new merger_tree_node(output);
    num_mergers++;
    root->state = STATE_STARTING;
    tasks.push(Callback(this,root));
  }

  ~merger_network()
  {
    merger_tree_node* tmp = NULL;
    { pthread_held_write_lock wr(&root_lock);
      pthread_held_write_lock wr2(&root->lock);
      pthread_held_mutex sl(&vars_lock);
      tmp = root;
      root = NULL;
      num_mergers--;
    }
    delete tmp; // root->lock is not held.
  }

 private:
  /* The tree of mergers is a balanced tree where "leaves" - 
   * which are the nodes containing read pipes from the input - are at
   * the bottom level of the tree.
   */

  /*  So adding to the tree works as follows:
   *  1) Identify the lower-right-most node in the tree
   *  2) Conceptually add another child to it. If it "overflows",
   *      we'll split the node into two parts - the left part contains
   *      what was there before we added to the node, and the right part 
   *      contains our new branch. 
   *  3) Go back up the tree to the root resolving the "overflows".
   */
  // Add another pipe to the tree at this node, or if there are too
  // many pipes for this node, creates a new node containing the pipe
  // and returns it for the parent to add as a sibling to this node.
  // Assumes that node does not have any write tiles out from the pipe
  //  to the parent.
  merger_tree_node* add_pipe(merger_tree_node* node, read_pipe* pipe)
  {
    merger_tree_node* new_node=NULL;

    { pthread_held_write_lock wl(&node->lock);

      if( node->is_intermediate_level() ) {
        input_info& last_info = node->inputs.back();
        merger_tree_node* child = last_info.child;
        // Go down to "leaf-level"
        new_node = add_pipe(child, pipe);
      } else {
        // We're at "leaf-level"
        // Add the pipe, or return a new node containing the pipe.
        if( node->inputs.size() < max_k ) {
          // Just add another pipe to this node.
          node->inputs.push_back(input_info(pipe));
          return NULL; // We added 
        } else {
          // Add another child node with the new pipe and return that.
          merger_tree_node* t = new merger_tree_node(tile_size, num_tiles);
          { pthread_held_mutex hl(&vars_lock);
            num_mergers++;
          }
          t->inputs.push_back(input_info(pipe));
          t->state = STATE_STARTING;
          tasks.push(Callback(this,t));
          return t; // We added a new sibling for our parent.
        }
      }

      // On the way back up, add the new child if possible.
      // Note that we need to add the write_pipe for the child.
      if( new_node ) {
        // If the recursive call needs us to add a sibling, do it.
        if( node->inputs.size() < max_k ) {
          // Add an input from the new child node.
          node->inputs.push_back(input_info(new_node));
          return NULL; // don't need to add anything to parent.
        } else {
          // Create yet another new node!
          merger_tree_node* t = new merger_tree_node(tile_size, num_tiles);
          { pthread_held_mutex hl(&vars_lock);
            num_mergers++;
          }
          t->inputs.push_back(input_info(new_node));
          t->state = STATE_STARTING;
          tasks.push(Callback(this,t));
          return t;
        }
      }
      return NULL;
    }
  }

 public:
  // The pipe will be deleted once the merger is finished with it.
  // This call is not reentrant; it should be called by only 1 thread.
  void add_read_pipe(read_pipe* pipe)
  {
    { pthread_held_write_lock wr(&root_lock);
      { pthread_held_mutex sl(&vars_lock);
        if( tasks.is_closed() ) {
          // If the root was the only one there, we'll reopen the tasks
          // queue.
          tasks.reopen();
        }
      }
      // Find where to add.
      merger_tree_node* new_node=add_pipe(root, pipe);
      { pthread_held_mutex sl(&vars_lock);
        if( new_node ) {
          // Create a new root containing the old root and new_node.
          // Create the buffered_pipe for new_node.
          async_pipe<Callback>* tmp_pipe = new async_pipe<Callback>(tile_size, num_tiles);
          write_pipe* final_output = root->final_output;
          assert(final_output);
          num_mergers++;
          // new_root will write to final_output
          merger_tree_node* new_root = new merger_tree_node(final_output);
          // old root will write to the tmp_pipe
          root->pipe = tmp_pipe;
          root->final_output = NULL;
          // new_root will contain root and read from tmp_pipe.
          new_root->inputs.push_back(input_info(root));
          // new_root will contain new_node as well
          new_root->inputs.push_back(input_info(new_node));
          // Now new_root is the new root!
          root = new_root;
          // Add the new node to our queue.
          new_root->state = STATE_STARTING;
          tasks.push(Callback(this,new_root));
        }
        num_input_pipes++;
        // Signal that we got a new pipe.
        wait_new_pipes.broadcast();
      } // destructor releases top_lock
    }
  }

 public:
  void run(size_t worker_num)
  {
    while( 1 ) {
      while( 1 ) {
        Callback cb = tasks.pop();
        if( cb.is_end() ) break;
        // Run that task!
        cb.run(worker_num);
      }

      // Wait for it to be closed.
      { pthread_held_mutex sl(&vars_lock);
        if( closed ) break;
        else wait_new_pipes.wait(&vars_lock);
      }
    }
  }

  bool is_closed()
  {
    { pthread_held_mutex sl(&vars_lock);
      return closed;
    }
  }

  size_t get_num_mergers()
  {
    { pthread_held_mutex sl(&vars_lock);
      return num_mergers;
    }
  }
  // Mark the fact that we won't get any new "add_read_pipe" calls
  // and so we can quit.
  void close()
  {
    { pthread_held_mutex sl(&vars_lock);
      closed = true;
      wait_new_pipes.broadcast();
    }
  }

  // Returns the number of leaf read pipes in this thing...
  size_t size() 
  {
    size_t ret;
    { pthread_held_mutex sl(&vars_lock);
      ret = num_input_pipes;
    }
    return ret;
  }

  void start_threads(size_t num_procs)
  {
    { pthread_held_mutex sl(&vars_lock);
      // Now create that many threads.
      assert(num_procs>0);
      if( MERGER_NET_DEBUG ) {
        std::cout << "Starting " << num_procs << " merger threads" << std::endl;
      }
      for( size_t i = 0; i < num_procs; i++ ) {
        merger_network_thread* t = new merger_network_thread(this, i);
        threads.push_back(t);
        t->start();
      }
    }
  }
  void finish_threads()
  {
    // Wait for all the threads to finish.
    for(size_t i = 0; i < threads.size(); i++ ) {
      threads[i]->finish();
      delete threads[i];
    }
    { pthread_held_mutex sl(&vars_lock);
      threads.clear();
    }
  }
};

/* This will close but not free the input pipes! and the output pipe!
 * */
template< typename Record,
          typename Criterion >
void multiway_merge(Criterion ctx, std::vector<read_pipe*>* input, write_pipe* output, size_t num_procs)
{
#ifdef USE_HEAP_ALWAYS
  heap_merger<Record,Criterion> heap(ctx, num_procs);
  pipe_back_inserter<Record> writer(output);
  for(size_t i = 0; i < input->size(); i++ ) {
    heap.add_read_pipe((*input)[i]);
  }
  heap.start();
  while( ! heap.empty() ) {
    const Record& r = heap.top();
    writer.push_back(r);
    heap.pop();
  }
  writer.finish(); // should close the output pipe!
  heap.finish();
#else
  merger_network<Record,Criterion> net(ctx, output);
  for(size_t i = 0; i < input->size(); i++ ) {
    net.add_read_pipe((*input)[i]);
  }
  net.close(); // no new pipes!
  net.start_threads(num_procs);
  // Wait for the merger network to finish!
  net.finish_threads();
  // Output pipe should be closed by merger network!
#endif
}

#define pipe_merger heap_merger


#endif
