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

  femto/src/utils_cc/distributor_node.hh
*/
#ifndef _DISTRIBUTOR_NODE_HH_
#define _DISTRIBUTOR_NODE_HH_

#include <cstdlib>
#include <cassert>
#include <algorithm> // std::sort
#include <limits> // lower_bound

extern "C" {
#include "processors.h"
#include "util.h"
}

#include "pipelining.hh"
#include "distribute_utils.hh"
#include "file_pipe.hh"
#include "bucket_sort.hh"
#include "sort_basic.hh" // general_sort, TotalMinMaxCount.
#include "utils.hh" // file_len

#define DEBUG_DISTRIBUTOR 0
#define USE_SORT_MERGE 1
//#define USE_PARALLEL_BSEARCH 0
#define SPLIT_BUFFER_SIZE 11


template <typename SplitterRecord,
          typename SplitterCriterion>
struct FixedSplitters {
  typedef SplitterRecord splitter_record_t;
  typedef SplitterCriterion splitter_criterion_t;
  typedef KeyCritTraits<SplitterRecord,SplitterCriterion> splitter_kc_traits_t;
  typedef typename splitter_kc_traits_t::key_t splitter_key_t;
  typedef typename splitter_kc_traits_t::ComparisonCriterion splitter_comp_t;

  SplitterCriterion crit;
  ssize_t num_bins;
  ssize_t target_bin;
  splitter_key_t min_all;
  splitter_key_t max_all;

  FixedSplitters(SplitterCriterion crit, ssize_t num_bins, ssize_t target_bin)
    : crit(crit), num_bins(num_bins), target_bin(target_bin)
  {
  }
 
  void set_target(ssize_t target_bin_in)
  {
    target_bin = target_bin_in;
  }

  void use_sample_minmax(splitter_key_t min, splitter_key_t max)
  {
    min_all = min;
    max_all = max;
  }

  template<typename RecordIterator>
  void use_sample(splitter_key_t min, splitter_key_t max, RecordIterator data, size_t num_records)
  {
    use_sample_minmax(min, max);
  }

  ssize_t get_bin(const splitter_key_t& key) const
  {
     if( EXTRA_CHECKS ) {
      splitter_comp_t comp(crit);
      assert( comp.compare( key, min_all ) >= 0 );
      assert( comp.compare( key, max_all ) <= 0 );
    }

    return target_bin;
  }
 
  template<typename Record,
           typename RecordIterator,
           typename Translator>
  void write_bins(RecordIterator start,
                  RecordIterator end,
                  pipe_back_inserter<Record>** restrict writers,
                  TotalMinMaxCountByBin<SplitterRecord,SplitterCriterion>* tot,
                  Translator trans)
  {
    for( RecordIterator cur = start; cur != end; ++cur ) {
      Record r = *cur;
      splitter_key_t key = trans.record_to_splitter_key(crit, r);
      ssize_t bin = get_bin(key);
      tot->update(bin, key);
      writers[bin]->push_back(r);
    }
  }

  splitter_key_t get_min(ssize_t bin) const
  {
    return min_all;
  }
 
  splitter_key_t get_max(ssize_t bin) const
  {
    return max_all;
  }
};

template <typename SplitterRecord,
          typename SplitterCriterion>
struct ZeroSplitters : public FixedSplitters<SplitterRecord,SplitterCriterion> {
  ZeroSplitters(SplitterCriterion crit, ssize_t num_bins)
    : FixedSplitters<SplitterRecord,SplitterCriterion>(crit,num_bins,0)
  {
  }
};


template <typename SplitterRecord,
          typename SplitterCriterion>
struct DividingSplitters {
  typedef SplitterRecord splitter_record_t;
  typedef SplitterCriterion splitter_criterion_t;
  typedef KeyCritTraits<SplitterRecord,SplitterCriterion> splitter_kc_traits_t;
  typedef typename splitter_kc_traits_t::key_t splitter_key_t;
  typedef typename splitter_kc_traits_t::ComparisonCriterion splitter_comp_t;
  typedef DistributeUtils<SplitterRecord,SplitterCriterion> DU;
  typedef typename DU::shift_t shift_t;

  SplitterCriterion crit;
  ssize_t num_bins;;
  uint64_t min_all;
  uint64_t max_all;
  uint64_t num_per_bin;
  uint64_t overlap;
  uint64_recip_t inv_num_per_bin;

  DividingSplitters(SplitterCriterion crit, ssize_t num_bins)
    : crit(crit), num_bins(num_bins)
  {
    memset(&inv_num_per_bin, 0, sizeof(uint64_recip_t));
  }
 
  void set_target(ssize_t target_bin_in)
  {
    assert(0); // only used in fixed splitters.
  }


  void use_sample_overlap(uint64_t _num_per_bin, uint64_t _overlap)
  {
    num_per_bin = _num_per_bin;
    overlap = _overlap;

    inv_num_per_bin = compute_reciprocal( num_per_bin );

    min_all = 0;
    max_all = _num_per_bin * num_bins - 1;
  }
  
  void use_sample_minmax(splitter_key_t min, splitter_key_t max)
  {
    uint64_t min_int, max_int;
    min_int = min;
    max_int = max;
    uint64_t n_per_bin = 0;

    n_per_bin = (max_int + 1 - min_int + num_bins - 1) / num_bins;
    if( n_per_bin == 0 ) n_per_bin = 1; // avoid divide by 0.
    use_sample_overlap(n_per_bin, 0);

    min_all = min_int;
    max_all = max_int;
  }

  template<typename RecordIterator>
  void use_sample(splitter_key_t min, splitter_key_t max, RecordIterator data, size_t num_records)
  {
    use_sample_minmax(min, max);
  }

  void get_bins(const splitter_key_t& key, ssize_t& bin, ssize_t& alt_bin) const
  {
    uint64_t key_int = key - min_all;
    // bin = key / num_per_bin;
    uint64_t bin_int = reciprocal_divide(key_int, inv_num_per_bin);
    uint64_t bin_start = bin_int * num_per_bin;
    if( EXTRA_CHECKS ) {
      assert( bin_int == key_int / num_per_bin );
      assert( bin_int < (uint64_t) num_bins );
      assert( key >= min_all );
      assert( key <= max_all );
    }

    bin = alt_bin = bin_int;

    if( overlap > 0 ) {
      if( bin_int > 0 && key_int < bin_start + overlap ) {
        alt_bin = bin_int - 1;
      }
    }
  }
 
  template<typename Record,
           typename RecordIterator,
           typename Translator>
  void write_bins(RecordIterator start,
                  RecordIterator end,
                  pipe_back_inserter<Record>** restrict writers,
                  TotalMinMaxCountByBin<SplitterRecord,SplitterCriterion>* tot,
                  Translator trans)
  {
    for( RecordIterator cur = start; cur != end; ++cur ) {
      Record r = *cur;
      splitter_key_t key = trans.record_to_splitter_key(crit, r);
      ssize_t bin, alt_bin;
      get_bins(key, bin, alt_bin);
      // Add it to the main bin.
      writers[bin]->push_back(r);
      // Update totals when adding to main bin.
      tot->update(bin, key);

      if( overlap > 0 && bin != alt_bin ) {
        // Add it to the alternate bin
        // Do not update totals
        writers[alt_bin]->push_back(r);
      }
    }
  }

  splitter_key_t get_min(ssize_t bin) const
  {
    return min_all + num_per_bin * bin;
  }
 
  splitter_key_t get_max(ssize_t bin) const
  {
    uint64_t ret = min_all + (num_per_bin * (bin+1)) - 1;
    if( ret > max_all ) ret = max_all;
    return ret;
  }

};

template <typename SplitterRecord,
          typename SplitterCriterion>
struct SampleSplitters {
  typedef SplitterRecord splitter_record_t;
  typedef SplitterCriterion splitter_criterion_t;
  typedef KeyCritTraits<SplitterRecord,SplitterCriterion> splitter_kc_traits_t;
  typedef typename splitter_kc_traits_t::key_t splitter_key_t;
  typedef typename splitter_kc_traits_t::ComparisonCriterion splitter_comp_t;

  typedef std::vector<splitter_key_t> vector_t;

  SplitterCriterion crit;
  splitter_comp_t comp;
  ssize_t num_bins;
  vector_t splitters;
  splitter_key_t min_all;
  splitter_key_t max_all;
 
  SampleSplitters(SplitterCriterion crit, ssize_t num_bins)
    : crit(crit), comp(crit), num_bins(num_bins)
  {
    assert(num_bins>0);
  }

  // Constructor for copying the splitters vector.
  SampleSplitters(SplitterCriterion crit, splitter_key_t min, splitter_key_t max, const vector_t & splitters)
    : crit(crit), comp(crit), splitters(splitters), min_all(min), max_all(max)
  {
    num_bins = splitters.size() + 1;

    // Check that the splitters are sorted!
    for( size_t i = 1; i < splitters.size(); i++ ) {
        splitter_key_t data_key = splitter_kc_traits_t::get_key(crit, splitters[i]);
        splitter_key_t last_key = splitter_kc_traits_t::get_key(crit, splitters[i-1]);
        assert( comp.compare( data_key, last_key ) >= 0 ); // data[i] >= last
    }
  }
 
  void set_target(ssize_t target_bin_in)
  {
    assert(0); // only used in fixed splitters.
  }

  template<typename RecordIterator>
  void use_sample(splitter_key_t min, splitter_key_t max, const RecordIterator data, size_t num_records)
  {
    min_all = min;
    max_all = max;
 
    /*if( DEBUG_DISTRIBUTOR > 10 ) {
      for( size_t i = 0; i < num_records; i++ ) {
      std::cout << "sampled " << i << ": " << data[i].to_string() << std::endl;
      }
      }*/
 
    GeneralSorter<SplitterRecord,SplitterCriterion>::check_sorted(
        crit, min, max, data, num_records);

    splitters.clear();
 
    {
      ssize_t num = num_records;
      ssize_t i = 0;

      for( ssize_t split_idx = 0; split_idx < num_bins; split_idx++ ) {
        ssize_t target = (split_idx+1)*num/num_bins;
        if( target < i ) target = i; // make sure targets are increasing...

        // Read until the next target.
        i = target;

        if( i >= num ) break;

        // Use this one as a splitter.
        splitters.push_back(splitter_kc_traits_t::get_key(crit, *(data+i)));
        i++;
      }
    }
 
    assert(splitters.size() == (size_t) num_bins || splitters.size() == (size_t) (num_bins-1) || num_records < (size_t) num_bins);
 
    // Remove duplicate splitters.
    {
      size_t j = 0;
      for( size_t i = 0; i < splitters.size(); i++ ) {
        if( j == 0 || 0 != comp.compare(splitters[j-1],splitters[i]) ) {
          // Not a duplicate, or 1st record.
          splitters[j++] = splitters[i];
        }
      }
      splitters.resize(j);
    }

    // if we have too many splitters, remove the last splitter
    // because we need to have n_bins-1
    // splitters since that will create data in n_bins bins.
    if( splitters.size() == (size_t) num_bins ) {
      //splitters.erase(splitters.begin());
      splitters.pop_back();
    }
 
    assert(splitters.size() < (size_t) num_bins);
 
    // Print out the splitters if debug is sufficient.
    /*if( DEBUG_DISTRIBUTOR > 2 ) {
      for( size_t i = 0; i < splitters.size(); i++ ) {
      std::cout << "splitter " << i << ": " << splitters[i].to_string() << std::endl;
      }
      }*/

    num_bins = splitters.size() + 1;

    // Check that the bin for a splitter is always
    // i+1.
    for( size_t i = 0; i < splitters.size(); i++ ) {
      ssize_t bin = get_bin(splitters[i]);
      assert( bin == (ssize_t) i + 1 );
    }
  }
 
  template<typename RecordIterator>
  void sort_then_use_sample(splitter_key_t min, splitter_key_t max, RecordIterator data, size_t num_records)
  {
    GeneralSorter<SplitterRecord,SplitterCriterion>::general_sort(crit, min, max, data, num_records,NULL);
    use_sample(min, max, data, num_records);
  }

  void use_sample_fctx(splitter_key_t min, splitter_key_t max, file_pipe_context fctx, size_t max_to_read)
  {
    int fd;

    fctx.open_file_if_needed();

    fd = fctx.fd;

    size_t num_to_read = max_to_read;
    size_t file_length = file_len(fd);
    if( num_to_read > file_length ) num_to_read = file_length;

    FileMMap memory;
    Pages pgs = get_pages_for_records(get_file_page_size(fd),0,num_to_read,RecordTraits<SplitterRecord>::record_size);
    memory.map( NULL, pgs.outer_length, PROT_READ|PROT_WRITE, MAP_SHARED, fd, pgs.outer_start);
    void* ptr = memory.data;
    ptr = PTR_ADD(ptr,pgs.start - pgs.outer_start);
    typename RecordTraits<SplitterRecord>::iterator_t data = 
                                  RecordTraits<SplitterRecord>::getiter(ptr);

    memory.advise_willneed();

    sort_then_use_sample(min, max, data, num_to_read);

    memory.unmap();

    fctx.close_file_if_needed();
  }

  ssize_t get_bin(const splitter_key_t & key) const
  {
    const splitter_key_t* it;
    const splitter_key_t* begin;
    const splitter_key_t* end;
 
    ssize_t ret;

    if( splitters.size() > 0 ) {
      // Choose the bin based on which value in the splitters we get.
      // lower_bound returns first position where *pos >= r
      // upper_bound returns first position where *pos > r
      //it = lower_bound(splitters.begin(), splitters.end(), r, comp);
      begin = &splitters[0];
      end = begin + splitters.size();
      it = std::upper_bound(begin, end, key, comp);
      ret = it - begin;
      if( EXTRA_CHECKS ) {
        assert( ret >= 0 );
        assert( ret < (ssize_t) num_bins );

        assert( comp.compare( key, min_all ) >= 0 );
        assert( comp.compare( key, max_all ) <= 0 );
      }
    } else {
      ret = 0;
    }

    return ret;
  }

  template<typename Record,
           typename RecordIterator,
           typename Translator>
  void write_bins(RecordIterator start,
                  RecordIterator end,
                  pipe_back_inserter<Record>** restrict writers,
                  TotalMinMaxCountByBin<SplitterRecord,SplitterCriterion>* tot,
                  Translator trans)
  {
    assert((size_t) num_bins == splitters.size() + 1 );

    if( USE_SORT_MERGE ) {
      // Sort the input tile
      // IMPORTANT: we sort by the splitter criterion, since we need to merge with the splitter records
      GeneralSorter<Record,SplitterCriterion>::general_sort(crit, min_all, max_all, start, end-start, NULL);

      // Merge it with the splitters.
      ssize_t bin = 0;
      RecordIterator cur = start;
      Record r = *cur;
      splitter_key_t key = trans.record_to_splitter_key(crit, r);
      splitter_key_t splitter_key = splitters[0];

      while( cur != end ) {
        // compare the record against the bin splitter.
        int cmp = comp.compare(key, splitter_key);
        if( cmp < 0 ) {
          if( EXTRA_CHECKS ) {
            ssize_t bin_chk = get_bin(key);
            assert( bin_chk == bin );
          }
          // Output 'key'
          tot->update(bin, key);
          writers[bin]->push_back(r);
          ++cur;
          if( cur != end ) {
            r = *cur;
            key = trans.record_to_splitter_key(crit, r);
          }
        } else {
          // Output 'splitter'
          bin++;
          if( bin == (ssize_t) splitters.size() ) break;
          splitter_key = splitters[bin];
        }
      }

      bin = num_bins - 1;
      // If we have any records left, they go in the final bin.
      while( cur != end ) {
        if( EXTRA_CHECKS ) {
          ssize_t bin_chk = get_bin(key);
          assert( bin_chk == bin );
        }
        tot->update(bin, key);
        writers[bin]->push_back(r);
        ++cur;
        if( cur != end ) {
          r = *cur;
          key = trans.record_to_splitter_key(crit, r);
        }
      }

    /*} else if( USE_PARALLEL_BSEARCH ) {
      int num_records = end - start;
      int a[num_records];
      int b[num_records];
      key_t keys[num_records];
      int i;

      i=0;
      for( RecordIterator cur = start; cur != end; ++cur ) {
        Record r = *cur;
        keys[i] = kc_traits_t::get_key(crit, r);
        totals->update(keys[i]);
        ++i;
      }
      parallel_binary_search<Record,Criterion,key_t>
          (crit, splitters.size(), &splitters[0],
                             num_records, keys,
                             a, b);

      i=0;
      for( RecordIterator cur = start; cur != end; ++cur ) {
        Record r = *cur;
        writers[b[i]]->push_back(r);
        ++i;
      }
      */
    } else {
      for( RecordIterator cur = start; cur != end; ++cur ) {
        Record r = *cur;
        splitter_key_t key = trans.record_to_splitter_key(crit, r);
        ssize_t bin = get_bin(key);
        tot->update(bin, key);
        writers[bin]->push_back(r);
      }
    }
  }
 
  splitter_key_t get_min(ssize_t bin) const
  {
    if( bin == 0 ) return min_all;
    return splitters[bin-1];
  }
 
  splitter_key_t get_max(ssize_t bin) const
  {
    if( bin == num_bins - 1 ) return max_all;
    return splitters[bin];
  }
};

template < typename Record,
           typename Criterion,
           typename Splitters,
           typename Translator = IdentitySplitterTranslator<Record,Criterion,typename Splitters::splitter_record_t,typename Splitters::splitter_criterion_t> >
class distributor_node: public pipeline_node
{
  public:
    typedef Record record_t;
    typedef Criterion criterion_t;
    typedef Splitters splitters_t;
    typedef Translator translator_t;
    typedef KeyCritTraits<Record,Criterion> kc_traits_t;
    typedef typename kc_traits_t::key_t key_t;
    typedef typename Splitters::splitter_record_t splitter_record_t;
    typedef typename Splitters::splitter_criterion_t splitter_criterion_t;
    typedef typename Splitters::splitter_kc_traits_t splitter_kc_traits_t;
    typedef typename splitter_kc_traits_t::key_t splitter_key_t;
    typedef typename RecordTraits<Record>::iterator_t iterator_t;
  private:
    Criterion crit;
    Translator trans;
    read_pipe *const input;
    std::vector<write_pipe*> *output;
    Splitters *splitters;
    bool dont_close;
  public:
    TotalMinMaxCountByBin<splitter_record_t,splitter_criterion_t> totals;
  private:

    virtual void run()
    {
      std::vector <pipe_back_inserter<Record>* > writers;

      try {

        writers.reserve(output->size());

        // Set up the output writers.
        for (size_t i = 0; i < output->size(); i++) {
          writers.push_back(new pipe_back_inserter<Record>((*output)[i],dont_close));  
        }


        if( splitters->num_bins == 1 ) {
          pipe_iterator<Record> read(input,dont_close);
          pipe_iterator<Record> end;
          // Now go through the input.
          while( read != end ) {
            Record r = *read;
            splitter_key_t key = trans.record_to_splitter_key(splitters->crit, r);
            totals.update(0, key);
            writers[0]->push_back(r);
            ++read;
          }
          read.finish();
        } else {
          size_t tile_size = file_pipe_context::default_tile_size;
          tile_size = round_up_to_multiple(tile_size, RecordTraits<Record>::record_size);

          while( 1 ) {
            tile t = input->get_full_tile();
            if( t.is_end() ) break;

            for( size_t cur = 0; cur < t.len; cur += tile_size )
            {
              size_t len = tile_size;
              if( cur + len > t.len ) len = t.len - cur;
              // This might be an IO tile - go through it in
              // normal tile size chunks.
              iterator_t start;
              iterator_t end;
              start = RecordTraits<Record>::getiter(PTR_ADD(t.data,cur));
              end = RecordTraits<Record>::getiter(PTR_ADD(t.data,cur+len));

              // Distribute the records in the input tile.
              splitters->write_bins(start, end, &writers[0],
                                    &totals, trans);
            }

            input->put_empty_tile(t);
          }
          if( ! dont_close ) {
            input->close_empty();
          }
        }
      } catch( ... ) {
        // Now free output writers.
        for(size_t i = 0; i < output->size(); i++) {
          if( writers[i] ) {
            delete writers[i];
          }
        }

        throw;
      }

      // Now free output writers.
      for(size_t i = 0; i < output->size(); i++) {
        if( writers[i] ) {
          writers[i]->finish();
          delete writers[i];
        }
      }
    }

 public:
  distributor_node(Criterion crit, read_pipe* input, std::vector <write_pipe*>* output, Splitters *splitters, Translator trans, bool dont_close=false)
      : crit(crit), trans(trans), input(input), output(output), splitters(splitters), dont_close(dont_close), totals(splitters->crit,splitters->num_bins) {
    assert((size_t) splitters->num_bins <= output->size());
    // When using SORT/MERGE, we assume that tile size is
    // big enough (so we sort independently each tile)
    // and then merge.
    if( USE_SORT_MERGE ) {
      assert( input->get_tile_size() > (size_t) 2*splitters->num_bins );
    }
  }
};

template < typename Record,
           typename Criterion,
           typename Splitters,
           typename Translator = IdentitySplitterTranslator<Record,Criterion,typename Splitters::splitter_record_t,typename Splitters::splitter_criterion_t> >
class parallel_distributor_node: public pipeline_node
{
  public:
    typedef Record record_t;
    typedef Criterion criterion_t;
    typedef Splitters splitters_t;
    typedef Translator translator_t;
    typedef distributor_node<Record,Criterion,Splitters,Translator> serial_dist_t;
    typedef typename Splitters::splitter_record_t splitter_record_t;
    typedef typename Splitters::splitter_criterion_t splitter_criterion_t;
  private:
    Criterion crit;
    Translator trans;
    read_pipe *const input;
    std::vector<write_pipe*> *output;
    Splitters *splitters;
    long use_procs;
  public:
    TotalMinMaxCountByBin<splitter_record_t,splitter_criterion_t> totals;
  private:

    virtual void run()
    {
      std::vector<serial_dist_t*> nodes;

      try {
        nodes.reserve(use_procs);
        for(long i = 0; i < use_procs; i++ ) {
          nodes.push_back(new serial_dist_t(crit, input, output, splitters, trans, true));

          if( use_procs > 1 ) {
            nodes[i]->start();
          } else {
            nodes[i]->runhere();
          }
        }
      } catch( ... ) {
        // Now free output writers.
        for(size_t i = 0; i < nodes.size(); i++) {
          if( nodes[i] ) {
            if( use_procs > 1 ) nodes[i]->finish();
            delete nodes[i];
          }
        }

        // Close the pipes.
        input->close_empty();
        for(size_t i = 0; i < (*output).size(); i++ ) {
          (*output)[i]->close_full();
        }

        throw;
      }

      // Now free output writers.
      for(size_t i = 0; i < nodes.size(); i++) {
        if( nodes[i] ) {
          if( use_procs > 1 ) nodes[i]->finish();
          totals.update(& nodes[i]->totals);
          delete nodes[i];
        }
      }

      // Close the pipes.
      input->close_empty();
      for(size_t i = 0; i < (*output).size(); i++ ) {
        (*output)[i]->close_full();
      }


    }

 public:
  parallel_distributor_node(Criterion crit, read_pipe* input, std::vector <write_pipe*>* output, Splitters *splitters, Translator trans, long use_procs=0)
      : crit(crit), trans(trans), input(input), output(output), splitters(splitters), use_procs(use_procs), totals(splitters->crit,splitters->num_bins) {
    assert((size_t) splitters->num_bins <= output->size());
    // When using SORT/MERGE, we assume that tile size is
    // big enough (so we sort independently each tile)
    // and then merge.
    if( USE_SORT_MERGE ) {
      assert( input->get_tile_size() > (size_t) 2*splitters->num_bins );
    }

    if( use_procs == 0 ) {
      use_procs = 1;
      error_t err = get_use_processors(&use_procs);
      if( err ) throw error(err);
      assert( use_procs > 0);
    }

    assert(input->get_num_tiles() > (size_t) use_procs);
    assert(input->is_thread_safe());
    for(size_t i = 0; i < output->size(); i++ ) {
      assert((*output)[i]->get_num_tiles() > (size_t) use_procs);
      assert((*output)[i]->is_thread_safe());
    }
  }
};

#endif
