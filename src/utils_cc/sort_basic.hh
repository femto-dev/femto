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

  femto/src/utils_cc/sort_basic.hh
*/
#ifndef _SORT_BASIC_HH_
#define _SORT_BASIC_HH_

#include "distribute_utils.hh"

#include "bucket_sort.hh"

#include <algorithm>

#ifdef HAVE_PARALLEL_ALGORITHM
#include <parallel/algorithm> // sort
#endif

#define MAX_UINT64 ((uint64_t)-1)

template<typename Record,
         typename Criterion,
         typename SplitterRecord=Record,
         typename SplitterCriterion=Criterion>
struct IdentitySplitterTranslator {
  typedef KeyCritTraits<Record,Criterion> kc_traits_t;
  typedef typename kc_traits_t::key_t key_t;
  typedef KeyCritTraits<SplitterRecord,SplitterCriterion> splitter_kc_traits_t;
  typedef typename splitter_kc_traits_t::key_t splitter_key_t;

  // These don't technically have to be static.

  // Used to get min/max in sort second_pass
  static
  key_t splitter_key_to_key(SplitterCriterion crit, const splitter_key_t& in, bool is_for_min)
  {
    key_t ret(in);
    return ret;
  }

  static
  splitter_key_t record_to_splitter_key(SplitterCriterion crit, const Record& r)
  {
    return splitter_kc_traits_t::get_key(crit, r);
  }
};

template <typename Record>
struct MinMaxCount {
  Record min;
  Record max;
  uint64_t count;
};

template <typename Record, typename Criterion>
struct TotalMinMaxCountByBin {
  typedef Record record_t;
  typedef Criterion criterion_t;
  typedef KeyCritTraits<Record,Criterion> kc_traits_t;
  typedef typename kc_traits_t::key_t key_t;
  typedef typename kc_traits_t::ComparisonCriterion comp_t;

  criterion_t crit;
  comp_t comp;
  std::vector< MinMaxCount<key_t> > t;

  TotalMinMaxCountByBin(Criterion crit, ssize_t n_bins)
    : crit(crit), comp(crit), t(n_bins)
  {
    for( ssize_t i = 0; i < n_bins; i++ ) {
      t[i].count = 0;
    }
  }

  void update(ssize_t bin, const key_t& r)
  {
    if( t[bin].count == 0 || comp.compare(r, t[bin].min) < 0 ) {
      t[bin].min = r;
    }
    if( t[bin].count == 0 || comp.compare(r, t[bin].max) > 0 ) {
      t[bin].max = r;
    }

    t[bin].count++;
  }

  void update(const TotalMinMaxCountByBin* other)
  {
    for( size_t i = 0; i < t.size() && i < other->t.size(); i++ ) {
      if( other->t[i].count > 0 ) {
        if( t[i].count == 0 || comp.compare(other->t[i].min, t[i].min) < 0 ) {
          t[i].min = other->t[i].min;
        }
        if( t[i].count == 0 || comp.compare(other->t[i].max, t[i].max) > 0 ) {
          t[i].max = other->t[i].max;
        }

        t[i].count += other->t[i].count;
      }
    }
  }
};

template<typename Record, typename Criterion>
struct TotalMinMaxCount {
  typedef Record record_t;
  typedef Criterion criterion_t;
  typedef KeyCritTraits<Record,Criterion> kc_traits_t;
  typedef typename kc_traits_t::key_t key_t;
  typedef typename kc_traits_t::ComparisonCriterion comp_t;

  criterion_t crit;
  comp_t comp;
  key_t total_min;
  key_t total_max;
  uint64_t total_num;

  TotalMinMaxCount(Criterion crit)
    : crit(crit), comp(crit), total_min(), total_max(), total_num(0)
  {
  }

  void update(const Record& r)
  {
    if( total_num == 0 || comp.compare(r, total_min) < 0 ) {
      total_min = r;
    }
    if( total_num == 0 || comp.compare(r, total_max) > 0 ) {
      total_max = r;
    }

    total_num++;
  }

  void update(const TotalMinMaxCount* other)
  {
    if( other->total_num > 0 ) {
      if( total_num == 0 || comp.compare(other->total_min, total_min) < 0 ) {
        total_min = other->total_min;
      }
      if( total_num == 0 || comp.compare(other->total_max, total_max) > 0 ) {
        total_max = other->total_max;
      }

      total_num += other->total_num;
    }
  }

  void update(const TotalMinMaxCountByBin<Record,Criterion>* other)
  {
    for( size_t i = 0; i < other->t.size(); i++ ) {

      if( other->t[i].count > 0 ) {
        if( total_num == 0 || comp.compare(other->t[i].min, total_min) < 0 ) {
          total_min = other->t[i].min;
        }
        if( total_num == 0 || comp.compare(other->t[i].max, total_max) > 0 ) {
          total_max = other->t[i].max;
        }

        total_num += other->t[i].count;
      }
    }
  }
};

struct SorterSettings {
  bool Parallel;
  bool Permute;
  SorterSettings()
    : Parallel(false), Permute(false)
  {
  }
};


template<typename Tag,
         typename Record,
         typename Criterion>
struct GeneralSorterReally {
  // Error! no general_sort - must have tag.
};

template<typename Record, typename Criterion>
struct GeneralSorterReally<return_key_criterion_tag, Record, Criterion>
{
  typedef typename KeyCritTraits<Record,Criterion>::key_t key_t;

  template<typename RecordIterator>
  static
  void general_sort(Criterion crit, key_t min, key_t max,
                    RecordIterator data, size_t num_records,
                    const SorterSettings* settings)
  {
    SorterSettings use_settings;
    if( settings ) use_settings = *settings;
    BucketSorterSettings bsettings;
    bsettings.Parallel = use_settings.Parallel;
    bsettings.Permute = use_settings.Permute;

    BucketSorter<Record,Criterion>::sort(crit, min, max, data, 0, num_records, &bsettings);
  }
};

template<typename Record, typename Criterion>
struct GeneralSorterReally<compare_criterion_tag, Record, Criterion>
{
  // min/max does not actually matter -- so we ignore it!
  // This allows implicit translation to be occuring
  // between Record and a different type of Record
  // to reach the common form for Criterion.
  template<typename Key, typename RecordIterator>
  static
  void general_sort(Criterion crit, Key min, Key max,
                    RecordIterator data, size_t num_records,
                    const SorterSettings* settings)
  {
    RecordSortingCriterion<Record,Criterion> comp(crit);

    SorterSettings use_settings;
    if( settings ) use_settings = *settings;

    if( use_settings.Parallel ) {
#ifdef HAVE_PARALLEL_ALGORITHM
      __gnu_parallel::sort(data, data+num_records, comp);
#else
      std::sort(data, data+num_records, comp);
#endif
    } else {
      std::sort(data, data+num_records, comp);
    }
  }
};
;

template<typename Record,
         typename Criterion>
struct GeneralSorter : GeneralSorterReally<typename Criterion::criterion_category, Record, Criterion>
{
  typedef typename KeyCritTraits<Record,Criterion>::key_t key_t;

  static
  void general_sort_ptr(Criterion crit, key_t min, key_t max,
                        Record* data, size_t num_records,
                        const SorterSettings* settings)
  {
    GeneralSorterReally<typename Criterion::criterion_category,Record,Criterion>::general_sort(crit, min, max, data, num_records, settings);
  }

  template<typename RecordIterator>
  static
  void check_sorted(Criterion crit, key_t min, key_t max,
                    RecordIterator data, size_t num_records)
  {
    RecordSortingCriterion<Record,Criterion> comp(crit);
    for( size_t i = 1; i < num_records; i++ ) {
      assert( comp.compare( data[i], data[i-1] ) >= 0 ); // data[i] >= last
    }
  }

  static
  void general_sort_file(Criterion crit, key_t min, key_t max, int fd,
                         size_t start_n, size_t end_n,
                         const SorterSettings* settings)
  {
    if( start_n == end_n ) return;

    size_t record_size = RecordTraits<Record>::record_size;
    size_t page_size = get_file_page_size(fd);

    Pages pgs = get_pages_for_records(page_size, start_n, end_n, record_size);
    if( pgs.outer_length == 0 ) return;

    int flags = MAP_SHARED;
#ifdef MAP_POPULATE
    flags |= MAP_POPULATE;
#endif

    FileMMap map(NULL, pgs.outer_length, PROT_READ|PROT_WRITE, flags, fd, pgs.outer_start);
    void* ptr = PTR_ADD(map.data, pgs.start - pgs.outer_start);
    typename RecordTraits<Record>::iterator_t it = RecordTraits<Record>::getiter(ptr);
    GeneralSorterReally<typename Criterion::criterion_category,Record,Criterion>::general_sort(crit, min, max, it, end_n - start_n, settings);
  }

  static
  void general_sort_file(Criterion crit, key_t min, key_t max, std::string path,
                         size_t start_n, size_t end_n,
                         const SorterSettings* settings)
  {
    int fd = open(path.c_str(), O_RDWR);
    if( fd < 0 ) throw error(ERR_IO_STR_OBJ("could not open", path.c_str()));

    general_sort_file(crit,min,max,fd,start_n,end_n,settings);

    int rc = close(fd);
    if( rc ) throw error(ERR_IO_STR_OBJ("close failed", path.c_str()));
  }

};

#endif
