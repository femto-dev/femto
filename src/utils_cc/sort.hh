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

  femto/src/utils_cc/sort.hh
*/
#ifndef _SORT_HH_
#define _SORT_HH_

#include "sort_basic.hh"
#include "bucket_sort.hh"
#include "file_pipe_context.hh"
#include "file_pipe.hh"
#include "distribute_utils.hh"
#include "distributor_node.hh"

#include <algorithm>

#ifdef HAVE_PARALLEL_ALGORITHM
#include <parallel/algorithm> // sort
#endif

#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

#define NOISY_SORTS 0

#define get_iter(data) (RecordTraits<Record>::getiter(data))
#define record_size (RecordTraits<Record>::record_size)

// We either MMap subfiles, or read/(and maybe write) them.
// I'm having trouble convincing mmap *not* to write the data
// to disk (the file is unlinked, descriptor is closed).
// For that reason, this is 0, but the drawback is that we're
// using more memory (page cache+local application memory).
#define ALWAYS_USE_MMAP 0

// We call read() in second pass (if we're not using mmap).
// We could readahead the next file. This doesn't seem to help
// performance, but it may in some cases.
// It's currently disabled to reduce memory requirements.
#define SECOND_PASS_READAHEAD 0

template<typename Record,
         typename Criterion,
         typename Splitters,
         typename Translator = IdentitySplitterTranslator<Record,Criterion,typename Splitters::splitter_record_t,typename Splitters::splitter_criterion_t> >
struct Sorter : GeneralSorter<Record, Criterion>
{
  typedef Record record_t;
  typedef Criterion criterion_t;
  typedef Splitters splitters_t;
  typedef Translator translator_t;
  typedef KeyCritTraits<Record,Criterion> kc_traits_t;
  typedef typename kc_traits_t::key_t key_t;
  typedef typename kc_traits_t::ComparisonCriterion comp_t;
  typedef typename RecordTraits<Record>::iterator_t iterator_t;
  typedef typename Splitters::splitter_record_t splitter_record_t;
  typedef typename Splitters::splitter_criterion_t splitter_criterion_t;
  typedef typename Splitters::splitter_comp_t splitter_comp_t;

  struct sort_data_node : public pipeline_node {
    Criterion crit;
    iterator_t data;
    key_t min;
    key_t max;
    size_t num;
    bool permute;

    virtual void run()
    {
      SorterSettings settings;
      settings.Parallel = false;
      settings.Permute = permute;

      //printf("Starting sort %li\n", (long int) num);

      general_sort(crit, min, max, data, num, &settings);

      //printf("Finished sort %li\n", (long int) num);
    }

    sort_data_node(Criterion crit,
                      iterator_t data,
                      key_t min,
                      key_t max,
                      size_t num,
                      bool permute) 
      : crit(crit), data(data), min(min), max(max), num(num), permute(permute)
    {
    }
  };

  struct SortFileJob {
    file_pipe_context fctx;
    FileMMap* memory;
    //void* malloced;
    struct allocated_pages alloced;
    void* buf;
    iterator_t data;
    sort_data_node* node;
    size_t num_records;
    bool is_sorted; // already sorted?

    off_t filelen;
    bool oversized;

    SortFileJob()
      : fctx(), memory(NULL), alloced(null_pages), buf(NULL), node(NULL), num_records(0), is_sorted(false), filelen(0), oversized(false)
    {
    }
    ~SortFileJob()
    {
      delete memory;
      delete node;
    }
  };

  static
  void setup_fctx(// IN
                  SortFileJob* job, fileset* set, size_t i,
                   // OUT
                  int* fd, size_t* num_records, size_t* page_size )
  {
    off_t file_length;

    // fctx must be setup in second_pass constructor.
    job->fctx.create = false;
    job->fctx.read = true;
    job->fctx.write = true;
    job->fctx.open_file_if_needed();
    //printf("setup open %s %i\n", job->fctx.fname.c_str(), job->fctx.fd);

    *fd = job->fctx.fd;
    assert(*fd >= 0);

    file_length = file_len(*fd);
    if( file_length != job->filelen ) {
      throw error(ERR_INVALID_STR("second pass file length changed"));
    }

    *num_records = file_length / record_size;
    // Should be no extra space at end of file!
    assert( file_length == (off_t) ((*num_records) * record_size) );

    *page_size = get_file_page_size(*fd);
  }

  static
  void start_job(SortFileJob* job, Criterion crit, key_t min, key_t max, fileset* set, bool permute, size_t i, bool delete_intermediates, long use_memory_per_bin, io_stats_t* stats)
  {
    int fd = -1;
    size_t num_records = 0;
    size_t page_size = 0;

    setup_fctx(job, set, i,
               &fd, &num_records, &page_size);

    Pages pgs = get_pages_for_records(page_size,0,num_records,record_size);

    if( NOISY_SORTS ) printf("Starting sort job on %20s of bytes %li\n", job->fctx.fname.c_str(), job->filelen);

    // MMap the next file..
    if( ALWAYS_USE_MMAP || job->oversized ) {
      if( ! ALWAYS_USE_MMAP ) {
        printf("Warning -- sorting file of %lf MB when use_memory_per_bin is %lf MB; falling back on mmap'd sort.\n", ((double)pgs.outer_length) / (1024.0*1024.0), ((double)use_memory_per_bin) / (1024.0*1024.0) );
      }

      void* ptr = NULL;
      if( pgs.outer_length > 0 ) {
        //job->memory = new FileMMap( NULL, pgs.outer_length, PROT_READ|PROT_WRITE, (delete_intermediates)?(MAP_PRIVATE):(MAP_SHARED), fd, pgs.outer_start);
        job->memory = new FileMMap( NULL, pgs.outer_length, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_POPULATE, fd, pgs.outer_start);
        ptr = job->memory->data;
      }

      ptr = PTR_ADD(ptr, pgs.start - pgs.outer_start);
      job->buf = ptr;
    } else {
      // Read the next file.
      job->alloced = allocate_pages( pgs.outer_length );
      job->buf = job->alloced.data;
      error_t err = read_file(fd, job->buf, job->filelen, stats);
      if( err ) throw error(err);
    }

    job->data = get_iter(job->buf);

    if( ! job->is_sorted ) {
      job->node = new sort_data_node(crit, job->data, min, max, num_records, permute);
    } else {
      job->node = NULL;
    }

    job->num_records = num_records;

    assert( ! (job->alloced.data && job->memory) );
    assert( job->buf || num_records == 0 );

    // Advise that we need the memory there.
    if( job->memory ) { // ie we're using mmap.
      job->memory->advise_willneed();

      // Wait for a readahead into the page cache.
      // This screws up valgrind. MAP_POPULATE should do it anyways.
      //{
      //  error_t err = blocking_readahead(fd, 0, job->filelen);
      //  if( err ) throw error(err);
      //}
    }

    // Close the file as we either use an mmap'd version or
    // the entire file read into memory.
    job->fctx.close_file_if_needed();

    // Start the sorting thread.
    if( job->node ) {
      job->node->start();
    }
  }

  static
  void finish_job(SortFileJob* job, Criterion crit, write_pipe* output, bool delete_intermediates, uint64_t max_output, uint64_t * num_output, io_stats_t* stats)
  {
    // Wait for the job to finish if we need to.
    if( job->node ) {
      job->node->finish();
    }

    if( NOISY_SORTS ) printf("Finished sort job on %20s of bytes %li\n", job->fctx.fname.c_str(), job->filelen);

    job->is_sorted = true;

    if( EXTRA_CHECKS && job->num_records > 0 ) {
      comp_t comp(crit);
      iterator_t data = job->data;
      for ( size_t i = 1; i < job->num_records; i++ ) {
        key_t data_key = kc_traits_t::get_key(crit, data[i]);
        key_t last_key = kc_traits_t::get_key(crit, data[i-1]);
        assert( comp.compare( data_key, last_key ) >= 0 ); // data[i] >= last
      }
    }

    // If we aren't deleting, write the data back.
    if( ! delete_intermediates ) {
      if( job->memory ) {
        // we're using MMAP, don't worry, data will be written back.
      } else {
        size_t file_length = job->num_records * record_size;
        job->fctx.open_file_if_needed();
        //printf("finish open %s %i\n", job->fctx.fname.c_str(), job->fctx.fd);
        int fd = job->fctx.fd;
        error_t err = write_file(fd, job->buf, file_length, stats);
        if( err ) throw error(err);
        assert( file_length == (size_t) file_len(fd) );
      }
    }
    //printf("finish close %s %i\n", job->fctx.fname.c_str(), job->fctx.fd);
    job->fctx.close_file_if_needed();

    if( output && job->num_records > 0 ) {
      // Output everything to a pipe.
      size_t num_records = job->num_records;
      size_t cur_record = 0;

      if( num_records > (max_output - *num_output) ) {
        num_records = max_output - *num_output;
      }

      while( cur_record < num_records ) {
        tile t = output->get_empty_tile();

        if( t.is_end() ) {
          fprintf(stderr, "Warning: sorter second pass encounter end of output pipe\n");
          break;
        }

        // compute the number of records that fit in the tile.
        size_t len_records = t.max / record_size;

        if( cur_record + len_records > num_records ) {
          len_records = num_records - cur_record;
        }

        memcpy(t.data,
               PTR_ADD(job->buf,cur_record*record_size),
               len_records*record_size);

        t.len = len_records*record_size;

        cur_record += len_records;

        output->put_full_tile(t);

        *num_output += len_records;
      }
    }

 
    if( job->node ) {
      delete job->node;
      job->node = NULL;
    }

    if( job->memory ) {
      job->memory->unmap();
      delete job->memory;
      job->memory = NULL;
    }

    if( job->alloced.data ) {
      free_pages(job->alloced);
      job->alloced.data = NULL;
    }

    job->buf = NULL; // aliases another pointer.

    // unlink the file if we're supposed to delete it.
    job->fctx.close_file_if_needed();
    if( delete_intermediates ) {
      std::string& fname = job->fctx.fname;
      int rc = unlink(fname.c_str());
      if( rc && errno != ENOENT ) {
        throw error(ERR_IO_STR_OBJ_NUM("unlink failed", fname.c_str(), errno));
      }
    }


  }

  static
  file_pipe_context make_base(std::string path, long use_procs)
  {
    file_pipe_context base = fctx_fixed_cached(path);
    assert(use_procs>0);
    base.num_io_groups = 2 * use_procs;
    base.create = true;
    base.write = true;
    base.tile_size = round_up_to_multiple(base.tile_size, record_size);
    base.io_group_size = base.tile_size*base.tiles_per_io_group;
    return base;
  }

  static
  long get_use_procs(long use_procs_in)
  {
    long use_procs = use_procs_in;
    if( use_procs == 0 ) {
      use_procs = 1;
      error_t err = get_use_processors(&use_procs);
      if( err ) throw error(err);
      assert( use_procs > 0);
      //use_procs /= 2; // half for other sort.
      // instead, allow nodes to be oversubscribed... 
      if( use_procs == 0 ) use_procs = 1;
    }
    return use_procs;
  }

  static
  size_t get_n_bins(uint64_t n_estimate, size_t use_memory, size_t in_record_size=record_size)
  {
    // n_bins is cieldiv(n_estimate,use_memory)
    uint64_t max_per_bin = use_memory;
    uint64_t n_estimate_bytes = n_estimate*in_record_size;
    size_t n_bins = (n_estimate_bytes+max_per_bin-1L)/ max_per_bin;

    // We need at least one bin!
    if( n_bins == 0 ) n_bins++;

    // This algo will become ridiculous if the splitters
    // would take up more than use_memory.... in fact
    // the splitters should be smaller than the default
    // I/O group size.
    // This is really a performance error and so should arguably
    // not be an assert.. But performance would probably be better
    // if we just used a larger use_memory than fit in RAM.
    assert(n_bins*sizeof(key_t) < DEFAULT_TILE_SIZE*DEFAULT_TILES_PER_IO_GROUP);

    // we need to store a buffer for each bin, and that should fit
    // into memory too.
    // but, because we sometimes test with really small I/O sizes, we
    // assume here that if use_memory < 1GB we skip this check.
    if( use_memory > 1024L*1024L*1024L ) {
      assert(n_bins*DEFAULT_TILE_SIZE*DEFAULT_TILES_PER_IO_GROUP < use_memory);
    }

    return n_bins;
  }

  // Used in firstpass and second pass.
  struct sort_status : private uncopyable {
    // Input settings
    Criterion crit; // how to compare records/get keys
    Translator trans; // how to convert keys between splitters&records
    std::string fname; // file name (or base for fileset)
    long use_procs_firstpass; // how many processors to use in each pass
    long use_procs_secondpass; // how many processors to use in each pass
    bool permute; // are we permuting data?

    // Available after 1st pass:
    TotalMinMaxCountByBin<splitter_record_t,splitter_criterion_t>* totals_bybin;
    TotalMinMaxCount<splitter_record_t,splitter_criterion_t>* totals;
    size_t n_bins; // how many bins to divide up into
                   // This one is needed when forming splitters...
                   // but wouldn't be used otherwise.

  //private: These aren't part of the interface
  //         but are used by implementors below and
  //         I don't want to muck with 'friend' classes
    file_pipe_context base_fctx; // file write settings
    fileset set; // filenames are stored in here.
 
    // Used in second pass.
    std::vector<SortFileJob> jobs;

    bool firstpass_complete;
    bool secondpass_complete;
    
    long use_memory_per_bin;

    sort_status(Criterion crit, Translator trans, std::string fname, long use_procs_firstpass_in, long use_procs_secondpass_in, bool permute, size_t n_bins, long use_memory_per_bin)
      : crit(crit), trans(trans), fname(fname),
        use_procs_firstpass(get_use_procs(use_procs_firstpass_in)),
        use_procs_secondpass(get_use_procs(use_procs_secondpass_in)),
        permute(permute),
        totals_bybin(NULL),
        totals(NULL),
        n_bins(n_bins),
        base_fctx(make_base(fname,use_procs_firstpass)),
        set(n_bins, base_fctx),
        jobs(),
        firstpass_complete(false),
        secondpass_complete(false),
        use_memory_per_bin(use_memory_per_bin)
    {
    }

    ~sort_status()
    {
      delete totals_bybin;
      delete totals;
    }

  };

  struct first_pass : public pipeline_node {
    sort_status* status;
    read_pipe* input;
    Splitters* splitters;

    virtual void run()
    {
      std::vector<write_pipe*> output;

      output = get_fileset_writers<file_write_pipe_t>(status->set);

      for( size_t i = 0; i < output.size(); i++ ) {
        assert( (output[i]->get_tile_size() % record_size) == 0 );
      }

      // Run a distributor node.
      typedef parallel_distributor_node<Record,Criterion,Splitters,Translator> Distributor;
     
      if( NOISY_SORTS ) printf("Starting distribution pass on %s to %li outputs\n", status->fname.c_str(), output.size());

      Distributor d(status->crit, input, &output, splitters,
                    status->trans, status->use_procs_firstpass);
      d.runhere();

      if( NOISY_SORTS ) printf("Finished distribution pass on %s to %li outputs\n", status->fname.c_str(), output.size());

      status->totals_bybin = new TotalMinMaxCountByBin<splitter_record_t,splitter_criterion_t>( splitters->crit, splitters->num_bins);
      status->totals = new TotalMinMaxCount<splitter_record_t,splitter_criterion_t>( splitters->crit );
      status->totals_bybin->update( &d.totals );
      status->totals->update( status->totals_bybin );
      status->firstpass_complete = true;

      for( size_t i = 0; i < output.size(); i++ ) {
        delete output[i];
        output[i] = NULL;
      }
      output.clear();
    }

    first_pass(sort_status* status, read_pipe* input, Splitters* splitters,
               const pipeline_node* parent=NULL,
               std::string name="",
               bool print_timing=false)
      : pipeline_node(parent,name,print_timing),
        status(status), input(input), splitters(splitters)
    {
      size_t record_size_num = record_size;
      assert(input->get_num_tiles() > (size_t) status->use_procs_firstpass);
      assert((input->get_tile_size() % record_size_num) == 0);
      assert( ! status->totals );
      
      // Set the reporting for the sort_status fctx to this thread.
      for( size_t i = 0; i < status->set.files.size(); i++ ) {
        status->set.files[i].io_stats = get_io_stats();
      }

      // Create the files.
      status->set.create_zero_length();
    }

    ~first_pass() {
      // Clear pointer in io_stats.
      for( size_t i = 0; i < status->set.files.size(); i++ ) {
        status->set.files[i].io_stats = NULL;
      }
    }

    // Check the work of the first pass!
    void check()
    {
      comp_t comp(status->crit);

      for( size_t i = 0; i < status->n_bins; i++ ) {
        file_pipe_context fctx = status->set.files[i];
        fctx.read = true;
        fctx.create = false;
        fctx.write = false;

        file_read_pipe_t pipe( fctx );
        pipe_iterator<Record> read(&pipe);
        pipe_iterator<Record> end;

        key_t min = splitters->get_min(i);
        key_t max = splitters->get_max(i);

        while( read != end ) {
          Record r = *read;
          key_t k = kc_traits_t::get_key(status->crit, r);
          assert( comp.compare( k, min ) >= 0 ); // k >= min
          assert( comp.compare( k, max ) <= 0 ); // k <= max
          ++read;
        }

        read.finish();
      }
    }
  };

  struct second_pass : public pipeline_node {
    sort_status* status;
    write_pipe* output;
    Splitters* splitters;
    uint64_t max_output;
    bool delete_intermediates;
    virtual void run()
    {
      long use_procs = status->use_procs_secondpass;
      long next_to_finish = 0;
      long next_to_start = 0;
      uint64_t num_output = 0;

      // This thread reads the data and then passes it to a sorter.
      // We wait for the sorters to complete in-order.

      // Figure out how many jobs we need based on the
      // max_output.
      long num_to_start = 0;// (long) status->jobs.size();
      {
        uint64_t num_total = 0;
        size_t i;
        for( i = 0;
             i < status->jobs.size() && num_total < max_output;
             i++ ) {
          size_t file_length = status->jobs[i].filelen;
          size_t num_records = ceildiv (file_length, record_size);

          num_total += num_records;
        }
        num_to_start = i;
      }

      // Now sort the fileset individually.
      while( next_to_start < num_to_start ) {

        // Are we full of jobs? If so - wait.
        // also wait if our next file to sort is oversized,
        // or if we're currently working on an oversized file.
        bool willwait = false;

        // check; are any of the jobs we're working on,
        // or the next job we would start, oversized?
        for( long i = next_to_finish;
             i <= next_to_start && i < (long) status->jobs.size();
             i++ ) {
          if( status->jobs[i].oversized ) {
            // this one is oversized, so it needs to be sorted alone
            willwait = true;
          }
        }
        
        if( next_to_start - next_to_finish >= use_procs ) willwait = true;

        if( willwait && next_to_finish < next_to_start ) {
          //printf("job starter waiting\n");
          finish_job(&status->jobs[next_to_finish], status->crit, output, delete_intermediates, max_output, &num_output, get_io_stats());
          next_to_finish++;
        }

        // Start a new job.
        typedef typename Splitters::splitter_record_t splitter_record_t;
        typedef typename Splitters::splitter_key_t splitter_key_t;
        typedef typename Splitters::splitter_kc_traits_t splitter_kc_traits_t;

        splitter_key_t min_sk, max_sk;
        min_sk = status->totals_bybin->t[next_to_start].min;
        max_sk = status->totals_bybin->t[next_to_start].max;

        key_t min = status->trans.splitter_key_to_key(splitters->crit, min_sk, true);
        key_t max = status->trans.splitter_key_to_key(splitters->crit, max_sk, false);

        // Advise that we'll need next_to_start+1
        if( SECOND_PASS_READAHEAD &&
            next_to_start + 1 < num_to_start ) {
          error_t err;
          size_t i = next_to_start + 1;
          int fd = -1;
          size_t num_records = 0;
          size_t page_size = 0;

          setup_fctx(&status->jobs[i], &status->set, i,
                     &fd, &num_records, &page_size);

          //printf("Reading ahead\n");
          err = advise_file_records(fd, page_size,
                                    0, num_records, ADVISE_WILLNEED,
                                    record_size);
          if( err ) throw error(err);
        }

        //printf("job starter reading\n");
        start_job(&status->jobs[next_to_start], status->crit, min, max, &status->set, status->permute, next_to_start, delete_intermediates, status->use_memory_per_bin, get_io_stats());
        //printf("job starter done reading\n");
        next_to_start++;
      }

      // Wait for any remaining jobs.
      while( next_to_finish < next_to_start ) {
        finish_job(&status->jobs[next_to_finish], status->crit, output, delete_intermediates, max_output, &num_output, get_io_stats());
        next_to_finish++;
      }

      output->close_full();

      if( next_to_finish == (long) status->jobs.size() ) {
        status->secondpass_complete = true;
      }
    }

    second_pass(sort_status* status, write_pipe* output, Splitters* splitters, uint64_t max_output=MAX_UINT64, bool delete_intermediates=true,
                const pipeline_node* parent=NULL,
                std::string name="",
                bool print_timing=false)
      : pipeline_node(parent,name,print_timing),
        status(status), output(output), splitters(splitters),
        max_output(max_output),
        delete_intermediates(delete_intermediates)
    {
      assert(MAX_UINT64>0xfffffffful);
      size_t record_size_num = record_size;
      if( output ) assert((output->get_tile_size() % record_size_num) == 0);
      status->jobs.resize(status->set.files.size());

      // Set the reporting for the sort_status fctx to this thread.
      // Set the fctx in jobs to the fctx in set.files
      // set the cached_filelen in the sort status, since
      // the file will not change size now that 1st pass is complete.
      for( size_t i = 0; i < status->set.files.size(); i++ ) {
        status->set.files[i].io_stats = get_io_stats();
        status->jobs[i].fctx = status->set.files[i];
        status->jobs[i].filelen = file_len(status->jobs[i].fctx.fname);
        status->jobs[i].oversized = status->jobs[i].filelen > 2L*status->use_memory_per_bin;
      }
    }

    ~second_pass() {
      // Clear pointer in io_stats.
      for( size_t i = 0; i < status->set.files.size(); i++ ) {
        status->set.files[i].io_stats = NULL;
      }
    }


  };

};

#undef get_iter
#undef record_size

#endif
