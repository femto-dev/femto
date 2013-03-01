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

  femto/src/utils_cc/form_runs_node.hh
*/
#ifndef _FORM_RUNS_NODE_HH_
#define _FORM_RUNS_NODE_HH_

#include <cassert>
#include <iostream>
#include <vector>

#include "config.h"


#include <algorithm> // sort


#ifdef HAVE_PARALLEL_ALGORITHM
#include <parallel/algorithm> // sort
#endif

extern "C" {
#include "timing.h"
}

#include "pipelining.hh"
#include "sort_context.hh"
#include "compare_record.hh"
#include "merge_node.hh"

#define FORM_RUNS_DEBUG 0
#define FORM_RUNS_TIMING 0
//#define REPLACE_SELECT -- doesn't work right
#define LIBRARY_SORT_ONLY
//#define SORT_MERGE

template< typename Record,
          typename Criterion >
class tile_sorter_team_node : public pipeline_node {
 private:
  class tile_sorter_node : public pipeline_node {
   public:
    typedef std::vector<Record> rec_vec;
    typedef typename rec_vec::iterator rec_vec_iter;

    // Sorts from in_tile to out_tile; returns the new sorted tile
    // (which is really a copy of out_tile).
    static
    void sort_tile(Criterion ctx, const tile in_tile, tile& out_tile)
    {
      rec_vec records;

      // Estimate number of records in tile based on first 3.
      {
        Record r;
        size_t in_pos = 0;
        size_t i;
        for( i = 0; i < 3 && in_pos < in_tile.len; i++ ) {
          r.decode(PTR_ADD(in_tile.data,in_pos));
          in_pos += r.get_record_length();
        }
        size_t expect = 128;
        if( in_pos > 0 ) {
          // expect in_tile.len / (in_pos/i)
          expect += i*in_tile.len / in_pos;
        }
        records.reserve(expect);
      }

      // Put all of the records into a vector.
      for(size_t in_pos=0;in_pos<in_tile.len;)
      {
        Record r;
        r.decode(PTR_ADD(in_tile.data,in_pos));
        in_pos += r.get_record_length();
        records.push_back(r); // here, r is copied!
        if( FORM_RUNS_DEBUG ) {
          std::cout << "run_former in: " << r.to_string() << std::endl;
        }
      }

      // Sort the vector.
      RecordSortingCriterion<Record,Criterion>  criterion();
      sort(records.begin(),records.end(),criterion);

      // Put it back into the out_tile
      // note that it must fit!
      out_tile.len = 0;
      for( rec_vec_iter pos=records.begin();
           pos < records.end();
           ++pos) 
      {
        // We have a record.
        const Record r = *pos;
        r.encode(PTR_ADD(out_tile.data,out_tile.len));
        out_tile.len += r.get_record_length();
        if( FORM_RUNS_DEBUG ) {
          std::cout << "run_former out: " << r.to_string() << std::endl;
        }
      }
    }
   private:
    Criterion ctx;
    read_pipe* const input;
    write_pipe* const output;
    virtual void run() {
      try {
        while( 1 ) {
          // Get a tile.
          tile in = input->get_full_tile();
          if( in.is_end() ) break;
          tile out = output->get_empty_tile();

          // sort the tile..
          sort_tile(ctx, in, out);

          output->put_full_tile(out);
          input->put_empty_tile(in);
        }
      } catch( ... ) {
        // Doesn't close 
        throw;
      }
      // If there are multiple threads, we want to close the pipe
      // only after they're all finished!
      // Doesn't close - tile_sorter_team does that.
    }
   public:
    tile_sorter_node(Criterion ctx, read_pipe* input, write_pipe* output,pipeline_node* parent=NULL, std::string name="", bool print_timing=false)
        : pipeline_node(parent, name, print_timing), ctx(ctx), input(input), output(output)
    {
      assert(input->get_tile_size() == output->get_tile_size());

    }
  };
 private:
  Criterion ctx;
  read_pipe* const input;
  write_pipe* const output;
  size_t num_sorting_threads;
  virtual void run() {
    std::vector<tile_sorter_node*> tile_sorters;
    try {
      // Start the tile sorters.
      for( size_t i = 0; i < num_sorting_threads; i++ ) {
        tile_sorter_node* node = new tile_sorter_node(ctx, input, output, this);
        tile_sorters.push_back(node);
        node->start();
      }

      // Finish the tile sorters.
      for( size_t i = 0; i < num_sorting_threads; i++ ) {
        tile_sorters[i]->finish();
        delete tile_sorters[i];
      }
    } catch( ... ) {
      input->close_empty();
      output->close_full();
      throw;
    }
    input->close_empty();
    output->close_full();
  }
 public:
  tile_sorter_team_node(Criterion ctx, read_pipe* input, write_pipe* output, size_t num_sorting_threads, pipeline_node* parent=NULL, std::string name="", bool print_timing=false)
      : pipeline_node(parent, name, print_timing), ctx(ctx), input(input), output(output), num_sorting_threads(num_sorting_threads)
  {
    assert(input->get_tile_size() == output->get_tile_size());
  }
};


template< typename Record,
          typename Criterion,
          typename FileWritePipeType >
class form_runs_node : public pipeline_node {
  public:
    typedef std::vector<Record> rec_vec;
    typedef typename rec_vec::iterator rec_vec_iter;
    typedef pipe_merger<Record,Criterion> pipe_merger_t;
    typedef tile_sorter_team_node<Record,Criterion> tile_sorter_team_node_t;
    typedef FileWritePipeType file_write_pipe_t;
  private:
    Criterion ctx;
    sort_context<FileWritePipeType>* sort_ctx;
    read_pipe* const input;
    size_t tiles_per_group;

    virtual void run() {
        // This is the "Use heaps and do replacement selection" way.
#ifdef REPLACE_SELECT
      //tinfo_t timing;
      //size_t total_bytes;
      tile in_tile;
      tile out_tile;
      //a vector that contains pointers to all the input tiles in a group
      size_t tile_size = input->get_tile_size();
      long num_procs = 1;
      error_t err = get_num_processors(&num_procs);
      if( err ) throw error(err);
      if( num_procs <= 0 ) num_procs = 1;
      size_t num_sorting_threads = num_procs/2;
      size_t num_merging_threads = num_procs - num_sorting_threads;
      if( num_sorting_threads == 0 ) num_sorting_threads = 1;
      if( num_merging_threads == 0 ) num_merging_threads = 1;
      size_t run_start = 0;

      // Input must be thread safe since we run multiple run-formers
      if(!input->is_thread_safe()) {
        std::cerr  << "Warning:: form_runs not multi-threaded - input not thread safe" << std::endl;
        num_sorting_threads = 1;
      }


      try {
        // Set a few "run formation" tile-sorter threads.
        buffered_pipe sorted_tiles(tile_size, tiles_per_group+num_sorting_threads*2);
        tile_sorter_team_node_t tile_sorter_team(ctx, input, &sorted_tiles, num_sorting_threads, this, "sorter team", get_print_timing());

        // We use the strategy from Dementiev using two queues.
        pipe_merger_t heap0(ctx, num_merging_threads);
        pipe_merger_t heap1(ctx, num_merging_threads);
        pipe_merger_t* current_heap;
        pipe_merger_t* next_heap;
        pipe_back_inserter<Record> writer(output);

        // start up the sorter team.
        tile_sorter_team.start();

        current_heap = &heap0;
        next_heap = &heap1;

        // This is really just a fancy way of doing "replacement selection"
        // 1) Fill up the heap with tiles_per_group tiles.
        while ( current_heap->size() < tiles_per_group ) {
          // Get a tile from the sorted tiles.
          tile sorted_tile = sorted_tiles.get_full_tile();
          // Exit the loop if there's no more data.
          if( sorted_tile.is_end() ) break;
          // Make sure that the length of that tile is > 0.
          if( sorted_tile.len != 0 ) {
            // Create a "special_tile_pipe" which calls
            // sorted_tiles->put_empty_tile when put_empty_tile is called.
            read_pipe * p = new single_tile_pipe(sorted_tile, &sorted_tiles);
            // Put the new sorted tile into the current heap.
            current_heap->add_read_pipe(p);
          } else {
            // If the length is 0, just return it to the pipe.
            sorted_tiles.put_empty_tile(sorted_tile);
          }
        }

        // Start merging through current_heap.
        current_heap->start();

        // 2) Read a tile. If it fits in current_heap, 
        //    that is, first record in tile > current_heap.head,
        //    put it in current_heap. Otherwise, put it in next_heap.
        //    Then, output data from the current heap until a tile is freed.
        while ( 1 ) {
          // Get a tile from the sorted tiles
          tile sorted_tile = sorted_tiles.get_full_tile();
          // Exit the loop if there's no more data.
          if( sorted_tile.is_end() ) break;
          // Make sure that the length of that tile is > 0.
          if( sorted_tile.len == 0 ) {
            sorted_tiles.put_empty_tile(sorted_tile);
          } else {
            // Create a "special_tile_pipe" which calls
            // sorted_tiles->put_empty_tile when put_empty_tile is called.
            read_pipe * p = new single_tile_pipe(sorted_tile, &sorted_tiles);

            // Put the new sorted tile into one of the heaps.
            // We can place it current_heap - which we're working on -
            // if it's greater than the top element there.
            if( ! current_heap->empty() ) {
              RecordSortingCriterion<Record,Criterion> crit(ctx);
              const Record& top = current_heap->top();
              Record r;
              r.decode(ctx,sorted_tile.data);
              // Would top go before the first element of the heap?
              if( crit.compare(r,top) < 0 ) {
                // If r < top, place the tile beginning with r in next_heap
                next_heap->add_read_pipe(p);
              } else {
                // r >= top; we can keep it in the current heap.
                current_heap->add_read_pipe(p);
              }
            } else {
              // If current_heap is empty, we place it there.
              current_heap->add_read_pipe(p);
            }

            // Now go through what's in current_heap
            // and output the least element until we've
            // got fewer than tiles_per_group - other_heap_size
            // tiles in the heap.
            // This way, we maintain that tiles_per_group tiles
            // are stored in memory always.
            ssize_t min_size = tiles_per_group - next_heap->size();
            while( ((ssize_t)current_heap->size()) >= min_size &&
                  !current_heap->empty() ) {
              // Get the top record.
              const Record& r = current_heap->top();
              // Write the record.
              writer.push_back(r);
              if( FORM_RUNS_DEBUG ) {
                std::cout << get_name() << " heap out0: " << r.to_string(ctx) << std::endl;
              }
              // Pop the record.
              current_heap->pop();
            }

            // if current_heap just became empty, switch the pointers
            // and start a new run
            if( current_heap->empty() ) {
              // Finish merge threads in current_heap.
              current_heap->finish();

              // switch the pointers.
              {
                pipe_merger_t* tmp = current_heap;
                current_heap = next_heap;
                next_heap = tmp;
              }

              // Start merge threads in new current_heap
              current_heap->start();

              // start a new run.
              writer.flush();
              sort_ctx->add_run(writer.get_num_written()-run_start);
              run_start = writer.get_num_written();
              if( FORM_RUNS_DEBUG ) {
                std::cout << get_name() << " heap exhausted, flushed output" << std::endl;
              }
            }
          } // end if we had a full tile with data in it.
        } // end while(1) -- not EOF on sorted_tiles.

        // finish up the sorter team.
        tile_sorter_team.finish();

        // Close the current heap.
        current_heap->finish();

        // Get the final run out of the current heap.
        if( ! current_heap->empty() ) {
          while( ! current_heap->empty() ) {
            // Get the top record.
            const Record& r = current_heap->top();
            // Write the record.
            writer.push_back(r);
            if( FORM_RUNS_DEBUG ) {
              std::cout << get_name() << " heap out1: " << r.to_string(ctx) << std::endl;
            }
            // Pop the record.
            current_heap->pop();
          }
          writer.flush();
          sort_ctx->add_run(writer.get_num_written()-run_start);
          run_start = writer.get_num_written();
        }

        // Close the next heap.
        next_heap->finish();

        // Get the final run out of the next heap.
        if( ! next_heap->empty() ) {
          // Get the final run out of the heap.
          while( ! next_heap->empty() ) {
            // Get the top record.
            const Record& r = next_heap->top();
            // Write the record.
            writer.push_back(r);
            if( FORM_RUNS_DEBUG ) {
              std::cout << get_name() << " heap out2: " << r.to_string(ctx) << std::endl;
            }
            // Pop the record.
            next_heap->pop();
          }
          writer.flush();
          sort_ctx->add_run(writer.get_num_written()-run_start);
          run_start = writer.get_num_written();
        }

        // Close the output.
        writer.finish();
      } catch ( ... ) {
        input->close_empty();
        output->close_full();
        throw;
      }

      // Close the input.
      input->close_empty();

      return;
#endif
      // endif REPLACE_SELECT

      // This is the "one big parallel sort" way.
#ifdef LIBRARY_SORT_ONLY
      tinfo_t timing;
      tile in_tile;
      tile out_tile;
      bool has_more_data=true;
      //a vector that contains pointers to all the input tiles in a group
      std::vector<tile> input_tiles;
      size_t total_bytes;
      //making record pointers which point to each record
      rec_vec records;

      try {
    
        // Adjust the number of input tiles for the read pipe.
        if( input->get_num_tiles() < 1+tiles_per_group ) {
          input->add_tiles(2*tiles_per_group);
          // Make sure that we succeeded in adding the tiles!
          assert(input->get_num_tiles() >= 2*tiles_per_group);
        }

        input_tiles.reserve(tiles_per_group);

        while( has_more_data ) {
          total_bytes = 0;
          input_tiles.clear();
          records.clear();

          // Get tiles_per_group empty tiles and get record pointers
          // for these, which we will then sort.
          for(size_t i=0;i<tiles_per_group;i++)
          {
              in_tile = input->get_full_tile();
              //printf("Form runs node got tile %i with ptr %p\n", i, in_tile.data);
              if( ! in_tile.has_tile() ) {
                has_more_data = false;
                break;
              }
              
              input_tiles.push_back(in_tile);

              // Add the record pointers - decode each record to get length
              for(size_t in_pos=0;in_pos<in_tile.len;)
              {
                Record r;
                r.decode(ctx, PTR_ADD(in_tile.data,in_pos));
                if( FORM_RUNS_DEBUG ) {
                  std::cout << get_name() << " in: " << r.to_string(ctx) << std::endl;
                }
                in_pos += r.get_record_length(ctx);
                records.push_back(r); // here, r is copied!
              }

              total_bytes += in_tile.len;
            }
          

            if( FORM_RUNS_TIMING > 1 ) {
              start_clock_r(&timing); 
            }

            RecordSortingCriterion<Record,Criterion>  criterion(ctx);
            //sort all the records in a group
#ifdef HAVE_PARALLEL_ALGORITHM
            __gnu_parallel::sort(records.begin(),records.end(),criterion);
#else
            sort(records.begin(),records.end(),criterion);
#endif


            write_pipe* output = sort_ctx->new_write_pipe();
            if( FORM_RUNS_TIMING > 1 ) {
              stop_clock_r(&timing);
              print_timings_r(&timing, get_name().c_str(), total_bytes, "#F#");
            }
            
            out_tile = output->get_empty_tile();
            if( ! out_tile.has_tile() ) 
             throw error(ERR_INVALID_STR("Could not get empty tile"));
            if( FORM_RUNS_DEBUG ) {
              std::cout << get_name() << " new tile" << std::endl;
            }
            
            //std::cout << "Form runs - record group " << std::endl;
            size_t counter_output_tiles=0;
            for( rec_vec_iter pos=records.begin();
                 pos < records.end();
                 ++pos) 
            {  
              // We have a record.
              Record r = *pos;
              //r.decode(ctx, *pos);
              // Write that record to the output.
              if(r.get_record_length(ctx) + out_tile.len > out_tile.max)
              {
                output->put_full_tile(out_tile);
                counter_output_tiles++;
                out_tile = output->get_empty_tile();
                if( ! out_tile.has_tile() ) 
                    throw error(ERR_INVALID_STR("Could not get empty tile"));
                if( FORM_RUNS_DEBUG ) {
                  std::cout << get_name() << " new tile" << std::endl;
                }
                assert(out_tile.len==0);
              }
              if( FORM_RUNS_DEBUG ) {
                std::cout << get_name() << " out: " << r.to_string(ctx) << std::endl;
              }
              
              //put the record into the output tile
              r.encode(ctx, PTR_ADD(out_tile.data, out_tile.len));
              out_tile.len += r.get_record_length(ctx);
            }
 
            if( out_tile.len > 0 ) {
              output->put_full_tile(out_tile);
              counter_output_tiles++;
              output->close_full();
            } else {
              output->close_full(out_tile);
            }

            delete output;

            // Save the number of tiles in this run.
            if( counter_output_tiles > 0 ) {
              sort_ctx->add_run(counter_output_tiles);
            }

            // Return the input tiles.
            for(size_t i=0;i<input_tiles.size();i++)  
              input->put_empty_tile(input_tiles[i]);

          }//while
       
      } catch ( ... ) {
        input->close_empty();
       
        throw;
      }

      input->close_empty();

      // Once form runs is finished, we'll be done with the extra tiles
      // we added to the input pipe above.
      input->free_empty_tiles();
 
      return;   
#endif
       // endif LIBRARY_SORT_ONLY

#ifdef SORT_MERGE
      size_t tile_size = input->get_tile_size();
      long num_procs = 1;
      error_t err = get_num_processors(&num_procs);
      if( err ) throw error(err);
      if( num_procs <= 0 ) num_procs = 1;
      size_t num_sorting_threads = 3*num_procs/4;
      size_t num_merging_threads = 3*num_procs/4;
      if( num_sorting_threads == 0 ) num_sorting_threads = 1;
      if( num_merging_threads == 0 ) num_merging_threads = 1;
      if(!input->is_thread_safe()) {
        std::cerr  << "Warning:: form_runs not multi-threaded - input not thread safe" << std::endl;
        num_sorting_threads = 1;
      }
      file_write_pipe_t *output=NULL;
      std::cout  << "Will use " << num_sorting_threads << " sorters and " << num_merging_threads << " mergers" << std::endl;

      try {
        // Set a few "run formation" tile-sorter threads.
        buffered_pipe sorted_tiles(tile_size, 2*tiles_per_group+num_sorting_threads*2);
        tile_sorter_team_node_t tile_sorter_team(ctx, input, &sorted_tiles, num_sorting_threads, this, "sorter team", get_print_timing());

        bool go=true;
        std::vector<read_pipe*> to_merge;
        to_merge.reserve(tiles_per_group);

        // start up the sorter team.
        tile_sorter_team.start();

        while( go ) {
          to_merge.clear();

          // Once we have enough tiles in sorted_tiles, we 
          while( to_merge.size() < tiles_per_group ) {
            // Get a tile from the sorted tiles.
            tile sorted_tile = sorted_tiles.get_full_tile();
            // Exit the loop if there's no more data.
            if( sorted_tile.is_end() ) {
              go = false;
              break;
            }
            // Make sure that the length of that tile is > 0.
            if( sorted_tile.len != 0 ) {
              // Add it to the list of mergers.
              // Create a "special_tile_pipe" which calls
              // sorted_tiles->put_empty_tile when put_empty_tile is called.
              read_pipe * p = new single_tile_pipe(sorted_tile, &sorted_tiles);
              to_merge.push_back(p);
            } else {
              // If the length is 0, just return it to the pipe.
              sorted_tiles.put_empty_tile(sorted_tile);
              continue; // try again!
            }
          }

          if( ! to_merge.empty() ) {
            output = sort_ctx->new_write_pipe();
            {
              counting_write_pipe counter(output);

              // Now we'll merge in two groups.
              // Now merge the tiles in to_merge
              // Note - this will close the output pipe and call the destructor for 
              //        each pipe that we have passed in.
              multiway_merge<Record,Criterion>
                (ctx, &to_merge, &counter, num_merging_threads);

              sort_ctx->add_run(counter.get_num_written());
            }

            delete output;
            output = NULL;
          }
        }

        // finish up the sorter team.
        tile_sorter_team.finish();

      } catch ( ... ) {
        input->close_empty();
        delete output;
       
        throw;
      }

      input->close_empty();

      return;
#endif
    }
  public:
    form_runs_node(Criterion ctx, sort_context<FileWritePipeType>* sort_ctx, read_pipe* input,size_t tiles_per_group, pipeline_node* parent=NULL, std::string name="", bool print_timing=false)
      : pipeline_node(parent, name, print_timing), ctx(ctx), sort_ctx(sort_ctx), input(input), tiles_per_group(tiles_per_group)
    {
      assert(tiles_per_group>=1); 

    }
};


#endif
