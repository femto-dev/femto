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

  femto/src/utils_cc/sort_node.hh
*/
#ifndef _SORT_NODE_HH_
#define _SORT_NODE_HH_

extern "C"
{
#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include "processors.h"
}

#include <cassert>
#include <cstdlib>
#include "sort_context.hh"
#include "compare_record.hh"
#include "pipelining.hh"
#include "form_runs_node.hh"
#include "merge_node.hh"
#include "file_pipe.hh"
#include <vector>

#define SORT_NODE_DEBUG 0

template < typename Record,
           typename Criterion >
class sort_node:public pipeline_node
{
 public:
  typedef file_read_pipe<> file_read_pipe_t;
  typedef file_write_pipe<> file_write_pipe_t;

 private:
  Criterion ctx;
  read_pipe *const input;
  write_pipe *const output;
  size_t tiles_per_group;
  //long num_threads;

  virtual void run()
  {
    /* The sort node does:
     * (for one thread)
     * --in-- -> form_runs -> merge -> --out--
     */

    // Wait for at least one full tile to be placed into the pipe.
    // This prevents us from allocating the extra tiles until we're
    // ready to start processing data.
    input->wait_full();

    //FILE *runs = fopen("/tmp/sort_tmp", "w+"); //tmp_file(ctx->tmp_dir);
    FILE *runs = tmp_file(ctx->tmp_dir);
    size_t file_tile_size = ctx->default_tile_size;
    file_pipe_context file_context(get_io_stats(),
                            fileno(runs),
                            0, PIPE_UNTIL_END,
                            file_tile_size,
                            ctx->default_tiles_per_io_group,
                            2*tiles_per_group/ctx->default_tiles_per_io_group);
// ctx->default_num_io_groups));
      //input->add_tiles(2*tiles_per_group);
    sort_context<file_write_pipe_t> sort_ctx(file_context);

    //creating output pipe for form_runs.
    {
      form_runs_node<Record,Criterion,file_write_pipe_t>
        form(ctx, &sort_ctx, input, tiles_per_group, this, get_name() + ":form_runs", get_print_timing());

      // start the threads.
      form.start();
      form.finish();
    }

    // Make a note of the average number of tiles per run.
    if( SORT_NODE_DEBUG > 0 ) {
      size_t tot=0;
      for( size_t i = 0; i < sort_ctx.tiles_per_run.size(); i++ ) {
        tot+=sort_ctx.tiles_per_run[i];
      }
      double avg = tot;
      avg /= sort_ctx.tiles_per_run.size();
      std::cout << "After form runs average number of tiles per run is " << avg << " in " << sort_ctx.tiles_per_run.size() << " runs" << std::endl;
    }

    // Now create the vector of readers to pass to the merge node.
    std::vector <read_pipe*> input_pipes;

    size_t total_bytes = 0;

    for( size_t i = 0; i < sort_ctx.tiles_per_run.size(); i++ ) {
      size_t this_run_bytes = file_tile_size * sort_ctx.tiles_per_run[i];
      file_read_pipe_t *input_pipe = new file_read_pipe_t(
          file_pipe_context(get_io_stats(),
                            fileno(runs),
                            total_bytes, this_run_bytes,
                            file_tile_size,
                            ctx->default_tiles_per_io_group,
                            ctx->default_num_io_groups));

      input_pipes.push_back(input_pipe);

      total_bytes += this_run_bytes;
    }
    merge_records_node<Record,Criterion>
      merge(ctx, &input_pipes, output, this, get_name() + ":merge", get_print_timing());

    merge.start();
    merge.finish();

    // The input_pipes are freed in the merger.
    //for (size_t i = 0; i < sort_ctx.tiles_per_run.size(); i++) {
    //  delete input_pipes[i];
    //}

    fclose(runs);
  }

 public:
  sort_node(Criterion ctx, read_pipe *input, write_pipe *output,
            size_t tiles_per_group, pipeline_node* parent, std::string name="", bool print_timing=false) //, long num_threads_in = 0)
  : pipeline_node(parent, name, print_timing), ctx(ctx), input(input), output(output),
    tiles_per_group(tiles_per_group) //, num_threads(num_threads_in)
  {
    // If num_threads is 0, set number of threads to n_processors/2
    /*if (num_threads <= 0) {
      long num_processors = 0;
      error_t err;
      err = get_num_processors(&num_processors);
      if (err)
        throw error(err);
      num_threads = num_processors / 2;
      if (num_threads <= 0)
        num_threads = 1;
    }*/
  }
};

#endif
