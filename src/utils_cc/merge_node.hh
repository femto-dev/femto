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

  femto/src/utils_cc/merge_node.hh
*/
#ifndef _MERGE_NODE_HH_
#define _MERGE_NODE_HH_

#include <cassert>
#include <iostream>
#include <algorithm>
#include <queue>
#include <vector>
#include <list>
#include <climits>

#include "pipelining.hh"
#include "compare_record.hh"
#include "merger.hh"

#define MERGE_DEBUG 0
// uncomment USE_HEAP_ALWAYS below to use older heap implementation 
//#define USE_HEAP_ALWAYS

template< typename Record,
          typename Criterion>
class merge_records_node: public pipeline_node
{
 private:
  typedef pipe_iterator<Record> reader_t;
  typedef pipe_back_inserter<Record> writer_t;
  typedef pipe_merger<Record,Criterion> pipe_merger_t;
  Criterion ctx;
  std::vector <read_pipe*>* input;
  write_pipe *output;

  virtual void run()
  {

    try {
      long use_procs = 1;
      error_t err = get_use_processors(&use_procs);
      if( err ) throw error(err);
      size_t num_merging_threads = use_procs;
      assert(use_procs > 0);

      multiway_merge<Record,Criterion>(ctx, input, output, num_merging_threads);

    } //try finished
    catch(...) {
      output->close_full();
      throw;
    }
    //output->close_full(); output pipe should be closed by the merger network.

  } //end of run function
 public:
  /* The input pipes will be closed but not freed
   * in the merger.run() as they run out.
   */
  merge_records_node(Criterion ctx,
                     std::vector<read_pipe*>* input,
                     write_pipe* output,
                     pipeline_node* parent=NULL,
                     std::string name="",
                     bool print_timing=false)
    : pipeline_node(parent,name,print_timing),
      ctx(ctx), input(input), output(output)
  {
    for( size_t i = 0; i < input->size(); i++ ) {
      assert((*input)[i]);
    }
  }
};

#endif
