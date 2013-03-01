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

  femto/src/utils_cc/file_find_node.hh
*/
#ifndef _FILE_FIND_NODE_HH_
#define _FILE_FIND_NODE_HH_

extern "C" {
#include "config.h"
#include "file_find.h"
}

#include "pipelining.hh"

struct ss_state {
  file_find_state_t ffs;
  int64_t num_bytes;
  // Provide the text to this pipe
  write_pipe* output;
  int64_t num_bytes_read;
  bool reverse_data;
  int verbose;
  int64_t num_docs;
  FILE* file_lengths_file; // store the doc lengths in a file.
};

class file_find_node: public pipeline_node
{
 private:
  std::vector<std::string> path_names;
  ss_state state;

 public:
  file_find_node(std::vector<std::string> path_names,
                 write_pipe* output,
                 //FILE* file_lengths_file,
                 pipeline_node* parent=NULL,
                 std::string name="",
                 bool print_timing=false);

    
  int64_t count_size();
 protected:
  virtual void run();
};

#endif

