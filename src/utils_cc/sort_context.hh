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

  femto/src/utils_cc/sort_context.hh
*/
#ifndef _SORT_CONTEXT_HH_
#define _SORT_CONTEXT_HH_

#include <iostream>
#include <vector>
#include <cstdlib>

#include "file_pipe_context.hh"

template<typename FileWritePipeType>
struct sort_context {
  typedef FileWritePipeType file_write_pipe_t;
  size_t num_tiles; // total number of tiles created.
  std::vector<size_t> tiles_per_run;
  file_pipe_context file_pipe_ctx;
  sort_context(file_pipe_context file_pipe_ctx)
    : num_tiles(0), tiles_per_run(), file_pipe_ctx(file_pipe_ctx) { };

  file_write_pipe_t* new_write_pipe()
  {
    file_pipe_context ctx = file_pipe_ctx;
    ctx.start = num_tiles * ctx.tile_size;
    ctx.len = PIPE_UNTIL_END;
    //std::cout << "Creating write pipe from " << ctx.start << std::endl;
    return new file_write_pipe_t(ctx);
  }

  void add_run(size_t tiles_in_run)
  {
    tiles_per_run.push_back(tiles_in_run);
    num_tiles += tiles_in_run;
  }
};


#endif

