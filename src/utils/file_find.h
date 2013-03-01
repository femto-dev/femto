/*
  (*) 2006-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/file_find.h
*/
#ifndef _FILE_FIND_H_
#define _FILE_FIND_H_

#include "error.h"


typedef error_t (*file_find_func_t)(char*, void*);


typedef struct {
  // Here's what we use to support file_find_get_next
  int path_max;
  char* cur_path;

  int names_depth; // what depth are we on?

  int names_stack_size;
  char*** names_stack; // the names stack...
                       // for each depth, an array of strings,
                       // last entry is NULL
                       // these are the elements of that directory.
  int names_i_size;
  int* names_i;

} file_find_state_t;

error_t init_file_find(file_find_state_t* s, int num_paths, const char** paths);

// *path is a pointer only valid until file_find_get_next is called again.
error_t file_find_get_next(file_find_state_t* s, char** path);

// Start a file find over again.
error_t reset_file_find(file_find_state_t* s);

// Calls fun on each file.
error_t file_find(file_find_state_t* s, file_find_func_t fun, void* state);

void free_file_find(file_find_state_t* s);

#endif
