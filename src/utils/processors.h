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

  femto/src/utils/processors.h
*/
#ifndef _PROCESSORS_H_
#define _PROCESSORS_H_

#include "config.h"

#include <unistd.h>
#include "error.h"

// Get the number of processors available in the machine.
error_t get_num_processors(long* num_procs);

// Get the number of processors to use for a new task.
error_t get_use_processors(long* use_procs);

// How much physical memory is on the machine?
error_t get_phys_mem(long long* bytes);

// How many open files can we have?
error_t get_max_open_files(long long* max_fds);

// How many simultaneous mappings?
error_t get_max_mappings(long long* max_mappings);

#endif
