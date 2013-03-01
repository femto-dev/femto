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

  femto/src/utils/find_util.c
*/

#include <stdlib.h>
#include <stdio.h>

#include "file_find.h"

error_t print_filepath(char* path, void* state)
{
  printf("%s\n", path);
  return ERR_NOERR;
}

int main( int argc, char** argv ) 
{
  error_t err;
  file_find_state_t ffs;
  char** paths;
  int num_paths;

  if( argc < 2 ) {
    printf("Usage: %s path [path...]\n", argv[0]);
    exit(-1);
  }

  paths = &argv[1];
  num_paths = argc-1;

  err = init_file_find(&ffs, num_paths, (const char**) paths);
  die_if_err(err);
  err = file_find(&ffs, print_filepath, NULL);
  die_if_err(err);
  free_file_find(&ffs);

  return 0;
}
