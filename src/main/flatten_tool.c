/*
  (*) 2011-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/flatten_tool.c
*/
#include "error.h"
#include "index.h"

int main(int argc, char** argv)
{
  char* from;
  char* to;
  error_t err;

  if( argc != 3 ) {
    printf("Usage: %s index_directory new_index_file\n", argv[0]);
    return -1;
  }

  from = argv[1];
  to = argv[2];

  printf("Flattening %s to %s\n", from, to);

  err = flatten_index(from, to);
  die_if_err(err);

  return 0;

}

