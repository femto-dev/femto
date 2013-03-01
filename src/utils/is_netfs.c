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

  femto/src/utils/is_netfs.c
*/
#include "page_utils.h"

#include <stdio.h>

int main(int argc, char** argv)
{
  int out = -1;
  error_t err;
  char* path=NULL;

  if( argc != 2 ) {
    printf("Usage: %s path\n  determines if path is a shared filesystem\n", argv[0]);
    return -1;
  }

  path = argv[1];

  err = is_netfs(path, &out);
  die_if_err(err);
  if( out == 0 ) printf("%s is a local filesystem\n", path);
  if( out == 1 ) printf("%s is a network filesystem\n", path);
  if( out == -1 ) printf("%s is a unknown kind of filesystem\n", path);

  return 0;
}

