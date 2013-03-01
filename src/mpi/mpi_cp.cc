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

  femto/src/mpi/mpi_cp.cc
*/
#include <cstdio>
#include <cstring>
#include <string>

#include "mpi_utils.hh"

void usage(char* pname)
{
  fprintf(stderr, "Usage: %s [--move] from_file from_proc to_file to_proc\n", pname);
}

int main(int argc, char** argv)
{
  MPI_handler::init_MPI(&argc, &argv);
  int rc = 0;
  bool move_not_copy = false;

  char* other_args[4];
  int num_other_args = 4;
  int other_arg = 0;

  int j = 0;
  std::string from_file;
  int from_proc;
  std::string to_file;
  int to_proc;

  int i;



  if( argc < 5 ) {
    fprintf(stderr, "Not enough arguments\n");
    usage(argv[0]);
    rc = -1;
    goto done;
  }
  for( i = 1; i < argc; i++ ) {
    if( 0 == strcmp(argv[i], "--move") ) {
      move_not_copy = true;
    } else if( 0 == strcmp(argv[i], "--help") ) {
      usage(argv[0]);
      goto done;
    } else {
      assert( other_arg < num_other_args );
      other_args[other_arg++] = argv[i];
    }
  }

  if( other_arg != num_other_args ) {
    fprintf(stderr, "Not enough arguments\n");
    usage(argv[0]);
    return -1;
  }

  j = 0;
  from_file = other_args[j++];
  from_proc = atoi(other_args[j++]);
  to_file = other_args[j++];
  to_proc = atoi(other_args[j++]);

  printf("Copying file %s on proc %i to %s on proc %i\n",
         from_file.c_str(), from_proc,
         to_file.c_str(), to_proc);


  mpi_copy_file(DEFAULT_COMM, from_file, from_proc, to_file, to_proc, move_not_copy);

done:

  MPI_handler::finalize_MPI();

  return 0;
}
