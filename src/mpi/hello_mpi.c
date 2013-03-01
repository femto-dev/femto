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

  femto/src/mpi/hello_mpi.c
*/
#include <stdio.h>
#include "config.h"

#ifdef HAVE_MPI_H
#include <mpi.h>

int main (int argc, char** argv)
{
  int rank, size, length;
  char name[BUFSIZ];


  MPI_Init (&argc, &argv);      /* starts MPI */
  MPI_Comm_rank (MPI_COMM_WORLD, &rank);        /* get current process id */
  MPI_Comm_size (MPI_COMM_WORLD, &size);        /* get number of processes */
  MPI_Get_processor_name(name, &length);

  printf( "%s: hello world from process %d of %d arg:%s\n", name, rank, size, argv[1]);

  MPI_Barrier(MPI_COMM_WORLD);

  MPI_Finalize();

  return 0;
}
#else

int main (int argc, char** argv)
{
  printf( "MPI not configured/installed\n");
  return 0;
}

#endif
