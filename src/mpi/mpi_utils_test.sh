#!/bin/sh

MATCH=`grep "#define HAVE_MPI_H" ../../config.h`
if [ "$MATCH" ]
then
  echo MPI run of ./mpi_utils_test running.
  echo Testing with 1 process
  mpirun -np 1 ./mpi_utils_test
  if [ "$?" -ne "0" ]; then exit 1; fi
  echo Testing with 2 processes
  mpirun -np 2 ./mpi_utils_test
  if [ "$?" -ne "0" ]; then exit 1; fi
  echo Testing with 3 processes
  mpirun -np 3 ./mpi_utils_test
  if [ "$?" -ne "0" ]; then exit 1; fi
  echo Testing with 4 processes
  mpirun -np 4 ./mpi_utils_test
  if [ "$?" -ne "0" ]; then exit 1; fi
  echo All tests pass
else
  echo "MPI not configured; Not running tests with mpirun"
fi

