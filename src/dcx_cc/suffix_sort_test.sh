#!/bin/sh

MATCH=`grep "#define HAVE_MPI_H" ../../config.h`
if [ "$MATCH" ]
then
  echo Testing mpirun ./suffix_sort_test
  # Allow more processes.
  ulimit -u 2048
  echo Testing with 1 process
  mpirun -np 1 ./suffix_sort_test
  if [ "$?" -ne "0" ]; then exit 1; fi
  echo Testing with 2 processes
  mpirun -np 2 ./suffix_sort_test
  if [ "$?" -ne "0" ]; then exit 1; fi
  echo Testing with 3 processes
  mpirun -np 3 ./suffix_sort_test
  if [ "$?" -ne "0" ]; then exit 1; fi
  echo Testing with 4 processes
  mpirun -np 4 ./suffix_sort_test
  if [ "$?" -ne "0" ]; then exit 1; fi
  echo All tests pass
else
  echo "MPI not configured; not running mpirun ./suffix_sort_test"
fi

