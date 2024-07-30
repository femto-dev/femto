#!/bin/bash

# ask bash to please exit if any command fails
# (as indicated by a nonzero exit code)
set -e

for name in Test*.chpl
do
  echo compiling $name : chpl $name -o a.out --set EXTRA_CHECKS=true
  chpl $name -o a.out --set EXTRA_CHECKS=true
  echo running $name : ./a.out
  ./a.out
  rm a.out
done

echo
echo OK
