#!/bin/bash

# ask bash to please exit if any command fails
# (as indicated by a nonzero exit code)
set -e

COMM=`chpl --print-chpl-settings | awk '/CHPL_COMM:/ { print $2 }'`

for name in Test*.chpl
do
  echo compiling $name : chpl $name -o a.out --set EXTRA_CHECKS=true
  chpl $name -o a.out --set EXTRA_CHECKS=true
  echo running $name : ./a.out -nl 1
  ./a.out -nl 1
  if [[ "$COMM" != "none" ]]
  then
    echo running $name : ./a.out -nl 2
    ./a.out -nl 3
  fi
  rm a.out
done

# also check that we can compile a few tools
echo checking that a few other tools compile
chpl SuffixSimilarity.chpl -o a.out
chpl SuffixSort.chpl -o a.out
chpl FindUnique.chpl -o a.out
chpl ExtractUniqueKmers.chpl -o a.out
rm a.out

echo
echo OK
