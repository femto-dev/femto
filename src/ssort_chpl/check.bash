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

# also check that we can compile a few tools
echo checking that a few other tools compile
chpl SuffixSimilarity.chpl -o a.out
chpl SuffixSort.chpl -o a.out
chpl FindUnique.chpl -o a.out
rm a.out

echo
echo OK
