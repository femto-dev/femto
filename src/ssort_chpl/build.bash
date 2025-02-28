#!/bin/bash

# ask bash to please exit if any command fails
# (as indicated by a nonzero exit code)
set -e

# ask bash to print commands being run
set -o xtrace

chpl --fast SuffixSimilarity.chpl
chpl --fast SuffixSort.chpl
chpl --fast FindUnique.chpl
chpl --fast ExtractUniqueKmers.chpl
chpl --fast Checksum.chpl
chpl --fast CopyData.chpl
