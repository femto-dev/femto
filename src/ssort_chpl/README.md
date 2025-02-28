# What's This?

This directory contains [Chapel](chapel-lang.org) source code for:
 * distributed-memory suffix array construction
 * longest common prefix (LCP) array construction
 * finding unique sequences by processing a suffix array
 * estimating file similarity by processing a suffix array
 * reading and checksumming FASTA sequences

These operations are available in several command line tools.

# Compiling

First, you will need a working installation of [Chapel](chapel-lang.org),
including having `chpl` available in the `PATH`. If you are interested in
distributed-memory suffix array construction, you'll need an installation
of Chapel that works with the network in your compute cluster.

To build the command-line tools here, you can use `build.bash`:

```
  ./build.bash
```

Each tool can alternatively be built with `chpl --fast`, e.g.:

```
chpl --fast SuffixSort.chpl
```

There are some compile-time knobs available in various files here as
`config param`s. For example `chpl --fast --set DEFAULT_PERIOD=21` will
change the Difference Cover period from the regular default of 57 to 21.

If you are interested in seeing timing details for suffix array
construction, compile with

```
chpl --fast --set TIMING=true --set TRACE=true SuffixSort.chpl
```

# Running

## A Note About FASTA Files

Please note that, programs in this directory have special behavior when
applied to FASTA files to make these tools more applicable to
bioinformatics. FASTA files are preprocessed in-memory in the following
manner:

 * Each `> description` lines is removed and replaced with a single a `>`
   character
 * All whitespace is removed
 * All characters are converted to uppercase
 * The reverse complement of each sequence is added to the input (this
   can be disabled by running with `--INCLUDE_REVERSE_COMPLEMENT=false`)

## A Note About Parallelism and Distributed Memory

These Chapel programs should work with fewer tasks if requested; e.g.
`--dataParTasksPerLocale=2` requests to only use 2 tasks. The
`CHPL_RT_NUM_THREADS_PER_LOCALE` environment variable can be used to
limit the number of threads. (In Chapel, threads run tasks).

To run a Chapel program on multiple nodes, use `-nl` to specify a number
of "locales" (the Chapel term for rank or compute node). For example,
`./SuffixSort -nl 3` would run with 3 locales.

## SuffixSort

The `SuffixSort` program generates a suffix array and times the process.
It's primarily useful for benchmarking. Run this program providing the
files or directories to use as input for the suffix array construction.

```
  ./SuffixSort file1 file2 directory3
```

At present, it does not save the suffix array to a file, but that would
be easy to add.

### FindUnique

The `FindUnique` program creates a suffix array and then uses the suffix
array to find minimal unique substrings. It prints a count of minimal
unique substrings and shows a histogram their lengths.

```
  ./FindUnique file1 file2 directory3 --output outputdir
```


The `--output` flag is optional. If it is provided, it saves a
description of the positions of the minimal unique substrings in each
input file to a corresponding output file in the output directory. These
unique substrings files can be read, along with the corresponding
original input, in `ExtractUniqueKmers`, to print out unique sequences.

The format of these minimal unique substrings files is this:

 * at each input position, store the length of the minimal unique
   substring starting at that offset in a single byte
 * for FASTA files, store '>' as a sequence separator just like it is in
   the input, to enable alignment checks.

For example, we can compute the substrings unique to Shakespeare plays:

```
% ./FindUnique midsummer.txt julius-caesar.txt hamlet.txt macbeth.txt --output unique-dir
Computing suffix array with 6 tasks
suffix array construction of 600791 bytes took 0.063862 seconds
9.40764 MB/s
Computing Sparse PLCP array
Sparse PLCP array construction took 0.000153 seconds
Computing unique substrings
finding unique substrings took 0.024393 seconds
hamlet.txt
  found 67605 unique substrings with lengths: min 1 avg 6.38127 max 44
    len <   2 *   1.11%
    len <   3 ****   8.28%
    len <   4 *********  18.25%
    len <   5 ******************  35.09%
    len <   6 ****************************  55.59%
    len <   7 ************************************  72.10%
    len <   8 ******************************************  84.85%
    len <   9 **********************************************  92.86%
    len <  10 ************************************************  96.65%
    len <  11 *************************************************  98.51%
    len <  13 **************************************************  99.69%

julius-caesar.txt
  found 43377 unique substrings with lengths: min 2 avg 6.53332 max 29
    len <   3 ****   8.48%
    len <   4 *********  18.14%
    len <   5 *****************  33.13%
    len <   6 **************************  52.36%
    len <   7 **********************************  68.76%
    len <   8 *****************************************  82.10%
    len <   9 **********************************************  91.31%
    len <  10 ************************************************  95.91%
    len <  11 *************************************************  98.05%
    len <  13 **************************************************  99.56%

macbeth.txt
  found 39396 unique substrings with lengths: min 2 avg 6.45248 max 34
    len <   2 *   2.26%
    len <   3 *****   9.18%
    len <   4 *********  17.75%
    len <   5 *****************  33.77%
    len <   6 ***************************  53.66%
    len <   7 ***********************************  69.98%
    len <   8 ******************************************  83.36%
    len <   9 **********************************************  92.21%
    len <  10 ************************************************  96.44%
    len <  11 *************************************************  98.24%
    len <  13 **************************************************  99.56%

midsummer.txt
  found 37448 unique substrings with lengths: min 1 avg 6.44499 max 44
    len <   2 *   1.60%
    len <   3 ****   7.37%
    len <   4 *********  17.93%
    len <   5 *****************  34.62%
    len <   6 ***************************  54.10%
    len <   7 ***********************************  70.64%
    len <   8 ******************************************  83.52%
    len <   9 **********************************************  92.41%
    len <  10 ************************************************  96.52%
    len <  11 *************************************************  98.44%
    len <  13 **************************************************  99.64%


Outputting minuniq files to unique-dir
Creating unique-dir
Writing unique substrings from macbeth.txt to unique-dir/macbeth.txt.unique
Writing unique substrings from julius-caesar.txt to unique-dir/julius-caesar.txt.unique
Writing unique substrings from midsummer.txt to unique-dir/midsummer.txt.unique
Writing unique substrings from hamlet.txt to unique-dir/hamlet.txt.unique
```

### ExtractUniqueKmers

The `ExtractUniqueKmers` program reads an input file along with a minimal unique substrings file generated by `FindUnique` and produces output describing unique kmers in a format that is intended to work with [uniqsketch](https://github.com/amazon-science/uniqsketch)

```
  ./ExtractUniqueKmers --input input.fna --unique unique-dir/input.fna.unique
```

If `--k` is provided, it will set the kmer length, e.g. `--k=73` requests
it print out minimal unique sequences that are 73 base pairs long.

For example, we can show the minimal unique substrings in Macbeth that do
not occur in the other Shakespeare plays we analyzed:

```
  ./ExtractUniqueKmers --input macbeth.txt --unique unique-dir/macbeth.txt.unique
```

### SuffixSimilarity

The `SuffixSimilarity` program reads in input files, generates the suffix
array, and then uses the suffix array to estimate the similarity between
different files. It prints out the similarities scores between pairs of
files starting with the most similar.

```
  ./SuffixSimilarity file1 file2 directory3
```

For example, we can compute similarity scores between several Shakespeare
plays:

```
% ./SuffixSimilarity midsummer.txt julius-caesar.txt hamlet.txt macbeth.txt
Computing suffix array with 6 tasks
suffix array construction of 600791 bytes took 0.058115 seconds
10.338 MB/s
Computing Sparse PLCP array
Sparse PLCP array construction took 9.8e-05 seconds
in computeSimilarityBlockLCP targetBlockSize=1000 minCommonStrLen=60
Computed Boundaries
Examining Blocks
Computing Score
similarity computation took 0.005406 seconds
macbeth.txt vs midsummer.txt : 0.15112
  found 18911 common substrings with lengths: min 60 avg 9508.0 max 18963
hamlet.txt vs macbeth.txt : 0.121544
  found 18921 common substrings with lengths: min 60 avg 9503.02 max 18963
hamlet.txt vs julius-caesar.txt : 0.114391
  found 19337 common substrings with lengths: min 60 avg 9304.16 max 18963
julius-caesar.txt vs macbeth.txt : 0.00357583
  found 506 common substrings with lengths: min 60 avg 233.48 max 467
hamlet.txt vs midsummer.txt : 0.00313658
  found 408 common substrings with lengths: min 60 avg 263.5 max 467
julius-caesar.txt vs midsummer.txt : 0.00130609
  found 148 common substrings with lengths: min 60 avg 98.027 max 163
```

# Utilities for Developers

## CopyData

The `CopyData` program reads in data (including doing the FASTA
preprocessing steps described above) and saves the data that would be
used as the input for suffix array construction to a single file. This
single file can be used in further experiments.

```
  ./CopyData --output=./concatenated file1 file2 directory3
```

## Checksum

The `Checksum` program is intended to verify that the I/O process is
working correctly. It can also be used to time the process of reading the
input data. It emits a SHA-256 checksum for each file and each sequence
after the preprocessing described above.

The output of `Checksum` can be compared against `fasta-checksum.py`
which does the same thing with Biopython.

```
  ./Checksum file1 file2 directory3
```


# Testing

To test this code, you can use `check.bash`

```
  ./check.bash
```

Alternatively, compile and run a particular test, e.g.:

```
  chpl --set EXTRA_CHECKS=true TestSuffixSort.chpl && ./TestSuffixSort
```

As a general procedure, when modifying this code, perform testing
starting with single-task single-locale and working up to multiple-task
multple-locale. This testing can be done on a laptop or PC using Chapel
configured with the
[UDP conduit](https://chapel-lang.org/docs/platforms/udp.html) to
simulate multiple nodes.

  * first make sure it works on 1 task on 1 locale, e.g.:
  ```
  ./TestSuffixSort -nl 1 dataParTasksPerLocale=1
  ```
  * next, make sure it works on many tasks on 1 locale, e.g.:
  ```
  ./TestSuffixSort -nl 1 dataParTasksPerLocale=2
  ./TestSuffixSort -nl 1 dataParTasksPerLocale=3
  ...
  ./TestSuffixSort -nl 1
  ```
  * finally, make sure it works running multi-locale, e.g.:
  ```
  CHPL_RT_NUM_THREADS_PER_LOCALE=8 ./TestSuffixSort -nl 4
  ```


