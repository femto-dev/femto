/*
  2024 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/ssort_chpl/Utility.chpl
*/

module Utility {


import CTypes.{c_int};
import FileSystem.{isFile, isDir, findFiles, getFileSize};
import FileSystem;
import IO;
import List.list;
import OS.EofError;
import Path;
import Sort.{sort,isSorted};
import BlockDist.blockDist;
import ChplConfig.CHPL_COMM;

import SuffixSort.{EXTRA_CHECKS, INPUT_PADDING, DISTRIBUTE_EVEN_WITH_COMM_NONE};

/* For FASTA files, when reading them, also read in the reverse complement */
config param INCLUDE_REVERSE_COMPLEMENT=true;

/* Compute the number of tasks to be used for a data parallel operation */
proc computeNumTasks(ignoreRunning: bool = dataParIgnoreRunningTasks) {
  if __primitive("task_get_serial") {
    return 1;
  }

  const tasksPerLocale = dataParTasksPerLocale;
  const ignoreRunning = dataParIgnoreRunningTasks;
  var nTasks = if tasksPerLocale > 0 then tasksPerLocale else here.maxTaskPar;
  if !ignoreRunning {
    const otherTasks = here.runningTasks() - 1; // don't include self
    nTasks = if otherTasks < nTasks then (nTasks-otherTasks):int else 1;
  }

  return nTasks;
}

/* Make a BlockDist domain, but fall back on DefaultRectangular if
   CHPL_COMM=none.
*/
proc makeBlockDomain(dom, targetLocales) {
  if CHPL_COMM=="none" && !DISTRIBUTE_EVEN_WITH_COMM_NONE {
    return dom;
  } else {
    return blockDist.createDomain(dom, targetLocales=targetLocales);
  }
}

/* This function gives the size of an array of triangular indices
   for use with flattenTriangular.
 */
inline proc triangleSize(n: int) {
  return (n-1)*n/2;
}

/* This function converts an (i,j) index with i!=j and 0<=i<n and 0<=j<n
   into an index for an array storing the triangular matrix.
   As such it considers (i,j) and (j,i) the same.
   This function assumes i!=j.
 */
inline proc flattenTriangular(in i: int, in j: int) {
  if EXTRA_CHECKS {
    assert(i != j);
  }
  if i < j {
    i <=> j;
  }
  // now i > j
  var ret = triangleSize(i) + j;
  return ret;
}

/*
  Finds and returns the integer index i such that

  arr[i] <= target < arr[i+1]

  May return -1 for i; in that case target < arr[0].
  May return n-1 for i; in that case arr[n-1] <= target.
  Assumes that 'arr' is sorted.
 */
proc bsearch(const arr: [] int, target: int) {
  const n = arr.size;
  var a, b, middle = 0;

  if EXTRA_CHECKS {
    assert(isSorted(arr));
  }
  if target < arr[0] then return -1;
  if arr[n-1] <= target then return n-1;

  a = 0;
  b = n-1;
  // always we have that arr[a] <= target < arr[b].

  // divide the search space in half
  while b - a > 1 {
    middle = (a + b) / 2;
    if target < arr[middle] then b = middle;
    else a = middle; // arr[middle] <= target
  }

  if EXTRA_CHECKS {
    assert(arr[a] <= target && target < arr[a+1]);
  }
  return a;
}

/*
 Gather files; if the passed path is a file, store it in to the list;
 if it is a directory, gather files contained in that directory, recursively,
 into the list.
 */
proc gatherFiles(ref files: list(string), path: string) throws {
  if isFile(path) {
    files.pushBack(path);
  }
  if isDir(path) {
    for found in findFiles(path, recursive=true) {
      files.pushBack(found);
    }
  }
}

private inline proc toUpper(x: uint(8)): uint(8) {
  extern proc toupper(c: c_int): c_int;
  return toupper(x):uint(8);
}

// assumes that the input is already upper case
private inline proc complement(x: uint(8)): uint(8) {
  param A = "A".toByte();
  param T = "T".toByte();
  param G = "G".toByte();
  param C = "C".toByte();

  if x == C then return G;
  if x == G then return C;
  if x == A then return T;
  if x == T then return A;

  // otherwise, assume not DNA
  return x;
}

/* Computes the reverse complement of a region of an input array and stores it
   in a region of the output array. The input and output arrays can be the same
   array provided that the regions are non-overlapping. */
proc reverseComplement(const ref input: [] uint(8),
                       inputRegion: range,
                       ref output: [] uint(8),
                       outputRegion: range) {
  if EXTRA_CHECKS {
    assert(inputRegion.size == outputRegion.size);
  }

  const n = inputRegion.size;
  for i in 0..<n {
    const inputIdx = i + inputRegion.first;
    const outputIdx = n - 1 - i + outputRegion.first;
    output[outputIdx] = complement(input[inputIdx]);
  }
}

private const fastaExtensions = [".fasta", ".fas", ".fa", ".fna",
                                 ".ffn", ".faa", ".mpfa", ".frn"];

/* Returns 'true' if 'path' refers to a fasta file */
proc isFastaFile(path: string): bool throws {
  var foundExt = false;
  for ext in fastaExtensions {
    if path.toLower().endsWith(ext) {
      foundExt = true;
    }
  }

  if foundExt {
    var r = IO.openReader(path, region=0..1);
    return r.readByte() == ">".toByte();
  }

  return false;
}

/* Computes the size of the nucleotide data that will
   be read by readFastaFileSequence */
proc computeFastaFileSize(path: string) throws {
  extern proc isspace(c: c_int): c_int;

  // compute the file size without > lines or whitespace
  var r = IO.openReader(path);
  var inDescLine = false;
  var count = 0;
  while true {
    try {
      var byte = r.readByte();
      if byte == ">".toByte() {
        inDescLine = true;
        count += 1; // we will put > characters to divide sequences
      } else if byte == "\n".toByte() && inDescLine {
        inDescLine = false;
      }
      if isspace(byte) == 0 && !inDescLine {
        count += 1;
      }
    } catch e: EofError {
      break;
    }
  }

  if INCLUDE_REVERSE_COMPLEMENT {
    count = 2*count;
  }

  return count;
}

/* Reads a the sequence portion of a fasta file into a region of an array.
   The resulting array elements will contain a > at the start of each sequence
   followed by the nucleotide data. The whitespace and sequence
   descriptions are removed.
   The region size should match 'computeFastaFileSize'. */
proc readFastaFileSequence(path: string,
                           ref data: [] uint(8),
                           region: range,
                           verbose = true) throws
{
  extern proc isspace(c: c_int): c_int;

  if region.strides != strideKind.one {
    compilerError("Range should be stride one");
  }
  var dataStart = region.low;
  var n = region.size;
  var r = IO.openReader(path);
  var inDescLine = false;
  var count = 0;
  var desc = "";
  while true {
    try {
      var byte = r.readByte();
      if byte == ">".toByte() {
        inDescLine = true;
        if count < n {
          data[dataStart + count] = byte;
        }
        desc = "";
        count += 1;
      } else if byte == "\n".toByte() && inDescLine {
        inDescLine = false;
        if verbose {
          writeln("Reading sequence ", desc);
        }
      }
      if inDescLine {
        desc.appendCodepointValues(byte);
      } else if isspace(byte) == 0 {
        if count < n {
          data[dataStart + count] = toUpper(byte);
        }
        count += 1;
      }
    } catch e: EofError {
      break;
    }
  }

  if INCLUDE_REVERSE_COMPLEMENT {
    // store the reverse complement just after the original sequence;
    // except the initial > would be a trailing >,
    // so emit a separator and don't revcomp the initial >
    data[dataStart + count] = ">".toByte();
    const countLessOne = count - 1; // don't revcomp the initial separator,
                                    // because it would end up at the end
    reverseComplement(data, dataStart+1..#countLessOne,
                      data, dataStart+1+count..#countLessOne);
    count = 2*count;
  }

  if n != count {
    // region does not match the file
    throw new Error("count mismatch in readFastaFileSequence");
  }
}

/* Computes the size of a file. Handles fasta files specially to compute the
   size of the nucleotide data only. */
proc computeFileSize(path: string) throws {
  if isFastaFile(path) {
    return computeFastaFileSize(path);
  } else {
    return getFileSize(path);
  }
}

/* Read the data in a file into a portion of an array. Handles fasta
   files specially to read only the nucleotide data.
   The region should match the file size. */
proc readFileData(path: string,
                  ref data: [] uint(8),
                  region: range,
                  verbose = true) throws
{
  if isFastaFile(path) {
    readFastaFileSequence(path, data, region, verbose);
  } else {
    var r = IO.openReader(path);
    r.readAll(data[region]);
  }
}

/* This function trims away the common portions of the paths
   to make output using the paths more consise. It modifies the 'paths'
   array.
 */
proc trimPaths(ref paths:[] string) {
  var common: string = Path.commonPath(paths);
  if common != "" && !common.endsWith("/") then common += "/";
  for p in paths {
    if p.startsWith(common) {
      p = p[common.size..];
    }
  }
}

/*
 Given a list of files, read in all files into a single array
 and produce several related data items:
   * the array containing all of the data
   * a sorted list of paths
   * a corresponding array of file sizes
   * a corresponding list of offsets where each file starts,
     which, contains an extra entry for the total size

 The resulting arrays will be Block distributed among 'locales'.
 */
proc readAllFiles(const ref files: list(string),
                  locales: [ ] locale,
                  out allData: [] uint(8),
                  out allPaths: [] string,
                  out concisePaths: [] string,
                  out fileSizes: [] int,
                  out fileStarts: [] int,
                  out totalSize: int) throws {
  var locPaths = files.toArray();
  for p in locPaths {
    p = Path.normPath(p);
  }
  sort(locPaths);

  const ByFileDom = makeBlockDomain({0..<locPaths.size}, targetLocales=locales);
  const paths:[ByFileDom] string = forall i in ByFileDom do locPaths[i];
  const nFiles = paths.size;

  if nFiles == 0 {
    throw new Error("no input files provided");
  }

  // compute the size for the concatenated input
  var sizes: [paths.domain] int;
  forall (path, sz) in zip(paths, sizes) {
    sz = computeFileSize(path);
    sz += 1; // add a null byte to separate files
  }

  const fileEnds = + scan sizes;
  const total = fileEnds.last;

  const TextDom = makeBlockDomain({0..<total+INPUT_PADDING},
                                  targetLocales=locales);
  var thetext:[TextDom] uint(8);

  // read each file
  forall (path, sz, end) in zip(paths, sizes, fileEnds) {
    const start = end - sz;
    const count = sz - 1; // we added a null byte above
    readFileData(path, thetext, start..#count);
  }

  // compute fileStarts
  const StartsDom = makeBlockDomain({0..nFiles}, targetLocales=locales);
  var starts:[StartsDom] int;
  starts[0] = 0;
  starts[1..nFiles] = fileEnds;

  // compute trimmed paths
  var tPaths = paths;
  trimPaths(tPaths);

  // return various values
  allData = thetext;
  allPaths = paths;
  concisePaths = tPaths;
  fileSizes = sizes;
  fileStarts = starts;
  totalSize = total;
}

proc offsetToFileIdx(const fileStarts: [] int, offset: int) {
  const fileIdx = bsearch(fileStarts, offset);
  if EXTRA_CHECKS {
    assert(0 <= fileIdx && fileIdx < fileStarts.size);
  }
  return fileIdx;
}

proc printSuffix(offset: int, thetext: [], fileStarts: [] int, lcp: int, amt: int) {
  const end = min(offset + amt, thetext.size);
  for i in offset..<end {
    var ch = thetext[i];
    if 32 <= ch && ch <= 126 { // note: 32 is ' ' and 126 is ~
      // char is OK
    } else {
      ch = 46; // .
    }
    writef("%c", ch);
  }
  const fileIdx = offsetToFileIdx(fileStarts, offset);
  writef(" % 8i f%i lcp%i\n", offset, fileIdx, lcp);
}


proc atomicStoreMinRelaxed(ref dst: atomic int, src: int) {
  // TODO: call atomic store min once issue #22867 is resolved
  var t = dst.read(memoryOrder.relaxed);
  while min(src, t) != t {
    // note: dst.compareExchangeWeak updates 't' if it fails
    // to the current value
    if dst.compareExchangeWeak(t, src, memoryOrder.relaxed) {
      return;
    }
  }
}

proc atomicStoreMaxRelaxed(ref dst: atomic int, src: int) {
  // TODO: call atomic store max once issue #22867 is resolved
  var t = dst.read(memoryOrder.relaxed);
  while max(src, t) != t {
    // note: dst.compareExchangeWeak updates 't' if it fails
    // to the current value
    if dst.compareExchangeWeak(t, src, memoryOrder.relaxed) {
      return;
    }
  }
}


}
