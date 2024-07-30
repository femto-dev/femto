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
import FileSystem;
import FileSystem.{isFile, isDir, findFiles, getFileSize};
import IO;
import OS.EofError;
import List.list;
import Sort.{sort,isSorted};
import SuffixSort.{EXTRA_CHECKS, INPUT_PADDING};

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
        count += 1;
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
  return count;
}

/* Reads a the sequence portion of a fasta file into a region of an array.
   The resulting array elements will contain a > at the start of each sequence
   followed by the nucleotide data. The whitespace and sequence
   descriptions are removed.
   The region size should match 'computeFastaFileSize'. */
proc readFastaFileSequence(path: string,
                           ref data: [] uint(8),
                           region: range) throws
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
        writeln("Reading sequence ", desc);
      }
      if inDescLine {
        desc.appendCodepointValues(byte);
      } else if isspace(byte) == 0 {
        if count < n {
          data[dataStart + count] = byte;
        }
        count += 1;
      }
    } catch e: EofError {
      break;
    }
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
                  region: range) throws
{
  if isFastaFile(path) {
    readFastaFileSequence(path, data, region);
  } else {
    var r = IO.openReader(path);
    r.readAll(data[region]);
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
 */
proc readAllFiles(const ref files: list(string),
                  out allData: [] uint(8),
                  out allPaths: [] string,
                  out fileSizes: [] int,
                  out fileStarts: [] int,
                  out totalSize: int) throws {
  var paths = files.toArray();
  sort(paths);

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

  var thetext:[0..<total+INPUT_PADDING] uint(8);

  // read each file
  forall (path, sz, end) in zip(paths, sizes, fileEnds) {
    const start = end - sz;
    const count = sz - 1; // we added a null byte above
    readFileData(path, thetext, start..#count);
  }

  // compute fileStarts
  var starts:[0..nFiles] int;
  starts[0] = 0;
  starts[1..nFiles] = fileEnds;

  // return various values
  allData = thetext;
  allPaths = paths;
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

proc testTriangles() {
  writeln("testTriangles");

  assert(triangleSize(0) == 0);
  assert(triangleSize(1) == 0);

  /*
     - -
     0 -
   */
  assert(triangleSize(2) == 1);
  assert(flattenTriangular(1,0) == 0);
  assert(flattenTriangular(0,1) == 0);

  /*
     - - -
     0 - -
     1 2 -
   */
  assert(triangleSize(3) == 3);
  assert(flattenTriangular(1,0) == 0);
  assert(flattenTriangular(0,1) == 0);

  assert(flattenTriangular(2,0) == 1);
  assert(flattenTriangular(0,2) == 1);
  assert(flattenTriangular(2,1) == 2);
  assert(flattenTriangular(1,2) == 2);

  /*
     - - - -
     0 - - -
     1 2 - -
     3 4 5 -
   */

  assert(triangleSize(4) == 6);
  assert(flattenTriangular(1,0) == 0);
  assert(flattenTriangular(0,1) == 0);

  assert(flattenTriangular(2,0) == 1);
  assert(flattenTriangular(0,2) == 1);
  assert(flattenTriangular(2,1) == 2);
  assert(flattenTriangular(1,2) == 2);

  assert(flattenTriangular(3,0) == 3);
  assert(flattenTriangular(0,3) == 3);
  assert(flattenTriangular(3,1) == 4);
  assert(flattenTriangular(1,3) == 4);
  assert(flattenTriangular(3,2) == 5);
  assert(flattenTriangular(2,3) == 5);
}

proc testBsearch() {
  writeln("testBsearch");

  assert(bsearch([1], 0) == -1);
  assert(bsearch([1], 1) == 0);
  assert(bsearch([1], 2) == 0);

  assert(bsearch([1,3], 0) == -1);
  assert(bsearch([1,3], 1) == 0);
  assert(bsearch([1,3], 2) == 0);
  assert(bsearch([1,3], 3) == 1);
  assert(bsearch([1,3], 4) == 1);
}

private proc arrToString(arr: [] uint(8)) {
  var result: string;
  for elt in arr {
    result.appendCodepointValues(elt);
  }
  return result;
}

proc testFastaFiles() throws {
  writeln("testFastaFiles");
  var fileContents = "> test \t seq\nA\n\rC\tG  TTA\nGGT\n\n\nA\n> seq 2\nCCG";
  var expect = ">ACGTTAGGTA>CCG";
  var n = expect.size;
  var filename = "tmp-testFastaFiles-test.fna";
  {
    var w = IO.openWriter(filename);
    w.write(fileContents);
  }
  {
    assert(computeFastaFileSize(filename) == n);
    assert(computeFileSize(filename) == n);
    var A: [0..n+1] uint(8);
    readFastaFileSequence(filename, A, 1..n);
    assert(A[0] == 0);
    assert(A[n+1] == 0);
    var str = arrToString(A[1..n]);
    assert(str == expect);

    A = 0;
    readFileData(filename, A, 1..n);
    assert(A[0] == 0);
    assert(A[n+1] == 0);
    var str2 = arrToString(A[1..n]);
    assert(str2 == expect);
  }

  FileSystem.remove(filename);
}

proc main() throws {
  testTriangles();
  testBsearch();
  testFastaFiles();
}


}
