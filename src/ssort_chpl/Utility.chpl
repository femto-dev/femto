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


import SuffixSort.{EXTRA_CHECKS, INPUT_PADDING};
import Sort.{sort,isSorted};
import FileSystem.{isFile, isDir, findFiles, getFileSize};
import List.list;
import IO;

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
    sz = getFileSize(path);
    sz += 1; // add a null byte to separate files
  }

  const fileEnds = + scan sizes;
  const total = fileEnds.last;

  var thetext:[0..<total+INPUT_PADDING] uint(8);

  // read each file
  forall (path, sz, end) in zip(paths, sizes, fileEnds) {
    var f = IO.open(path, IO.ioMode.r);
    const start = end - sz;
    f.reader().readAll(thetext[start..#sz]);
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
    if 32 <= ch && ch <= 126 {
      // char is OK
    } else {
      ch = 46; // .
    }
    writef("%c", ch);
  }
  const fileIdx = offsetToFileIdx(fileStarts, offset);
  writef(" % 8i f%i lcp%i\n", offset, fileIdx, lcp);
}

/*
class ArrayMinReduceOp: ReduceScanOp {
  /* the type of the elements to be reduced -- expecting [] int */
  type eltType;

  /* task-private accumulator/reduction state */
  var value: eltType;

  /* identity w.r.t. the reduction operation */
  proc identity {
    var ret: eltType;
    ret = max(ret.eltType);
    return ret;
  }

  /* accumulate a single element onto the accumulator */
  proc accumulate(elm)  {
    foreach (vElt, aElt) in zip(value, elm) {
      vElt = min(vElt, aElt);
    }
  }

  /* accumulate a single element onto the state */
  proc accumulateOntoState(ref state, elm)  {
    foreach (sElt, aElt) in zip(state, elm) {
      sElt = min(sElt, aElt);
    }
  }

  // Note: 'this' can be accessed by multiple calls to combine()
  // concurrently. The Chapel implementation serializes such calls
  // with a lock on 'this'.
  // 'other' will not be accessed concurrently.
  /* combine the accumulations in 'this' and 'other' */
  proc combine(other: borrowed ArrayMinReduceOp(?)) {
    foreach (vElt, aElt) in zip(value, other.value) {
      vElt = min(vElt, aElt);
    }
  }

  /* Convert the accumulation into the value of the reduction
     that is reported to the user. */
  proc generate()    do return value;

  /* produce a new instance of this class */
  proc clone()       do return new unmanaged ArrayMinReduceOp(eltType=eltType);
}

class ArrayMaxReduceOp: ReduceScanOp {
  /* the type of the elements to be reduced -- expecting [] int */
  type eltType;

  /* task-private accumulator/reduction state */
  var value: eltType;

  /* identity w.r.t. the reduction operation */
  proc identity {
    var ret: eltType;
    ret = min(ret.eltType);
    return ret;
  }

  /* accumulate a single element onto the accumulator */
  proc accumulate(elm)  {
    foreach (vElt, aElt) in zip(value, elm) {
      vElt = max(vElt, aElt);
    }
  }

  /* accumulate a single element onto the state */
  proc accumulateOntoState(ref state, elm)  {
    foreach (sElt, aElt) in zip(state, elm) {
      sElt = max(sElt, aElt);
    }
  }

  // Note: 'this' can be accessed by multiple calls to combine()
  // concurrently. The Chapel implementation serializes such calls
  // with a lock on 'this'.
  // 'other' will not be accessed concurrently.
  /* combine the accumulations in 'this' and 'other' */
  proc combine(other: borrowed ArrayMaxReduceOp(?)) {
    foreach (vElt, aElt) in zip(value, other.value) {
      vElt = max(vElt, aElt);
    }
  }

  /* Convert the accumulation into the value of the reduction
     that is reported to the user. */
  proc generate()    do return value;

  /* produce a new instance of this class */
  proc clone()       do return new unmanaged ArrayMaxReduceOp(eltType=eltType);
}
*/

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

/*proc testArrayMinReduce() {
  writeln("testArrayMinReduce");

  var TupA = ([2, 3, 9, 1, 7, 4],
              [4, 2, 2, 9, 2, 4],
              [0, 5, 8, 3, 6, 1]);
  var Expect= [0, 2, 2, 1, 2, 1]; 

  var State:[TupA[0].domain] int = max(int);

  forall i in 0..<TupA.size with (ArrayMinReduceOp reduce State) {
    State reduce= TupA[i]; 
  }

  writeln(State);

  assert(&& reduce (State == Expect));
}

proc testArrayMaxReduce() {
  writeln("testArrayMaxReduce");

  var TupA = ([2, 3, 9, 1, 7, 4],
              [4, 2, 2, 9, 2, 4],
              [0, 5, 8, 3, 6, 1]);
  var Expect= [4, 5, 9, 9, 7, 4]; 

  var State:[TupA[0].domain] int = min(int);

  forall i in 0..<TupA.size with (ArrayMaxReduceOp reduce State) {
    State reduce= TupA[i]; 
  }

  assert(&& reduce (State == Expect));
}*/

proc main() {
  testTriangles();
  testBsearch();
  /*testArrayMinReduce();
  testArrayMaxReduce();*/
}


}
