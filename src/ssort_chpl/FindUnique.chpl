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

  femto/src/ssort_chpl/FindUnique.chpl
*/

module FindUnique {


import SuffixSort.INPUT_PADDING;
import SuffixSort.EXTRA_CHECKS;
import SuffixSort.computeSuffixArray;
import SuffixSort.computeSuffixArrayAndLCP;
import SuffixSortImpl.offsetAndCached;

import Partitioning.computeNumTasks;

import FileSystem;
import IO;
import List;
import Sort;
import Time;
import Math.{divCeil, log};

use Utility;

config const MAX_OCCURRENCES = 100;
config const MIN_K = 1;
config const MAX_K = 100;
config const SHOW_MATCHES = true;

// helpers to make this code work with suffix array storing offsetAndCached
proc offset(a: integral) {
  return a;
}
proc offset(a: offsetAndCached(?)) {
  return a.offset;
}

record hit {
  var start: int;
  var size: int;
}

record hits {
  var lst: List.list(hit);
}

// allow + reduce on hits
operator +(x: hits, y: hits) {
  var ret: hits;

  ret.lst.pushBack(x.lst);
  ret.lst.pushBack(y.lst);
  return ret;
}

// compute an LCP value while considering file boundaries
inline proc boundaryLCP(offset: int, lcp: int, fileStarts: [] int) {
  const doc = offsetToFileIdx(fileStarts, offset);
  const docEnd = fileStarts[doc+1];
  return min(lcp, docEnd - offset - 1);
}

/* Find substrings that are unique to a document.
   Returns a 'record hits' which needs to be sorted
   to remove unnecessary hits.
 */
proc findUnique(SA: [], LCP: [], thetext: [], fileStarts: [] int)
{
  const n = SA.size;
  type MinUniqueElt = uint(8);
  var MinUnique:[0..n] MinUniqueElt;
  param MAX_STORE = max(MinUniqueElt);

  forall i in SA.domain with (ref MinUnique) {
    if 0 < i && i < n - 1 {
      // What is j such that SA[i..j] refers to a range
      // of suffixes that come from only one file?
      const off = offset(SA[i]);
      const doc = offsetToFileIdx(fileStarts, off);

      var countSameDocument = 1;
      while countSameDocument < MAX_OCCURRENCES && i + countSameDocument < n {
        const nextOff = offset(SA[i+countSameDocument]);
        const nextDoc = offsetToFileIdx(fileStarts, nextOff);
        if nextDoc != doc {
          break;
        }
        countSameDocument += 1;
      }
      var j = i + countSameDocument - 1;

      // what prefix length would match SA[i-1] or SA[j+1]
      // in addition to SA[i..j] ?
      const curLcp = boundaryLCP(offset(SA[i]), LCP[i], fileStarts);
      const nextLcp = if j+1 < n
                      then boundaryLCP(offset(SA[j+1]), LCP[j+1], fileStarts)
                      else 0;
      const uniqueLen = max(curLcp, nextLcp) + 1;

      if uniqueLen <= MAX_STORE {
        MinUnique[offset(SA[i])] = uniqueLen:MinUniqueElt;
      }
    }
  }

  return MinUnique;
}

proc main(args: [] string) throws {
  var inputFilesList: List.list(string);

  for arg in args[1..] {
    if arg.startsWith("-") {
      halt("argument not handled ", arg);
    }
    gatherFiles(inputFilesList, arg);
  }

  if inputFilesList.size == 0 {
    writeln("please specify input files and directories");
    return 1;
  }

  const allData; //: [] uint(8);
  const allPaths; //: [] string;
  const fileSizes; //: [] int;
  const fileStarts; //: [] int;
  const totalSize: int;
  readAllFiles(inputFilesList,
               allData=allData,
               allPaths=allPaths,
               fileSizes=fileSizes,
               fileStarts=fileStarts,
               totalSize=totalSize);

  writeln("Files are: ", allPaths);
  writeln("FileStarts are: ", fileStarts);

  var t: Time.stopwatch;

  writeln("Computing suffix array with ", computeNumTasks(), " tasks");
  t.reset();
  t.start();
  //var SA = computeSuffixArray(thetext, totalSize);
  const SA, LCP;
  computeSuffixArrayAndLCP(allData, totalSize, SA, LCP);
  t.stop();

  writeln("suffix array construction took of ", totalSize, " bytes ",
          "took ", t.elapsed(), " seconds");
  writeln(totalSize / 1000.0 / 1000.0 / t.elapsed(), " MB/s");

  const nFiles = allPaths.size;

  writeln("Computing unique");
  var MinUnique = findUnique(SA, LCP, allData, fileStarts);

  var curDoc = -1;
  for i in 0..<SA.size {
    const offset = i;
    var sz = MinUnique[i];
    if MinUnique[i] > MinUnique[i+1] {
      sz = 0;
    }

    if sz > 0 {
      const doc = offsetToFileIdx(fileStarts, offset);
      const docStart = fileStarts[doc];
      const docEnd = fileStarts[doc+1];
      const docOffset = offset - docStart;
      if offset + sz < docEnd {
        if curDoc != doc {
          writeln(allPaths[doc]);
          curDoc = doc;
        }
        writeln("  ", sz, " unique chars at offset ", docOffset);
        if SHOW_MATCHES {
          write("    ");
          for i in offset..#sz {
            var ch = allData[i];
            if 32 <= ch && ch <= 126 {
              // char is OK
            } else {
              ch = 46; // .
            }
            writef("%c", ch);
          }
          writeln();
        }
      }
    }
  }

  return true;
}


}
