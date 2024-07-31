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

import Utility.computeNumTasks;

import FileSystem;
import IO;
import List;
import Math.{divCeil, log};
import Path;
import Sort;
import Time;

use Utility;

/* the output directory */
config const output="";

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

  // Set MinUnique[i] = 0 if MinUnique[i] > MinUnique[i+1]
  {
    var nTasks = computeNumTasks();
    // Divide the input into nTasks chunks.
    const blockSize = divCeil(n, nTasks);
    const nBlocks = divCeil(n, blockSize);

    // save MinUnique[i] values for final comparison of each task
    var NextTaskValue:[0..<nTasks] MinUniqueElt;
    forall tid in 0..<nTasks {
      var nextTaskStart = (tid+1) * blockSize;
      if nextTaskStart <= n {
        NextTaskValue[tid] = MinUnique[nextTaskStart];
      }
    }

    coforall tid in 0..<nTasks {
      var taskStart = tid * blockSize;
      var taskEnd = min(taskStart + blockSize - 1, n); // an inclusive bound

      // handle the portion entirely within this task
      for i in taskStart..<taskEnd {
        if MinUnique[i] > MinUnique[i+1] {
          MinUnique[i] = 0;
        }
      }

      // handle the final element
      if MinUnique[taskEnd] > NextTaskValue[tid] {
        MinUnique[taskEnd] = 0;
      }
    }
  }

  return MinUnique;
}

record uniqueStats {
  // default values for these are the identity for accumulating
  var count: int = 0;
  var minLength: int = max(int);
  var maxLength: int = min(int);
  var sumLengths: int = 0;
}

// this operator + allows + reduce on arrays of uniqueStats
// as a workaround for https://github.com/chapel-lang/chapel/issues/25658 .
operator +(x: uniqueStats, y: uniqueStats) {
  var ret: uniqueStats;
  ret.count = x.count + y.count;
  ret.minLength = min(x.minLength, y.minLength);
  ret.maxLength = max(x.maxLength, y.maxLength);
  ret.sumLengths = x.sumLengths + y.sumLengths;

  return ret;
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

  if output == "" {
    writeln("please specify an output directory with --output <dirname>");
    return 1;
  }

  const allData; //: [] uint(8);
  const allPaths; //: [] string;
  const concisePaths; // : [] string
  const fileSizes; //: [] int;
  const fileStarts; //: [] int;
  const totalSize: int;
  readAllFiles(inputFilesList,
               allData=allData,
               allPaths=allPaths,
               concisePaths=concisePaths,
               fileSizes=fileSizes,
               fileStarts=fileStarts,
               totalSize=totalSize);

  writeln("Files are: ", concisePaths);
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

  writeln("Computing unique substrings");
  t.reset();
  t.start();
  var MinUnique = findUnique(SA, LCP, allData, fileStarts);
  t.stop();
  writeln("finding unique substrings took ", t.elapsed(), " seconds");
 
  // compute statistics for each file
  writeln("Computing substring statistics");
  const nFiles = allPaths.size;
  var fileStats:[0..<nFiles] uniqueStats;

  forall (elt,i) in zip(MinUnique, MinUnique.domain) with (+ reduce fileStats) {
    if elt > 0 {
      var amt: uniqueStats;
      amt.count = 1;
      amt.minLength = elt;
      amt.maxLength = elt;
      amt.sumLengths = elt;

      const offset = i;
      const doc = offsetToFileIdx(fileStarts, offset);
      const docStart = fileStarts[doc];
      const docEnd = fileStarts[doc+1];
      const docOffset = offset - docStart;
      if offset + elt < docEnd {
        fileStats[doc] += amt;
      }
    }
  }

  for (path,stats) in zip(concisePaths, fileStats) {
    writeln(path);
    if stats.count == 0 {
      writeln("  found 0 unique substrings");
    } else {
      writeln("  found ", stats.count, " unique substrings with lengths:",
              " min ", stats.minLength,
              " avg ", stats.sumLengths:real / stats.count,
              " max ", stats.maxLength);
    }
  }

  writeln();
  writeln("Outputting minuniq files to ", output);
  if !FileSystem.exists(output) {
    writeln("Creating ", output);
    FileSystem.mkdir(output, parents=true);
  }
  // create the directories for the output
  for shortPath in concisePaths {
    var upath = Path.normPath(output + "/" + shortPath + ".unique");
    var dir = Path.dirname(upath);
    if dir != "" && dir != "." && dir != "/" {
      if !FileSystem.exists(dir) {
        writeln("Creating ", dir);
        FileSystem.mkdir(output, parents=true);
      }
    }
  }

  forall (shortPath, fullPath, doc, stats) in
      zip(concisePaths, allPaths, concisePaths.domain, fileStats)
  {
    if stats.count > 0 {
      const docStart = fileStarts[doc];
      const docEnd = fileStarts[doc+1];

      var upath = Path.normPath(output + "/" + shortPath + ".unique");
      writeln("Writing unique substrings from ", fullPath, " to ", upath);
      var w = IO.openWriter(upath);
      if isFastaFile(fullPath) {
        assert(MinUnique.eltType == uint(8)); // otherwise, update below code
        for i in docStart..<docEnd-1 { // don't write the trailing null byte
          // write > according to the input to help keep
          // the file aligned with the genome data
          if allData[i] == ">".toByte() {
            w.writeByte(">".toByte());
          } else {
            w.writeByte(MinUnique[i]);
          }
        }
      } else {
        w.writeBinary(MinUnique[docStart..<docEnd]);
      }
    }
  }

  /*
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
  }*/

  return true;
}


}
