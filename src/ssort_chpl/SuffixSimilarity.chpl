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

  femto/src/ssort_chpl/SuffixSimilarity.chpl
*/

module SuffixSimilarity {


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
import Math.divCeil;

// this size * the number of files = the window size
config const STRATEGY="cosine";
config const WINDOW_SIZE_RATIO = 2.5;
config const WINDOW_SIZE_OVERRIDE = 0;
config const NSIMILAR_TO_OUTPUT = 200;
config const MAX_BLOCK_SIZE = 1000;

// helpers to make this code work with suffix array storing offsetAndCached
proc offset(a: integral) {
  return a;
}
proc offset(a: offsetAndCached(?)) {
  return a.offset;
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
    assert(Sort.isSorted(arr));
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

proc offsetToFileIdx(const fileStarts: [] int, offset: int) {
  const fileIdx = bsearch(fileStarts, offset);
  if EXTRA_CHECKS {
    assert(0 <= fileIdx && fileIdx < fileStarts.size);
  }
  return fileIdx;
}

proc printSuffix(offset: int, thetext: [], fileStarts: [] int, lcp: int) {
  const end = min(offset + 10, thetext.size);
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

proc printWindow(windowCounts: [] int) {
  write("window ");
  for i in windowCounts.domain {
    write("f", i, "=", windowCounts[i], " ");
  }
  writeln();
}

proc slidingWindowPush(ref windowCounts: [] int,
                       offset: int,
                       fileStarts: [] int) {
  const fileIdx = offsetToFileIdx(fileStarts, offset);
  windowCounts[fileIdx] += 1;
  //write("    pushing ", offset, " f", fileIdx, " then ");
  //printWindow(windowCounts);
}
proc slidingWindowPop(ref AllPairsSimilarity: [] int,
                      ref windowCounts: [] int,
                      offset: int,
                      fileStarts: [] int,
                      checkWindowSize: int) {
  const fileIdx = offsetToFileIdx(fileStarts, offset);
  if EXTRA_CHECKS {
    assert(fileCounts[fileIdx] > 0);
  }

  // give a point for (fileIdx, otherIdx) for all other files
  // in the window with this file.
  var total = 0;
  for (count, otherIdx) in zip(windowCounts, windowCounts.domain) {
    total += count;
    if count > 0 {
      // A variant of this ignores the count
      var a = min(fileIdx, otherIdx);
      var b = max(fileIdx, otherIdx);
      AllPairsSimilarity[a, b] += count;
    }
  }
  if EXTRA_CHECKS && checkWindowSize != 0 {
    assert(total == checkWindowSize);
  }

  windowCounts[fileIdx] -= 1;

  //write("    popping ", offset, " f", fileIdx, " then ");
  //printWindow(windowCounts);
}

proc computeSimilaritySlidingWindow(SA: [], fileStarts: [] int, thetext: [],
                                    out windowSize: int) {
  const n = SA.size;
  const nFiles = fileStarts.size;
  var AllPairsSimilarity:[0..<nFiles, 0..<nFiles] int;

  /*for i in SA.domain {
    printSuffix(offset(SA[i]), thetext, fileStarts);
  }*/

  windowSize = 2;
  if WINDOW_SIZE_OVERRIDE > 1 {
    windowSize = WINDOW_SIZE_OVERRIDE;
  } else
    windowSize = (WINDOW_SIZE_RATIO * nFiles):int;

  const nTasks = computeNumTasks();
  writeln("nTasks is ", nTasks);
  writeln("windowSize is ", windowSize);
  const blockSize = divCeil(n, nTasks);
  const nBlocks = divCeil(n, blockSize);

  coforall tid in 0..<nTasks with (+ reduce AllPairsSimilarity) {
    var taskStart = tid * blockSize;
    var taskEnd = min(taskStart + blockSize - 1, n - 1); // inclusive

    // ignore the first windowSize
    if taskStart < windowSize then taskStart = windowSize;
    //if taskEnd > n - windowSize then taskEnd = n - windowSize;

    if 0 <= taskStart && taskStart < taskEnd && taskEnd < n {
      var windowCounts:[fileStarts.domain] int;

      // warm-up the sliding window
      var warmupStart = taskStart - windowSize;
      for i in warmupStart..taskStart-1 {
        //writeln("warmup ", offset(SA[i]));
        slidingWindowPush(windowCounts, offset(SA[i]), fileStarts);
      }

      for i in taskStart..taskEnd {
        slidingWindowPop(AllPairsSimilarity, windowCounts,
                         offset(SA[i-windowSize]),
                         fileStarts, windowSize);
        //printSuffix(offset(SA[i]), thetext, fileStarts);

        slidingWindowPush(windowCounts, offset(SA[i]), fileStarts);
      }

      // wind down the sliding window
      /*var cooldownEnd = taskEnd + windowSize - 1;
      for i in taskEnd+1..cooldownEnd {
        writeln("cooldown ", offset(SA[i]));
        slidingWindowPop(AllPairsSimilarity, windowCounts,
                         offset(SA[i-windowSize]),
                         fileStarts, 0);
      }*/
    }
  }

  return AllPairsSimilarity;
}

proc computeSimilarityAdj(SA: [], fileStarts: [] int, thetext: [],
                          out windowSize: int) {
  const n = SA.size;
  const nFiles = fileStarts.size;
  var AllPairsSimilarity:[0..<nFiles, 0..<nFiles] int;

  windowSize = 2;

  /*
  for i in SA.domain {
    printSuffix(offset(SA[i]), thetext, fileStarts);
  }*/

  forall i in SA.domain with (+ reduce AllPairsSimilarity) {
    if i > 0 {
      const prevFileIdx = offsetToFileIdx(fileStarts, offset(SA[i-1]));
      const fileIdx = offsetToFileIdx(fileStarts, offset(SA[i]));
      const a = min(fileIdx, prevFileIdx);
      const b = max(fileIdx, prevFileIdx);
      AllPairsSimilarity[a, b] += 1;
    }
  }

  return AllPairsSimilarity;
}

proc computeSimilarityLCP(SA: [], LCP: [], thetext: [], fileStarts: [] int) {
  const n = SA.size;
  const nFiles = fileStarts.size;

  // this function uses cosine similarity

  // sum of products of term counts for terms common to docs A and B
  var AllPairsSimilarity:[0..<nFiles, 0..<nFiles] real;

  // sum of squares of term counts for each document
  // (used in denominator of cosine similarity)
  var SumSqTermCounts:[0..<nFiles] real;

  /*for i in SA.domain {
    printSuffix(offset(SA[i]), thetext, fileStarts, LCP[i]);
  }*/

  const nTasks = computeNumTasks();
  writeln("nTasks is ", nTasks);
  const blockSize = divCeil(n, nTasks);
  const nBlocks = divCeil(n, blockSize);

  coforall tid in 0..<nTasks with (+ reduce AllPairsSimilarity) {
    var taskStart = tid * blockSize;
    var taskEnd = min(taskStart + blockSize - 1, n - 1); // inclusive
    if taskStart < taskEnd {
      var DocToCountsThisBlock:[0..<nFiles] int;

      var cur = taskStart;
      while cur <= taskEnd {
        //writeln("in main loop cur is ", cur);
        // pass any positions with LCP == 0
        while cur <= taskEnd && LCP[cur] == 0 {
          cur += 1;
        }
        //writeln("skipped zeros, cur is ", cur);

        // consider the LCP. There are two cases:
        // 1. There are too many things starting with this prefix.
        //    In that case, just advance to the next prefix with an LCP
        //    that is larger that the current one. The current
        //    prefix is too common to be considered a term.
        // 2. There are a small number of things starting with this prefix.
        //    We can consider it to be a term because it is not too common.
        //    Gather term frequencies for the region until the LCP is smaller.
        const startLCP = LCP[cur];
        var t = 1;
        var lastEqual = cur;
        while cur+t <= taskEnd && t < MAX_BLOCK_SIZE {
          //writeln("in loop with t ", t);
          const curLCP = LCP[cur+t];
          if curLCP < startLCP {
            break; // we found the end of the prefix
          }
          if curLCP == startLCP {
            lastEqual = cur + t;
          }
          t += 1;
        }
        if t >= MAX_BLOCK_SIZE {
          //writeln("reached MAX_BLOCK_SIZE");

          // we have reached case 1. continue past any with equal LCP
          cur = lastEqual;
          while cur <= taskEnd && startLCP == LCP[cur] {
            cur += 1;
          }
          // and continue the outer while loop
        } else {
          // cur+t had LCP < startLCP, so go until cur+t-1.
          var end = min(cur+t-1, n-1);
          //writeln("considering block ", cur-1..end, " with LCP ", startLCP);

          // consider the block cur..cur+t
          // with the corresponding documents in SA[cur-1..cur+t]
          // these all start with at least startLCP characters.

          const block = cur-1..end;
          cur = end+1;

          // count the occurrences within each document
          foreach doc in 0..<nFiles {
            DocToCountsThisBlock[doc] = 0;
          }

          for i in block {
            //printSuffix(offset(SA[i]), thetext, fileStarts, LCP[i]);
            const fileIdx = offsetToFileIdx(fileStarts, offset(SA[i]));
            DocToCountsThisBlock[fileIdx] += 1;
          }

          // compute the contribution to the denominator
          foreach doc in 0..<nFiles {
            const count = (DocToCountsThisBlock[doc]):real;
            SumSqTermCounts[doc] += count*count;
          }

          // compute the contribution to the numerator
          for docA in 0..<nFiles {
            const countA = DocToCountsThisBlock[docA];
            if countA > 0 {
              for docB in docA+1..<nFiles {
                const countB = DocToCountsThisBlock[docB];
                if countB > 0 {
                  const countAr = countA:real;
                  const countBr = countB:real;
                  AllPairsSimilarity[docA, docB] += countAr * countBr;
                }
              }
            }
          }
        }
      }
    }
  }

  var SqrtSumSqTermCounts:[0..<nFiles] real = sqrt(SumSqTermCounts);

  // Combine AllPairsSimilarity with SumSqTermCounts to form
  // the cosine similarity.
  forall (elt, (docA, docB)) in zip(AllPairsSimilarity,
                                    AllPairsSimilarity.domain) {
    if docA < docB {
      elt /= SqrtSumSqTermCounts[docA] * SqrtSumSqTermCounts[docB];
    }
  }

  return AllPairsSimilarity;
}


proc main(args: [] string) throws {
  var inputFilesList: List.list(string);

  for arg in args[1..] {
    if arg.startsWith("-") {
      halt("argument not handled ", arg);
    }
    if FileSystem.isFile(arg) {
      inputFilesList.pushBack(arg);
    }
    if FileSystem.isDir(arg) {
      writeln("dir walk not handled yet");
      // TODO: why doesn't this work? file issue
      /*for path in FileSystem.walkDirs(arg, followlinks = true) {
        if FileSystem.isFile(arg) {
          inputFilesList.pushBack(arg);
        }
      }*/
    }
  }

  var inputFiles = inputFilesList.toArray();
  Sort.sort(inputFiles);
  const nInputFiles = inputFiles.size;

  if nInputFiles == 0 {
    writeln("please specify input files and directories");
    return 1;
  }

  writeln("Working with ", nInputFiles, " input files:");
  writeln(inputFiles);

  // compute the size for the concatenated input
  var fileSizes: [inputFiles.domain] int;
  forall (f,sz) in zip(inputFiles,fileSizes) {
    sz = FileSystem.getFileSize(f);
    sz += 1; // add a null byte to separate files
  }

  const fileEnds = + scan fileSizes;
  const totalSize = fileEnds.last;

  writeln("Total size is ", totalSize);

  var thetext:[0..<totalSize+INPUT_PADDING] uint(8);

  // read each file
  forall (path,sz,end) in zip(inputFiles, fileSizes, fileEnds) {
    writeln("Reading in ", sz, " bytes from ", path);
    var f = IO.open(path, IO.ioMode.r);
    const start = end - sz;
    f.reader().readAll(thetext[start..#sz]);
  }

  var fileStarts:[0..nInputFiles] int;
  fileStarts[0] = 0;
  fileStarts[1..nInputFiles] = fileEnds;

  var t: Time.stopwatch;

  writeln("Computing suffix array");
  t.reset();
  t.start();
  //var SA = computeSuffixArray(thetext, totalSize);
  const SA, LCP;
  computeSuffixArrayAndLCP(thetext, totalSize, SA, LCP);
  t.stop();

  writeln("suffix array construction took of ", totalSize, " bytes ",
          "took ", t.elapsed(), " seconds");
  writeln(totalSize / 1000.0 / 1000.0 / t.elapsed(), " MB/s");

  if STRATEGY == "adjacent" || STRATEGY == "a" ||
     STRATEGY == "window" || STRATEGY == "w" {
    handleSlidingStrategies(SA, thetext, fileSizes, fileStarts, inputFiles);
  } else if STRATEGY == "cosine" {
    handleCosineStrategies(SA, LCP, thetext, fileStarts, inputFiles);
  } else {
    throw new Error("Unknown strategy '" + STRATEGY + "'");
  }

  return 0;
}

proc handleSlidingStrategies(SA: [], thetext: [],
                             fileSizes: [] int, fileStarts: [] int,
                             inputFiles: [] string) {
  const nInputFiles = fileSizes.size;
  var windowSize = 2;
  const SimilarityInts;
  if STRATEGY == "adjacent" || STRATEGY == "a" {
    SimilarityInts = computeSimilarityAdj(SA, fileStarts, thetext, windowSize);
  } else if STRATEGY == "window" || STRATEGY == "w" {
    SimilarityInts = computeSimilaritySlidingWindow(SA, fileStarts, thetext,
                                                    windowSize);
  } else {
    return;
  }

  const total = (+ reduce SimilarityInts) : real;
  record similarity {
    var i: int;
    var j: int;
    var rawScore: int;
    var score: real;
  }
  var SimilarityScores: [0..nInputFiles*nInputFiles] similarity;
  forall (i, j) in SimilarityInts.domain {
    if i < j && i < nInputFiles && j < nInputFiles {
      const maximumCount = max(1, min(fileSizes[i], fileSizes[j]));
      const denom = maximumCount * windowSize;
      SimilarityScores[i*nInputFiles + j] =
        new similarity(i, j, SimilarityInts[i,j],
                       SimilarityInts[i,j]:real / denom);
    }
  }
  record similarityComparator {
    proc key(a: similarity) {
      return -a.score;
    }
  }
  Sort.sort(SimilarityScores, comparator=new similarityComparator());
  // output the top 20 or so
  const nprint = min(SimilarityScores.size, NSIMILAR_TO_OUTPUT);
  for elt in SimilarityScores[0..<nprint] {
    if elt.score > 0 {
      writeln(inputFiles[elt.i], " vs ", inputFiles[elt.j], " : ", elt.score,
              "  (raw: ", elt.rawScore, ")");
    }
  }
}

proc handleCosineStrategies(SA: [], LCP: [], thetext: [],  fileStarts: [] int,
                            inputFiles: [] string) {
  const nFiles = inputFiles.size;
  const AllPairsSimilarity = computeSimilarityLCP(SA, LCP, thetext, fileStarts);

  record similarity2 {
    var i: int;
    var j: int;
    var score: real;
  }
  var SimilarityScores: [0..nFiles*nFiles] similarity2;
  forall (i, j) in AllPairsSimilarity.domain {
    if i < j && i < nFiles && j < nFiles {
      SimilarityScores[i*nFiles + j] =
        new similarity2(i, j, AllPairsSimilarity[i,j]);
    }
  }
  record similarityComparator2 {
    proc key(a: similarity2) {
      return -a.score;
    }
  }
  Sort.sort(SimilarityScores, comparator=new similarityComparator2());
  // output the top 20 or so
  const nprint = min(SimilarityScores.size, NSIMILAR_TO_OUTPUT);
  for elt in SimilarityScores[0..<nprint] {
    if elt.score > 0 {
      writeln(inputFiles[elt.i], " vs ", inputFiles[elt.j], " : ", elt.score);
    }
  }
}


}
