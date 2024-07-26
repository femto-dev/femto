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

use Utility;

// this size * the number of files = the window size
config const STRATEGY="adaptive-lcp";
config const WINDOW_SIZE_RATIO = 2.5;
config const WINDOW_SIZE_OVERRIDE = 0;
config const NSIMILAR_TO_OUTPUT = 200;

// these control the adaptive-lcp
config const MAX_BLOCK_SIZE = 1000;
config const MIN_COMMON = 8;

// helpers to make this code work with suffix array storing offsetAndCached
proc offset(a: integral) {
  return a;
}
proc offset(a: offsetAndCached(?)) {
  return a.offset;
}

record similarity {
  var docA: int; // docA < docB
  var docB: int;
  var score: real;
  var numPrefixes: int;
  var minPrefix: int;
  var maxPrefix: int;
  var sumPrefixes: int;
}
record similarityComparator {
  proc key(a: similarity) {
    return -a.score;
  }
}


proc computeSimilarityAdjacentNoLCP(ref Similarity: [] similarity,
                                    SA: [], thetext: [], fileStarts: [] int)
{
  const n = SA.size;
  var CooccurenceCounts:[Similarity.domain] int;
  var TermCounts:[0..<fileStarts.size] int;

  forall i in SA.domain with (+ reduce CooccurenceCounts,
                              + reduce TermCounts) {
    if 0 < i && i < n - 1 {
      // assume that there is a common prefix between i-1 and i
      // that we are considering a term.
      const docA = offsetToFileIdx(fileStarts, offset(SA[i-1]));
      const docB = offsetToFileIdx(fileStarts, offset(SA[i]));
      // add the contribution to the denominator
      TermCounts[docA] += 1;
      TermCounts[docB] += 1;
      // add the contribution to the numerator
      if docA != docB {
        CooccurenceCounts[flattenTriangular(docA, docB)] += 1;
      }
    }
  }

  // Combine AllPairsSimilarity with SumSqTermCounts to form
  // the cosine similarity.
  forall (elt, cooCount) in zip(Similarity, CooccurenceCounts) {
    const numerator = cooCount: real;
    const denominatorA = sqrt(TermCounts[elt.docA]: real);
    const denominatorB = sqrt(TermCounts[elt.docB]: real);
    elt.score = sqrt(numerator / denominatorA / denominatorB);
  }
}


proc computeSimilarityAdaptiveLCP(ref Similarity: [] similarity,
                                  SA: [], LCP: [], thetext: [],
                                  fileStarts: [] int)
{
  const n = SA.size;
  const nFiles = fileStarts.size;

  // this function uses cosine similarity

  // sum of products of term counts for terms common to docs A and B
  var SumCoTermCounts:[Similarity.domain] real;

  // number of prefixes considered; min, max, and sum
  var NumPrefixes:[Similarity.domain] int;
  var MinPrefixes:[Similarity.domain] int = n+1;
  var MaxPrefixes:[Similarity.domain] int;
  var SumPrefixes:[Similarity.domain] int;

  // will be stored in Similarity[flattenTriangular(docA, docB)].score

  // sum of squares of term counts for each document
  // (used in denominator of cosine similarity)
  var SumSqTermCounts:[0..<nFiles] real;

  const nTasks = computeNumTasks();
  const blockSize = divCeil(n, nTasks);
  const nBlocks = divCeil(n, blockSize);

  var lock: sync bool;

  coforall tid in 0..<nTasks with (+ reduce SumCoTermCounts,
                                   + reduce NumPrefixes,
                                   + reduce SumPrefixes,
                                   + reduce SumSqTermCounts) {
    var myMinPrefixes:[Similarity.domain] int = n+1;
    var myMaxPrefixes:[Similarity.domain] int;

    var taskStart = tid * blockSize;
    var taskEnd = min(taskStart + blockSize - 1, n - 1); // inclusive
    if taskStart < taskEnd {
      var DocToCountsThisBlock:[0..<nFiles] int;

      var cur = taskStart;
      while cur <= taskEnd {
        // pass any positions with LCP < MIN_COMMON
        while cur <= taskEnd && LCP[cur] < MIN_COMMON {
          cur += 1;
        }

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
          // we have reached case 1. continue past any with equal LCP
          cur = lastEqual;
          while cur <= taskEnd && startLCP == LCP[cur] {
            cur += 1;
          }
          // and continue the outer while loop
        } else {
          // cur+t had LCP < startLCP, so go until cur+t-1.
          var end = min(cur+t-1, n-1);

          // consider the block cur..cur+t
          // with the corresponding documents in SA[cur-1..cur+t]
          // these all start with at least startLCP characters.

          const block = cur-1..end;
          cur = end+1;

          var blockLCP = startLCP;
          var minFileIdx = max(int);
          var maxFileIdx = min(int);
          writeln("Block has startLCP=", startLCP);
          for i in block {
            const off = offset(SA[i]);
            const fileIdx = offsetToFileIdx(fileStarts, off);
            minFileIdx = min(minFileIdx, fileIdx);
            maxFileIdx = max(maxFileIdx, fileIdx);

            printSuffix(off, thetext, fileStarts, LCP[i], blockLCP+1);
            // if the match goes beyond a file, limit blockLCP
            // to keep everything within a file
            const nextFileStarts = fileStarts[fileIdx+1];
            if off + blockLCP > nextFileStarts - 1 {
              blockLCP = nextFileStarts - off - 1;
              writeln("reducing blockLCP from ", startLCP, " to ", blockLCP);
            }
          }

          if minFileIdx < maxFileIdx && blockLCP >= MIN_COMMON {
            // count the occurrences within each document
            foreach doc in 0..<nFiles {
              DocToCountsThisBlock[doc] = 0;
            }

            var minFileIdx = max(int);
            var maxFileIdx = min(int);
            for i in block {
              const fileIdx = offsetToFileIdx(fileStarts, offset(SA[i]));
              DocToCountsThisBlock[fileIdx] += 1;

              //printSuffix(offset(SA[i]), thetext, fileStarts, LCP[i], blockLCP+1);
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
                    const idx = flattenTriangular(docA, docB);
                    SumCoTermCounts[idx] += countAr * countBr;
                    NumPrefixes[idx] += 1;
                    MinPrefixes[idx] = min(MinPrefixes[idx], blockLCP);
                    MaxPrefixes[idx] = max(MaxPrefixes[idx], blockLCP);
                    SumPrefixes[idx] += blockLCP;
                  }
                }
              }
            }
          }
        }
      }
    }

    // use a critical section to accumulate myMinPrefixes / myMaxPrefixes
    // into MinPrefixes / MaxPrefixes
    // A workaround for https://github.com/chapel-lang/chapel/issues/25658
    lock.writeEF(true);

    forall (elt, my) in zip(MinPrefixes, myMinPrefixes) {
      elt = min(elt, my);
    }
    forall (elt, my) in zip(MaxPrefixes, myMaxPrefixes) {
      elt = max(elt, my);
    }

    // release the lock
    lock.readFE();
  }

  var SqrtSumSqTermCounts:[0..<nFiles] real = sqrt(SumSqTermCounts);

  // Combine SumCoTermCounts with SumSqTermCounts to form
  // the cosine similarity.
  forall (elt, cooScore, nump, minp, maxp, sump) in
      zip(Similarity, SumCoTermCounts,
          NumPrefixes, MinPrefixes, MaxPrefixes, SumPrefixes) {
    const docA = elt.docA;
    const docB = elt.docB;
    const denom = SqrtSumSqTermCounts[docA] * SqrtSumSqTermCounts[docB];
    elt.score = cooScore / denom;
    elt.numPrefixes = nump;
    elt.minPrefix = minp;
    elt.maxPrefix = maxp;
    elt.sumPrefixes = sump;
  }
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

  var Similarity:[0..<triangleSize(nFiles)] similarity;
  forall (i, j) in {0..<nFiles,0..<nFiles} {
    if i < j {
      ref sim = Similarity[flattenTriangular(i,j)];
      sim.docA = i;
      sim.docB = j;
      // other fields start at 0
    }
  }

  if STRATEGY == "adaptive-lcp" {
    computeSimilarityAdaptiveLCP(Similarity, SA, LCP, allData, fileStarts);
  } else if STRATEGY == "adjacent-nolcp" {
    computeSimilarityAdjacentNoLCP(Similarity, SA, allData, fileStarts);

  } else {
    return false;
  }

  Sort.sort(Similarity, comparator=new similarityComparator());

  // output the top 20 or so
  const nprint = min(Similarity.size, NSIMILAR_TO_OUTPUT);
  for elt in Similarity[0..<nprint] {
    if elt.score > 0 {
      const docAName = allPaths[elt.docA];
      const docBName = allPaths[elt.docB];
      writeln(docAName, " vs ", docBName, " : ", elt.score);
      if elt.numPrefixes {
        writeln("  found ", elt.numPrefixes, " common substrings with lengths:",
                " min ", elt.minPrefix,
                " avg ", elt.sumPrefixes:real / elt.numPrefixes,
                " max ", elt.maxPrefix);
      }
    }
  }

  return true;
}


}
