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

import Utility.computeNumTasks;

import FileSystem;
import IO;
import List;
import Sort;
import Time;
import Math.{divCeil, log};

use Utility;

// this size * the number of files = the window size
config const STRATEGY="block-lcp";
config const WINDOW_SIZE_RATIO = 2.5;
config const WINDOW_SIZE_OVERRIDE = 0;
config const NSIMILAR_TO_OUTPUT = 200;

// these control adaptive-lcp and block-lcp
config const MAX_BLOCK_SIZE = 1000;
config const MIN_COMMON = 60;
config const MAX_OCCURRENCES = 1000;
config const TARGET_BLOCK_SIZE = 1000;

// helpers to make this code work with suffix array storing offsetAndCached
proc offset(a: integral) {
  return a;
}
proc offset(a: offsetAndCached(?)) {
  return a.offset;
}

record similarity {
  // assumption: docA < docB
  var docA: int = -1;
  var docB: int = -1;

  // default values for these are the identity for accumulating
  var score: real = 0.0;
  var numPrefixes: int = 0;
  var minPrefix: int = max(int);
  var maxPrefix: int = min(int);
  var sumPrefixes: int = 0;
}

// this operator + allows + reduce on arrays of similarity
// as a workaround for https://github.com/chapel-lang/chapel/issues/25658 .
operator +(x: similarity, y: similarity) {
  var ret: similarity;
  if EXTRA_CHECKS {
    if x.docA != -1 && y.docA != -1 then assert(x.docA == y.docA);
    if x.docB != -1 && y.docB != -1 then assert(x.docB == y.docB);
  }
  ret.docA = if x.docA != -1 then x.docA else y.docA;
  ret.docB = if x.docB != -1 then x.docB else y.docB;

  ret.score = x.score + y.score;
  ret.numPrefixes = x.numPrefixes + y.numPrefixes;
  ret.minPrefix = min(x.minPrefix, y.minPrefix);
  ret.maxPrefix = max(x.maxPrefix, y.maxPrefix);
  ret.sumPrefixes = x.sumPrefixes + y.sumPrefixes;

  return ret;
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
          //writeln("Block has startLCP=", startLCP);
          for i in block {
            const off = offset(SA[i]);
            const fileIdx = offsetToFileIdx(fileStarts, off);
            minFileIdx = min(minFileIdx, fileIdx);
            maxFileIdx = max(maxFileIdx, fileIdx);

            //printSuffix(off, thetext, fileStarts, LCP[i], blockLCP+1);
            // if the match goes beyond a file, limit blockLCP
            // to keep everything within a file
            const nextFileStarts = fileStarts[fileIdx+1];
            if off + blockLCP > nextFileStarts - 1 {
              blockLCP = nextFileStarts - off - 1;
              //writeln("reducing blockLCP from ", startLCP, " to ", blockLCP);
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

proc computeSimilarityBlockLCP(ref Similarity: [] similarity,
                               SA: [], LCP: [], thetext: [],
                               fileStarts: [] int,
                               targetBlockSize: int,
                               minCommonStrLen: int)
{
  writeln("in computeSimilarityBlockLCP");
  // The idea of this function is to account for long similar
  // sequences as well as shorter ones.
  //
  // It works by finding blocks in the SA / LCP arrays that
  // represent common substrings (suffixes with a common prefix)
  // and then it considers the LCP relationships between the
  // files in that block.
  //
  // In the first phase, the block boundaries are determined as
  // the minimum LCP value within a window.

  // The scoring in this function is working with
  // cosine similarity with notional term frequencies per document.
  // The term frequency for a common substring of C characters is:
  //   C * Count / DocSufArrSize
  // Where an n-character document's suffix array size is n(n+1)/2.
  //
  // When adding up scores, we are working with two documents which
  // have different lengths, so we use
  //
  //   C * CountA / DocSufArrSizeA + C * CountB / DocSufArrSizeB
  //
  // To avoid dividing on each addition, we put these with a common
  // denominator:
  //
  //   C * CountA * DocSufArrSizeB + C * CountB * DocSufArrSizeA
  //   ---------------------------------------------------------
  //   DocSufArrSizeA * DocSufArrSizeB


  const nFiles = fileStarts.size-1;
  const n = SA.size;
  const nBlocks = divCeil(n, targetBlockSize);
  const blockSize = divCeil(n, nBlocks);
  var Boundaries:[0..nBlocks+1] int;
  forall blockIdx in 0..<nBlocks {
    const blockStart = blockIdx * blockSize;
    const blockEnd = min(blockStart + blockSize - 1, n - 1); // inclusive

    // find the index of the minimum LCP value in blockStart..blockEnd
    const (minVal, minIdx) =
      minloc reduce zip(LCP[blockStart..blockEnd], blockStart..blockEnd);

    // store it in Boundaries
    Boundaries[blockIdx+1] = max(1, minIdx);
  }
  Boundaries[0] = 1;
  // Boundaries[1] ... Boundaries[nBlocks] computed above
  Boundaries[nBlocks+1] = n;


  writeln("Computed Boundaries");

  // Initialize arrays to store the scores

  // sum of products of term counts for terms common to docs A and B
  // information about substrings common to docs A and B
  // uses AccumCoOccurences[flattenTriangular(docA, docB)]
  var AccumCoOccurences:[Similarity.domain] similarity = Similarity;

  // sum of squares of term counts for each document
  // (used in denominator of cosine similarity)
  var SumSqTermCounts:[0..<nFiles] real;

  var FileSufArrSize:[0..<nFiles] real;
  forall doc in 0..<nFiles {
    const fileSize = (fileStarts[doc+1] - fileStarts[doc]): real;
    FileSufArrSize[doc] = fileSize * (fileSize+1.0) / 2.0;
  }

  writeln("Examining Blocks");

  // consider each block, including the potentially short 0th
  // and nBlock'th blocks.
  forall blockIdx in 0..nBlocks with (+ reduce AccumCoOccurences,
                                      + reduce SumSqTermCounts) {
    const blockStart = Boundaries[blockIdx] + 1;
    const blockEnd = Boundaries[blockIdx+1] - 1; // inclusive
    const block = blockStart..blockEnd;
    //writeln("Working on block ", blockIdx, " with range ", block);

    // compute the minimum LCP in this block
    var minLCP = min reduce LCP[blockStart..blockEnd];
    minLCP = max(0, minLCP);

    // limit minLCP to avoid crossing file boundaries
    for i in block {
      const off = offset(SA[i]);
      const doc = offsetToFileIdx(fileStarts, off);
      const nextFileStarts = fileStarts[doc+1];
      minLCP = min(minLCP, nextFileStarts - off - 1);
    }

    var DocToCountsThisBlock:[0..<nFiles] int = 0;

    /*writeln("Block has minLCP=", minLCP);
    for i in blockStart-1..blockEnd+1 {
      if 0 <= i && i < n {
        write("i", i, " ");
        printSuffix(offset(SA[i]), thetext, fileStarts, LCP[i], minLCP+1);
      }
    }*/

    // Loop over the block to
    //  1. Count the number of times each file occurs in the block
    //  2. Compute the score contribution from adjacent suffixes
    for i in block {

      // LCP indicates the common prefix between SA[i-1] and SA[i]
      var curLCP = LCP[i];

      const curOff = offset(SA[i]);
      const curDoc = offsetToFileIdx(fileStarts, curOff);
      const curDocEnds = fileStarts[curDoc+1];

      const prevOff = offset(SA[i-1]);
      const prevDoc = offsetToFileIdx(fileStarts, prevOff);
      const prevDocEnds = fileStarts[prevDoc+1];

      // Count the number of times each file occurs in the block
      DocToCountsThisBlock[curDoc] += 1;

      // Limit curLCP to avoid crossing file boundaries
      curLCP = min(curLCP, curDocEnds - curOff - 1, prevDocEnds - prevOff - 1);

      if curLCP > minLCP && curLCP >= minCommonStrLen && curDoc != prevDoc {
        // ### accumulate score information for adjacent suffixes ###
        const docA = min(curDoc, prevDoc);
        const docB = max(curDoc, prevDoc);
        const lcpR = curLCP: real;
        //const score = SqFileSize[docB] * lcpR + SqFileSize[docA] * lcpR;
        //const score = lcpR * (SqFileSize[docA] + SqFileSize[docB]);
        //const score = lcpR;
        const score = lcpR * (FileSufArrSize[docB] + FileSufArrSize[docA]);
        var amt: similarity;
        amt.docA = docA;
        amt.docB = docB;
        amt.score = score;
        amt.numPrefixes = 1;
        amt.minPrefix = curLCP;
        amt.maxPrefix = curLCP;
        amt.sumPrefixes = curLCP;

        AccumCoOccurences[flattenTriangular(docA, docB)] += amt;

        // add the contribution to the denominator
        //const sqScore = score * score;
        //SumSqTermCounts[docA] += sqScore;
        //SumSqTermCounts[docB] += sqScore;
      }
    }

    // ### accumulate score information for being together in the block ###
    if minLCP >= minCommonStrLen {
      // compute the contribution to the denominator
      /*foreach doc in 0..<nFiles {
        const count = DocToCountsThisBlock[doc];
        if count > 0 {
          //const score = minLCP: real * DocToCountsThisBlock[doc];
          const score = DocToCountsThisBlock[doc] : real;
          SumSqTermCounts[doc] += score*score;
        }
      }*/

      // compute the contribution to the numerator
      for docA in 0..<nFiles {
        const countA = DocToCountsThisBlock[docA];
        if countA > 0 {
          for docB in docA+1..<nFiles {
            const countB = DocToCountsThisBlock[docB];
            if countB > 0 {
              const lcpR = minLCP : real;
              //const score = lcpR * countA * SqFileSize[docB] +
              //              lcpR * countB * SqFileSize[docA];
              //const score = lcpR * (countA * SqFileSize[docB] +
              //                      countB * SqFileSize[docA]);
              //const score = lcpR * (countA + countB);
              const score = lcpR * (countA * FileSufArrSize[docB] +
                                    countB * FileSufArrSize[docA]);
              var amt: similarity;
              amt.docA = docA;
              amt.docB = docB;
              amt.score = score;
              amt.numPrefixes = min(countA, countB);
              amt.minPrefix = minLCP;
              amt.maxPrefix = minLCP;
              amt.sumPrefixes = minLCP;

              AccumCoOccurences[flattenTriangular(docA, docB)] += amt;
            }
          }
        }
      }
    }
  }

  writeln("Computing Score");

  // Combine AccumCoOccurences with SumSqTermCounts to form
  // the cosine similarity and store that in Similarity.
  var SqrtSumSqTermCounts:[0..<nFiles] real = sqrt(min(1, SumSqTermCounts));

  forall (elt, cooScore) in zip(Similarity, AccumCoOccurences) {
    assert(elt.docA == cooScore.docA);
    assert(elt.docB == cooScore.docB);
    const docA = elt.docA;
    const docB = elt.docB;
    const fileSizeA = (fileStarts[docA+1] - fileStarts[docA]):real;
    const fileSizeB = (fileStarts[docB+1] - fileStarts[docB]):real;
    const sumSizes = fileSizeA + fileSizeB;
    //const denom = (sumSizes * (sumSizes+1)) / 2.0;
    //const denom = SqFileSize[docA] * SqFileSize[docB];
    const denom = 2.0 * FileSufArrSize[docA] * FileSufArrSize[docB];
    elt.score = sqrt(cooScore.score / denom);
    //elt.score = cooScore.score / denom;
    elt.numPrefixes = cooScore.numPrefixes;
    elt.minPrefix = cooScore.minPrefix;
    elt.maxPrefix = cooScore.maxPrefix;
    elt.sumPrefixes = cooScore.sumPrefixes;
  }
}

inline proc computeScore(minLCP: int,
                         docA: int, docB: int,
                         countA: int, countB: int,
                         const ref FileSufArrSize: [] real) {
  const lcpR = minLCP : real;
  //const score = lcpR * countA * SqFileSize[docB] +
  //              lcpR * countB * SqFileSize[docA];
  //const score = lcpR * (countA * SqFileSize[docB] +
  //                      countB * SqFileSize[docA]);
  //const score = lcpR * (countA + countB);
  //const score = lcpR * (countA * FileSufArrSize[docB] +
  //                      countB * FileSufArrSize[docA]);
  const score = lcpR * (countA * FileSufArrSize[docB] +
                        countB * FileSufArrSize[docA]);

  var amt: similarity;
  amt.docA = docA;
  amt.docB = docB;
  amt.score = score;
  amt.numPrefixes = min(countA, countB);
  amt.minPrefix = minLCP;
  amt.maxPrefix = minLCP;
  amt.sumPrefixes = minLCP;

  return amt;
}

proc computeSimilarityRecursive(block: range,
                                ref AccumCoOccurences: [] similarity,
                                const ref FileSufArrSize: [] real,
                                minLCP: int,
                                SA: [], LCP: [], thetext: [],
                                fileStarts: [] int) {

  /*writeln("computeSimilarityRecursive(", block, ", minLCP=", minLCP, ")");
  {
    var blockStart = block.low;
    var blockEnd = block.high;
    const n = SA.size;
    var nPrint = min(minLCP+1, 10);
    for i in blockStart-1..blockEnd+1 {
      if 0 <= i && i < n {
        write("i", i, " ");
        if !block.contains(i) {
          write("|");
          var lcp = if LCP.domain.contains(i) then LCP[i] else 0;
          printSuffix(offset(SA[i]), thetext, fileStarts, lcp, nPrint);
        } else {
          printSuffix(offset(SA[i]), thetext, fileStarts, LCP[i], nPrint);
        }
      }
    }
  }*/

  const nFiles = fileStarts.size-1;

  // base case: block size is 0
  if block.size == 0 {
    return;
  }

  // base case: block size is 1
  if block.size == 1 {
    const i = block.low;
    const offA = offset(SA[i - 1]);
    const docA = offsetToFileIdx(fileStarts, offA);
    const offB = offset(SA[i]);
    const docB = offsetToFileIdx(fileStarts, offB);

    if docA != docB && minLCP >= MIN_COMMON {
      if EXTRA_CHECKS {
        assert(minLCP == LCP[block.low]);
      }

      var nUniqueDocsThisBlock = 2;
      const amt = computeScore(minLCP, docA, docB, 1, 1, FileSufArrSize);
      AccumCoOccurences[flattenTriangular(docA, docB)] += amt;
    }
    return;
  }

  // #### loop over the block to gather information ####
  //  1. Find the next smallest LCP for recursion
  //  2. Count the number of times each file occurs in the block
  var splitLCP = max(int);
  var DocToCountsThisBlock:[0..<nFiles] int = 0;

  // since block is in LCP entries, which indicate commonality
  // between SA[i-1] and SA[i], the first common thing is actually
  // SA[block.low-1] so count that first
  if block.size > 0 {
    const curOff = offset(SA[block.low - 1]);
    const curDoc = offsetToFileIdx(fileStarts, curOff);
    // Count the number of times each file occurs in the block
    DocToCountsThisBlock[curDoc] += 1;
  }

  for i in block {
    // LCP indicates the common prefix between SA[i-1] and SA[i]

    // compute the next smallest LCP and its position for recursion
    const curLCP = LCP[i];
    if curLCP > minLCP && curLCP < splitLCP {
      splitLCP = curLCP;
    }

    const curOff = offset(SA[i]);
    const curDoc = offsetToFileIdx(fileStarts, curOff);
    // Count the number of times each file occurs in the block
    DocToCountsThisBlock[curDoc] += 1;
  }

  var nUniqueDocsThisBlock = 0;
  for count in DocToCountsThisBlock {
    if count > 0 {
      nUniqueDocsThisBlock += 1;
    }
  }

  // ### accumulate score information for being together in the block ###
  if nUniqueDocsThisBlock >= 2 &&
     minLCP >= MIN_COMMON &&
     block.size <= MAX_OCCURRENCES {
    // compute the contribution to the numerator
    for docA in 0..<nFiles {
      const countA = DocToCountsThisBlock[docA];
      if countA > 0 {
        for docB in docA+1..<nFiles {
          const countB = DocToCountsThisBlock[docB];
          if countB > 0 {
            const amt = computeScore(minLCP, docA, docB, countA, countB,
                                     FileSufArrSize);
            AccumCoOccurences[flattenTriangular(docA, docB)] += amt;
          }
        }
      }
    }
  }

  // #### recurse with sub-blocks with LCP >= splitLCP ####

  if splitLCP != max(int) && splitLCP > minLCP {
    var cur = block.low;
    var end = block.high+1;
    while cur < end {
      // pass any with LCP < splitLCP
      while cur < end && LCP[cur] < splitLCP {
        cur += 1;
      }

      if cur < end && LCP[cur] >= splitLCP {
        // pass any with LCP >= splitLCP
        var count = 0;
        while cur+count < end && LCP[cur+count] >= splitLCP {
          count += 1;
        }
        // now we can recurse on cur..#count
        computeSimilarityRecursive(cur..#count, AccumCoOccurences,
                                   FileSufArrSize, splitLCP,
                                   SA, LCP, thetext, fileStarts);
        // now consider the next region
        cur = cur + count;
      }
    }
  }
}


proc computeSimilarityRecursiveLCP(ref Similarity: [] similarity,
                                   SA: [], LCP: [], thetext: [],
                                   fileStarts: [] int)
{
  writeln("in computeSimilarityRecursiveLCP");
  // The idea of this function is to account for long similar
  // sequences as well as shorter ones.
  //
  // It works by finding blocks in the SA / LCP arrays that
  // represent common substrings (suffixes with a common prefix)
  // and then it considers the LCP relationships between the
  // files in that block.
  //
  // In the first phase, the block boundaries are determined as
  // the minimum LCP value within a window. This enables
  // parallel processing in the later phases and enforces
  // that only substrings with up to MAX_OCCURENCES are considered.

  // The scoring in this function is working with
  // cosine similarity with notional term frequencies per document.
  // The term frequency for a common substring of C characters is:
  //   C * Count / DocSufArrSize
  // Where an n-character document's suffix array size is n(n+1)/2.
  //
  // When adding up scores, we are working with two documents which
  // have different lengths, so we use
  //
  //   C * CountA / DocSufArrSizeA + C * CountB / DocSufArrSizeB
  //
  // To avoid dividing on each addition, we put these with a common
  // denominator:
  //
  //   C * CountA * DocSufArrSizeB + C * CountB * DocSufArrSizeA
  //   ---------------------------------------------------------
  //   DocSufArrSizeA * DocSufArrSizeB


  const nFiles = fileStarts.size-1;
  const n = SA.size;
  const nBlocks = divCeil(n, MAX_OCCURRENCES/2);
  // minimum-to-minimum blocks are < MAX_OCCURENCES long
  const blockSize = divCeil(n, nBlocks);
  var Boundaries:[0..nBlocks+1] int;
  forall blockIdx in 0..<nBlocks {
    const blockStart = blockIdx * blockSize;
    const blockEnd = min(blockStart + blockSize - 1, n - 1); // inclusive

    // find the index of the minimum LCP value in blockStart..blockEnd
    const (minVal, minIdx) =
      minloc reduce zip(LCP[blockStart..blockEnd], blockStart..blockEnd);

    // store it in Boundaries
    Boundaries[blockIdx+1] = max(1, minIdx);
  }
  Boundaries[0] = 1;
  // Boundaries[1] ... Boundaries[nBlocks] computed above
  Boundaries[nBlocks+1] = n;


  writeln("Computed Boundaries");

  // Initialize arrays to store the scores

  // sum of products of term counts for terms common to docs A and B
  // information about substrings common to docs A and B
  // uses AccumCoOccurences[flattenTriangular(docA, docB)]
  var AccumCoOccurences:[Similarity.domain] similarity = Similarity;

  // sum of squares of term counts for each document
  // (used in denominator of cosine similarity)
  var SumSqTermCounts:[0..<nFiles] real;

  var FileSufArrSize:[0..<nFiles] real;
  forall doc in 0..<nFiles {
    const fileSize = (fileStarts[doc+1] - fileStarts[doc]): real;
    FileSufArrSize[doc] = fileSize * (fileSize+1.0) / 2.0;
  }

  writeln("Examining Blocks");

  // consider each block, including the potentially short 0th
  // and nBlock'th blocks.
  forall blockIdx in 0..nBlocks with (+ reduce AccumCoOccurences,
                                      + reduce SumSqTermCounts) {
    const blockStart = Boundaries[blockIdx] + 1;
    const blockEnd = Boundaries[blockIdx+1] - 1; // inclusive
    const block = blockStart..blockEnd;
    //writeln("Working on block ", blockIdx, " with range ", block);

    // Compute the full LCP for this block and limit anything
    // crossing file boundaries
    const biggerEnd = min(blockEnd+1, n-1);
    const ext = blockStart-1..biggerEnd;
    var BlockLCP:[ext] int;
    for (blockLCPElt, saElt, lcpElt) in zip(BlockLCP, SA[ext], LCP[ext]) {
      const off = offset(saElt);
      const doc = offsetToFileIdx(fileStarts, off);
      const nextFileStarts = fileStarts[doc+1];
      blockLCPElt = min(lcpElt, nextFileStarts - off - 1);
    }

    var DocToCountsThisBlock:[0..<nFiles] int = 0;

    // compute the minimum LCP in this block
    var minLCP = min reduce LCP[block];
    minLCP = max(0, minLCP);

    computeSimilarityRecursive(block, AccumCoOccurences,
                               FileSufArrSize, minLCP,
                               SA, BlockLCP, thetext, fileStarts);
  }

  writeln("Computing Score");

  // Combine AccumCoOccurences with SumSqTermCounts to form
  // the cosine similarity and store that in Similarity.
  var SqrtSumSqTermCounts:[0..<nFiles] real = sqrt(min(1, SumSqTermCounts));

  forall (elt, cooScore) in zip(Similarity, AccumCoOccurences) {
    assert(elt.docA == cooScore.docA);
    assert(elt.docB == cooScore.docB);
    const docA = elt.docA;
    const docB = elt.docB;
    const fileSizeA = (fileStarts[docA+1] - fileStarts[docA]):real;
    const fileSizeB = (fileStarts[docB+1] - fileStarts[docB]):real;
    const sumSizes = fileSizeA + fileSizeB;
    //const denom = (sumSizes * (sumSizes+1)) / 2.0;
    //const denom = SqFileSize[docA] * SqFileSize[docB];
    //writeln("Raw score between f", docA, ", f", docB, " ", cooScore);
    const denom = 2.0 * FileSufArrSize[docA] * FileSufArrSize[docB];
    elt.score = sqrt(cooScore.score / denom);
    //elt.score = cooScore.score / denom;
    elt.numPrefixes = cooScore.numPrefixes;
    elt.minPrefix = cooScore.minPrefix;
    elt.maxPrefix = cooScore.maxPrefix;
    elt.sumPrefixes = cooScore.sumPrefixes;
  }
}

/* Compute all-to-all similarity and return an array representing it.
   The returned array should be accessed with flattenTriangular(i,j)
   for i and j in 0..<fileStarts.size-1 */
proc computeSimilarity(SA: [], LCP: [], thetext: [],
                       fileStarts: [] int,
                       targetBlockSize: int = TARGET_BLOCK_SIZE,
                       minCommonStrLen: int = MIN_COMMON) {
  const nFiles = fileStarts.size-1;
  var Similarity:[0..<triangleSize(nFiles)] similarity;
  forall (i, j) in {0..<nFiles,0..<nFiles} {
    if i < j {
      ref sim = Similarity[flattenTriangular(i,j)];
      sim.docA = i;
      sim.docB = j;
      // other fields start at 0
    }
  }

  computeSimilarityBlockLCP(Similarity, SA, LCP, thetext, fileStarts,
                            targetBlockSize, minCommonStrLen);

  return Similarity;
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
  const concisePaths; //: [] string;
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
  //var SA = computeSuffixArray(allData, totalSize);
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

  t.reset();
  t.start();

  if STRATEGY == "recursive-lcp" {
    computeSimilarityRecursiveLCP(Similarity, SA, LCP, allData, fileStarts);
  } else if STRATEGY == "block-lcp" {
    computeSimilarityBlockLCP(Similarity, SA, LCP, allData, fileStarts,
                              TARGET_BLOCK_SIZE, MIN_COMMON);
  } else if STRATEGY == "adaptive-lcp" {
    computeSimilarityAdaptiveLCP(Similarity, SA, LCP, allData, fileStarts);
  } else if STRATEGY == "adjacent-nolcp" {
    computeSimilarityAdjacentNoLCP(Similarity, SA, allData, fileStarts);
  } else {
    return false;
  }

  Sort.sort(Similarity, comparator=new similarityComparator());

  t.stop();

  writeln("similarity computation took ", t.elapsed(), " seconds");


  // output the top 20 or so
  const nprint = min(Similarity.size, NSIMILAR_TO_OUTPUT);
  for elt in Similarity[0..<nprint] {
    if elt.score > 0 {
      const docAName = concisePaths[elt.docA];
      const docBName = concisePaths[elt.docB];
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
