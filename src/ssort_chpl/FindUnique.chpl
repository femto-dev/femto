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


/* the output directory */
config const output="";

config const removeNearDuplicates = false;
config const minUnique = 10; // discard some near-duplicates if there
                             // aren't this many per file

config const maxDuplicateSteps = 10; // max tries removing near duplicates
config const maxSimilar = 0.98; // maximum similarity score to allow
                                // when processing near-duplicates
config const histogramWidth = 50; // how wide to make histogram chart

// upper-case names for the config constants to better identify them in code
const OUTPUT = output;
const REMOVE_NEAR_DUPLICATES = removeNearDuplicates;
const MIN_UNIQUE = minUnique;
const MAX_DUPLICATE_STEPS = maxDuplicateSteps;
const MAX_SIMILAR = maxSimilar;
const HISTOGRAM_WIDTH = histogramWidth;

param HISTOGRAM_MIN = 0;
param HISTOGRAM_MAX = 255;

import SuffixSort.INPUT_PADDING;
import SuffixSort.EXTRA_CHECKS;
import SuffixSort.{computeSuffixArray,computeSparsePLCP,lookupLCP};
import SuffixSortImpl.offsetAndCached;
import SuffixSimilarity;

import Utility.computeNumTasks;

import FileSystem;
import IO;
import List;
import Math.{divCeil, log};
import Path;
import Sort;
import Time;

use Utility;


// helpers to make this code work with suffix array storing offsetAndCached
proc offset(a: integral) {
  return a;
}
proc offset(a: offsetAndCached(?)) {
  return a.offset;
}

// Consider the range offset..#count which refer to positions in thetext,
// where we have already computed that 'offset' is contained within 'doc'.
// Return a new 'count' such that offset..#count does not cross any file
// boundaries.
inline proc adjustForFileBoundaries(count, offset: int, doc: int,
                                    fileStarts: [] int) {
  const docEnd = fileStarts[doc+1];
  return (min(count:int, docEnd - offset)):count.type;
}

/* Find substrings that are unique to a document.
   Returns a 'record hits' which needs to be sorted
   to remove unnecessary hits.

   If IgnoreDocs[doc] is true, entries from that file will be ignored.
 */
proc findUnique(SA: [], SparsePLCP: [], thetext: [], fileStarts: [] int,
                IgnoreDocs: [] bool, type MinUniqueEltType=uint(8))
{
  const n = SA.size;
  // We always compare i vs i+1 so we allocate an extra element in MinUnique
  var MinUnique:[0..n] MinUniqueEltType;
  param MAX_STORE = max(MinUniqueEltType);
  const nIgnore = + reduce (IgnoreDocs:int);
  const numFiles = fileStarts.size - 1;
  const numFilesNotIgnored = numFiles - nIgnore;

  // This algorithm is based on MinUnique-LeftEnd from
  // "Minimum Unique Substring and Maximum Repeats" by Ilie and Smyth,
  // with four modifications:
  //  1. It is parallel
  //  2. When there are multiple documents, it considers something a
  //     unique substring only if it occurs in only one document,
  //     which it achieves by ignoring matches within a document
  //  3. It can ignore certain documents
  //  4. It truncates LCP entries to the document boundaries

  // The MinUnique-LeftEnd algorithm works by computing the
  // MinUnique[SA[i]] = 1 + maximum common prefix shared by another suffix
  //                  = 1 + max(LCP[i], LCP[i+1])
  //
  // Here we extend this idea to consider the longest common prefix shared by
  // SA[prev],SA[i] or SA[i],SA[next], with prev < i and i < next.
  // Here 'prev' is the nearest earlier suffix array position referring
  // to a different document from SA[i]; and similarly 'next' is the
  // nearest later suffix array position referring to a different document from
  // SA[i].

  // If the number of files considered (and not ignored) is <= 1,
  // it uses prev=i-1 and next=i+1 to find the minimal unique substrings
  // as described in Illie & Smyth.

  forall i in SA.domain with (ref MinUnique) {
    // What is j such that SA[i..j] refers to a range
    // of suffixes that come from only one file?
    const off = offset(SA[i]);
    const doc = offsetToFileIdx(fileStarts, off);

    if !IgnoreDocs[doc] {
      // Find the position of the previous entry for a different document
      var prev = i-1;
      var prevOffset = -1;
      var prevDoc = -1;
      while prev >= 0 {
        prevOffset = offset(SA[prev]);
        prevDoc = offsetToFileIdx(fileStarts, prevOffset);
        if prevDoc != doc && !IgnoreDocs[prevDoc] {
          break; // found another doc we aren't ignoring
        }
        if numFilesNotIgnored <= 1 {
          break; // don't consider multiple documents in this case
        }
        prev -= 1;
      }
      // note: now, prev, prevOffset, and prevDoc might be -1

      // Find the position of the next entry for a different document
      var next = i+1;
      var nextOffset = n;
      var nextDoc = numFiles;
      while next < n {
        nextOffset = offset(SA[next]);
        nextDoc = offsetToFileIdx(fileStarts, nextOffset);
        if nextDoc != doc && !IgnoreDocs[nextDoc] {
          break; // found another doc we aren't ignoring
        }
        if numFilesNotIgnored <= 1 {
          break; // don't consider multiple documents in this case
        }
        next += 1;
      }
      // note: now, next,nextOffset might be n, and nextDoc might be numFiles

      // compute the longest common prefix between prev and i
      // this is the minimum value of LCP[prev+1..i]
      // (because the LCP array stores the longest common prefix between
      //  SA[i-1] and SA[i])
      var prevPrefix = max(int);
      for j in prev+1..i {
        const LCPj = lookupLCP(thetext, n, SA, SparsePLCP, j);
        prevPrefix = min(prevPrefix, LCPj);
      }
      // reduce prevPrefix to account for file boundaries
      prevPrefix = adjustForFileBoundaries(prevPrefix, prevOffset, prevDoc,
                                           fileStarts);

      // compute the longest common prefix between i and next
      // this is the minimum value of LCP[i+1..next]
      var nextPrefix = max(int);
      for j in i+1..next {
        var LCPj = 0;
        if j < n {
          LCPj = lookupLCP(thetext, n, SA, SparsePLCP, j);
        }
        nextPrefix = min(nextPrefix, LCPj);
      }
      // reduce nextPrefix to account for file boundaries
      if nextDoc < numFiles && nextOffset < n {
        nextPrefix = adjustForFileBoundaries(nextPrefix, nextOffset, nextDoc,
                                             fileStarts);
      }

      // compute the maximum the two prefixes
      var commonPrefix = max(prevPrefix, nextPrefix);

      // reduce commonPrefix to account for file boundaries
      commonPrefix = adjustForFileBoundaries(commonPrefix, off, doc,
                                             fileStarts);

      const uniqueLen = commonPrefix + 1;
      if uniqueLen <= MAX_STORE {
        MinUnique[offset(SA[i])] = uniqueLen:MinUniqueEltType;
      }
    }
  }

  if numFiles > 1 {
    // Assume there is a padding byte at the end of each document.
    // we should not exceed the padding byte in terms of min unique strings.
    // We set this to 1 so that the next step doesn't rule out a preceding
    // unique byte.
    forall doc in 0..<numFiles {
      MinUnique[fileStarts[doc+1]-1] = 1;
    }
  }

  // Set MinUnique[i] = 0 if MinUnique[i] > MinUnique[i+1]
  {
    var nTasks = computeNumTasks();
    // Divide the input into nTasks chunks.
    const blockSize = divCeil(n, nTasks);
    const nBlocks = divCeil(n, blockSize);

    // save MinUnique[i] values for final comparison of each task
    var NextTaskValue:[0..<nTasks] MinUniqueEltType;
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
        } else if numFiles > 1 && i < n {
          // TODO: binary search not necessary here
          const doc = offsetToFileIdx(fileStarts, i);
          MinUnique[i] = adjustForFileBoundaries(MinUnique[i], i+1, doc,
                                                 fileStarts);
        }
      }

      // handle the final element
      if MinUnique[taskEnd] > NextTaskValue[tid] {
        MinUnique[taskEnd] = 0;
      } else if numFiles > 1 && taskEnd < n {
        const doc = offsetToFileIdx(fileStarts, taskEnd);
        MinUnique[taskEnd] =
          adjustForFileBoundaries(MinUnique[taskEnd], taskEnd+1, doc,
                                  fileStarts);
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
  // histogram of lengths
  var histogram: HISTOGRAM_MAX * int;
}

// this operator + allows + reduce on arrays of uniqueStats
// as a workaround for https://github.com/chapel-lang/chapel/issues/25658 .
operator +(x: uniqueStats, y: uniqueStats) {
  var ret: uniqueStats;
  ret.count = x.count + y.count;
  ret.minLength = min(x.minLength, y.minLength);
  ret.maxLength = max(x.maxLength, y.maxLength);
  ret.sumLengths = x.sumLengths + y.sumLengths;
  ret.histogram += x.histogram + y.histogram;

  return ret;
}

// accumulate histogram information
proc ref uniqueStats.add(length: int) {
  this.count += 1;
  this.minLength = min(length, this.minLength);
  this.maxLength = max(length, this.maxLength);
  this.sumLengths += length;

  // add 1 to the appropriate histogram bin
  param nBins = this.histogram.size;
  var bin = min(length, nBins-1);
  histogram[bin] += 1;
}

// Returns MinUnique
// OutputUniqueForFile[doc] is true if unique strings should be output
// for that file.
// NumUnique has the number of unique substrings for each document.
proc runFindUnique(SA: [], SparsePLCP: [], thetext: [], fileStarts: [] int,
                   concisePaths: [] string,
                   ref IgnoreDocs: [0..<concisePaths.size] bool,
                   ref NumUnique: [0..<concisePaths.size] int,
                   ref FileStats: [0..<concisePaths.size] uniqueStats) {
  // initially, IgnoreDocs[doc] is false for all docs

  writeln("Computing unique substrings");

  var t: Time.stopwatch;
  t.reset();
  t.start();
  const MinUnique = findUnique(SA, SparsePLCP, thetext, fileStarts, IgnoreDocs);
  t.stop();
  writeln("finding unique substrings took ", t.elapsed(), " seconds");

  // compute statistics for each file
  const nFiles = concisePaths.size;
  // reset FileStats
  FileStats = new uniqueStats();

  forall (elt,i) in zip(MinUnique, MinUnique.domain) with (+ reduce FileStats)
  {
    if elt > 0 {
      const offset = i;
      const doc = offsetToFileIdx(fileStarts, offset);
      const docStart = fileStarts[doc];
      const docEnd = fileStarts[doc+1];
      const docOffset = offset - docStart;
      if offset + elt < docEnd {
        FileStats[doc].add(elt);
      }
    }
  }

  // reset NumUnique
  NumUnique = max(int); // to make it easy to check numUnique < MIN_UNIQUE
                        // this way, ignored documents arent near-duplicate

  // Set NumUnique
  forall (stats,ignore,nUnique) in zip(FileStats, IgnoreDocs, NumUnique) {
    if !ignore {
      nUnique = stats.count;
    }
  }

  return MinUnique;
}

proc printStats(concisePaths: [] string,
                IgnoreDocs: [0..<concisePaths.size] bool,
                FileStats: [0..<concisePaths.size] uniqueStats) {
  for (path,stats,ignore) in zip(concisePaths, FileStats, IgnoreDocs) {
    if !ignore {
      writeln(path);
      if stats.count == 0 {
        writeln("  found 0 unique substrings");
      } else {
        writeln("  found ", stats.count, " unique substrings with lengths:",
                " min ", stats.minLength,
                " avg ", stats.sumLengths:real / stats.count,
                " max ", stats.maxLength);

        var lastPrintedPercent = 0.0;
        var accum = 0;
        const countR = stats.count: real;
        param nBins = stats.histogram.size;
        for bin in 0..<nBins-1 {
          accum += stats.histogram[bin];
          var ratio = accum / countR;
          var percent = 100.0 * ratio;
          if percent > lastPrintedPercent + 1.0 {
            var nStars = round(ratio*HISTOGRAM_WIDTH):int;
            writef("    len < %3i ", bin);
            for i in 0..<nStars {
              write("*");
            }
            writef("  %{##.##}%%\n", percent);
            lastPrintedPercent = percent;
          }
        }
        writeln();
      }
    }
  }

  writeln();
}

proc writeOutput(MinUnique: [], IgnoreDocs: [], FileStats: [],
                 allData: [], allPaths: [],
                 concisePaths: [], fileStarts: []) throws {

  printStats(concisePaths, IgnoreDocs, FileStats);

  if output == "" {
    writeln("Not saving the unique substrings since " +
            "an output directory was not provided.");
    writeln("You can specify an output directory with --output <dirname>");
    return;
  }

  writeln("Outputting minuniq files to ", output);
  if !FileSystem.exists(output) {
    writeln("Creating ", output);
    FileSystem.mkdir(output, parents=true);
  }
  // create the directories for the output
  for (shortPath, ignoreDoc) in zip(concisePaths, IgnoreDocs) {
    if !ignoreDoc {
      var upath = Path.normPath(output + "/" + shortPath + ".unique");
      var dir = Path.dirname(upath);
      if dir != "" && dir != "." && dir != "/" {
        if !FileSystem.exists(dir) {
          writeln("Creating ", dir);
          FileSystem.mkdir(output, parents=true);
        }
      }
    }
  }

  forall (shortPath, fullPath, doc, ignoreDoc) in
      zip(concisePaths, allPaths, concisePaths.domain, IgnoreDocs)
  {
    if !ignoreDoc {
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
}

proc niaveClustering() {
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
  const SA = computeSuffixArray(allData, totalSize);
  t.stop();

  writeln("suffix array construction of ", totalSize, " bytes ",
          "took ", t.elapsed(), " seconds");
  writeln(totalSize / 1000.0 / 1000.0 / t.elapsed(), " MB/s");

  writeln("Computing Sparse PLCP array");
  t.reset();
  t.start();
  const SparsePLCP = computeSparsePLCP(allData, totalSize, SA);
  t.stop();
  writeln("Sparse PLCP array construction took ", t.elapsed(), " seconds");

  const nFiles = concisePaths.size;
  var IgnoreDocs: [0..<nFiles] bool;
  var NumUnique: [0..<nFiles] int;
  var FileStats: [0..<nFiles] uniqueStats;
  {
    const MinUnique =
      runFindUnique(SA, SparsePLCP, allData, fileStarts, concisePaths,
                    IgnoreDocs, NumUnique, FileStats);

    const nNearDuplicates = + reduce ((NumUnique < MIN_UNIQUE):int);
    if nNearDuplicates== 0 || MAX_DUPLICATE_STEPS==1 || !REMOVE_NEAR_DUPLICATES
    {
      writeOutput(MinUnique, IgnoreDocs, FileStats,
                  allData, allPaths, concisePaths, fileStarts);
      return 0;
    }
  }

  // compute document similarity
  writeln("Computing similarity to help ignoring near duplicates");
  t.reset();
  t.start();
  const Similarity =
    SuffixSimilarity.computeSimilarity(SA, SparsePLCP, allData, fileStarts);
  t.stop();
  writeln("computing similarity took ", t.elapsed(), " seconds");

  for retry in 2..MAX_DUPLICATE_STEPS {
    // remove some of the near duplicates
    writeln();
    var nNearDuplicates = + reduce ((NumUnique < MIN_UNIQUE):int);
    writeln("Ignoring some of the ", nNearDuplicates, " near duplicates");

    // Gather the document with the maximum score (and the maximum score)
    // for each near duplicate. When doing this, don't consider near
    // duplicates (since, it might be that two near duplicates are
    // causing each other not to have any unique substrings, but removing
    // one of them will resolve the situation)
    var MaximumNearDuplicateScore: [0..<nFiles] (real, int, int);
    // stores (score, doc, otherDoc)

    forall (tup, nu, doc, ignore) in
        zip(MaximumNearDuplicateScore, NumUnique, 0..<nFiles, IgnoreDocs) {
      if nu < MIN_UNIQUE && !ignore {
        var myScore = 0.0;
        var otherDocHighestScore = doc;
        for otherDoc in 0..<nFiles {
          if doc != otherDoc && !IgnoreDocs[otherDoc] &&
             NumUnique[otherDoc] >= MIN_UNIQUE {
            // gather information for other docs we aren't ignoring
            // that aren't near-duplicates
            const ref sim = Similarity[flattenTriangular(doc,otherDoc)];
            if sim.score > myScore {
              myScore = sim.score;
              otherDocHighestScore = otherDoc;
            }
          }
        }
        tup[0] = myScore;
        tup[1] = doc;
        tup[2] = otherDocHighestScore;
      }
    }

    Sort.sort(MaximumNearDuplicateScore, new Sort.ReverseComparator());

    var half = nNearDuplicates/2;
    half = max(1, half);
    var nRemoved = 0;
    // ignore the other doc for the first half of MaximumNearDuplicateScore.
    for i in 0..<half {
      var (score, doc, otherDoc) = MaximumNearDuplicateScore[i];
      if doc != otherDoc {
        writeln("Ignoring near duplicate ", concisePaths[doc],
                " (with only ", NumUnique[doc], " unique substrings)",
                " because it is closely related to ", concisePaths[otherDoc],
                " (similarity score ", score, ")");
        IgnoreDocs[doc] = true;
        nRemoved += 1;
      }
    }

    if nRemoved == 0 {
      // remove one of the near duplicates with the lowest number of unique
      // strings
      const (minVal, minIdx) = minloc reduce zip(NumUnique, NumUnique.domain);
      writeln("Ignoring one near duplicate ", concisePaths[minIdx],
              " (with ", NumUnique[minIdx], " unique substrings)");
      IgnoreDocs[minIdx] = true;
      nRemoved += 1;
    }

    // compute the minimal unique substrings with the new IgnoreDocs
    const MinUnique = runFindUnique(SA, SparsePLCP, allData,
                                    fileStarts, concisePaths,
                                    IgnoreDocs, NumUnique, FileStats);

    // output if appropriate
    nNearDuplicates = + reduce ((NumUnique < MIN_UNIQUE):int);
    if nNearDuplicates == 0 || nRemoved == 0 || retry == MAX_DUPLICATE_STEPS {

      writeln();

      for (doc, ignore, shortPath) in
          zip(IgnoreDocs.domain, IgnoreDocs, concisePaths) {
        if ignore {
          writeln("Ignored near duplicate ", shortPath);
        }
      }
      writeln();

      writeOutput(MinUnique, IgnoreDocs, FileStats,
                  allData, allPaths, concisePaths, fileStarts);
      return 0;
    }
  }

  return 0;
}


}
