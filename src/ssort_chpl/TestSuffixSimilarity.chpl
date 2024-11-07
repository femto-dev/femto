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

  femto/src/ssort_chpl/TestSuffixSimilarity.chpl
*/

module TestSuffixSimilarity {


use SuffixSimilarity;
use Utility;
import SuffixSort.{computeSuffixArray, computeSparsePLCP, INPUT_PADDING};
import Random;

config const debugOutput = false;

proc testDuplicates(docLen:int, nDocs: int) {
  writeln("testDuplicates(", docLen, ",", nDocs, ")");

  assert(docLen > 1);
  assert(nDocs > 1);

  // note: this code assumes docLen includes a null byte
  const n = docLen * nDocs;

  var allData: [0..<n+INPUT_PADDING] uint(8);
  var allPaths: [0..<nDocs] string;
  var concisePaths: [0..<nDocs] string;
  var fileSizes: [0..<nDocs] int;
  var fileStarts: [0..nDocs] int;
  var totalSize: int = n;

  // set up each file
  forall doc in 0..<nDocs {
    const docStart = doc*docLen;
    forall i in 0..<docLen {
      allData[docStart+i] = (i%10 + 97):uint(8);
    }
    // make sure last byte in each doc is 0, to separate
    allData[docStart+docLen-1] = 0;

    allPaths[doc] = "t" + (doc:string);
    concisePaths[doc] = allPaths[doc];
    fileSizes[doc] = docLen;
    fileStarts[doc] = docStart;
  }
  fileStarts[nDocs] = n;

  // compute the suffix array
  const SA = computeSuffixArray(allData, totalSize);
  if debugOutput {
    writeln("allData");
    writeln(allData);
    writeln("SA");
    writeln(SA);
  }

  // compute the sparse PLCP array
  const SparsePLCP = computeSparsePLCP(allData, n, SA);

  // compute the similarity
  const Similarity = computeSimilarity(SA, SparsePLCP, allData, fileStarts,
                                       targetBlockSize = max(2, docLen / 2),
                                       minCommonStrLen = max(1, docLen / 4));

  for (i, j) in {0..<nDocs,0..<nDocs} {
    if i < j {
      var sim = Similarity[flattenTriangular(i,j)];
      if debugOutput {
        writeln("score for (", i, " vs ", j, ") = ", sim.score);
      }
      assert(sim.score > 0.3);
    }
  }
}

proc testRandom(docLen:int, nDocs: int) {
  writeln("testRandom(", docLen, ",", nDocs, ")");

  assert(docLen > 1);
  assert(nDocs > 1);

  // note: this code assumes docLen includes a null byte
  const n = docLen * nDocs;

  var allData: [0..<n+INPUT_PADDING] uint(8);
  var allPaths: [0..<nDocs] string;
  var concisePaths: [0..<nDocs] string;
  var fileSizes: [0..<nDocs] int;
  var fileStarts: [0..nDocs] int;
  var totalSize: int = n;

  Random.fillRandom(allData, seed=17);

  // set up each file
  forall doc in 0..<nDocs {
    const docStart = doc*docLen;
    // make sure last byte in each doc is 0, to separate
    allData[docStart+docLen-1] = 0;

    allPaths[doc] = "t" + (doc:string);
    concisePaths[doc] = allPaths[doc];
    fileSizes[doc] = docLen;
    fileStarts[doc] = docStart;
  }
  fileStarts[nDocs] = n;

  // compute the suffix array
  const SA = computeSuffixArray(allData, totalSize);
  if debugOutput {
    writeln("allData");
    writeln(allData);
    writeln("SA");
    writeln(SA);
  }

  // compute the sparse PLCP array
  const SparsePLCP = computeSparsePLCP(allData, n, SA);

  // compute the similarity
  const Similarity = computeSimilarity(SA, SparsePLCP, allData, fileStarts,
                                       targetBlockSize = max(2, docLen / 2),
                                       minCommonStrLen = max(1, docLen / 4));

  for (i, j) in {0..<nDocs,0..<nDocs} {
    if i < j {
      var sim = Similarity[flattenTriangular(i,j)];
      if debugOutput {
        writeln("score for (", i, " vs ", j, ") = ", sim.score);
      }
      assert(sim.score < 0.1);
    }
  }
}


proc runTests() {
  testDuplicates(3, 2);
  testDuplicates(10, 2);
  testDuplicates(10, 3);
  testDuplicates(10, 4);
  testRandom(10, 2);
  testRandom(10, 3);
  testRandom(10, 4);
}


// Run the tests
proc main() {
  serial {
    runTests();
  }

  runTests();

  writeln("OK");
}


}
