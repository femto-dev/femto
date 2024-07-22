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

  femto/src/ssort_chpl/SuffixSort.chpl
*/

module SuffixSort {


config param DEFAULT_PERIOD = 133;
config param DEFAULT_K = 81;
config param ENABLE_CACHED_TEXT = true;
config param EXTRA_CHECKS = false;
config param TRACE = false;
config type CACHED_DATA_TYPE = nothing;
config type LOAD_WORD_TYPE = uint;

// how much padding does the algorithm need at the end of the input?
param INPUT_PADDING = 8;


/* TODO after https://github.com/chapel-lang/chapel/issues/25569 is fixed
include public module DifferenceCovers;
include private module SuffixSortImpl;
include private module TestSuffixSort;
include private module TestDifferenceCovers;
*/

public use DifferenceCovers;
private use SuffixSortImpl;

private import IO;
private import Time;

proc computeSuffixArray(input: [], const n: input.domain.idxType) {
  if !(input.domain.rank == 1 &&
       input.domain.low == 0 &&
       input.domain.high == input.domain.size-1) {
    halt("computeSuffixArray requires 1-d array over 0..n");
  }
  if n + INPUT_PADDING > input.size {
    halt("computeSuffixArray needs extra space at the end of the array");
    // expect it to be zero-padded past n.
  }

  const cfg = new ssortConfig(idxType = input.idxType,
                              characterType = input.eltType,
                              offsetType = input.idxType,
                              cachedDataType = CACHED_DATA_TYPE,
                              loadWordType = LOAD_WORD_TYPE,
                              cover = new differenceCover(DEFAULT_PERIOD));

  return ssortDcx(cfg, input, n);
}

/*
proc computeUniqueK(input: [], const n: input.domain.idxType) {
  if !(input.domain.rank == 1 &&
       input.domain.low == 0 &&
       input.domain.high == input.domain.size-1) {
    halt("computeUniqueK requires 1-d array over 0..n");
  }
  if n + INPUT_PADDING > input.size {
    halt("computeUniqueK needs extra space at the end of the array");
    // expect it to be zero-padded past n.
  }

  const cfg = new ssortConfig(idxType = input.idxType,
                              characterType = input.eltType,
                              offsetType = input.idxType,
                              cachedDataType = CACHED_DATA_TYPE,
                              loadWordType = LOAD_WORD_TYPE,
                              cover = new differenceCover(DEFAULT_PERIOD));
  minUniqueK(cfg, input, n, DEFAULT_K);
}*/

config const input:string;

proc main() throws {
  if input == "" {
    writeln("please use --input to specify an input file");
    return 1;
  }

  writeln("Reading in ", input);
  var f = IO.open(input, IO.ioMode.r);
  const n = f.size;
  var thetext:[0..<n+INPUT_PADDING] uint(8);
  f.reader().readAll(thetext);

  var t: Time.stopwatch;

  writeln("Computing suffix array");
  t.reset();
  t.start();
  var SA = computeSuffixArray(thetext, n);
  t.stop();

  writeln("suffix array construction took of ", n, " bytes ",
          "took ", t.elapsed(), " seconds");
  writeln(n / 1000.0 / 1000.0 / t.elapsed(), " MB/s");

  /*
  {
    // Compute LCP
    writeln("Computing LCP");
    var LCP: [SA.domain] SA.eltType.offsetType;
    {
      var PLCP: LCP.type;
      {
        var PHI: LCP.type;
        PHI[SA[0].offset] = -1;
        forall i in SA.domain {
          if i != 0 {
            PHI[SA[i].offset] = SA[i-1].offset;
          }
        }
        var h = 0;
        for i in 0..<n {
          if PHI[i] == -1 {
            h = 0;
          } else {
            while thetext[i+h] == thetext[PHI[i]+h] {
              h += 1;
            }
            if h > 0 then {
              h -= 1;
            }
            PLCP[i] = h;
          }
        }
      }
      forall i in 0..<n {
        LCP[i] = PLCP[SA[i].offset];
      }
    }
    // compute min unique array
    writeln("Computing MinUnique");
    var MinUnique: LCP.type;
    for i in 0..<n {
      const nextLCP = if i+1 < n then LCP[i+1] else 0;
      const lcp = max(LCP[i], nextLCP);
      ref mu = MinUnique[SA[i].offset + lcp];
      mu = max(mu, SA[i].offset);
    }
    /*forall i in 0..<n {
      if i < n-1 {
        MinUnique[SA[i].offset] = 1 + max(LCP[i], LCP[i+1]);
      }
    }*/
    // count the number of minimum unique substrings
    /*var i = 0;
    while i < n-1 && i + MinUnique[i] < n {
      while MinUnique[i] > MinUnique[i+1] {
        MinUnique[i] = 0;
        i += 1;
      }
    }*/

    writeln("Counting");
    var numUnique = 0;
    var numUnique81 = 0;
    forall j in MinUnique.domain with (+ reduce numUnique,
                                       + reduce numUnique81) {
      //if MinUnique[i] > 0 && MinUnique[i] <= MinUnique[i+1] {
      if MinUnique[j] > 0 {
        const i = MinUnique[j];
        const len = j - i;
        numUnique += 1;
        if 0 < len && len <= DEFAULT_K {
          numUnique81 += 1;
        }
      }
    }
    writeln("numUnique is ", numUnique);
    writeln("numUnique <= ", DEFAULT_K, " long is ", numUnique81);

    // Unique 81 base pair kmers?
    const cfg = new ssortConfig(idxType = thetext.idxType,
                                characterType = thetext.eltType,
                                offsetType = thetext.idxType,
                                cachedDataType = CACHED_DATA_TYPE,
                                loadWordType = LOAD_WORD_TYPE,
                                cover = new differenceCover(DEFAULT_PERIOD));

    var Unique81Starts: [0..<numUnique81] SA.eltType;
    var next = 0;
    for j in MinUnique.domain {
      var i = MinUnique[j];
      const len = j - i;
      if 0 < len && len <= DEFAULT_K {
        Unique81Starts[next] = makeOffsetAndCached(cfg, i, thetext, n);
        next += 1;
      }
    }
    sortSuffixesByPrefix(cfg, thetext, n, Unique81Starts, DEFAULT_K);
    var unique81 = 0;
    forall i in 0..<numUnique81 with (+ reduce unique81) {
      if prefixDiffersFromPrevious(cfg, i, Unique81Starts,
                                   thetext, n,
                                   maxPrefix=DEFAULT_K) {
        unique81 += 1;
      }
    }
    writeln("num unique kmers starting at minimal unique substrings ",
             unique81);
  }


  writeln("Computing unique k-mers (k=", DEFAULT_K, ")");
  t.reset();
  t.start();
  computeUniqueK(thetext, n);
  t.stop();
  writeln("computing over ", n, " bytes ", "took ", t.elapsed(), " seconds");
  writeln(n / 1000.0 / 1000.0 / t.elapsed(), " MB/s");
  */

  return 0;
}


}
