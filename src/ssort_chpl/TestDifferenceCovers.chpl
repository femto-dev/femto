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

  femto/src/ssort_chpl/TestDifferenceCovers.chpl
*/

module TestDifferenceCovers {


use DifferenceCovers;
import SuffixSort.TRACE;

proc testCover(param period) {
  if TRACE {
    writeln("testing difference cover with period ", period);
  }

  const dc = new differenceCover(period);
  assert(dc.sampleSize == dc.cover.size);

  // print out information about this difference cover
  var minDist = dc.period;
  var maxDist = 0;
  for i in 0..dc.sampleSize { // inclusive of top bound to consider last vs 0
    const cur = dc.cover[i % dc.sampleSize];
    var next = dc.cover[(i+1) % dc.sampleSize];
    while next < cur do next += dc.period;
    const dist = next - cur;
    minDist = min(dist, minDist);
    maxDist = max(dist, maxDist);
  }

  if TRACE {
    writeln("  sample size = ", dc.sampleSize);
    writeln("  sample percentage = ", (100.0 * dc.sampleSize) / dc.period);
    writeln("  min distance between samples = ", minDist);
    writeln("  max distance between samples = ", maxDist);
  }

  // check containedInCover and coverIndex
  for i in 0..<period {
    var found = -1;
    for j in 0..<dc.cover.size {
      if dc.cover[j] == i then found = j;
    }
    assert(dc.containedInCover(i) == (found >= 0));
    assert(dc.coverIndex(i) == found);
  }

  // check findInCover
  var maxSampleRanksPassed = -1;
  forall i in 0..<period with (max reduce maxSampleRanksPassed) {
    for j in 0..<period {
      const k = dc.findInCover(i, j);
      assert(0 <= k && k < period);
      assert(dc.containedInCover((i + k) % period));
      assert(dc.containedInCover((j + k) % period));

      if TRACE && period < 1000 {
        // k is a distance that is less than the cover period.
        // what is the maximum distance in terms of sample ranks passed?
        var idist, jdist = -1;
        for ii in i..i+k {
          if dc.containedInCover(ii % period) {
            idist += 1;
          }
        }
        for jj in j..j+k {
          if dc.containedInCover(jj % period) {
            jdist += 1;
          }
        }
        maxSampleRanksPassed = max(maxSampleRanksPassed, idist, jdist);
      }
    }
  }

  if TRACE && maxSampleRanksPassed != -1 {
    writeln("  maximum sample ranks passed ", maxSampleRanksPassed);
  }
}

proc testCovers() {
  testCover(3);
  testCover(7);
  testCover(13);
  testCover(21);
  testCover(31);
  testCover(39);
  testCover(57);
  testCover(73);
  testCover(91);
  testCover(95);
  testCover(133);
  testCover(1024);
  testCover(2048);
  testCover(4096);
  testCover(8192);
}
proc main() {
  if TRACE then writeln("Testing Difference Covers");
  testCovers();

  writeln("TestDifferenceCovers OK");
}


} // end module DifferenceCovers
