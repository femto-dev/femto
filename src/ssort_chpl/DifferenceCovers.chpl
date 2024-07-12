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

  femto/src/ssort_chpl/DifferenceCovers.chpl
*/

module DifferenceCovers {


config param EXTRA_CHECKS = false;

const cover3    = (0,1); // alternately, 1,2
const cover7    = (0,1,3);
const cover13   = (0,1,3,9);
const cover21   = (0,1,6,8,18);
const cover31   = (0,1,3,8,12,18);
const cover39   = (0,1,16,20,22,27,30);
const cover57   = (0,1,9,11,14,35,39,51);
const cover73   = (0,1,3,7,15,31,36,54,63);
const cover91   = (0,1,7,16,27,56,60,68,70,73);
const cover95   = (0,1,5,8,18,20,29,31,45,61,67);
const cover133  = (0,1,32,42,44,48,51,59,72,77,97,111);
const cover1024 = (0, 1, 2, 3, 4, 5, 6, 13, 26, 39, 52, 65, 78, 91, 118, 145, 172, 199, 226, 253, 280, 307, 334, 361, 388, 415, 442, 456, 470, 484, 498, 512, 526, 540, 541, 542, 543, 544, 545, 546);
const cover2048 = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 19, 38, 57, 76, 95, 114, 133, 152, 171, 190, 229, 268, 307, 346, 385, 424, 463, 502, 541, 580, 619, 658, 697, 736, 775, 814, 853, 892, 931, 951, 971, 991, 1011, 1031, 1051, 1071, 1091, 1111, 1131, 1132, 1133, 1134, 1135, 1136, 1137, 1138, 1139, 1140);
const cover4096 = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 27, 54, 81, 108, 135, 162, 189, 216, 243, 270, 297, 324, 351, 378, 433, 488, 543, 598, 653, 708, 763, 818, 873, 928, 983, 1038, 1093, 1148, 1203, 1258, 1313, 1368, 1423, 1478, 1533, 1588, 1643, 1698, 1753, 1808, 1863, 1891, 1919, 1947, 1975, 2003, 2031, 2059, 2087, 2115, 2143, 2171, 2199, 2227, 2255, 2256, 2257, 2258, 2259, 2260, 2261, 2262, 2263, 2264, 2265, 2266, 2267, 2268);
const cover8192 = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 37, 74, 111, 148, 185, 222, 259, 296, 333, 370, 407, 444, 481, 518, 555, 592, 629, 666, 703, 778, 853, 928, 1003, 1078, 1153, 1228, 1303, 1378, 1453, 1528, 1603, 1678, 1753, 1828, 1903, 1978, 2053, 2128, 2203, 2278, 2353, 2428, 2503, 2578, 2653, 2728, 2803, 2878, 2953, 3028, 3103, 3178, 3253, 3328, 3403, 3478, 3516, 3554, 3592, 3630, 3668, 3706, 3744, 3782, 3820, 3858, 3896, 3934, 3972, 4010, 4048, 4086, 4124, 4162, 4200, 4201, 4202, 4203, 4204, 4205, 4206, 4207, 4208, 4209, 4210, 4211, 4212, 4213, 4214, 4215, 4216, 4217, 4218);

/** Returns the tuple representing a difference cover with period 'period',
    or 'none' if this implementation doesn't have a difference cover with that
    period.
 */
proc coverTuple(param period) {
  if period == 3 then return cover3;
  if period == 7 then return cover7;
  if period == 13 then return cover13;
  if period == 21 then return cover21;
  if period == 31 then return cover31;
  if period == 39 then return cover39;
  if period == 57 then return cover57;
  if period == 73 then return cover73;
  if period == 91 then return cover91;
  if period == 95 then return cover95;
  if period == 133 then return cover133;
  if period == 1024 then return cover1024;
  if period == 2048 then return cover2048;
  if period == 4096 then return cover4096;
  if period == 8192 then return cover8192;
  return none;
}

private proc makeEllTable(param period): period*int {
  const cover = coverTuple(period);
  const sampleSize = cover.size;
  var ellTable: period*int;

  for i in 0..<sampleSize {
    for j in 0..<sampleSize {
      const iprime = cover[i];
      const jprime = cover[j];
      // now i' and j' are in the difference cover
      // d = (i'-j') mod v
      var d = iprime - jprime;
      if d < 0 then d += period;
      ellTable[d] = iprime;
    }
  }

  return ellTable;
}

private proc makeSampleTable(param period): period*int {
  const cover = coverTuple(period);
  const sampleSize = cover.size;
  var sampleTable: period*int;

  for i in 0..<period {
    sampleTable[i] = -1;
  }

  for j in 0..<sampleSize {
    sampleTable[cover[j]] = j;
  }

  return sampleTable;
}

record differenceCover {
  /** the period of the difference cover
      aka v in Karkkainen Sanders Burkhardt */
  param period;

  /** given (i-j)mod v, return i' so that l=(i`-i) mod v
      and (i+l) mod v and (j+l) mod v are both in
      the difference cover. */
  /*private*/ const ellTable: period*int;

  /** sample[i mod v]=index s.t. cover[index]=i, else -1 */
  /*private*/ const sampleTable: period*int;

  /** returns the size of the difference cover, that is, cover.size */
  proc sampleSize param : int { return coverTuple(period).size; }
  /** returns period - sampleSize */
  proc nonsampleSize param : int { return period - sampleSize; }
  /** returns the cover tuple */
  inline proc cover : sampleSize*int { return coverTuple(period); }

  proc init(param period) {
    this.period = period;
    this.ellTable = makeEllTable(period);
    this.sampleTable = makeSampleTable(period);
  }

  /**
    Given offsets i and j, with 0 <= i < period and 0 <= j < period,
    return k such that (i+k) mod period and (j+k) mod period
    are in in the difference cover.
    Returns such a k.
   */
  inline proc findInCover(i: int, j: int) : int {
    if EXTRA_CHECKS {
      assert(0 <= i && i < period);
      assert(0 <= j && j < period);
    }
    var d = i - j;
    if d < 0 then d += period;
    if EXTRA_CHECKS {
      assert(d == (d % period));
      assert(0 <= d && d < period);
    }

    const iprime = ellTable[d];
    var ell = iprime - i;
    if ell < 0 then ell += period;
    if EXTRA_CHECKS {
      assert(ell == (ell % period));
      assert(0 <= ell && ell < period);
    }

    return ell;
  }

  /**
   Given offset i with 0 <= i < period, returns 'true'
   if and only if 'i' is in the difference cover.
   */
  inline proc containedInCover(i: int) {
    if EXTRA_CHECKS {
      assert(0 <= i && i < period);
    }
    return sampleTable[i] >= 0;
  }

  /**
   Given offset i with 0 <= i < period
     * if it is in the difference cover, returns j such that cover[j] = i
     * otherwise, returns -1
   */
  inline proc coverIndex(i: int) {
    if EXTRA_CHECKS {
      assert(0 <= i && i < period);
    }
    return sampleTable[i];
  }
}


/////////// Begin Testing Code ////////////

proc testCover(param period) {
  writeln("testing difference cover with period ", period);

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

  writeln("  sample size = ", dc.sampleSize);
  writeln("  sample percentage = ", (100.0 * dc.sampleSize) / dc.period);
  writeln("  min distance between samples = ", minDist);
  writeln("  max distance between samples = ", maxDist);

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
  for i in 0..<period {
    for j in 0..<period {
      const k = dc.findInCover(i, j);
      assert(0 <= k && k < period);
      assert(dc.containedInCover((i + k) % period));
      assert(dc.containedInCover((j + k) % period));
    }
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
  writeln("Testing Difference Covers");
  testCovers();
}


} // end module DifferenceCovers
