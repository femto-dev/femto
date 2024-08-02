/*
  2024 Shreyas Khandekar
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

  femto/src/ssort_chpl/TestFindUnique.chpl
*/

module TestFindUnique {
  use FindUnique;
  import SuffixSort.computeSuffixArrayAndLCP;

  config const debugOutput = false;

  private proc bytesToArray(s: bytes) {
    const nWithPadding = s.size+8;
    const A:[0..<nWithPadding] uint(8)=
      for i in 0..<nWithPadding do
        if i < s.size then s[i] else 0;

    return A;
  }

  private proc bytesToArray(s: string) {
    return bytesToArray(s:bytes);
  }

  proc testAbaababa() {
    writeln("testAbaababa()");

    // Sample data for testing
    /*

    abaababa
    01234567

    here is the suffix array and LCP:
              SA         LCP
    a         7          0
    aababa    2          1
    aba       5          1
    abaababa  0          3
    ababa     3          3
    ba        6          0
    baababa   1          2
    baba      4          2
    */
    const thetext : [0..15] uint(8) = bytesToArray('abaababa');
    const fileStarts = [0, 9];
    const ignoreDocs = [false];
    // const SA, LCP;
    // computeSuffixArrayAndLCP(thetext, n=8, SA, LCP);

    const SA = [7, 2, 5, 0, 3, 6, 1, 4];
    const LCP = [0, 1, 1, 3, 3, 0, 2, 2];

    // Expected output
    const expectedMinUnique = [0, 0, 2, 0, 3, 0, 0, 0, 0];

    if debugOutput {
      writeln("SA: ", SA);
      writeln("LCP: ", LCP);
      writeln("thetext: ", thetext);
      writeln("fileStarts: ", fileStarts);
      writeln("expectedMinUnique: ", expectedMinUnique);
    }

    // Call the function
    var result = findUnique(SA, LCP, thetext, fileStarts, ignoreDocs);
    if debugOutput {
      writeln("Result: ", result);
    }

    // Verify the result
    for i in 0..#result.size {
      assert(result[i] == expectedMinUnique[i], "Test failed at index " + i:string);
    }
  }

  proc testDifferentInputs() {
    writeln("testDifferentInputs()");

    /*
      banana
      012345

             SA  LCP  1+max(LCP[i],LCP[i+1])
      a      5   0    2
      ana    3   1    4
      anana  1   3    4
      banana 0   0    1
      na     4   0    3
      nana   2   2    3

      last column permuted to input positions (row i sets MinUnique[SA[i]])

      143431 (MinUnique)
      012345 (offsets)

      setting elts to 0 when MinUnique[i] > MinUnique[i+1]

      103000 (MinUnique)
      012345 (offsets)
     */

    const testCases = [
      ("banana", [1, 0, 3, 0, 0, 0, 0]),
      ("abcdefg", [1, 1, 1, 1, 1, 1, 0, 0]),
      ("aabbcc", [2, 2, 2, 2, 2, 0, 0])
    ];

    for (input, expected) in testCases {
      writeln("  ", input);

      const thetext = bytesToArray(input);
      const fileStarts = [0, input.size];
      const ignoreDocs = [false];
      const SA, LCP;
      computeSuffixArrayAndLCP(thetext, n = input.size, SA, LCP);

      if debugOutput {
        writeln("Input: ", input);
        writeln("SA: ", SA);
        writeln("LCP: ", LCP);
        writeln("thetext: ", thetext);
        writeln("fileStarts: ", fileStarts);
        writeln("expectedMinUnique: ", expected);
      }

      var result = findUnique(SA, LCP, thetext, fileStarts, ignoreDocs);
      if debugOutput {
        writeln("Result: ", result);
      }

      for i in 0..#result.size {
        assert(result[i] == expected[i], "Test failed for input " + input + " at index " + i:string);
      }
    }
  }

  proc testMultipleFiles() {
    writeln("testMultipleFiles()");

    const testCases = [
      ("aaa0bbb0", [0, 4, 8], [1, 1, 1, 0, 1, 1, 1, 0, 0]),
      ("aba0bab0", [0, 4, 8], [3, 0, 0, 0, 3, 0, 0, 0, 0]),
      ("aab0bba0", [0, 4, 8], [2, 2, 0, 0, 2, 2, 0, 0, 0]),
      ("aaaa0aaab0abab0", [0, 5, 10, 15], [4,0,0,0,0,0,3,0,0,0,0,2,0,0,0,0]),
    ];

    for (input, fileStarts, expected) in testCases {
      writeln("  ", input);

      const thetext = bytesToArray(input);
      const ignoreDocs:[0..<fileStarts.size-1] bool = false;

      const SA, LCP;
      computeSuffixArrayAndLCP(thetext, n = input.size, SA, LCP);

      if debugOutput {
        writeln("Input: ", input);
        writeln("SA: ", SA);
        writeln("LCP: ", LCP);
        writeln("thetext: ", thetext);
        writeln("fileStarts: ", fileStarts);
        writeln("expectedMinUnique: ", expected);
      }

      var result = findUnique(SA, LCP, thetext, fileStarts, ignoreDocs);

      if debugOutput {
        writeln("Result: ", result);
      }

      for i in 0..#result.size {
        assert(result[i] == expected[i], "Test failed for input " + input + " at index " + i:string);
      }
    }
  }

  proc runTests() {
    testAbaababa();
    testDifferentInputs();
    testMultipleFiles();
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
