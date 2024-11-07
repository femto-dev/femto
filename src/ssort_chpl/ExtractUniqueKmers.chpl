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

  femto/src/ssort_chpl/ExtractUniqueKmers.chpl
*/

/* This program extracts the unique kmers based on the result of running
   FindUnique. It is focused on handling fasta files. */
module ExtractUniqueKmers {


config const input:string; // input file
config const unique:string; // .unique file from FindUnique
config const replaceFilename:string; // use this filename in the output
config const k: int = 0; // K as in KMER (common prefix length)

// upper-case names for the config constants to better identify them in code
const INPUT = input;
const UNIQUE = unique;
const REPLACE_FILENAME = replaceFilename;
const K = k;

use Utility;

import IO;
import OS.EofError;
import Path;

proc main() throws {
  if INPUT == "" {
    writeln("please use --INPUT <filename> to specify an input file");
    return 1;
  }
  if UNIQUE == "" {
    writeln("please use --UNIQUE <filename> to specify a .unique file");
    writeln("FindUnique can generate these");
    return 1;
  }

  const isFasta = isFastaFile(INPUT);
  const n = computeFileSize(INPUT);
  var Text:[0..n] uint(8);
  readFileData(INPUT, Text, 0..<n, verbose=false);

  var uniqueF = IO.open(UNIQUE, IO.ioMode.r);
  const n2 = uniqueF.size;
  var MinUnique:[0..<n2] uint(8);
  uniqueF.reader().readAll(MinUnique);
  uniqueF.close();

  if n != n2 {
    halt("File sizes do not match" +
         "-- does the input file correspond to the unique file?");
  }

  var useFilename = Path.basename(INPUT);
  if !REPLACE_FILENAME.isEmpty() {
    useFilename = REPLACE_FILENAME;
  }

  if isFasta {
    var r = IO.openReader(INPUT);
    // process the input file, Text, and MinUnique together
    // use the input file just to get the descriptions


    var textOffset: int = 0;

    // read until next description
    while textOffset < n {
      var desc: string;
      try {
        r.advanceTo(">");
        r.readLine(desc, stripNewline=true);
        // verify that Text[textOffset] is a >
        if Text[textOffset] != ">".toByte() {
          halt("Contig misalignment");
        }
        textOffset += 1;
      } catch e: EofError {
        break;
      }
      // check that the first line matches
      var check: bytes;
      r.mark();
      // read until we get a non-empty line
      while r.readLine(check, stripNewline=true) &&
            !check.isEmpty() {
      }
      r.revert();

      desc = desc.strip(">", leading=true, trailing=false);
      desc = desc.replace(" ", "_");
      desc = desc.replace("\t", "_");

      // verify that 'check' matches Text[textOffset..]
      if !check.isEmpty() {
        var len = min(n - textOffset, check.size);
        for i in 0..len {
          if Text[textOffset+i] != check[i] {
            halt("Nucleotide did not match");
          }
        }
      }

      // find the end of this contig in Text
      var contigLen = 0;
      while textOffset + contigLen < n {
        if Text[textOffset + contigLen] == ">".toByte() {
          break;
        }
        contigLen += 1;
      }
      // now Text[textOffset + contigLen] == > or is at n
      const contigEnd = textOffset + contigLen;

      // OK, now emit the unique substrings
      for i in 0..<contigLen {
        const len = MinUnique[textOffset + i]: int;
        if len > 0 && (K == 0 || len <= K) {
          const useK = if K == 0 then len else K;
          const amtBefore = (useK-len)/2;
          var startOffset = textOffset + i - amtBefore; 
          // but don't let start offset be earlier than the contig start
          startOffset = max(startOffset, textOffset);

          // output the kmer
          if startOffset + useK <= contigEnd {
            for j in 0..<useK {
              writef("%c", Text[startOffset + j]);
            }
          }

          write(" 0 ", useFilename, " 1 ", desc, " ", i);
          writeln();
        }
      }

      textOffset += contigLen;
    }
  } else {
    halt("Not implemented yet");
  }

  return 0;
}


}
