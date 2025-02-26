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
import List;
import OS.EofError;
import Path;

proc normalizeDesc(in desc: string) {
  desc = desc.strip(">", leading=true, trailing=false);
  desc = desc.replace(" ", "_");
  desc = desc.replace("\t", "_");
  return desc;
}

proc main() throws {
  if INPUT == "" {
    writeln("please use --input <filename> to specify an input file");
    return 1;
  }
  if UNIQUE == "" {
    writeln("please use --unique <filename> to specify a .unique file");
    writeln("FindUnique can generate these");
    return 1;
  }

  const isFasta = isFastaFile(INPUT);

  var inputFilesList: List.list(string);
  inputFilesList.pushBack(INPUT);

  const allData; //: [] uint(8);
  const allPaths; //: [] string;
  const concisePaths; // : [] string
  const fileStarts; //: [] int;
  const totalSize: int;
  const sequenceDescriptions; //: [] string;
  const sequenceStarts; //: [] int;
  readAllFiles(inputFilesList,
               Locales,
               allData=allData,
               allPaths=allPaths,
               concisePaths=concisePaths,
               fileStarts=fileStarts,
               totalSize=totalSize,
               sequenceDescriptions=sequenceDescriptions,
               sequenceStarts=sequenceStarts,
               skipDescriptions=false);

  var uniqueF = IO.open(UNIQUE, IO.ioMode.r);
  const n2 = uniqueF.size;
  var MinUnique:[0..<n2] uint(8);
  uniqueF.reader().readAll(MinUnique);
  uniqueF.close();

  const n = totalSize - 1; // ignore trailing null byte

  if n != n2 {
    halt("File sizes do not match" +
         "-- does the input file correspond to the unique file?");
  }


  var useFilename = Path.basename(INPUT);
  if !REPLACE_FILENAME.isEmpty() {
    useFilename = REPLACE_FILENAME;
  }

  var curDesc: string = "-";
  var curSequence = -1;
  for i in 0..<n {
    const len = MinUnique[i]: int;
    if len > 0 && (K == 0 || len <= K) {
      const useK = if K == 0 then len else K;
      const amtBefore = (useK-len)/2;
      var startOffset = max(i - amtBefore, 0); // don't go before

      // output the kmer if it doesn't go beyond the end
      if startOffset + useK <= n {
        var ignoreDueToSequenceBoundary = false;
        if isFasta {
          for j in 0..<useK {
            if allData[startOffset+j] == ">".toByte() {
              ignoreDueToSequenceBoundary = true;
            }
          }
        }

        if !ignoreDueToSequenceBoundary {
          var seqIdx = offsetToFileIdx(sequenceStarts, i);
          if curSequence != seqIdx {
            curSequence = seqIdx;
            curDesc = sequenceDescriptions[curSequence];
            curDesc = normalizeDesc(curDesc);
          }
          for j in 0..<useK {
            writef("%c", allData[startOffset + j]);
          }

          write(" 0 ", useFilename, " 1 ", curDesc, " ", i);
          writeln();
        }
      }
    }
  }

  return 0;
}


}
