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


config const unique:string; // dir containing .unique files from FindUnique
config const k: int = 0; // K as in KMER (common prefix length)
config const tsv: bool = false; // output uniquesketch tab-separated format
config const shifts: bool = false; // output shifts

// upper-case names for the config constants to better identify them in code
const UNIQUE_DIR = unique;
const K = k;
const OUTPUT_TSV = tsv;
const SHIFTS = shifts;

use Utility;

import FileSystem;
import IO;
import List;
import OS.EofError;
import Path;
import Set;
import Sort;

proc normalizeDesc(in desc: string) {
  desc = desc.strip(">", leading=true, trailing=false);
  return desc;
}

proc replaceSpacesWithUnderlines(in desc: string) {
  desc = desc.replace(" ", "_");
  desc = desc.replace("\t", "_");
  return desc;
}

const toEscape = (">",   "\n",  "\r",  "\\");
const escaped  = ("\\>", "\\n", "\\r", "\\\\");

proc escapeCh(ch: uint(8)) {
  for i in 0..<toEscape.size {
    if toEscape(i).toByte() == ch {
      return escaped(i);
    }
  }
  assert(false); // don't call this it's not a character to escape
  return "";
}
proc escapeString(in s: string) {
  for i in 0..<toEscape.size {
    s = s.replace(toEscape(i), escaped(i));
  }
  return s;
}

proc outputKmer(offset: int,
                uniqueLen: int,
                outputLen: int,
                startOffsetInSequence: int,
                uniqueOffsetInSequence: int,
                fileIdx: int,
                nFiles: int,
                allData: [] uint(8),
                useFilename: string,
                curDesc: string,
                isFasta: bool) {
  if OUTPUT_TSV {
    // output tab-separated data:
    //  kmer
    //  file idx (integer)
    //  file name
    //  num files
    //  sequence description
    //  kmer start position in sequence

    // output the kmer
    for j in 0..<outputLen {
      writef("%c", allData[offset + j]);
    }
    // output the rest
    writef("\t%i\t%s\t%i\t%s\t%i\n",
           fileIdx, useFilename, nFiles, curDesc, startOffsetInSequence);
  } else {
    // output matches in a FASTA-ish format FASTA
    //   >
    //   sequence description|
    //   filename|
    //   kmer start in sequence
    //   kmer len
    //   unique start in sequence
    //   unique len
    // <then sequence data in separate line>

    writef("> %s | %s | %i %i %i %i\n",
           curDesc, useFilename,
           startOffsetInSequence, outputLen,
           uniqueOffsetInSequence, uniqueLen);

    // output the kmer, with some escaping
    for j in 0..<outputLen {
      var ch:uint(8) = allData[offset + j];
      if ch == ">".toByte() || ch == "\n".toByte() ||
         ch == "\r".toByte() || ch == "\\".toByte() {
        write(escapeCh(ch));
      } else {
        writef("%c", ch);
      }
    }
    writeln();
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
  if UNIQUE_DIR == "" {
    writeln("please use --unique <dir> to specify a directory containing .unique files");
    writeln("FindUnique can generate such a directory");
    return 1;
  }
  if K == 0 && SHIFTS {
    writeln("please set --k=<number> for use with --shifts");
    return 1;
  }


  // compute the paths array
  var inputPaths = inputFilesList.toArray();
  for p in inputPaths {
    p = Path.normPath(p);
  }
  Sort.sort(inputPaths);

  // compute the concise paths array
  var concisePaths = inputPaths;
  trimPaths(concisePaths);

  // compute the unique files paths array
  var uniquePaths = concisePaths;
  for p in uniquePaths {
    var p1 = Path.normPath(UNIQUE_DIR + "/" + p + ".unique");
    var b = Path.basename(p);
    var p2 = Path.normPath(UNIQUE_DIR + "/" + b + ".unique");
    if FileSystem.isFile(p1) {
      p = p1;
    } else if FileSystem.isFile(p2) {
      p = p2;
    } else {
      halt("Could not find unique file at ", p1);
    }
  }

  {
    // check that the unique files paths are unique
    var s:Set.set(string);
    for (f,p) in zip(inputPaths, uniquePaths) {
      if s.contains(p) {
        halt("could not find non-ambiguous unique file for ", f);
      }
      s.add(p);
    }
  }

  var nFiles = inputPaths.size;

  // now process all of the files together
  // this could be parallel if it didn't mean that the output
  // would be interleaved
  for (inputPath, concisePath, uniquePath, fileIdx)
  in zip(inputPaths, concisePaths, uniquePaths, inputPaths.domain) {
    const isFasta = isFastaFile(inputPath);

    // Read just the one file being processed here
    var inputFilesList: List.list(string);
    inputFilesList.pushBack(inputPath);

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

    var uniqueF = IO.open(uniquePath, IO.ioMode.r);
    const n2 = uniqueF.size;
    var MinUnique:[0..<n2] uint(8);
    uniqueF.reader().readAll(MinUnique);
    uniqueF.close();

    var n = totalSize;

    if n != n2 {
      halt("File sizes do not match (",
           totalSize, " vs ", n2, ") ",
           "-- does the input file correspond to the unique file?");
    }

    var useFilename = concisePath;
    useFilename = escapeString(useFilename);

    var curDesc: string = "-";
    var curSequence = -1;
    var curSequenceStart = 0;
    for i in 0..<n {
      const len = MinUnique[i]: int;
      if len > 0 && (K == 0 || len <= K) {
        const useK = if K == 0 then len else K;
        var startOffset:int = i;
        if K != 0 {
          const amtBefore = if SHIFTS then useK-len else (useK-len)/2;
          // set startOffset to i - amtBefore,
          // but don't go negative and don't pass a sequence boundary
          // (>)
          if isFasta {
            var j = 0;
            while i - j >= 0 && j <= amtBefore {
              if allData[i - j] == ">".toByte() {
                break;
              }
              j += 1;
            }
            startOffset = i - j;
          } else {
            startOffset = i - amtBefore;
          }
          // startOffset should not be negative
          startOffset = max(startOffset, 0);
          // startOffset should not be larger than the position unique starts
          startOffset = min(startOffset, i);
        }

        var skip = true;
        if startOffset + useK <= n {
          skip = false;
          if isFasta {
            // skip any sequences that contain the sequence boundary >
            for j in 0..<useK {
              if allData[startOffset+j] == ">".toByte() {
                skip = true;
              }
            }
          }
        }

        if !skip {
          // update the current sequence description
          var seqIdx = offsetToFileIdx(sequenceStarts, i);
          if curSequence != seqIdx {
            curSequence = seqIdx;
            curSequenceStart = sequenceStarts[curSequence];
            curDesc = sequenceDescriptions[curSequence];
            curDesc = normalizeDesc(curDesc);
            if OUTPUT_TSV {
              curDesc = replaceSpacesWithUnderlines(curDesc);
            }
            curDesc = escapeString(curDesc);
          }

          if SHIFTS {
            for curStartOffset in startOffset..i {
              // stop if the end of the sequence is beyond the end of the string
              if curStartOffset + useK > n {
                break;
              }
              // stop if there is an > at the starting offset
              if isFasta && allData[curStartOffset] == ">".toByte() {
                break;
              }
              // or if there is one at useK-1
              if isFasta && allData[curStartOffset+useK-1] == ">".toByte() {
                break;
              }

              // output the kmer at this shift
              outputKmer(offset=curStartOffset,
                         uniqueLen=len,
                         outputLen=useK,
                         startOffsetInSequence=curStartOffset-curSequenceStart,
                         uniqueOffsetInSequence=i-curSequenceStart,
                         fileIdx=fileIdx,
                         nFiles=nFiles,
                         allData=allData,
                         useFilename=useFilename,
                         curDesc=curDesc,
                         isFasta=isFasta);
            }
          } else {
            outputKmer(offset=startOffset,
                       uniqueLen=len,
                       outputLen=useK,
                       startOffsetInSequence=startOffset-curSequenceStart,
                       uniqueOffsetInSequence=i-curSequenceStart,
                       fileIdx=fileIdx,
                       nFiles=nFiles,
                       allData=allData,
                       useFilename=useFilename,
                       curDesc=curDesc,
                       isFasta=isFasta);
          }
        }
      }
    }
  }

  return 0;
}


}
