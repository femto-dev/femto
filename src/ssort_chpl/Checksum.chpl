/*
  2025 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/ssort_chpl/Checksum.chpl
*/

module Checksum {

private import List;
private import Help;

private use Utility;

proc usage(args: [] string) {
  writeln("usage: ", args[0], " <files-and-directories>");
  writeln("this program reads in files and computes a checksum");
  Help.printUsage();
}

proc main(args: [] string) throws {
  var inputFilesList: List.list(string);

  for arg in args[1..] {
    if arg == "--help" || arg == "-h" {
      usage(args);
      return 0;
    }
    if arg.startsWith("-") {
      writeln("argument not handled ", arg);
      usage(args);
      return 1;
    }
    gatherFiles(inputFilesList, arg);
  }

  if inputFilesList.size == 0 {
    writeln("please specify input files and directories");
    usage(args);
    return 1;
  }

  var readTime = startTime(true);
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

  /*writeln("Files are:");
  for p in concisePaths {
    writeln(" ", p);
  }
  writeln("FileStarts are: ", fileStarts);
  writeln("Sequences are:");
  for (d,i) in zip(sequenceDescriptions, 0..) {
    writeln(" ", i, " ", d);
  }
  writeln("SequenceStarts are: ", sequenceStarts);*/

  reportTime(readTime, "reading input", totalSize, 1);

  var chksumTime = startTime(true);
  const allFileChecksums;
  hashAllFiles(allData, fileStarts, totalSize, allFileChecksums);
  const allSeqChecksums;
  hashAllFiles(allData, sequenceStarts, totalSize, allSeqChecksums);
  reportTime(chksumTime, "checksum", 2*totalSize, 1);

  for idx in 0..<allPaths.size {
    const f = allPaths[idx];
    const chksum = allFileChecksums[idx];

    for i in 0..<chksum.size {
      writef("%08xu", chksum[i]);
    }
    write("  ");
    write(f);
    writeln();
  }

  for idx in 0..<sequenceDescriptions.size {
    const d = sequenceDescriptions[idx];
    const chksum = allSeqChecksums[idx];

    /*var dataregion = sequenceStarts[idx]..<sequenceStarts[idx+1];
    writeln("data in ", dataregion, " for ", d, ":");
    for j in dataregion {
      writef("%c", allData[j]);
    }
    writeln();*/

    for i in 0..<chksum.size {
      writef("%08xu", chksum[i]);
    }
    write("  ");
    write(d);
    writeln();
  }

  return 0;
}


}
