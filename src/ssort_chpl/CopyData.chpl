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

  femto/src/ssort_chpl/CopyData.chpl
*/

module CopyData {

private import List;
private import Help;

private use Utility;

config const output: string = "";

proc usage(args: [] string) {
  writeln("usage: ", args[0], " --output <filename> <files-and-directories>");
  writeln("this program reads in files and outputs the data to one file");
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

  if output == "" {
    writeln("please specify an output file");
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
               sequenceStarts=sequenceStarts);

  reportTime(readTime, "reading input", totalSize, 1);

  writeln("Saving output to ", output);
  writeBlockArray(output, allData, 0..<totalSize);

  return 0;
}


}
