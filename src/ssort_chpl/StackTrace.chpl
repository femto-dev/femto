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

  femto/src/ssort_chpl/StackTrace.chpl
*/

module StackTrace {


import CTypes.{c_ptr, c_char};
import ChplConfig.CHPL_UNWIND;

private extern proc chpl_stack_unwind_to_string(sep: uint(8)): c_ptr(c_char);

/* Computes a stack trace and returns it as a string.
   Trace entries are separated by the first byte in 'sep',
   or newline if it is empty. */
proc stackTraceAsString(sep: bytes = b"\n"): string { 
  if CHPL_UNWIND == "none" {
    compilerWarning("stackTraceAsString always returns the empty string in this configuration. Please recompile with CHPL_UNWIND=system or CHPL_UNWIND=bundled");
  }
  var useSep: uint(8) = "\n".toByte();
  if sep.size > 0 {
    useSep = sep[0];
  }

  var buf = chpl_stack_unwind_to_string(useSep);

  var s = try! string.createAdoptingBuffer(buf);
  return s;
}


}
