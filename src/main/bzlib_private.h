/*
  (*) 2006-2013 Michael Ferguson <michaelferguson@acm.org>
      based on bzlib_private.h (C) 1996-2010 by Julian Seward

    * This is a work of the United States Government and is not protected by
      copyright in the United States.

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

  femto/src/main/bzlib_private.h
*/

#ifndef _BZLIB_PRIVATE_H_
#define _BZLIB_PRIVATE_H_

#include "index_types.h"

typedef char            Char;
typedef unsigned char   Bool;
typedef unsigned char   UChar;
typedef int             Int32;
typedef unsigned int    UInt32;
typedef short           Int16;
typedef unsigned short  UInt16;

#define True  ((Bool)1)
#define False ((Bool)0)

#define BZ_MAX_CODE_LEN    23
#define BZ_MAX_ALPHA_SIZE MTF_ALPHA_SIZE

extern void BZ2_bz__AssertH__fail ( int errcode );
#define AssertH(cond,errcode) \
   { if (!(cond)) BZ2_bz__AssertH__fail ( errcode ); }


extern void 
BZ2_hbAssignCodes ( Int32*, UChar*, Int32, Int32, Int32 );

extern void 
BZ2_hbMakeCodeLengths ( UChar*, Int32*, Int32, Int32 );


extern void 
BZ2_hbCreateDecodeTables ( Int32*, Int32*, Int32*, UChar*,
                           Int32,  Int32, Int32 );

#endif
