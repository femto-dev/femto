/*
  (*) 2006-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/error.c
*/
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "error.h"

#define MAX_ERRORS 32
static error_info_t errors[MAX_ERRORS];
static int next_error = 0;

#define MAX_E_STRING 512
static char e_string[MAX_E_STRING];

error_t construct_error_errno(err_code_t code, int std_errno, const char* object_name, const char* msg, const char* file, int line)
{
  error_t ret;

  assert( code );

  ret = next_error;
  errors[ret].code = code;
  if( ! msg ) msg = "";
  errors[ret].msg = msg;
  if( ! object_name ) object_name = "";
  errors[ret].object_name = object_name;
  errors[ret].file = file;
  errors[ret].line = line;
  errors[ret].std_errno = std_errno;

  next_error = (next_error+1)%MAX_ERRORS;
  
  
  return ret+1;
}

error_t construct_error(err_code_t code, const char* msg, const char* file, int line)
{
  return construct_error_errno(code, errno, NULL, msg, file, line);
}
error_t construct_error_obj(err_code_t code, const char* object_name, const char* msg, const char* file, int line)
{
  return construct_error_errno(code, errno, object_name, msg, file, line);
}

static const char* err_code_string(err_code_t err)
{

  switch (err) {
    case ERR_CODE_NOERR:
      return "no error";
    case ERR_CODE_MEM:
      return "out of memory";
    case ERR_CODE_IO:
      return "io error";
    case ERR_CODE_PARAM:
      return "invalid parameters";
    case ERR_CODE_FORMAT:
      return "broken file format";
    case ERR_CODE_BZ_DATA:
      return "compression error";
    case ERR_CODE_INVALID:
      return "invalid state";
    case ERR_CODE_MISSING:
      return "missing data";
    case ERR_CODE_FULL:
      return "structure is full";
    case ERR_CODE_OVERWORKED:
      return "Too much work";
    default:
      return "unknown error";
  }
}

error_info_t* err_info(error_t err)
{
  error_info_t* inf;
  if( err == 0 ) return NULL;
  if( err > MAX_ERRORS ) return NULL;
  if( err < 0 ) return NULL;
  inf = & errors[err-1];
  return inf;
}

err_code_t err_code(error_t err)
{
  error_info_t* inf = err_info(err);
  if( inf ) return inf->code;
  else return ERR_CODE_NOERR;
}

const char* err_string(error_t err )
{
  error_info_t* inf;
 
  if( err == 0 ) return "no error";
  if( err > MAX_ERRORS ) {
    return "UNKNOWN ERROR";
  }
  if( err < 0 ) {
    return "UNKNOWN ERROR";
  }

  inf = err_info(err);

  if( inf->std_errno != 0 ) {
    snprintf(e_string, MAX_E_STRING, "%s:%s %s on %s line %i (%s)",
             err_code_string(inf->code), inf->msg, inf->object_name, inf->file, inf->line,
             strerror(inf->std_errno));
  } else {
    snprintf(e_string, MAX_E_STRING, "%s:%s %s on %s line %i",
             err_code_string(inf->code), inf->msg, inf->object_name, inf->file, inf->line);
  }

  return e_string;
}

void warn_if_err(error_t err)
{
  if( err ) {
    fprintf(stderr, "Warning: %s\nContinuing\n", err_string(err));
  }
}

void die_if_err(error_t err)
{
  if( err ) {
    fprintf(stderr, "Error: %s\nAborting\n", err_string(err));
    assert(0);
    exit(-1);
  }
}
