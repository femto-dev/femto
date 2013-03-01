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

  femto/src/utils/error.h
*/
#ifndef _ERROR_H_
#define _ERROR_H_

typedef enum {
  ERR_CODE_NOERR = 0,
  ERR_CODE_MEM,
  ERR_CODE_IO,
  ERR_CODE_PARAM,
  ERR_CODE_FORMAT,
  ERR_CODE_BZ_DATA,
  ERR_CODE_INVALID,
  ERR_CODE_PTHREADS,
  ERR_CODE_MISSING,
  ERR_CODE_CANCELED,
  ERR_CODE_FULL,
  ERR_CODE_OVERWORKED,
  ERR_CODE_UNKNOWN,
} err_code_t;

#define ERR_NOERR 0
#define ERR_MAKE(code) construct_error(code, NULL, __FILE__, __LINE__)
#define ERR_MAKE_STR(code,msg) construct_error(code, msg, __FILE__, __LINE__)
#define ERR_MAKE_STR_NUM(code,msg,std_errno) construct_error_errno(code, std_errno, NULL, msg, __FILE__, __LINE__)
#define ERR_MAKE_STR_OBJ(code,msg,obj) construct_error_obj(code, obj, msg, __FILE__, __LINE__)
#define ERR_MAKE_STR_OBJ_NUM(code,msg,obj,std_errno) construct_error_errno(code, std_errno, obj, msg, __FILE__, __LINE__)
#define ERR_MEM (ERR_MAKE(ERR_CODE_MEM))
#define ERR_MEM_STR_NUM(msg,std_errno) (ERR_MAKE_STR_NUM(ERR_CODE_MEM, msg, std_errno))
#define ERR_IO_UNK (ERR_MAKE(ERR_CODE_IO))
#define ERR_PARAM (ERR_MAKE(ERR_CODE_PARAM))
#define ERR_PARAM_STR(msg) (ERR_MAKE_STR(ERR_CODE_PARAM, msg))
#define ERR_FORMAT (ERR_MAKE(ERR_CODE_FORMAT))
#define ERR_FORMAT_STR(msg) (ERR_MAKE_STR(ERR_CODE_FORMAT,msg))
#define ERR_BZ_DATA (ERR_MAKE(ERR_CODE_BZ_DATA))
#define ERR_INVALID (ERR_MAKE(ERR_CODE_INVALID))
#define ERR_INVALID_STR(yyyy) (ERR_MAKE_STR(ERR_CODE_INVALID, yyyy))
#define ERR_PTHREADS(msg,pthread_errno) (ERR_MAKE_STR_NUM(ERR_CODE_PTHREADS,msg, pthread_errno))
#define ERR_MISSING (ERR_MAKE(ERR_CODE_MISSING))
#define ERR_FULL (ERR_MAKE(ERR_CODE_FULL))
#define ERR_OVERWORKED (ERR_MAKE(ERR_CODE_OVERWORKED))
// These conflict with an OpenMPI header...
//#define ERR_UNKNOWN (ERR_MAKE(ERR_CODE_UNKNOWN))
//#define ERR_UNKNOWN_STR(msg) (ERR_MAKE_STR(ERR_CODE_UNKNOWN,msg))
#define ERR_IO_STR(msg) (ERR_MAKE_STR(ERR_CODE_IO,msg))
#define ERR_IO_STR_NUM(msg,std_errno) (ERR_MAKE_STR_NUM(ERR_CODE_IO,msg,std_errno))
#define ERR_IO_STR_OBJ(msg,obj) (ERR_MAKE_STR_OBJ(ERR_CODE_IO,msg,obj))
#define ERR_IO_STR_OBJ_NUM(msg,obj,std_errno) ERR_MAKE_STR_OBJ_NUM(ERR_CODE_IO,msg,obj,std_errno)
#define ERR_IO_FILE(fileptr) construct_error_errno(ERR_CODE_IO, ferror(fileptr), NULL, NULL, __FILE__, __LINE__)
#define ERR_IO_FILE_NAME(fileptr,name) construct_error_errno(ERR_CODE_IO, ferror(fileptr), name, "", __FILE__, __LINE__)
#define ERR_IO_FILE_MSG(fileptr,name,msg) construct_error_errno(ERR_CODE_IO, ferror(fileptr), name, msg, __FILE__, __LINE__)

typedef int error_t;

const char* err_string(error_t err);

err_code_t err_code(error_t err);

error_t construct_error(err_code_t code, const char* msg, const char* file, int line);
error_t construct_error_obj(err_code_t code, const char* object_name, const char* msg, const char* file, int line);
error_t construct_error_errno(err_code_t code, int std_errno, const char* object_name, const char* msg, const char* file, int line);

void die_if_err(error_t err);
void warn_if_err(error_t err);


typedef struct {
  err_code_t code;
  const char* msg;
  const char* object_name;
  const char* file;
  int line;
  int std_errno;
} error_info_t;

error_info_t* err_info(error_t err);
#endif
