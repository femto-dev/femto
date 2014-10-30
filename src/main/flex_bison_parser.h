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

  femto/src/main/flex_bison_parser.h
*/
#ifndef _FLEX_BISON_PARSER_H_
#define _FLEX_BISON_PARSER_H_ 1

#include "ast.h"

/* uncomment to debug the parser */
/*
#define YYDEBUG 1
extern int yydebug;
*/

typedef struct {
  string_t string;
  int string_buf_max;
} my_extra_t;

typedef struct parser_state_s {
  struct ast_node* root_node;

  void* yyscanner; // flex scanner state.
  const char* buf;
  int pos;
  int len;

  // my extra
  my_extra_t extra;

  int errors;
} parser_state_t;

union my_yystype {
  struct ast_node* node;

  /* All of these can be cast to struct ast_node * */
  struct regexp_node* regexp;
  struct sequence_node* sequence;
  struct atom_node* atom; 
  struct set_node* set;
  struct character_node* character;
  struct string_node* string;
  struct range_node* range;
  struct boolean_node* boolean;
  struct approx_node* approx;
};

typedef union my_yystype my_yystype;

#define YYSTYPE my_yystype
#define YY_EXTRA_TYPE parser_state_t *

#define YYPARSE_PARAM state
#define YYLEX_PARAM ((parser_state_t*)state)->yyscanner

int     yylex(YYSTYPE *, void *);
int     yylex_init(void **);
int     yylex_destroy(void *);
void    yyset_extra(YY_EXTRA_TYPE, void *);
int     yyparse(void*);
//void    yyerror(void* scanner, parser_state_t* state, const char* yymsg);

#endif
