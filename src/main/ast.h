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

  femto/src/main/ast.h
*/
#ifndef _AST_H_
#define _AST_H_ 1

/**
  This file and its companion ast.c describe the abstract syntax
  tree (AST) used by the querying code. Each node in the AST
  is a "subclass" of struct ast_node - that means that its first
  structure member is struct ast_node so it can always be cast
  to struct ast_node. That structure - struct ast_node - contains a
  type, and so it is possible to find out the type of any AST node
  given a generic pointer and then to recast it.

  The functions in this class operate with the lexer.
  */

// Used by the lexer:
#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "index_types.h"
#include "buffer.h"

typedef struct {
  int len;
  alpha_t* chars;
} string_t;
string_t construct_hex_string(char* hex);
string_t construct_quoted_string(char* str);
string_t dup_string(string_t str);
void free_string(string_t lit);

static inline
string_t construct_c_string(char* string)
{
  string_t ret;
  ret.len = strlen(string);
  ret.chars = strtoalpha(ret.len, (unsigned char*) string);
  return ret;
}

// Used by the parser:
typedef enum {
  AST_NODE_REGEXP = 1,
  AST_NODE_SEQUENCE,
  AST_NODE_ATOM,
  AST_NODE_SET,
  AST_NODE_CHARACTER,
  AST_NODE_STRING,
  AST_NODE_RANGE,
  AST_NODE_BOOL,
  AST_NODE_APPROX
} ast_node_type_t;

struct ast_node {
  ast_node_type_t type;
  char is_reversed;
};

typedef struct {
  int max;
  int num;
  struct ast_node** list;
} node_list_t;
void append_node_list(node_list_t* list, struct ast_node* elt);
void remove_node_list(node_list_t* targetList,int index);

struct string_node {
  struct ast_node node;
  string_t string;
};
struct string_node* string_node_new(string_t string);
void string_node_free(struct string_node* n );

typedef struct {
  int min;
  int max;
} range_t;

extern range_t period_range;

#define UNBOUNDED_REPEATS INT_MAX
struct range_node {
  struct ast_node node;
  range_t r;
};
struct range_node* range_node_new(int min, int max);
struct range_node* range_node_new_r(range_t r);
void range_node_free(struct range_node* r);

struct character_node {
  struct ast_node node;
  alpha_t ch;
};
/* ch is 0 is ascii 0, can be negative */
struct character_node* character_node_new(int ch);
void character_node_free(struct character_node* n);

 
#define SET_NODE_CHUNKS (ALPHA_SIZE_DIV64)
struct set_node {
  struct ast_node node;
  uint64_t bits[SET_NODE_CHUNKS];
};
struct set_node* set_node_new(void);
struct set_node* set_node_set(struct set_node* n, range_t r);
struct set_node* set_node_set_one(struct set_node* n, alpha_t ch);
struct set_node* set_node_invert(struct set_node* n);


struct atom_node {
  struct ast_node node;
  struct ast_node* child; // maybe REGEXP, SET, CHARACTER, or STRING
  range_t repeat;
};
struct atom_node* atom_node_new(struct ast_node* a);
struct atom_node* atom_node_set_repeats(struct atom_node* n, range_t r);


struct sequence_node {
  struct ast_node node;
  // made up of many atoms to be run
  node_list_t atoms;
};
struct sequence_node* sequence_node_new(struct atom_node* child);
struct sequence_node* sequence_node_new_empty(void);
struct sequence_node* sequence_node_add_atom(struct sequence_node* n, struct atom_node* child);

struct approx_node {
  struct ast_node node;
  regexp_settings_t s;
};

struct regexp_node {
  struct ast_node node;
  regexp_settings_t s;
  // made up of many sequence_node alternatives
  node_list_t choices;
};
struct regexp_node* regexp_node_new(struct sequence_node* child);
struct regexp_node* regexp_node_add_choice(struct regexp_node* n, struct sequence_node* child);

struct approx_node* approx_node_new(const char* text);
void approx_node_free(struct approx_node* n);
void regexp_set_approx(struct regexp_node* n, struct approx_node* a);

/****BOOLEAN NODE FUNCTIONS****/

typedef enum{
  BOOL_AND = 1,
  BOOL_OR,
  BOOL_NOT,
  BOOL_THEN,
  BOOL_WITHIN
} bool_op_t;

struct boolean_node {
  struct ast_node node;
  struct ast_node* left;
  struct ast_node* right;
  bool_op_t nodeType;
  int distance;
};
struct ast_node* boolean_node_set_children( struct boolean_node* root,
					    struct ast_node* leftEle,
					    struct ast_node* rightEle ); 
struct boolean_node* boolean_node_new( bool_op_t newType, int dist );
/****END BOOLEAN NODE FUNCTIONS****/


void free_ast_node(struct ast_node* n);
void print_ast_node(FILE* f, struct ast_node* n, int indent);
error_t ast_to_buf(struct ast_node* n, buffer_t* buf, int noncapturinggroups);
// You must free the returned string.
char* ast_to_string(struct ast_node* n, int noncapturinggroups);
struct ast_node* parse_file(FILE* f);
struct ast_node* parse_string(int len, const char* data);

void reverse_regexp(struct ast_node* r);
void icase_ast(struct ast_node** n);

#endif
