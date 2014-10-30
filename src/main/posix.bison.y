%{
#include <stdlib.h>
#include <stdio.h>
#include "ast.h"
#include "flex_bison_parser.h"

#include "flex_bison_parser.c"

%}

/*This seems to only exist for Bison 3? not 2.3 at least*/
/*%define api.pure full*/ /* this is the current documented method */
/* %pure-parser seems to be the way to do it before then. */
%pure-parser

/*%parse-param {struct parser_state_s *state}*/
%parse-param {void *state}
%lex-param {yyscan_t *scanner}

/*
%token <string> STRING
%type  <string> string
*/
%token <character> T_CHARACTER
%token <set> T_ANY_PERIOD
%token <string> T_STRING
%token <range> T_REPEAT_RANGE
%token <range> T_BOOL_WITHIN
%token <range> T_BOOL_THEN
%token <approx> T_APPROX
%destructor { string_node_free($$); } T_STRING
%destructor { range_node_free($$); } T_REPEAT_RANGE
%destructor { range_node_free($$); } T_BOOL_WITHIN
%destructor { range_node_free($$); } T_BOOL_THEN
%destructor { character_node_free($$); } T_CHARACTER
%destructor { approx_node_free($$); } T_APPROX

%type <regexp> regexp_top
%type <regexp> regexp
%type <sequence> sequence
%type <atom> piece
%type <range> repeat_op
%type <atom> atom
%type <set> bracket_exp
%type <set> bracket_items
%type <range> bracket_item
%type <node> boolean_exp
%type <node> boolean_rest
%type <boolean> boolean_op

%token T_SET_START T_NEGATED_SET_START T_SET_END
%token T_GROUP_START T_GROUP_END T_REPEAT_ANY T_REPEAT_PLUS T_REPEAT_QUESTION
%token T_OR T_SET_DASH T_BOOL_AND T_BOOL_OR T_BOOL_NOT

%% /* Grammar rules and actions follow.  */

toplevel:       boolean_exp           {((parser_state_t*)state)->root_node = (struct ast_node*) $1; }
;

regexp_top:   T_APPROX regexp        { regexp_set_approx($2, $1); $$ = $2; }
              | regexp               { $$ = $1; };
;

regexp:         sequence              { $$ = regexp_node_new($1); }
              | regexp T_OR sequence  { $$ = regexp_node_add_choice($1,$3); }
;

sequence:       piece           { $$ = sequence_node_new($1); }
              | sequence piece  { $$ = sequence_node_add_atom($1,$2); }
;

piece:        atom            { $$ = $1; }
            | atom repeat_op  { struct range_node* r = $2;
                                $$ = atom_node_set_repeats($1,r->r);
                                range_node_free(r); }
            /*| atom approx_settings*/
;

repeat_op:    T_REPEAT_ANY      { $$ = range_node_new(0,UNBOUNDED_REPEATS); }
            | T_REPEAT_PLUS     { $$ = range_node_new(1,UNBOUNDED_REPEATS); }
            | T_REPEAT_QUESTION { $$ = range_node_new(0,1); }
            | T_REPEAT_RANGE    { $$ = $1; }
;

atom:         T_GROUP_START regexp T_GROUP_END  { $$ = atom_node_new((struct ast_node*) $2); }
            | bracket_exp  { $$ = atom_node_new((struct ast_node*) $1); }
            | T_CHARACTER  { $$ = atom_node_new((struct ast_node*) $1); }
            | T_ANY_PERIOD  { $$ = atom_node_new((struct ast_node*) $1); }
            | T_STRING     { $$ = atom_node_new((struct ast_node*) $1); }
;

bracket_exp:  T_SET_START bracket_items T_SET_END         { $$ = $2; }
            | T_NEGATED_SET_START bracket_items T_SET_END { $$ = $2;
                                                            set_node_invert($$);
                                                          }
;
          
bracket_items:   bracket_item              { struct range_node* r = $1;
                                             $$ = set_node_new();
                                             $$ = set_node_set($$, r->r);
                                             range_node_free(r); 
                                           }
               | bracket_items bracket_item  { struct range_node* r = $2;
                                             $$ = set_node_set($1, r->r);
                                             range_node_free(r);
                                           }
;

bracket_item: T_CHARACTER T_SET_DASH T_CHARACTER  { struct character_node* min = $1;
                                             struct character_node* max = $3;
                                             $$ = range_node_new(min->ch,max->ch);
                                             character_node_free(min);
                                             character_node_free(max);
                                           }
            | T_CHARACTER                  {
                                             struct character_node* c = $1;
                                             $$ = range_node_new(c->ch,c->ch);
                                             character_node_free(c);
                                           }
;
boolean_exp: boolean_exp boolean_op boolean_rest {
                 $$ = boolean_node_set_children($2, $1, $3); }
	    | boolean_rest { $$ = $1; }
;
boolean_rest: regexp_top { $$ = (struct ast_node*) $1; }
	     | T_GROUP_START boolean_exp boolean_op boolean_rest T_GROUP_END {
	         $$ = boolean_node_set_children($3, $2, $4); }
;

boolean_op:  T_BOOL_AND  { $$ = boolean_node_new(BOOL_AND, 0); }
	   | T_BOOL_OR   { $$ = boolean_node_new(BOOL_OR, 0); }
	   | T_BOOL_NOT  { $$ = boolean_node_new(BOOL_NOT, 0); }
	   | T_BOOL_THEN  { $$ = boolean_node_new(BOOL_THEN, $1->r.min); }
	   | T_BOOL_WITHIN { $$ = boolean_node_new(BOOL_WITHIN, $1->r.min); }						
;

%%


