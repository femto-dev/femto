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

  femto/src/main/ast.c
*/
#include "ast.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>

#include "util.h"
#include "bit_array.h"
#include "buffer_funcs.h"

range_t period_range = {CHARACTER_OFFSET + 0, CHARACTER_OFFSET + 255};

/**
  Given a string which contains hexadecimal characters, 
  this function will return a binary string. 
  For example, given "af", it will return a string with one
  character - 0xAF.
  */
string_t construct_hex_string(char* hex)
{
  int i;
  int digits;
  char xchars[3];
  int b;
  unsigned int temp;
  string_t ret;

  digits = 0;
  // go through hex looking for hex chars.
  for( i = 0; hex[i]; i++ ) {
    if( isxdigit( (unsigned char) hex[i]) ) {
      digits++;
    }
  }

  // if digits is odd, we'll just ignore the last part.
  ret.len = digits / 2;
  ret.chars = malloc(sizeof(alpha_t) * ret.len);
  b = 0;

  digits = 0;

  // now read the number, one byte at a time.
  for( i = 0; hex[i] && b < ret.len; i++ ) {
    if( isxdigit( (unsigned char) hex[i]) ) {
      if( digits & 1 ) {
        xchars[1] = hex[i];
        xchars[2] = '\0';
        // save in digits/2
        sscanf(xchars, "%x", &temp);
        ret.chars[b] = CHARACTER_OFFSET + temp;
        b++;
      } else {
        xchars[0] = hex[i];
        xchars[1] = 0;
      }
      digits++;
    }
  }

  // if odd, we just leave off the last one.
  return ret;
}

string_t construct_quoted_string(char* str)
{
  string_t ret;
  int i;

  ret.len = strlen(str);
  ret.chars = malloc(sizeof(alpha_t) * ret.len);
  for( i = 0; str[i]; i++ ) {
    ret.chars[i] = CHARACTER_OFFSET + str[i];
  }
  return ret;
}

string_t dup_string(string_t str)
{
  string_t ret;
  int i;
  ret.len = str.len;
  ret.chars = malloc(sizeof(alpha_t) * str.len);
  for( i = 0; i < str.len; i++ ) {
    ret.chars[i] = str.chars[i];
  }
  return ret;
}

/** Free a string. 
  */
void free_string(string_t lit)
{
  free(lit.chars);
}

/** Append a node to a node list. 
  */
void append_node_list(node_list_t* list, struct ast_node* elt)
{
  if( list->num + 1 >= list->max ) {
    list->max = 2 * (list->num + 1);
    list->list = realloc(list->list, list->max * sizeof(struct ast_node*));
  }
  list->list[list->num] = elt;
  list->num++;
}

/** Remove a node from a node list. This function also frees
    the ast node at that index.
    */
void remove_node_list(node_list_t* targetList,int index)
{
  assert(targetList->num > 0 && index < targetList->num);
  free_ast_node( targetList->list[index] );
  // move things after index to the left
  for(int i = index; i < targetList->num - 1; i++ )
  {
    targetList->list[i] = targetList->list[i+1];
  }
  
  targetList->num--;
}

/** Construct a new regular expression AST node. */
struct regexp_node* regexp_node_new(struct sequence_node* child)
{
  struct regexp_node* ret = malloc(sizeof(struct regexp_node));
  memset(ret, 0, sizeof(struct regexp_node));
  ret->node.type = AST_NODE_REGEXP;
  set_default_regexp_settings(&ret->s);
  assert(child->node.type == AST_NODE_SEQUENCE);
  append_node_list(&ret->choices, (struct ast_node*) child);
  return ret;
}

struct regexp_node* regexp_node_add_choice(struct regexp_node* n, struct sequence_node* child)
{
  assert(n->node.type == AST_NODE_REGEXP);
  assert(child->node.type == AST_NODE_SEQUENCE);
  append_node_list(&n->choices, (struct ast_node*) child);
  return n;
}

struct approx_node* approx_node_new(const char* text)
{
  struct approx_node* ret = malloc(sizeof(struct approx_node));
  regexp_settings_t s;
  memset(ret, 0, sizeof(struct approx_node));
  ret->node.type = AST_NODE_APPROX;

  set_default_regexp_settings(&s);

  // allow one error by default.
  s.cost_bound++;

  // approx arguments are in terms of # errors.
  s.cost_bound--;

  sscanf(text, "%*[a-zA-Z] %i:%i:%i:%i",
         &s.cost_bound,
         &s.subst_cost,
         &s.delete_cost,
         &s.insert_cost);

  s.cost_bound++;

  ret->s = s;
  return ret;
}
void approx_node_free(struct approx_node* n)
{
  free(n);
}


void regexp_set_approx(struct regexp_node* n, struct approx_node* a)
{
  n->s = a->s;
}

struct sequence_node* sequence_node_new_empty(void)
{
  struct sequence_node* ret = malloc(sizeof(struct sequence_node));
  memset(ret, 0, sizeof(struct sequence_node));
  ret->node.type = AST_NODE_SEQUENCE;
  return ret;
}


struct sequence_node* sequence_node_new(struct atom_node* child)
{
  struct sequence_node* ret = malloc(sizeof(struct sequence_node));
  memset(ret, 0, sizeof(struct sequence_node));
  ret->node.type = AST_NODE_SEQUENCE;
  assert(child->node.type == AST_NODE_ATOM);
  append_node_list(&ret->atoms, (struct ast_node*) child);
  return ret;
}

struct sequence_node* sequence_node_add_atom(struct sequence_node* n, struct atom_node* child)
{
  assert(n->node.type == AST_NODE_SEQUENCE);
  assert(child->node.type == AST_NODE_ATOM);
  append_node_list(&n->atoms, (struct ast_node*) child);
  return n;
}

struct atom_node* atom_node_new(struct ast_node* a)
{
  struct atom_node* ret = malloc(sizeof(struct atom_node));
  memset(ret, 0, sizeof(struct atom_node));
  ret->node.type = AST_NODE_ATOM;
  ret->child = a;
  ret->repeat.min = 1;
  ret->repeat.max = 1;
  return ret;

}

struct atom_node* atom_node_set_repeats(struct atom_node* n, range_t r)
{
  assert(n->node.type == AST_NODE_ATOM);
  //printf("setting repeats %p [%i,%i]\n", n, r.min, r.max);
  n->repeat = r;
  return n;
}

struct character_node* character_node_new(int ch)
{
  struct character_node* ret = malloc(sizeof(struct character_node));
  memset(ret, 0, sizeof(struct character_node));
  ret->node.type = AST_NODE_CHARACTER;
  ret->ch = CHARACTER_OFFSET + ch;
  return ret;
}
void character_node_free(struct character_node* n)
{
  free(n);
}

struct string_node* string_node_new(string_t string)
{
  struct string_node* ret = malloc(sizeof(struct string_node));
  memset(ret, 0, sizeof(struct string_node));
  ret->node.type = AST_NODE_STRING;
  ret->string = string;
  return ret;
}
void string_node_free(struct string_node* n )
{
  free_string(n->string);
  free(n);
}

struct range_node* range_node_new(int min, int max)
{
  struct range_node* ret = malloc(sizeof(struct range_node));
  memset(ret, 0, sizeof(struct range_node));
  ret->node.type = AST_NODE_RANGE;
  ret->r.min = min;
  ret->r.max = max;
  return ret;
}
struct range_node* range_node_new_r(range_t r)
{
  return range_node_new(r.min, r.max);
}

void range_node_free(struct range_node* r)
{
  free(r);
}

struct set_node* set_node_new(void)
{
  struct set_node* ret = malloc(sizeof(struct set_node));
  memset(ret, 0, sizeof(struct set_node));
  ret->node.type = AST_NODE_SET;
  return ret;
}

struct set_node* set_node_set(struct set_node* n, range_t r)
{
  int ch;
  for( ch = r.min; ch <= r.max; ch++ ) {
    ch = ch;
    if( 0 <= ch && ch < ALPHA_SIZE ) {
      set_bit(SET_NODE_CHUNKS, n->bits, ch);
    }
  }
  return n;
}
struct set_node* set_node_set_one(struct set_node* n, alpha_t ch)
{
  if( 0 <= ch && ch < ALPHA_SIZE ) {
    set_bit(SET_NODE_CHUNKS, n->bits, ch);
  }
  return n;
}

struct set_node* set_node_invert(struct set_node* n)
{
  uint64_t zero_through_ff[SET_NODE_CHUNKS];

  // Set up bit array storing 0x00-0xff set.
  for( int i = 0; i < SET_NODE_CHUNKS; i++ ) zero_through_ff[i] = 0;

  for( int i = 0; i <= 0xff; i++ ) {
    int ch = i + CHARACTER_OFFSET;
    set_bit(SET_NODE_CHUNKS, zero_through_ff, ch);
  }

  invert_bit_array(SET_NODE_CHUNKS, n->bits);
  bit_array_and(SET_NODE_CHUNKS, n->bits, n->bits, zero_through_ff);

  return n;
}

void print_indent(FILE* f, int indent)
{
  for( int i = 0; i < indent; i++ ) fprintf(f, " ");
}

void free_node_list(node_list_t* list)
{
  for( int i = 0; i < list->num; i++ ) {
    free_ast_node(list->list[i]);
  }
  free(list->list);
}

void reverse_node_list(node_list_t* list)
{
  reverse_array(list->list, list->num, sizeof(struct ast_node*));
}
void reverse_string(string_t* string)
{
  reverse_array(string->chars, string->len, sizeof(alpha_t));
}

void reverse_regexp_node_list(node_list_t* list)
{
  for( int i = 0; i < list->num; i++ ) {
    reverse_regexp(list->list[i]);
  }
}

/** Reverse a regular expression. For example, given
    a(b|c)d*ef, this function returns
    fed*(b|c)a.
    */
void reverse_regexp(struct ast_node* n)
{
  if( n->is_reversed ) return;

  // go through all of the nodes.
  // When we get to a sequence_list, reverse it.
  switch(n->type) {
    case AST_NODE_REGEXP:
     {
       struct regexp_node* r = (struct regexp_node*) n;

       reverse_regexp_node_list(&r->choices);
     }
     break;
    case AST_NODE_SEQUENCE:
     {
       struct sequence_node* s = (struct sequence_node*) n;

       reverse_node_list(&s->atoms);
       reverse_regexp_node_list(&s->atoms);
     }
     break;
    case AST_NODE_ATOM:
     {
       struct atom_node* a = (struct atom_node*) n;

       reverse_regexp(a->child);
     }
     break;
    case AST_NODE_SET:
     {
       //struct set_node* s = (struct set_node*) n;
       // do nothing..
     }
     break;
    case AST_NODE_CHARACTER:
     {
       //struct character_node* c = (struct character_node*) n;
       // do nothing
     }
     break;
    case AST_NODE_STRING: 
     {
       struct string_node* s = (struct string_node*) n;
     
       reverse_string(& s->string);  
     }
     break;
    case AST_NODE_RANGE:
     { 
       //struct range_node* r = (struct range_node*) n;

       // do nothing
     }
     break;
    case AST_NODE_APPROX:
     {
       // do nothing
     }
     break;
    default:
      assert(0);
  }

  n->is_reversed = 1;
}


static
void do_icase_ast(struct ast_node** n_ptr);

static
void icase_regexp_node_list(node_list_t* list)
{
  for( int i = 0; i < list->num; i++ ) {
    do_icase_ast(& list->list[i]);
  }
}

/** Turn a regular expression case-insensitive.
    */
static
void do_icase_ast(struct ast_node** n_ptr)
{
  struct ast_node* n = *n_ptr;

  // go through all of the nodes.
  // When we get to a sequence_list, reverse it.
  switch(n->type) {
    case AST_NODE_REGEXP:
     {
       struct regexp_node* r = (struct regexp_node*) n;

       icase_regexp_node_list(&r->choices);
     }
     break;
    case AST_NODE_SEQUENCE:
     {
       struct sequence_node* s = (struct sequence_node*) n;

       icase_regexp_node_list(&s->atoms);
     }
     break;
    case AST_NODE_ATOM:
     {
       struct atom_node* a = (struct atom_node*) n;

       do_icase_ast(&a->child);
     }
     break;
    case AST_NODE_SET:
     {
       struct set_node* s = (struct set_node*) n;
       for( int i = 0; i < ALPHA_SIZE; i++ ) {
         if( get_bit(SET_NODE_CHUNKS, s->bits, i) ) {
           set_node_set_one(s, toloweralpha(i));
           set_node_set_one(s, toupperalpha(i));
         }
       }
     }
     break;
    case AST_NODE_CHARACTER:
     {
       struct character_node* c = (struct character_node*) n;
       struct set_node* s = set_node_new();
       
       set_node_set_one(s, toloweralpha(c->ch));
       set_node_set_one(s, toupperalpha(c->ch));

       free_ast_node((struct ast_node*) c);

       *n_ptr = (struct ast_node*) s;
     }
     break;
    case AST_NODE_STRING: 
     {
       struct string_node* str = (struct string_node*) n;
       struct sequence_node* q = sequence_node_new_empty();
       
       for( int i = 0; i < str->string.len; i++ ) {
         struct set_node* s;
         struct atom_node* a;
         alpha_t ch = str->string.chars[i];
         s = set_node_new();
         set_node_set_one(s, toloweralpha(ch));
         set_node_set_one(s, toupperalpha(ch));
         a = atom_node_new((struct ast_node*) s);
         sequence_node_add_atom(q, a);
       }

       free_ast_node((struct ast_node*) str);
       *n_ptr = (struct ast_node*) q;
     }
     break;
    case AST_NODE_RANGE:
     { 
       //struct range_node* r = (struct range_node*) n;

       // do nothing
     }
     break;
    case AST_NODE_APPROX:
     {
       // do nothing
     }
     break;
    case AST_NODE_BOOL:
     {
       struct boolean_node* b = (struct boolean_node*) n;

       // icase the left
       do_icase_ast(& b->left);
       // icase the right
       do_icase_ast(& b->right);
    }
    break;
    default:
      assert(0);
  }
}

void icase_ast(struct ast_node** n_ptr)
{
  struct ast_node* n;
  do_icase_ast(n_ptr);
  n = *n_ptr;
  // Now, if we turned a toplevel or under Boolean
  // AST_NODE_STRING into an AST_NODE_SEQUENCE,
  // put the AST_NODE_REGEXP back in.
  switch(n->type) {
    case AST_NODE_SEQUENCE:
     {
       struct sequence_node* s = (struct sequence_node*) n;
       struct regexp_node* r = regexp_node_new(s);
       *n_ptr = (struct ast_node*) r;
     }
     break;
    case AST_NODE_BOOL:
     {
       struct boolean_node* b = (struct boolean_node*) n;

       // icase the left
       icase_ast(& b->left);
       // icase the right
       icase_ast(& b->right);
    }
    break;
    default:
    break;
  }
}


/** Returns an array of pointers into an AST for everything other than a Boolean
 *  query (ie so that the non-Boolean parts can be counted separately).
 *  *results must start out as NULL, *nresults as 0.
 */
error_t unbool_ast(struct ast_node* n, struct ast_node*** results, int* nresults)
{
  error_t err = 0;
  // go through all of the nodes.
  // When we get to a sequence_list, reverse it.
  switch(n->type) {
    case AST_NODE_BOOL:
      {
       struct boolean_node* b = (struct boolean_node*) n;

       err = unbool_ast(b->left, results, nresults);
       if( err ) return err;
       err = unbool_ast(b->right, results, nresults);
       if( err ) return err;
      }
      break;
    default:
     return append_array(nresults, results, sizeof(struct ast_node*), &n);
     break;
  }
  return 0;
}

int ast_is_bool(struct ast_node* n)
{
  switch(n->type) {
    case AST_NODE_BOOL:
      return 1;
    default:
      return 0;
  }
}

/****BOOLEAN NODE FUNCTIONS****/
struct boolean_node* boolean_node_new(bool_op_t newType, int dist)
{
  struct boolean_node* ret = malloc(sizeof(struct boolean_node));
  memset(ret, 0, sizeof(struct boolean_node));
  ret->node.type = AST_NODE_BOOL;
  ret->nodeType = newType;
  ret->distance = dist;
  ret->left = NULL;
  ret->right = NULL;
  return ret;
}

struct ast_node* boolean_node_set_children( struct boolean_node* root,
						struct ast_node* leftEle,
						struct ast_node* rightEle )
{
  if( root == NULL )
  {
    return NULL;
  }
  root->left = leftEle;
  root->right = rightEle;
  return (struct ast_node*)root;
}
/****END BOOLEAN NODE FUNCTIONS****/

/** Free an AST node. 
  */
void free_ast_node(struct ast_node* n)
{
  switch(n->type) {
    case AST_NODE_REGEXP:
     {
       struct regexp_node* r = (struct regexp_node*) n;

       free_node_list(&r->choices);
       free(r);
     }
     break;
    case AST_NODE_SEQUENCE:
     {
       struct sequence_node* s = (struct sequence_node*) n;

       free_node_list(&s->atoms);
       free(s);
     }
     break;
    case AST_NODE_ATOM:
     {
       struct atom_node* a = (struct atom_node*) n;

       free_ast_node(a->child);
       free(a);
     }
     break;
    case AST_NODE_SET:
     {
       struct set_node* s = (struct set_node*) n;

       free(s);

     }
     break;
    case AST_NODE_CHARACTER:
     {
       struct character_node* c = (struct character_node*) n;
       
       character_node_free(c);
     }
     break;
    case AST_NODE_STRING: 
     {
       struct string_node* s = (struct string_node*) n;
       
       string_node_free(s);
     }
     break;
    case AST_NODE_RANGE:
     { 
       struct range_node* r = (struct range_node*) n;

       range_node_free(r);
     }
     break;
    case AST_NODE_BOOL:
     {
       struct boolean_node* b = (struct boolean_node*) n;
       free_ast_node( (b->left) );
       free_ast_node( (b->right) );
       free(b);
     }
     break;
    case AST_NODE_APPROX:
     {
       struct approx_node* a = (struct approx_node*) n;
       approx_node_free(a);
     }
     break;
    default:
     assert(0);
  }

}

void print_ast_node(FILE* f, struct ast_node* n, int indent)
{
  // print out the indenting
  print_indent(f, indent);

  switch(n->type) {
    case AST_NODE_REGEXP:
     {
       struct regexp_node* r = (struct regexp_node*) n;
       fprintf(f, "AST_NODE_REGEXP: ");
       if( r->s.cost_bound > 1 ) {
         fprintf(f, "cost_bound=%i subst=%i delete=%i insert=%i ",
                 r->s.cost_bound,
                 r->s.subst_cost,
                 r->s.delete_cost,
                 r->s.insert_cost);
       }
       fprintf(f, "{\n");
       for( int i = 0; i < r->choices.num; i++ ) {
         print_ast_node(f, r->choices.list[i], indent+2);
       }
       print_indent(f, indent);
       fprintf(f, "}\n");
     }
     break;
    case AST_NODE_SEQUENCE:
     {
       struct sequence_node* s = (struct sequence_node*) n;
       fprintf(f, "AST_NODE_SEQUENCE: {\n");
       for( int i = 0; i < s->atoms.num; i++ ) {
         print_ast_node(f, s->atoms.list[i], indent+2);
       }
       print_indent(f, indent);
       fprintf(f, "}\n");
     }
     break;
    case AST_NODE_ATOM:
     {
       struct atom_node* a = (struct atom_node*) n;
       fprintf(f, "AST_NODE_ATOM: repeats [%i-", a->repeat.min);
       if( a->repeat.max != UNBOUNDED_REPEATS ) fprintf(f, "%i", a->repeat.max);
       fprintf(f, "] {\n");
       print_ast_node(f, a->child, indent+2);
       print_indent(f, indent);
       fprintf(f, "}\n");
     }
     break;
    case AST_NODE_SET:
     {
       struct set_node* s = (struct set_node*) n;
       fprintf(f, "AST_NODE_SET: [");
       for( int i = 0; i < SET_NODE_CHUNKS; i++ ) {
         for( int j = 0; j < 64; j++ ) {
           int bit_idx = 64*i + j;
           if( get_bit(SET_NODE_CHUNKS, s->bits, bit_idx) ) {
             int ch = 64*i + j - CHARACTER_OFFSET;
             if( isgraph(ch) && ch > 0 ) fprintf(f, "%c", ch);
             else fprintf(f, "\\x%02x", ch);
           }
         }
       }
       fprintf(f, "] \n");
     }
     break;
    case AST_NODE_CHARACTER:
     {
       struct character_node* c = (struct character_node*) n;
       int ch;
       fprintf(f, "AST_NODE_CHARACTER: ");
       ch = c->ch - CHARACTER_OFFSET;
       fprintf(f, "%#x", ch);
       if( isgraph(ch) ) fprintf(f, " '%c'", ch);
       fprintf(f,"\n");
     }
     break;
    case AST_NODE_STRING: 
     {
       struct string_node* s = (struct string_node*) n;
       int ch;
       fprintf(f, "AST_NODE_STRING: \"");
       for( int i = 0; i < s->string.len; i++ ) {
         ch = s->string.chars[i] - CHARACTER_OFFSET;
         if( isgraph(ch) ) fprintf(f, "%c", ch);
         else fprintf(f, "\\x%02x", ch);
       }
       fprintf(f, "\"\n");
     }
     break;
    case AST_NODE_RANGE:
     { 
       struct range_node* r = (struct range_node*) n;
       fprintf(f, "AST_NODE_RANGE: %i-%i\n", r->r.min, r->r.max);
     }
     break;
    case AST_NODE_BOOL:
     {
       struct boolean_node* b = (struct boolean_node*) n;
       char* type;
       fprintf(f, "AST_NODE_BOOL: ");
       switch( b->nodeType) {
         case BOOL_AND:
	   type = "BOOL_AND";
	 break;
	 case BOOL_OR:
	   type = "BOOL_OR";
	 break;
	 case BOOL_NOT:
	   type = "BOOL_NOT";
	 break;
	 case BOOL_THEN:
	   type = "BOOL_THEN";
           
	 break;
	 case BOOL_WITHIN:
	   type = "BOOL_WITHIN";
	 break;
	 default:
	   type = "BOOL_UNKNOWN";
	   assert(0);
       }
       fprintf(f, "%s %i\n", type, b->distance);
       print_indent(f, indent);
       fprintf(f, " LEFT:\n");
       print_ast_node(f, b->left, indent+2);
       print_indent(f, indent);
       fprintf(f, " RIGHT:\n");
       print_ast_node(f, b->right, indent+2);
     }
     break;
    case AST_NODE_APPROX:
     { 
       struct approx_node* r = (struct approx_node*) n;
       fprintf(f, "AST_NODE_APPROX: cost_bound=%i subst=%i delete=%i insert=%i\n",
                   r->s.cost_bound,
                   r->s.subst_cost,
                   r->s.delete_cost,
                   r->s.insert_cost);
     }
     break;
    default:
     fprintf(f, "Unknown AST node type %i\n", n->type);
  }

}

static int ast_in_re = 0;
static int ast_in_set = 1;
static int ast_in_dquotes = 2;
static void ast_char_append(buffer_t* buf, int ch, int context)
{
   char* needs_escape = "[]()|*+?-{}.'\"\\";
   char* needs_escape_set = "]-\\";
   int chr = ch - CHARACTER_OFFSET;
   assert(buf->len < buf->max);
   if( (context == ast_in_dquotes && chr == '"') ||
       (context == ast_in_set && chr > 0 && strchr(needs_escape_set, chr)) ||
       (context == ast_in_re && chr > 0 && strchr(needs_escape, chr)) ) {
     buf->data[buf->len++] = '\\';
     buf->data[buf->len++] = chr;
   } else if( chr > 0 && (isgraph( chr ) || chr == ' ') ) {
     buf->data[buf->len++] = ch - CHARACTER_OFFSET;
   } else {
     if( chr < 0 ) {
       buf->len += sprintf((char*) &buf->data[buf->len], "\\x-%02x", -chr);
     } else {
       buf->len += sprintf((char*) &buf->data[buf->len], "\\x%02x", chr);
     }
   }

}

error_t ast_to_buf(struct ast_node* n, buffer_t* buf, int noncapturing, int usequotes)
{
  char* groupstart = "(";
  int groupstartlen;
  if( noncapturing ) groupstart = "(?:";
  groupstartlen = strlen(groupstart);

  // make sure buffer has lots of room.
  error_t err;
  err = buffer_extend(buf, 2048);
  if( err ) return err;

  switch(n->type) {
    case AST_NODE_REGEXP:
     {
       struct regexp_node* r = (struct regexp_node*) n;
       if( r->choices.num > 1 ) {
         memcpy(&buf->data[buf->len], groupstart, groupstartlen);
         buf->len += groupstartlen;
         for( int i = 0; i < r->choices.num; i++ ) {
           err = ast_to_buf(r->choices.list[i], buf, noncapturing, usequotes);
           if( err ) return err;
           if( i != r->choices.num - 1 ) {
             buf->data[buf->len++] = '|';
           }
         }
         buf->data[buf->len++] = ')';
       } else if( r->choices.num > 0 ) {
         err = ast_to_buf(r->choices.list[0], buf, noncapturing, usequotes);
         if( err ) return err;
       }
     }
     break;
    case AST_NODE_SEQUENCE:
     {
       struct sequence_node* s = (struct sequence_node*) n;
       for( int i = 0; i < s->atoms.num; i++ ) {
         err = ast_to_buf(s->atoms.list[i], buf, noncapturing, usequotes);
         if( err ) return err;
       }
     }
     break;
    case AST_NODE_ATOM:
     {
       struct atom_node* a = (struct atom_node*) n;
       int justone = (a->repeat.min == 1 && a->repeat.max == 1);
       if( a->child->type == AST_NODE_CHARACTER ) justone = 1;

       if( !justone ) {
         memcpy(&buf->data[buf->len], groupstart, groupstartlen);
         buf->len += groupstartlen;
       }

       // print out the atom
       err = ast_to_buf(a->child, buf, noncapturing, usequotes);
       if( err ) return err;

       if( !justone ) buf->data[buf->len++] = ')';

       // print out the repeat operator
       if( a->repeat.max == UNBOUNDED_REPEATS ) {
         // *, +, or {8,}
         if( a->repeat.min == 0 ) {
           buf->data[buf->len++] = '*';
         } else if ( a->repeat.min == 1 ) {
           buf->data[buf->len++] = '+';
         } else {
           buf->len += sprintf( (char*) &buf->data[buf->len], "{%i,}", a->repeat.min);
         }
       } else {
         // "" ? {0,3} {4}
         if( a->repeat.min == 1 && a->repeat.max == 1 ) {
           // print no repeat operator
         } else if( a->repeat.min == 0 && a->repeat.max == 1 ) {
           buf->data[buf->len++] = '?';
         } else if( a->repeat.min == a->repeat.max ) {
           buf->len += sprintf( (char*) &buf->data[buf->len], "{%i}", a->repeat.min);
         } else {
           buf->len += sprintf( (char*) &buf->data[buf->len], "{%i,%i}", a->repeat.min, a->repeat.max);
         }
       }

     }
     break;
    case AST_NODE_SET:
     {
       struct set_node* s = (struct set_node*) n;
       int nset = 0;
       int lastset = 0;
       int is_period = 1;
       int chr = 1;
       int run_start, run_after;

       for( int i = 0; i < ALPHA_SIZE; i++ ) {
         chr = i;
         if( get_bit(SET_NODE_CHUNKS, s->bits, i) ) {
           nset++;
           lastset = i;
           // it's not a period if the set bit is outside of the period range.
           if( period_range.min > chr || chr > period_range.max ) {
             is_period = 0;
           }
         } else {
           // it's not a period if the not set bit is in the period range.
           if( period_range.min <= chr && chr <= period_range.max ) {
             is_period = 0;
           }
         }
       }
       
       if( is_period ) {
         buf->data[buf->len++] = '.';
       } else if( nset == 1 ) {
         ast_char_append(buf, lastset, ast_in_set);
       } else {
         buf->data[buf->len++] = '[';
         // Try turning it in to ranges...
         for( run_start = 0; run_start < ALPHA_SIZE; ) {
           run_after = run_start + 1;
           if( get_bit(SET_NODE_CHUNKS, s->bits, run_start) ) {
             while( run_after < ALPHA_SIZE &&
                     get_bit(SET_NODE_CHUNKS, s->bits, run_after) ) {
               run_after++;
             }
             if( run_after - run_start == 1 ) {
               ast_char_append(buf, run_start, ast_in_set);
             } else if( run_after - run_start == 2 ) {
               ast_char_append(buf, run_start, ast_in_set);
               ast_char_append(buf, run_start + 1, ast_in_set);
             } else {
               ast_char_append(buf, run_start, ast_in_set);
               buf->data[buf->len++] = '-';
               ast_char_append(buf, run_after - 1, ast_in_set);
             }
           }
           run_start = run_after;
         }
         buf->data[buf->len++] = ']';
       }
     }
     break;
    case AST_NODE_CHARACTER:
     {
       struct character_node* c = (struct character_node*) n;
       ast_char_append(buf, c->ch, ast_in_re);
     }
     break;
    case AST_NODE_STRING: 
     {
       struct string_node* s = (struct string_node*) n;

       err = buffer_extend(buf, 5*s->string.len + 4);
       if( err ) return err;
       
       if( usequotes ) {
         // Add a space for readability if we don't already have one.
         if( buf->len > 0 && buf->data[buf->len-1] != ' ' )
           buf->data[buf->len++] = ' ';
         buf->data[buf->len++] = '"';
       }

       for( int i = 0; i < s->string.len; i++ ) {
         if( usequotes )
           ast_char_append(buf, s->string.chars[i], ast_in_dquotes);
         else
           ast_char_append(buf, s->string.chars[i], ast_in_re);
       }
       if( usequotes ) buf->data[buf->len++] = '"';
     }
     break;
    case AST_NODE_RANGE:
     { 
       struct range_node* r = (struct range_node*) n;
       r = NULL;
       assert(0);
     }
     break;
    case AST_NODE_BOOL:
     {
       struct boolean_node* b = (struct boolean_node*) n;

       // print the left
       err = ast_to_buf(b->left, buf, noncapturing, usequotes);
       if( err ) return err;

       switch( b->nodeType) {
         case BOOL_AND:
           buf->len += sprintf( (char*) &buf->data[buf->len], " AND ");
	 break;
	 case BOOL_OR:
           buf->len += sprintf( (char*) &buf->data[buf->len], " OR ");
	 break;
	 case BOOL_NOT:
           buf->len += sprintf( (char*) &buf->data[buf->len], " NOT ");
	 break;
	 case BOOL_THEN:
           buf->len += sprintf( (char*) &buf->data[buf->len], " THEN %i ", b->distance);
	 break;
	 case BOOL_WITHIN:
           buf->len += sprintf( (char*) &buf->data[buf->len], " WITHIN %i ", b->distance);
	 break;
	 default:
	   assert(0);
       }

       // print the right
       err = ast_to_buf(b->right, buf, noncapturing, usequotes);
       if( err ) return err;
     }
     break;
    default:
     assert(0);
  }

  return ERR_NOERR;
}

/**
  Converts an AST back into a string which will (hopefully) be in the
  same format as the original.
  The caller is responsible for freeing this string.
  */
char* ast_to_string(struct ast_node* n, int noncapturinggroups, int usequotes)
{
  error_t err;
  buffer_t buf = build_buffer(0, NULL);
  err = ast_to_buf(n, &buf, noncapturinggroups, usequotes);
  if( err ) goto error;

  err = buffer_extend(&buf, 1);
  // put a \0 at the end of the string
  buf.data[buf.len++] = '\0';
  return (char*) buf.data;

error:
  free(buf.data);
  return NULL;
}

static
int and_add(int a, int b)
{
  if( a == -1 ) return -1;
  if( b == -1 ) return -1;
  return a + b;
}

// Does the query actually use any regular expression functionality?
// ie. is it a simple string search?
// If pat is non-NULL, save the pattern to pat using the length in *npat,
//  which must have room (call this function twice).
// Returns the length of the simple query (if it is a simple query)
//  or -1 if it is not.
static
int get_simple_query(struct ast_node* node, alpha_t* pat, int* npat)
{
  switch(node->type)
  {
    case AST_NODE_BOOL:
      return -1;
    case AST_NODE_APPROX:
      return -1;
    case AST_NODE_RANGE:
      // Should never see this
      return -1;
    case AST_NODE_REGEXP:
    {
      struct regexp_node* rnode = (struct regexp_node*)node;
      if( rnode->choices.num == 1 && rnode->s.cost_bound <= 1 ) {
        return get_simple_query(rnode->choices.list[0], pat, npat);
      }
      return -1;
    }
    case AST_NODE_SEQUENCE:
    {
      struct sequence_node* snode = (struct sequence_node*)node;
      int count = 0;
      for( int i = 0; i < snode->atoms.num; i++ ) {
        count = and_add(count,get_simple_query(snode->atoms.list[i],pat,npat));
      }
      return count;
    }
    case AST_NODE_ATOM:
    {
      struct atom_node* anode = (struct atom_node*)node;
      if( anode->repeat.min == anode->repeat.max ) {
        int repeat = anode->repeat.min;
        int count = 0;
        for( int i = 0; i < repeat; i++ ) {
          count = and_add(count, get_simple_query(anode->child, pat, npat)); 
        }
        return count;
      }
      return -1;
    }
    case AST_NODE_SET:
    {
      struct set_node* snode = (struct set_node*)node;
      int nset = 0;
      alpha_t chr = -1;
      for( int i = 0; i < ALPHA_SIZE; i++ ) {
        if( get_bit(SET_NODE_CHUNKS, snode->bits, i) ) {
          nset++;
          chr = i;
        }
      }
      if( nset == 1 ) {
        if( pat ) {
          pat[(*npat)++] = chr;
        }
        return 1;
      }
      return -1;
    }
    case AST_NODE_CHARACTER:
    {
      struct character_node* cnode = (struct character_node*)node;
      alpha_t chr = cnode->ch;
      if( pat ) pat[(*npat)++] = chr;
      return 1;
    }
    case AST_NODE_STRING:
    {
      struct string_node* snode = (struct string_node*)node;
      alpha_t chr;
      if( pat ) {
        for( int i = 0; i < snode->string.len; i++ ) {
          chr = snode->string.chars[i];
          pat[(*npat)++] = chr;
        }
      }
      return snode->string.len;
    }
  }
  assert(0);
  return 0;
}

error_t simplify_query(struct ast_node** node)
{
  int simplecount;
  if( !node || !*node ) return 0;
  if( (*node)->type != AST_NODE_STRING ) {
    if( (*node)->type == AST_NODE_BOOL ) {
      // Simplify what's inside the BOOLEAN operation.
      struct boolean_node* b = (struct boolean_node*) *node;
      simplify_query(&b->left);
      simplify_query(&b->right);
    } else {
      simplecount = get_simple_query(*node, NULL, NULL);
      if( simplecount != -1 ) {
        alpha_t* pat = calloc(simplecount, sizeof(alpha_t));
        int npat = 0;
        string_t string;
        struct string_node* new_node;
        if( ! pat ) return ERR_MEM;
        get_simple_query(*node, pat, &npat);
        string.len = npat;
        string.chars = pat;
        // Now replace the whole query with a AST_NODE_STRING.
        new_node = string_node_new(string);
        if( !new_node ) return ERR_MEM;
        free_ast_node(*node);
        *node = (struct ast_node*) new_node;
      }
    }
  }
  return 0;
}

