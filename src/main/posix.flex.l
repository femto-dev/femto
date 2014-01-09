
%{
#include "util.h"
#include "ast.h"
#include "flex_bison_parser.h"
#include "posix.bison.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <strings.h>

#if YYDEBUG > 0 
#define LEX_DEBUG 1
#else
#define LEX_DEBUG 0
#endif

#define PARM    yyget_extra(yyscanner)

#define YY_INPUT(buffer, res, max_size)             \
do {                                                \
    if (PARM->pos >= PARM->len)                     \
      res = YY_NULL;                                \
    else                                            \
    {                                               \
      res = PARM->len - PARM->pos;                  \
      if( res > (int)max_size ) res = max_size;     \
      memcpy(buffer, PARM->buf + PARM->pos, res);   \
      PARM->pos += res;                             \
    }                                               \
} while (0)

void append_alpha(my_extra_t* ext, alpha_t ch)
{
  assert(ext->string.len <= ext->string_buf_max);
  if( ext->string.len == ext->string_buf_max ) {
    append_array(&ext->string_buf_max, &ext->string.chars, sizeof(alpha_t), &ch);
    ext->string.len++;
  } else {
    ext->string.chars[ext->string.len++] = ch;
  }
}

void append_char(my_extra_t* ext, unsigned int ch)
{
  append_alpha(ext, CHARACTER_OFFSET + ch);
}

void clear_str(my_extra_t* ext)
{
  ext->string.len = 0;
}

range_t construct_range(const char* textin)
{
  range_t ret;
  int i;
  int start;
  char end;
  char* text = strdup(textin);

  i = 1; // pass the {

  start = 1;
  // pass the digits
  for( ; text[i] && isdigit( (unsigned char) text[i]); i++ );
  end = text[i];
  text[i] = '\0';
  i++;
  sscanf(&text[start], "%i", &ret.min);
  if( end == '}' ) {
    // we're done reading.
    ret.max = ret.min;
    goto done;
  }

  if( end == ',' && text[i] == '}' ) {
    // we're done reading.
    ret.max = UNBOUNDED_REPEATS;
    goto done;
  }

  // otherwise, we've got a second range number.
  // pass the digits
  start = i;
  for( ; text[i] && isdigit( (unsigned char) text[i]); i++ );
  end = text[i];
  text[i] = '\0';
  i++;
  sscanf(&text[start], "%i", &ret.max);

done:
  //printf("Constructed range [%i,%i] from %s\n", ret.min, ret.max, textin);

  free(text);
  return ret;
}

int read_int_part(char* s, int default_value)
{
  int ret = default_value;
  sscanf(s, "%*[a-zA-Z] %i", &ret);
  return ret;
}

int handle_escape(int x)
{
  switch(x) {
    case 'n': return 0x0A; // newline
    case 't': return 0x09; // tab
    case 'r': return 0x0D; // carriage return
    case 'b': return 0x08; // backspace
    case 'f': return 0x0C; // form feed
    case 'a': return 0x07; // bell
    case 'e': return 0x1B; // escape character
    case 'v': return 0x0B; // vertical tab
    default : return x; // includes \\ and \, etc.
  }
}

int handle_hex_escape(char* str)
{
  unsigned int temp;
  sscanf(str, "%x", &temp);
  temp = (temp & 0xff);
  return temp;
}

%}

%option reentrant bison-bridge
%option noyywrap

%x doublestr
%x singlestr
%x bracket
%x word

%%

 /* handle comments */
#[^\n]*
 /* handle single-quoted strings. */
\'                      { clear_str(&PARM->extra); BEGIN(singlestr); }
<singlestr>\'           { /* closing quote - all done */
                          BEGIN(INITIAL);
                          yylval->string = string_node_new(dup_string(PARM->extra.string));
                          clear_str(&PARM->extra);
                          return T_STRING;
                       }
<singlestr>[^\']+      { int i;
                         for( i = 0; i < yyleng; i++ ) {
                           append_char(&PARM->extra, yytext[i]);
                         }
                       }

 /* handle double-quoted strings */
\"                      { clear_str(&PARM->extra); BEGIN(doublestr); }
<doublestr>\"           { /* closing quote - all done */
                          BEGIN(INITIAL);
                          yylval->string = string_node_new(dup_string(PARM->extra.string));
                          clear_str(&PARM->extra);
                          return T_STRING;
                        }

<doublestr>\\x[[:xdigit:]]{2}  { /* \x00 */
                                 append_char(&PARM->extra,
                                             handle_hex_escape(&yytext[2]));
                                }
<doublestr>"\\"(.|"\n") { append_char(&PARM->extra, handle_escape(yytext[1])); }
<doublestr>[^\\\"]+ {
                      int i;
                      for( i = 0; i < yyleng; i++ ) {
                        append_char(&PARM->extra, yytext[i]);
                      }
                    } 

 /* handle e.g. \x-01  negative characters (headers, EOF) */
"\\x-"[[:xdigit:]]{2}     { unsigned int temp;
                          // \x-00
                          sscanf(&yytext[3], "%x", &temp);
                          temp = -(temp & 0xff);
                          yylval->character = character_node_new(temp);
                          if( LEX_DEBUG ) printf("T_CHARACTER %x\n", temp);
                          return T_CHARACTER;
                        }
 /* handle e.g. \x01 */
"\\x"[[:xdigit:]]{2} {
                    /* \x00 */
                    unsigned int temp = handle_hex_escape(&yytext[2]);
                    yylval->character = character_node_new(temp);
                    if( LEX_DEBUG ) printf("\\x T_CHARACTER %x\n", temp);
                    return T_CHARACTER; }

 /* handle e.g. \\ \, \n \* etc. */
"\\"(.|"\n")      { unsigned int temp = handle_escape(yytext[1]);
                    yylval->character = character_node_new(temp);
                    if( LEX_DEBUG ) printf("\\p T_CHARACTER token %x\n", temp);
                    return T_CHARACTER; }
 /* was "\\"[[:punct:][:space:]]  */

 /* handle a negated character set start */
"[^"              { BEGIN(bracket);
                    if( LEX_DEBUG ) printf("T_NEGATED_SET_START\n");
                    yylval->node = NULL;
                    return T_NEGATED_SET_START; }
 /* handle a character set */
"["               { BEGIN(bracket);
                    if( LEX_DEBUG ) printf("T_SET_START\n"); 
                    return T_SET_START; }
 /* handle a character set end */
<bracket>"]"      { BEGIN(INITIAL);
                    if( LEX_DEBUG ) printf("T_SET_END\n");
                    yylval->node = NULL;
                    return T_SET_END; }
<bracket>"-"      { if( LEX_DEBUG ) printf("T_SET_DASH\n");
                    yylval->node = NULL;
                    return T_SET_DASH; }
<bracket>\\x[[:xdigit:]]{2}  {
                    /* \x00 */
                    unsigned int temp = handle_hex_escape(&yytext[2]);
                    yylval->character = character_node_new(temp);
                    if( LEX_DEBUG ) printf("\\x [] T_CHARACTER %x\n", temp);
                    return T_CHARACTER; }
<bracket>"\\"(.|"\n") {
                    unsigned int temp = handle_escape(yytext[1]);
                    yylval->character = character_node_new(temp);
                    if( LEX_DEBUG ) printf("\\p [] T_CHARACTER token %x\n", temp);
                    return T_CHARACTER; }
<bracket>[^\\\]\-] { unsigned int temp = yytext[0];
                     yylval->character = character_node_new(temp);
                     if( LEX_DEBUG ) printf("catchall [] T_CHARACTER token %x\n", temp);
                     return T_CHARACTER; }

 /* handle an open paren */
"("                { if( LEX_DEBUG ) printf("T_GROUP_START\n"); 
                    yylval->node = NULL;
                    return T_GROUP_START; }
 /* handle a close paren */
")"                { if( LEX_DEBUG ) printf("T_GROUP_END\n");
                    yylval->node = NULL;
                    return T_GROUP_END; }
 /* handle alternation */
"|"                { if( LEX_DEBUG ) printf("T_OR\n");
                    yylval->node = NULL;
                    return T_OR; }
"*"                { if( LEX_DEBUG ) printf("T_REPEAT_ANY\n");
                    yylval->node = NULL;
                    return T_REPEAT_ANY; }
"+"                { if( LEX_DEBUG ) printf("T_REPEAT_PLUS\n");
                    yylval->node = NULL;
                    return T_REPEAT_PLUS; }
"?"                { if( LEX_DEBUG ) printf("T_REPEAT_QUESTION\n");
                    yylval->node = NULL;
                    return T_REPEAT_QUESTION; }
"."                { if( LEX_DEBUG ) printf("T_ANY_PERIOD\n");
                    yylval->set = set_node_set(set_node_new(), period_range);
                    return T_ANY_PERIOD; }
"{x"[[:xdigit:][:space:]]*"}"   {
                    yylval->string = string_node_new(construct_hex_string(yytext));
                    if( LEX_DEBUG ) printf("{x  } T_STRING\n");
                    return T_STRING; }
"{"[[:digit:]]+,?[[:digit:]]*"}"  {
                    yylval->range = range_node_new_r(construct_range(yytext));
                    if( LEX_DEBUG ) printf("T_REPEAT_RANGE\n");
                    return T_REPEAT_RANGE; }

("AND"|"and")	  { if( LEX_DEBUG ) printf("T_BOOL_AND\n");
                    yylval->node = NULL;
                    return T_BOOL_AND; }
("OR"|"or")	  { if( LEX_DEBUG ) printf("T_BOOL_OR\n");
                    yylval->node = NULL;
                    return T_BOOL_OR; }
("NOT"|"not")	  { if( LEX_DEBUG ) printf("T_BOOL_NOT\n");
                    yylval->node = NULL;
                    return T_BOOL_NOT; }
("THEN"|"then")[[:space:]]+[[:digit:]]*	{ if( LEX_DEBUG ) printf("T_BOOL_THEN\n");
				  int x = read_int_part(yytext, INT_MAX);
				  yylval->range = range_node_new(x,x);
				  return T_BOOL_THEN; }
("WITHIN"|"within")[[:space:]]+[[:digit:]]+	{ if( LEX_DEBUG ) printf("T_BOOL_WITHIN\n");
				  int x = read_int_part(yytext, INT_MAX);
				  yylval->range = range_node_new(x,x);
				  return T_BOOL_WITHIN; }
  
("APPROX"|"approx")[[:space:]]+([[:digit:]]+(:[[:digit:]]+){0,3})?	  { if( LEX_DEBUG ) printf("T_APPROX\n");
                    yylval->approx = approx_node_new(yytext);

                    return T_APPROX; }

 /* group characters to avoid something like aORb pulling out the OR
    but if it's followed by a repeat specification, we must not
    do this, since the repeat only applies to the last character.
  */
[^[:space:][:punct:]]/[^[:space:][:punct:]][^[:space:][:punct:]]  {
                   if( LEX_DEBUG ) printf("starting T_STRING word %c\n", yytext[0]);
                        clear_str(&PARM->extra);
                        append_char(&PARM->extra, yytext[0]); 
                        BEGIN(word); }

 /* grab characters followed char followed by punctuation - this one wins
    because it is the longest match.
 */
<word>[^[:space:][:punct:]]+/[^[:space:][:punct:]][[:punct:]] {
                      int i;
                      for( i = 0; i < yyleng; i++ ) {
                        if( LEX_DEBUG ) printf("appending T_STRING word %c\n", yytext[i]);
                        append_char(&PARM->extra, yytext[i]);
                      }
                      BEGIN(INITIAL);
                      yylval->string = string_node_new(dup_string(PARM->extra.string));
                      clear_str(&PARM->extra);
                      return T_STRING;
                    }

 /* These terminate the <word> area */
<word>[[:space:]] |
<word><<EOF>> {          
                      if( LEX_DEBUG ) printf("finishing T_STRING word\n");
                      BEGIN(INITIAL);
                      yylval->string = string_node_new(dup_string(PARM->extra.string));
                      clear_str(&PARM->extra);
                      return T_STRING;

             }

 /* Any other character in <word> just gets appended */
<word>. {
                      if( LEX_DEBUG ) printf(".appending T_STRING word %c\n", yytext[0]);
                      append_char(&PARM->extra, yytext[0]);
                      }


[^[:space:]]    { unsigned int temp = yytext[0];
                    yylval->character = character_node_new(temp);
                    if( LEX_DEBUG ) printf("catchall T_CHARACTER %c token %x\n", yytext[0], temp);
                    return T_CHARACTER; }
 
[[:space:]]       /* eat up whitespace */

%%


/*
int main(int argc, char** argv)
{
  yyin = stdin;
  while(1) {
    yylex(); 
  }
}
*/
