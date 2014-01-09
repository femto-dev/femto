#include "index_types.h"

#define MAX_JSON 16

// dst must have space for MAX_JSON per character.
int encode_ch_json(char* dst, int ch);
int encode_str_json(char* dst, const char* str);
// dst must have space for MAX_JSON characters
int encode_alpha_json(char* dst, alpha_t alpha);
void fprint_alpha_json(FILE* f, int len, alpha_t* pat);
void fprint_str_json(FILE* f, int len, const unsigned char* str);
void fprint_cstr_json(FILE* f, const char* str);
