/*
 *	Sherlock Search Engine -- Lexical Analysis of Queries
 *
 *	(c) 1997--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/chartype.h"
#include "search/sherlockd.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "search/parse.tab.h"

static byte *pos;

void
lex_init(byte *buf)
{
  pos = buf;
}

struct keywd {
  char *keyword;
  uns class, val;
};

static struct keywd keywords[] = {
  { "ANY",			ANY,		0 },
  { "AND",			AND,		0 },
  { "OR",			OR,		0 },
  { "NOT",			NOT,		0 },
  { "DEBUG",			K_DEBUG,	0 },
  { "CONTROL",			CONTROL,	0 },
  { "ACCENTS",			ACCENTS,	0 },
  { "LIST",			LIST,		0 },
  { "SHOW",			SHOW,		0 },
  { "DUMP",			DUMP,		0 },
  { "CONTEXT",			CONTEXT,	0 },
  { "METALEN",			METALEN,	0 },
  { "INTERVALS",		INTERVALS,	0 },
  { "DB",			DB,		0 },
  { "SITE",			SITE,		0 },
  { "CARDID",			CARDID,		0 },
  { "URLS",			URLS,		0 },
  { "SITEMAX",			SITEMAX,	0 },
  { "PARTIAL",			PARTIAL,	0 },
  { "APPROX",			APPROX,		0 },
  { "MAYBE",			MAYBE,		0 },
  { "STATS",			STATS,		0 },
  { "SORTBY",			SORTBY,		0 },
  { "ONLY",			ONLY,		0 },
  { "MORPH",			MORPH,		0 },
  { "SPELL",			SPELL,		0 },
  { "SYN",			SYN,		0 },
  { "SYNEXP",			SYNEXP,		0 },
  { "FULL",			FULL,		0 },
  { "TYPES",			TYPES,		0 },
#define T(x,y) { #x, TYPE, y },
WORD_TYPE_NAMES
#undef T
#define T(x,y) { #x, TYPE, (y) << 16 },
META_TYPE_NAMES
#undef T
#define T(x,y) { #x, TYPE, 0x8000 | (y) },
STRING_TYPE_NAMES
#undef T
#define INT_ATTR(id,keywd,gf,pf) { #keywd, CUSTOM, OFFSETOF(struct query, id##_min) },
#define SMALL_SET_ATTR(id,keywd,gf,pf) { #keywd, CUSTOM, OFFSETOF(struct query, id##_set) },
#define LATE_INT_ATTR INT_ATTR
#define LATE_SMALL_SET_ATTR SMALL_SET_ATTR
  EXTENDED_ATTRS
#undef INT_ATTR
#undef SMALL_SET_ATTR
#undef LATE_INT_ATTR
#undef LATE_SMALL_SET_ATTR
  { "AGE",			CUSTOM,		PARAM_AGE },
  /* SITE again, now with class CUSTOM for sake of lookup_custom_attr() */
  { "SITE",			CUSTOM,		PARAM_SITE },
#ifdef CONFIG_EXPLAIN
  { "EXPLAIN",			EXPLAIN,	0 },
#endif
#ifdef CONFIG_IMAGES_SIM
  { "IMAGESIM",			IMAGESIM,	0 },
  { "SIG",			SIG,		0 },
#endif
#define CUSTOM_MATCH_KWD(id,keywd,parse) { #keywd, CUSTOM, OFFSETOF(struct query, id##_placeholder) },
  CUSTOM_MATCH_PARSE
#undef CUSTOM_MATCH_KWD
};

int
yylex(void)
{
  while (Cspace(*pos))
    pos++;
  if (!*pos)
    return EOLN;
  if (Cdigit(*pos))
    {
      uns i = 0;
      while (Cdigit(*pos))
	{
	  if (i > 214748364)
	    err("Number too large");
	  i = 10*i + *pos++ - '0';
	  if (i >= 2147483648U)
	    err("Number too large");
	}
      yylval.i = i;
      return NUM;
    }
  if (Calpha(*pos))
    {
      byte *start = pos;
      byte x;
      uns k;

      while (Calnum(*pos))
	{
	  *pos = Cupcase(*pos);
	  pos++;
	}
      x = *pos;
      *pos = 0;
      for(k=0; k<ARRAY_SIZE(keywords); k++)
	if (!strcmp(start, keywords[k].keyword))
	  {
	    *pos = x;
	    yylval.i = keywords[k].val;
	    return keywords[k].class;
	  }
      err("Unknown keyword <%s>", start);
    }
  switch (*pos++)
    {
    case '"':
      {
	byte *z = pos;
	while (*pos && *pos != '"')
	  pos++;
	if (!*pos)
	  err("Misquoted string");
	*pos++ = 0;
	yylval.s = z;
	return STRING;
      }
    case '&':
      return AND;
    case '|':
      return OR;
    case '!':
      return NOT;
    case '.':
      if (*pos == '.')
	{
	  pos++;
	  return DOTDOT;
	}
      /* fall-thru */
    case ',':
    case '/':
    case '(':
    case ')':
    case '-':
    case '=':
    case '{':
    case '}':
    case ':':
      return pos[-1];
    case '<':
      if (*pos == '=')
	{
	  pos++;
	  return LE;
	}
      else if (*pos == '>')
	{
	  pos++;
	  return NE;
	}
      return '<';
    case '>':
      if (*pos == '=')
	{
	  pos++;
	  return GE;
	}
      return '>';
    case '#':
      {
	u64 L = 0;
	if (!Cxdigit(*pos))
	  err("Invalid #ID");
	byte *start_pos = pos;
	while (Cxdigit(*pos))
	  {
	    L = (L << 4) | Cxvalue(*pos);
	    pos++;
	  }
	if (start_pos > pos + 16)
	  err("#ID too long");
	yylval.L = L;
	return ID;
      }
    default:
      err("Invalid character 0x%02x", pos[-1]);
    }
}

int
lookup_custom_attr(byte *name)
{
  uns k;

  for(k=0; k<ARRAY_SIZE(keywords); k++)
    if (keywords[k].class == CUSTOM && !strcasecmp(name, keywords[k].keyword))
      return keywords[k].val;
  return -1;
}
