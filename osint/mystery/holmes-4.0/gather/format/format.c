/*
 *	Sherlock Gatherer: Document Format Multiplexer
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *
 *	The multiplexer is split to two parts: format.c and parse.c
 *	to avoid linking of all the parsing machinery where only the
 *	list of known content types and encodings is needed.
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "gather/gather.h"

#include <string.h>

/*** Known parser names ***/

const char * const parser_names[] = {
#define P(x) #x,
#include "gather/format/parsers.h"
#undef P
NULL
};

/*** Configuration ***/

uns trace_parse, max_conversions;

static clist type_hooks, encoding_hooks;

static char *
parse_init_hook(struct parser_hook *h)
{
  h->parser = -1;
  return NULL;
}

static char *
parse_commit_hook(struct parser_hook *h)
{
  if (!h->type_patt)
    return "Missing type pattern";
  if (h->parser < 0)
    return "Missing parser name";
  return NULL;
}

static struct cf_section parse_config_hook = {
  CF_TYPE(struct parser_hook),
  CF_INIT(parse_init_hook),
  CF_COMMIT(parse_commit_hook),
  CF_ITEMS {
    CF_STRING("Type", PTR_TO(struct parser_hook, type_patt)),
    CF_LOOKUP("Parser", PTR_TO(struct parser_hook, parser), parser_names),
    CF_STRING_DYN("Args", PTR_TO(struct parser_hook, args), CF_ANY_NUM),
    CF_END
  }
};

static struct cf_section parse_config = {
  CF_ITEMS {
    CF_UNS("Trace", &trace_parse),
    CF_UNS("MaxConversions", &max_conversions),
    CF_LIST("Type", &type_hooks, &parse_config_hook),
    CF_LIST("Encoding", &encoding_hooks, &parse_config_hook),
    CF_END
  }
};

static void CONSTRUCTOR parse_init_config(void)
{
  cf_declare_section("Parse", &parse_config, 0);
}

/*** Format identification ***/

struct parser_hook *
identify_content_type(byte *type)
{
  ASSERT(type);
  CLIST_FOR_EACH(struct parser_hook *, hook, type_hooks)
    if (match_ct_patt(hook->type_patt, type))
      return hook;
  return NULL;
}

struct parser_hook *
identify_content_encoding(byte *enc)
{
  ASSERT(enc);
  CLIST_FOR_EACH(struct parser_hook *, hook, encoding_hooks)
    if (!strcasecmp(hook->type_patt, enc))
      return hook;
  return NULL;
}
