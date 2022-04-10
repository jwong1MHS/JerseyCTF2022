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
#include "gather/gather.h"

#include <string.h>

#define TRACE(x,y...) do { if (trace_parse) log(L_DEBUG, x,##y); } while (0)

/*** Known parser functions ***/

static int (*parser_functions[])(char **) = {
#define P(x) x##_parse,
#include "gather/format/parsers.h"
#undef P
};

/*** The parsing multiplexer ***/

void
gather_parse(void)
{
  /* Do not parse redirects */
  if (gthis->error_code == 1)
    return;

  uns convcnt = 0;
  struct parser_hook *p;
  byte *t;

  if (gthis->robot_file_p)
    gthis->content_type = "x-sherlock/robots";
  byte *orig_content_type = NULL;
  do
    {
      gather_filter(0);
      if (t = gthis->content_encoding)
	{
	  if (p = identify_content_encoding(gthis->content_encoding))
	    {
	      TRACE("Parsing content-encoding %s by %s", t, parser_names[p->parser]);
	      cut_inenc_suffix(gthis->url_s.rest, t);
	      goto work;
	    }
	  gerror(2403, "Unknown content encoding %s", t);
	}
      if (!(t = gthis->content_type))
	gerror(2400, "Document has no content type");
      if (gthis->robot_file_p && strcmp(gthis->content_type, "x-sherlock/robots"))
	gerror(2400, "robots.txt has invalid content-type %s", gthis->content_type);
      if (!orig_content_type)
	orig_content_type = gthis->content_type;
      p = identify_content_type(gthis->content_type);
      if (!p)
	gerror(2400, "Unknown content type %s", t);
      TRACE("Parsing content-type %s by %s", t, parser_names[p->parser]);
    work:
      if (parser_functions[p->parser](p->args))
	{
	  validate_document();
	  gobj_calc_sum();
	  gthis->content_type = orig_content_type;
	  return;
	}
      convcnt++;
    }
  while (!max_conversions || convcnt <= max_conversions);
  gerror(2402, "Too many conversions");
}

/* Internal procedure used inside all decompressors --- it replaces the
 * original stream by the decompressed one */

void
switch_content_encoding(void)
{
  obj_add_attr(gthis->aa, 'E', gthis->content_encoding);
  gthis->content_encoding = NULL;
  bclose(gthis->contents);
  gthis->contents = gthis->temp;
  gthis->temp = NULL;
}

void
switch_content_type(byte *what)
{
  obj_add_attr(gthis->aa, 'E', what ? what : gthis->content_type);
  gthis->content_type = NULL;
  bclose(gthis->contents);
  gthis->contents = gthis->temp;
  gthis->temp = NULL;
}
