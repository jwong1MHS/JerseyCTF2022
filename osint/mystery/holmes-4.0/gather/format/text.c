/*
 *	Sherlock Gatherer: Text Parser
 *
 *	(c) 2001 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "ucw/unicode.h"
#include "ucw/ff-unicode.h"
#include "charset/unicat.h"
#include "gather/gather.h"

#include <string.h>

static inline int
is_word_char(int c)
{
  return Uprint(c) && !Uspace(c);
}

static void
flush_word(struct fastbuf *out, u16 *w, int len)
{
  if (!len || len >= MAX_WORD_CHARS)	/* Enormous words are ignored instead of truncated */
    return;
  bputc(out, btell(out) ? ' ' : 0x90 | WT_TEXT);
  while (len--)
    bput_utf8(out, *w++);
}

int
text_parse(char **args UNUSED)
{
  struct fastbuf *in, *out;
  u16 w[MAX_WORD_CHARS];
  int c, wlen=0;

  convert_charset(NULL);
  in = fbmem_clone_read(gthis->contents);
  out = gthis->text = fbmem_create(16384);

  while ((c = bget_utf8(in)) >= 0)
    {
      if (is_word_char(c))
	{
	  if (wlen < MAX_WORD_CHARS)
	    w[wlen++] = c;
	}
      else
	{
	  flush_word(out, w, wlen);
	  wlen = 0;
	}
    }
  if (!gthis->truncated)
    flush_word(out, w, wlen);

  bclose(in);
  return 1;
}
