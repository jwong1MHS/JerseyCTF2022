/*
 *	Sherlock Gatherer: FastBuf as wrapper over another Fastbuf
 *
 *      - passing only words < MAX_WORD_CHARS
 *      - compressing spaces
 *
 *	(c) 2004 Tomas Holusa <tomas.holusa@netcentrum.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "ucw/fastbuf.h"
#include "ucw/unicode.h"
#include "ucw/ff-unicode.h"
#include "charset/unicat.h"
#include "gather/gather.h"

#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#define BUFSIZE MAX(1024, MAX_WORD_BYTES + 3)

struct fb_textpacker {
  struct fastbuf fb;
  struct fastbuf *out;
  byte buf[BUFSIZE];
};

#define FB_TEXTPACKER(f) ((struct fb_textpacker *)(f)->is_fastbuf)

static inline int
is_word_char(int c)
{
  return Uprint(c) && !Uspace(c);
}

static void
flush_word(struct fastbuf *out, u16 *w, int len)
{
  if (!len || len >= MAX_WORD_CHARS)      /* Enormous words are ignored instead of truncated */
    return;
  bputc(out, btell(out) ? ' ' : 0x90 | WT_TEXT);
  while (len--)
    bput_utf8(out, *w++);
}

static void
fb_textpacker_spout(struct fastbuf *f)
{
  u16 w[MAX_WORD_CHARS], *wt= w;
  int c= 0, wlen= 0;
  struct fastbuf *out= FB_TEXTPACKER(f)->out;
  byte *bcur, *b= f->buffer;

  bcur= f->buffer;
  while(bcur < f->bptr && bcur + utf8_encoding_len(*bcur) <= f->bptr)
  {
    bcur = utf8_get(bcur, &c);
    if(is_word_char(c))
    {
      if(wlen < MAX_WORD_CHARS)
	w[wlen++] = c;
    }
    else
    {
      flush_word(out, w, wlen);
      wlen = 0;
    }
  }

  /* copy last (possibly partial) word into start of buffer, will be processed on next spout() */
  while(wlen--)
    b = utf8_put(b, *wt++);

  /* append last (at most 3) unprocessed bytes into start of buffer, it can be partial-UTF8-char, will be processed on next spout() */
  memmove(b, bcur, f->bptr - bcur);
  f->bptr= b + (f->bptr - bcur);
}

static void
fb_textpacker_close(struct fastbuf *f)
{
  bputc(f, ' ');
  bflush(f);
  bflush(FB_TEXTPACKER(f)->out);
  xfree(f);
}

struct fastbuf *
fb_wrap_textpacker_out(struct fastbuf *dest)
{
  struct fastbuf *f = xmalloc_zero(sizeof(struct fb_textpacker));

  FB_TEXTPACKER(f)->out = dest;
  f->name = "<textpacker-out>";
  f->spout = fb_textpacker_spout;
  f->close = fb_textpacker_close;
  f->buffer = f->bstop = f->bptr = FB_TEXTPACKER(f)->buf;
  f->bufend = f->buffer + BUFSIZE;
  return f;
}

#ifdef TEST
int main(void)
{
  struct fastbuf *in = bfdopen(0, 7);
  struct fastbuf *out = bfdopen(1, 13);
  struct fastbuf *f = fb_wrap_textpacker_out(out);
  bbcopy_slow(in, f, ~0U);
  bclose(f);
  bclose(out);
  bclose(in);
  return 0;
}
#endif
