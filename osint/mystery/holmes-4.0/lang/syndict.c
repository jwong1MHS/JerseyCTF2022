/*
 *	Sherlock Language Processing Library -- Synonymic Dictionaries
 *
 *	(c) 2003--2008 Martin Mares <mj@ucw.cz>
 */

#include "ucw/lib.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "lang/lang.h"

#include <fcntl.h>
#include <string.h>

void
syndict_open(struct syndict *sd)
{
  ASSERT(!sd->fb);
  sd->fb = bopen(sd->name, O_RDONLY, 4096);
}

byte **
syndict_read_entry(struct syndict *sd, struct mempool *mp)
{
  byte buf[4096], *e, *cp, **res, **r, *x;

  if (!(e = bgets(sd->fb, buf, sizeof(buf))))
    return NULL;
  cp = mp_alloc(mp, e-buf+1);
  memcpy(cp, buf, e-buf+1);
  uns cnt = 2;
  for (x=buf; *x; x++)
    if (*x == ':')
      cnt++;
  r = res = mp_alloc(mp, cnt*sizeof(byte *));
  x = cp;
  while (*x)
    {
      *r = x;
      while (*x && *x != ':')
	x++;
      if (*x)
	*x++ = 0;
      if (utf8_strlen(*r) > MAX_WORD_CHARS)
	die("Error reading synonymic dictionary %s: Word too long", sd->fb->name);
      r++;
    }
  *r++ = NULL;
  return res;
}

void
syndict_close(struct syndict *sd)
{
  bclose(sd->fb);
  sd->fb = NULL;
}
