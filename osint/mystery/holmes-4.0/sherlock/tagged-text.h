/*
 *	Sherlock Library -- Processing of tagged characters
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

#ifndef _SHERLOCK_TAGGED_TEXT_H
#define _SHERLOCK_TAGGED_TEXT_H

#include "ucw/fastbuf.h"
#include "ucw/ff-unicode.h"

/* Reading of tagged text (Unicode values, tags mapped to 0x80000000 and higher) */

static inline const byte *
get_tagged_char(const byte *p, uns *uu)
{
  uns u = *p;
  if (u >= 0xc0)
    return utf8_get(p, uu);
  else if (u >= 0x80)
    {
      p++;
      if (u >= 0xb0)
        {
	  ASSERT(u == 0xb0);
	  u += 0x80020000;
        }
      else if (u >= 0xa0)
        {
	  ASSERT(*p >= 0x80 && *p <= 0xbf);
	  u = 0x80010000 + ((u & 0x0f) << 6) + (*p++ & 0x3f);
        }
      else
	u += 0x80000000;
    }
  else
    p++;
  *uu = u;
  return p;
}

static inline const byte *
skip_tagged_char(const byte *p)
{
  if (*p >= 0x80 && *p < 0xc0)
    {
      uns u = *p++;
      if (u >= 0xa0 && u < 0xb0 && *p >= 0x80 && *p < 0xc0)
	p++;
    }
  else
    UTF8_SKIP(p);
  return p;
}

#define GET_TAGGED_CHAR(p,u) p=(byte*)get_tagged_char(p, &u)
#define SKIP_TAGGED_CHAR(p) p=(byte*)skip_tagged_char(p)

/* The same from a fastbuf */

static inline uns
bget_tagged_char(struct fastbuf *f)
{
  uns u = bgetc(f);
  if ((int)u < 0x80)
    ;
  else if (u < 0xc0)
    {
      if (u >= 0xb0)
	{
	  ASSERT(u == 0xb0);
	  u += 0x80020000;
	}
      else if (u >= 0xa0)
	{
	  uns v = bgetc(f);
	  ASSERT(v >= 0x80 && v <= 0xbf);
	  u = 0x80010000 + ((u & 0x0f) << 6) + (v & 0x3f);
	}
      else
	u += 0x80000000;
    }
  else
    {
      bungetc(f);
      u = bget_utf8(f);
    }
  return u;
}

#endif
