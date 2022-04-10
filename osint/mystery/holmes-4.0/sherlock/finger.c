/*
 *	Sherlock Library -- String Fingerprints
 *
 *	(c) 2001--2003 Martin Mares <mj@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

/*
 *  We use a hashing function to map all the URL's and other
 *  hairy strings we work with to a much simpler universe
 *  of constant length bit strings (currently 96-bit ones).
 *  With a random hashing function (which is equivalent to
 *  having a fixed function and random input), the probability
 *  of at least one collision happening is at most c*n^2/m
 *  where n is the number of strings we hash, m is the size
 *  of our bit string universe (2^96) and c is a small constant.
 *  We set m sufficiently large and expect no collisions
 *  to occur. On the other hand, the worst thing which could
 *  be caused by a collision is mixing up two strings or labels
 *  of two documents which is relatively harmless.
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "ucw/md5.h"

#include <string.h>

void
fingerprint(byte *string, struct fingerprint *fp)
{
  md5_context c;

  md5_init(&c);
  md5_update(&c, string, strlen(string));
  memcpy(fp->hash, md5_final(&c), 12);
}

#ifdef TEST

#include "ucw/fastbuf.h"

int main(void)
{
  struct fastbuf *in = bfdopen_shared(0, 4096);
  struct fastbuf *out = bfdopen_shared(1, 4096);
  byte buf[4096];
  while (bgets(in, buf, sizeof(buf)))
    {
      struct fingerprint f;
      fingerprint(buf, &f);
      for (uns i=0; i<12; i++)
	bprintf(out, "%02x", f.hash[i]);
      bputc(out, '\n');
    }
  bclose(in);
  bclose(out);
  return 0;
}

#endif
