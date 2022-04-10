/*
 *	Frequency Histogram of Lines
 *
 *	(c) 2005 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"

struct hist {
  uns cnt;
  byte x[1];
};

#define HASH_NODE struct hist
#define HASH_PREFIX(x) hh_##x
#define HASH_KEY_ENDSTRING x
#define HASH_WANT_LOOKUP
#define HASH_AUTO_POOL 16384
#define HASH_ZERO_FILL
#include "ucw/hashtable.h"

int
main(void)
{
  log_init("histogram");
  hh_init();

  struct fastbuf *f = bfdopen_shared(0, 65536);
  byte line[4096];
  while (bgets(f, line, sizeof(line)))
    hh_lookup(line)->cnt++;
  bclose(f);

  struct fastbuf *o = bfdopen_shared(1, 65536);
  HASH_FOR_ALL(hh, h)
    {
      bprintf(o, "%d\t%s\n", h->cnt, h->x);
    }
  HASH_END_FOR;
  bclose(o);

  return 0;
}
