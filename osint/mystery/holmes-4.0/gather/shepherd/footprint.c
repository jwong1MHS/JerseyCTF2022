/*
 *	Sherlock Shepherd Daemon -- Footprints
 *
 *	(c) 2003 Martin Mares <mj@ucw.cz>
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/md5.h"
#include "gather/shepherd/shepherd.h"

#include <string.h>

int
urlrec_site_fp(struct site_fp *fp, struct url *ur)
{
  md5_context c;
  struct s {
    u16 port;
    byte proto;
    byte name[MAX_URL_SIZE];
  } s;

  if (!ur->protoid)
    return URL_ERR_UNKNOWN_PROTOCOL;
  s.proto = ur->protoid;
  s.port = ur->port;
  int l = strlen(ur->host);
  memcpy(s.name, ur->host, l);
  md5_init(&c);
  md5_update(&c, (unsigned char *) &s, OFFSETOF(struct s, name) + l);
  memcpy(fp, md5_final(&c), sizeof(*fp));
  return 0;
}

int
urlrec_rest_fp(struct rest_fp *fp, struct url *ur)
{
  md5_context c;

  md5_init(&c);
  md5_update(&c, (unsigned char *) ur->rest, strlen(ur->rest));
  memcpy(fp, md5_final(&c), sizeof(*fp));
  return 0;
}

int
urlrec_footprint(struct footprint *fp, struct url *ur)
{
  int err;

  if (err = urlrec_site_fp(&fp->site, ur))
    return err;
  else
    return urlrec_rest_fp(&fp->rest, ur);
}

int
url_footprint(struct footprint *fp, byte *url)
{
  struct url ur;
  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];
  int err;

  if (err = url_canon_split(url, buf1, buf2, &ur))
    return err;
  return urlrec_footprint(fp, &ur);
}

void
random_footprint(struct footprint *fp)
{
  fp->site.x[0] = random_u32();
  fp->site.x[1] = random_u32();
  fp->rest.x[0] = random_u32();
  fp->rest.x[1] = random_u32();
}

#define ASORT_PREFIX(x) footprint_array_sort_##x
#define ASORT_KEY_TYPE struct footprint
#define ASORT_LT(x, y) (fp_cmp(&(x), &(y)) < 0)
#include "ucw/sorter/array-simple.h"

void
footprint_array_sort(uns n, struct footprint *fps)
{
  footprint_array_sort_sort(fps, n);
}

#define SORT_KEY_REGULAR struct footprint
#define SORT_PREFIX(x) footprint_sort_##x
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_HASH_BITS 32
static inline int
footprint_sort_compare(struct footprint *x, struct footprint *y)
{
  return fp_cmp(x, y);
}

static inline uns
footprint_sort_hash(struct footprint *x)
{
  return x->site.x[0];
}
#include "ucw/sorter/sorter.h"

struct fastbuf *
footprint_sort(struct fastbuf *src, struct fastbuf *dest)
{
  ASSERT(src);
  return footprint_sort_sort(src, dest);
}

#define SORT_PREFIX(x) url_state_by_fp_sort_##x
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_BY_FP
#include "gather/shepherd/index-sort.h"

struct fastbuf *
url_state_by_fp_sort(struct fastbuf *src, struct fastbuf *dest)
{
  ASSERT(src);
  return url_state_by_fp_sort_sort(src, dest);
}

