/*
 *	Sherlock Gatherer -- Gathering Objects
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2004--2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/md5.h"
#include "sherlock/index.h"
#include "filter/filter.h"
#include "gather/gather.h"

#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>

struct gobject *gthis;		/* Current gatherer object we're working on */

struct gobject *
gobj_new(struct mempool *pool)
{
  struct gobject *g;
  struct timeval tv;

  if (!pool)
    pool = mp_new(4096);
  g = mp_alloc_zero(pool, sizeof(struct gobject));
  g->pool = pool;
  g->aa = obj_new(pool);
  clist_init(&g->ref_list);
  if (gettimeofday(&tv, NULL) < 0)
    die("gettimeofday failed: %m");
  g->start_time = tv.tv_sec;
  g->start_time_us = tv.tv_usec;
  g->filter_user_mark = F_UNDEF_INT;
  g->error_msg = "OK";
  return g;
}

void
gobj_free(struct gobject *g)
{
  gather_filter_undo();
  bclose(g->contents);
  bclose(g->text);
  bclose(g->meta);
  bclose(g->thumbnail);
  bclose(g->temp);
  mp_delete(g->pool);
}

byte *
gstrdup(byte *s)
{
  if (!s)
    return s;
  return mp_strdup(gthis->pool, s);
}

void
gerror(int code, char *msg, ...)
{
  va_list args;
  alarm(0);
  va_start(args, msg);
  gthis->error_code = code;
  gthis->error_msg = mp_vprintf(gthis->pool, msg, args);
  va_end(args);
  gthis->error_hook();
  die("error_hook has returned");
}

void
gobj_set_redirect(char *msg, ...)
{
  va_list args;
  va_start(args, msg);
  if (gthis->robot_file_p)
    {
      /* robots.txt is a redirect which we consider to be an error */
      gerror(2305, "Robot file is a redirect");
    }
  gthis->error_msg = mp_vprintf(gthis->pool, msg, args);
  gthis->error_code = 1;
  va_end(args);
}

static void
gobj_calc_fb_sum(byte *md5, struct fastbuf *f)
{
  md5_context m;
  byte block[4096];
  uns len;

  md5_init(&m);
  f = fbmem_clone_read(f);
  while (len = bread(f, block, sizeof(block)))
    md5_update(&m, block, len);
  bclose(f);
  memcpy(md5, md5_final(&m), MD5_SIZE);
}

void
gobj_calc_sum(void)
{
  if (gthis->MD5_valid || gthis->error_code || !min_summed_size || gthis->orig_size < min_summed_size)
    return;
  gobj_calc_fb_sum(gthis->MD5, gthis->contents);
  gthis->MD5_valid = 1;
}

byte *
gobj_parse_url(struct url *url, byte *u, byte *m, uns allow_rel)
{
  int e;
  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], buf3[MAX_URL_SIZE], buf4[MAX_URL_SIZE];
  struct url ur;

  if ((e = url_deescape(u, buf1)) ||
      (e = url_split(buf1, &ur, buf2)))
    goto urlerr;
  if (e = url_normalize(&ur, NULL))
    {
      if (e != URL_ERR_REL_NOTHING)
	goto urlerr;
      if (!gthis->url && !gthis->base_url)	/* We really have no base */
	goto urlerr;
      if (!allow_rel && log_base_errors)
	log(L_ERROR_R, "Relative %s URL encountered: %s", m, u);
      if (gthis->base_url)
	e = url_normalize(&ur, &gthis->base_url_s);
      else
	e = url_normalize(&ur, &gthis->url_s);
      if (e)
	goto urlerr;
    }
  if ((e = url_canonicalize(&ur)) ||
      (e = url_pack(&ur, buf3)) ||
      (e = url_enescape(buf3, buf4)))
    goto urlerr;
  ur.protocol = gstrdup(ur.protocol);
  ur.user = gstrdup(ur.user);
  ur.pass = gstrdup(ur.pass);
  ur.host = gstrdup(ur.host);
  ur.rest = gstrdup(ur.rest);
  *url = ur;	/* We need a local copy as we might have used the same URL as a base */
  return gstrdup(buf4);

 urlerr:
  gerror(2000+e, "Error parsing %s URL %s: %s", m, u, url_error(e));
}

struct url *
gobj_base_url(void)
{
  if (gthis->base_url)
    return &gthis->base_url_s;
  else
    return &gthis->url_s;
}

struct gobj_ref *
gobj_add_ref_full(int type, byte *url, byte *ctype, struct url *base)
{
  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], buf3[MAX_URL_SIZE], buf4[MAX_URL_SIZE];
  int e;
  struct url u;
  struct gobj_ref *r;

  if (!url)
    return NULL;
  if (!base)
    base = gobj_base_url();
  if ((e = url_canon_split_rel(url, buf1, buf2, &u, base)) ||
      (e = url_pack(&u, buf3)) ||
      (e = url_enescape(buf3, buf4)))
    {
      if (log_ref_errors)
	log(L_WARN_R, "Invalid ref to %s: %s", url, url_error(e));
      return NULL;
    }
  url = buf4;

  if (!ctype)
    {
      byte *cenc = NULL;
      guess_content_by_name(u.rest, &ctype, &cenc);
      if (!ctype)
	ctype = "";
    }
  else
    {
      byte *x = ctype;
      ctype = mp_alloc(gthis->pool, strlen(x)+1);
      strcpy(ctype, x);
    }

  CLIST_WALK(r, gthis->ref_list)
    {
      /* FIXME: This is quadratic. */
      if (r->type == type && !strcmp(r->url, url) && !strcmp(r->content_type, ctype))
	return r;
    }

  r = mp_alloc(gthis->pool, sizeof(struct gobj_ref) + strlen(url));
  r->type = type;
  r->content_type = ctype;
  r->id = gthis->ref_count++;
  r->dont_follow = 0;
  strcpy(r->url, url);
  clist_add_tail(&gthis->ref_list, &r->n);
  return r;
}

struct gobj_ref *
gobj_add_ref(int type, byte *url)
{
  return gobj_add_ref_full(type, url, NULL, NULL);
}

void
gobj_truncate(void)
{
  gthis->truncated = 1;
  if (!allow_truncate)
    gerror(2405, "Object too large");
}
