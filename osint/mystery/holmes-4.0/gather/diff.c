/*
 *	Sherlock Gatherer -- Comparing Objects
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2004--2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/md5.h"
#include "ucw/bitarray.h"
#include "ucw/string.h"
#include "gather/gather.h"

#include <string.h>
#include <stdlib.h>

static uns
gobj_diff_text(void)
{
  byte *old_md5_txt = obj_find_aval(gthis->refreshing, 'C');
  byte old_md5[MD5_SIZE];

  if (!old_md5_txt || !gthis->MD5_valid)
    return GOBJ_CHG_TEXT_LARGE;
  hex_to_mem(old_md5, old_md5_txt, MD5_SIZE, 0);
  return memcmp(gthis->MD5, old_md5, MD5_SIZE) ? GOBJ_CHG_TEXT_LARGE : 0;
}

static uns
gobj_diff_refs(void)
{
  BIT_ARRAY(types_used, 128);
  struct gobj_ref *ref, *r;
  struct oattr *attr;
  byte url[MAX_URL_SIZE];

  /* Only newly added references are detected */
  bit_array_zero(types_used, 128);
  CLIST_WALK(ref, gthis->ref_list)
    if (!bit_array_test_and_set(types_used, ref->type))
      {
	attr = obj_find_attr(gthis->refreshing, ref->type);
	for (r=ref; r; r=clist_next(&gthis->ref_list, &r->n))
	  {
	    if (r->type != ref->type)
	      continue;
	    if (!attr)
	      url[0] = 0;
	    else
	      {
		byte *u = attr->val;
		byte *stop = (byte*)strchr(u, ' ') ? : (u + strlen(u));
		ASSERT(stop-u < MAX_URL_SIZE);
		memcpy(url, u, stop-u);
		url[stop-u] = 0;
	      }
	    if (strcmp(url, r->url))
	      return GOBJ_CHG_REFS;
	    attr = attr->same;
	  }
      }
  return 0;
}

static uns
gobj_diff_redirect(void)
{
  byte *rx = obj_find_aval(gthis->aa, 'Y') ? : (byte*)"";
  byte *ry = obj_find_aval(gthis->refreshing, 'Y') ? : (byte*)"";
  return strcmp(rx, ry) ? GOBJ_CHG_REDIRECT : 0;
}

static uns
gobj_diff_http(void)
{
  /* Skey */
  byte *kx = obj_find_aval(gthis->aa, 'k') ? : (byte*)"";
  byte *ky = obj_find_aval(gthis->refreshing, 'k') ? : (byte*)"";
  if (strcmp(kx, ky))
    return GOBJ_CHG_HTTP;

  /* ETag */
  byte *ex = gthis->etag ? : (byte *)"";
  byte *ey = obj_find_aval(gthis->refreshing, 'g') ? : (byte *)"";
  if (strcmp(ex, ey))
    return GOBJ_CHG_HTTP;

  /* Last Modified */
  byte *ly;
  ucw_time_t lm;
  if (ly = obj_find_aval(gthis->refreshing, 'L'))
    lm = atol(ly);
  else
    lm = 0;
  if (gthis->lastmod_time != lm)
    return GOBJ_CHG_HTTP;

  return 0;
}

static uns
gobj_diff(void)
{
  if (!gthis->refreshing)
    return ~0;

  int diff = 0;
  diff |= gobj_diff_text();
  diff |= gobj_diff_refs();
  diff |= gobj_diff_http();
  diff |= gobj_diff_redirect();

  byte *x = obj_find_aval(gthis->refreshing, 'D');
  if (x && gthis->start_time - (ucw_time_t)atol(x) > max_refresh_age)
    diff |= GOBJ_CHG_FORCED;

  return diff;
}

uns
gobj_check_update(void)
{
  struct odes *old = gthis->refreshing;
  byte *x;

  uns diff = gobj_diff();

  if (diff & (GOBJ_CHG_TEXT_LARGE | GOBJ_CHG_REDIRECT))
    {
      /* Big update: new "J" and save the old one to "p" */
      obj_set_attr_num(gthis->aa, 'J', gthis->start_time);
      if (old && (x = obj_find_aval(old, 'J')))
	obj_set_attr(gthis->aa, 'p', x);
    }
  else
    {
      /* Small or no update: inherit "J" and "p" from the previous version */
      if (old)
	{
	  obj_set_attr(gthis->aa, 'J', obj_find_aval(old, 'J'));
	  obj_set_attr(gthis->aa, 'p', obj_find_aval(old, 'p'));
	}
    }

  return diff;
}
