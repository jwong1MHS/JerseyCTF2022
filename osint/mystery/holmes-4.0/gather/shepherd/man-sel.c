/*
 *	Sherlock Shepherd -- Manual Control -- Selectors
 *
 *	(c) 2004--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2006--2007 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/bucket.h"
#include "sherlock/object.h"
#include "ucw/conf.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/chartype.h"
#include "ucw/heap.h"
#include "ucw/string.h"
#include "gather/gather.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/reap.h"
#include "gather/shepherd/protocol.h"
#include "gather/shepherd/man.h"

#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>

struct select_alg {
  /* generic */
  void (*init)(struct selector *selector);	/* Initialize internal structures */
  void (*cleanup)(struct selector *selector);	/* Cleanup internal structures */
  int (*prepare)(struct selector *selector);	/* Called after having processed all options */
  struct fastbuf * (*preselect)(struct selector *selector, int *order);
						/* Generate candidate URL footprints and/or site footprints */

  /* index records */
  struct fastbuf * (*presort)(struct selector *selector, struct fastbuf *index, int *order);
  int (*pass1_test)(struct url_state *s);
  int (*pass2_test)(struct url_state *s, byte *url);

  /* sites */
  int (*site_test)(struct site *s);
};

struct select_alg_usage {
  enum sel_type result_type;
  enum sel_opt opt_type;
  struct select_alg *alg;
  void (*add_arg)(struct selector *selector, byte *x);
};

struct selector_internal {
  struct selector pub;
  struct select_alg *alg;
  void (*add_arg)(struct selector *selector, byte *x);
  enum sel_opt last_opt_type;
  clist resolve_list;
};

/*** Auxiliary functions ***/

static const char *type_names[] = URL_STATE_TYPE_NAMES;
static const char flag_names[] = URL_STATE_ALL_FLAG_NAMES;

static uns
parse_x8(byte *i, u32 *o)
{
  uns x = 0;
  for (uns j = 0; j < 8; j++)
    if (Cxdigit(i[j]))
      x = (x << 4) | Cxvalue(i[j]);
    else
      return 0;
  *o = x;
  return 1;
}

static int
parse_x16(byte *i, u32 *o)
{
  return parse_x8(i, o) && parse_x8(i+8, o+1);
}

static void
cut_at_space(byte *x)
{
  while (*x && *x != ' ' && *x != '\n')
    x++;
  *x = 0;
}

/*** Filtering of sorted index by FP's ***/

static struct fastbuf *
preselect_add_fp(struct fastbuf *f, struct footprint *fp)
{
  if (unlikely(!f))
    f = bopen_tmp(65536);
  bwrite(f, fp, sizeof(*fp));
  DBG("Preselecting %08x%08x:%08x%08x", FP_QUAD(*fp));
  return f;
}

static struct fastbuf *
preselect_add_site_fp(struct fastbuf *f, struct site_fp *site_fp)
{
  struct footprint fp;
  fp.site = *site_fp;
  bzero(&fp.rest, sizeof(fp.rest));
  return preselect_add_fp(f, &fp);
}

static struct fastbuf *
preselect_index(struct selector *selector, struct fastbuf *idx, struct fastbuf * (*preselect)(struct selector *selector, int *order))
{
  int fps_order;
  struct fastbuf *fps = preselect(selector, &fps_order);
  if (!fps)
    return idx;
  brewind(fps);
  if (fps_order != INDEX_ORDER_BY_FP)
    fps = footprint_sort(fps, NULL);
  struct sel_src *src = (struct sel_src *)sel_binary_open_index(idx->name);
  bclose(idx);
  idx = bopen_tmp(65536);

  struct footprint fp, last_fp;
  while (breadb(fps, &fp, sizeof(fp)))
    {
      DBG("wpt0 %08x%08x:%08x%08x", FP_QUAD(fp));
    }
  brewind(fps);
  uns gt;
  if (!breadb(fps, &fp, sizeof(fp)))
    goto end;
next:
  DBG("Seeking %08x%08x:%08x%08x", FP_QUAD(fp));
  if (!src->find_forward(src, &fp, &gt))
    goto end;
  if (!fp.rest.x[0] && !fp.rest.x[1])
    {
      DBG("wpt1 %08x%08x:%08x%08x", FP_QUAD(*(struct footprint *)src->record));
      while (!site_fp_cmp(&fp.site, &((struct footprint *)src->record)->site))
        {
          DBG("writing  %08x%08x:%08x%08x OID=%08x", FP_QUAD(*(struct footprint *)src->record), ((struct url_state *)src->record)->oid);
	  bwrite(idx, src->record, sizeof(struct url_state));
	  if (!src->find_next(src))
	    goto end;
	}
      last_fp.site = fp.site;
      while (breadb(fps, &fp, sizeof(fp)))
        {
      DBG("wpt3 %08x%08x:%08x%08x", FP_QUAD(fp));
	if (site_fp_cmp(&fp.site, &last_fp.site))
	  goto next;
	}
    }
  else
    {
      while (!fp_cmp(&fp, src->record))
        {
	  bwrite(idx, src->record, sizeof(struct url_state));
	  if (!src->find_next(src))
	    goto end;
	}
      last_fp = fp;
      while (breadb(fps, &fp, sizeof(fp)))
	if (fp_cmp(&fp, &last_fp))
	  goto next;
    }
end:
  sel_binary_close((struct sel_binary_src *)src);
  bclose(fps);
  brewind(idx);
  return idx;
}

/*** Selection of everything ***/

static int
sel_all_query(struct url_state *s UNUSED)
{
  return 1;
}

static int
sel_all_site(struct site *s UNUSED)
{
  return 1;
}

/*** Selection of index entries by matching ***/

enum site_matcher_flags {
  SF_ANY = 0x1,
};

struct site_matcher {
  struct site_matcher *next;
  enum {
    UM_FP,
    UM_URL
  } type;
};

struct site_matcher_fp {
  struct site_matcher m;
  struct rest_fp fp;
};

struct site_matcher_rest {
  struct site_matcher m;
  byte patt[1];
};

static int site_matcher_chance;
static int site_matcher_all;
static struct fastbuf *sel_match_fps;
static int sel_match_fakes;

enum pattern_class {
  PATT_EQ,
  PATT_MATCH,
  PATT_ALL
};

static void
sel_match_init(struct selector *selector UNUSED)
{
  site_matcher_chance = 0;
  site_matcher_all = 0;
  maybe_load_sites(man_opt.state, 0);
  sel_match_fps = bopen_tmp(65536);
  sel_match_fakes = 0;
}

static void
sel_match_cleanup(struct selector *selector UNUSED)
{
  if (sel_match_fps)
    bclose(sel_match_fps);
  sel_match_fps = NULL;
}

static int
sel_match_prepare(struct selector *selector UNUSED)
{
  return site_matcher_chance;
}

static struct fastbuf *
sel_match_preselect(struct selector *selector UNUSED, int *order)
{
  struct fastbuf *fps = sel_match_fps;
  sel_match_fps = NULL;
  *order = INDEX_ORDER_UNDEF;
  return fps;
}

static struct site *
site_lookup_or_forge(struct site_fp *fp)
{
  struct site *site = site_lookup(fp);
  if (unlikely(!site))
    {
      byte fakename[24];
      log(L_ERROR, "Site %08x%08x not found in site list", FP_PAIR(*fp));
      sprintf(fakename, "site-%08x%08x", FP_PAIR(*fp));
      site = site_create(fp, URL_PROTO_HTTP, fakename, 80);
      site->flags |= SITE_TEMP;
      sel_match_fakes++;
    }
  return site;
}

static void
connect_matcher(struct selector *selector, struct site *site, struct site_matcher **mp, uns size)
{
  site_matcher_chance = 1;
  if (!mp || !*mp)
    {
      if (sel_match_fps)
        preselect_add_site_fp(sel_match_fps, &site->fp);
      site->u.ctrl.flags |= SF_ANY;
    }
  else
    {
      struct site_matcher *m = *mp;
      if (m->next != site->u.ctrl.first_matcher)
        {
	  m = mp_alloc(selector->pool, size);
	  memcpy(m, *mp, size);
	  m->next = site->u.ctrl.first_matcher;
	  *mp = m;
	}
      site->u.ctrl.first_matcher = m;
    }
}

static enum pattern_class
classify_pattern(byte *x)
{
  if (!strcmp(x, "*"))
    return PATT_ALL;
  while (*x)
    {
      if (*x == '*' || *x == '?')
	return PATT_MATCH;
      x++;
    }
  return PATT_EQ;
}

static int
match_classified_pattern(byte *patt, enum pattern_class class, byte *text)
{
  switch (class)
    {
      case PATT_EQ:
	return !strcmp(patt, text);
      case PATT_MATCH:
	return str_match_pattern(patt, text);
      default:
	return 1;
    }
}

struct url_pattern {
  byte *protocol;
  byte *host;
  int port;                     /* -1=not given, -2=any */
  byte *rest;
  enum pattern_class protocol_class, host_class, rest_class;
};

static void
split_url_pattern(struct selector *selector, byte *x, struct url_pattern *up)
{
  byte buf1[MAX_URL_SIZE], *c, *d, *e;
  int sep;

  if (url_deescape(x, buf1))
    {
    bad:
      man_usage("Invalid URL pattern `%s'", x);
    }
  c = d = buf1;
  while (*c && *c != ':')
    c++;
  if (c[0] != ':' || c[1] != '/' || c[2] != '/')
    goto bad;
  *c = 0;
  up->protocol = mp_strdup(selector->pool, d);
  c += 3;
  d = c;
  while (*c && *c != '/' && *c != '?')
    c++;
  sep = *c;
  *c = 0;
  if (e = strchr(d, ':'))
    {
      *e++ = 0;
      if (!strcmp(e, "*"))
	up->port = -2;
      else
	up->port = atol(e);
    }
  else
    up->port = -1;
  up->host = mp_strdup(selector->pool, d);
  for (e=up->host; *e; e++)
    *e = Clocase(*e);
  *c = sep;
  up->rest = mp_strdup(selector->pool, c);
  up->protocol_class = classify_pattern(up->protocol);
  up->host_class = classify_pattern(up->host);
  if (!strcmp(up->rest, "/*"))
    up->rest_class = PATT_ALL;  /* This is a small swindle, but forgiveable */
  else
    up->rest_class = classify_pattern(up->rest);
}

static int
site_match_url_pattern(struct url_pattern *up, struct site *site)
{
  if (!match_classified_pattern(up->protocol, up->protocol_class, url_proto_names[site->proto]))
    return 0;
  if (up->port < 0)
    {
      static const uns default_ports[] = URL_DEFPORTS;
      if (up->port == -1 && site->port != default_ports[site->proto])
	return 0;
    }
  else if (up->port != site->port)
    return 0;
  return match_classified_pattern(up->host, up->host_class, site->hostname);
}

static void
sel_match_add_url(struct selector *selector, byte *x)
{
  struct url_pattern up;

  split_url_pattern(selector, x, &up);
  // printf("@%s@%s@%d@%s@\n", up.protocol, up.host, up.port, up.rest);

  struct site_matcher *m = NULL;
  uns m_size = 0;
  switch (up.rest_class)
    {
      case PATT_EQ:
	{
	  m_size = sizeof(struct site_matcher_fp);
	  struct site_matcher_fp *mf = mp_alloc_zero(selector->pool, m_size);
	  mf->m.type = UM_FP;
	  struct url ur;
	  ur.rest = up.rest;
	  urlrec_rest_fp(&mf->fp, &ur);
	  m = &mf->m;
	  break;
	}
      case PATT_MATCH:
	{
	  m_size = sizeof(struct site_matcher_rest) + strlen(up.rest);
	  struct site_matcher_rest *mr = mp_alloc_zero(selector->pool, m_size);
	  mr->m.type = UM_URL;
	  strcpy(mr->patt, up.rest);
	  m = &mr->m;
	  break;
	}
      case PATT_ALL:
	break;
    }

  for (struct site *site=NULL; site=site_next(site);)
    if (site_match_url_pattern(&up, site))
      {
	if (sel_match_fps)
	  {
	    if (up.rest_class == PATT_EQ)
	      {
	        struct footprint fp;
		fp.site = site->fp;
		fp.rest = SKIP_BACK(struct site_matcher_fp, m, m)->fp;
		preselect_add_fp(sel_match_fps, &fp);
	      }
	    else
	      preselect_add_site_fp(sel_match_fps, &site->fp);
	  }
        connect_matcher(selector, site, &m, m_size);
      }
}

static void
sel_match_add_qkey(struct selector *selector, byte *x)
{
  u32 sk;
  u64 qk = 0, qkm = 0;
  byte *y;

  if (y = strchr(x, ':'))
    {
      char *end;
      unsigned long port = strtoul(x, &end, 16);
      if (errno == ERANGE || port > 0xffff || x == y || (byte*)end != y)
	man_usage("Invalid qkey `%s'", x);
      qk |= make_qkey(0, port);
      qkm |= make_qkey(0, 0xffff);
      x = y+1;
    }
  if (!strcmp(x, "non-ip"))
    {
      qk |= SKEY_NONIP;
      qkm |= SKEY_TYPE_MASK;
    }
  else if (!strcmp(x, "invalid"))
    {
      qk |= SKEY_NONEXISTENT;
      qkm |= SKEY_TYPE_MASK;
    }
  else if (!strcmp(x, "unresolved"))
    {
      qk |= SKEY_UNRESOLVED;
      qkm |= SKEY_TYPE_MASK;
    }
  else if (y = strchr(x, '/'))
    {
      *y++ = 0;
      if (!parse_x8(x, &sk))
	man_usage("Invalid qkey `%s'", x);
      uns pxlen;
      if (cf_parse_int(y, &pxlen) || pxlen > 32)
	man_usage("Invalid prefix length `%s'", y);
      u32 skm = (pxlen == 32) ? ~0U : ~(~0U >> pxlen);
      qk |= sk & skm;
      qkm |= skm;
    }
  else if (parse_x8(x, &sk))
    {
      qk |= sk;
      qkm |= (u32)~0U;
    }
  else
    man_usage("Invalid qkey `%s'", x);
  for (struct site *site=NULL; site=site_next(site);)
    if ((site_qkey(site) & qkm) == qk)
      connect_matcher(selector, site, NULL, 0);
}

static int
parse_fp_or_any(byte *x, u32 *f)
{
  if (!strcmp(x, "*"))
    return 1;
  else if (strlen(x) == 16 && parse_x16(x, f))
    return 0;
  else
    man_usage("Invalid fingerprint part `%s'", x);
}

static void
do_sel_match_add_fp(struct selector *selector, byte *x, int normalized)
{
  struct footprint fp;
  int sfp_any = 0, rfp_any = 0;

  cut_at_space(x);
  if (selector->result_type == ST_SITES)
    {
      byte *y = strchr(x, ':');
      if (y)
	*y = 0;
      sfp_any = parse_fp_or_any(x, fp.site.x);
      rfp_any = 1;
    }
  else
    {
      byte *y = strchr(x, ':');
      if (!y)
	man_usage("Invalid fingerprint `%s'", x);
      *y++ = 0;
      sfp_any = parse_fp_or_any(x, fp.site.x);
      rfp_any = parse_fp_or_any(y, fp.rest.x);
    }

  struct site_matcher *m = NULL;
  if (!rfp_any)
    {
      struct site_matcher_fp *M = mp_alloc_zero(selector->pool, sizeof(*M));
      M->m.type = UM_FP;
      M->fp = fp.rest;
      m = &M->m;
      if (sel_match_fps)
	preselect_add_fp(sel_match_fps, &fp);
    }

  if (sfp_any)
    {
      bclose(sel_match_fps);
      sel_match_fps = NULL;
      site_matcher_all = 1;
    }
  else if (!normalized)
    connect_matcher(selector, site_lookup_or_forge(&fp.site), &m, sizeof(*m));
  else
    for (struct site *site=NULL; site=site_next(site);)
      if (!site_fp_cmp(&site->norm_fp, &fp.site))
        connect_matcher(selector, site, &m, sizeof(*m));
}

static void
sel_match_add_fp(struct selector *selector, byte *x)
{
  do_sel_match_add_fp(selector, x, 0);
}

static void
sel_match_add_norm_fp(struct selector *selector, byte *x)
{
  do_sel_match_add_fp(selector, x, 1);
}

static int
sel_match_pass1(struct url_state *s)
{
  if (site_matcher_all)
    return 1;

  struct site *site = site_lookup_or_forge(&s->fp.site);

  if (site->u.ctrl.flags & SF_ANY)
    return 1;

  uns need_pass2 = 0;
  for (struct site_matcher *m = site->u.ctrl.first_matcher; m; m=m->next)
    switch (m->type)
      {
	case UM_FP:
	  if (!rest_fp_cmp(&((struct site_matcher_fp *)m)->fp, &s->fp.rest))
	    return 1;
	  break;
	case UM_URL:
	  need_pass2 = 2;
	  break;
	default:
	  ASSERT(0);
      }

  return need_pass2;
}

static int
sel_match_pass2(struct url_state *s, byte *url)
{
  struct url ur;
  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];

  if (ustate_type(s) == UTYPE_SKEY || ustate_type(s) == UTYPE_ZOMBIE || (s->flags & USF_CONTRIB))
    return 0;   /* These have only a fake URL */

  int e = url_canon_split(url, buf1, buf2, &ur);
  if (e)
    {
      log(L_ERROR, "Unable to parse URL %s: %s", url, url_error(e));
      return 0;
    }

  struct site *site = site_lookup(&s->fp.site);
  ASSERT(site);

  for (struct site_matcher *m = site->u.ctrl.first_matcher; m; m=m->next)
    if (m->type == UM_URL)
      {
        struct site_matcher_rest *r = (struct site_matcher_rest *) m;
	if (str_match_pattern(r->patt, ur.rest))
	  return 1;
      }
  return 0;
}

static int
sel_match_site(struct site *site)
{
  return (site->u.ctrl.flags & SF_ANY) || site->u.ctrl.first_matcher;
}

/*** Batch selection of index entries ***/

static struct fastbuf *sel_batch_tmp;

static void
sel_batch_init(struct selector *selector UNUSED)
{
  sel_batch_tmp = bopen_tmp(65536);
  MAN_VERBOSE("Preparing batch");
}

static void
sel_batch_cleanup(struct selector *selector UNUSED)
{
  bclose(sel_batch_tmp);
}

static void
sel_batch_add_url(struct selector *selector UNUSED, byte *x)
{
  struct footprint fp;
  int e;
  if (e = url_footprint(&fp, x))
    log(L_ERROR, "Error parsing URL %s: %s", x, url_error(e));
  bwrite(sel_batch_tmp, &fp, sizeof(fp));
}

static void
sel_batch_add_fp(struct selector *selector UNUSED, byte *x)
{
  struct footprint fp;
  cut_at_space(x);
  if (strlen(x) == 33
      && x[16] == ':'
      && parse_x16(x, fp.site.x)
      && parse_x16(x+17, fp.rest.x))
    {
      bwrite(sel_batch_tmp, &fp, sizeof(fp));
      return;
    }
  log(L_ERROR, "Invalid footprint %s", x);
}

static struct footprint sba_next;
static int sba_have_next;

static int
sel_batch_prepare(struct selector *selector UNUSED)
{
  if (!btell(sel_batch_tmp))
    return 0;
  MAN_VERBOSE("Sorting batch");
  brewind(sel_batch_tmp);
  sel_batch_tmp = footprint_sort(sel_batch_tmp, NULL);
  sba_have_next = breadb(sel_batch_tmp, &sba_next, sizeof(sba_next));
  return 1;
}

static struct fastbuf *
sel_batch_presort(struct selector *selector UNUSED, struct fastbuf *idx, int *order)
{
  if (*order == INDEX_ORDER_BY_FP)
    return idx;
  *order = INDEX_ORDER_BY_FP;
  MAN_VERBOSE("Sorting index");
  return url_state_by_fp_sort(idx, NULL);
}

static struct fastbuf *
sel_batch_preselect(struct selector *selector UNUSED, int *order)
{
  *order = INDEX_ORDER_BY_FP;
  return bopen(sel_batch_tmp->name, O_RDONLY, 65536);
}

static int
sel_batch_query(struct url_state *s)
{
  int r;
  if (!sba_have_next)
    return 0;
  while ((r = fp_cmp(&sba_next, &s->fp)) < 0)
    {
      if (!(sba_have_next = breadb(sel_batch_tmp, &sba_next, sizeof(sba_next))))
	return 0;
    }
  return !r;
}

/*** Gathering of URL's for contributions ***/

static void
sel_contrib_init(struct selector *selector UNUSED)
{
  contrib_init(man_opt.state);
}

static void
sel_contrib_cleanup(struct selector *selector UNUSED)
{
  contrib_cleanup();
}

static void
sel_contrib_add_url(struct selector *selector UNUSED, byte *url)
{
  byte curl[MAX_URL_SIZE];
  int err = url_auto_canonicalize(url, curl);
  if (err)
    {
      log(L_INFO, "%s: %s", url, url_error(err));
      return;
    }
  byte *m = add_contrib(curl, NULL, 0, (man_opt.set_weight >= 0 ? man_opt.set_weight : (int) default_insert_weight),
      man_opt.set_flag_val, man_opt.set_section, man_opt.set_area, AREA_ANY);
  if (man_opt.verbose > 1 && m)
    log(L_INFO, "%s: %s", curl, m);
}

/*** CMD_FILTER is implemented inside its own selector ***/

static void
sel_filter_init(struct selector *selector UNUSED)
{
  contrib_init(NULL);
}

static void
sel_filter_add_url(struct selector *selector UNUSED, byte *url)
{
  struct cfilter_data cfd;
  cfd.url = url;
  byte *msg = verify_contrib(&cfd, 0);
  if (msg)
    printf("%s: %d %s\n", url, cfd.error_code, msg);
  else
    printf("%s: OK\n", url);
}

/*** Limiters ***/

static uns lim_types = ~0U;
static uns lim_flags_mask, lim_flags_val;
static uns lim_type_mask, lim_type_val;
static uns lim_age_min, lim_age_max = ~0U;
#ifdef CONFIG_AREAS
static uns lim_area = ~0U;
#endif

static void
sel_add_only_types(struct selector *selector UNUSED, byte *x)
{
  uns i;
  for (i=0; i<UTYPE_MAX; i++)
    if (!strcasecmp(type_names[i], x))
      break;
  if (i >= UTYPE_MAX)
    man_usage("Unknown type `%s'", x);
  if (lim_types == ~0U)
    lim_types = 0;
  lim_types |= 1 << i;
}

static void
sel_add_only_flags(struct selector *selector UNUSED, byte *x)
{
  uns plus = 1;
  char *f;
  byte *xorig = x;

  if (x[0] == '+' || x[0] == '-')
    {
      plus = (x[0] == '+');
      x++;
    }
  if (!x[0] || x[1] || !(f = strchr(flag_names, x[0])) || *f == '-')
    man_usage("Invalid flag name `%s'", xorig);
  uns flag = f - flag_names;
  if (flag < 8)
    {
      lim_flags_mask |= (1 << flag);
      if (plus)
        lim_flags_val |= (1 << flag);
      else
        lim_flags_val &= ~(1 << flag);
    }
  else
    {
      flag = flag - 8 + 4;
      lim_type_mask |= (1 << flag);
      if (plus)
        lim_type_val |= (1 << flag);
      else
        lim_type_val &= ~(1 << flag);
    }
}

static void
sel_add_only_older(struct selector *selector UNUSED, byte *x)
{
  byte *err;
  if (err = cf_parse_int(x, &lim_age_min))
    man_usage("Invalid age `%s': %s", x, err);
}

static void
sel_add_only_newer(struct selector *selector UNUSED, byte *x)
{
  byte *err;
  if (err = cf_parse_int(x, &lim_age_max))
    man_usage("Invalid age `%s': %s", x, err);
}

static void
sel_add_only_area(struct selector *selector UNUSED, byte *x UNUSED)
{
#ifdef CONFIG_AREAS
  char *end;
  errno = 0;
  lim_area = strtoul(x, &end, 10);
  if (errno == ERANGE || (end && *end))
    man_usage("Invalid area `%s'", x);
#else
  man_usage("Areas are not available, compile Sherlock with CONFIG_AREAS first");
#endif
}

static inline int
sel_check_limiters(struct url_state *s)
{
  uns age = man_url_age(s);
  return
    (lim_types & (1 << ustate_type(s))) &&
    ((s->flags & lim_flags_mask) == lim_flags_val) &&
    ((s->type & lim_type_mask) == lim_type_val) &&
#ifdef CONFIG_AREAS
    (lim_area == ~0U || s->area == lim_area) &&
#endif
    age >= lim_age_min &&
    age <= lim_age_max;
}

/*** Area selection ***/

#ifdef CONFIG_AREAS

static clist area_id_list;
static int area_id_all;

struct area_id {
  cnode n;
  area_t id;
};

static void
sel_area_init(struct selector *selector)
{
  areas_init(man_opt.state, selector->areas_rw);
  clist_init(&area_id_list);
}

static void
sel_area_all_init(struct selector *selector)
{
  areas_init(man_opt.state, selector->areas_rw);
  area_id_all = 1;
}

static void
sel_area_cleanup(struct selector *selector UNUSED)
{
  areas_cleanup();
}

static void
sel_area_add(struct selector *selector, byte *arg)
{
  struct area_id *a = mp_alloc(selector->pool, sizeof(*a));
  a->id = atol(arg);
  clist_add_tail(&area_id_list, &a->n);
}

area_t
area_next_id(struct selector *selector UNUSED)
{
  if (area_id_all)
    {
      static area_t area_id_this;
      if (area_id_this < areas_max_id)
	return area_id_this++;
      else
	return AREA_ANY;
    }
  else
    {
      static struct area_id *area_id_curr;
      if (!area_id_curr)
	area_id_curr = clist_head(&area_id_list);
      else
	area_id_curr = clist_next(&area_id_list, &area_id_curr->n);
      return area_id_curr ? area_id_curr->id : AREA_ANY;
    }
}

static struct select_alg sel_ar_all_alg = {
  .init = sel_area_all_init,
  .cleanup = sel_area_cleanup
};

static struct select_alg sel_area_alg = {
  .init = sel_area_init,
  .cleanup = sel_area_cleanup
};

#endif

/*** Insert all selected URL's to a hash table ***/

#define HASH_NODE struct sel_hash_entry
#define HASH_PREFIX(x) man_sel_hash_##x
#define HASH_KEY_COMPLEX(x) x fp
#define HASH_KEY_DECL struct footprint fp
#define HASH_GIVE_HASHFN
#define HASH_GIVE_EQ
#define HASH_GIVE_INIT_KEY
#define HASH_ZERO_FILL
#define HASH_WANT_FIND
#define HASH_WANT_LOOKUP
#define HASH_AUTO_POOL 65536

static inline uns
man_sel_hash_hash(struct footprint fp)
{
  return fp.site.x[0] ^ fp.rest.x[0];
}

static inline int
man_sel_hash_eq(struct footprint x, struct footprint y)
{
  return !memcmp(&x, &y, sizeof(struct footprint));
}

static inline void
man_sel_hash_init_key(struct sel_hash_entry *e, struct footprint fp)
{
  e->fp = fp;
}

#include "ucw/hashtable.h"

static void
sel_hash_init(struct selector *selector UNUSED)
{
  man_sel_hash_init();
}

uns
sel_hash_count(struct selector *selector UNUSED)
{
  return man_sel_hash_table.hash_count;
}

struct sel_hash_entry *
sel_hash_find(struct selector *selector UNUSED, struct footprint fp)
{
  return man_sel_hash_find(fp);
}

void
sel_hash(struct selector *selector UNUSED, void (*f)(struct sel_hash_entry *e))
{
  HASH_FOR_ALL(man_sel_hash, e)
    {
      f(e);
    }
  HASH_END_FOR;
}

static void
sel_hash_add_url(struct selector *selector UNUSED, byte *arg)
{
  struct footprint fp;
  struct sel_hash_entry *e;
  struct url ur;
  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], buf3[MAX_URL_SIZE], norm[MAX_URL_SIZE];
  int err;

  if ((err = url_canon_split(arg, buf1, buf2, &ur)) ||
      (err = url_pack(&ur, buf3)) ||
      (err = url_enescape(buf3, norm)))
    {
      log(L_ERROR, "Unable to parse URL %s: %s", arg, url_error(err));
      return;
    }
  if (urlrec_footprint(&fp, &ur))
    {
      log(L_ERROR, "Unable to compute footprint for URL %s", arg);
      return;
    }
  e = man_sel_hash_lookup(fp);
  if (!e->url)
    e->url = mp_strdup(man_sel_hash_table.pool, norm);
}

/*** Table of all selection algorithms and operators ***/

static struct select_alg sel_all_alg = {
  .pass1_test = sel_all_query,
  .site_test = sel_all_site
};

static struct select_alg sel_match_alg = {
  .init = sel_match_init,
  .cleanup = sel_match_cleanup,
  .prepare = sel_match_prepare,
  .preselect = sel_match_preselect,
  .pass1_test = sel_match_pass1,
  .pass2_test = sel_match_pass2,
  .site_test = sel_match_site
};

static struct select_alg sel_batch_alg = {
  .init = sel_batch_init,
  .cleanup = sel_batch_cleanup,
  .prepare = sel_batch_prepare,
  .presort = sel_batch_presort,
  .preselect = sel_batch_preselect,
  .pass1_test = sel_batch_query
};

static struct select_alg sel_contrib_alg = {
  .init = sel_contrib_init,
  .cleanup = sel_contrib_cleanup
};

static struct select_alg sel_filter_alg = {
  .init = sel_filter_init
};

static struct select_alg sel_hash_alg = {
  .init = sel_hash_init
};

struct select_alg_usage usages[] = {
 { ST_INDEX_ENTRIES,	SEL_ALL,	&sel_all_alg,		NULL },
 { ST_INDEX_ENTRIES,	SEL_URL,	&sel_match_alg,		sel_match_add_url },
 { ST_INDEX_ENTRIES,	SEL_URLS,	&sel_batch_alg,		sel_batch_add_url },
 { ST_INDEX_ENTRIES,	SEL_QKEY,	&sel_match_alg,		sel_match_add_qkey },
 { ST_INDEX_ENTRIES,	SEL_FP,		&sel_match_alg,		sel_match_add_fp },
 { ST_INDEX_ENTRIES,	SEL_NORM_FP,	&sel_match_alg,		sel_match_add_norm_fp },
 { ST_INDEX_ENTRIES,	SEL_FPS,	&sel_batch_alg,		sel_batch_add_fp },
 { ST_INDEX_ENTRIES,	SEL_ONLY_TYPES,	&sel_all_alg,		sel_add_only_types },
 { ST_INDEX_ENTRIES,	SEL_ONLY_FLAGS,	&sel_all_alg,		sel_add_only_flags },
 { ST_INDEX_ENTRIES,	SEL_ONLY_NEWER,	&sel_all_alg,		sel_add_only_newer },
 { ST_INDEX_ENTRIES,	SEL_ONLY_OLDER,	&sel_all_alg,		sel_add_only_older },
 { ST_INDEX_ENTRIES,	SEL_ONLY_AREA,	&sel_all_alg,		sel_add_only_area },
 { ST_INDEX_ENTRIES,	SEL_ONLY_TYPES,	&sel_batch_alg,		sel_add_only_types },
 { ST_INDEX_ENTRIES,	SEL_ONLY_FLAGS,	&sel_batch_alg,		sel_add_only_flags },
 { ST_INDEX_ENTRIES,	SEL_ONLY_NEWER,	&sel_batch_alg,		sel_add_only_newer },
 { ST_INDEX_ENTRIES,	SEL_ONLY_OLDER,	&sel_batch_alg,		sel_add_only_older },
 { ST_INDEX_ENTRIES,	SEL_ONLY_AREA,	&sel_batch_alg,		sel_add_only_area },
 { ST_INDEX_ENTRIES,	SEL_ONLY_TYPES,	&sel_match_alg,		sel_add_only_types },
 { ST_INDEX_ENTRIES,	SEL_ONLY_FLAGS,	&sel_match_alg,		sel_add_only_flags },
 { ST_INDEX_ENTRIES,	SEL_ONLY_NEWER,	&sel_match_alg,		sel_add_only_newer },
 { ST_INDEX_ENTRIES,	SEL_ONLY_OLDER,	&sel_match_alg,		sel_add_only_older },
 { ST_INDEX_ENTRIES,	SEL_ONLY_AREA,	&sel_match_alg,		sel_add_only_area },

 { ST_CONTRIB,		SEL_URL,	&sel_contrib_alg,	sel_contrib_add_url },
 { ST_CONTRIB,		SEL_URLS,	&sel_contrib_alg,	sel_contrib_add_url },

 { ST_SITES,		SEL_ALL,	&sel_all_alg,		NULL },
 { ST_SITES,		SEL_URL,	&sel_match_alg,		sel_match_add_url },
 { ST_SITES,		SEL_QKEY,	&sel_match_alg,		sel_match_add_qkey },
 { ST_SITES,		SEL_FP,		&sel_match_alg,		sel_match_add_fp },
 { ST_SITES,		SEL_NORM_FP,	&sel_match_alg,		sel_match_add_norm_fp },

 { ST_FILTERING,	SEL_URL,	&sel_filter_alg,	sel_filter_add_url },
 { ST_FILTERING,	SEL_URLS,	&sel_filter_alg,	sel_filter_add_url },

#ifdef CONFIG_AREAS
 { ST_AREAS,		SEL_ALL,	&sel_ar_all_alg,	NULL },
 { ST_AREAS,		SEL_AREA,	&sel_area_alg,		sel_area_add },
#endif

 { ST_URL_HASH,		SEL_URL,	&sel_hash_alg,		sel_hash_add_url },
 { ST_URL_HASH,		SEL_URLS,	&sel_hash_alg,		sel_hash_add_url },

 { ST_MIXED,		SEL_ALL,	&sel_all_alg,		NULL },
 { ST_MIXED,		SEL_URL,	&sel_match_alg,		sel_match_add_url },
 { ST_MIXED,		SEL_URLS,	&sel_batch_alg,		sel_batch_add_url },
 { ST_MIXED,		SEL_QKEY,	&sel_match_alg,		sel_match_add_qkey },
 { ST_MIXED,		SEL_FP,		&sel_match_alg,		sel_match_add_fp },
 { ST_MIXED,		SEL_NORM_FP,	&sel_match_alg,		sel_match_add_norm_fp },
 { ST_MIXED,		SEL_FPS,	&sel_batch_alg,		sel_batch_add_fp },
 { ST_MIXED,		SEL_ONLY_TYPES,	&sel_all_alg,		sel_add_only_types },
 { ST_MIXED,		SEL_ONLY_FLAGS,	&sel_all_alg,		sel_add_only_flags },
 { ST_MIXED,		SEL_ONLY_NEWER,	&sel_all_alg,		sel_add_only_newer },
 { ST_MIXED,		SEL_ONLY_OLDER,	&sel_all_alg,		sel_add_only_older },
 { ST_MIXED,		SEL_ONLY_AREA,	&sel_all_alg,		sel_add_only_area },
 { ST_MIXED,		SEL_ONLY_TYPES,	&sel_batch_alg,		sel_add_only_types },
 { ST_MIXED,		SEL_ONLY_FLAGS,	&sel_batch_alg,		sel_add_only_flags },
 { ST_MIXED,		SEL_ONLY_NEWER,	&sel_batch_alg,		sel_add_only_newer },
 { ST_MIXED,		SEL_ONLY_OLDER,	&sel_batch_alg,		sel_add_only_older },
 { ST_MIXED,		SEL_ONLY_AREA,	&sel_batch_alg,		sel_add_only_area },
 { ST_MIXED,		SEL_ONLY_TYPES,	&sel_match_alg,		sel_add_only_types },
 { ST_MIXED,		SEL_ONLY_FLAGS,	&sel_match_alg,		sel_add_only_flags },
 { ST_MIXED,		SEL_ONLY_NEWER,	&sel_match_alg,		sel_add_only_newer },
 { ST_MIXED,		SEL_ONLY_OLDER,	&sel_match_alg,		sel_add_only_older },
 { ST_MIXED,		SEL_ONLY_AREA,	&sel_match_alg,		sel_add_only_area },

 { 0,			0,		NULL,			0 },
};

int
sel_index(struct selector *selector, void (*f)(struct url_state *s, byte *url, int matching), int resolve)
{
  struct selector_internal *sel = (struct selector_internal *)selector;
  struct select_alg *alg = sel->alg;
  DBG("Running selector %p", sel);
  struct fastbuf *idx;
  struct fastbuf *to_resolve = NULL;
  struct fastbuf *to_pass2 = NULL;
  struct url_state s;
  byte url[MAX_URL_SIZE];
  uns all_cnt = 0, match_cnt = 0;
  int e;
  uns order;

  if (alg->prepare && !alg->prepare(selector))
    {
      MAN_VERBOSE("No records selected");
      return 0;
    }

  idx = read_state_file(man_opt.state, "index");
  all_cnt = (uns)(bfilesize(idx) / sizeof(struct url_state));
  order = (state_flags_get(man_opt.state) & STATE_FLAG_SORTED) ? INDEX_ORDER_BY_FP : INDEX_ORDER_UNDEF;

  if (!selector->need_all && alg->preselect && order == INDEX_ORDER_BY_FP)
    {
      MAN_VERBOSE("Preselecting URL index");
      idx = preselect_index(selector, idx, alg->preselect);
      MAN_VERBOSE("Found %u candidates", (uns)(bfilesize(idx) / sizeof(struct url_state)));
    }

  if (alg->presort)
    idx = alg->presort(selector, idx, &order);

  MAN_VERBOSE("Filtering URL index");
  while (breadb(idx, &s, sizeof(s)))
    {
      if (!sel_check_limiters(&s))
	e = 0;
      else
	e = alg->pass1_test(&s);
      if (e == 2)
	to_pass2 = resolve_add(to_pass2, &s);
      else if (e)
	{
	  if (resolve)
	    to_resolve = resolve_add(to_resolve, &s);
	  else
	    f(&s, NULL, 1);
	  match_cnt++;
	}
      else
	f(&s, NULL, 0);
    }
  bclose(idx);

  if (to_resolve)
    {
      MAN_VERBOSE("Resolving URLs");
      to_resolve = resolve_go(to_resolve, (resolve > 1), order);
      while (resolve_read(to_resolve, &s, url))
	f(&s, url, 1);
      resolve_close(to_resolve);
    }

  if (to_pass2)
    {
      MAN_VERBOSE("Selecting by URL");
      to_pass2 = resolve_go(to_pass2, (resolve > 1), order);
      while (resolve_read(to_pass2, &s, url))
	{
	  int e = alg->pass2_test(&s, url);
	  f(&s, url, e);
	  if (e)
	    match_cnt++;
	}
      resolve_close(to_pass2);
    }

  MAN_VERBOSE("Matched %d of %d URLs", match_cnt, all_cnt);
  return 1;
}

void
sel_mixed_add_source(struct selector *selector, struct sel_src *src, uns flags)
{
  DBG("Added source %p to selector %p", src, selector);
  clist_add_tail(&selector->lsrc, &src->n);
  src->flags = flags;
  if ((flags & SEL_MIXED_INDEX) && !selector->idx_src)
    selector->idx_src = src;
}

static struct selector *sel_mixed_selector;
static void (*sel_mixed_f)(struct selector *selector);
static uns sel_mixed_count;
static uns sel_mixed_merge_count;
static uns sel_mixed_merge_bool_mask;
static struct sel_src **sel_mixed_srcs;
static struct sel_src **sel_mixed_heap;
static uns sel_mixed_heap_n;
#define SEL_MIXED_HEAP_CMP(x, y) (fp_cmp((x)->record, (y)->record) < 0)

static void
sel_mixed_resolve(void)
{
  CLIST_FOR_EACH(void *, s, ((struct selector_internal *)sel_mixed_selector)->resolve_list)
    {
      struct sel_src *src = SKIP_BACK(struct sel_src, resolve_node, s);
      if (sel_mixed_selector->bool_mask & src->bool_mask)
        if (sel_mixed_selector->url = src->resolve(src))
	  break;
    }
}

static void
sel_mixed_record(uns bool_mask)
{
  struct selector *selector = sel_mixed_selector;
  struct selector_internal *sel = (struct selector_internal *)selector;
  selector->bool_mask = bool_mask;
  selector->url = NULL;
  struct select_alg *alg = sel->alg;
  struct sel_src *idx_src = selector->idx_src;
  sel_mixed_resolve();

  if (idx_src && (bool_mask & idx_src->bool_mask))
    if (!sel_check_limiters(idx_src->record))
      DBG("Unmatched limiters for %08x%08x:%08x%08x", FP_QUAD(*(struct footprint *)idx_src->record));
    else
      {
        int e = alg->pass1_test(idx_src->record);
	if (!e)
          DBG("Unmatched pass1 for %08x%08x:%08x%08x", FP_QUAD(*(struct footprint *)idx_src->record));
	else if (!selector->url)
	  if (e == 2)
	    DBG("Unresolvable %08x%08x:%08x%08x", FP_QUAD(*(struct footprint *)idx_src->record));
	  else
	    {
	      DBG("Matched %08x%08x:%08x%08x", FP_QUAD(*(struct footprint *)idx_src->record));
	      selector->bool_mask |= SEL_MIXED_MATCHED;
	    }
	else if (e != 2 || alg->pass2_test(idx_src->record, selector->url))
	  {
	    DBG("Matched %08x%08x:%08x%08x (%s)", FP_QUAD(*(struct footprint *)idx_src->record), selector->url);
	    selector->bool_mask |= SEL_MIXED_MATCHED;
	  }
	else
	  DBG("Unmatched pass2 for %08x%08x:%08x%08x (%s)", FP_QUAD(*(struct footprint *)idx_src->record), selector->url);
      }
  sel_mixed_f(selector);
}

static void
sel_mixed_next(struct sel_src *src, struct footprint *fp)
{
  while (src->record && !fp_cmp(src->record, fp))
    src->find_next(src);
}

static uns
sel_mixed_and_step(void)
{
  DBG("step");
  struct sel_src *first = sel_mixed_srcs[0];
  ASSERT(first->record);
  struct footprint *fp = &sel_mixed_selector->fp;
  *fp = *(struct footprint *)first->record;
  uns bool_mask = sel_mixed_merge_bool_mask;
  uns i, gt;
  for (i = 1; i < sel_mixed_merge_count; i++)
    if (!sel_mixed_srcs[i]->find_forward(sel_mixed_srcs[i], fp, &gt))
      return 0;
    else if (gt)
      goto next;
  for (; i < sel_mixed_count; i++)
    if (sel_mixed_srcs[i]->find_forward(sel_mixed_srcs[i], fp, &gt) && !gt)
      bool_mask |= sel_mixed_srcs[i]->bool_mask;
  sel_mixed_record(bool_mask);
next:
  sel_mixed_next(first, fp);
  return 1;
}

static void
sel_mixed_or_seek(struct footprint *fp)
{
  uns gt;
  while (sel_mixed_heap_n && fp_cmp((struct footprint *)sel_mixed_heap[1]->record, fp) < 0)
    if (sel_mixed_heap[1]->find_forward(sel_mixed_heap[1], fp, &gt))
      HEAP_INCREASE(struct sel_src *, sel_mixed_heap, sel_mixed_heap_n, SEL_MIXED_HEAP_CMP, HEAP_SWAP, 1);
    else
      HEAP_DELMIN(struct sel_src *, sel_mixed_heap, sel_mixed_heap_n, SEL_MIXED_HEAP_CMP, HEAP_SWAP);
}

static void
sel_mixed_or_step(void)
{
  struct sel_src **heap = sel_mixed_heap;
  ASSERT(sel_mixed_heap_n);
  struct footprint *fp = &sel_mixed_selector->fp;
  *fp = *(struct footprint *)heap[1]->record;
  uns bool_mask = 0, m = 0, i, gt;
  do
    {
      m++;
      bool_mask |= heap[1]->bool_mask;
      HEAP_DELMIN(struct sel_src *, sel_mixed_heap, sel_mixed_heap_n, SEL_MIXED_HEAP_CMP, HEAP_SWAP);
    }
  while (sel_mixed_heap_n && !fp_cmp(heap[1]->record, fp));
  for (i = sel_mixed_merge_count; i < sel_mixed_count; i++)
    if (sel_mixed_srcs[i]->find_forward(sel_mixed_srcs[i], fp, &gt) && !gt)
      bool_mask |= sel_mixed_srcs[i]->bool_mask;
  sel_mixed_record(bool_mask);
  i = sel_mixed_heap_n;
  while (m--)
    {
      i++;
      sel_mixed_next(heap[i], fp);
      if (heap[i]->record)
        {
	  heap[++sel_mixed_heap_n] = heap[i];
	  HEAP_INSERT(struct sel_src *, sel_mixed_heap, sel_mixed_heap_n, SEL_MIXED_HEAP_CMP, HEAP_SWAP);
	}
    }
}

int
sel_mixed(struct selector *selector, void (*f)(struct selector *selector))
{
  ASSERT(selector->result_type == ST_MIXED);
  struct selector_internal *sel = (struct selector_internal *)selector;
  struct select_alg *alg = sel->alg;
  ASSERT(alg);

  /* Prepare algorithm */
  DBG("Preparing selector algorithm");
  if (alg->prepare && !alg->prepare(selector))
    {
      MAN_VERBOSE("No records selected");
      return 0;
    }

  /* Prepare sources */
  DBG("Preparing sources");
  clist_init(&sel->resolve_list);
  uns count = 0, needed_count = 0, maybe_count = 0;
  CLIST_FOR_EACH(struct sel_src *, src, selector->lsrc)
    {
      if (count == 31)
	die("Too many sources");
      src->bool_id = count++;
      src->bool_mask = 1 << src->bool_id;
      ASSERT((src->flags & (SEL_MIXED_NEEDED | SEL_MIXED_MAYBE)) != (SEL_MIXED_NEEDED | SEL_MIXED_MAYBE));
      needed_count += !!(src->flags & SEL_MIXED_NEEDED);
      maybe_count += !!(src->flags & SEL_MIXED_MAYBE);
      if (!src->find_first(src) && (src->flags & SEL_MIXED_NEEDED))
        {
	  DBG("Needed source is empty");
	  MAN_VERBOSE("No recors selected");
	  return 0;
	}
      if (src->resolve)
	clist_add_tail(&sel->resolve_list, &src->resolve_node);
    }
  uns merge_count = count - maybe_count;
  if (!merge_count)
    {
      DBG("No sources to merge");
      MAN_VERBOSE("No records selected");
      return 0;
    }
  uns merge_and = needed_count || merge_count == 1;

  sel_mixed_selector = selector;
  sel_mixed_f = f;
  sel_mixed_count = count;
  sel_mixed_merge_count = merge_count;
  sel_mixed_srcs = alloca(sel_mixed_count * sizeof(*sel_mixed_srcs));
  sel_mixed_merge_bool_mask = 0;
  uns i = 0;
  CLIST_FOR_EACH(struct sel_src *, src, selector->lsrc)
    if ((merge_count == 1 || !merge_and) && !(src->flags & SEL_MIXED_MAYBE) || merge_and && (src->flags & SEL_MIXED_NEEDED))
      {
        sel_mixed_srcs[i++] = src;
	sel_mixed_merge_bool_mask |= src->bool_mask;
      }
  CLIST_FOR_EACH(struct sel_src *, src, selector->lsrc)
    if (!((merge_count == 1 || !merge_and) && !(src->flags & SEL_MIXED_MAYBE) || merge_and && (src->flags & SEL_MIXED_NEEDED)))
      sel_mixed_srcs[i++] = src;
  ASSERT(i == count);

  /* Generate candidate footprints */
  struct fastbuf *fps = NULL;
  if (!selector->need_all && alg->preselect)
    {
      MAN_VERBOSE("Preselecting candidates");
      uns order;
      fps = alg->preselect(selector, &order);
      brewind(fps);
      if (!bfilesize(fps))
        {
          DBG("No preselected records");
          MAN_VERBOSE("No records selected");
	  goto end;
	}

      if (order != INDEX_ORDER_BY_FP)
        {
	  MAN_VERBOSE("Sorting candidates");
	  fps = footprint_sort(fps, NULL);
	}
    }

  /* Initialize merge heap */
  sel_mixed_heap_n = 0;
  sel_mixed_heap = alloca((sel_mixed_merge_count + 1) * sizeof(*sel_mixed_heap));
  if (!merge_and)
    {
      for (uns i = 0; i < sel_mixed_merge_count; i++)
        if (sel_mixed_srcs[i]->record)
          sel_mixed_heap[++sel_mixed_heap_n] = sel_mixed_srcs[i];
      HEAP_INIT(struct sel_src *, sel_mixed_heap, sel_mixed_heap_n, SEL_MIXED_HEAP_CMP, HEAP_SWAP);
      DBG("Initialized heap of %u records", sel_mixed_heap_n);
    }

  /* Process everything */
  uns gt;
  if (!fps)
    {
      if (merge_and)
        {
	  DBG("Filtering records (all, and)");
	  while (sel_mixed_srcs[0]->record && sel_mixed_and_step());
	}
      else
        {
	  DBG("Filtering records (all, or)");
	  while (sel_mixed_heap_n)
	    sel_mixed_or_step();
	}
      return 1;
    }

  /* Process candidates only */
  DBG("Filtering records (candidates, %s)", merge_and ? "and" : "or");
  struct footprint fp, last_fp;
  breadb(fps, &fp, sizeof(fp));
next:
  if (!fp.rest.x[0] && !fp.rest.x[1])
    {
      DBG("Read candidate site %08x%08x", FP_PAIR(fp.site));
      if (merge_and)
        {
	  sel_mixed_srcs[0]->find_forward(sel_mixed_srcs[0], &fp, &gt);
	  while (sel_mixed_srcs[0]->record && !site_fp_cmp(&((struct footprint *)sel_mixed_srcs[0]->record)->site, &fp.site))
	    if (!sel_mixed_and_step())
	      goto end;
	}
      else
        {
	  sel_mixed_or_seek(&fp);
	  while (sel_mixed_heap_n && !site_fp_cmp(&((struct footprint *)sel_mixed_heap[1]->record)->site, &fp.site))
	    sel_mixed_or_step();
	}
      last_fp.site = fp.site;
      while (bread(fps, &fp, sizeof(fp)))
        if (site_fp_cmp(&fp.site, &last_fp.site))
	  goto next;
    }
  else
    {
      DBG("Read candidate FP %08x%08x:%08x%08x", FP_QUAD(fp));
      if (merge_and)
        {
	  if (sel_mixed_srcs[0]->find_forward(sel_mixed_srcs[0], &fp, &gt) && !gt)
	    if (!sel_mixed_and_step())
	      goto end;
	}
      else
        {
	  sel_mixed_or_seek(&fp);
	  if (sel_mixed_heap_n && !fp_cmp(sel_mixed_heap[1]->record, &fp))
	    sel_mixed_or_step();
	}
      last_fp = fp;
      while (bread(fps, &fp, sizeof(fp)))
        if (fp_cmp(&fp, &last_fp))
	  goto next;
    }
end:
  bclose(fps);
  DBG("Done");
  return 1;
}

struct site *
sel_site_next(struct selector *selector, struct site *s)
{
  struct select_alg *alg = ((struct selector_internal *)selector)->alg;
  do
    s = site_next(s);
  while (s && (alg->site_test && !alg->site_test(s)));
  return s;
}

struct selector *
selector_init(enum sel_type result_type)
{
  struct mempool *pool = mp_new(65536);
  struct selector_internal *s = mp_alloc_zero(pool, sizeof(*s));
  DBG("Creating new selector %p of type %u", s, result_type);
  s->pub.pool = pool;
  s->pub.result_type = result_type;
  clist_init(&s->pub.lsrc);
  return &s->pub;
}

void
selector_cleanup(struct selector *selector)
{
  struct selector_internal *s = (struct selector_internal *)selector;
  DBG("Destroying selector %p", s);
  ASSERT(s);
  if (s->alg && s->alg->cleanup)
    s->alg->cleanup(selector);
  void *tmp;
  CLIST_FOR_EACH_DELSAFE(struct sel_src *, src, selector->lsrc, tmp)
    if (src->flags & SEL_MIXED_AUTO_CLOSE)
      sel_src_close(src);
  mp_delete(s->pub.pool);
}

byte *
selector_opt(struct selector *selector, enum sel_opt opt_type, byte *value)
{
  struct selector_internal *s = (struct selector_internal *)selector;
  DBG("Adding option %s of type %u to selector %p", value, opt_type, selector);
  ASSERT(s);
  if (opt_type != s->last_opt_type)
    {
      struct select_alg_usage *u = usages;
      while (u->result_type && (u->result_type != s->pub.result_type || u->opt_type != opt_type || (s->alg && s->alg != u->alg)))
        u++;
      if (!u->result_type)
        {
	  if (s->alg)
	    return "This combination of selector's options is not allowed.";
	  else
	    return "This combination of selector and option is not allowed.";
	}
      DBG("Selecting algorithm %p", u->alg);
      s->last_opt_type = opt_type;
      s->alg = u->alg;
      s->add_arg = u->add_arg;
      if (s->alg->init)
	s->alg->init(selector);
    }
  if (value)
    s->add_arg(selector, value);
  return NULL;
}

byte *
selector_check(struct selector *selector)
{
  DBG("Checking selector %p", selector);
  struct selector_internal *s = (struct selector_internal *)selector;
  ASSERT(s);
  if (!s->alg)
    return selector_opt(selector, SEL_ALL, NULL);
  ASSERT(s->alg);
  return NULL;
}
