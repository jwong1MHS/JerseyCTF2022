/*
 *	Sherlock Filter Engine -- Builtin Functions and Variables
 *
 *	(c) 1999--2007 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2003 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/url.h"
#include "ucw/wildmatch.h"
#include "filter/filter.h"

#ifdef CONFIG_CUSTOM_FILTER
#include CONFIG_CUSTOM_FILTER
#endif

#ifndef CUSTOM_FILTER_BUILTINS
#define CUSTOM_FILTER_BUILTINS
#endif

#ifndef CUSTOM_FILTER_VARS
#define CUSTOM_FILTER_VARS
#endif

#include <errno.h>
#include <stdlib.h>
#include <string.h>

/*
 *  Functions
 */

static void
fv_str2int(struct filter_args *args UNUSED, struct filter_value *ret, struct filter_value arg[MAX_FUNC_ARGS])
{
	if (arg[0].undef)
		ret->undef = 1;
	else
	{
		char *c;
		errno = 0;
		ret->v.i = strtoul(arg[0].v.s, &c, 0);
		if (c && *c || errno == ERANGE)
			ret->undef = 1;
		else
			ret->undef = 0;
	}
}

#define	MAX_REGEX_BUF	1024

static void
fv_replace(struct filter_args *args, struct filter_value *ret, struct filter_value arg[MAX_FUNC_ARGS])
{
	byte buf[MAX_REGEX_BUF];
	int res;
	ret->undef = 1;
	if (arg[0].undef || arg[1].undef || arg[2].undef)
		return;
	res = rx_subst(args->filter->lookup[arg[1].v.i].regex->regex, arg[2].v.s, arg[0].v.s, buf, MAX_REGEX_BUF);
	if (res < 0)
	{
		log(L_ERROR, "Cannot perform fv_replace, buffer of length %d is too short for %s -> %s", MAX_REGEX_BUF, arg[0].v.s, arg[2].v.s);
	}
	else if (res == 0)
	{
		ret->undef = arg[3].undef;
		ret->v.s = arg[3].v.s;
	}
	else
	{
		ret->undef = 0;
		ret->v.s = mp_alloc(args->pool, strlen(buf)+1);
		strcpy(ret->v.s, buf);
	}
}

static void
fv_has_repeated_component(struct filter_args *args UNUSED, struct filter_value *ret, struct filter_value arg[MAX_FUNC_ARGS])
{
	if (arg[0].undef)
	{
		ret->undef = 1;
		return;
	}
	ret->undef = 0;
	ret->v.i = url_has_repeated_component(arg[0].v.s);
}

static void
fv_standardize_domain(struct filter_args *args UNUSED, struct filter_value *ret, struct filter_value arg[MAX_FUNC_ARGS])
{
	if (arg[0].undef)
	{
		ret->undef = 1;
		return;
	}
	ret->undef = 0;
	byte *domain = arg[0].v.s;
	if (*domain == '.')
		domain++;
	if (!strncasecmp(domain, "www.", 4))
		domain += 4;
	ret->v.s = mp_alloc(args->pool, strlen(domain)+2);
	ret->v.s[0] = '.';
	strcpy(ret->v.s+1, domain);
}

static void
fv_cut_at(struct filter_args *args UNUSED, struct filter_value *ret, struct filter_value arg[MAX_FUNC_ARGS])
{
	if (arg[0].undef || arg[1].undef)
	{
		ret->undef = 1;
		return;
	}
	byte *p = strrchr(arg[0].v.s, *arg[1].v.s);
	if (p)
	{
		ret->v.s = mp_strdup(args->pool, p);
		ret->undef = 0;
	}
	else
		ret->undef = 1;
}

static void
fv_remove_url_param(struct filter_args *args, struct filter_value *ret, struct filter_value arg[MAX_FUNC_ARGS])
{
  ret->undef = arg[0].undef;
  ret->v.s = arg[0].v.s;

  char *argv;
  if (!arg[0].undef && !arg[1].undef && *(argv = arg[1].v.s))
    {
      struct url url;
      char buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], buf3[MAX_URL_SIZE];
      if (url_canon_split(arg[0].v.s, buf1, buf2, &url))
	return;
      char *in = strchr(url.rest, '?'), *out = in, *next;
      if (!in)
	return;

      char *p = argv;
      do
        {
          int c = *p++;
          if (c == '*')
            break;
          if (c == '?')
            break;
        }
      while (*p);
      const uns l = p - argv;

      struct wildpatt *wp = !*p ? NULL : wp_compile(argv, args->pool);

      uns sep = '?';
      while (*in++)
        {
	  next = strchr(in, '&') ? : in + strlen(in);

          int nb;
          if (!wp)
            {
              nb = strncmp(in, argv, l) || in[l] != '=';
            }
          else
            {
              char *m = memchr(in, '=', next-in) ? : next;
              int c = *m;
              *m = 0;
              nb = !wp_match(wp, in);
              *m = c;
            }

	  if (nb)
	    {
	      *out++ = sep;
	      sep = '&';
	      if (out != in)
		memmove(out, in, next - in);
	      out += next - in;
	    }
	  in = next;
	}
      if (in == out)
	return;
      *out = 0;
      out = mp_start_noalign(args->pool, MAX_URL_SIZE);
      if (url_pack(&url, buf3) || url_enescape(buf3, out))
	return;
      ret->v.s = mp_end(args->pool, out + strlen(out) + 1);
    }
}

static void
fv_set_url_param(struct filter_args *args, struct filter_value *ret, struct filter_value arg[MAX_FUNC_ARGS])
{
  ret->undef = arg[0].undef;
  ret->v.s = arg[0].v.s;

  struct url url;
  char buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE], buf3[MAX_URL_SIZE];

  if (!arg[0].undef && !arg[1].undef && !arg[2].undef && !url_canon_split(arg[0].v.s, buf1, buf2, &url))
    {
      char *argv = arg[1].v.s;
      char *val = arg[2].v.s;

      if (*argv && *val)
        {
          char dst[MAX_URL_SIZE];

          char *rest = url.rest;
          char *in;
          for (in = rest; *in && *in != '?'; in++);
          if (*in)
            in++;
          if (*in)
            {
              const uns val_len = strlen(val);

              char *p = argv;
              do
                {
                  int c = *p++;
                  if (c == '*')
                    break;
                  if (c == '?')
                    break;
                }
              while (*p);
              const uns argv_len = p-argv;

              struct wildpatt *wp = !*p ? NULL : wp_compile(argv, args->pool);

              char *d = dst;

              if((d-dst)+(in-rest) >= MAX_URL_SIZE)
                return;

              memcpy(d, rest, in-rest);
              d += in-rest;
              while (*in)
                {
                  char *m, *e;
                  for (m = in; *m && *m != '=' && *m != '&'; m++);
                  for (e = m; *e && *e != '&'; e++);

                  int b;
                  if (!wp)
                    {
                      b = (argv_len == (uns)(m-in)) && !memcmp(in, argv, argv_len);
                    }
                  else
                    {
                      int c = *m;
                      *m = 0;
                      b = wp_match(wp, in);
                      *m = c;
                    }

                  if(!b)
                    {
                      if ((d-dst)+(e-in)+1 >= MAX_URL_SIZE)
                        return;

                      for(; in < e; *d++ = *in++);
                      if (*in)
                        *d++ = *in++;
                    }
                  else
                    {
                      if ((d-dst)+(m-in)+1+val_len+1 >= MAX_URL_SIZE)
                        return;

                      for (; in < m; *d++ = *in++);
                      *d++ = '=';
                      memcpy(d, val, val_len);
                      d += val_len;

                      in = e;
                      if (*in)
                        {
                          *d++ = '&';
                          in++;
                        }
                    }
                }

              if (d-dst+1 >= MAX_URL_SIZE)
                return;

              *d = 0;
              url.rest = dst;
            }

          char *dst_url = mp_start_noalign(args->pool, MAX_URL_SIZE);
          if (!url_pack(&url, buf3) && !url_enescape(buf3, dst_url))
            ret->v.s = mp_end(args->pool, dst_url + strlen(dst_url) + 1);
        }
    }
}

struct filter_function filter_builtin_func[] = {
	{ "toint",			1, {F_ET_STRING},				F_ET_INT,	fv_str2int },
	{ "replace",			4, {F_ET_STRING, F_ET_REGEXP, F_ET_STRING, F_ET_STRING}, F_ET_STRING,	fv_replace },
	{ "has_repeated_component",	1, {F_ET_STRING},		F_ET_INT,	fv_has_repeated_component },
	{ "standardize_domain",		1, {F_ET_STRING},		F_ET_STRING,	fv_standardize_domain },
	{ "cut_at",			2, {F_ET_STRING, F_ET_STRING},	F_ET_STRING,	fv_cut_at },
	{ "remove_url_param",		2, {F_ET_STRING, F_ET_STRING},	F_ET_STRING,	fv_remove_url_param },
	{ "set_url_param",		3, {F_ET_STRING, F_ET_STRING, F_ET_STRING},	F_ET_STRING,	fv_set_url_param },
	CUSTOM_FILTER_BUILTINS
	{ NULL,				0, {},						0,		NULL }
};

/*
 *  Variables. Remember to update filter-test.c accordingly.
 */

struct filter_variable filter_builtin_vars[] = {
  /* URL and its parts */
  { "url",		F_LVC_RAW,	F_ET_STRING,	1 },
  { "protocol",		F_LVC_RAW,	F_ET_STRING,	1 },
  { "host",		F_LVC_RAW,	F_ET_STRING,	1 },
  { "port",		F_LVC_RAW,	F_ET_INT,	1 },
  { "path",		F_LVC_RAW,	F_ET_STRING,	1 },
  { "username",		F_LVC_RAW,	F_ET_STRING,	0 },
  { "password",		F_LVC_RAW,	F_ET_STRING,	0 },
  /* Gatherer attributes */
  { "server_type",	F_LVC_RAW,	F_ET_STRING,	1 },
  { "content_type",	F_LVC_RAW,	F_ET_STRING,	0 },
  { "content_encoding",	F_LVC_RAW,	F_ET_STRING,	0 },
  { "ignore_links",	F_LVC_RAW,	F_ET_INT,	0 },
  { "ignore_text",	F_LVC_RAW,	F_ET_INT,	0 },
  { "error_code",	F_LVC_RAW,	F_ET_INT,	0 },
  { "user_mark",	F_LVC_RAW,	F_ET_INT,	0 },
  /* Sections, limits and queueing */
  { "section",		F_LVC_RAW,	F_ET_INT,	0 },
  { "section_soft_max",	F_LVC_RAW,	F_ET_INT,	0 },	/* gatherd only */
  { "section_hard_max",	F_LVC_RAW,	F_ET_INT,	0 },	/* gatherd only */
  { "queue_bonus",	F_LVC_RAW,	F_ET_INT,	0 },
  { "queue_key",	F_LVC_RAW,	F_ET_INT,	1 },
  /* Shepherd limits and timings */
  { "soft_limit",	F_LVC_RAW,	F_ET_INT,	0 },
  { "hard_limit",	F_LVC_RAW,	F_ET_INT,	0 },
  { "fresh_limit",	F_LVC_RAW,	F_ET_INT,	0 },
  { "min_delay",	F_LVC_RAW,	F_ET_INT,	0 },
  { "max_conn",		F_LVC_RAW,	F_ET_INT,	0 },
  { "monitor",		F_LVC_RAW,	F_ET_INT,	0 },
  { "select_bonus",	F_LVC_RAW,	F_ET_INT,	0 },
  { "refresh_schema",	F_LVC_RAW,	F_ET_INT,	0 },
  { "refresh_boost",	F_LVC_RAW,	F_ET_INT,	0 },
  { "stable_time",	F_LVC_RAW,	F_ET_INT,	0 },
  { "filter_robots",	F_LVC_RAW,	F_ET_INT,	0 },
  /* Indexer attributes */
  { "site",		F_LVC_RAW,	F_ET_STRING,	0 },
  { "site_level",	F_LVC_RAW,	F_ET_INT,	0 },
  { "bonus",		F_LVC_RAW,	F_ET_INT,	0 },
  { "language",		F_LVC_RAW,	F_ET_STRING,	0 },
  { "card_bonus",	F_LVC_RAW,	F_ET_INT,	0 },
  { "title",		F_LVC_RAW,	F_ET_STRING,	1 },
  { "image_size",	F_LVC_RAW,	F_ET_INT,	1 },
  { "image_aspect_ratio",F_LVC_RAW,	F_ET_INT,	1 },
  { "image_colors",	F_LVC_RAW,	F_ET_INT,	1 },
  { "audio_length",	F_LVC_RAW,	F_ET_INT,	1 },
  { "audio_bitrate",	F_LVC_RAW,	F_ET_INT,	1 },
  { "audio_srate",	F_LVC_RAW,	F_ET_INT,	1 },
  { "audio_channels",	F_LVC_RAW,	F_ET_INT,	1 },
  { "noindex",		F_LVC_RAW,	F_ET_INT,	0 },

  /* Area attributes */
  { "area",		F_LVC_RAW,	F_ET_INT,	0 },
  /* Link attributes */
  { "want_xform",	F_LVC_RAW,	F_ET_INT,	0 },
  { "url_xform",	F_LVC_RAW,	F_ET_STRING,	0 },
  { "src_url",		F_LVC_RAW,	F_ET_STRING,	1 },
  /* Extended attributes, all attributes not listed explicitly are of type
   * string by default */
  { "s",		F_LVC_ATTR,	F_ET_INT,	1 },

  CUSTOM_FILTER_VARS
  { NULL,		0,		0,		0 }
};
