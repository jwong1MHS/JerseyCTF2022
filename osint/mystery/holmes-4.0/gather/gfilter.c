/*
 *	Sherlock Gatherer -- Filtering
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "gather/gather.h"
#include "filter/filter.h"
#include "analyser/analyser.h"

static struct filter_binding gf_bindings[] = {
  /* URL and its parts */
  { "url",		OFFSETOF(struct gobject, url) },
  { "protocol",		OFFSETOF(struct gobject, url_s.protocol) },
  { "host",		OFFSETOF(struct gobject, url_s.host) },
  { "port",		OFFSETOF(struct gobject, url_s.port) },
  { "path",		OFFSETOF(struct gobject, url_s.rest) },
  /* Read-write version of url_s.user and url_s.pass */
  { "username",		OFFSETOF(struct gobject, auth_user) },
  { "password",		OFFSETOF(struct gobject, auth_pass) },
  /* Gatherer attributes */
  { "server_type",	OFFSETOF(struct gobject, http_server) },
  { "content_type",	OFFSETOF(struct gobject, content_type) },
  { "content_encoding",	OFFSETOF(struct gobject, content_encoding) },
  { "ignore_links",	OFFSETOF(struct gobject, dont_follow_links) },
  { "ignore_text",	OFFSETOF(struct gobject, dont_save_contents) },
  { "error_code",	OFFSETOF(struct gobject, filter_error_code) },
  { "user_mark",	OFFSETOF(struct gobject, filter_user_mark) },
  /* Document language (if in final stage) */
  { "language",		OFFSETOF(struct gobject, filter_language) },
  { NULL,		0 }
};

static struct filter *gf_filter;
static struct filter_args *gf_filter_args;
static uns gf_active;

void
gather_init_filter(void)
{
  if (!gather_filter_name)
    return;
  gf_filter = filter_load(gather_filter_name, filter_builtin_vars, gf_bindings, NULL);
  gf_filter_args = filter_intr_new(gf_filter);
  gf_filter_args->config_changes_mode = 1;
}

void
gather_filter(uns final)
{
  guess_content();
  gthis->auth_user = gthis->url_s.user;
  gthis->auth_pass = gthis->url_s.pass;
  gthis->filter_error_code = 2401;
  if (final)
    {
      obj_set_attr(gthis->aa, 'l', gthis->language);	/* XXX: See the hack alert in ganalyser.c */
      gthis->filter_language = an_lang_decide_language(gthis->aa);
      obj_set_attr(gthis->aa, 'l', NULL);
    }
  else
    gthis->filter_language = NULL;
  if (gf_filter)
    {
      struct filter_args *a = gf_filter_args;
      a->pool = gthis->pool;
      a->raw = gthis;
      a->attr = gthis->aa;
      if (!gf_active++)
	filter_intr_undo_init(gf_filter_args);
      if (!filter_intr_run(a) && !gthis->robot_file_p)
	gerror(gthis->filter_error_code, "%s", a->msg ? : (byte *) "Filtered out");
    }
  if (gthis->content_encoding && !identify_content_encoding(gthis->content_encoding))
    gerror(2403, "Unknown content encoding %s", gthis->content_encoding);
  if (gthis->content_type && !identify_content_type(gthis->content_type))
    gerror(2400, "Unknown content type %s", gthis->content_type);
}

void
gather_filter_undo(void)
{
  if (gf_active)
    {
      filter_intr_undo(gf_filter_args);
      gf_active = 0;
    }
}
