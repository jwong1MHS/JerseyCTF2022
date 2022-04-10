/*
 *	Sherlock Analyser -- Simple Testing Program
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "sherlock/object.h"
#include "analyser/analyser.h"

#include <stdio.h>
#include <stdlib.h>

static char *options = CF_SHORT_OPTS "h:v";

static char *help = "\
Usage: cat <source> | atest <options>\n\
\n\
Options:\n"
CF_USAGE "\
-h <hook>\t\tAct as if called from the given hook (default: atest)\n\
-v\t\t\tIncrease verbosity (Analyser.Trace)\n\
";

static void NONRET
usage(void)
{
  fputs(help, stderr);
  exit(1);
}

static struct an_context an_context;

static void
setup_ai(struct an_iface *ai, struct odes *o)
{
  bzero(ai, sizeof(*ai));
  ai->obj = o;
  if (obj_find_attr(o, 'U'))
    {
      ai->url_block = o;
      if (an_context.need_mask & AN_NEED_ALL_URLS)
	{
	  ai->all_urls = mp_alloc_zero(o->pool, 2*sizeof(struct odes *));
	  ai->all_urls[0] = o;
	}
    }
  else
    {
      struct oattr *urls = obj_find_attr(o, 'U' + OBJ_ATTR_SON);
      if (!urls)
	die("Malformed object: no URL blocks found");
      ai->url_block = urls->son;
      if (an_context.need_mask & AN_NEED_ALL_URLS)
	{
	  uns cnt = 1;
	  for (struct oattr *u = urls; u; u=u->same)
	    {
	      cnt++;
	      for (struct oattr *r = obj_find_attr(u->son, 'y' + OBJ_ATTR_SON); r; r=r->same)
		cnt++;
	    }
	  ai->all_urls = mp_alloc_zero(o->pool, cnt*sizeof(struct odes *));
	  cnt = 0;
	  for (struct oattr *u = urls; u; u=u->same)
	    {
	      ai->all_urls[cnt++] = u->son;
	      for (struct oattr *r = obj_find_attr(u->son, 'y' + OBJ_ATTR_SON); r; r=r->same)
		ai->all_urls[cnt++] = r->son;
	    }
	}
    }
}

static void
setup_streams(struct an_iface *ai, uns need)
{
  static struct fastbuf *text, *metas;

  if (need & AN_NEED_TEXT)
    {
      if (an_trace)
	log(L_DEBUG, "Preparing text stream");
      if (!text)
	text = fbgrow_create(4096);
      fbgrow_reset(text);
      for (struct oattr *a = obj_find_attr(ai->obj, 'X'); a; a=a->same)
	{
	  bputs(text, a->val);
	  if (a->same)
	    bputc(text, ' ');
	}
      fbgrow_rewind(text);
      ai->text = text;
    }
  if (need & AN_NEED_METAS)
    {
      if (an_trace)
	log(L_DEBUG, "Preparing meta stream");
      if (!metas)
	metas = fbgrow_create(4096);
      fbgrow_reset(metas);
      for (struct oattr *a = obj_find_attr(ai->obj, 'M'); a; a=a->same)
	bputs(metas, a->val);
      fbgrow_rewind(metas);
      ai->metas = metas;
    }
}

int
main(int argc, char **argv)
{
  int opt;
  int hook = AN_HOOK_ATEST;

  log_init("atest");
  while ((opt = cf_getopt(argc, argv, options, CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
      {
      case 'h':
	if ((hook = analyser_lookup_hook(optarg)) < 0)
	  {
	    fprintf(stderr, "Unknown hook name `%s'\n", optarg);
	    usage();
	  }
	break;
      case 'v':
	an_trace++;
	break;
      default:
	usage();
      }
  if (optind != argc)
    usage();

  struct mempool *mp = mp_new(4096);
  analyser_init_hook(hook);
  analyser_init(&an_context, hook, AN_NEED_TEXT | AN_NEED_METAS | AN_NEED_ALL_URLS, NULL);
  struct fastbuf *bin = bfdopen_shared(0, 4096);
  struct fastbuf *bout = bfdopen_shared(1, 4096);
  int next;
  do
    {
      mp_flush(mp);
      struct odes *o = obj_new(mp);
      next = obj_read(bin, o);
      if (!o->attrs)
	break;

      struct an_iface ai;
      setup_ai(&ai, o);
      ai.pool = mp;
      uns need = analyser_need(&an_context, &ai);
      if (need)
	{
	  setup_streams(&ai, need);
	  analyser_run_needed(&an_context, &ai);
	}

      obj_write(bout, o, BUCKET_TYPE_PLAIN);
      bputc(bout, '\n');
    }
  while (next);
  bclose(bin);
  bclose(bout);
  analyser_log_stats(&an_context);
  analyser_cleanup(&an_context);
  return 0;
}
