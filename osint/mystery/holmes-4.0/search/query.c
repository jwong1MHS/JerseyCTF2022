/*
 *	Sherlock Search Engine -- Query Processing
 *
 *	(c) 1997--2005 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/stkstring.h"
#include "sherlock/index.h"
#include "ucw/unicode.h"
#include "indexer/params.h"
#include "search/sherlockd.h"

#ifdef CONFIG_LANG
#include "lang/lang.h"
#endif

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <setjmp.h>
#include <unistd.h>

struct query *current_query;
struct database *current_dbase;
static jmp_buf query_err_jmp;
static u64 incarnation;

/*** Profiling counters ***/

#define P(x) prof_##x
prof_t PROFILERS(COMMA);
prof_t prof_send;
#undef P

static prof_t *profiler_current;
static timestamp_t query_timer;

static void
profiler_init(void)
{
  init_timer(&query_timer);
#define P(x) prof_init(&prof_##x)
  PROFILERS(;);	/* Initialize all profilers */
#undef P
  prof_init(&prof_send);
  profiler_current = NULL;
}

static void
profiler_show(struct query *q)
{
#ifdef PROFILER
  byte stats[1024], *x = stats;
#define P(x) str_##x[PROF_STR_SIZE]
  byte PROFILERS(COMMA);
#undef P
#define P(x) prof_format(str_##x, &prof_##x)
  PROFILERS(;);
#undef P
#define P(x) "%.4s=%s"
  x += sprintf(x, PROFILERS(" "),
#undef P
#define P(x) #x, str_##x
	 PROFILERS(COMMA));
#undef P
  if (q->cache_age >= 0)
    x += sprintf(x, " cage:%d", q->cache_age);
  else
    x += sprintf(x, " wrds:%d phrs:%d near:%d chns:%d chKB:%u",
		 q->nwords, q->nphrases, q->nnears,
		 q->stat_num_chains, q->stat_len_chains / 1024);
  q->profile_stats = mp_memdup(q->pool, stats, x-stats+1);
  add_footer("T%s", stats);
#endif

  q->time_total = get_timer(&query_timer);
  add_footer("t%d", q->time_total);
}

prof_t *
profiler_switch(prof_t *p)
{
  prof_t *o = profiler_current;
  if (p == o)
    return o;
  if (p)
    {
      if (o)
	prof_switch(o, p);
      else
	prof_start(p);
    }
  else if (o)
    prof_stop(o);
  profiler_current = p;
  return o;
}

/*** Debugging dumps ***/

static byte *
format_expr(struct query *q, struct expr *e, byte *buf, byte *bend, uns lastop, uns mode)
{
  if (!buf || buf + 64 > bend)
    return NULL;
  if (!e)
    {
      buf += sprintf(buf, "<null>");
      return buf;
    }
  switch (e->type)
    {
    case EX_MATCH:
      if (buf + 32 + strlen(e->u.match.word) > bend)
	return NULL;
      buf += sprintf(buf, "\"%s\"[T%08x t%d S%d W%d A%d M%d P%d Y%d X%lld]",
		     e->u.match.word,
		     e->u.match.type_mask,
		     e->u.match.is_string,
		     e->u.match.sense,
		     e->u.match.o.weight,
		     e->u.match.o.accent_mode,
		     e->u.match.o.morphing,
		     e->u.match.o.spelling,
		     e->u.match.o.synonyming,
		     (long long) e->u.match.o.syn_expand);
      if (e->u.match.next_simple && mode)
	{
	  *buf++ = '.';
	  buf = format_expr(q, e->u.match.next_simple, buf, bend, EX_MATCH, mode);
	}
      break;
    case EX_OPTIONS:
      buf += sprintf(buf, "OPTIONS(W=%d A=%d M=%d P=%d Y=%d X=%lld T=%08x ",
		     e->u.options.o.weight,
		     e->u.options.o.accent_mode,
		     e->u.options.o.morphing,
		     e->u.options.o.spelling,
		     e->u.options.o.synonyming,
		     (long long) e->u.options.o.syn_expand,
		     e->u.options.o.default_word_types);
      buf = format_expr(q, e->u.options.inside, buf, bend, EX_OPTIONS, mode);
      if (!buf || buf + 2 >= bend)
	buf = NULL;
      else
	buf += sprintf(buf, ")");
      break;
    case EX_ANY:
      buf += sprintf(buf, "ANY");
      break;
    case EX_NONE:
      buf += sprintf(buf, "NONE");
      break;
    case EX_IGNORE:
      buf += sprintf(buf, "IGNORE");
      break;
    case EX_NOT:
      *buf++ = '!';
      buf = format_expr(q, e->u.op.l, buf, bend, EX_NOT, mode);
      break;
    case EX_AND:
    case EX_OR:
      if (lastop != e->type)
	*buf++ = '(';
      buf = format_expr(q, e->u.op.l, buf, bend, e->type, mode);
      if (!buf || buf + 32 > bend)
	return NULL;
      buf += sprintf(buf, " %c ", (e->type == EX_AND ? '&' : '|'));
      buf = format_expr(q, e->u.op.r, buf, bend, e->type, mode);
      if (buf)
	{
	  if (lastop != e->type)
	    *buf++ = ')';
	  *buf = 0;
	}
      break;
    case EX_WORD:
      buf += sprintf(buf, "W(%d)", e->u.word.w->index);
      break;
    case EX_PHRASE:
      buf += sprintf(buf, "P(%d)", e->u.phrase.p->index);
      break;
    case EX_IMAGE_SIM_MATCH:
      buf = image_sim_match_format(&e->u.image_sim_match, buf, bend);
      break;
    case EX_IMAGE_SIM:
      buf = image_sim_format(e->u.image_sim, buf, bend);
      break;
    default:
      ASSERT(0);
    }
  return buf;
}

static void
debug_expr(struct query *q, struct expr *e, uns attr)
{
  byte buf[2048];

  if (!(q->debug & DEBUG_ANALYSE))
    return;
  if (!format_expr(q, e, buf, buf+sizeof(buf)-1, EX_RESERVED, 1))
    strcpy(buf, "<too long>");
  add_reply(".%c: %s", attr, buf);
}

static void
debug_simple(struct query *q, clist *l, uns attr)
{
  struct simple *s;
  byte buf[2048], *b=buf, *bend=buf+sizeof(buf)-1;
  uns cnt = 0;

  if (!(q->debug & DEBUG_ANALYSE))
    return;
  CLIST_WALK(s, *l)
    {
      if (b >= bend-2)
	{
	toolong:
	  b = NULL;
	  break;
	}
      if (cnt++)
	*b++ = ' ';
      *b++ = '(';
      if (!(b = format_expr(q, s->raw, b, bend, EX_RESERVED, 0)) || b >= bend)
	goto toolong;
      *b++ = ',';
      if (!(b = format_expr(q, s->cooked, b, bend, EX_RESERVED, 0)) || b >= bend)
	goto toolong;
      *b++ = ')';
    }
  if (b)
    *b = 0;
  else
    strcpy(buf, "<too long>");
  add_reply(".%c: %s", attr, buf);
}

/*** Propagation of query options ***/

static struct expr *
propagate_opts(struct expr *e, struct options *o)
{
  if (!e)
    return e;
  switch (e->type)
    {
    case EX_MATCH:
      merge_options(&e->u.match.o, o, &e->u.match.o);
      if (e->u.match.type_mask == ~0U)
	e->u.match.type_mask = e->u.match.o.default_word_types;
      e->u.match.is_string = !!(e->u.match.type_mask & 0x8000);
      e->u.match.type_mask &= 0xffff00ff;
      e->u.match.next_simple = propagate_opts(e->u.match.next_simple, o);
      return e;
    case EX_OPTIONS:
      merge_options(&e->u.options.o, o, &e->u.options.o);
      return propagate_opts(e->u.options.inside, &e->u.options.o);
    case EX_AND:
    case EX_OR:
    case EX_NOT:
      e->u.op.l = propagate_opts(e->u.op.l, o);
      e->u.op.r = propagate_opts(e->u.op.r, o);
      return e;
    case EX_ANY:
      return e;
    case EX_IMAGE_SIM_MATCH:
      image_sim_match_propagate_opts(&e->u.image_sim_match, o);
      return e;
    default:
      ASSERT(0);
      return e;
    }
}

static void
process_options(struct query *q)
{
  q->expr = propagate_opts(q->expr, &q->default_options);
}

/*** Generation of normalized string form of the query for cache lookups ***/

static byte *
format_norm_query(struct query *q, byte *buf, uns size)
{
  byte *bend = buf + size - 1;

  buf += sprintf(buf, "[D%d S%llx M%d A%d P%d T%d O%d a%d G%d,%d X%x] ",
		 q->debug,
		 (long long) q->site_hash,
		 q->site_max,
		 q->allow_approx,
		 q->partial_answers,
		 q->custom_sorting ^ q->custom_sort_reverse,
		 q->custom_sort_only,
		 q->contains_accents,
		 q->age_raw_min,
		 q->age_raw_max,
		 q->explain_id);
#define INT_ATTR(id,keywd,gf,pf) \
    buf += sprintf(buf, "[" #id "=%d,%d] ", q->id##_min, q->id##_max);
#define SMALL_SET_ATTR(id,keywd,gf,pf) \
    buf += sprintf(buf, "[" #id "=%08x] ", q->id##_set);
#define LATE_INT_ATTR INT_ATTR
#define LATE_SMALL_SET_ATTR SMALL_SET_ATTR
  EXTENDED_ATTRS
#undef INT_ATTR
#undef SMALL_SET_ATTR
#undef LATE_INT_ATTR
#undef LATE_SMALL_SET_ATTR

  char *custom_key = CUSTOM_MATCH_CACHE_KEY(q, q->pool);
  if (custom_key)
    {
      uns len = strlen(custom_key);
      if (buf + len >= bend)
	return NULL;
      memcpy(buf, custom_key, len + 1);
      buf += len;
    }
  return format_expr(q, q->expr, buf, bend, EX_RESERVED, 1);
}

/*** Reply caching ***/

static clist cache_lru, *cache_hash;
static uns cache_count, hash_size;

void
cache_init(void)
{
  clist_init(&cache_lru);
  hash_size = 1;
  while (hash_size < cache_size)
    hash_size *= 2;
  cache_hash = xmalloc(sizeof(clist) * hash_size);
  for(uns i=0; i<hash_size; i++)
    clist_init(&cache_hash[i]);
}

static uns
cache_hash_fn(byte *x)
{
  struct fingerprint fp;

  fingerprint(x, &fp);
  return fp_hash(&fp) % hash_size;
}

static struct results *
lookup_cache(struct query *q)
{
  byte norm[4096];
  struct results *r;
  struct results *recycle = NULL;
  time_t now = time(NULL);
  struct mempool *pool;

  if (!format_norm_query(q, norm, sizeof(norm)))
    {
      add_err("-101 Normal form of query too long");
      return NULL;
    }
  if (q->debug & DEBUG_ANALYSE)
    add_reply(".N: %s (db_mask=%x, need %d results)", norm, q->db_mask, q->needed_results);

  uns hash = cache_hash_fn(norm);
  CLIST_WALK(r, cache_hash[hash])
    if (!strcmp(r->request, norm) &&
	(q->needed_results ? (r->db_mask == q->db_mask)
			   : ((r->db_mask & q->db_mask) == q->db_mask)))
      {
	if ((q->debug & DEBUG_NOCACHE) ||
	    r->max_matches < q->needed_results && r->num_matches == r->max_matches)
	  {
	    /*
	     * Currently, we are not smart enough to be able to re-use query
	     * analysis and just produce more results, because it would involve
	     * modifying already generated database headers in the reply.
	     */
	    recycle = r;
	  }
	else
	  {
	    clist_remove(&r->n);
	    clist_add_tail(&cache_lru, &r->n);
	    r->access_time = now;
	    return r;
	  }
      }

  if (!recycle && cache_count >= cache_size)
    recycle = SKIP_BACK(struct results, n, clist_head(&cache_lru));

  if (recycle)
    {
      clist_remove(&recycle->h);
      clist_remove(&recycle->n);
      pool = recycle->pool;
      mp_flush(pool);
    }
  else
    {
      pool = mp_new(4096);
      cache_count++;
    }

  r = mp_alloc_zero(pool, sizeof(struct results));
  clist_add_tail(&cache_hash[hash], &r->h);
  clist_add_tail(&cache_lru, &r->n);
  r->pool = pool;
  r->access_time = r->create_time = now;
  r->request = mp_strdup(pool, norm);
  r->db_mask = q->db_mask;
  if (q->needed_results > num_matches)
    r->max_matches = max_matches;
  else if (q->needed_results)
    r->max_matches = num_matches;
  init_reply_buf(&r->reply_header, pool);
  r->status = -1;
  return r;
}

/*** Query analysis ***/

static void
simple_to_list(struct query *q, clist *l, struct expr *e)
{
  struct expr *f;

  clist_init(l);
  while (e)
    {
      struct simple *s = mp_alloc(q->pool, sizeof(struct simple));
      clist_add_tail(l, &s->n);
      s->raw = e;
      s->cooked = NULL;
      f = e->u.match.next_simple;
      e = f;
    }
}

static struct expr *
simple_fold(clist *l)
{
  struct simple *s;
  struct expr *maybes, *sharps;
  uns seen_yes = 0;

  /* Process MAYBE parts of the query */
  maybes = NULL;
  CLIST_WALK(s, *l)
    {
      ASSERT(s->cooked);
      if (!s->raw->u.match.sense && s->cooked->type != EX_IGNORE)
	maybes = maybes ? new_op(EX_OR, maybes, s->cooked) : s->cooked;
    }

  /* Process YES and NO parts */
  sharps = NULL;
  CLIST_WALK(s, *l)
    if (s->raw->u.match.sense && s->cooked->type != EX_IGNORE)
      {
	struct expr *f = (s->raw->u.match.sense > 0) ? s->cooked : new_op(EX_NOT, s->cooked, NULL);
	sharps = sharps ? new_op(EX_AND, sharps, f) : f;
	if (s->raw->u.match.sense > 0)
	  seen_yes++;
      }

  if (!maybes && !sharps)
    {
      add_err("-119 Simple search expression contains only non-indexed words");
      eval_err(319);
    }
  if (!maybes || !sharps)
    return maybes ? : sharps;
  if (seen_yes)
    maybes = new_op(EX_OR, new_node(EX_ANY), maybes);
  return new_op(EX_AND, maybes, sharps);
}

static struct expr *
analyse_query(struct query *q, struct expr *e)
{
  struct expr *ll, *rr;
  clist l;

  switch (e->type)
    {
    case EX_MATCH:
      simple_to_list(q, &l, e);
      debug_simple(q, &l, 's');
      string_analyse_simple(q, &l);
      word_analyse_simple(q, &l);
      debug_simple(q, &l, 't');
      e = simple_fold(&l);
      debug_expr(q, e, 'x');
      return analyse_query(q, e);
    case EX_AND:
    case EX_OR:
      ll = analyse_query(q, e->u.op.l);
      rr = analyse_query(q, e->u.op.r);
      if (ll->type == EX_IGNORE)
	return rr;
      if (rr->type == EX_IGNORE)
	return ll;
      if (e->u.op.l != ll || e->u.op.r != rr)
	return new_op(e->type, ll, rr);
      return e;
    case EX_NOT:
      ll = analyse_query(q, e->u.op.l);
      if (ll->type == EX_IGNORE)
	return ll;
      if (e->u.op.l != ll)
	return new_op(e->type, ll, NULL);
      return e;
    case EX_IMAGE_SIM_MATCH:
      return image_sim_match_analyse(q, e);
    default:
      return e;
    }
}

/*** Taking care of words ***/

struct word *
lookup_word(struct query *q, struct expr *e, byte *wd)
{
  uns i;
  struct word *w;

  for (i=0; i<q->nwords; i++)
    {
      w = q->words[i];
      if (w->type_mask == e->u.match.type_mask &&
	  w->is_string == e->u.match.is_string &&
	  w->options.accent_mode == e->u.match.o.accent_mode &&
	  w->options.morphing == e->u.match.o.morphing &&
	  w->options.spelling == e->u.match.o.spelling &&
	  w->options.synonyming == e->u.match.o.synonyming &&
	  w->options.syn_expand == e->u.match.o.syn_expand &&
	  !strcmp(w->word, wd))
	{
	  w->use_count++;
	  return w;
	}
    }
  if (q->nwords >= max_words)
    {
      add_err("-103 Too many words");
      eval_err(103);
    }
  w = mp_alloc_zero(q->results->pool, sizeof(*w));
  q->words[q->nwords] = w;
  w->index = q->nwords++;
  w->type_mask = e->u.match.type_mask;
  w->is_string = e->u.match.is_string;
  memcpy(&w->options, &e->u.match.o, sizeof(struct options));
  w->word = mp_strdup(q->results->pool, wd);
  w->use_count = 1;
  w->boolean_id = -1;
  return w;
}

static void
debug_words(struct query *q)
{
  if (!(q->debug & DEBUG_ANALYSE))
    return;

  for (uns i=0; i<q->nwords; i++)
    {
      struct word *w = q->words[i];
      add_reply(".W%d: <%s> class=%08x is_string=%d outer=%d wei=%d acc=%d mor=%d spl=%d syn=%d sex=%llx stat=%d docs=%d vars=%d wcl=%d uc=%d hc=%d wild=%d",
		i,
		w->word,
		w->type_mask,
		w->is_string,
		w->is_outer,
		w->weight,
		w->options.accent_mode,
		w->options.morphing,
		w->options.spelling,
		w->options.synonyming,
		(long long) w->options.syn_expand,
		w->status,
		w->doc_count,
		w->var_count,
		w->word_class,
		w->use_count,
		w->hide_count,
		w->is_wild);
      word_dump_variants(w);
    }
  for (uns i=0; i<q->nphrases; i++)
    {
      struct phrase *p = q->phrases[i];
      byte buf[1024], *z = buf;
      for (uns j=0; j<p->length; j++)
	z += sprintf(z, "(%d,%d%s)",
		     p->word[j],
		     p->relpos[j],
		     (p->prox_map & (1 << j)) ? "+" : "");
      *z = 0;
      add_reply(".P: W%d [%s]", p->weight, buf);
    }
  for (uns i=0; i<q->nnears; i++)
    {
      struct phrase *p = q->nears[i];
      byte buf[1024], *z = buf;
      for (uns j=0; j<p->length; j++)
	z += sprintf(z, "(%d,%d)",
		     p->word[j],
		     p->relpos[j]);
      *z = 0;
      add_reply(".n: [%s]", buf);
    }
}

/*** Check words for errors and assign word ids ***/

static void
fixup_words(struct query *q)
{
  /* XXX: Make sure that all words have at least one variant (this is needed
   * at several places, most notably in the fulltext matcher). For normal words,
   * word_expand() takes care of it, but ignored words which don't have their
   * ph_word at all are missed. If it's the case, synthesize the dummy variant here.
   * Hopefully this will disappear soon with a rewrite of words.c.
   */
  for (uns i=0; i<q->nwords; i++)
    {
      struct word *w = q->words[i];
      if (!w->is_string)
	word_ensure_variants(w);
    }
}

static void
check_words(struct query *q)
{
  uns i, j;
  uns id = 0;
  uns tracing = (q->debug & DEBUG_ANALYSE);

  q->query_word_count = 0;
  for (i=0; i<q->nwords; i++)
    {
      struct word *w = q->words[i];
      if (w->use_count && w->status && !q->allow_approx)
	{
	  switch (w->status)
	    {
	    case 105:
	      add_err("-105 Too many word matches for <%s>", w->word);
	      break;
	    case 111:
	      add_err("-111 Wildcard pattern too long");
	      break;
	    case 113:
	      add_err("-113 Maximum wildcard zone exceeded");
	      break;
	    case 114:
	      add_err("-114 Wildcard prefix <%s> too short", w->word);
	      break;
	    case 116:
	      add_err("-116 Word <%s> not indexed", w->word);
	      break;
	    default:
	      add_err("-%d Unknown error for word <%s>", w->status, w->word);
	    }
	  eval_err(w->status + 200);
	}

      /* Assign the boolean ID only when there are variants which can be matched */
      uns var_count = 0;
      SLIST_FOR_EACH(struct variant *, v, w->variants)
	if (v->refchain_len)
	  var_count++;
      w->boolean_id = (var_count ? (int)id++ : -1);
      if (w->use_count > w->hide_count)
	q->query_word_count++;
      if (tracing)
	add_reply(".B Word <%s>: bool ID %d", w->word, w->boolean_id);
    }
  for (i=0; i<q->nphrases; i++)
    {
      struct phrase *p = q->phrases[i];
      p->word_mask = 0;
      for (j=0; j<p->length; j++)
	{
	  struct word *w = q->words[p->word[j]];
	  p->word_mask |= 1 << ((w->boolean_id < 0) ? 31 : w->boolean_id);
	}
      p->boolean_id = ((p->word_mask & 0x80000000) ? -1 : (int)id++);
      if (tracing)
	add_reply(".B Phrase %d: bool ID %d, mask %08x", i, p->boolean_id, p->word_mask);
    }
  for (i=0; i<q->nnears; i++)
    {
      struct phrase *p = q->nears[i];
      p->word_mask = 0;
      for (j=0; j<p->length; j++)
	{
	  struct word *w = q->words[p->word[j]];
	  p->word_mask |= 1 << ((w->boolean_id < 0) ? 31 : w->boolean_id);
	}
      if (tracing)
	add_reply(".B Near %d: mask %08x", i, p->word_mask);
    }
#ifdef CONFIG_IMAGES_SIM
  for (i=0; i<q->nimage_sims; i++)
    {
      struct image_sim *s = q->image_sims[i];
      s->boolean_id = id++;
      if (tracing)
	add_reply(".B ImageSim %d: bool ID %d", i, s->boolean_id);
    }
#endif
  if (id >= max_bools)
    {
      add_err("-118 Boolean expression too complex");
      eval_err(118);
    }
  q->n_bool_ids = id;
  if (tracing)
    add_reply(". Word check: %d total words, %d query words, %d boolean ID's", q->nwords, q->query_word_count, q->n_bool_ids);
}

/*** Shortcut checks ***/

static int
shortcut(struct query *q, struct expr *e)
{
  int i,j;

  /* Genuine tri-state logic: 1=yes, -1=no, 0=maybe */
  switch (e->type)
    {
    case EX_WORD:
      return ((e->u.word.w->boolean_id < 0) ? -1 : 0);
    case EX_AND:
      i = shortcut(q, e->u.op.l);
      j = shortcut(q, e->u.op.r);
      return MIN(i, j);
    case EX_OR:
      i = shortcut(q, e->u.op.l);
      j = shortcut(q, e->u.op.r);
      return MAX(i, j);
    case EX_NOT:
      return -shortcut(q, e->u.op.l);
    case EX_ANY:
      return 1;
    case EX_NONE:
      return -1;
    case EX_PHRASE:
      return ((e->u.phrase.p->boolean_id < 0) ? -1 :0);
    case EX_IMAGE_SIM:
      return 0;
    default:
      ASSERT(0);
    }
}

static void
check_shortcut(struct query *q, struct expr *e)
{
  int err = shortcut(q, e);

  if (err < 0)
    {
      add_reply(". Shortcut: no solutions");
      eval_err(1);
    }
#ifndef CONFIG_ALLOW_ANY
  if (err > 0)
    {
      add_err("-104 All documents match");
      eval_err(304);
    }
#endif
}

/*** Boolean expression evaluation ***/

static u32 *bool_map_buf;
static uns bool_bytes;

static void
map_bool(struct query *q, struct expr *x, u32 *Z)
{
  u32 *X, *Y, *X0, *Y0;
  uns c;
  int i;

  switch (x->type)
    {
    case EX_AND:
    case EX_OR:
      X = X0 = xmalloc(bool_bytes);
      Y = Y0 = xmalloc(bool_bytes);
      map_bool(q, x->u.op.l, X);
      map_bool(q, x->u.op.r, Y);
      if (x->type == EX_AND)
	for(c=0; c<bool_bytes; c += sizeof(u32))
	  *Z++ = *X++ & *Y++;
      else
	for(c=0; c<bool_bytes; c += sizeof(u32))
	  *Z++ = *X++ | *Y++;
      xfree(Y0);
      xfree(X0);
      break;
    case EX_NOT:
      X = X0 = xmalloc(bool_bytes);
      map_bool(q, x->u.op.l, X);
      for(c=0; c<bool_bytes; c += sizeof(u32))
	*Z++ = ~*X++;
      xfree(X0);
      break;
    case EX_PHRASE:
      i = x->u.phrase.p->boolean_id;
      goto got_id;
    case EX_WORD:
      ASSERT(x->u.word.w->use_count);
      i = x->u.word.w->boolean_id;
      goto got_id;
    case EX_IMAGE_SIM:
      i = x->u.image_sim->boolean_id;
      /* fall-thru */
    got_id:
      if (i < 0)
	memset(Z, 0, bool_bytes);
      else if (i < 5)			/* Fits in u32 */
	{
	  uns k = 1 << i;
	  uns t;
	  u32 j = 0;
	  for(t=0; t < 32; t++)
	    if (t & k)
	      j |= 1 << t;
	  for(c=0; c<bool_bytes; c += sizeof(u32))
	    *Z++ = j;
	}
      else
	{
	  int per = 1 << (i - 5);
	  uns t = 2*per*sizeof(u32);
	  for(c=0; c<bool_bytes; c += t)
	    {
	      for(i=0; i<per; i++)
		*Z++ = 0;
	      for(i=0; i<per; i++)
		*Z++ = ~0;
	    }
	}
      break;
    case EX_ANY:
      memset(Z, 255, bool_bytes);
      break;
    case EX_NONE:
      memset(Z, 0, bool_bytes);
      break;
    default:
      die("Lost in mist and scared of seeing ghost #%d", x->type);
    }
}

static void
construct_bool(struct query *q, struct expr *x)
{
  if (q->n_bool_ids <= 5)
    bool_bytes = sizeof(u32);
  else
    bool_bytes = 1 << (q->n_bool_ids - 3);
  DBG("Constructing boolean expression map of size %d", bool_bytes);
  map_bool(q, x, bool_map_buf);
  q->bool_map = bool_map_buf;

  if (q->n_bool_ids < 5)
    {
      uns mask = (1 << (1 << q->n_bool_ids)) - 1;
      if (!(bool_map_buf[0] & mask))
	goto no_solution;
      if ((bool_map_buf[0] & mask) == mask)
	goto all_matched;
    }
  else
    {
      uns or = 0, and = 0xffffffff;
      for (uns i = 0; i < bool_bytes / 4; i++)
        {
	  or |= bool_map_buf[i];
	  and &= bool_map_buf[i];
	  if (unlikely(or && and != 0xffffffff))
	    break;
	}
      if (!or)
	goto no_solution;
      else if (and == 0xffffffff)
	goto all_matched;
    }

#ifndef CONFIG_ALLOW_ANY
  if (bool_map_buf[0] & 1)
    {
      add_err("-123 Negative queries not allowed");
      eval_err(323);
    }
#endif
  return;

no_solution:
  add_reply(". Boolean check: no solutions");
  eval_err(1);

all_matched: ;
#ifndef CONFIG_ALLOW_ANY
  add_err("-104 All documents match");
  eval_err(304);
#endif
}

/*** Constructing boolean expression for optimistic matching ***/

static u32 *optimistic_bool_map_buf;

static struct expr *
optimistic_expr(struct expr *x)
{
  struct expr *l, *r;

  switch (x->type)
    {
    case EX_WORD:
      return x;
    case EX_PHRASE:
    case EX_NOT:
    case EX_IMAGE_SIM:
      return NULL;
    case EX_ANY:
    case EX_NONE:
      return x;
    case EX_AND:
    case EX_OR:
      l = optimistic_expr(x->u.op.l);
      r = optimistic_expr(x->u.op.r);
      if (x->type == EX_OR && (!l || !r))
	return NULL;
      if (!l)
	return r;
      if (!r)
	return l;
      if (l == x->u.op.l && r == x->u.op.r)
	return x;
      return new_op(x->type, l, r);
    default:
      ASSERT(0);
    }
}

static void
construct_optimistic_bool(struct query *q, struct expr *x)
{
  x = optimistic_expr(x) ? : new_op(EX_ANY, NULL, NULL);
  debug_expr(q, x, 'O');
  map_bool(q, x, optimistic_bool_map_buf);
  q->optimistic_bool_map = optimistic_bool_map_buf;
}

/*** Partial evaluation of expressions for the fulltext matcher ***/

int
simplified_eval_expr(struct expr *x, uns bool_mask)
{
  int l, r;

  /* Returns 1 for true, -1 for false, 0 for "only string types found" */
  switch (x->type)
    {
    case EX_WORD:
      if (x->u.word.w->is_string)
	return 0;
      else if (x->u.word.w->boolean_id < 0)
	return -1;
      else
	return ((bool_mask & (1 << x->u.word.w->boolean_id)) ? 1 : -1);
    case EX_PHRASE:
      return ((bool_mask & (1 << x->u.phrase.p->boolean_id)) ? 1 : -1);
    case EX_IMAGE_SIM:
      return ((bool_mask & (1 << x->u.image_sim->boolean_id)) ? 1 : -1);
    case EX_ANY:
      return 1;
    case EX_NONE:
      return -1;
    case EX_AND:
      l = simplified_eval_expr(x->u.op.l, bool_mask);
      r = simplified_eval_expr(x->u.op.r, bool_mask);
      if (l < 0 || r < 0)
	return -1;
      else if (l > 0 || r > 0)
	return 1;
      else
	return 0;
    case EX_OR:
      l = simplified_eval_expr(x->u.op.l, bool_mask);
      r = simplified_eval_expr(x->u.op.r, bool_mask);
      if (l < 0 && r < 0)
	return -1;
      else if (l > 0 || r > 0)
	return 1;
      else
	return -1;
    case EX_NOT:
      return -simplified_eval_expr(x->u.op.l, bool_mask);
    default:
      ASSERT(0);
    }
}

/*** Display query statistics ***/

static void
show_phrase_stat(struct query *q, struct phrase *p, uns attr)
{
  char *words[2*p->length];
  for (uns j=0; j<p->length; j++)
    {
      int rp = (p->relpos[j] > 1) || (p->prox_map & (1 << j));
      words[2*j] = j ? (" * " + 2 - 2*rp) : "";

      struct word *w = q->words[p->word[j]];
      if (w->root)
	w = w->root;
      words[2*j+1] = w->word;
    }
  add_reply("%c\"%s\" %d", attr, stk_strarraycat(words, 2*p->length), p->matches);
}

#ifdef CONFIG_FILETYPE
static void
ext_ft_show(struct stats *s, void (*add)(char *fmt, ...))
{
  char *t[MAX_FILE_TYPES];
  for (uns i=0; i<MAX_FILE_TYPES; i++)
    t[i] = stk_printf(" %s=%d", custom_file_type_names[i], s->matching_per_type[i]);
  add("t%s", stk_strarraycat(t, MAX_FILE_TYPES)+1);
}
#endif

static void
show_query_stats(struct query *q)
{
  for (uns i=0; i<q->nwords; i++)
    {
      struct word *w = q->words[i];
      if (w->use_count && (w->use_count != w->hide_count || w->doc_count))
	{
	  /* We need to avoid spaces in complexes, so convert them to non-breakable spaces */
	  byte disp[2*strlen(w->word) + 2];
	  byte *pp = w->word;
	  byte *qq = disp;
	  while (*qq++ = *pp++)
	    if (qq[-1] == ' ')
	      {
		qq--;
		qq = utf8_put(qq, 0xa0);
	      }
	  add_reply("W%s %d %d %d %d %d%d%08x%d%d%d%llx", disp, w->var_count, w->ref_total_len / 1024,
		    (q->results->status ? ((w->boolean_id < 0) ? -1 : 0) : (int)w->doc_count),
		    w->status, w->is_string, w->options.accent_mode, w->type_mask,
		    w->options.morphing, w->options.spelling, w->options.synonyming, (long long) w->options.syn_expand);
	}
    }
  for (uns i=0; i<q->nphrases; i++)
    show_phrase_stat(q, q->phrases[i], 'P');
  for (uns i=0; i<q->nnears; i++)
    show_phrase_stat(q, q->nears[i], 'n');

  struct stats *st = &q->stats;
  add_reply("T%d", st->matching_docs);
  EXTENDED_SHOW_STATS(q, st, add_reply);
}

/*** Caching of interim results ***/

#define INTERIMS(x) x(words) x(nwords) x(phrases) x(nphrases) x(nears) x(nnears) x(nimage_sims) x(image_sims)

static struct expr *
copy_expr(struct mempool *pool, struct expr *e)
{
  if (!e)
    return e;

  struct expr *f = mp_alloc(pool, sizeof(*f));
  *f = *e;
  switch (e->type)
    {
    case EX_AND:
    case EX_OR:
    case EX_NOT:
      f->u.op.l = copy_expr(pool, e->u.op.l);
      f->u.op.r = copy_expr(pool, e->u.op.r);
      break;
    case EX_ANY:
    case EX_NONE:
    case EX_WORD:
    case EX_PHRASE:
    case EX_IMAGE_SIM:
      /* The literal copy done above is enough */
      break;
    default:
      ASSERT(0);	/* The other types cannot occur in preprocessed query */
    }
  return f;
}

static void
save_interims(struct query *q, struct results *r, struct expr *e)
{
  struct cached_interims *c = mp_alloc_zero(r->pool, sizeof(*c));

  c->db = q->dbase;
  c->expr = copy_expr(r->pool, e);
#define SAVE_INTERIM(item) c->item = q->item;
  INTERIMS(SAVE_INTERIM);
#undef SAVE_INTERIM
  clist_add_tail(&r->interims, &c->n);
}

int
restore_interims(struct query *q, struct results *r, struct database *db)
{
  current_dbase = db;
  CLIST_FOR_EACH(struct cached_interims *, c, r->interims)
    if (c->db == db)
      {
	q->dbase = db;
	q->prep_expr = c->expr;
#define RESTORE_INTERIM(item) q->item = c->item;
	INTERIMS(RESTORE_INTERIM);
#undef RESTORE_INTERIM
	return 1;
      }
  return 0;
}

/*** Top-level query processing ***/

void
init_stats(struct query *q UNUSED, struct stats *st)
{
  st->matching_docs = 0;
  EXTENDED_INIT_STATS(q, st);
}

void
merge_stats(struct query *q UNUSED, struct stats *f, struct stats *t)
{
  t->matching_docs += f->matching_docs;
  EXTENDED_MERGE_STATS(q, f, t);
}

static int
eval_query_db(struct query *q)
{
  struct expr *e;
  struct results *r = q->results;
  int status;

  if (status = setjmp(query_err_jmp))
    return status;

  db_switch_config(q->dbase);
  q->words = mp_alloc_zero(r->pool, sizeof(struct word *) * max_words);
  q->nwords = 0;
  q->phrases = mp_alloc_zero(r->pool, sizeof(struct phrase *) * max_phrases);
  q->nphrases = 0;
  q->nears = mp_alloc_zero(r->pool, sizeof(struct phrase *) * max_nears);
  q->nnears = 0;
  q->image_sims = mp_alloc_zero(r->pool, sizeof(struct image_sim *) * max_image_sims);
  q->nimage_sims = 0;
  init_stats(q, &q->stats);

  q->age_min = convert_age(r->create_time - MIN(r->create_time,q->age_raw_min), q->dbase->params->ref_time);
  q->age_max = convert_age(r->create_time - MIN(r->create_time,q->age_raw_max), q->dbase->params->ref_time);
  e = analyse_query(q, q->expr);
  if (e->type == EX_IGNORE)
    e->type = EX_ANY;
  debug_expr(q, e, 'A');
  fixup_words(q);
  debug_words(q);
  if (r->max_matches)
    spell_check(q);
  check_words(q);
  save_interims(q, r, e);
  if (r->max_matches)
    {
      check_shortcut(q, e);
      construct_bool(q, e);
      construct_optimistic_bool(q, e);
      process_refs(q);
    }
  return 0;
}

void
eval_err(int code)
{
  longjmp(query_err_jmp, code);
}

static void
eval_query(struct query *q, struct results *r)
{
  uns i, status;
  uns successful_dbs = 0;
  struct reply *first_error = NULL;
  uns first_err_code = 0;

  if (!images_eval(q))
    {
      memory_flush(q);
      return;
    }
  
  r->status = 0;
  q->current_reply_buf = &r->reply_header;
  clist_init(&r->interims);

  query_init_refs(q);
  debug_expr(q, q->expr, 'S');
  if (q->debug & DEBUG_ANALYSE)
    add_reply(".r%d", r->max_matches);

  i = 0;
  CLIST_FOR_EACH(struct database *, db, databases)
    {
      q->dbase = db;
      if (r->status)
	break;
      if (q->db_mask & (1 << i))
        {
	  current_dbase = q->dbase;
	  if (!current_dbase->params)
	    continue;
	  add_reply("(D");
	  add_reply("D%s", current_dbase->name);
	  add_reply("N%d", current_dbase->num_ids);
	  add_reply("I%d", current_dbase->params->objects_in);
	  add_reply("L%d", (int) current_dbase->params->ref_time);
	  add_reply("V%08x", (int) current_dbase->params->ref_time);
	  status = eval_query_db(q);
	  if (status < 100)	/* OK or no solution */
	    successful_dbs++;
	  else if (status >= 300 && q->partial_answers)
	    {
	      /* Partial answer: Move the status message at the end */
	      struct reply *err = first_reply_last(&r->reply_header);
	      if (!first_error)
	        {
		  first_error = err;
		  first_err_code = status - 200;
	        }
	    }
	  else
	    r->status = (status >= 300) ? (status - 200) : status;
	  show_query_stats(q);
	  memory_flush(q);
	  add_reply(")");
        }
      i++;
    }
  if (!successful_dbs && first_error)
    {
      char buf[first_error->len];
      memcpy(buf, first_error->text, first_error->len);
      buf[first_error->len - 1] = 0;
      add_err(buf);
      r->status = first_err_code;
    }
  if (r->status < 100)
    {
      query_finish_refs(q);
      add_err("+000 OK");
    }
  q->current_reply_buf = &q->reply_header;
}

void
query_init(void)
{
  if (max_words > HARD_MAX_WORDS)
    die("MaxWords (%d) > HARD_MAX_WORDS (%d)", max_words, HARD_MAX_WORDS);
  bool_bytes = 1 << (MAX(5, max_bools) - 3);
  bool_map_buf = xmalloc(bool_bytes);
  optimistic_bool_map_buf = xmalloc(bool_bytes);
  incarnation = ((u64)time(NULL) << 32) + getpid();
}

static void
init_query(struct query *q)
{
  q->db_mask = ~0;
  q->debug = global_debug;
  struct options *o = &q->default_options;
  o->accent_mode = global_accent_mode;
  o->weight = word_bonus;
  o->morphing = global_morphing;
  o->spelling = global_spelling;
  o->synonyming = global_synonyming;
  o->syn_expand = (global_syn_expand >= 0x80000000) ? ~0ULL : global_syn_expand;
  o->default_word_types = default_word_types;
  q->context_chars = global_context_chars;
  memcpy(q->meta_chars, global_meta_chars, sizeof(global_meta_chars));
  q->intervals = global_intervals;
  q->site_max = global_site_max;
  q->url_max = global_url_max;
  q->partial_answers = global_partial_answers;
  q->allow_approx = global_allow_approx;
  q->age_raw_max = ~0U;
#define INT_ATTR(id,keywd,gf,pf) q->id##_max = ~0U;
#define SMALL_SET_ATTR(id,keywd,gf,pf) q->id##_set = ~0U;
#define LATE_INT_ATTR INT_ATTR
#define LATE_SMALL_SET_ATTR SMALL_SET_ATTR
  EXTENDED_ATTRS
#undef INT_ATTR
#undef SMALL_SET_ATTR
#undef LATE_INT_ATTR
#undef LATE_SMALL_SET_ATTR
  CUSTOM_MATCH_INIT(q);
  q->custom_sorting = global_sorting;
  q->custom_sort_reverse = global_sort_reverse;
}

static void
tidy_query(struct query *q UNUSED)
{
#ifdef CONFIG_LANG
  q->lang_set |= 1 << LANG_NONE;
#endif
}

void
process_query(struct query *q)
{
  byte *err;
  struct results *r;

  current_query = q;
  init_query(q);
  add_reply("V" SHER_VER);
  add_reply("I%llx", (long long) incarnation);

  profiler_init();
  profiler_switch(&prof_analyse);
  if (!q->iobuf[0])
    {
      add_err("-102 Empty request");
      return;
    }
  q->contains_accents = word_contains_accents(q->iobuf);
  if (err = parse_query(q->iobuf))
    {
      add_err("%s%s", (err[0] == '-' ? "" : "-102 "), err);
      return;
    }
  if (!q->expr)
    {
      do_command(q);
      return;
    }

  tidy_query(q);

  if (check_result_set(q))
    return;
  prefetch_results(q);

  debug_expr(q, q->expr, 'I');
  process_options(q);
  q->results = lookup_cache(q);

  if (r = q->results)
    {
      if (r->status >= 0)
	{
	  q->cache_age = r->access_time - r->create_time;
	  add_reply("C%d", q->cache_age);
	}
      else
	{
	  q->cache_age = -1;
	  eval_query(q, r);
	}
      ship_reply_buf(q, &r->reply_header);
      q->q_status = r->status;
    }
  flush_reply_buf(q, &q->reply_header);
  send_reply_string("\n");
  q->current_reply_buf = &q->reply_footer;
  add_footer("+");
  if (r && !r->status)
    show_results(q);
  profiler_switch(NULL);
  profiler_show(q);
}
