/*
 *	Sherlock Shepherd -- Manual Control -- Commands
 *
 *	(c) 2004--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2006--2007 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/string.h"
#include "gather/gather.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/reap.h"
#include "gather/shepherd/shep-plan.h"
#include "gather/shepherd/protocol.h"
#include "gather/shepherd/man.h"

#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>

static struct fastbuf *output;

/*** Auxiliary functions ***/

#define VERBOSE MAN_VERBOSE

static const char *type_names[] = URL_STATE_TYPE_NAMES;
static const char flag_names[] = URL_STATE_ALL_FLAG_NAMES;

/*** Listing commands ***/

void
list_fp_header(struct fastbuf *output)
{
  bputs(output, "Site footprint   Path footprint  ");
}

void
list_plan_header(struct fastbuf *output)
{
  bputs(output, "PlannPrior");
}

void
list_plan_entry(struct fastbuf *output, struct url_state *s)
{
  struct plan_entry p;
  struct site *site = site_lookup(&s->fp.site);
  if (!site)
    bputs(output, "-no-site--");
  else if (plan_gather_p(s, &p, man_ref_time))
    {
      plan_adjust_bonus(&p, site);
      bprintf(output, "%10u", p.priority);
    }
  else
    bputs(output, "----------");
}

void
list_index_header(struct fastbuf *output)
{
  bputs(output, "Bucket          Age Stb Frq Tim Rtr Wei Tim ");
#ifdef CONFIG_AREAS
  bputs(output, "  Area ");
#endif
  bputs(output, "Flags        Typ");
}

void
list_index_entry(struct fastbuf *output, struct url_state *s)
{
  byte flags[sizeof(flag_names)];
  bprintf(output, "%08x %10d %3d %3d %3d %3d %3d %3d "
#ifdef CONFIG_AREAS
      "%6d "
#endif
      "%s %-3s",
      s->oid,
      man_url_age(s) / man_opt.age_display_unit,
      s->stable_time,
      s->refresh_freq,
      s->download_time,
      s->retry_count,
      s->weight,
      s->section,
#ifdef CONFIG_AREAS
      s->area,
#endif
      str_format_flags(flags, flag_names, ustate_all_flags(s)),
      (ustate_type(s) < ARRAY_SIZE(type_names) ? type_names[ustate_type(s)] : "???"));
}

void
list_site_entry(struct fastbuf *output, struct url_state *s)
{
  struct site *site = site_lookup(&s->fp.site);
  if (site)
    bprintf(output, "%s://%s:%d/", url_proto_names[site->proto], site->hostname, site->port);
  else
    bprintf(output, "???");
}

static void
list_entry(struct url_state *s, byte *url, int matching)
{
  if (!matching)
    return;
  if (!man_opt.show_idx && (ustate_type(s) == UTYPE_SKEY || ustate_type(s) == UTYPE_ZOMBIE))
    return;
  if (man_opt.show_idx)
    {
      bprintf(output, "%08x%08x:%08x%08x", FP_QUAD(s->fp));
      if (man_opt.show_plan)
        bputc(output, ' '), list_plan_entry(output, s);
      if (man_opt.show_idx > 1)
        bputc(output, ' '), list_index_entry(output, s);
      if (man_opt.show_urls || man_opt.show_sites)
	bputc(output, ' ');
    }
  if (man_opt.show_urls)
    bprintf(output, "%s", url);
  else if (man_opt.show_sites)
    list_site_entry(output, s);
  bputc(output, '\n');
}

void
cmd_list(struct selector *selector)
{
  output = bfdopen_shared(1, 65536);
  if (man_opt.show_sites || man_opt.show_plan)
    maybe_load_sites(man_opt.state, man_opt.show_sites);
  if (!man_opt.bare && man_opt.show_idx)
    {
      list_fp_header(output);
      if (man_opt.show_plan)
	bputc(output, ' '), list_plan_header(output);
      if (man_opt.show_idx > 1)
         bputc(output, ' '), list_index_header(output);
      if (man_opt.show_urls)
        bputs(output, " URL");
      else if (man_opt.show_sites)
        bputs(output, " Site");
      bputc(output, '\n');
    }
  sel_index(selector, list_entry, man_opt.show_urls);
  bclose(output);
}

static void
bucket_entry(struct url_state *s, byte *url UNUSED, int matching)
{
  if (!matching || !resolve_object)
    return;
  bprintf(output, "### %08x\n", s->oid);
  obj_write(output, resolve_object, BUCKET_TYPE_PLAIN);
  bputc(output, '\n');
}

void
cmd_buckets(struct selector *selector)
{
  output = bfdopen_shared(1, 65536);
  sel_index(selector, bucket_entry, 2);
  bclose(output);
}

void
cmd_sites(struct selector *selector)
{
  maybe_load_sites(man_opt.state, 0);
  output = bfdopen_shared(1, 65536);
  if (!man_opt.bare)
    bputsn(output, "Site footprint   Norm. footprint  Active Inact. Gathrd Oscill  Fresh SKey     AvT ErrCyc Name");
  for (struct site *s=NULL; s=sel_site_next(selector, s);)
    {
      bprintf(output, "%08x%08x %08x%08x %6d %6d %6d %6d %6d %08x %3d %6d %s://%s:%d/\n",
	      FP_PAIR(s->fp),
	      FP_PAIR(s->norm_fp),
	      s->num_active, s->num_inactive, s->num_gathered, s->num_oscillations, s->num_fresh,
	      s->skey, s->avg_download_time, s->error_cycles,
	      url_proto_names[s->proto], s->hostname, s->port);
    }
  bclose(output);
}

void
cmd_sites_filter(struct selector *selector)
{
  maybe_load_sites(man_opt.state, 0);
  output = bfdopen_shared(1, 65536);
  if (!man_opt.bare)
    bputsn(output, "Site footprint   Active Inact. Gathrd SKey     SoftLim HardLim FrshLim MinD     QBonus SBonus AvT Fil Name");
  for (struct site *s=NULL; s=sel_site_next(selector, s);)
    {
      site_filter(s, NULL);
      bprintf(output, "%08x%08x %6d %6d %6d %08x %7d %7d %7d %4d %10d %6d %3d %s %s://%s:%d/\n",
	      FP_PAIR(s->fp),
	      s->num_active, s->num_inactive, s->num_gathered, s->skey,
	      s->soft_limit, s->hard_limit, s->fresh_limit, s->min_delay, s->queue_bonus, s->select_bonus,
	      s->avg_download_time, (s->flags & SITE_REJECTED) ? "rej" : "acc",
	      url_proto_names[s->proto], s->hostname, s->port);
    }
  bclose(output);
}

/*** Modification commands ***/

static struct fastbuf *set_out;

static void
set_entry(struct url_state *s, byte *url UNUSED, int matching)
{
  if (matching)
    {
      if (man_opt.set_weight >= 0)
	s->weight = man_opt.set_weight;
      if (man_opt.set_freq >= 0)
	s->refresh_freq = man_opt.set_freq;
      if (man_opt.set_section >= 0)
	s->section = man_opt.set_section;
#ifdef CONFIG_AREAS
      if (man_opt.set_area != AREA_ANY)
	s->area = man_opt.set_area;
#endif
      s->flags = (s->flags & ~man_opt.set_flag_mask) | man_opt.set_flag_val;
      /* We silently ignore adding sacred flags to zombies */
      if (ustate_type(s) == UTYPE_ZOMBIE)
	{
	  s->flags &= ~(USF_REGATHER | USF_INIT);
	  ASSERT(!USF_IS_SACRISIMMUS(s->flags));
	}
    }
  bwrite(set_out, s, sizeof(*s));
}

void
cmd_set(struct selector *selector)
{
  set_out = temp_state_file();
  if (sel_index(selector, set_entry, 0))
    put_state_file(man_opt.state, "index", set_out, STATE_FLAG_SORTED);
  else
    bclose(set_out);
}

static void
del_entry(struct url_state *s, byte *url UNUSED, int matching)
{
  if (!matching)
    bwrite(set_out, s, sizeof(*s));
}

void
cmd_delete(struct selector *selector)
{
  set_out = temp_state_file();
  if (sel_index(selector, del_entry, 0))
    put_state_file(man_opt.state, "index", set_out, STATE_FLAG_SORTED);
  else
    bclose(set_out);
}

static uns turn_to_zombie_error;
static uns turn_to_zombie_sacred;

static void
turn_to_zombie_entry(struct url_state *s, byte *url UNUSED, int matching)
{
  if (matching)
    if (!USF_IS_SACRISIMMUS(s->flags))
      {
	ustate_set_type(s, UTYPE_ZOMBIE);
	s->oid = turn_to_zombie_error;
      }
    else
      turn_to_zombie_sacred++;
  bwrite(set_out, s, sizeof(*s));
}

void
cmd_turn_to_zombie(struct selector *selector, uns error_code)
{
  ASSERT(error_code >= 2000 && error_code <= 2999);
  set_out = temp_state_file();
  turn_to_zombie_sacred = 0;
  turn_to_zombie_error = error_code;
  if (sel_index(selector, turn_to_zombie_entry, 0))
    {
      if (turn_to_zombie_sacred)
        log(L_WARN, "%d entries skipped due to incompatible type", turn_to_zombie_sacred);
      put_state_file(man_opt.state, "index", set_out, STATE_FLAG_SORTED);
    }
  else
    bclose(set_out);
}

/*** Insertion commands ***/

void
cmd_insert(struct selector *selector UNUSED)
{
  /* All real work is done in the selector routines */
}

static void
insert_refs_entry(struct url_state *s, byte *url UNUSED, int matching)
{
  if (!matching || !resolve_object)
    return;

  write_contribs_obj(resolve_object, 0, s->weight, s->area);
}

void
cmd_insert_refs(struct selector *selector)
{
  contrib_init(man_opt.state);
  sel_index(selector, insert_refs_entry, 2);
  contrib_cleanup();
}

/*** Histograms ***/

struct hist_box {
  uns cnt;
  uns scaled_cnt;
  uns prio_cnt;
};

static struct hist_box *histogram;
static uns hist_per_flag[sizeof(flag_names)-1];
static uns hist_per_type[UTYPE_MAX];

static void
hist_entry(struct url_state *s, byte *url UNUSED, int matching)
{
  if (!matching)
    return;

  uns type = ustate_type(s);
  hist_per_type[type]++;
  uns flags = ustate_all_flags(s);
  for (uns i=0; i<sizeof(flag_names)-1; i++)
    if (flags & (1 << i))
      hist_per_flag[i]++;
  if (type != UTYPE_SLEEPING && type != UTYPE_SKEY && type != UTYPE_ZOMBIE)
    {
      uns age = man_raw_url_age(s);
      uns box = MIN(age / man_opt.hist_box_width, man_opt.hist_num_boxes);
      struct hist_box *b = &histogram[box];
      if (s->flags & USF_REGATHER)
	b->prio_cnt++;
      if (type != UTYPE_NEW)
	{
	  b->cnt++;
	  uns age2 = age * (s->refresh_freq ? : 1);
	  uns box2 = MIN(age2 / man_opt.hist_box_width, man_opt.hist_num_boxes);
	  histogram[box2].scaled_cnt++;
	}
    }
}

void
cmd_histogram(struct selector *selector)
{
  histogram = xmalloc_zero(sizeof(struct hist_box) * (man_opt.hist_num_boxes+1));
  sel_index(selector, hist_entry, 0);

  printf(" Box:    Normal    Scaled  Regather\n");
  struct hist_box bb = { .cnt = 0, .scaled_cnt = 0, .prio_cnt = 0 };
  for (uns i=0; i<=man_opt.hist_num_boxes; i++)
    {
      struct hist_box *b = &histogram[i];
      if (i >= man_opt.hist_num_boxes)
	printf("Over");
      else
	printf("%4d", i);
      printf(": %9d %9d %9d\n", b->cnt, b->scaled_cnt, b->prio_cnt);
      bb.cnt += b->cnt;
      bb.scaled_cnt += b->scaled_cnt;
      bb.prio_cnt += b->prio_cnt;
    }
  printf(" All: %9d %9d %9d\n\n", bb.cnt, bb.scaled_cnt, bb.prio_cnt);

  printf("Type:");
  for (uns i=0; i<UTYPE_MAX; i++)
    printf(" %9s", ((char *[]) URL_STATE_TYPE_NAMES)[i]);
  printf("\n Cnt:");
  uns total = 0;
  for (uns i=0; i<UTYPE_MAX; i++)
    {
      printf(" %9d", hist_per_type[i]);
      total += hist_per_type[i];
    }

  printf("\n\nFlag:");
  for (uns i=0; flag_names[i]; i++)
    if (flag_names[i] != '-')
      printf(" %9c", flag_names[i]);
  printf("\n Cnt:");
  for (uns i=0; flag_names[i]; i++)
    if (flag_names[i] != '-')
      printf(" %9d", hist_per_flag[i]);

  printf("\n\nGrand total: %d\n", total);
}

/*** Per-qkey statistics ***/

struct qkey_stats {
  u64 qkey;
  uns num_sites;
  uns num_active, num_inactive;
  uns min_delay, max_conn;
  uns cnt_refresh, cnt_regather, cnt_over, cnt_retry, cnt_new;
  uns schema;
  struct site *any_site;
};

#define HASH_NODE struct qkey_stats
#define HASH_PREFIX(x) qkey_hash_##x
#define HASH_KEY_ATOMIC qkey
#define HASH_ATOMIC_TYPE u64
#define HASH_WANT_LOOKUP
#define HASH_ZERO_FILL
#define HASH_USE_POOL man_pool
#include "ucw/hashtable.h"

static void
qkey_rfsh_entry(struct url_state *s, byte *url UNUSED, int matching)
{
  uns type = ustate_type(s);
  if (!matching ||
      type == UTYPE_SKEY ||
      type == UTYPE_SLEEPING ||
      type == UTYPE_ZOMBIE)
    return;

  struct site *site = site_lookup(&s->fp.site);
  if (!site)
    {
      log(L_ERROR, "Site %08x%08x not found in site list", FP_PAIR(s->fp.site));
      return;
    }
  struct qkey_stats *q = site->u.ctrl.qkey_stats;
  if (!q)
    return;

  uns age = man_url_age(s);
  if (type == UTYPE_NEW)
    q->cnt_new++;
  else if (s->flags & USF_REGATHER)
    q->cnt_regather++;
  else if (age >= 2*refresh_cycle)
    q->cnt_over++;
  else if (age >= refresh_cycle)
    q->cnt_refresh++;
  if (s->retry_count)
    q->cnt_retry++;
}

void
cmd_qkey_stats(struct selector *selector, uns refresh)
{
  maybe_load_sites(man_opt.state, 1);
  qkey_hash_init();

  for (struct site *s=NULL; s=site_next(s);)
    s->u.ctrl.qkey_stats = NULL;
  for (struct site *s=NULL; s=sel_site_next(selector, s);)
    {
      uns sk = s->skey & SKEY_TYPE_MASK;
      u64 qkey;
      if (sk == SKEY_UNRESOLVED || sk == SKEY_NONEXISTENT || sk == SKEY_NONIP)
	qkey = sk;
      else
	qkey = site_qkey(s);
      struct qkey_stats *q = qkey_hash_lookup(qkey);
      s->u.ctrl.qkey_stats = q;
      q->num_sites++;
      q->num_active += s->num_active;
      q->num_inactive += s->num_inactive;
      q->min_delay = MAX(q->min_delay, s->min_delay);
      if (!q->max_conn || q->max_conn > s->max_conn)
	q->max_conn = s->max_conn;
      if (!q->any_site)
	q->any_site = s;
      if (q->schema < s->refresh_schema && refresh_schemas[s->refresh_schema])
	q->schema = s->refresh_schema;
    }
  if (refresh)
    sel_index(selector, qkey_rfsh_entry, 0);

  output = bfdopen_shared(1, 65536);
  if (!man_opt.bare)
    {
      bputs(output, "Qkey                  Sites    Active  Inactive Delay SoftLimit HardLimit");
      if (refresh)
	bputs(output, "       New  NormRfsh  LateRfsh    Regath     Retry");
      if (man_opt.show_sites)
	bputs(output, " SomeSite");
      bputc(output, '\n');
    }
  HASH_FOR_ALL(qkey_hash, q)
    {
      /* XXX: These rules should be the same as in shep-select. */
      uns soft_limit = q->min_delay ? double_to_uns(refresh_cycle * duty_factor * q->max_conn / q->min_delay) : ~0U;
      uns hard_limit = double_to_uns(soft_limit * hard_limit_factor);
      uns frequent = refresh_schemas[q->schema] ? double_to_uns(soft_limit * refresh_schemas[q->schema]->frequent_factor) : 0;
      soft_limit -= frequent;
      hard_limit -= frequent;
      bprintf(output, "%04x:%08x %9d %9d %9d %5d %9d %9d",
	      QK_PAIR(q->qkey), q->num_sites, q->num_active, q->num_inactive,
	      q->min_delay, soft_limit, hard_limit);
      if (refresh)
	bprintf(output, " %9d %9d %9d %9d %9d",
		q->cnt_new, q->cnt_refresh, q->cnt_over, q->cnt_regather, q->cnt_retry);
      if (man_opt.show_sites)
	bprintf(output, " %s://%s:%d/", url_proto_names[q->any_site->proto], q->any_site->hostname, q->any_site->port);
      bputc(output, '\n');
    }
  HASH_END_FOR;
  bclose(output);
}

/*** Remote commands ***/

void
cmd_remote_set(uns command, uns arg)
{
  shepp_connect(man_opt.server);
  struct shepp_packet_hdr rq, rp;
  u32 data = arg;
  shepp_send_raw(&rq, command, NULL, &data, sizeof(data));
  shepp_recv(&rp, &rq);
  if (rp.type != SHEPP_REPLY_OK)
    die("Operation failed: got reply %08x", rp.type);
  log(L_INFO, "Option set.");
}

void
cmd_borrow(uns command, byte *phase)
{
  shepp_connect(man_opt.server);
  if (command == SHEPP_REQ_RETURN_STATE)
    {
      byte *ctrl = mp_strcat(man_pool, state_dir, "/current/control");
      if (!phase)
	phase = "closed";
      struct fastbuf *f = bopen(ctrl, O_WRONLY | O_CREAT | O_TRUNC, 1024);
      bputsn(f, phase);
      bclose(f);
      log(L_INFO, "Set phase to %s", phase);
    }
  struct shepp_packet_hdr rq, rp;
  shepp_send_none(&rq, command, NULL);
  shepp_recv(&rp, &rq);
  switch (rp.type)
    {
    case SHEPP_REPLY_OK:
      switch (command)
	{
	case SHEPP_REQ_BORROW_STATE:
	case SHEPP_REQ_BORROW_STATE_Q:
	  log(L_INFO, "State borrowed");
	  break;
	case SHEPP_REQ_RETURN_STATE:
	  log(L_INFO, "State returned.");
	  break;
	case SHEPP_REQ_ROLLBACK_STATE:
	  log(L_INFO, "State rolled back.");
	  break;
	default:
	  ASSERT(0);
	}
      break;
    case SHEPP_REPLY_IN_PROGRESS:
      die("Operation already in progress");
    case SHEPP_REPLY_NO_BORROWED:
      die("There is no borrowed state known to the server");
    case SHEPP_REPLY_RETURNING_BAD:
      die("The state has been refused by the server, see the log");
    default:
      die("Operation failed: got reply %08x", rp.type);
    }
}

void
cmd_remote_unlock(void)
{
  shepp_connect(man_opt.server);
  struct shepp_packet_hdr rq, rp;
  shepp_send_none(&rq, SHEPP_REQ_UNLOCK_STATES, NULL);
  shepp_recv(&rp, &rq);
  switch (rp.type)
    {
    case SHEPP_REPLY_OK:
      log(L_INFO, "Shepherd server unlocked");
      break;
    default:
      die("Unlocking failed: got reply %08x", rp.type);
    }
}

/*** Area commands ***/

#ifdef CONFIG_AREAS

void
cmd_areas(struct selector *selector)
{
  area_t id;
  output = bfdopen_shared(1, 65536);
  if (!man_opt.bare)
    bputsn(output, "  Area   Soft   Hard Active  Inact Gathrd PlanMx Plannd");
  while ((id = area_next_id(selector)) != AREA_ANY)
    {
      struct area_info *a = area_lookup(id, -1);
      if (a)
	bprintf(output, "%6d %6d %6d %6d %6d %6d %6d %6d\n", id, a->soft_limit, a->hard_limit,
		a->num_active, a->num_inactive, a->num_gathered, a->plan_limit, a->num_planned);
      else
	bprintf(output, "%6d ---\n", id);
    }
  bclose(output);
}

void
cmd_set_area_params(struct selector *selector)
{
  area_t id;
  while ((id = area_next_id(selector)) != AREA_ANY)
    {
      struct area_info *a = area_lookup(id, 1);
      if (man_opt.set_area_soft >= 0)
	a->soft_limit = man_opt.set_area_soft;
      if (man_opt.set_area_hard >= 0)
	a->hard_limit = man_opt.set_area_hard;
      if (man_opt.set_area_plan >= 0)
	a->plan_limit = man_opt.set_area_plan;
    }
}

#endif

/*** Replacing of initial URL set ***/

static uns cnt_contribs;

static void
cmd_replace_callback(struct sel_hash_entry *e)
{
  if (!e->seen)
    {
      byte *m = add_contrib(e->url, NULL, 0, default_insert_weight, USF_INIT, -1, AREA_ANY, AREA_ANY);
      if (man_opt.verbose > 1 && m)
        log(L_INFO, "%s: %s", e->url, m);
      cnt_contribs++;
    }
}

void
cmd_replace_init(struct selector *selector)
{
  log(L_INFO, "Loaded initial URL set with %d entries", sel_hash_count(selector));
  struct fastbuf *in = read_state_file(man_opt.state, "index");
  struct fastbuf *out = temp_state_file();
  struct url_state s;
  uns cnt_add = 0, cnt_rem = 0, cnt_zom = 0;
  while (breadb(in, &s, sizeof(s)))
    {
      switch (ustate_type(&s))
        {
	  case UTYPE_SKEY:
	    break;
	  case UTYPE_ZOMBIE:
	    if (sel_hash_find(selector, s.fp))
	      {
		cnt_zom++;
		continue;
	      }
	    break;
	  default: ;
	    struct sel_hash_entry *e = sel_hash_find(selector, s.fp);
	    if (e)
	      {
		e->seen = 1;
		if (!(s.flags & USF_INIT))
		  cnt_add++;
		s.flags |= USF_INIT;
	      }
	    else
	      {
		if (s.flags & USF_INIT)
		  cnt_rem++;
		s.flags &= ~USF_INIT;
	      }
	}
      bwrite(out, &s, sizeof(s));
    }
  bclose(in);
  put_state_file(man_opt.state, "index", out, STATE_FLAG_SORTED);
  log(L_INFO, "Initial set gained %d, lost %d entries, exorcised %d zombies", cnt_add, cnt_rem, cnt_zom);
  contrib_init(man_opt.state);
  sel_hash(selector, cmd_replace_callback);
  contrib_cleanup();
  log(L_INFO, "Inserted %d new URL's", cnt_contribs);
}

static void
dump_entry(struct selector *selector)
{
  uns bool_mask = selector->bool_mask;
  if (!(bool_mask & SEL_MIXED_MATCHED))
    return;
  uns i = 0;
  CLIST_FOR_EACH(struct sel_src *, src, selector->lsrc)
    if (src->dump && (bool_mask & src->bool_mask))
      {
	if (!i++)
	  {
	    bprintf(output, "FP=%08x%08x:%08x%08x", FP_QUAD(*(struct footprint *)src->record));
	    if (selector->url)
	      bprintf(output, " URL=%s", selector->url);
	    bputc(output, '\n');
	  }
	do
	  src->dump(src, output);
	while (src->find_next(src) && !fp_cmp(src->record, &selector->fp));
      }
  bputc(output, '\n');
}

void
cmd_dump(struct selector *selector)
{
  ASSERT(selector->result_type == ST_MIXED);
  output = bfdopen_shared(1, 65536);
  sel_mixed(selector, dump_entry);
  bclose(output);
}
