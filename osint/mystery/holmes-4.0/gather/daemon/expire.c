/*
 *	Sherlock Gatherer Daemon -- The Expirer
 *
 *	(c) 1997--2003 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/url.h"
#include "sherlock/db.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/hashfunc.h"
#include "ucw/mempool.h"
#include "sherlock/object.h"
#include "gather/daemon/gatherd.h"

#include <stdio.h>
#include <time.h>
#include <setjmp.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <alloca.h>

/*** Globals ***/

ucw_time_t now, cycle_start, cycle_last;

static uns do_not_expire;
static uns one_pass;

/*** Configuration ***/

static int min_revalidate_age = 0x7fffffff;
static uns revalidate_cycle;
static int queue_discard_age = 0x7fffffff;
static int error_discard_age = 0x7fffffff;
static int robot_expire_age = 0x7fffffff;
static char *tmp_queue_name = "db/queue.tmp";
static uns qk_expire_age = ~0U;
static char *stamp_name = "db/expire-stamp";
static int trace;
static int queue_postpone = 0x7fffffff;
static uns queue_bonus_refresh;
static uns queue_bonus_regather;
static uns queue_penalty_retry;
static uns hist_num_boxes = 0;
static uns hist_box_width = 86400;

static struct cf_section expire_config = {
  CF_ITEMS {
    CF_INT("Trace", &trace),
    CF_INT("MinRevalidateAge", &min_revalidate_age),
    CF_UNS("RevalidateCycle", &revalidate_cycle),
    CF_INT("QueueDiscardAge", &queue_discard_age),
    CF_INT("ErrorDiscardAge", &error_discard_age),
    CF_STRING("TmpQueueFile", &tmp_queue_name),
    CF_UNS("QKeyExpireAge", &qk_expire_age),
    CF_STRING("StampFile", &stamp_name),
    CF_INT("RobotExpireAge", &robot_expire_age),
    CF_INT("QueuePostpone", &queue_postpone),
    CF_UNS("QueueBonusRefresh", &queue_bonus_refresh),
    CF_UNS("QueueBonusRegather", &queue_bonus_regather),
    CF_UNS("QueuePenaltyRetry", &queue_penalty_retry),
    CF_UNS("HistNumBoxes", &hist_num_boxes),
    CF_UNS("HistBoxWidth", &hist_box_width),
    CF_END
  }
};

#define TRACE(msg) do { if (trace) log(L_DEBUG, msg); } while(0)

static void
stamp_read(void)
{
  FILE *f;

  if (f = fopen(stamp_name, "r"))
    {
      if (fscanf(f, "%d%d", &cycle_start, &cycle_last) != 2)
	log(L_ERROR, "%s: syntax error, ignoring the stamp", stamp_name);
      fclose(f);
    }
  if (!cycle_last)
    {
      log(L_INFO, "Restarting expiration cycle");
      cycle_start = cycle_last = now;
    }
}

static void
stamp_write(void)
{
  FILE *f;

  if (!(f = fopen(stamp_name, "w")))
    die("fopen(%s): %m", stamp_name);
  fprintf(f, "%d %d\n", (int) cycle_start, (int) now);
  fclose(f);
}

/*** Here Comes The Judge ***/

static struct rfilter_data rfdata;
static int j_low, j_high;

#define DENOM 4096			/* Common denominator of all our fractions */

enum verdict {
  JUDGE_FILTER_OUT,
  JUDGE_EXPIRE,
  JUDGE_REFRESH,
  JUDGE_ASIS,
  JUDGE_MAX
};

static int
judge_hash(byte *name)
{
  return hash_string(name) & (DENOM-1);
}

static int
judge_filter(byte *url)
{
  byte *m;

  rfdata.url = url;
  m = ref_filter(&rfdata);
  if (m)
    {
      if (trace > 1)
	log(L_DEBUG, "%s: %s", url, m);
      return JUDGE_FILTER_OUT;
    }
  return JUDGE_ASIS;
}

static void
judge_init(void)
{
  if (now - cycle_last >= revalidate_cycle)
    {
      j_low = 0;
      j_high = DENOM;
    }
  else
    {
      j_low = (int)((float)(cycle_last - cycle_start) * DENOM / revalidate_cycle) % DENOM;
      j_high = (int)((float)(now - cycle_start) * DENOM / revalidate_cycle) % DENOM;
    }
  log(L_DEBUG, "Revalidation cycle: %d--%d of %d", j_low, j_high, DENOM);
}

static int
judge(byte *url, struct urlrec *ur)
{
  int age, verdict;
  byte *m = "OK";
  static int warned_time_skew;

  verdict = judge_filter(url);
  if (verdict != JUDGE_ASIS)
    return verdict;
  age = now - ur->access;
  if (age < 0)
    {
      if (!warned_time_skew++)
	log(L_ERROR, "Clock skew detected");
      age = 0;
    }
  if (do_not_expire)
    m = "not expired";
  else if (ur->flags & URF_QUEUED)
    {
      if (age > queue_discard_age)
	{
	  m = "expiring from queue";
	  verdict = JUDGE_EXPIRE;
	}
      else
	m = "already queued";
    }
  else if (ur->oid >= OID_FIRST_ERROR)
    {
      if (age > error_discard_age)
	{
	  m = "expiring error";
	  verdict = JUDGE_EXPIRE;
	}
      else
	m = "error marker";
    }
  else if (age > min_revalidate_age)
    {
      int j = judge_hash(url);
      if ((j_low <= j_high) ?
	  (j >= j_low && j < j_high) :
	  (j >= j_low || j < j_high))
	{
	  m = "revalidate";
	  verdict = JUDGE_REFRESH;
	}
      else
	m = "waiting for revalidation";
    }
  if (trace > 1)
    log(L_DEBUG, "%s: %s", url, m);
  return verdict;
}

/* The ID Array */

struct id {
  oid_t oid;				/* Original OID */
  oid_t new;				/* New OID after renumbering (0=not seen, OID_UNDEFINED if deleted) */
};

#define ID_ARRAY_SIZE 65536

struct id_array {
  struct id_array *next;
  uns count;
  struct id id[ID_ARRAY_SIZE];
};

static struct id_array **id_arrays, *id_first_arr, *id_this_arr;
static uns id_num_arrays, id_count;

#define ID(i) (id_arrays[(i)/ID_ARRAY_SIZE]->id[(i)%ID_ARRAY_SIZE])

static void
ids_init(void)
{
  id_num_arrays = 1;
  id_first_arr = id_this_arr = xmalloc(sizeof(struct id_array));
  id_this_arr->next = NULL;
  id_this_arr->count = 0;
}

static inline void
ids_add(oid_t oid, oid_t new)
{
  struct id *id;

  if (id_this_arr->count >= ID_ARRAY_SIZE)
    {
      id_this_arr = id_this_arr->next = xmalloc(sizeof(struct id_array));
      id_this_arr->next = NULL;
      id_this_arr->count = 0;
      id_num_arrays++;
    }
  id = &id_this_arr->id[id_this_arr->count++];
  id->oid = oid;
  id->new = new;
}

static void
ids_close(void)
{
  uns i = 0;

  id_arrays = xmalloc(sizeof(struct id_array *) * id_num_arrays);
  while (id_first_arr)
    {
      id_arrays[i++] = id_first_arr;
      id_count += id_first_arr->count;
      id_first_arr = id_first_arr->next;
    }
  DBG("Found %d object ID's (internally: %d blocks)", id_count, id_num_arrays);
}

#define ASORT_PREFIX(x) ids_##x
#define ASORT_KEY_TYPE oid_t
#define ASORT_ELT(i) ID(i).oid
#define ASORT_SWAP(i,j) do { struct id tmp = ID(i); ID(i)=ID(j); ID(j)=tmp; } while(0)
#include "ucw/sorter/array-simple.h"

static int
ids_lookup(oid_t oid)
{
  int l = 0;
  int r = id_count-1;
  int m;

  while (l <= r)
    {
      m = (l+r)/2;
      if (ID(m).oid == oid)
	return m;
      if (ID(m).oid < oid)
	l = m+1;
      else
	r = m-1;
    }
  die("ID %08x lost", oid);
}

static void
ids_debug(byte *m)
{
  uns i;
  if (trace < 3)
    return;
  log(L_DEBUG, "%s", m);
  for (i=0; i<id_count; i++)
    {
      ASSERT(!i || ID(i).oid > ID(i-1).oid);
      fprintf(stderr, "\t%08x %08x\n", ID(i).oid, ID(i).new);
    }
}

static inline oid_t
oid_translate(oid_t oid)
{
  if (!one_pass && oid < OID_FIRST_ERROR)
    {
      int i = ids_lookup(oid);
      oid = ID(i).new;
    }
  return oid;
}

static inline void
oid_delete(oid_t oid)
{
  if (one_pass)
    {
      if (oid < OID_FIRST_ERROR)
	ids_add(oid, OID_UNDEFINED);
    }
  else
    ASSERT(oid >= OID_FIRST_ERROR);
}

/*** Initial scan of URL database ***/

static void
scan_urls(void)
{
  byte url[MAX_URL_SIZE];
  struct urlrec ur;
  uns total=0, del=0;

  TRACE("Prescanning URL database");
  urldb_open();
  urldb_rewind();
  while (urldb_get_next(url, &ur))
    {
      if (ur.oid < OID_FIRST_ERROR)
	{
	  int verdict = judge(url, &ur);
	  total++;
	  if (verdict == JUDGE_REFRESH || verdict == JUDGE_ASIS)
	    ids_add(ur.oid, 0);
	  else
	    {
	      del++;
	      ids_add(ur.oid, OID_UNDEFINED);
	    }
	}
    }
  urldb_close();
  log(L_INFO, "URL prescan: total %d buckets, %d to delete", total, del);
}

/*** Expiration of robot records ***/

static uns rob_total, rob_del, rob_filter;

static int
judge_robot(struct qhost *h)
{
  struct url ur;
  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];
  int e, age;
  byte *m = "OK";

  if (h->robot_id == OID_UNDEFINED)
    return JUDGE_ASIS;
  ur.protocol = url_proto_names[h->protocol];
  ur.protoid = h->protocol;
  ur.port = h->port;
  ur.user = NULL;
  ur.pass = NULL;
  ur.host = h->name;
  ur.rest = "/robots.txt";
  if ((e = url_pack(&ur, buf1)) || (e = url_enescape(buf1, buf2)))
    {
      log(L_ERROR, "Error creating robots.txt URL: %s", url_error(e));
      return JUDGE_FILTER_OUT;
    }
  judge_filter(buf2);			/* We deliberately ignore the verdict and just let the filters alter configuration */
  e = JUDGE_ASIS;
  age = now - h->robot_time;
  if (do_not_expire)
    m = "not expired";
  else if (age > robot_expire_age)
    {
      e = JUDGE_EXPIRE;
      m = "aged out";
      h->rec_err_count = 0;
    }
  if (trace > 1)
    log(L_DEBUG, "%s: %s", buf2, m);
  return e;
}

static void
scan_robots_host(struct qhost *h)
{
  if (!h->protocol)			/* Free list entry */
    return;
  if (h->robot_id < OID_FIRST_ERROR)
    {
      rob_total++;
      int e = judge_robot(h);
      if (e == JUDGE_REFRESH || e == JUDGE_ASIS)
	ids_add(h->robot_id, 0);
      else
	{
	  rob_del++;
	  ids_add(h->robot_id, OID_UNDEFINED);
	}
    }
}

static void
scan_robots(void)
{
  TRACE("Prescanning robot records");
  walk_hosts(scan_robots_host);
  log(L_INFO, "Robot prescan: total %d buckets, %d to delete", rob_total, rob_del);
}

static void
chew_robots_host(struct qhost *h)
{
  int verdict;

  if (!h->protocol)			/* Free list entry */
    return;
  if (h->robot_id == OID_UNDEFINED)	/* No robots.txt */
    return;
  verdict = judge_robot(h);
  h->robot_id = oid_translate(h->robot_id);
  rob_total++;
  if (verdict == JUDGE_FILTER_OUT || verdict == JUDGE_EXPIRE)
    {
      if (h->robot_id != OID_UNDEFINED)	/* Might be if the bucket has been lost */
	oid_delete(h->robot_id);
      h->robot_id = OID_UNDEFINED;
      h->robot_time = now;
      if (verdict != JUDGE_FILTER_OUT)
	rob_del++;
      else
	rob_filter++;
    }
}

static void
chew_robots(void)
{
  TRACE("Expiring robot records");
  rob_total = rob_del = rob_filter = 0;
  walk_hosts(chew_robots_host);
  log(L_INFO, "Robots: %d total, %d expired, %d filtered out", rob_total, rob_del, rob_filter);
}

/*** Expiration of queue keys ***/

static int qk_low, qk_high;
static uns qk_seen, qk_zero, qk_expired;

static void
chew_qkeys_host(struct qhost *h)
{
  int q;

  qk_seen++;
  if (!h->qkey)
    {
      qk_zero++;
      return;
    }
  q = judge_hash(h->name);
  if ((qk_low <= qk_high) ?
      (q >= qk_low && q < qk_high) :
      (q >= qk_low || q < qk_high))
    {
      qk_expired++;
      h->qkey = 0;
    }
}

static void
chew_qkeys(void)
{
  if (qk_expire_age == ~0U || do_not_expire)
    return;
  TRACE("Expiring queue keys");
  if (now - cycle_last >= qk_expire_age)
    {
      qk_low = 0;
      qk_high = DENOM;
    }
  else
    {
      qk_low = (int)((float)(cycle_last - cycle_start) * DENOM / qk_expire_age) % DENOM;
      qk_high = (int)((float)(now - cycle_start) * DENOM / qk_expire_age) % DENOM;
    }
  DBG("Expiring %d--%d/%d", qk_low, qk_high, DENOM);
  walk_hosts(chew_qkeys_host);
  log(L_INFO, "Queue keys: %d total with %d unset, %d expired [%d--%d of %d]",
      qk_seen, qk_zero, qk_expired, qk_low, qk_high, DENOM);
}

/*** Shaking down buckets and reconstructing MD5 database ***/

static uns shake_rover;
static uns shake_total, shake_deleted_old, shake_deleted_new, shake_errors;
static uns md5_sums, md5_dups;

static struct buck2obj_buf *read_buf;
static struct mempool *pool;

static inline uns
shake_md5(oid_t new, uns type, byte *buck, uns len)
{
  struct fastbuf fb;
  fbbuf_init_read(&fb, buck, len, 0);
  mp_flush(pool);
  struct odes *o = obj_read_bucket(read_buf, pool, type, len, &fb, NULL, 1);
  if (unlikely(!o))
    {
      log(L_ERROR, "Cannot parse object %08x, deleting", new);
      return 0;
    }
  byte *C = obj_find_aval(o, 'C');
  if (C)
    {
      if (md5db_exists(C))
	md5_dups++;
      else
	{
	  md5db_store(C, new);
	  md5_sums++;
	}
    }
  return 1;
}

static int
shake_kibitz(struct obuck_header *old, oid_t new, byte *buck)
{
  shake_total++;
  if (new == OBUCK_OID_DELETED)
    {
      shake_deleted_old++;
      return 0;
    }
  while (shake_rover < id_count && ID(shake_rover).oid < old->oid)
    {
      log(L_ERROR, "Bucket %08x missing", ID(shake_rover).oid);
      ID(shake_rover).new = OID_UNDEFINED;
      shake_rover++;
      shake_errors++;
    }
  if (shake_rover >= id_count || ID(shake_rover).oid > old->oid)
    {
      log(L_ERROR, "Bucket %08x unreferenced, deleting it", old->oid);
      shake_errors++;
      return 0;
    }
  if (ID(shake_rover).new == OID_UNDEFINED)
    {
      shake_rover++;
      shake_deleted_new++;
      return 0;
    }
  else
    {
      ID(shake_rover).new = new;
      shake_rover++;
      return shake_md5(new, old->type, buck, old->length);
    }
}

static void
shake(void)
{
  TRACE("Rebuilding MD5 database");
  if (unlink(md5db_name) < 0 && errno != ENOENT)
    die("Cannot unlink old MD5 database: %m");
  md5db_open();

  TRACE("Shaking down buckets");
  read_buf = buck2obj_alloc();
  pool = mp_new(1<<16);
  obuck_shakedown(&bucket_file, shake_kibitz);
  mp_delete(pool);
  buck2obj_free(read_buf);

  while (shake_rover < id_count)
    {
      log(L_ERROR, "Bucket %08x missing", ID(shake_rover).oid);
      ID(shake_rover).new = OID_UNDEFINED;
      shake_rover++;
      shake_errors++;
    }

  log(L_INFO, "Buckets: %d total, %d old + %d new deleted, %d bad",
      shake_total, shake_deleted_old, shake_deleted_new, shake_errors);
  log(L_INFO, "MD5: %d sums + %d duplicates", md5_sums, md5_dups);
  md5db_close();
}

/*** Quick deletion without bucket file shakedown ***/

static void
quick_delete(void)
{
  uns i;
  uns count=0;

  TRACE("Deleting unused buckets");
  for (i=0; i<id_count; i++)
    {
      struct id *id = &ID(i);
      if (id->new == OID_UNDEFINED)
	{
	  DBG("Deleting %08x", id->oid);
	  obuck_delete(&bucket_file, id->oid);
	  count++;
	}
      else
	id->new = id->oid;
    }
  log(L_INFO, "Deleted %d buckets", count);
}

/*** Remaking the queue ***/

#define SORT_PREFIX(x) qs_##x
#define SORT_KEY struct qs_key
#define SORT_DATA_SIZE(k) ((k).len+8)
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB

struct qs_key {
  u32 weight;
  uintptr_t host;
  uns len;
};

static inline int
qs_compare(struct qs_key *a, struct qs_key *b)
{
  COMPARE(a->host, b->host);
  REV_COMPARE(a->weight, b->weight);
  return 0;
}

static inline int
qs_read_key(struct fastbuf *f, struct qs_key *k)
{
  k->weight = bgetl(f);
  k->host = bgeta(f);
  k->len = bgetw(f);
  return (k->weight != 0xffffffff);
}

static inline void
qs_write_key(struct fastbuf *f, struct qs_key *k)
{
  bputl(f, k->weight);
  bputa(f, k->host);
  bputw(f, k->len);
}

#include "ucw/sorter/sorter.h"

#define QL_MAGIC 0x430121dd

static void
rebuild_queue(struct fastbuf *b)
{
  u32 weight, maxobj, section;
  uns len, count=0, inactive=0, postponed=0;
  struct qhost *h;
  byte name[MAX_URL_SIZE];

  TRACE("Sorting queue items");
  bflush(b);
  bsetpos(b, 0);
  bconfig(b, BCONFIG_IS_TEMP_FILE, 1);
  b = qs_sort(b, NULL);

  TRACE("Rebuilding queue");
  while ((weight = bgetl(b)) != 0xffffffff)
    {
      h = (struct qhost *) bgeta(b);
      len = bgetw(b);
      ASSERT(len && len < MAX_URL_SIZE);
      breadb(b, name, len);
      name[len] = 0;
      section = bgetl(b);
      maxobj = bgetl(b);
      DBG("Q: h=%s n=%s w=%d s=%d m=%d", h->name, name, weight, section, maxobj);
      if (section != ~0U && ++h->obj_count[section] > maxobj)
	inactive++;
      else if (++h->ex_qh_qcount > (uns) queue_postpone)
	postponed++;
      else
	{
	  enqueue_item(h, name, weight);
	  count++;
	}
    }
  bclose(b);
  log(L_INFO, "Queue: %d items, %d inactive, %d postponed", count, inactive, postponed);
}

/*** Chewing the URL database ***/

enum url_class {
  UC_NEW,				/* queued new */
  UC_REFRESH,				/* queued refresh */
  UC_PRIORITY,				/* queued priority gathering (new or refresh) */
  UC_ERROR,				/* error marker */
  UC_STATIC,				/* static document */
  UC_MAX
};

static uns url_stats[UC_MAX][JUDGE_MAX];
static uns url_in[UC_MAX], url_out[UC_MAX], url_verdict[JUDGE_MAX], url_lost;

static inline uns
classify_ur(struct urlrec *ur)
{
  if (ur->flags & URF_QUEUED)
    {
      if (ur->flags & URF_REGATHER)
	return UC_PRIORITY;
      if (ur->oid == OID_UNDEFINED)
	return UC_NEW;
      else
	return UC_REFRESH;
    }
  else if (ur->oid >= OID_FIRST_ERROR)
    return UC_ERROR;
  else
    return UC_STATIC;
}

static void
chew_urls(void)
{
  struct sdbm_options so;
  struct sdbm *newdb;
  byte url[MAX_URL_SIZE];
  struct urlrec ur;
  byte newdb_name[strlen(urldb_name) + 5];
  struct qhost *h;
  struct fastbuf *qlist;
  uns hboxes = hist_num_boxes+1, hbox, timestamp;
  uns hist[UC_MAX * hboxes];
  uns sec_count[SHERLOCK_NUM_SECTIONS][UC_MAX];
  int age;

  TRACE("Expiring URL database");

  /* Initialize the histograms */
  bzero(hist, sizeof(uns) * UC_MAX * hboxes);
  bzero(sec_count, sizeof(sec_count));

  /* Create new URL database */
  sprintf(newdb_name, "%s.tmp", urldb_name);
  if (unlink(newdb_name) < 0 && errno != ENOENT)
    die("Cannot unlink old URL database: %m");
  /* Must match parameters in urldb_open() */
  so.name = newdb_name;
  so.flags = SDBM_CREAT | SDBM_WRITE;
  so.page_order = 12;
  so.cache_size = urldb_cache_size;
  so.key_size = -1;
  so.val_size = sizeof(struct urlrec);
  if (!(newdb = sdbm_open(&so)))
    die("Cannot open the URL database");

  /* Create the list of to-be-queued items */
  qlist = bopen(tmp_queue_name, O_CREAT|O_TRUNC|O_RDWR, 1048576);

  urldb_open();
  urldb_rewind();
  queue_reset();
  while (urldb_get_next(url, &ur))
    {
      int verdict = judge(url, &ur);
      uns type = classify_ur(&ur);
      url_in[type]++;
      ur.oid = oid_translate(ur.oid);
      DBG("%s: %08x %c %d [%c]", url, ur.oid, "FDRO"[verdict], now-ur.access, "NRPES"[type]);
      if (ur.oid == OID_UNDEFINED && (verdict == JUDGE_REFRESH || verdict == JUDGE_ASIS) && !(ur.flags & URF_QUEUED))
	{
	  log(L_ERROR, "URL %s lost, will re-gather", url);
	  url_lost++;
	  ur.flags |= URF_REGATHER;
	  verdict = JUDGE_REFRESH;
	}
      url_stats[type][verdict]++;
      url_verdict[verdict]++;

      /* Copy to new URLdb and adjust according to verdict */
      switch (verdict)
	{
	case JUDGE_FILTER_OUT:
	case JUDGE_EXPIRE:
	  oid_delete(ur.oid);
	  continue;
	case JUDGE_REFRESH:
	  ur.flags |= URF_QUEUED;
	  break;
	case JUDGE_ASIS:
	  break;
	default:
	  ASSERT(0);
	}
      if (!sdbm_store(newdb, url, strlen(url), (byte *) &ur, sizeof(ur)))
	log(L_ERROR, "Found and ignored duplicate URL: %s", url);
      type = classify_ur(&ur);
      url_out[type]++;
      sec_count[rfdata.section][type]++;

      /*
       *  The host's object count handling is going to be somewhat tricky:
       *  URLdb contains records even for overflowed URL's and we want to
       *  to include as many of them as possible without exceeding the limit.
       *  Hence we update object count here for already downloaded URL's
       *  (which must not be dropped) only and do the rest when rebuilding
       *  the queue.
       */
      h = find_host(rfdata.url_s.protoid, rfdata.url_s.host, rfdata.url_s.port);
      if (!h)
	{
	  log(L_WARN, "Creating host entry for %s", url);
	  h = new_host(rfdata.url_s.protoid, rfdata.url_s.host, rfdata.url_s.port);
	  put_host(h);
	}
      if (ur.oid != OID_UNDEFINED)
	{
	  h->obj_count[rfdata.section]++;
	  if (h->obj_count[rfdata.section] == rfdata.section_hard_max+1)
	    log(L_WARN, "Host %s overfull (probably changed limits, don't worry)", rfdata.url_s.host);
	}

      /* Put to the queue if needed */
      timestamp = ur.access;
      if (timestamp > now)
	timestamp = now;
      age = now - timestamp;
      if (ur.flags & URF_REGATHER)
	ur.flags |= URF_QUEUED;
      if (ur.flags & URF_QUEUED)
	{
	  u32 w = age;
	  if (ur.flags & URF_REGATHER)
	    w += queue_bonus_regather;
	  else if (ur.oid != OID_UNDEFINED)
	    w += queue_bonus_refresh;
	  w += rfdata.queue_bonus;
	  u32 rp = ur.retries * queue_penalty_retry;
	  if (rp < w)
	    w -= rp;
	  else
	    w = 0;
	  bputl(qlist, w);
	  bputa(qlist, (uintptr_t) h);
	  bputw(qlist, strlen(rfdata.url_s.rest));
	  bputs(qlist, rfdata.url_s.rest);
	  bputl(qlist, (ur.oid == OID_UNDEFINED) ? rfdata.section : ~0U);
	  bputl(qlist, rfdata.section_soft_max);
	}

      /* Add to histogram */
      hbox = ((now - now % hist_box_width + hist_box_width) - timestamp) / hist_box_width;
      if (hbox > hist_num_boxes)
	hbox = hist_num_boxes;
      hist[UC_MAX*hbox+type]++;
    }
  urldb_close();
  sdbm_close(newdb);
  if (rename(newdb_name, urldb_name))
    die("urldb rename: %m");
  log(L_INFO, "Total URL's: %d in -> %d out: %d ok, %d expired, %d requeued, %d filtered out, %d lost",
      url_in[UC_NEW] + url_in[UC_REFRESH] + url_in[UC_PRIORITY] + url_in[UC_ERROR] + url_in[UC_STATIC],
      url_out[UC_NEW] + url_out[UC_REFRESH] + url_out[UC_PRIORITY] + url_out[UC_ERROR] + url_out[UC_STATIC],
      url_verdict[JUDGE_ASIS],
      url_verdict[JUDGE_EXPIRE],
      url_verdict[JUDGE_REFRESH],
      url_verdict[JUDGE_FILTER_OUT],
      url_lost);
  for (uns i=0; i<UC_MAX; i++)
    {
      log(L_INFO, "%s URL's: %d in -> %d out: %d ok, %d requeued, %d expired, %d filtered out",
	  ((char *[]){ "Queued new", "Queued refresh", "Queued priority", "Error", "Static" })[i],
	  url_in[i],
	  url_out[i],
	  url_stats[i][JUDGE_ASIS],
	  url_stats[i][JUDGE_REFRESH],
	  url_stats[i][JUDGE_EXPIRE],
	  url_stats[i][JUDGE_FILTER_OUT]);
    }

  if (hist_num_boxes)
    {
      uns bpl = 10, hs, hb;
      byte hh[bpl*60];
      log(L_INFO, "Age histogram (new/rfsh/prio/err/stat):");
      for (hs=0; hs<hboxes; hs+=bpl)
	{
	  byte *h = hh;
	  for (hb=hs; hb<hs+bpl && hb<hboxes; hb++)
	    h += sprintf(h, " %d/%d/%d/%d/%d",
			 hist[UC_MAX*hb + UC_NEW],
			 hist[UC_MAX*hb + UC_REFRESH],
			 hist[UC_MAX*hb + UC_PRIORITY],
			 hist[UC_MAX*hb + UC_ERROR],
			 hist[UC_MAX*hb + UC_STATIC]);
	  log(L_INFO, "hist[%d..%d]:%s", hs, hb-1, hh);
	}
    }
#if SHERLOCK_NUM_SECTIONS > 1
  {
      byte hh[SHERLOCK_NUM_SECTIONS*60], *h = hh;
      for (hbox=0; hbox<SHERLOCK_NUM_SECTIONS; hbox++)
	h += sprintf(h, " %d/%d/%d/%d/%d",
		     sec_count[hbox][UC_NEW],
		     sec_count[hbox][UC_REFRESH],
		     sec_count[hbox][UC_PRIORITY],
		     sec_count[hbox][UC_ERROR],
		     sec_count[hbox][UC_STATIC]);
      log(L_INFO, "Section histogram (new/rfsh/prio/err/stat):%s", hh);
  }
#endif

  rebuild_queue(qlist);
}

/*** Main ***/

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: expire [<switches>]\n\n"
CF_USAGE
"-f\t\tOnly filter URL's and rebuild the queue, don't expire anything\n\
-q\t\tQuick mode, don't shake down buckets nor rebuild MD5db\n\
-v\t\tIncrease verbosity (a.k.a. Expire.Trace)\n\
");
  die("Invalid arguments.");
}

int
main(int argc, char **argv)
{
  int c;

  log_init(argv[0]);
  cf_declare_section("Expire", &expire_config, 0);
  while ((c = cf_getopt(argc, argv, "fqv" CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (c)
      {
      case 'f':
	do_not_expire = 1;
	break;
      case 'q':
	one_pass = 1;
	break;
      case 'v':
	trace++;
	break;
      default:
	usage();
      }
  if (optind != argc)
    usage();

  log(L_INFO, "Starting expirer");
  now = time(NULL);
  gather_lock(0);
  queue_init(1);
  refs_init(1);
  bucket_open(1);
  stamp_read();

  ids_init();
  judge_init();
  if (one_pass)
    {
      gather_note_state("expiring");
      chew_urls();
      chew_robots();
      chew_qkeys();
      ids_close();
      ids_sort(id_count);
      ids_debug("ID's to be deleted");
      quick_delete();
    }
  else
    {
      scan_urls();
      scan_robots();
      ids_close();
      ids_sort(id_count);
      ids_debug("Initial table of ID's");
      gather_note_state("shakedown");
      shake();
      ids_debug("ID's after shakedown");
      chew_urls();
      chew_robots();
      chew_qkeys();
    }

  stamp_write();
  bucket_close();
  queue_cleanup();
  gather_unlock();
  return 0;
}
