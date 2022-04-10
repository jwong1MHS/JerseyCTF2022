/*
 *	Sherlock Gatherer Control Utility
 *
 *	(c) 1997--2003 Martin Mares <mj@ucw.cz>
 *	(c) 2005--2009 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "gather/daemon/gatherd.h"
#include "ucw/url.h"
#include "ucw/getopt.h"
#include "sherlock/db.h"
#include "ucw/mempool.h"
#include "sherlock/object.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

/* Globals */

static int verbose;
ucw_time_t now;

/* Support functions */

static struct url url;
static byte ubuf0[MAX_URL_SIZE], ubuf1[MAX_URL_SIZE], ubuf2[MAX_URL_SIZE], ubuf3[MAX_URL_SIZE];

static void
parse_url(byte *u)
{
  int err = url_canon_split(u, ubuf0, ubuf1, &url);
  if (err)
    die("Error parsing %s: %s", u, url_error(err));
}

static byte *
canon_url(byte *u)
{
  int err = url_auto_canonicalize(u, ubuf0);
  if (err)
    die("Error parsing %s: %s", u, url_error(err));
  return ubuf0;
}

/* List hosts and queued objects */

static void
show_host(struct qhost *h)
{
  uns i;

  if (!h->protocol)			/* Free list entry */
    return;
  printf("%s://%s:%d [robots %08x age %d] [",
	 url_proto_names[h->protocol], h->name, h->port,
	 h->robot_id,
	 (h->robot_id == OID_UNDEFINED) ? 0 : (now - h->robot_time));
  for (i=0; i<SHERLOCK_NUM_SECTIONS; i++)
    {
      if (i)
	putchar('+');
      printf("%d", h->obj_count[i]);
    }
  printf(" objects] [key %08x] [pri %u]%s\n",
	 h->qkey,
	 h->qpriority,
	 h->qf_pos ? "" : " [IDLE]");
}

static void
show_host_queue(struct qhost *h)
{
  uns pos;
  struct qitem qi;

  if (!h->protocol)			/* Free list entry */
    return;
  show_host(h);
  putchar('\n');
  pos = queue_walk_start(h);
  while (queue_walk_next(&pos, &qi))
    printf("\t%s (%u)\n", qi.text, qi.priority);
  putchar('\n');
}

static void
list_queue(byte *u)
{
  struct qhost *h;

  queue_init(1);
  if (u)
    {
      parse_url(u);
      h = find_host(url.protoid, url.host, url.port);
      if (!h)
	die("Host not found");
      show_host_queue(h);
    }
  else
    walk_hosts(show_host_queue);
}

static void
list_hosts(byte *u)
{
  struct qhost *h;

  queue_init(1);
  if (u)
    {
      parse_url(u);
      h = find_host(url.protoid, url.host, url.port);
      if (!h)
	die("Host not found");
      show_host(h);
    }
  else
    walk_hosts(show_host);
}

static void
unqueue(byte *ur)
{
  struct qhost *h;
  struct qitem *a;
  struct urlrec u;
  int err;

  parse_url(ur);
  gather_lock(0);
  urldb_open();
  queue_init(1);
  if (! (h = find_host(url.protoid, url.host, url.port)))
    log(L_ERROR, "Unknown host");
  else
    {
      while (a = dequeue_item(h))
	{
	  printf("%s: ", a->text);
	  url.rest = a->text;
	  if (err = url_pack(&url, ubuf2))
	    die("url_pack: %s", url_error(err));
	  if (err = url_enescape(ubuf2, ubuf3))
	    die("url_enescape: %s", url_error(err));
	  if (urldb_lookup(ubuf3, &u))
	    {
	      if (! (u.flags & URF_QUEUED))
		printf("BAD DB RECORD\n");
	      else if (u.oid)
		{
		  printf("keeping object %08x\n", u.oid);
		  u.flags &= ~(URF_QUEUED | URF_REGATHER);
		  urldb_store(ubuf3, &u);
		}
	      else if (urldb_delete(ubuf3))
		printf("DELETED\n");
	      else
		printf("DELETE FAILED\n");
	    }
	  else
	    printf("NO DB ITEM\n");
	}
    }
  queue_cleanup();
  urldb_close();
  gather_unlock();
}

/* Sorting of URL's by db hashes */

struct uhash_key {
  uns len;
  byte url[MAX_URL_SIZE];
};

#define SORT_PREFIX(x) uhash_##x
#define SORT_KEY struct uhash_key
#define SORT_KEY_SIZE(k) ((k).len + OFFSETOF(struct uhash_key, url) + 1)
#define SORT_UNIFY
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB

static int
uhash_compare(struct uhash_key *a, struct uhash_key *b)
{
  u32 ha = sdbm_hash(a->url, a->len);
  u32 hb = sdbm_hash(b->url, b->len);
  COMPARE(ha, hb);
  return strcmp(a->url, b->url);
}

static int
uhash_read_key(struct fastbuf *f, struct uhash_key *k)
{
  byte *x = bgets(f, k->url, MAX_URL_SIZE);
  if (!x)
    return 0;
  k->len = x - k->url;
  return 1;
}

static inline void
uhash_write_key(struct fastbuf *fb, struct uhash_key *k)
{
  bwrite(fb, k->url, k->len);
  bputc(fb, '\n');
}

static inline void
uhash_write_merged(struct fastbuf *fb, struct uhash_key **keys, void **data UNUSED, uns n UNUSED, void *buf UNUSED)
{
  uhash_write_key(fb, *keys);
}

#include "ucw/sorter/sorter.h"

static struct fastbuf *
read_urls_from_stdin(void)
{
  byte buf[MAX_URL_SIZE], buf2[MAX_URL_SIZE];
  struct fastbuf *b_in = bfdopen_shared(0, 65536);
  struct fastbuf *b_out = bopen_tmp(65536);
  while (bgets(b_in, buf, sizeof(buf)))
    {
      if (!buf[0] || buf[0] == '#')
	continue;
      int err;
      if (!(err = url_auto_canonicalize(buf, buf2)))
	bputsn(b_out, buf2);
      else if (log_ref_errors)
	log(L_ERROR, "%s: %s", buf, url_error(err));
    }
  bclose(b_in);
  brewind(b_out);
  return b_out;
}

static void
for_each_url(byte *arg, void (*fn)(byte *url), byte *m)
{
  if (arg)
    {
      arg = canon_url(arg);
      fn(arg);
    }
  else
    {
      log(L_INFO, "Canonicalizing URL's");
      struct fastbuf *f = read_urls_from_stdin();
      log(L_INFO, "Pre-sorting URL's");
      f = uhash_sort(f, NULL);
      log(L_INFO, "%s URL's", m);

      byte buf[MAX_URL_SIZE];
      uns cnt = 0;
      while (bgets(f, buf, sizeof(buf)))
	{
	  if (buf[0] && buf[0] != '#')
	    {
	      fn(buf);
	      cnt++;
	    }
	}
      bclose(f);
      log(L_INFO, "Processed %d URL's", cnt);
    }
}

/* URLs */

static void
show_url0(byte *n, struct urlrec *z)
{
  struct tm *tm;

  printf("%s%s%s%s ", n,
	 (z->flags & URF_QUEUED) ? " [QUEUED]" : "",
	 (z->flags & URF_INITIAL) ? " [INIT]" : "",
	 (z->flags & URF_REGATHER) ? " [REGATHER]" : "");
  if (z->oid == OID_UNDEFINED)
    printf("<unknown>");
  else if (z->oid >= OID_FIRST_ERROR)
    printf("Error %d", z->oid - OID_FIRST_ERROR);
  else
    printf("%08x", z->oid);
  if (verbose)
    {
      time_t t;
      t = z->access;
      if (! (tm = localtime(&t)))
	die("Time parse error");
      printf (" T=%04d%02d%02d:%02d%02d",
	      tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
	      tm->tm_hour, tm->tm_min);
      if (t = z->http_last_mod)
	{
	  if (! (tm = localtime(&t)))
	    die("Lastmod parse error");
	  printf (" L=%04d%02d%02d:%02d%02d",
		  tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
		  tm->tm_hour, tm->tm_min);
	}
      if (z->avg_change_time)
	printf(" C=%d", z->avg_change_time);
      if (z->retries)
	printf(" R=%d", z->retries);
    }
  putchar('\n');
}

static void
show_url_single(byte *url)
{
  struct urlrec z;
  if (!urldb_lookup(url, &z))
    log(L_ERROR, "%s: Not found", url);
  else
    show_url0(url, &z);
}

static void
show_url(byte *u)
{
  gather_lock(1);
  urldb_open();
  if (!u)
    {
      struct urlrec z;
      byte url[MAX_URL_SIZE];
      urldb_rewind();
      while (urldb_get_next(url, &z))
	show_url0(url, &z);
    }
  else
    {
      if (!strcmp(u, "-"))
	u = NULL;
      for_each_url(u, show_url_single, "Listing");
    }
  urldb_close();
  gather_unlock();
}

static void
del_url_single(byte *u)
{
  if (!urldb_delete(u))
    log(L_ERROR, "%s: Not found", u);
}

static void
del_url(byte *u)
{
  gather_lock(0);
  urldb_open();
  for_each_url(u, del_url_single, "Deleting");
  urldb_close();
  gather_unlock();
}

static void
del_obj_single(byte *u)
{
  struct urlrec ur;

  if (!urldb_lookup(u, &ur))
    log(L_ERROR, "%s: Not found", u);
  else
    {
      if (ur.oid != OID_UNDEFINED && ur.oid < OID_FIRST_ERROR)
	obuck_delete(&bucket_file, ur.oid);
      if (!urldb_delete(u))
	log(L_ERROR, "%s: Delete failed, don't ask me why", u);
    }
}

static void
delete_objects(byte *u)
{
  gather_lock(0);
  urldb_open();
  bucket_open(1);
  for_each_url(u, del_obj_single, "Deleting");
  bucket_close();
  urldb_close();
  gather_unlock();
}

/* MD5 Database */

static void
show_md5(byte *m)
{
  struct md5rec z;

  gather_lock(1);
  md5db_open();
  if (m)
    {
      if (!md5db_lookup(m, &z))
	log(L_ERROR, "%s: Not found", m);
      else
	printf("%s: %08x\n", m, z.oid);
    }
  else
    {
      byte M[MD5_HEX_SIZE];
      md5db_rewind();
      while (md5db_get_next(M, &z))
	printf("%s: %08x\n", M, z.oid);
    }
  md5db_close();
  gather_unlock();
}

static void
del_md5(byte *m)
{
  gather_lock(0);
  md5db_open();
  if (!md5db_delete(m))
    log(L_ERROR, "%s: Not found", m);
  md5db_close();
  gather_unlock();
}

/* Insertion of new URL's */

static void
insert_url(byte *url)
{
  add_ref(url, URF_INITIAL);
}

static void
insert_urls(byte *arg)
{
  gather_lock(0);
  bucket_open(1);
  bucket_close();
  queue_init(1);
  urldb_open();
  refs_init(1);
  for_each_url(arg, insert_url, "Inserting");
  urldb_close();
  queue_cleanup();
  gather_unlock();
}

/* Calculation of statistics */

struct hs {
  struct hs *next;
  uns okays, errs, queued, requeued;
  u16 port;
  byte protocol;
  byte hostname[1];
};

#define HASH_NODE struct hs
#define HASH_PREFIX(x) hs_##x
#define HASH_KEY_COMPLEX(x) x hostname, x protocol, x port
#define HASH_KEY_DECL byte *hostname, uns protocol, uns port
#define HASH_WANT_LOOKUP

#define HASH_GIVE_HASHFN
#include "ucw/hashfunc.h"
static inline uns hs_hash(byte *host, uns proto UNUSED, uns port UNUSED)
{
  return hash_string(host);
}

#define HASH_GIVE_EQ
static inline uns hs_eq(byte *h1, uns p1, uns t1, byte *h2, uns p2, uns t2)
{
  return !strcmp(h1, h2) && p1 == p2 && t1 == t2;
}

#define HASH_GIVE_EXTRA_SIZE
static inline int hs_extra_size(byte *h, uns p UNUSED, uns t UNUSED)
{
  return strlen(h);
}

#define HASH_GIVE_INIT_KEY
static inline void hs_init_key(struct hs *n, byte *h, uns p, uns t)
{
  strcpy(n->hostname, h);
  n->protocol = p;
  n->port = t;
  /* Does data initialization as well */
  n->okays = n->errs = n->queued = n->requeued = 0;
}

#define HASH_AUTO_POOL 65536

#include "ucw/hashtable.h"

static void
host_stats(void)
{
  struct urlrec z;
  byte url[MAX_URL_SIZE], buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];
  struct url ur;
  int e;
  struct hs *h;

  hs_init();
  gather_lock(1);
  urldb_open();
  urldb_rewind();
  while (urldb_get_next(url, &z))
    {
      if (e = url_canon_split(url, buf1, buf2, &ur))
	{
	  log(L_ERROR, "Unable to parse %s: %s", url, url_error(e));
	  continue;
	}
      h = hs_lookup(ur.host, ur.protoid, ur.port);
      if (z.flags & URF_QUEUED)
	{
	  if (z.oid == OID_UNDEFINED)
	    h->queued++;
	  else
	    h->requeued++;
	}
      if (z.oid == OID_UNDEFINED)
	;
      else if (z.oid >= OID_FIRST_ERROR)
	h->errs++;
      else
	h->okays++;
    }
  urldb_close();
  gather_unlock();
  HASH_FOR_ALL(hs, h)
    {
      printf("%d\t%d\t%d\t%d\t%s://%s:%d\n", h->okays, h->errs, h->queued, h->requeued,
	     url_proto_names[h->protocol], h->hostname, h->port);
    }
  HASH_END_FOR;
}

/* Testing of ref filter */

static void
test_ref_filter(void)
{
  struct fastbuf *f = bfdopen_shared(0, 4096);
  byte buf[MAX_URL_SIZE], url[MAX_URL_SIZE], *msg;
  struct rfilter_data rfdata;
  int err;

  refs_init(1);
  while (bgets(f, buf, sizeof(buf)))
    {
      if (err = url_auto_canonicalize(buf, url))
	puts(url_error(err));
      else
	{
	  rfdata.url = url;
	  if (msg = ref_filter(&rfdata))
	    puts(msg);
	  else
	    printf("OK: sec=%d sectmax=%d/%d ct=%s ce=%s qbonus=%d url_key=%s\n",
		   rfdata.section, rfdata.section_soft_max, rfdata.section_hard_max,
		   rfdata.content_type, rfdata.content_encoding,
		   rfdata.queue_bonus, rfdata.url_key);
	}
    }
}

/* Requeueing */

static uns requeue_delete_orig;

static void
requeue_url(byte *url)
{
  struct url ur;
  struct urlrec u;
  byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];

  if (url_canon_split(url, buf1, buf2, &ur))
    ASSERT(0);
  if (urldb_lookup(url, &u))
    {
      struct qhost *h = find_host(ur.protoid, ur.host, ur.port);
      ASSERT(h);
      if (u.oid >= OID_FIRST_ERROR)
	u.oid = OID_UNDEFINED;
      if (requeue_delete_orig && u.oid != OID_UNDEFINED)
	{
	  obuck_delete(&bucket_file, u.oid);
	  u.oid = OID_UNDEFINED;
	}
      u.retries = 0;
      if (!(u.flags & URF_QUEUED))
	enqueue_item(h, ur.rest, 0);
      u.flags |= URF_QUEUED | URF_REGATHER;
      urldb_store(url, &u);
    }
  else
    add_ref(url, URF_REGATHER);
}

static void
requeue(uns delete_orig, byte *arg)
{
  requeue_delete_orig = delete_orig;
  gather_lock(0);
  queue_init(1);
  urldb_open();
  refs_init(1);
  bucket_open(1);
  for_each_url(arg, requeue_url, "Requeueing");
  bucket_close();
  urldb_close();
  queue_cleanup();
  gather_unlock();
}

/* Rebuilding of databases */

struct reb_key {
  uns len;				/* URL length */
  u32 hash;				/* SDBM hash */
  struct fingerprint fp;
  oid_t oid;
  uns lastmod;
  byte url[MAX_URL_SIZE];
};

static struct buck2obj_buf *read_buf;
static struct mempool *reb_pool;
static uns reb_counter;

#define SORT_PREFIX(x) reb_##x
#define SORT_KEY struct reb_key
#define SORT_KEY_SIZE(k) ((k).len + OFFSETOF(struct reb_key, url))
#define SORT_UNIFY
#define SORT_HASH_BITS 32
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB

static int
reb_compare(struct reb_key *a, struct reb_key *b)
{
  COMPARE(a->hash, b->hash);
  return memcmp(&a->fp, &b->fp, sizeof(a->fp));
}

static uns
reb_hash(struct reb_key *a)
{
  return a->hash;
}

static int
reb_read_key(struct fastbuf *f, struct reb_key *k)
{
  if (!breadb(f, k, sizeof(struct reb_key) - MAX_URL_SIZE))
    return 0;
  breadb(f, k->url, k->len);
  return 1;
}

static inline void
reb_write_key(struct fastbuf *f, struct reb_key *key)
{
  bwrite(f, key, SORT_KEY_SIZE(*key));
}

static inline void
reb_write_merged(struct fastbuf *f, struct reb_key **keys, void **data UNUSED, uns n, void *buf UNUSED)
{
  uns i = 0;
  for (uns j = 1; j < n; j++)
    if (keys[j]->oid > keys[i]->oid)
      i = j;
  reb_write_key(f, keys[i]);
}

#include "ucw/sorter/sorter.h"

static struct fastbuf *
reb_input(void)
{
  struct fastbuf *out = bopen_tmp(65536);
  struct fastbuf *fb;
  struct obuck_header bhdr;
  struct odes *obj;
  struct reb_key k;
  byte *url, *x;
  while (fb = obuck_slurp_pool(&bucket_file, &bhdr, OBUCK_OID_ANY))
    {
      mp_flush(reb_pool);
      obj = obj_read_bucket(read_buf, reb_pool, bhdr.type, bhdr.length, fb, NULL, 1);
      if (unlikely(!obj))
        {
    	  log(L_ERROR, "Cannot parse bucket %08x", bhdr.oid);
	  continue;
    	}
      
      url = obj_find_aval(obj, 'U');
      if (!url)
        {
    	  log(L_ERROR, "Bucket %08x has no URL", bhdr.oid);
	  continue;
    	}
      if ((x = obj_find_aval(obj, 'T')) && !strcmp(x, "x-sherlock/robots"))
	continue;
      k.lastmod = obj_find_anum(obj, 'D', now);
      k.len = strlen(url) + 1;
      memcpy(k.url, url, k.len);
      k.oid = bhdr.oid;
      k.hash = sdbm_hash(url, k.len-1);
      fingerprint(url, &k.fp);\
      if (!(reb_counter++ % 1024) && verbose)
        {
    	  printf("%d\r", reb_counter);
    	  fflush(stdout);
    	}
      reb_write_key(out, &k);
    }
  return out;
}

static void
synchronize_urldb(void)
{
  struct fastbuf *f;
  struct reb_key k;
  struct urlrec ur;
  oid_t last_oid = ~(oid_t)0;
  uns buck_cnt=0, new_cnt=0, update_cnt=0;

  gather_lock(0);
  gather_note_state("reconstructing");

  bucket_open(0);
  reb_pool = mp_new(65536);
  read_buf = buck2obj_alloc();
  reb_counter = 0;
  log(L_INFO, "Scanning buckets");
  f = reb_sort(reb_input(), NULL);
  bucket_close();
  log(L_INFO, "Found %d URL's", reb_counter);

  log(L_INFO, "Updating URLdb");

  urldb_open();
  while (reb_read_key(f, &k))
    {
      ASSERT(k.oid != last_oid);
      if (urldb_lookup(k.url, &ur))
	{
	  if (ur.oid != k.oid)
	    {
	      ur.oid = k.oid;
	      update_cnt++;
	      urldb_store(k.url, &ur);
	    }
	}
      else
	{
	  new_cnt++;
	  bzero(&ur, sizeof(ur));
	  ur.access = k.lastmod;
	  ur.oid = k.oid;
	  urldb_store(k.url, &ur);
	}
      last_oid = k.oid;
      buck_cnt++;
      if (!(buck_cnt % 1024) && verbose)
	{
	  printf("%d total, %d new, %d upd\r", buck_cnt, new_cnt, update_cnt);
	  fflush(stdout);
	}
    }
  buck2obj_free(read_buf);
  mp_delete(reb_pool);
  urldb_close();
  bclose(f);
  gather_unlock();
  log(L_INFO, "Processed %d URLs, %d were missing from the db, %d had old data.", buck_cnt, new_cnt, update_cnt);
  log(L_INFO, "URLdb synchronized, remember to run full expire to fix the rest.");
}

/* Normalization of URL's */

static void
show_single_norm_url(struct fastbuf *f, byte *url)
{
  byte buf[MAX_URL_SIZE];
  int err = url_auto_canonicalize(url, buf);
  if (err)
    log(L_ERROR_R, "Invalid URL %s: %s", url, url_error(err));
  else
    bputsn(f, buf);
}

static void
show_norm_url(byte *url)
{
  struct fastbuf *o = bfdopen_shared(1, 4096);
  if (url)
    show_single_norm_url(o, url);
  else
    {
      struct fastbuf *i = bfdopen_shared(0, 4096);
      byte buf[MAX_URL_SIZE];
      while (bgets(i, buf, sizeof(buf)))
	show_single_norm_url(o, buf);
      bclose(i);
    }
  bclose(o);
}

/* Main program */

static char *shortopts = CF_SHORT_OPTS "d::fh::i::lm::M:n::q::Q:r::R::su::U::vX";

static void
usage(void)
{
  fprintf(stderr, "Usage: gc [<options>] <commands>\n\
\n\
Options:\n"
CF_USAGE
"-v\t\t\tBe more verbose\n\n\
Commands:\n\
-d[<url>]\tDelete a given URL and its object\n\
-f\t\tTest reference filter\n\
-h[<url>]\tList all known hosts or a specified one\n\
-i[<url>]\tInsert a new URL\n\
-m[<md5>]\tList object matching a given MD5 sum\n\
-M<md5>\t\tDelete from MD5 database\n\
-n[<url>]\tNormalize a given URL\n\
-q[<url>]\tList queued objects [matching host, protocol and port of given URL]\n\
-Q<url>\t\tRemove all queued objects matching host, protocol and port of given URL\n\
-r[<url>]\tSchedule URL for regathering as soon as possible\n\
-R[<url>]\tLike -r, but delete the original contents immediately\n\
-s\t\tCalculate per-host document statistics\n\
-u[<url>]\tList object matching a given URL [if \"-\", read them from stdin]\n\
-U[<url>]\tDelete given URL from database [don't you want -d instead?]\n\
-X\t\tSynchronize URLdb with bucket file\n\
\n\
If no <url> is given to `list' commands, all matching records all listed.\n\
If no <url> is given to `modify' commands, URL's are taken from stdin.\n\
");
  exit(1);
}

int
main(int argc, char **argv)
{
  int opt, jobs = 0;
  log_init("gc");
  now = time(NULL);

  while ((opt = cf_getopt(argc, argv, shortopts, CF_NO_LONG_OPTS, NULL)) >= 0)
  {
    jobs++;
    gatherer_init();
    switch (opt)
    {
    case 'v':
      verbose++;
      break;
    case 'd':
      delete_objects(optarg);
      break;
    case 'f':
      test_ref_filter();
      break;
    case 'h':
      list_hosts(optarg);
      break;
    case 'i':
      insert_urls(optarg);
      break;
    case 'm':
      show_md5(optarg);
      break;
    case 'M':
      del_md5(optarg);
      break;
    case 'n':
      show_norm_url(optarg);
      break;
    case 'q':
      list_queue(optarg);
      break;
    case 'Q':
      unqueue(optarg);
      break;
    case 'r':
      requeue(0, optarg);
      break;
    case 'R':
      requeue(1, optarg);
      break;
    case 's':
      host_stats();
      break;
    case 'u':
      show_url(optarg);
      break;
    case 'U':
      del_url(optarg);
      break;
    case 'X':
      synchronize_urldb();
      break;
    default:
      usage();
    }
  }
  if (!jobs)
    usage();
  return 0;
}
