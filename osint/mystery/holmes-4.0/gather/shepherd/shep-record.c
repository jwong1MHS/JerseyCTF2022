/*
 *	Sherlock Shepherd Daemon -- Record Contributions to the Active Set
 *
 *	(c) 2003--2005 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "sherlock/object.h"
#include "gather/shepherd/shepherd.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static time_t now;

struct contrib_req {
  u32 recid;				/* Bit 0 set if it's a auto_go_root contribution */
  uintptr_t id;			/* For auto_go_root contribs it's a pointer to the corresponding site */
};

#define SORT_KEY_REGULAR struct contrib_req
#define SORT_PREFIX(x) creq_##x
#define SORT_UNIQUE
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
static inline int
creq_compare(struct contrib_req *x, struct contrib_req *y)
{
  COMPARE(x->recid & 1, y->recid & 1);
  COMPARE(x->id, y->id);
  return 0;
}
#include "ucw/sorter/sorter.h"

#define SORT_KEY_REGULAR struct contrib_req
#define SORT_PREFIX(x) crep_##x
#define SORT_UNIQUE
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_INT(x) (x).recid
#include "ucw/sorter/sorter.h"

static struct fastbuf *
extract(struct fastbuf *oidx)
{
  struct fastbuf *creq = temp_state_file();
  struct url_state s;
  uns cnt = 0;
  while (breadb(oidx, &s, sizeof(s)))
    if (s.flags & USF_CONTRIB)
      {
	ASSERT(ustate_type(&s) != UTYPE_ZOMBIE);
	struct contrib_req r;
	r.recid = cnt;
	cnt += 2;
	if (s.oid != OID_UNDEFINED)
	  r.id = s.oid;
	else
	  {
	    ASSERT(auto_go_root && !memcmp(&s.fp.rest, &ROOT_FOOTPRINT, sizeof(struct rest_fp)));
	    r.recid |= 1;
	    struct site *site = site_lookup(&s.fp.site);
	    ASSERT(site);
	    r.id = (uintptr_t) site;
	  }
	bwrite(creq, &r, sizeof(r));
      }
  brewind(creq);
  return creq;
}

static struct fastbuf *
create_buckets(byte *old_state, struct fastbuf *creq)
{
  struct fastbuf *cres = temp_state_file();
  struct fastbuf *ctr = read_state_file(old_state, "contrib");
  struct contrib_req r;
  struct contrib *c = alloca(sizeof(struct contrib) + MAX_URL_SIZE);
  uns bcnt = 0;

  bucket_open(1);
  struct url_db *url_db = url_db_open(O_RDWR | O_CREAT | O_APPEND, 1);
  if (url_db)
    log(L_INFO, "Updating URL database");
  while (breadb(creq, &r, sizeof(r)))
    {
      struct fastbuf *bf = obuck_create(&bucket_file);
      if (r.recid & 1)			/* It's a auto_go_root item */
	{
	  struct site *site = (struct site *) r.id;
	  c->fp.site = site->fp;
	  c->fp.rest = ROOT_FOOTPRINT;
	  struct url u;
	  byte buf1[MAX_URL_SIZE];
	  u.protocol = url_proto_names[site->proto];
	  u.protoid = site->proto;
	  u.user = u.pass = NULL;
	  u.host = site->hostname;
	  u.port = site->port;
	  u.rest = "/";
	  if (url_pack(&u, buf1) || url_enescape(buf1, c->url))
	    ASSERT(0);
	}
      else
	{
	  bsetpos(ctr, (ucw_off_t)r.id << CONTRIB_SHIFT);
	  breadb(ctr, c, sizeof(struct contrib));
	  breadb(ctr, c->url, c->url_len);
	  c->url[c->url_len] = 0;
	}
      put_attr_set_type(BUCKET_TYPE_V33);
      bput_attr_format(bf, 'O', "%08x%08x%08x%08x", FP_QUAD(c->fp));
      bput_attr_str(bf, 'U', c->url);
      struct obuck_header bh;
      obuck_create_end(&bucket_file, bf, BUCKET_TYPE_V33, &bh);
      r.id = bh.oid;
      bwrite(cres, &r, sizeof(r));
      if (url_db)
	url_db_write(url_db, bh.oid, c->url);
      bcnt++;
    }
  log(L_INFO, "Bucket file size: %d MB", (uns)(obuck_get_pos(obuck_predict_last_oid(&bucket_file)) / 1048576));
  if (url_db)
    url_db_close(url_db);
  bucket_close();
  bclose(creq);
  brewind(cres);
  log(L_INFO, "Created %d buckets", bcnt);
  return cres;
}

static void
record(struct fastbuf *nidx, struct fastbuf *oidx, struct fastbuf *crep)
{
  struct url_state s;
  uns sk_del = 0, rob_del = 0;

  while (breadb(oidx, &s, sizeof(s)))
    {
      if (s.flags & USF_CONTRIB)
	{
	  struct contrib_req c;
	  if (!breadb(crep, &c, sizeof(c)))
	    ASSERT(0);
	  s.flags &= ~USF_CONTRIB;
	  s.oid = c.id;
	}
      if (ustate_type(&s) == UTYPE_SKEY || (s.flags & USF_ROBOTS))
	{
	  /* Remove leftover skeys and robot records for sites which no longer exist. */
	  if (!site_lookup(&s.fp.site))
	    {
	      if (ustate_type(&s) == UTYPE_SKEY)
		sk_del++;
	      else
		rob_del++;
	      continue;
	    }
	}
      bwrite(nidx, &s, sizeof(s));
    }
  bflush(nidx);
  log(L_INFO, "Deleted %d server keys and %d robot records for pruned sites", sk_del, rob_del);
}

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-record [<config-options>] <state>\n");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) > 0 || optind != argc-1)
    usage();

  byte *state = argv[optind];
  now = time(NULL);

  log(L_INFO, "Loading site list");
  site_hash_init(NULL);
  site_hash_load(state, 0);

  log(L_INFO, "Extracting contribution requests");
  struct fastbuf *orig_idx = read_state_file(state, "index");
  struct fastbuf *creq = extract(orig_idx);

  log(L_INFO, "Sorting contribution requests");
  creq = creq_sort(creq, NULL);

  log(L_INFO, "Creating new buckets");
  creq = create_buckets(state, creq);

  log(L_INFO, "Sorting new buckets");
  creq = crep_sort(creq, NULL, (u32)~0U);

  log(L_INFO, "Recording contributions");
  struct fastbuf *new_idx = temp_state_file();
  bsetpos(orig_idx, 0);
  record(new_idx, orig_idx, creq);
  bclose(orig_idx);
  bclose(creq);
  put_state_file(state, "index", new_idx, STATE_FLAG_SORTED);

  site_hash_save(state);
  log(L_INFO, "Done");
  return 0;
}
