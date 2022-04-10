/*
 *	Sherlock Shepherd Daemon -- Remote Database Backup
 *
 *	(c) 2004 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 *	(c) 2006--2007 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "sherlock/bucket.h"
#include "ucw/mempool.h"
#include "sherlock/object.h"
#include "sherlock/lizard-fb.h"
#include "ucw/bbuf.h"
#include "ucw/lizard.h"
#include "gather/shepherd/shepherd.h"
#include "gather/shepherd/protocol.h"

#include <stdio.h>
#include <setjmp.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

static uns connect_timeout;
static uns reply_timeout;
static uns retry_count;
static uns retry_delay;
static uns fix_pointers;
static uns opt_no_buckets;
static uns opt_urls;

static struct cf_section mirror_config = {
  CF_ITEMS {
    CF_UNS("ConnectTimeout", &connect_timeout),
    CF_UNS("ReplyTimeout", &reply_timeout),
    CF_UNS("RetryCount", &retry_count),
    CF_UNS("RetryDelay", &retry_delay),
    CF_UNS("FixPointers", &fix_pointers),
    CF_END
  }
};

static sigjmp_buf errjmp;

static void
my_shepp_error(uns code, char *err)
{
  log(L_ERROR, "%d: %s", code, err);
  siglongjmp(errjmp, 1);
}

static uns compress_threshold;

static struct buck2obj_buf *b2o;
static struct mempool *bpool;
static bb_t buck_buf;
static u64 comp_in, comp_out;

static void
reformat_init(void)
{
  b2o = buck2obj_alloc();
  bpool = mp_new(65536);
  bb_init(&buck_buf);
}

static void
reformat_cleanup(void)
{
  bb_done(&buck_buf);
  mp_delete(bpool);
  buck2obj_free(b2o);
  if (comp_in || comp_out)
    log(L_INFO, "Compressed %dMB to %dMB (%d%%)", (uns)(comp_in>>20), (uns)(comp_out>>20),
      (uns)((float)comp_out/comp_in*100));
}

static byte *
reformat_odes(byte *w, struct odes *o)
{
  for (struct oattr *oa = o->attrs; oa; oa=oa->next)
    for (struct oattr *a = oa; a; a=a->same)
      w = put_attr_str(w, a->attr, a->val);
  return w;
}

struct bpos {
  struct footprint fp;
  oid_t oid;
};

static void
reformat_bucket(struct fastbuf *in, struct shepp_bucket_header *sh, struct fastbuf *fbpos)
{
  struct fastbuf *out;
  struct obuck_header bh;
  struct odes *o_hdr;
  struct odes *o_body;
  uns body_start;

  if (compress_threshold && sh->type != BUCKET_TYPE_V33_LIZARD)
    {
      // Reformat bucket
      mp_flush(bpool);
      o_hdr = obj_new(bpool);
      o_body = obj_new(bpool);
    }
  else if (fbpos)
    {
      // Parse header, copy body
      mp_flush(bpool);
      o_hdr = obj_new(bpool);
      o_body = NULL;
    }
  else
    {
      // Copy all
      struct fastbuf *out = obuck_create(&bucket_file);
      bbcopy(in, out, sh->length);
      obuck_create_end(&bucket_file, out, sh->type, &bh);
      return;
    }

  // Parse header and body if needed
  if (buck2obj_parse(b2o, sh->type, sh->length, in, o_hdr, o_body ? NULL : &body_start, o_body, 1) < 0)
    {
      log(L_ERROR, "Error parsing bucket: %m");
      return;
    }
  // Decode fingerprint
  struct bpos bp;
  if (fbpos)
    {
      byte *fpr = obj_find_aval(o_hdr, 'O');
      if (!fpr || strlen(fpr) != 32 || sscanf(fpr, "%8x%8x%8x%8x", &bp.fp.site.x[0], &bp.fp.site.x[1], &bp.fp.rest.x[0], &bp.fp.rest.x[1]) != 4)
        {
          log(L_ERROR, "Bucket has missing or invalid footprint");
          return;
        }
    }
  out = obuck_create(&bucket_file);
  // Reformat bucket
  if (o_body)
    {
      uns exp_len = sh->length + sh->length/10;
      byte *hdr_start = bb_grow(&buck_buf, exp_len);
      byte *w = hdr_start;
      put_attr_set_type(BUCKET_TYPE_V33);
      w = reformat_odes(w, o_hdr);
      uns hdr_len = w - hdr_start;
      byte *body_start = w;
      if (o_body->attrs)
        w = reformat_odes(w, o_body);
      uns body_len = w - body_start;
      ASSERT(hdr_len + body_len < exp_len);
      // Header
      bwrite(out, hdr_start, hdr_len);
      bput_attr_separator(out);
      // Body
      byte *write_pos;
      int avail_out = bdirect_write_prepare(out, &write_pos);
      struct lizard_block_req req = {
        .type = BUCKET_TYPE_V33_LIZARD,
        .ratio = compress_threshold / 100.,
        .in_ptr = body_start,
        .in_len = body_len,
        .out_ptr = write_pos,
        .out_len = avail_out,
      };
      lizard_compress_req_static(&req);
      if (req.out_ptr == write_pos)
        bdirect_write_commit(out, req.out_ptr + req.out_len);
      else
        bwrite(out, req.out_ptr, req.out_len);
      DBG("%d + %d -> %d + %d (%d%%)", hdr_len, body_len, hdr_len, req.out_len, (int)((float)(hdr_len+req.out_len)/(hdr_len+body_len)*100));
      comp_in += hdr_len + body_len + 1;
      comp_out += hdr_len + req.out_len + 1;
      obuck_create_end(&bucket_file, out, req.type, &bh);
    }
  // Copy
  else
    {
      put_attr_set_type(sh->type);
      bput_object(out, o_hdr);
      bput_attr_separator(out);
      bbcopy(in, out, sh->length - body_start);
      obuck_create_end(&bucket_file, out, sh->type, &bh);
    }
  // Write bucket pointer
  if (fbpos)
    {
      bp.oid = bh.oid;
      bwrite(fbpos, &bp, sizeof(bp));
    }
}

#define SORT_PREFIX(x) idx_##x
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_UNIQUE
#define SORT_HASH_FP
static inline int
idx_compare(struct url_state *x, struct url_state *y)
{
  int r = fp_cmp(&x->fp, &y->fp);
  if (r)
    return r;
  uns x_type = ustate_type(x), y_type = ustate_type(y);
  if (x_type != UTYPE_NEW && y_type == UTYPE_NEW)
    return -1;
  if (x_type == UTYPE_NEW && y_type != UTYPE_NEW)
    return 1;
  REV_COMPARE(x->last_seen, y->last_seen);
  REV_COMPARE(x->oid, y->oid);
  return 0;
}
#include "gather/shepherd/index-sort.h"

#define SORT_PREFIX(x) bpos_##x
#define SORT_KEY_REGULAR struct bpos
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_UNIQUE
#define SORT_HASH_BITS 32
static inline int
bpos_compare(struct bpos *x, struct bpos *y)
{
  int r = fp_cmp(&x->fp, &y->fp);
  if (r)
    return r;
  REV_COMPARE(x->oid, y->oid);
  return 0;
}

static inline uns
bpos_hash(struct bpos *x)
{
  return x->fp.site.x[0];
}
#include "ucw/sorter/sorter.h"

static struct fastbuf *
fix_index(struct fastbuf *index, struct fastbuf *fbpos)
{
  struct fastbuf *out = temp_state_file();
  struct url_state st;
  struct bpos bp;
  struct footprint last_bp;
  uns lost_cnt = 0, unref_cnt = 0, upd_cnt = 0, cp_cnt = 0;
  uns have_st = breadb(index, &st, sizeof(st));
  uns have_bp = breadb(fbpos, &bp, sizeof(bp));
  while (have_st || have_bp)
    {
      int cmp;
      if (!have_bp)
        cmp = -1;
      else if (!have_st)
        cmp = 1;
      else
        cmp = fp_cmp(&st.fp, &bp.fp);
      if (cmp < 0)
        {
	  if (ustate_type(&st) == UTYPE_SKEY || ustate_type(&st) == UTYPE_ZOMBIE)
	    {
	      bwrite(out, &st, sizeof(st));
	      cp_cnt++;
	    }
	  else
	    lost_cnt++; // URL state without bucket
        }
      else if (cmp > 0)
        unref_cnt++; // Bucket without URL state
      else
        {
          st.oid = bp.oid;
          bwrite(out, &st, sizeof(st)); // Fix pointer
          upd_cnt++;
        }
      if (cmp <= 0)
	have_st = breadb(index, &st, sizeof(st));
      if (cmp >= 0)
	{
	  last_bp = bp.fp;
	  do
	    have_bp = breadb(fbpos, &bp, sizeof(bp));
	  while (have_bp && !fp_cmp(&last_bp, &bp.fp));
	}
  }
  bclose(index);
  bclose(fbpos);
  log(L_INFO, "%d URL's updated, %d objects copied, %d lost, %d unreferenced buckets", upd_cnt, cp_cnt, lost_cnt, unref_cnt);
  return out;
}

static char *shortopts = "c:bu" CF_SHORT_OPTS;

static struct option longopts[] = {
  CF_LONG_OPTS
  { "compress",		1, 0, 'c' },
  { "no-buckets",	0, 0, 'b' },
  { "urls",		0, 0, 'u' },
  { NULL,		0, 0, 0 }
};

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-mirror [<options>] <host>[:<port>][:<extras>]\n\
\n\
Options:\n\
" CF_USAGE "\
-c, --compress=<p>\tReformat and compress buckets if it gives <p %% of the original\n\
-b, --no-buckets\tDo not mirror bucket file\n\
-u, --urls\t\tDownload URL database (incrementally if possible)\n\
");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  setproctitle_init(argc, argv);
  cf_declare_section("ShepMirror", &mirror_config, 0);

  int opt;
  while ((opt = cf_getopt(argc, argv, shortopts, longopts, NULL)) > 0)
    switch (opt)
      {
      case 'c':
	compress_threshold = atol(optarg);
	break;
      case 'b':
	opt_no_buckets++;
	break;
      case 'u':
	opt_urls++;
	break;
      default:
	usage();
      }
  if (optind != argc-1)
    usage();
  byte *host = cf_strdup(argv[optind]);

  /* This is copied almost literally from indexer/iconnect.c, but I don't see a good common place to put it to */
  shepp_error_cb = my_shepp_error;
  volatile uns retry = retry_count;
  struct shepp_packet_hdr rq;
  struct odes *attrs;
  setproctitle("shep-mirror: Connecting");
  for(;;)
    {
      if (sigsetjmp(errjmp, 1))
	{
	  if (shepp_fd > 0)
	    {
	      close(shepp_fd);
	      shepp_fd = -1;
	    }
	  if (!retry--)
	    die("No input data available (cannot connect to Shepherd server)");
	  log(L_INFO, "Sleeping for %d seconds before trying again", retry_delay);
	  sleep(retry_delay);
	  continue;
	}
      shepp_timeout = connect_timeout;

      byte src[strlen(host) + 4];
      strcpy(src, host);
      if (opt_no_buckets)
	strcpy(src + strlen(host), ":M0");
      byte **extras = shepp_connect(src);

      log(L_INFO, "Asking for state lock");
      attrs = shepp_send_mode(extras);
      break;
    }

  byte *state = create_new_state();
  uns objects = obj_find_anum(attrs, 'N', 0);
  if (!opt_no_buckets)
    log(L_INFO, "Locked gatherer state %s (%d entries)", obj_find_aval(attrs, 'S'), objects);
  else
    log(L_INFO, "Locked gatherer state %s", obj_find_aval(attrs, 'S'));
  log(L_INFO, "Created local state %s", state);

  struct fastbuf *fbpos = NULL;
  if (!opt_no_buckets)
    {
      shepp_send_none(&rq, SHEPP_REQ_SEND_RAW_BUCKETS, NULL);
      struct fastbuf *in = shepp_fb_open_read(&rq);
      if (fix_pointers)
        fbpos = temp_state_file();
      struct shepp_bucket_header sh;
      log(L_INFO, "Mirroring buckets");
      bucket_open(1);
      if (compress_threshold || fbpos)
        reformat_init();
      uns n = 0;
      while (breadb(in, &sh, sizeof(sh)))
        {
          reformat_bucket(in, &sh, fbpos);
          if (!(n++ % 1024))
	    setproctitle("shep-mirror: %d objects (%d%%)", n, (int)((float)n/objects*100));
        }
      if (compress_threshold || fbpos)
        reformat_cleanup();
      bucket_close();
      bclose(in);
    }

  if (opt_urls)
    {
      if (!url_database_file || !*url_database_file)
	die("Undefined path to URL database");
      struct fastbuf *in, *u = bopen_try(url_database_file, O_RDONLY, 65536);
      u64 payload;
      if (u)
        {
	  struct url_db_hdr hdr1, hdr2;
	  u64 size = bfilesize(u);
	  breadb(u, &hdr1, sizeof(hdr1));
	  bclose(u);

	  log(L_INFO, "Fetching URL database header");
	  payload = ~0ULL;
	  shepp_send_raw(&rq, SHEPP_REQ_SEND_URLS, NULL, &payload, sizeof(payload));
	  in = shepp_fb_open_read(&rq);
	  if (bread(in, &hdr2, sizeof(hdr2)) != sizeof(hdr2) || bgetc(in) >= 0 ||
	      hdr2.magic != URL_DB_MAGIC || hdr2.version != URL_DB_VERSION)
	    die("Invalid header format");
	  bclose(in);
	  if (hdr1.time != hdr2.time)
	    goto mirror_urls;

         log(L_INFO, "Appending URL database");
	 payload = size;
	 shepp_send_raw(&rq, SHEPP_REQ_SEND_URLS, NULL, &payload, sizeof(payload));
	 in = shepp_fb_open_read(&rq);
	 u = bopen(url_database_file, O_WRONLY | O_APPEND, 65536);
	 bbcopy(in, u, ~0U);
	 DBG("Downloaded %llu bytes", (long long)(btell(u) - payload));
	 bclose(u);
	 bclose(in);
	}
      else
        {
mirror_urls:
	  log(L_INFO, "Mirroring URL database");
	  u = bopen(url_database_file, O_WRONLY | O_CREAT | O_TRUNC, 65536);
	  payload = 0;
	  shepp_send_raw(&rq, SHEPP_REQ_SEND_URLS, NULL, &payload, sizeof(payload));
	  in = shepp_fb_open_read(&rq);
	  bbcopy(in, u, ~0U);
	  DBG("Downloaded %llu bytes", (long long)btell(u));
	  bclose(u);
	  bclose(in);
	}
    }

  static struct {
    u32 req_type;
    byte *name;
  } files[] = {
    { SHEPP_REQ_SEND_RAW_INDEX, "index" },
    { SHEPP_REQ_SEND_RAW_SITES, "sites" },
    { SHEPP_REQ_SEND_RAW_PARAMS, "params" },
  };

  state_params_new(state);

  struct fastbuf *index = NULL;
  for (uns i=0; i<ARRAY_SIZE(files); i++)
    {
      shepp_send_none(&rq, files[i].req_type, NULL);
      struct fastbuf *in = shepp_fb_open_read(&rq);
      struct fastbuf *out = temp_state_file();
      log(L_INFO, "Mirroring %s", files[i].name);
      setproctitle("shep-mirror: Mirroring %s", files[i].name);
      bbcopy_slow(in, out, ~0U);
      if (fbpos && files[i].req_type == SHEPP_REQ_SEND_RAW_INDEX)
        index = out;
      else
        put_state_file(state, files[i].name, out, 0);
      bclose(in);
    }

  if (fbpos)
    {
      log(L_INFO, "Fixing bucket pointers");
      ASSERT(index);
      brewind(fbpos);
      brewind(index);
      fbpos = bpos_sort(fbpos, NULL);
      index = idx_sort(index, NULL);
      index = fix_index(index, fbpos);
      put_state_file(state, "index", index, 0);

      struct fastbuf *x = temp_state_file();
      bputs(x, "closed\n");
      put_state_file(state, "control", x, 0);

      state_flags_set(state, STATE_FLAG_SORTED);
    }

  log(L_INFO, "Finished");
  return 0;
}
