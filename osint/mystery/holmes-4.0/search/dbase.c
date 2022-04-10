/*
 *	Sherlock Search Engine -- Databases
 *
 *	(c) 1997--2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "ucw/lfs.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/mempool.h"
#include "ucw/conf.h"
#include "ucw/bitarray.h"
#include "indexer/lexicon.h"
#include "indexer/params.h"
#include "search/sherlockd.h"

#include <string.h>
#include <alloca.h>
#include <unistd.h>
#include <sys/mman.h>

struct lexicon_config lexicon_config;

void
db_switch_config(struct database *db)
{
  memcpy(&lexicon_config, &db->params->lex_config, sizeof(struct lexicon_config));
}

byte *
db_file_name(struct database *db, byte *fn)
{
  return mp_multicat(db->pool, db->directory, "/", fn, NULL);
}

struct merge_status {
  struct merge_status *next;
  struct fastbuf *fb;
  struct card_print next_fp;
  bitarray_t dup_flags;
};

static void
db_init_dup_flags(struct database *db)
{
  db->dup_flags = bit_array_xmalloc_zero(db->num_ids);
}

static void
db_apply_dup_flags(struct database *db)
{
  bitarray_t a = db->dup_flags;
  for (uns i = 0; i < db->num_ids; i++)
    if (bit_array_isset(a, i))
      db->card_attrs[i].flags |= CARD_FLAG_DUP;
    else
      db->card_attrs[i].flags &= ~CARD_FLAG_DUP;
  xfree(a);
}

static void
db_apply_blacklists(struct database *db)
{
  bitarray_t a = db->dup_flags;
  uns count = 0, x;
  for (uns i = 0; i < DARY_LEN(db->blacklists); i++)
    {
      struct fastbuf *b = bopen_try(db_file_name(db, db->blacklists[i]), O_RDONLY, 65536);
      if (!b)
	{
	  log(L_INFO, "Blacklist `%s' not found", db->blacklists[i]);
	  continue;
	}
      while ((x = bgetl(b)) != ~0U)
	count += !bit_array_test_and_set(a, x);
      bclose(b);
    }
  log(L_INFO, "Blacklisted %d cards", count);
}

static void
db_merge(void)
{
  struct merge_status *m, *mm, **mp, *first_ms = NULL;
  uns index_cnt = 0;
  uns override_cnt = 0;

  CLIST_FOR_EACH(struct database *, db, databases)
    if (db->fb_card_prints)
      {
	m = alloca(sizeof(*m));
	m->dup_flags = db->dup_flags;
	m->fb = db->fb_card_prints;
	if (breadb(m->fb, &m->next_fp, sizeof(struct card_print)))
	  {
	    m->next = first_ms;
	    first_ms = m;
	  }
	index_cnt++;
      }

  for(;;)
    {
      int eq = 0;
      mm = first_ms;
      if (unlikely(!mm))
	break;
      m = mm->next;
      if (unlikely(!m))
	break;
      while (m)
	{
	  int cmp = memcmp(&m->next_fp.fp, &mm->next_fp.fp, sizeof(struct fingerprint));
	  if (cmp < 0)
	    {
	      mm = m;
	      eq = 0;
	    }
	  else if (!cmp)
	    eq++;
	  m = m->next;
	}
      if (eq)
	{
	  mp = &mm->next;
	  while (m = *mp)
	    {
	      if (!memcmp(&m->next_fp.fp, &mm->next_fp.fp, sizeof(struct fingerprint)))
		{
		  bit_array_set(m->dup_flags, m->next_fp.cardid);
		  override_cnt++;
		  if (unlikely(!breadb(m->fb, &m->next_fp, sizeof(struct card_print))))
		    {
		      *mp = m->next;
		      continue;
		    }
		}
	      mp = &m->next;
	    }
	}
      if (unlikely(!breadb(mm->fb, &mm->next_fp, sizeof(struct card_print))))
	{
	  for (mp=&first_ms; (m = *mp) != mm; mp = &m->next)
	    ;
	  *mp = mm->next;
	}
    }

  log(L_INFO, "Merged %d indices: %d cards overriden", index_cnt, override_cnt);
}

void
db_init(int merge_only)
{
  uns seen_prints = 0;

  CLIST_FOR_EACH(struct database *, db, databases)
    {
      db->pool = cf_pool;
      byte *fn_params = db_file_name(db, "parameters");
      int fd = open(fn_params, O_RDONLY);
      if (fd < 0)
	{
	  if (db->is_optional)
	    {
	      log(L_INFO, "Database %s missing", db->name);
	      continue;
	    }
	  die("Cannot open %s: %m", fn_params);
	}
      db->params = mp_alloc(db->pool, sizeof(struct index_params) + 1);
      int e = read(fd, db->params, sizeof(struct index_params) + 1);
      if (e < 0)
	die("Cannot read database parameters from %s: %m", fn_params);
      if (e != sizeof(struct index_params) || db->params->version != INDEX_VERSION)
	die("%s: Incompatible index", fn_params);
      close(fd);
      db_switch_config(db);
      if (db->parts & DB_PART_PRINTS)
	{
	  db->fb_card_prints = bopen(db_file_name(db, "card-prints"), O_RDONLY, 65536);
	  seen_prints++;
	}
      int rw = (db->parts & DB_PART_PRINTS) || DARY_LEN(db->blacklists);
      uns size;
      db->card_attrs = mmap_file(db_file_name(db, "card-attrs"), &size, rw);
      db->num_ids = size / sizeof(struct card_attr);
      if (db->num_ids)
	db->num_ids--;
      db->card_attrs_end = db->card_attrs + db->num_ids;

      byte *fn_cards = db_file_name(db, "cards");
      db->fd_cards = ucw_open(fn_cards, O_RDONLY);
      if (db->fd_cards < 0)
	die("Unable to open %s: %m", fn_cards);
      db->card_file_size = ucw_seek(db->fd_cards, 0, SEEK_END);

      byte *fn_refs = db_file_name(db, "references");
      db->fd_refs = ucw_open(fn_refs, O_RDONLY);
      if (db->fd_refs < 0)
	die("Unable to open %s: %m", fn_refs);
      db->ref_file_size = ucw_seek(db->fd_refs, 0, SEEK_END);

      log(L_INFO, "Loading database %s: %d documents", db->name, db->num_ids);
      if (db->parts & DB_PART_PRINTS || DARY_LEN(db->blacklists))
	db_init_dup_flags(db);
      if (DARY_LEN(db->blacklists))
	db_apply_blacklists(db);
      if (!merge_only)
	{
	  words_init(db);
	  strings_init(db);
#ifdef CONFIG_SITES
	  bzero(&db->sites, sizeof(db->sites));
	  byte *fn_sites = db_file_name(db, "sites");
	  if (site_map(&db->sites, fn_sites))
	    die("Unable to load site array %s", fn_sites);
#endif
	}

      get_slice_start(db->params, db->slice_start);
      images_init(db);
    }
  if (seen_prints)
    db_merge();
  CLIST_FOR_EACH(struct database *, db, databases)
    {
      if (db->fb_card_prints)
	{
	  bclose(db->fb_card_prints);
	  db->fb_card_prints = NULL;
	}
      if ((db->parts & DB_PART_PRINTS) || DARY_LEN(db->blacklists))
	{
	  db_apply_dup_flags(db);
	  msync(db->card_attrs, db->num_ids * sizeof(struct card_attr), MS_SYNC);
	  if (mprotect(db->card_attrs, db->num_ids * sizeof(struct card_attr), PROT_READ) < 0)
	    die("Cannot reprotect card attributes read-only: %m");
	}
    }
}

struct database *
attr_to_db(struct card_attr *attr, oid_t *ooid)
{
  CLIST_FOR_EACH(struct database *, db, databases)
    if (attr >= db->card_attrs && attr < db->card_attrs_end)
      {
	if (ooid)
	  *ooid = attr - db->card_attrs;
	return db;
      }
  die("attr_to_db: Orphan object with attribute %p", attr);
}
