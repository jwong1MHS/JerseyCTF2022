/*
 *	Sherlock Search Engine -- String Index
 *
 *	(c) 1997--2006 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/lfs.h"
#include "ucw/fastbuf.h"
#include "ucw/url.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "ucw/unaligned.h"
#include "charset/unicat.h"
#include "search/sherlockd.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

static byte *
string_normalize(struct query *q, byte *w, uns types)
{
  byte buf[3*MAX_URL_SIZE];
  int e;
  byte *c;

  if (strlen(w) >= MAX_URL_SIZE-1)
    {
      add_err("-115 Word too long");
      eval_err(115);
    }
  if (types & STRING_TYPES_URL)
    {
      if (e = url_auto_canonicalize(w, buf))
	{
	  add_err("-112 URL error: %s", url_error(e));
	  eval_err(112);
	}
    }
  else if (types & STRING_TYPES_CASE_INSENSITIVE)
    {
      c = buf;
      while (*w)
	{
	  uns u;
	  w = utf8_get(w, &u);
	  u = Utolower(u);
	  c = utf8_put(c, u);
	}
      *c = 0;
    }
  else
    strcpy(buf, w);

  return mp_strdup(q->pool, buf);
}

static struct expr *
string_split(struct query *q, struct expr *e, uns mask)
{
  struct expr *w1, *w2;

  w1 = mp_alloc(q->pool, sizeof(*e));
  *w1 = *e;
  w1->u.match.type_mask &= mask;
  w1 = string_analyse(q, w1);

  w2 = mp_alloc(q->pool, sizeof(*e));
  *w2 = *e;
  w2->u.match.type_mask &= ~mask;
  w2 = string_analyse(q, w2);

  if (!w1 || !w2)
    return NULL;

  return new_op(EX_OR, w1, w2);
}

int
string_db_find_refchain(struct query *q, struct database *db, byte *s, ucw_off_t *refchain_start, uns *refchain_len)
{
  struct fingerprint fp;
  uns hash;
  ucw_off_t buck_start, buck_size;
  byte *bk, *bkend;

  fingerprint(s, &fp);
  hash = fp_hash(&fp) >> db->string_hash_order;
  buck_start = db->string_hash[hash];
  buck_size = db->string_hash[hash+1] - buck_start;
  if (!buck_size)
    return 0;
  if (hash < ((u32)~0U >> db->string_hash_order))
    buck_size++; /* We need one more record to compute refchain's length */
  buck_start *= sizeof(struct fingerprint) + BYTES_PER_O;
  buck_size *= sizeof(struct fingerprint) + BYTES_PER_O;
  bk = mmap_region(q, db->fd_string_map, buck_start, buck_start + buck_size);
  if (!bk)
    return 0;
  bkend = bk + buck_size - 2 * (BYTES_PER_O + sizeof(fp));

  while (bk <= bkend)
    {
      if (!memcmp(bk, &fp, sizeof(fp)))
	{
	  bk += sizeof(fp);
	  *refchain_start = GET_O(bk);
	  bk += BYTES_PER_O + sizeof(fp);
	  *refchain_len = GET_O(bk) - *refchain_start;
	  return 1;
	}
      bk += sizeof(fp) + BYTES_PER_O;
    }

  return 0;
}

static void
string_lookup(struct query *q, struct word *w)
{
  ucw_off_t refchain_start;
  uns refchain_len;

  if (string_db_find_refchain(q, q->dbase, w->word, &refchain_start, &refchain_len))
    {
      struct variant *v = mp_alloc_zero(current_query->results->pool, sizeof(*v));
      v->lang_mask = ~0U;
      v->flags = VF_QUERY;
      v->refchain_start = refchain_start;
      v->refchain_len = refchain_len;
      slist_add_tail(&w->variants, &v->n);
      w->var_count++;
    }
}

struct expr *
string_analyse(struct query *q, struct expr *e)
{
  byte *str;
  struct word *w;

  if (!q->dbase->string_hash)
    return new_node(EX_NONE);

  uns types = e->u.match.type_mask;

  /* Resolve types with different normalization */
  if ((types & STRING_TYPES_URL) && (types & ~STRING_TYPES_URL))
    return string_split(q, e, STRING_TYPES_URL);
  if ((types & STRING_TYPES_CASE_INSENSITIVE) && (types & ~STRING_TYPES_CASE_INSENSITIVE))
    return string_split(q, e, STRING_TYPES_CASE_INSENSITIVE);

  str = string_normalize(q, e->u.match.word, types);
  w = lookup_word(q, e, str);
  if (w->weight < e->u.match.o.weight)
    w->weight = e->u.match.o.weight;
  e = new_node(EX_WORD);
  e->u.word.w = w;

  if (!w->expanded)			/* If not already resolved */
    {
      w->expanded = 2;
      w->is_outer = 1;
      string_lookup(q, w);
    }

  return e;
}

void
string_analyse_simple(struct query *q, clist *l)
{
  struct simple *s;

  CLIST_WALK(s, *l)
    if (s->raw->u.match.is_string)
      s->cooked = string_analyse(q, s->raw);
}

void
strings_init(struct database *db)
{
  uns i;

  if (!(db->parts & DB_PART_STRINGS))
    return;
  db->string_hash = mmap_file(db_file_name(db, "string-hash"), &db->string_hash_file_size, 0);
  db->string_buckets = (db->string_hash_file_size / 4) - 1;
  for (i=1; (1U << i) < db->string_buckets; i++)
    ;
  if ((1U << i) != db->string_buckets)
    die("Invalid string hash size: %d", db->string_buckets);
  db->string_hash_order = 32 - i;
  byte *fn_map = db_file_name(db, "string-map");
  db->fd_string_map = ucw_open(fn_map, O_RDONLY);
  if (db->fd_string_map < 0)
    die("Unable to open %s: %m", fn_map);
  ucw_off_t len = ucw_seek(db->fd_string_map, 0, SEEK_END);
  if ((u64)len > (u64)0xffffffff * (sizeof(struct fingerprint) + BYTES_PER_O))
    die("String map %s too large", fn_map);
  db->string_map_file_size = len;
  db->string_count = db->string_map_file_size / (sizeof(struct fingerprint) + BYTES_PER_O);
  log(L_INFO, "Loaded string index %s: %d strings, hash order %d",
      db->name,
      db->string_count,
      i);
}
