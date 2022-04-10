/*
 *  Simple Suffix Dictionary Driven Stemmer: Dictionary Generator
 *
 *  (c) 2005 Martin Mares <mj@ucw.cz>
 *
 *  The source file is a text file containing a sequence of classes. Each class
 *  starts with a lemma on a separate line, the following lines contain words
 *  belonging to the class, delta-compressed (each word starts with a number
 *  determinining how long a prefix it shares with the previous word; the first
 *  word can share a prefix with the lemma). If you need to encode a word starting
 *  with a real digit, you can escape it by ":". Words equivalent to the lemma
 *  (alternative spelling etc.) can be marked with "+".
 *
 *  Example:
 *	write
 *	5s			<-- writes
 *	4ing			<-- writing
 *	4ten			<-- written
 *	2ote			<-- wrote
 *	be
 *	0was			<-- was (the `0' cannot be omitted)
 *	zero
 *	0:00			<-- 00
 *	0:+0			<-- 0 marked as alternative spelling of lemma
 *
 *  Possible optimizations:
 *	- discover suffixes common to multiple classes (e.g., -ost)
 *	- optimize out rare suffixes and rare patterns
 *	- 8-bit references to frequently used patterns
 *	- multi-byte skips
 *	- 24-bit main/sub stem pointers
 */

#undef LOCAL_DEBUG
#undef TREE_DEBUG

#include "ucw/lib.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/mempool.h"
#include "ucw/clists.h"
#include "ucw/chartype.h"
#include "ucw/hashfunc.h"
#include "ucw/md5.h"
#include "ucw/unaligned.h"
#include "charset/charconv.h"
#include "charset/unicat.h"
#include "lang/stem-dict.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <alloca.h>

#ifdef TREE_DEBUG
#define TDBG(x...) msg(L_DEBUG, x)
#else
#define TDBG(x...) do { } while(0)
#endif

/*** Options ***/

static byte *charset = "iso-8859-2";
static byte *out_patterns, *out_suffixes, *out_stems, *out_dict, *out_stem_pos;
static uns max_suffix = 6;
static uns min_stem = 2;
static uns max_table = 10;
static uns strip_accents, fold_case;
static byte *prep_in, *prep_out;

static byte prefix_names[MAX_PREFIXES][MAX_PREFIX_LEN];
static uns prefix_lengths[MAX_PREFIXES];
static uns num_prefixes;

/*** Table of lemma variants ***/

static uns num_lemmata, num_variants;

struct lemma {
  byte var_cnt;
  byte w[1];
};

static inline void
lemma_init_data(struct lemma *v)
{
  num_lemmata++;
  v->var_cnt = 0;
}

#define HASH_NODE struct lemma
#define HASH_PREFIX(x) lemma_##x
#define HASH_KEY_ENDSTRING w
#define HASH_WANT_LOOKUP
#define HASH_GIVE_INIT_DATA
#define HASH_AUTO_POOL 65536
#define HASH_WANT_CLEANUP
#include "ucw/hashtable.h"

static uns
lemma_identify_variant(byte *lemma)
{
  struct lemma *v = lemma_lookup(lemma);
  if (v->var_cnt)
    num_variants++;
  return v->var_cnt++;
}

/*** Table of suffixes ***/

static uns num_suffixes;

struct suffix {
  uns use_cnt;
  suffix_id_t id;
  byte len;
  byte w[1];
};

static inline void
suffix_init_data(struct suffix *s)
{
  s->use_cnt = 0;
  s->id = num_suffixes++;
  ASSERT(num_suffixes < MAX_SUFFIXES);
}

#define HASH_NODE struct suffix
#define HASH_PREFIX(x) suffix_##x
#define HASH_KEY_ENDSTRING w
#define HASH_WANT_LOOKUP
#define HASH_GIVE_INIT_DATA
#define HASH_AUTO_POOL 65536
#include "ucw/hashtable.h"

static struct suffix **suffix_by_old_id, **suffixes;

static inline int
suff_lt(struct suffix *x, struct suffix *y)
{
  uns i = x->len;
  uns j = y->len;
  while (i && j)
    {
      uns a = x->w[--i];
      uns b = y->w[--j];
      COMPARE_LT(a,b);
    }
  COMPARE_LT(i,j);
  return 0;
}

#define ASORT_PREFIX(x) suff_##x
#define ASORT_KEY_TYPE struct suffix *
#define ASORT_ELT(i) suffixes[i]
#define ASORT_LT(x,y) suff_lt(x,y)
#include "ucw/sorter/array-simple.h"

static void
suffix_gather(void)
{
  suffix_by_old_id = xmalloc_zero(sizeof(*suffix_by_old_id) * num_suffixes);
  HASH_FOR_ALL(suffix, s)
    {
      suffix_by_old_id[s->id] = s;
      s->len = strlen(s->w);
    }
  HASH_END_FOR;
  suffixes = xmalloc(sizeof(*suffixes) * num_suffixes);
  memcpy(suffixes, suffix_by_old_id, sizeof(*suffixes) * num_suffixes);
  suff_sort(num_suffixes);
  for (uns i=0; i<num_suffixes; i++)
    suffixes[i]->id = i;

  if (out_suffixes)
    {
      struct fastbuf *out = bopen(out_suffixes, O_WRONLY|O_CREAT|O_TRUNC, 65536);
      bputsn(out, "Id\tUseCnt\tSuffix");
      for (uns i=0; i<num_suffixes; i++)
	bprintf(out, "%d\t%d\t%s\n", suffixes[i]->id, suffixes[i]->use_cnt, suffixes[i]->w);
      bclose(out);
      msg(L_INFO, "Dumped suffix table to %s", out_suffixes);
    }
}

/*** Table of patterns ***/

static uns num_patterns;

struct pattern {
  uns use_cnt;
  pattern_id_t id;
  u16 flags;		// bottom 8 bits contain the prefix mask
  suffix_id_t suff[1];	// suff[0] is the number of suffixes
};

static inline uns
pattern_hash(const suffix_id_t *suff, uns flags)
{
  return hash_block((const byte *) suff, (1+suff[0]) * sizeof(suff[0])) ^ flags;
}

static inline int
pattern_eq(const suffix_id_t *x, uns xflg, const suffix_id_t *y, uns yflg)
{
  return (x[0] == y[0]) && !memcmp(x+1, y+1, sizeof(x[0]) * x[0]) && xflg == yflg;
}

static inline int
pattern_extra_size(const suffix_id_t *suff, uns flags UNUSED)
{
  return sizeof(suffix_id_t) * suff[0];
}

static inline void
pattern_init_key(struct pattern *p, const suffix_id_t *suff, uns flags)
{
  p->id = num_patterns++;
  ASSERT(num_patterns <= MAX_PATTERNS);
  memcpy(p->suff, suff, sizeof(suffix_id_t) * (1+suff[0]));
  p->use_cnt = 0;
  p->flags = flags;
}

#define HASH_NODE struct pattern
#define HASH_PREFIX(x) pattern_##x
#define HASH_KEY_COMPLEX(x) x suff, x flags
#define HASH_KEY_DECL const suffix_id_t *suff, uns flags
#define HASH_WANT_LOOKUP
#define HASH_GIVE_HASHFN
#define HASH_GIVE_EQ
#define HASH_GIVE_EXTRA_SIZE
#define HASH_GIVE_INIT_KEY
#define HASH_AUTO_POOL 65536
#include "ucw/hashtable.h"

static struct pattern **patterns;

static void
pattern_gather(void)
{
  patterns = xmalloc_zero(sizeof(*patterns) * num_patterns);
  HASH_FOR_ALL(pattern, p)
    {
      ASSERT(p->use_cnt);
      patterns[p->id] = p;
      for (uns j=1; j<=p->suff[0]; j++)
	p->suff[j] = (suffix_by_old_id[p->suff[j] & MAX_SUFFIXES] -> id) | (p->suff[j] & SUFFIX_FLAG_LEMMA);
    }
  HASH_END_FOR;

  if (out_patterns)
    {
      struct fastbuf *out = bopen(out_patterns, O_WRONLY|O_CREAT|O_TRUNC, 65536);
      bputsn(out, "Id\tUseCnt\tPxMask\tPattern");
      for (uns i=0; i<num_patterns; i++)
	{
	  struct pattern *p = patterns[i];
	  bprintf(out, "%d\t%d\t%04x", i, p->use_cnt, p->flags);
	  for (uns j=1; j<=p->suff[0]; j++)
	    bprintf(out, "\t-%s%s", suffixes[p->suff[j] & MAX_SUFFIXES]->w, (p->suff[j] & SUFFIX_FLAG_LEMMA) ? "!" : "");
	  bputc(out, '\n');
	}
      bclose(out);
      msg(L_INFO, "Dumped pattern table to %s", out_patterns);
    }
}

/*** Table of stems ***/

static uns num_stems, num_substems;

struct stem {
  struct stem *same;
  struct pattern *patt;
  struct stem *main, *sub;
  uns tree_pos;
  byte variant;
  byte w[1];
};

static inline void
stem_init_data(struct stem *s)
{
  s->patt = 0;
  s->same = NULL;
  s->main = s->sub = NULL;
  s->variant = 0;
  num_stems++;
}

#define HASH_NODE struct stem
#define HASH_PREFIX(x) stem_##x
#define HASH_KEY_ENDSTRING w
#define HASH_WANT_LOOKUP
#define HASH_GIVE_INIT_DATA
#define HASH_AUTO_POOL 65536
#include "ucw/hashtable.h"

static struct stem *
stem_add(byte *s, struct pattern *patt, struct stem *main)
{
  DBG("\t-> stem %s + pattern %d", s, patt->id);
  struct stem *st = stem_lookup(s);
  patt->use_cnt++;
  if (!st->patt)
    st->patt = patt;
  else
    {
      struct stem *last = st;
      while (st && st->patt != patt)
	{
	  last = st;
	  st = st->same;
	}
      /* The same entry could already have been present, which is ok and we should link ourselves just after it */
      st = mp_alloc_zero(stem_table.pool, sizeof(*st) - 1);
      st->same = last->same;
      last->same = st;
      st->patt = patt;
    }

  if (!main)
    main = st;
  else
    {
      st->main = main;
      if (!(patt->flags & PATTERN_FLAG_NOACCENT) || strip_accents > 1)
	{
	  st->sub = main->sub;
	  main->sub = st;
	}
      num_substems++;
    }
  return main;
}

static struct stem **stems;

static inline int
stem_lt(struct stem *x, struct stem *y)
{
  return strcmp(x->w, y->w) < 0;
}

#define ASORT_PREFIX(x) stem_##x
#define ASORT_KEY_TYPE struct stem *
#define ASORT_ELT(i) stems[i]
#define ASORT_LT(x,y) stem_lt(x,y)
#include "ucw/sorter/array-simple.h"

static void
stem_gather(void)
{
  stems = xmalloc(sizeof(struct stem *) * num_stems);
  uns cnt = 0;
  HASH_FOR_ALL(stem, st)
    {
      stems[cnt++] = st;
    }
  HASH_END_FOR;
  ASSERT(cnt == num_stems);
  stem_sort(num_stems);

  if (out_stems)
    {
      struct fastbuf *out = bopen(out_stems, O_WRONLY|O_CREAT|O_TRUNC, 65536);
      for (uns i=0; i<num_stems; i++)
	{
	  struct stem *st = stems[i];
	  bprintf(out, "%d: %s\n", i, st->w);
	  for (struct stem *s=st; s; s=s->same)
	    {
	      bprintf(out, "\t%p", s);
	      if (!s->main)
		{
		  if (s->variant)
		    bprintf(out, " #%d", s->variant);
		  if (s->sub)
		    {
		      bputs(out, " ");
		      for (struct stem *t=s->sub; t; t=t->sub)
			{
			  ASSERT(t->main == s);
			  bprintf(out, "%c%p", ((t==s->sub) ? '[' : ' '), t);
			}
		      bputc(out, ']');
		    }
		}
	      else
		bprintf(out, " -> %p", s->main);
	      struct pattern *p = s->patt;
	      bprintf(out, " patt %d (flg %04x):", p->id, p->flags);
	      for (uns j=1; j<=p->suff[0]; j++)
		bprintf(out, " -%s%s", suffixes[p->suff[j] & MAX_SUFFIXES]->w, (p->suff[j] & SUFFIX_FLAG_LEMMA) ? "!" : "");
	      bputc(out, '\n');
	    }
	}
      bclose(out);
      msg(L_INFO, "Dumped stem table to %s", out_stems);
    }
}

/*** Words ***/

static uns num_exc, num_splits;
static struct mempool *pool;
static clist wl;

struct w {
  cnode n;
  byte *w;
  byte is_lemma;
};

static void
wl_init(void)
{
  mp_flush(pool);
  clist_init(&wl);
}

static void
wl_add_to_list(clist *wl, byte *wo, uns is_lemma)
{
  struct w *w = mp_alloc(pool, sizeof(*w));
  if (strlen(wo) > MAX_WORD_BYTES)
    die("Word <%s> is too long, see MAX_WORD_BYTES", wo);
  w->w = mp_strdup(pool, wo);
  w->is_lemma = is_lemma;
  clist_add_tail(wl, &w->n);
}

static void
wl_add(byte *wo, uns is_lemma)
{
  wl_add_to_list(&wl, wo, is_lemma);
}

static uns
wl_member(clist *wl, byte *wo)
{
  struct w *w;
  CLIST_WALK(w, *wl)
    if (!strcmp(w->w, wo))
      return 1;
  return 0;
}

static uns
wl_count(clist *l)
{
  struct w *w;
  uns cnt = 0;
  CLIST_WALK(w, *l)
    cnt++;
  return cnt;
}

static uns
wl_max_len(clist *l)
{
  struct w *w;
  uns maxlen = 0;
  CLIST_WALK(w, *l)
    {
      uns len = strlen(w->w);
      maxlen = MAX(maxlen, len);
    }
  return maxlen;
}

static uns
wl_common_prefix(clist *wl)
{
  struct w *w, *first = clist_head(wl);
  int common, l;
  for(common=0; l = first->w[common]; common++)
    {
      CLIST_WALK(w, *wl)
	if (w->w[common] != l)
	  return common;
    }
  return common;
}

static uns
wl_try_prefix(clist *wl, byte *px, uns pxl)
{
  struct w *w;
  CLIST_WALK(w, *wl)
    if (strlen(w->w) < pxl || memcmp(w->w, px, pxl))
      return 0;
  CLIST_WALK(w, *wl)
    w->w += pxl;
  return 1;
}

#ifdef LOCAL_DEBUG
static void
dump(clist *wl)
{
  struct w *w;
  CLIST_WALK(w, *wl)
    msg(L_DEBUG, "\t%s (%d)", w->w, w->is_lemma);
}
#endif

static struct stem *
add_pattern(clist *wl, uns cnt, uns common, uns flags, struct stem *main)
{
  suffix_id_t suff[1+cnt];
  suff[0] = cnt;
  cnt = 1;
  struct w *w;
  CLIST_WALK(w, *wl)
    {
      struct suffix *s = suffix_lookup(w->w + common);
      suff[cnt++] = s->id | (w->is_lemma ? SUFFIX_FLAG_LEMMA : 0);
      s->use_cnt++;
    }
  struct pattern *patt = pattern_lookup(suff, flags);
  byte stem[common+1];
  memcpy(stem, ((struct w *) clist_head(wl))->w, common);
  stem[common] = 0;
  return stem_add(stem, patt, main);
}

static struct stem *
try_list(clist *wl, uns flags, struct stem *main)
{
  struct w *w;
  uns cnt = wl_count(wl);
#ifdef LOCAL_DEBUG
  DBG("Trying: flags=%04x, cnt=%d", flags, cnt);
  dump(wl);
#endif

  if (cnt == 1)
    {
      num_exc++;
      w = clist_head(wl);
      pattern_id_t p[] = { 1, (w->is_lemma ? SUFFIX_FLAG_LEMMA : 0) };
      struct pattern *patt = pattern_lookup(p, flags);
      return stem_add(((struct w *) clist_head(wl))->w, patt, main);
    }

  uns maxlen = wl_max_len(wl);
  uns common = wl_common_prefix(wl);
  DBG("\tmaxlen=%d, common=%d", maxlen, common);

  if (common >= min_stem && maxlen - common <= max_suffix)
    return add_pattern(wl, cnt, common, flags, main);

  int ac=-1;
  clist al, bl;
  clist_init(&al);
  clist_init(&bl);
  CLIST_WALK(w, *wl)
    if (w->is_lemma)
      {
	ac = w->w[common];
	break;
      }
  CLIST_WALK(w, *wl)
    {
      struct w *ww = mp_alloc(pool, sizeof(*ww));
      ww->w = w->w;
      ww->is_lemma = w->is_lemma;
      if (ac < 0 || ww->w[common] == ac)
	{
	  ac = ww->w[common];
	  clist_add_tail(&al, &ww->n);
	}
      else
	clist_add_tail(&bl, &ww->n);
    }
  num_splits++;
  main = try_list(&al, flags, main);
  main = try_list(&bl, flags, main);
  return main;
}

/*** Unaccenting and case folding ***/

static byte unacc_table[256], case_fold_table[256];
static uns num_strips;

static void
char_table_init(void)
{
  int cs = find_charset_by_name(charset);
  if (cs < 0)
    die("Charset `%s' not recognized", charset);
  if (cs == CONV_CHARSET_UTF8)
    die("Accent stripping of UTF-8 characters not supported");
  struct conv_context cc_in, cc_out;
  conv_init(&cc_in);
  conv_set_charset(&cc_in, cs, CONV_CHARSET_UTF8);
  conv_init(&cc_out);
  conv_set_charset(&cc_out, CONV_CHARSET_UTF8, cs);

  for (uns c=0; c<256; c++)
    {
      uns u = conv_in_to_ucs(&cc_in, c);
      int x = conv_ucs_to_out(&cc_out, Uunaccent(u));
      unacc_table[c] = (x < 0) ? c : (uns)x;
      x = conv_ucs_to_out(&cc_out, Utolower(u));
      case_fold_table[c] = (x < 0) ? c : (uns)x;
    }
}

static void
case_fold_string(byte *dest, byte *src)
{
  while (*src)
    *dest++ = case_fold_table[*src++];
  *dest =  0;
}

static int
unacc_string(byte *dest, byte *src)
{
  int chg = 0;
  while (*src)
    {
      *dest = unacc_table[*src];
      if (*src++ != *dest++)
	chg++;
    }
  *dest =  0;
  return chg;
}

static clist *
wl_unacc_list(clist *wl)
{
  clist *ul = mp_alloc(pool, sizeof(*ul));
  clist_init(ul);
  struct w *w;
  CLIST_WALK(w, *wl)
    {
      byte uw[MAX_WORD_BYTES+1];
      if (unacc_string(uw, w->w) && !wl_member(wl, uw) && !wl_member(ul, uw))
	{
	  DBG("Unaccented %s -> %s", w->w, uw);
	  wl_add_to_list(ul, uw, 0);
	  num_strips++;
	}
    }
  return clist_empty(ul) ? NULL : ul;
}

/*** Preprocessing ***/

static uns prep_in_cnt, prep_merge_cnt, px_cnt;

struct prep_key {
  u16 prefix_mask;
  u16 data_len;
  byte hash[MD5_SIZE];
};

#define SORT_PREFIX(x) prep_##x
#define SORT_KEY_REGULAR struct prep_key
#define SORT_DATA_SIZE(k) ((k).data_len)
#define SORT_UNIFY
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#define SORT_HASH_BITS 32

static inline int
prep_compare(struct prep_key *x, struct prep_key *y)
{
  return memcmp(x->hash, y->hash, MD5_SIZE);
}

static inline uns
prep_hash(struct prep_key *x)
{
  return get_u32_be(x->hash);
}

static inline void
prep_write_merged(struct fastbuf *f, struct prep_key **k, void **d, uns n, void *buf UNUSED)
{
  for (uns i = 1; i < n; i++)
    {
      k[0]->prefix_mask |= k[i]->prefix_mask;
      ASSERT(k[0]->data_len == k[i]->data_len);
    }
  bwrite(f, k[0], sizeof(**k));
  bwrite(f, d[0], k[0]->data_len);
  prep_merge_cnt++;
}

static inline void
prep_copy_merged(struct prep_key **k, struct fastbuf **d, uns n, struct fastbuf *f)
{
  for (uns i = 1; i < n; i++)
    {
      k[0]->prefix_mask |= k[i]->prefix_mask;
      ASSERT(k[0]->data_len == k[i]->data_len);
      bskip(d[i], k[0]->data_len);
    }
  bwrite(f, k[0], sizeof(**k));
  bbcopy(d[0], f, k[0]->data_len);
}

#include "ucw/sorter/sorter.h"

#define ASORT_PREFIX(x) prew_##x
#define ASORT_KEY_TYPE struct w *
#define ASORT_LT(x,y) strcmp(x->w, y->w) < 0
#include "ucw/sorter/array-simple.h"

static void
prep_flush(struct fastbuf *f)
{
  struct w *w, **words;
  struct prep_key key;
  uns cnt = 0;
  prep_in_cnt++;

  CLIST_WALK(w, wl)
    cnt++;
  words = alloca(sizeof(*words) * cnt);
  cnt = 0;
  CLIST_WALK(w, wl)
    words[cnt++] = w;

  prew_sort(words, cnt);
  clist_init(&wl);
  for (uns i=0; i<cnt; i++)
    if (!i || strcmp(words[i-1]->w, words[i]->w))
      clist_add_tail(&wl, &words[i]->n);
    else
      ((struct w *) clist_tail(&wl))->is_lemma |= words[i]->is_lemma;
  if (clist_head(&wl) == clist_tail(&wl)) /* empty or 1-elt */
    return;

  uns prefixes = 0;
  for (uns i=0; i<num_prefixes; i++)
    prefixes |= wl_try_prefix(&wl, prefix_names[i], prefix_lengths[i]) ? (1 << i) : 0;
  if (prefixes)
    px_cnt++;
  key.prefix_mask = 1 << prefixes;

  md5_context mc;
  md5_init(&mc);
  uns data_len = 1;
  CLIST_WALK(w, wl)
    {
      uns len = strlen(w->w) + 1;
      md5_update(&mc, w->w, len);
      md5_update(&mc, &w->is_lemma, 1);
      data_len += len + 1;
    }
  memcpy(key.hash, md5_final(&mc), MD5_SIZE);
  ASSERT(data_len < 65536);
  key.data_len = data_len;

  bwrite(f, &key, sizeof(key));
  ucw_off_t pos0 = btell(f);
  CLIST_WALK(w, wl)
    {
      bputc(f, w->is_lemma + 1);
      bputsn(f, w->w);
    }
  bputc(f, '\n');
  ASSERT(btell(f) == pos0+key.data_len);
}

static struct fastbuf *
preprocess(struct fastbuf *in)
{
  if (prep_in)
    {
      bclose(in);
      return bopen(prep_in, O_RDONLY, 65536);
    }

  struct fastbuf *tmp = bopen_tmp(65536);
  byte line[256], last[256];
  wl_init();
  msg(L_INFO, "Scanning input");
  while (bgets(in, line, sizeof(line)))
    {
      int c = 0;
      byte *x = line;
      if (!Cdigit(*x))
	{
	  prep_flush(tmp);
	  wl_init();
	}
      else while (Cdigit(*x))
	c = 10*c + *x++ - '0';
      if (*x == ':')
	x++;
      if (*x == '+')			/* FIXME: Does not do anything yet */
	x++;
      if (fold_case)
	case_fold_string(last+c, x);
      else
	strcpy(last+c, x);
      wl_add(last, !!clist_empty(&wl));
    }
  prep_flush(tmp);
  bclose(in);

  msg(L_INFO, "Preprocessing %d classes", prep_in_cnt);
  brewind(tmp);
  tmp = prep_sort(tmp, NULL);

  msg(L_INFO, "Merged %d classes differing only by prefixes", prep_merge_cnt);
  if (prep_out)
    {
      bfix_tmp_file(tmp, prep_out);
      msg(L_INFO, "Stored preprocessed data to %s", prep_out);
      exit(0);
    }
  return tmp;
}

static void
chew(struct fastbuf *in)
{
  struct prep_key k;
  while (breadb(in, &k, sizeof(k)))
    {
      wl_init();
      byte buf[MAX_WORD_BYTES+1];
      int lemma_var = -1;
      while (bgets(in, buf, sizeof(buf)) && buf[0])
	{
	  byte *w = buf+1;
	  uns is_lemma = buf[0]-1;
	  if (is_lemma && lemma_var < 0)
	    lemma_var = lemma_identify_variant(w);
	  wl_add(w, is_lemma);
	}
      ASSERT(lemma_var >= 0);
      struct stem *main_stem = try_list(&wl, k.prefix_mask, NULL);
      main_stem->variant = lemma_var;
      if (strip_accents)
	{
	  clist *ul = wl_unacc_list(&wl);
	  if (ul)
	    try_list(ul, k.prefix_mask | PATTERN_FLAG_NOACCENT, main_stem);
	}
    }
  bclose(in);
}

/*** Writing the dictionary ***/

static struct stem_dict_hdr dict_hdr;
static struct fastbuf *dict_out;

static void
gen_suffixes(void)
{
  dict_hdr.suffix_table_start = btell(dict_out);
  dict_hdr.num_suffixes = num_suffixes;
  dict_hdr.max_suffix = max_suffix;
  uns suffix_dir_len = 4*(num_suffixes+1);
  uns stab_start = dict_hdr.suffix_table_start + suffix_dir_len;
  uns spos = stab_start;
  for (uns i=0; i<num_suffixes; i++)
    {
      bputl(dict_out, spos);
      spos += suffixes[i]->len;
    }
  bputl(dict_out, spos);
  ASSERT((uns)btell(dict_out) == stab_start);
  for (uns i=0; i<num_suffixes; i++)
    for (int j=suffixes[i]->len-1; j>=0; j--)
      bputc(dict_out, suffixes[i]->w[j]);
  ASSERT((uns)btell(dict_out) == spos);
  msg(L_INFO, "Suffix table: %d bytes pointers + %d bytes data", suffix_dir_len, spos-stab_start);
}

static void
gen_patterns(void)
{
  dict_hdr.pattern_table_start = btell(dict_out);
  dict_hdr.num_patterns = num_patterns;
  uns pattern_dir_len = 4*(num_patterns+1);
  uns ptab_start = dict_hdr.pattern_table_start + pattern_dir_len;
  uns ppos = ptab_start;
  for (uns i=0; i<num_patterns; i++)
    {
      bputl(dict_out, ppos);
      ppos += sizeof(struct stem_dict_pattern) + patterns[i]->suff[0]*sizeof(pattern_id_t);
    }
  bputl(dict_out, ppos);
  ASSERT((uns)btell(dict_out) == ptab_start);
  for (uns i=0; i<num_patterns; i++)
    {
      struct pattern *p = patterns[i];
      bputw(dict_out, p->flags);
      bwrite(dict_out, p->suff+1, p->suff[0]*sizeof(p->suff[0]));
    }
  ASSERT((uns)btell(dict_out) == ppos);
  msg(L_INFO, "Pattern table: %d bytes pointers + %d bytes data", pattern_dir_len, ppos-ptab_start);
}

static uns max_vertices, num_vertices, num_son_ids, num_vertices_pass1, num_son_ids_pass1;
static uns *vertex_pos, *vertex_son_ids;
static byte *vertex_types;

struct vertex {
  uns start, cnt;
  uns id;
  uns subtree_size, subtree_full_size;
  uns first_son_id;
  byte split_char;
  byte type;
  byte skip_chars;
  byte level;
};

enum vertex_type {
  VT_UNKNOWN,
  VT_BRANCH,
  VT_BRANCH_SMALL,
  VT_BRANCH_LARGE,
  VT_TABLE,
  VT_SKIP,
  VT_MAX
};

static uns vt_stats[VT_MAX];

#define VT_NAMES ((const char * const []) { "???", "BRANCH", "BRANCH-S", "BRANCH-L", "TABLE", "SKIP" })

static uns
find_sons(struct vertex *v, struct vertex *sons, struct vertex *leaves)
{
  ASSERT(num_vertices < max_vertices);
  v->id = num_vertices++;

  leaves->start = v->start;
  leaves->type = VT_TABLE;
  leaves->level = v->level;
  if (v->cnt == 1)
    {
      v->type = VT_TABLE;
      leaves->cnt = 1;
      return 0;
    }
  uns i = 0;
  while (i < v->cnt && !stems[v->start+i]->w[v->level])
    i++;
  leaves->cnt = i;

  struct vertex *son = NULL;
  uns deg = 0;
  while (i < v->cnt)
    {
      struct stem *st = stems[v->start + i];
      if (!son || son->split_char != st->w[v->level])
	{
	  ASSERT(deg < MAX_BRANCHING);
	  son = &sons[deg++];
	  son->start = v->start + i;
	  son->cnt = 1;
	  son->split_char = st->w[v->level];
	  son->type = VT_UNKNOWN;
	  son->skip_chars = 0;
	  son->level = v->level + 1;
	}
      else
	son->cnt++;
      i++;
    }

  if (!deg)
    v->type = VT_TABLE;
  else if (deg == 1 && !leaves->cnt)
    {
      v->type = VT_SKIP;
      for (v->skip_chars=1;; v->skip_chars++)
	{
	  uns l = v->level + v->skip_chars;
	  uns c0 = stems[v->start]->w[l];
	  uns i;
	  for (i=0; i<v->cnt; i++)
	    if (stems[v->start+i]->w[l] != c0)
	      break;
	  if (i < v->cnt)
	    break;
	}
      sons[0].level = v->level + v->skip_chars;
    }
  else
    v->type = VT_BRANCH;

#ifdef TREE_DEBUG
  TDBG("Vertex %d (%d+%d) on level %d with deg %d, pre-type %s and skip %d:", v->id, v->start, v->cnt, v->level, deg, VT_NAMES[v->type], v->skip_chars);
  if (leaves->cnt)
    TDBG("\t== (%d+%d)", leaves->start, leaves->cnt);
  for (uns i=0; i<deg; i++)
    TDBG("\t`%c' (%d+%d)", sons[i].split_char, sons[i].start, sons[i].cnt);
#endif

  return deg;
}

static uns
size_stem(struct vertex *parent, struct stem *first, struct stem *st)
{
  uns len = 1;
  if (st == first)
    len += strlen(st->w + parent->level);
  len += sizeof(pattern_id_t);
  if (st->main)
    len += 4;	// pointer to main stem
  else
    {
      if (st->variant)
	len++;
      for (struct stem *sub=st->sub; sub; sub=sub->sub)
	len += 4;	// pointers to substems
    }
  return len;
}

static uns
size_leaves(struct vertex *leaves)
{
  if (!leaves->cnt)
    return 0;
  ASSERT(leaves->cnt == 1);
  struct stem *st = stems[leaves->start];
  uns len = 0;
  for (struct stem *ss=st; ss; ss=ss->same)
    len += size_stem(leaves, st, ss);
  return len;
}

static uns
decide_type(struct vertex *v, struct vertex *sons, uns deg, struct vertex *leaves)
{
  uns leaf_len = size_leaves(leaves);			// Leaves ending at this level
  uns this_len;
  switch (v->type)
    {
    case VT_SKIP:
      this_len = 2*v->skip_chars;
      v->subtree_size = sons[0].subtree_size + this_len;
      v->subtree_full_size = sons[0].subtree_full_size + v->skip_chars*v->cnt;
      break;
    case VT_TABLE:
      this_len = leaf_len;
      v->subtree_size = v->subtree_full_size = this_len;
      break;
    case VT_BRANCH: ;
      uns sub_len = 0, sub_len_full = 0;
      for (uns i=0; i<deg; i++)
	{
	  sub_len += sons[i].subtree_size;
	  sub_len_full += sons[i].subtree_full_size;
	}
      v->type = VT_BRANCH_SMALL;
      uns table_len = 1 + 3*deg;
      uns last_sub = table_len + leaf_len + sub_len - sons[deg-1].subtree_size;
      if (deg >= 64 || last_sub >= 0x10000)
	{
	  v->type = VT_BRANCH_LARGE;
	  table_len = 2 + 5*deg;
	}
      uns len_branched = table_len + leaf_len + sub_len;
      uns len_full = leaf_len + sub_len_full + (v->cnt - leaves->cnt);
      TDBG("\tlen_branched=%d, len_full=%d", len_branched, len_full);
      if (len_full < len_branched && v->cnt <= max_table)
	{
	  v->type = VT_TABLE;
	  this_len = len_full;
	  v->subtree_size = v->subtree_full_size = this_len;
	  num_vertices = v->id+1;
	  num_son_ids = v->first_son_id;
	}
      else
	{
	  this_len = table_len + leaf_len;
	  v->subtree_size = len_branched;
	  v->subtree_full_size = len_full;
	}
      break;
    default:
      ASSERT(0);
    }
  TDBG("V%d: decided on type %s, len=%d, tree_len=%d, full_len=%d, leaf_len=%d", v->id, VT_NAMES[v->type], this_len, v->subtree_size, v->subtree_full_size, leaf_len);
  vertex_types[v->id] = v->type;
  return this_len;
}

static void
prepare_subtree(struct vertex *v)
{
  struct vertex *sons = alloca(sizeof(*sons) * MAX_BRANCHING);
  struct vertex leaves;
  uns deg = find_sons(v, sons, &leaves);
  v->first_son_id = num_son_ids;
  uns *son_ids = &vertex_son_ids[num_son_ids];
  num_son_ids += deg;

  for (uns i=0; i<deg; i++)
    {
      prepare_subtree(&sons[i]);
      son_ids[i] = sons[i].id;
    }
#ifdef TREE_DEBUG
  for (uns i=0; i<deg; i++)
    TDBG("V%d: son #%d is %d", v->id, i, son_ids[i]);
#endif
  vertex_pos[v->id] = decide_type(v, sons, deg, &leaves);
}

static void
prepare_tree(void)
{
  max_vertices = 3*num_stems;
  vertex_pos = xmalloc(sizeof(*vertex_pos) * (max_vertices+1));
  vertex_types = xmalloc(sizeof(*vertex_types) * max_vertices);
  vertex_son_ids = xmalloc(sizeof(*vertex_son_ids) * max_vertices);

  struct vertex root = { .start = 0, .cnt = num_stems, .level=0 };
  prepare_subtree(&root);
  num_vertices_pass1 = num_vertices;
  num_son_ids_pass1 = num_son_ids;

  uns pos = 0;
  for (uns i=0; i<num_vertices; i++)
    {
      uns l = vertex_pos[i];
      vertex_pos[i] = pos;
      pos += l;
      vt_stats[vertex_types[i]]++;
    }
  vertex_pos[num_vertices] = pos;
  msg(L_INFO, "Planned tree with %d vertices, %d bytes large", num_vertices, pos);
  msg(L_INFO, "Vertex types: %d table, %d skip, %d small + %d large branching",
      vt_stats[VT_TABLE], vt_stats[VT_SKIP], vt_stats[VT_BRANCH_SMALL], vt_stats[VT_BRANCH_LARGE]);
}

static void
calc_stem_pos_range(struct vertex *v, uns start, uns cnt, uns pos)
{
  while (cnt--)
    {
      struct stem *st = stems[start];
      for (struct stem *ss=st; ss; ss=ss->same)
	{
	  ss->tree_pos = pos;
	  pos += size_stem(v, st, ss);
	}
      start++;
    }
}

static void
calc_stem_pos_subtree(struct vertex *v)
{
  struct vertex *sons = alloca(sizeof(*sons) * MAX_BRANCHING);
  struct vertex leaves;
  uns deg = find_sons(v, sons, &leaves);
  uns pos = vertex_pos[v->id];
  uns type = vertex_types[v->id];

  if (type == VT_TABLE)
    calc_stem_pos_range(v, v->start, v->cnt, pos);
  else
    {
      if (type == VT_BRANCH_SMALL)
	pos += 1 + 3*deg;
      else if (type == VT_BRANCH_LARGE)
	pos += 2 + 5*deg;
      calc_stem_pos_range(v, leaves.start, leaves.cnt, pos);
      for (uns i=0; i<deg; i++)
	calc_stem_pos_subtree(&sons[i]);
    }
}

static void
calc_stem_pos(void)
{
  struct vertex root = { .start = 0, .cnt = num_stems, .level=0 };
  num_vertices = 0;
  calc_stem_pos_subtree(&root);
  ASSERT(num_vertices == num_vertices_pass1 && num_son_ids == num_son_ids_pass1);
  msg(L_INFO, "Calculated stem positions");

  if (out_stem_pos)
    {
      struct fastbuf *f = bopen(out_stem_pos, O_WRONLY|O_CREAT|O_TRUNC, 65536);
      for (uns i=0; i<num_stems; i++)
	{
	  struct stem *st = stems[i];
	  for (struct stem *ss=st; ss; ss=ss->same)
	    bprintf(f, "%x\t%s\n", ss->tree_pos, (ss==st ? st->w : (byte*)"-\"-"));
	}
      bclose(f);
      msg(L_INFO, "Dumped stem positions to %s", out_stem_pos);
    }
}

static void
gen_leaf(struct vertex *v, uns id)
{
  struct stem *sm = stems[id];
  for (struct stem *st = sm; st; st=st->same)
    {
      if (st == sm)
	{
	  uns l = strlen(sm->w + v->level);
	  bputc(dict_out, 0x40 + l);
	  bwrite(dict_out, sm->w + v->level, l);
	}
      else
	bputc(dict_out, 0xf1);
      pattern_id_t pid = st->patt->id;
      bwrite(dict_out, &pid, sizeof(pid));
      if (st->main)
	{
	  ASSERT(!st->main->main);
	  uns pos = st->main->tree_pos;
	  bputc(dict_out, 0xc0 | ((pos >> 24) & 0xff));
	  bputc(dict_out, (pos >> 16) & 0xff);
	  bputc(dict_out, (pos >> 8) & 0xff);
	  bputc(dict_out, pos & 0xff);
	}
      else
	{
	  if (st->variant)
	    {
	      ASSERT(st->variant < 16);
	      bputc(dict_out, 0xe0 + st->variant);
	    }
	  for (struct stem *sx=st->sub; sx; sx=sx->sub)
	    {
	      uns pos = sx->tree_pos;
	      bputc(dict_out, 0x80 | ((pos >> 24) & 0xff));
	      bputc(dict_out, (pos >> 16) & 0xff);
	      bputc(dict_out, (pos >> 8) & 0xff);
	      bputc(dict_out, pos & 0xff);
	    }
	}
    }
}

static void
gen_subtree(struct vertex *v)
{
  struct vertex *sons = alloca(sizeof(*sons) * MAX_BRANCHING);
  struct vertex leaves;
  uns deg = find_sons(v, sons, &leaves);
  uns type = vertex_types[v->id];
  if (type == VT_TABLE)
    deg = 0;
  uns *son_ids = &vertex_son_ids[num_son_ids];
  num_son_ids += deg;

  TDBG("Gen V%d at pos %d (exp %d, abs 0x%x) type %s (%d+%d)", v->id, (uns)btell(dict_out) - dict_hdr.stem_tree_start,
       vertex_pos[v->id], (uns)btell(dict_out), VT_NAMES[type], v->start, v->cnt);
  ASSERT((uns)btell(dict_out) == dict_hdr.stem_tree_start + vertex_pos[v->id]);
  switch (type)
    {
    case VT_SKIP:
      for (uns i=0; i<v->skip_chars; i++)
	{
	  bputc(dict_out, 0xf0);
	  bputc(dict_out, stems[v->start]->w[v->level+i]);
	}
      break;
    case VT_TABLE:
      for (uns i=0; i<v->cnt; i++)
	gen_leaf(v, v->start + i);
      break;
    case VT_BRANCH_LARGE:
      bputc(dict_out, 0);
    case VT_BRANCH_SMALL:
      bputc(dict_out, deg);
      for (uns i=0; i<deg; i++)
	{
	  bputc(dict_out, sons[i].split_char);
	  uns pos = vertex_pos[son_ids[i]] - vertex_pos[v->id];
	  if (type == VT_BRANCH_LARGE)
	    bputl(dict_out, pos);
	  else
	    {
	      ASSERT(pos < 0x10000);
	      bputw(dict_out, pos);
	    }
	}
      if (leaves.cnt == 1)
	gen_leaf(v, leaves.start);
      break;
    default:
      ASSERT(0);
    }
  for (uns i=0; i<deg; i++)
    {
      gen_subtree(&sons[i]);
      ASSERT(son_ids[i] == sons[i].id);
    }
}

static void
gen_tree(void)
{
  struct vertex root = { .start = 0, .cnt = num_stems, .level=0 };
  dict_hdr.stem_tree_start = btell(dict_out);
  dict_hdr.stem_tree_length = vertex_pos[num_vertices];
  num_vertices = 0;
  num_son_ids = 0;
  gen_subtree(&root);
  ASSERT(num_vertices == num_vertices_pass1 && num_son_ids == num_son_ids_pass1);
  ASSERT((uns)btell(dict_out) == dict_hdr.stem_tree_start + dict_hdr.stem_tree_length);
  msg(L_INFO, "Written tree as promised");
}

static void
gen_dict(void)
{
  if (!out_dict)
    return;
  dict_out = bopen(out_dict, O_RDWR|O_CREAT|O_TRUNC, 65536);
  strcpy(dict_hdr.charset, charset);
  for (uns i=0; i<num_prefixes; i++)
    {
      strcpy(dict_hdr.prefixes[i], prefix_names[i]);
      dict_hdr.prefix_lengths[i] = prefix_lengths[i];
    }
  dict_hdr.num_prefixes = num_prefixes;
  bwrite(dict_out, &dict_hdr, sizeof(dict_hdr));

  gen_suffixes();
  gen_patterns();
  prepare_tree();
  calc_stem_pos();
  gen_tree();

  brewind(dict_out);
  dict_hdr.magic = STEM_DICT_MAGIC;
  bwrite(dict_out, &dict_hdr, sizeof(dict_hdr));
  bclose(dict_out);
  msg(L_INFO, "Written dictionary to %s", out_dict);
}

/*** Main ***/

static void NONRET
usage(void)
{
  fputs("\
Usage: cat <src> | stem-dict-gen <options>\n\
\n\
Options:\n\
-a\t\tInclude also word forms with stripped accents\n\
-aa\t\tConsider accent-less forms for generation\n\
-c <set>\tSet character set in which the source is encoded\n\
-e <prep>\tRead preprocessed input from a file\n\
-E <out>\tPreprocess only\n\
-f\t\tFold letter case [preprocessing]\n\
-m <len>\tSet maximum suffix length\n\
-M <len>\tSet minimum stem length\n\
-o <dict>\tGenerate a dictionary\n\
-p <px>\t\tAdd a prefix [preprocessing]\n\
-P <patts>\tDump patterns\n\
-s <suffs>\tDump suffixes\n\
-t <stems>\tDump stems\n\
-T <stems>\tDump stem positions\n\
-X <ent>\tSet maximum number of table node entries\n\
", stderr);
  exit(1);
}

int main(int argc, char **argv)
{
  log_init(argv[0]);
  int opt;
  while ((opt = cf_getopt(argc, argv, CF_SHORT_OPTS "ac:e:E:fm:M:o:p:P:s:t:T:X:", CF_NO_LONG_OPTS, NULL)) >= 0)
    switch (opt)
      {
      case 'a':
	strip_accents++;
	break;
      case 'c':
	charset = optarg;
	break;
      case 'e':
	prep_in = optarg;
	break;
      case 'E':
	prep_out = optarg;
	break;
      case 'f':
	fold_case = 1;
	break;
      case 'm':
	if (cf_parse_int(optarg, &max_suffix))
	  usage();
	break;
      case 'M':
	if (cf_parse_int(optarg, &min_stem))
	  usage();
	break;
      case 'o':
	out_dict = optarg;
	break;
      case 'p':
	if (num_prefixes >= MAX_PREFIXES)
	  die("Too many prefixes");
	prefix_lengths[num_prefixes] = strlen(optarg);
	if (num_prefixes >= MAX_PREFIX_LEN)
	  die("Prefix too long");
	strcpy(prefix_names[num_prefixes++], optarg);
	break;
      case 'P':
	out_patterns = optarg;
	break;
      case 's':
	out_suffixes = optarg;
	break;
      case 't':
	out_stems = optarg;
	break;
      case 'T':
	out_stem_pos = optarg;
	break;
      case 'X':
	if (cf_parse_int(optarg, &max_table))
	  usage();
	break;
      default:
	usage();
      }

  msg(L_INFO, "Initializing");
  pool = mp_new(65536);
  suffix_init();
  suffix_lookup("");
  pattern_init();
  stem_init();
  if (strip_accents || fold_case)
    char_table_init();
  lemma_init();

  struct fastbuf *tmp = preprocess(bfdopen_shared(0, 65536));
  msg(L_INFO, "Chewing");
  chew(tmp);
  msg(L_INFO, "Generated %d stems, %d substems, %d suffixes and %d patterns", num_stems, num_substems, num_suffixes, num_patterns);
  msg(L_INFO, "Encountered %d exceptions, %d splits and %d strips", num_exc, num_splits, num_strips);
  msg(L_INFO, "Found %d lemmata + %d variants", num_lemmata, num_variants);
  lemma_cleanup();

  msg(L_INFO, "Postprocessing");
  suffix_gather();
  pattern_gather();
  stem_gather();
  gen_dict();

  return 0;
}
