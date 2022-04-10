/*
 *	Sherlock Search Engine -- Spelling Checker
 *
 *	(c) 1997-2005 Martin Mares <mj@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/unicode.h"
#include "ucw/conf.h"
#include "charset/unicat.h"
#include "search/sherlockd.h"
#include "search/lexicon.h"

#include <string.h>
#include <alloca.h>

#define IS_TRACING (current_query->debug & DEBUG_WORDS)
#define TRACE(msg...) do { if (IS_TRACING) add_reply(msg); } while(0)

#ifdef CONFIG_SPELL

#define HARD_MAX_SPELLS 16
#define HARD_MAX_PHRASE_DIF 4

enum spell_found {
  SPELL_FOUND_ADD,
  SPELL_FOUND_DEL,
  SPELL_FOUND_MOD,
  SPELL_FOUND_XPOS,
  SPELL_FOUND_PHRASE,
  SPELL_FOUND_KB_TRAN,
};

struct spell_best {
  uns id, unacc_id;
  int pts;
};

struct spell_phrase_item {
  cnode n;
  byte *src, *dest;
  uns len, bytes, penalty;
  int dif;
};

struct spell_phrase_group {
  cnode n;
  clist items;
  int dif;
};

struct spell_kb_node {
  uns x, y;
};

static struct spell_best spell_best[HARD_MAX_SPELLS];
static uns spell_max, spell_n, spell_threshold, spell_accent_mode;
static uns *spell_known_vars, spell_n_known_vars;
static byte *spell_wa;
static struct spell_phrase_item *spell_phrase;
static struct spell_kb_tran *spell_kb_tran;
static clist spell_phrase_groups;

#define HASH_PREFIX(x) spell_kb_##x
#define HASH_NODE struct spell_kb_node
#define HASH_KEY_ATOMIC x
#define HASH_TABLE_DYNAMIC
#define HASH_ZERO_FILL
#define HASH_WANT_LOOKUP
#define HASH_WANT_FIND
#include "ucw/hashtable.h"

void
spell_init(void)
{
  struct spell_phrase_group *groups[HARD_MAX_PHRASE_DIF * 2 + 1];
  bzero(groups, sizeof(groups));
  clist_init(&spell_phrase_groups);
  CLIST_FOR_EACH(struct spell_phrase *, p, spell_phrases)
    {
      struct spell_phrase_item *item = cf_malloc(sizeof(*item));
      item->src = p->src;
      item->dest = p->dest;
      item->penalty = p->penalty;
      item->bytes = strlen(p->src);
      item->len = utf8_strlen(p->src);
      item->dif = (int)utf8_strlen(p->dest) - (int)item->len;
      if (item->dif < -HARD_MAX_PHRASE_DIF || item->dif > HARD_MAX_PHRASE_DIF)
	die("Too large lengths difference between spell phrases '%s' and '%s'", p->src, p->dest);
      uns i = item->dif + HARD_MAX_PHRASE_DIF;
      if (!groups[i])
        {
	  groups[i] = cf_malloc(sizeof(*groups[i]));
	  clist_add_tail(&spell_phrase_groups, &groups[i]->n);
	  clist_init(&groups[i]->items);
	  groups[i]->dif = item->dif;
	}
      clist_add_tail(&groups[i]->items, &item->n);
    }
  CLIST_FOR_EACH(struct spell_kb_tran *, t, spell_kb_trans)
    {
      struct spell_kb_table *table = t->table = cf_malloc_zero(sizeof(*table));
      spell_kb_init(table);
      CLIST_FOR_EACH(struct spell_pair *, p, t->pairs)
        {
	  struct spell_kb_node *node = spell_kb_lookup(table, p->x);
	  if (node->y)
	    die("Found a duplicate source character 0x%x in Search.SpellKBTranslation.Pairs", p->x);
	  node->y = p->y;
	}
    }
}

static inline int
spell_class_ok(enum word_class class)
{
  return (class == WC_NORMAL || class == WC_CONTEXT);
}

static inline int
spell_diff_char(uns x, uns y)
{
  /* Calculate how much do two characters differ */
  if (x == y)
    return 0;
  x = Uunaccent(x);
  y = Uunaccent(y);
  if (x == y)
    return spell_accent_penalty;
  CLIST_FOR_EACH(struct spell_pair *, p, spell_common_pairs)
    if (x == p->x && y == p->y || x == p->y && y == p->x)
      return spell_common_penalty;
  return spell_mod_penalty;
}

static int PURE
spell_differs(struct lex_entry *l, uns li, uns oi, uns len)
{
  /* Calculate how much does a fragment of the original word and a fragment of the match differ */
  int diff = 0;
  byte *ow, *lw, *le;
  uns ou, lu;

  lw = l->w;
  le = lw + l->length;
  while (li--)
    UTF8_SKIP(lw);
  ow = spell_wa;
  while (oi--)
    UTF8_SKIP(ow);
  while (len-- && lw < le)
    {
      lw = utf8_get(lw, &lu);
      ow = utf8_get(ow, &ou);
      diff += spell_diff_char(lu, ou);
    }
  return diff;
}

static void
spell_found(uns id, uns unacc_id, uns pos, enum spell_found found_what)
{
  /* Word has been found by the spelling checker without accents. */
  struct lex_entry *l = lex_get(id);
  uns i;
  int pts;
  byte *msg;

  if (l->freq < spell_threshold)
    {
      msg = "freq < threshold";
      pts = 0;
      goto done;
    }
  if (!spell_class_ok(l->class))
    {
      msg = "wrong class";
      pts = 0;
      goto done;
    }

  /* Calculate similarity points */
  pts = l->freq * 100;
  switch (found_what)
    {
    case SPELL_FOUND_ADD:
      pts -= spell_add_penalty + spell_differs(l, 0, 0, pos-1) + spell_differs(l, pos, pos-1, ~0U);
      break;
    case SPELL_FOUND_DEL:
      pts -= spell_del_penalty + spell_differs(l, 0, 0, pos) + spell_differs(l, pos, pos+1, ~0U);
      break;
    case SPELL_FOUND_MOD:
      pts -= spell_differs(l, 0, 0, ~0U);
      break;
    case SPELL_FOUND_XPOS:
      pts -= spell_xpos_penalty + spell_differs(l, 0, 0, pos-2) + spell_differs(l, pos, pos, ~0U);
      break;
    case SPELL_FOUND_PHRASE:
      pts -= spell_phrase->penalty + spell_differs(l, 0, 0, pos-spell_phrase->len-spell_phrase->dif) + spell_differs(l, pos, pos-spell_phrase->dif, ~0U);
      break;
    case SPELL_FOUND_KB_TRAN:
      pts -= spell_kb_tran->penalty;
      break;
    }

  if (spell_n == spell_max && spell_best[spell_n-1].pts >= pts)
    msg = "clearly pessimal";
  else
    {
      for (i=0; i<spell_n_known_vars; i++)
	if (spell_known_vars[i] == id)
	  {
	    msg = "found in expansions";
	    goto done;
	  }
      msg = "recorded";
      for (i=0; i<spell_n; i++)
	if (spell_best[i].id == id ||
	    spell_accent_mode != 2 && spell_best[i].unacc_id == unacc_id)
	  {
	    if (pts < spell_best[i].pts)
	      {
		msg = "kept old match";
		goto done;
	      }
	    memmove(&spell_best[i], &spell_best[i+1], sizeof(struct spell_best) * (spell_n - i - 1));
	    spell_n--;
	    msg = "replaced old match";
	    break;
	  }
      for (i=0; i<spell_n && spell_best[i].pts > pts; i++)
	;
      memmove(&spell_best[i+1], &spell_best[i], sizeof(struct spell_best) * (spell_n - i + (spell_n < spell_max)));
      if (spell_n < spell_max)
	spell_n++;
      spell_best[i].id = id;
      spell_best[i].unacc_id = unacc_id;
      spell_best[i].pts = pts;
    }
 done:
  if (IS_TRACING)
    {
      byte buf[MAX_WORD_BYTES+1];
      lex_extract(id, buf);
      add_reply(".Z <%s> freq=%d pts=%d: %s", buf, l->freq, pts, msg);
    }
#ifdef LOCAL_DEBUG
  byte buf[MAX_WORD_BYTES+1];
  lex_extract(id, buf);
  DBG("Found <%s> freq=%d pts=%d what=%d@%d: %s", buf, l->freq, pts, found_what, pos, msg);
#endif
}

static void
spell_extract_suffix(int id, uns prefix_len, byte *buf)
{
  struct lex_entry *l = lex_get(id);
  byte *w = l->w;
  byte *we = l->w + l->length;
  uns u;
  while (prefix_len--)
    UTF8_SKIP(w);
  while (w < we)
    {
      w = utf8_get(w, &u);
      u = Uunaccent(u);
      buf = utf8_put(buf, u);
    }
  *buf = 0;
}

static void
spell_check_rest(int l, int r, uns prefix_len, byte *suffix, enum spell_found found_what)
{
  /* Given an interval [l,r] of words with a common prefix, process
   * all words which are equal to suffix after removing the prefix.
   */
  int r0 = r;
  byte buf[MAX_WORD_BYTES+1];
  DBG("\t\t\tSearching [%d,%d] for <%s>, prefix_len=%d", l, r, suffix, prefix_len);
  while (l < r)		     /* Invariant: first word lies in [l,r] */
    {
      int m = (l+r)/2;
      spell_extract_suffix(m, prefix_len, buf);
      if (strcmp(buf, suffix) < 0)
	l = m+1;
      else
	r = m;
    }
  int l0 = l;
  while (l <= r0)
    {
      spell_extract_suffix(l, prefix_len, buf);
      if (strcmp(buf, suffix))
	break;
      spell_found(l, l0, prefix_len, found_what);
      l++;
    }
}

static inline uns
spell_nth_char(int id, uns n)
{
  byte *w = lex_get(id)->w;
  for (uns i=0; i<n; i++)
    UTF8_SKIP(w);
  uns u;
  w = utf8_get(w, &u);
  return Uunaccent(u);
}

static int
spell_find_char(int l, int r, uns prefix_len, uns c)
{
  /* Given an interval [l,r] of words with a common prefix, find the
   * first word with the next character after the prefix >= c. Uses modified
   * binary search which combines doubling and halving to get better
   * performance on short distances.
   */
  int s = 1;
  while (l+s <= r && spell_nth_char(l+s, prefix_len) < c)
    s += s;
  /* Invariant: Result lies in [l,l+s], l+s might be out of range */
  while (s > 1)
    {
      s /= 2;
      if (l+s <= r && spell_nth_char(l+s, prefix_len) < c)
	l += s;
    }
  if (l <= r && spell_nth_char(l, prefix_len) < c)
    l++;
  return l;
}

static inline void
spell_restrict(int *ll, int *rr, uns prefix_len, uns nextc)
{
  /* Restrict interval [l,r] of words with a common prefix to those having
   * the prefix followed by char nextc.
   */
  *ll = spell_find_char(*ll, *rr, prefix_len, nextc);
  *rr = spell_find_char(*ll, *rr, prefix_len, nextc+1) - 1;
}

static inline int
spell_skip_char(int l, uns prefix_len, int r)
{
  /* Find the first word in interval [l,r] of words with a common prefix
   * which differs just after the prefix; r+1 if no such word exists.
   */
  uns u = spell_nth_char(l, prefix_len);
  return spell_find_char(l, r, prefix_len, u+1);
}

static void
spell_check_del(byte *wu, uns len)
{
  int l = current_dbase->lex_by_len[len-1];
  int r = current_dbase->lex_by_len[len]-1;
  DBG("check_del: length=%d, interval=[%d,%d]", len-1, l, r);
  for (uns i=0; i<len && l<=r; i++)
    {
      uns u;
      wu = utf8_get(wu, &u);
      DBG("\ti=%d (char %x) [%d,%d]", i, u, l, r);
      spell_check_rest(l, r, i, wu, SPELL_FOUND_DEL);
      spell_restrict(&l, &r, i, u);
    }
}

static void
spell_check_add(byte *wu, uns len)
{
  int l = current_dbase->lex_by_len[len+1];
  int r = current_dbase->lex_by_len[len+2]-1;
  DBG("check_add: length=%d, interval=[%d,%d]", len+1, l, r);
  for (uns i=0; i<=len && l<=r; i++)
    {
      byte *c = wu;
      uns u;
      wu =  utf8_get(wu, &u);
      DBG("\ti=%d (char %x) [%d,%d]", i, u, l, r);
      int l1=l, r1;
      while (l1 <= r)
	{
	  r1 = spell_skip_char(l1, i, r);
	  spell_check_rest(l1, r1-1, i+1, c, SPELL_FOUND_ADD);
	  l1 = r1;
	}
      if (u)
	spell_restrict(&l, &r, i, u);
    }
}

static void
spell_check_mod(byte *wu, uns len)
{
  int l = current_dbase->lex_by_len[len];
  int r = current_dbase->lex_by_len[len+1]-1;
  DBG("check_mod: length=%d, interval=[%d,%d]", len, l, r);
  for (uns i=0; i<len; i++)
    {
      uns u;
      wu = utf8_get(wu, &u);
      DBG("\ti=%d (char %x) [%d,%d]", i, u, l, r);
      int l1=l, r1;
      while (l1 <= r)
	{
	  r1 = spell_skip_char(l1, i, r);
	  spell_check_rest(l1, r1-1, i+1, wu, SPELL_FOUND_MOD);
	  l1 = r1;
	}
      spell_restrict(&l, &r, i, u);
    }
}

static void
spell_check_xpos(byte *wu, uns len)
{
  int l = current_dbase->lex_by_len[len];
  int r = current_dbase->lex_by_len[len+1]-1;
  DBG("check_xpos: length=%d, interval=[%d,%d]", len, l, r);
  for (uns i=0; i<len-1; i++)
    {
      byte *t;
      uns u1, u2;
      t = wu;
      t = utf8_get(t, &u1);
      t = utf8_get(t, &u2);
      t = wu;
      t = utf8_put(t, u2);
      t = utf8_put(t, u1);
      spell_check_rest(l, r, i, wu, SPELL_FOUND_XPOS);
      t = wu;
      t = utf8_put(t, u1);
      wu = t;
      t = utf8_put(t, u2);
      spell_restrict(&l, &r, i, u1);
    }
}

static void
spell_check_phrases(byte *wu, uns len)
{
  int la[len + 1], ra[len + 1];
  byte *wa[len + 1];
  CLIST_FOR_EACH(struct spell_phrase_group *, g, spell_phrase_groups)
    {
      if ((int)len <= -g->dif)
        continue;
      la[0] = current_dbase->lex_by_len[len + g->dif];
      ra[0] = current_dbase->lex_by_len[len + g->dif + 1]-1;
      wa[0] = wu;
      DBG("check_phrases: length=%d, dif=%d, interval=[%d,%d]", len, g->dif, la[0], ra[0]);
      for (uns i = 1; i <= len; i++)
        {
          uns u1;
          wu = utf8_get(wu, &u1);
          wa[i] = wu;
          la[i] = la[i - 1];
          ra[i] = ra[i - 1];
          spell_restrict(la + i, ra + i, i - 1, u1);
          CLIST_FOR_EACH(struct spell_phrase_item *, p, g->items) /* not optimal, but fast enough for a small number of short phrases */
	    if (i >= p->len)
              {
	        uns j = i - p->len;
	        if (p->bytes == (uns)(wu - wa[j]) && !memcmp(p->src, wa[j], p->bytes))
	          {
	            uns l = la[j], r = ra[j];
	            byte *d = p->dest;
		    for (; l <= r; j++)
		      {
		        uns u2;
		        d = utf8_get(d, &u2);
		        if (!u2)
		          {
			    spell_phrase = p;
	                    spell_check_rest(l, r, j, wu, SPELL_FOUND_PHRASE);
		            break;
		          }
		        spell_restrict(&l, &r, j, u2);
		      }
	          }
	      }
        }
      wu = wa[0];
    }
}

static void
spell_check_kb_trans(byte *wu, uns len UNUSED)
{
  byte buf[MAX_WORD_BYTES + 4];
  CLIST_FOR_EACH(struct spell_kb_tran *, t, spell_kb_trans)
    {
      byte *d = buf;
      uns found = 0;
      for (const byte *s = wu; *s; )
        {
	  uns u;
	  s = utf8_get(s, &u);
	  struct spell_kb_node *n = spell_kb_find(t->table, u);
	  if (n)
	    {
	      u = Uunaccent(n->y);
	      found = 1;
	    }
	  d = utf8_put(d, u);
	  if (d >= buf + MAX_WORD_BYTES)
	    goto next;
	}
      *d = 0;
      if (found)
        {
	  int l = current_dbase->lex_by_len[len];
	  int r = current_dbase->lex_by_len[len + 1] - 1;
          DBG("check_kb_trans: `%s', interval=[%d,%d]", buf, l, r);
	  spell_kb_tran = t;
          spell_check_rest(l, r, 0, buf, SPELL_FOUND_KB_TRAN);
	}
next: ;
    }
}

static void
spell_word(uns idx)
{
  struct word *w = current_query->words[idx];

  /* Calculate frequency and set thresholds */
  uns freq = 0;
  SLIST_FOR_EACH(struct variant *, v, w->variants)
    freq = MAX(freq, lex_get(v->lex_id)->freq);
  if (freq >= spell_good_freq || freq + spell_margin > 256)
    {
      TRACE(".Z Spelling <%s> (freq %d): too frequent to consider", w->word, freq);
      return;
    }
  if (freq >= spell_dwarf)
    spell_threshold = freq + spell_margin;
  else
    spell_threshold = spell_dwarf_margin;

  /* Prepare unaccented word and check length */
  spell_wa = w->word;
  byte wu[MAX_WORD_BYTES+3];
  if (!word_unaccent_utf8(spell_wa, wu))
    return;
  uns len = utf8_strlen(wu);
  if (len < spell_min_len)
    return;
  spell_accent_mode = w->options.accent_mode;

  TRACE(".Z Spelling <%s> (freq %d): with freq_threshold=%d and accent_mode=%d", wu, freq, spell_threshold, spell_accent_mode);

  /* Check all possible variants with edit distance 1 */
  spell_check_del(wu, len);
  spell_check_add(wu, len);
  spell_check_mod(wu, len);
  /* Check transpositions */
  spell_check_xpos(wu, len);
  /* Check phrases */
  spell_check_phrases(wu, len);
  /* Check keyboard layouts */
  if (spell_accent_mode)
    spell_check_kb_trans(wu, len);

  /* If the word is a dwarf, we should consider restarting the search with a lower threshold
   * if there are no matches, but we'll better do it in a single pass by clever filtering.
   */
  if (freq < spell_dwarf && spell_n > 0)
    {
      uns r = 0, w = 0;
      while (r < spell_n)
	{
	  if (lex_get(spell_best[r].id)->freq >= freq + spell_margin)
	    spell_best[w++] = spell_best[r];
	  r++;
	}
      if (w > 0)
	spell_n = w;
    }
}

void
spell_check(struct query *q)
{
  uns i, j, spell_requested=0;

  /* Prepare list of IDs of all known variants */
  spell_n_known_vars = 0;
  for (i=0; i<q->nwords; i++)
    {
      struct word *w = q->words[i];
      if (!w->is_string && spell_class_ok(w->word_class))
	{
	  spell_n_known_vars += w->var_count;
	  spell_requested += w->options.spelling;
	}
    }
  if (!spell_requested)
    return;
  spell_known_vars = alloca(sizeof(uns) * spell_n_known_vars);
  j = 0;
  for (i=0; i<q->nwords; i++)
    {
      struct word *w = q->words[i];
      if (!w->is_string && spell_class_ok(w->word_class))
	SLIST_FOR_EACH(struct variant *, v, w->variants)
	  spell_known_vars[j++] = v->lex_id;
    }
  ASSERT(j == spell_n_known_vars);

  /* Check all words from the query */
  for (i=0; i<q->nwords; i++)
    {
      struct word *w = q->words[i];
      if (!w->is_string &&
	  w->use_count > w->hide_count && /* FIXME: Beware of WC_CONTEXT which can have use_count==0 after complexification */
	  spell_class_ok(w->word_class) &&
	  !w->is_wild &&
	  w->options.spelling)
	{
	  spell_max = MIN(w->options.spelling, HARD_MAX_SPELLS);
	  spell_n = 0;
	  spell_word(i);
	  for (j=0; j<spell_n; j++)
	    {
	      byte wbuf[MAX_WORD_BYTES+1];
	      lex_extract(spell_best[j].id, wbuf);
	      add_reply("S%s %s %d", wbuf, w->word, spell_best[j].pts);
	    }
	}
    }
}

#else

void spell_init(void)
{
}

void spell_check(struct query *q UNUSED)
{
}

#endif
