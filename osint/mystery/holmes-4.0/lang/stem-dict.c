/*
 *  Simple Suffix Dictionary Driven Stemmer
 *
 *  (c) 2005 Martin Mares <mj@ucw.cz>
 *
 *  This stemmer supports all three stemming modes, including lemma
 *  variants and case-insensitive stemming.
 */

#undef LOCAL_DEBUG
#undef DONT_CONVERT

#include "ucw/lib.h"
#include "ucw/clists.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "ucw/unaligned.h"
#include "lang/lang.h"
#include "lang/stemmer.h"
#include "lang/stem-dict.h"
#include "charset/charconv.h"
#include "charset/unicat.h"

#include <string.h>

struct dict_stemmer {
  struct stem_dict_hdr *hdr;
  int charset;
  struct conv_context conv_in, conv_out;
};

#define DICT(t,hdr,p) ((t)((byte *)(hdr) + (p)))
#define OFF(hdr,p) (uns)((byte *)(p) - (byte *)(hdr))

/*** Charset conversion ***/

static uns
stem_dict_conv_in(struct dict_stemmer *ts, const byte *src, byte *dest)
{
  byte *dest_start = dest;
  byte *dest_end = dest + MAX_WORD_BYTES - 4;
#ifdef DONT_CONVERT
  while (*src)
    {
      if (dest >= dest_end)
	return 0;
      *dest++ = *src++;
    }
#else
  struct conv_context *cc = &ts->conv_in;

  for (;;)
    {
      uns u;
      src = utf8_get(src, &u);
      if (!u)
	break;
      if (Uupper(u))
	u = Utolower(u);
      else if (!Ualpha(u))
	return 0;
      if (dest > dest_end)
	return 0;
      if (ts->charset == CONV_CHARSET_UTF8)
	dest = utf8_put(dest, u);
      else
	{
	  int c = conv_ucs_to_out(cc, u);
	  if (c < 0)
	    return 0;
	  *dest++ = c;
	}
    }
#endif
  *dest = 0;
  return dest - dest_start;
}

static void
stem_dict_add_to_wl(struct dict_stemmer *s, byte *w, struct mempool *mp, clist *wl,
		    uns word_form, uns stem_form, uns variant, uns unaccented)
{
#ifndef DONT_CONVERT
  byte y[MAX_WORD_BYTES+1];
  if (s->charset != CONV_CHARSET_UTF8)
    {
      struct conv_context *cc = &s->conv_out;
      cc->source = w;
      cc->source_end = w + strlen(w);
      cc->dest = cc->dest_start = y;
      cc->dest_end = y + sizeof(y) - 1;
      int status = conv_run(cc);
      if (!(status & CONV_SOURCE_END))
	{
	  msg(L_ERROR, "stem-dict output too long");
	  return;
	}
      *cc->dest = 0;
      w = y;
    }
#endif
  word_list_add_unique(mp, wl, w, word_form, stem_form, variant, unaccented);
}

/*** Suffixes ***/

static inline uns
get_suffix_char(struct stem_dict_hdr *h, uns suff, uns i)
{
  u32 *suffix_table = DICT(u32 *, h, h->suffix_table_start);
  uns pos = suffix_table[suff] + i - 1;
  if (pos >= suffix_table[suff+1])
    return 0;
  else
    return *DICT(byte *, h, pos);
}

static inline uns
suff_first_le(struct stem_dict_hdr *h, uns l, uns r, uns pos, uns cc)
{
  while (l < r)
    {
      uns m = (l+r)/2;
      if (get_suffix_char(h, m, pos) < cc)
	l = m + 1;
      else
	r = m;
    }
  return l;
}

static uns
find_suffixes(struct stem_dict_hdr *h, byte *w, uns len, uns *suffixes)
{
  uns pos = 0;
  uns l = 0;
  uns r = h->num_suffixes;
  uns max = MIN(h->max_suffix, len);
  w += len;

  DBG("Searching for suffixes (max=%d):", max);
  for (;;)
    {
      if (!get_suffix_char(h, l, pos+1))
	{
	  DBG("len=%d => %d", pos, l);
	  suffixes[pos++] = l;
	}
      else
	suffixes[pos++] = ~0U;
      if (pos > max)
	return pos;
      uns cc = *--w;
      uns i = suff_first_le(h, l, r, pos, cc);
      // DBG("(%d,%d) '%c'@%d -> %d", l, r, cc, pos, i);
      if (i >= r || get_suffix_char(h, i, pos) != cc)
	return pos;
      l = i;
      r = suff_first_le(h, i, r, pos, cc+1);
    }
}

static void
suffix_tack_on(struct stem_dict_hdr *h, byte *dest, byte *stem, uns suff)
{
  u32 *suffix_table = DICT(u32 *, h, h->suffix_table_start);
  uns pos = suffix_table[suff];
  byte *sf = DICT(byte *, h, pos);
  uns sufflen = suffix_table[suff+1] - pos;
  uns stemlen = strlen(stem);
  memcpy(dest, stem, stemlen);
  for (uns i=0; i<sufflen; i++)
    dest[stemlen+i] = sf[sufflen-1-i];
  dest[stemlen+sufflen] = 0;
}

/*** Patterns ***/

static struct stem_dict_pattern *
pattern_lookup(struct stem_dict_hdr *h, uns patt, uns *len)
{
  ASSERT(patt < h->num_patterns);
  u32 *pattern_table = DICT(u32 *, h, h->pattern_table_start);
  u32 pos = pattern_table[patt];
  u32 pos2 = pattern_table[patt+1];
  *len = (pos2-pos-sizeof(struct stem_dict_pattern)) / sizeof(suffix_id_t);
  return DICT(struct stem_dict_pattern *, h, pos);
}

static int
pattern_match(struct stem_dict_pattern *p, uns len, uns suffix, uns *plemma)
{
  for (uns i=0; i<len; i++)
    if ((uns)(p->suffixes[i] & MAX_SUFFIXES) == suffix)
      {
	*plemma = !!(p->suffixes[i] & SUFFIX_FLAG_LEMMA);
	return 1;
      }
  return 0;
}

/*** Forward searching for stems (stem -> pc) ***/

struct dict_match {
  cnode n;
  byte *stem_pc;			/* starting opcode of the stem */
  byte *ptrs_pc;			/* starting opcode of the pointers */
  byte *stop;
  byte is_lemma;
  byte is_unaccented;
  byte variant;
  byte prefix_set;
};

static byte *
parse_table_entry(struct stem_dict_hdr *h, byte *pc, byte *stop, uns pos, uns len, clist *results, uns *suffixes, uns nsuff, struct mempool *pool, uns prefix_set)
{
  byte *start = pc;
  ASSERT((*pc & 0xc0) == 0x40);
  pc += 1 + (*pc & 0x3f);

 restart: ;
  uns patt = GET_PATTERN_ID(pc), plen, plemma, var;
  pc += sizeof(pattern_id_t);
  if (pc < stop && (*pc & 0xf0) == 0xe0)	/* Extract variant ID */
    var = *pc++ & 0x0f;
  else
    var = 0;
  if (results &&			/* This was a real match */
      len-pos < nsuff &&		/* Suffix available */
      suffixes[len-pos] != ~0U)
    {
      DBG("\tUsing pattern %d", patt);
      struct stem_dict_pattern *p = pattern_lookup(h, patt, &plen);
      if ((p->pattern_flags & (1 << prefix_set)) &&
	  pattern_match(p, plen, suffixes[len-pos], &plemma))
	{
	  struct dict_match *match = mp_alloc(pool, sizeof(*match));
	  clist_add_tail(results, &match->n);
	  match->stem_pc = start;
	  match->ptrs_pc = pc;
	  match->stop = stop;
	  match->is_lemma = plemma;
	  match->is_unaccented = !!(p->pattern_flags & PATTERN_FLAG_NOACCENT);
	  match->variant = var;
	  match->prefix_set = prefix_set;
	  DBG("\tFull match: stem=%x ptrs=%x stop=%x; lemma=%d unacc=%d var=%d pxset=%02x",
	      OFF(h,match->stem_pc), OFF(h,match->ptrs_pc), OFF(h,match->stop),
	      match->is_lemma, match->is_unaccented, match->variant, match->prefix_set);
	}
    }
  while (pc < stop)
    {
      uns op = *pc;
      DBG("\t... %x: op %02x", OFF(h,pc), op);
      if (op == 0xf1)		/* Repeat */
	{
	  start = pc++;
	  goto restart;
	}
      else if (op >= 0x80 && op < 0xe0)	/* main/sub pointer */
	pc += 4;
      else
	break;
    }
  return pc;
}

static void
find_stems_ll(struct stem_dict_hdr *h, byte *w, uns len, uns *suffixes, uns nsuff, struct mempool *pool, clist *results, uns prefix_set)
{
  byte *pc = DICT(byte *, h, h->stem_tree_start);
  byte *stop = pc + h->stem_tree_length;
  uns pos = 0;
  uns go_to, go_to_stop;
  byte *next, *next_stop;

  DBG("Searching for stems for word <%s>, prefix_set=%02x:", w, prefix_set);
  while (pc < stop)
    {
      uns op = *pc;
      DBG("%x: op %02x, <%.*s|%.*s>", OFF(h,pc), op, pos, w, len-pos, w+pos);
      if (!op)			/* large branching */
	{
	  uns deg = pc[1];
	  DBG("\tLarge branching of degree %d", deg);
	  byte *tab = pc+2;
	  uns l = 0, r = deg;
	  go_to = go_to_stop = 0;
	  if (pos < len)
	    {
	      while (l < r)
		{
		  uns m = (l+r)/2;
		  if (w[pos] == tab[5*m])
		    {
		      go_to = GET_U32(tab+5*m+1);
		      if (m+1 < deg)
			go_to_stop = GET_U32(tab+5*m+6);
		      DBG("\tMatched branch %d, goto +%d stop +%d", m, go_to, go_to_stop);
		      break;
		    }
		  if (w[pos] < tab[5*m])
		    r = m;
		  else
		    l = m+1;
		}
	    }
	  next = tab + 5*deg;
	  next_stop = pc + GET_U32(tab+1);
	  goto branching;
	}
      else if (op < 0x40)		/* small branching */
	{
	  uns deg = op & 0x3f;
	  DBG("\tSmall branching of degree %d", deg);
	  byte *tab = pc+1;
	  uns l = 0, r = deg;
	  go_to = go_to_stop = 0;
	  if (pos < len)
	    {
	      while (l < r)
		{
		  uns m = (l+r)/2;
		  if (w[pos] == tab[3*m])
		    {
		      go_to = GET_U16(tab+3*m+1);
		      if (m+1 < deg)
			go_to_stop = GET_U16(tab+3*m+4);
		      DBG("\tMatched branch %d, goto +%d stop +%d", m, go_to, go_to_stop);
		      break;
		    }
		  if (w[pos] < tab[3*m])
		    r = m;
		  else
		    l = m+1;
		}
	    }
	  next = tab + 3*deg;
	  next_stop = pc + GET_U16(tab+1);
	branching:
	  if (next < next_stop && *next == 0x40)
	    {
	      DBG("\tMatched leaf");
	      parse_table_entry(h, next, next_stop, pos, len, results, suffixes, nsuff, pool, prefix_set);
	    }
	  pos++;
	  if (!go_to)
	    return;
	  if (go_to_stop)
	    stop = pc + go_to_stop;
	  pc += go_to;
	  DBG("\tGoto %x, stop %x", OFF(h,pc), OFF(h,stop));
	}
      else if (op < 0x80)		/* table */
	{
	  uns elen = op & 0x3f;
	  DBG("\tTable entry <%.*s>", elen, pc+1);
	  if (pos + elen <= len && !memcmp(pc+1, w+pos, elen))
	    {
	      DBG("\tMatched <%.*s>", pos+elen, w);
	      pc = parse_table_entry(h, pc, stop, pos+elen, len, results, suffixes, nsuff, pool, prefix_set);
	    }
	  else
	    {
	      DBG("\tNo match, skipping");
	      pc = parse_table_entry(h, pc, stop, 0, 0, NULL, NULL, 0, NULL, 0);
	    }
	}
      else if (op == 0xf0)		/* skip */
	{
	  if (pos >= len || w[pos++] != pc[1])
	    return;
	  pc += 2;
	}
      else
	die("Malformed stemming dictionary: unrecognized opcode %02x", op);
    }
  DBG("%x: STOP", OFF(h,pc));
}

static void
find_stems_recurse(struct stem_dict_hdr *h, byte *w, uns len, uns *suffixes, uns nsuff, struct mempool *pool, clist *results, uns prefix, uns prefix_set)
{
  if (prefix >= h->num_prefixes)
    return find_stems_ll(h, w, len, suffixes, nsuff, pool, results, prefix_set);
  else
    {
      uns pxlen = h->prefix_lengths[prefix];
      DBG("Trying prefix <%s> len %d", h->prefixes[prefix], pxlen);
      if (pxlen <= len && !memcmp(h->prefixes[prefix], w, pxlen))
	find_stems_recurse(h, w+pxlen, len-pxlen, suffixes, MIN(len-pxlen, nsuff), pool, results, prefix+1, prefix_set | (1 << prefix));
      find_stems_recurse(h, w, len, suffixes, nsuff, pool, results, prefix+1, prefix_set);
    }
}

static clist *
find_stems(struct stem_dict_hdr *h, byte *w, uns len, uns *suffixes, uns nsuff, struct mempool *pool)
{
  clist *results = mp_alloc(pool, sizeof(*results));
  clist_init(results);
  find_stems_recurse(h, w, len, suffixes, nsuff, pool, results, 0, 0);
  return results;
}

/*** Operations with main/sub stem pointers ***/

static byte *
get_main_entry(struct stem_dict_hdr *h, struct dict_match *m)
{
  byte *pc = m->ptrs_pc;
  if (pc < m->stop && *pc >= 0xc0 && *pc < 0xe0)
    {
      uns x = ((*pc & 0x3f) << 24) | (pc[1] << 16) | (pc[2] << 8) | pc[3];
      DBG("\tRedirected to %x", x);
      return DICT(byte *, h, h->stem_tree_start + x);
    }
  else
    return m->stem_pc;
}

/*** Reverse searching for stems (pc -> stem) ***/

static byte *
construct_stem(struct stem_dict_hdr *h, byte *stem_pc, byte **pstop, byte *stem, uns prefix_set)
{
  byte *pc = DICT(byte *, h, h->stem_tree_start);
  byte *stop = pc + h->stem_tree_length;
  byte *next, *next_stop, *last_stem=NULL;
  byte * UNUSED stem_start = stem;

  DBG("Constructing stem at position %x with pxset %02x:", OFF(h,stem_pc), prefix_set);
  for (uns px=0; px<h->num_prefixes; px++)
    if (prefix_set & (1 << px))
      {
	DBG("Prefix <%s>", h->prefixes[px]);
	memcpy(stem, h->prefixes[px], h->prefix_lengths[px]);
	stem += h->prefix_lengths[px];
      }

  while (pc < stop)
    {
      uns op = *pc;
      DBG("%x: op %02x", OFF(h,pc), op);
      if (!op)			/* large branching */
	{
	  uns deg = pc[1];
	  DBG("\tLarge branching of degree %d", deg);
	  byte *tab = pc+2;
	  next = tab + 5*deg;
	  next_stop = pc + GET_U32(tab+1);
	  if (stem_pc >= next && stem_pc < next_stop)
	    {
	      pc = next;
	      stop = next_stop;
	    }
	  else
	    {
	      uns l = 0, r = deg;
	      while (l < r)
		{
		  uns m = (l+r)/2;
		  byte *p = pc + GET_U32(tab+5*m+1);
		  if (stem_pc < p)
		    r = m;
		  else
		    l = m + 1;
		}
	      ASSERT(l);
	      l--;
	      DBG("\tGoing through branch %d ('%c')", l, tab[5*l]);
 	      *stem++ = tab[5*l];
	      if (l < deg-1)
		stop = pc + GET_U32(tab+5*(l+1)+1);
	      pc += GET_U32(tab+5*l+1);
	      DBG("\tGoto %x, stop %x", OFF(h,pc), OFF(h,stop));
	    }
	}
      else if (op < 0x40)		/* small branching */
	{
	  uns deg = op & 0x3f;
	  DBG("\tSmall branching of degree %d", deg);
	  byte *tab = pc+1;
	  next = tab + 3*deg;
	  next_stop = pc + GET_U16(tab+1);
	  if (stem_pc >= next && stem_pc < next_stop)
	    {
	      pc = next;
	      stop = next_stop;
	    }
	  else
	    {
	      uns l = 0, r = deg;
	      while (l < r)
		{
		  uns m = (l+r)/2;
		  byte *p = pc + GET_U16(tab+3*m+1);
		  if (stem_pc < p)
		    r = m;
		  else
		    l = m + 1;
		}
	      ASSERT(l);
	      l--;
	      DBG("\tGoing through branch %d ('%c')", l, tab[3*l]);
 	      *stem++ = tab[3*l];
	      if (l < deg-1)
		stop = pc + GET_U16(tab+3*(l+1)+1);
	      pc += GET_U16(tab+3*l+1);
	      DBG("\tGoto %x, stop %x", OFF(h,pc), OFF(h,stop));
	    }
	}
      else if (op < 0x80)		/* table */
	{
	  uns elen = op & 0x3f;
	  last_stem = pc;
	  DBG("\tTable entry <%.*s>", elen, pc+1);
	  if (pc == stem_pc)
	    {
	      memcpy(stem, pc+1, elen);
	      stem[elen] = 0;
	      *pstop = stop;
	      pc += 1+elen;
	      DBG("\tFOUND <%s> at %x", stem_start, OFF(h,pc));
	      return pc;
	    }
	  pc += 1+elen+2;
	}
      else if (op < 0xe0)		/* main/sub ptr */
	pc += 4;
      else if (op < 0xf0)		/* variant ID */
	pc++;
      else if (op == 0xf0)		/* skip */
	{
	  *stem++ = pc[1];
	  pc += 2;
	}
      else if (op == 0xf1)		/* repeat */
	{
	  ASSERT(last_stem);
	  if (pc == stem_pc)
	    {
	      uns elen = *last_stem & 0x3f;
	      memcpy(stem, last_stem+1, elen);
	      stem[elen] = 0;
	      *pstop = stop;
	      pc++;
	      DBG("\tFOUND repeat <%s> at %x", stem_start, OFF(h,pc));
	      return pc;
	    }
	  pc += 3;
	}
      else
	die("Malformed stemming dictionary: unrecognized opcode %02x", op);
    }
  die("Malformed stemming dictionary: unable to reconstruct stem for position %x", OFF(h,stem_pc));
}

clist *
stem_dict(struct stemmer *st, struct word_node *req, struct mempool *mp)
{
  struct dict_stemmer *s = st->priv;
  struct stem_dict_hdr *h = s->hdr;

  byte in[MAX_WORD_BYTES+1];
  uns len;
  if (!(len = stem_dict_conv_in(s, req->w, in)))
    return NULL;
  DBG("Analysing word <%s>", in);

  clist *res = mp_alloc(mp, sizeof(*res));
  clist_init(res);

  uns suffixes[MAX_WORD_BYTES+1];
  uns nsuff = find_suffixes(h, in, len, suffixes);
  clist *matches = find_stems(h, in, len, suffixes, nsuff, mp);

  struct dict_match *m;
  CLIST_WALK(m, *matches)
    {
      DBG("Processing match at %x for stem %x", OFF(h,m->ptrs_pc), OFF(h,m->stem_pc));
      if (req->word_form == WORD_FORM_LEMMA && (!m->is_lemma || m->variant != req->variant) ||
	  !req->unaccented && m->is_unaccented)
	continue;

      byte *pc = get_main_entry(h, m);
      byte stem[MAX_WORD_BYTES+1], buf[MAX_WORD_BYTES+1], *stop;
      pc = construct_stem(h, pc, &stop, stem, m->prefix_set);
      DBG("Constructed stem <%s>", stem);
      uns patt, pi, plen;
      struct stem_dict_pattern *p;

      if (req->stem_form == WORD_FORM_LEMMA)
	{
	  patt = GET_PATTERN_ID(pc);
	  DBG("Going to use pattern %d", patt);
	  p = pattern_lookup(h, patt, &plen);
	  for (pi=0; pi<plen && !(p->suffixes[pi] & SUFFIX_FLAG_LEMMA); pi++)
	    ;
	  ASSERT(pi < plen);
	  suffix_tack_on(h, buf, stem, p->suffixes[pi] & MAX_SUFFIXES);
	  DBG("Lemma <%s>", buf);
	  stem_dict_add_to_wl(s, buf, mp, res,
			      (m->is_lemma ? WORD_FORM_LEMMA : WORD_FORM_OTHER),
			      WORD_FORM_LEMMA,
			      m->variant,
			      m->is_unaccented);
	}
      else
	{
	  byte *ppc, *pstop;
	  ppc = pc; pstop = stop;
	  pc += sizeof(pattern_id_t);
	  if (pc < stop && *pc >= 0xe0 && *pc < 0xf0)
	    pc++;
	  for (;;)
	    {
	      patt = GET_PATTERN_ID(ppc);
	      DBG("Going to use pattern %d", patt);
	      p = pattern_lookup(s->hdr, patt, &plen);
	      for (pi=0; pi<plen; pi++)
		{
		  suffix_tack_on(s->hdr, buf, stem, p->suffixes[pi] & MAX_SUFFIXES);
		  DBG("Generated <%s>", buf);
		  stem_dict_add_to_wl(s, buf, mp, res,
				      (m->is_lemma ? WORD_FORM_LEMMA : WORD_FORM_OTHER),
				      ((p->suffixes[pi] & SUFFIX_FLAG_LEMMA) ? WORD_FORM_LEMMA : WORD_FORM_OTHER),
				      0,
				      m->is_unaccented || (p->pattern_flags & PATTERN_FLAG_NOACCENT));
		}
	      if (pc >= pstop || *pc < 0x80 || *pc >= 0xc0)
		break;
	      uns pos = (((*pc & 0x3f) << 24) | (pc[1] << 16) | (pc[2] << 8) | pc[3]) + h->stem_tree_start;
	      pc += 4;
	      DBG("Sub-stem at %x", pos);
	      ppc = DICT(byte *, h, pos);
	      ppc = construct_stem(h, ppc, &stop, stem, m->prefix_set);
	    }

	}
    }
  return res;
}

void
stem_init_dict(struct stemmer *st)
{
  struct dict_stemmer *ds = xmalloc_zero(sizeof(*ds));
  st->priv = ds;
  uns len;
  ds->hdr = mmap_file(st->params, &len, 0);
  DBG("Loading dictionary %s", st->params);
  if (len < sizeof(struct stem_dict_hdr) ||
      ds->hdr->magic != STEM_DICT_MAGIC)
    die("Stemmer dictionary %s has an invalid header", st->params);
  ds->charset = find_charset_by_name(ds->hdr->charset);
  if (ds->charset < 0)
    die("Stemmer dictionary %s built for an unknown charset %s", st->params, ds->hdr->charset);
  conv_init(&ds->conv_in);
  conv_set_charset(&ds->conv_in, CONV_CHARSET_UTF8, ds->charset);
  conv_init(&ds->conv_out);
  conv_set_charset(&ds->conv_out, ds->charset, CONV_CHARSET_UTF8);
  DBG("Loaded");
}
