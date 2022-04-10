/*
 *  Simple Table-Driven Stemmer
 *
 *  (c) 2005 Martin Mares <mj@ucw.cz>
 *
 *  This stemmer supports all three stemming modes, but it's always case
 *  sensitive and it doesn't report lemma variants.
 */

#undef LOCAL_DEBUG

#include "ucw/lib.h"
#include "ucw/mempool.h"
#include "ucw/fastbuf.h"
#include "ucw/unicode.h"
#include "lang/lang.h"
#include "lang/stemmer.h"
#include "lang/stem-table.h"
#include "charset/charconv.h"
#include "charset/unicat.h"

#include <fcntl.h>
#include <string.h>

struct table_stemmer {
  struct stem_table_hdr *hdr;
  int charset;
  struct conv_context conv_in, conv_out;
};

static uns
stem_table_conv_in(struct table_stemmer *ts, const byte *src, byte *dest)
{
  struct conv_context *cc = &ts->conv_in;

  for (;;)
    {
      uns u;
      src = utf8_get(src, &u);
      if (!u)
	break;
      if (Uupper(u))
	u = Utolower(u);
      else if (!Ulower(u))
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
  *dest = 0;
  return 1;
}

static inline void *
st_go(struct table_stemmer *ts, u32 pos)
{
  return ((byte *) ts->hdr) + pos;
}

static inline uns
st_pos(struct table_stemmer *ts, void *p)
{
  return (byte *) p - (byte *) ts->hdr;
}

static inline struct stem_table_vertex *
stem_table_follow_edge(struct table_stemmer *ts, struct stem_table_vertex *v, u32 label)
{
  uns hs = v->edge_hash_size & 0xffffff;
  if (!hs)
    return NULL;
  uns h = label % hs;
  uns h0 = h;
  while (v->edges[h].label)
    {
      if (v->edges[h].label == label)
	return st_go(ts, v->edges[h].dest);
      h++;
      if (h == hs)
	h = 0;
      if (h == h0)
	break;
    }
  return NULL;
}

static struct stem_table_vertex *
stem_table_lookup_vertex(struct table_stemmer *ts, byte *w)
{
  struct stem_table_vertex *v = st_go(ts, ts->hdr->root);
  DBG("Searching for vertex with label <%s>, root at %x:", w, st_pos(ts, v));
  while (*w)
    {
      u32 label = 0;
      for (uns i=0; i<4; i++)
	{
	  label = (label << 8) | *w;
	  if (*w)
	    w++;
	}
      v = stem_table_follow_edge(ts, v, label);
      if (!v)
	{
	  DBG("  %08x -> NIL", label);
	  return NULL;
	}
      DBG("  %08x -> %x", label, st_pos(ts, v));
    }
  return v;
}

static struct stem_table_vertex *
stem_table_lookup_word_vertex(struct table_stemmer *ts, byte *src)
{
  byte w[MAX_WORD_BYTES+1];
  if (!stem_table_conv_in(ts, src, w))
    {
      DBG("<%s>: not a valid word", src);
      return NULL;
    }
  DBG("Looking up <%s>", w);

  struct stem_table_vertex *v = stem_table_lookup_vertex(ts, w);
  if (!v)
    return NULL;
  uns cnt = (v->edge_hash_size >> 24) & 0xff;
  if (!cnt)
    {
      DBG("  Not a tree leaf");
      return NULL;
    }

  return v;
}

static byte *
stem_table_get_vertex_name(struct table_stemmer *ts, struct stem_table_vertex *v, byte *b)
{
  *--b = 0;
  while (v->parent)
    {
      u32 p = v->parent_label;
      while (p)
	{
	  *--b = p;
	  p >>= 8U;
	}
      v = st_go(ts, v->parent);
    }
  return b;
}

static int
stem_table_conv_vertex_out(struct table_stemmer *ts, struct stem_table_vertex *v, byte *y)
{
  byte x[MAX_WORD_BYTES+1];
  byte *xx = stem_table_get_vertex_name(ts, v, x+sizeof(x));
  if (ts->charset != CONV_CHARSET_UTF8)
    {
      struct conv_context *cc = &ts->conv_out;
      cc->source = xx;
      cc->source_end = xx + strlen(xx);
      cc->dest = cc->dest_start = y;
      cc->dest_end = y + MAX_WORD_BYTES;
      int status = conv_run(cc);
      if (!(status & CONV_SOURCE_END))
	{
	  msg(L_ERROR, "stem-table output too long");
	  return 0;
	}
      *cc->dest = 0;
    }
  else
    strcpy(y, xx);
  DBG("%s", y);
  return 1;
}

clist *
stem_table(struct stemmer *st, struct word_node *req, struct mempool *mp)
{
  struct table_stemmer *ts = st->priv;
  struct stem_table_vertex *v = stem_table_lookup_word_vertex(ts, req->w);
  if (!v)
    return NULL;

  clist *res = mp_alloc(mp, sizeof(*res));
  clist_init(res);

  u32 *lemmata = (u32*)(v->edges + (v->edge_hash_size & 0x00ffffff));
  uns cnt = (v->edge_hash_size >> 24) & 0xff;
  for (uns i=0; i<cnt; i++)
    {
      struct stem_table_lemma *lm = st_go(ts, lemmata[i]);
      DBG("Found lemma %d with %d variants at %x:", i, lm->num_variants, st_pos(ts, lm));
      uns cnt = (req->stem_form == WORD_FORM_OTHER) ? lm->num_variants : 1;
      byte buf[MAX_WORD_BYTES+1];
      if (!stem_table_conv_vertex_out(ts, st_go(ts, lm->variants[0]), buf))
	continue;
      uns form = (strcmp(buf, req->w) ? WORD_FORM_OTHER : WORD_FORM_LEMMA);
      if (req->word_form == WORD_FORM_LEMMA && form != req->word_form)
	continue;
      word_list_add_unique(mp, res, buf, form, WORD_FORM_LEMMA, 0, 0);
      if (req->stem_form == WORD_FORM_OTHER)
	for (uns j=1; j<cnt; j++)
	  if (stem_table_conv_vertex_out(ts, st_go(ts, lm->variants[j]), buf))
	    word_list_add_unique(mp, res, buf, form, WORD_FORM_OTHER, 0, 0);
    }

  return res;
}

void
stem_init_table(struct stemmer *st)
{
  struct table_stemmer *ts = xmalloc_zero(sizeof(*ts));
  st->priv = ts;
  uns len;
  ts->hdr = mmap_file(st->params, &len, 0);
  DBG("Loading table %s", st->params);
  if (len < sizeof(struct stem_table_hdr))
    die("Stemmer table %s is too short", st->params);
  if (ts->hdr->magic != STEM_TABLE_MAGIC)
    die("Stemmer table %s is inconsistent", st->params);
  ts->charset = find_charset_by_name(ts->hdr->charset);
  if (ts->charset < 0)
    die("Stemmer table %s built for unknown charset %s", st->params, ts->hdr->charset);
  conv_init(&ts->conv_in);
  conv_set_charset(&ts->conv_in, CONV_CHARSET_UTF8, ts->charset);
  conv_init(&ts->conv_out);
  conv_set_charset(&ts->conv_out, ts->charset, CONV_CHARSET_UTF8);
  DBG("Loaded");
}
