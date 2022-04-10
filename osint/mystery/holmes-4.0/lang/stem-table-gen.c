/*
 *  Simple Table-Driven Stemmer: Table Preprocessor
 *
 *  (c) 2005 Martin Mares <mj@ucw.cz>
 *
 *  The source file is a text file containing a sequence of classes. Each class
 *  starts with a lemma on a separate line, the following lines contain words
 *  belonging to the class, delta-compressed (each word starts with a number
 *  determinining how long a prefix it shares with the previous word; the first
 *  word can share a prefix with the lemma).
 *
 *  Example:
 *	write
 *	5s			<-- writes
 *	4ing			<-- writing
 *	4ten			<-- written
 *	2ote			<-- wrote
 *	be
 *	0was			<-- was (the `0' cannot be omitted)
 */

#undef LOCAL_DEBUG

#include "ucw/lib.h"
#include "ucw/mempool.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/chartype.h"
#include "ucw/prime.h"
#include "lang/stem-table.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

static struct mempool *pool;

struct lemma {
  struct variant *first_var, **last_var;
  uns pos;
  byte w[1];
};

struct variant {
  struct variant *vertex_next, *lemma_next;
  struct lemma *lemma;
  struct vertex *vertex;
};

struct vertex {
  u32 label;
  struct vertex *parent, *first_son, *next_sibling;
  struct variant *variants;
  uns pos, hash_size;
};

static struct vertex trie_root;

#define GBUF_PREFIX(x) lembuf_##x
#define GBUF_TYPE struct lemma *
#include "ucw/gbuf.h"
static lembuf_t lemmata;
static uns lm_cnt, var_cnt, vert_cnt;

#ifdef LOCAL_DEBUG
static void
dump_trie(struct vertex *v, uns depth)
{
  for (uns i=0; i<depth; i++)
    putchar(' ');
  for (uns i=0; i<4; i++)
    {
      uns x = ((v->label << 8*i) >> 24) & 0xff;
      putchar(x ? : '.');
    }
  if (v->variants)
    {
      putchar(':');
      for (struct variant *w = v->variants; w; w=w->vertex_next)
	printf(" %s", w->lemma->w);
    }
  putchar('\n');
  for (struct vertex *w = v->first_son; w; w=w->next_sibling)
    dump_trie(w, depth+4);
}

static void
dump_lemmata(void)
{
  for (uns i=0; i<lm_cnt; i++)
    {
      struct lemma *lm = lemmata.ptr[i];
      printf("Lemma %s:\n", lm->w);
      for (struct variant *v=lm->first_var; v; v=v->lemma_next)
	{
	  byte buf[MAX_WORD_BYTES+1], *b=buf+sizeof(buf);
	  *--b = 0;
	  for (struct vertex *w=v->vertex; w; w=w->parent)
	    {
	      u32 code = w->label;
	      for (uns j=0; j<4; j++)
		{
		  if (code & 0xff)
		    *--b = code;
		  code >>= 8;
		}
	    }
	  printf("\t%s\n", b);
	}
    }
}
#endif

static struct lemma *
add_lemma(byte *w)
{
  struct lemma *lm = mp_alloc_fast(pool, sizeof(*lm) + strlen(w));
  lm->first_var = NULL;
  lm->last_var = &lm->first_var;
  strcpy(lm->w, w);
  lembuf_grow(&lemmata, lm_cnt+1);
  lemmata.ptr[lm_cnt++] = lm;
  return lm;
}

static struct vertex *
lookup_vertex(struct vertex *v, u32 label)
{
  struct vertex *w;
  for (w=v->first_son; w; w=w->next_sibling)
    if (w->label == label)
      return w;
  w = mp_alloc_fast(pool, sizeof(struct vertex));
  w->label = label;
  w->parent = v;
  w->first_son = NULL;
  w->next_sibling = v->first_son;
  v->first_son = w;
  w->variants = NULL;
  vert_cnt++;
  return w;
}

static void
add_word(struct lemma *lm, byte *w)
{
  struct vertex *v = &trie_root;
  while (*w)
    {
      u32 px = 0;
      uns i = 4;
      while (i--)
	{
	  px = (px << 8) | *w;
	  if (*w)
	    w++;
	}
      v = lookup_vertex(v, px);
    }
  struct variant *var = mp_alloc_fast(pool, sizeof(struct variant));
  var->vertex_next = v->variants;
  v->variants = var;
  var->lemma = lm;
  *lm->last_var = var;
  lm->last_var = &var->lemma_next;
  var->lemma_next = NULL;
  var->vertex = v;
  var_cnt++;
}

static uns
assign_lemma_positions(uns pos)
{
  for (uns i=0; i<lm_cnt; i++)
    {
      struct lemma *lm = lemmata.ptr[i];
      lm->pos = pos;
      pos += sizeof(struct stem_table_lemma);
      for (struct variant *v = lm->first_var; v; v=v->lemma_next)
	pos += 4;
    }
  return pos;
}

static uns
assign_vertex_positions(uns pos, struct vertex *v)
{
  uns degree = 0;
  for (struct vertex *w=v->first_son; w; w=w->next_sibling)
    degree++;
  v->hash_size = (degree < 3) ? degree : nextprime(4*degree/3);
  v->pos = pos;
  pos += sizeof(struct stem_table_vertex);
  pos += v->hash_size * sizeof(struct stem_table_edge);
  for (struct variant *w=v->variants; w; w=w->vertex_next)
    pos += 4;
  for (struct vertex *w=v->first_son; w; w=w->next_sibling)
    pos = assign_vertex_positions(pos, w);
  return pos;
}

static void
write_lemmata(struct fastbuf *f)
{
  for (uns i=0; i<lm_cnt; i++)
    {
      struct lemma *lm = lemmata.ptr[i];
      ASSERT((ucw_off_t)lm->pos == btell(f));
      struct stem_table_lemma l;
      l.num_variants = 0;
      for (struct variant *v=lm->first_var; v; v=v->lemma_next)
	l.num_variants++;
      bwrite(f, &l, sizeof(l));
      for (struct variant *v=lm->first_var; v; v=v->lemma_next)
	bputl(f, v->vertex->pos);
    }
}

static void
write_hash(struct fastbuf *f, struct vertex *v)
{
  struct stem_table_edge edges[v->hash_size];
  bzero(edges, sizeof(edges));
  for (struct vertex *w=v->first_son; w; w=w->next_sibling)
    {
      uns h = w->label % v->hash_size;
      uns i = h;
      while (edges[i].label)
	{
	  i = (i+1) % v->hash_size;
	  ASSERT(i != h);
	}
      edges[i].label = w->label;
      edges[i].dest = w->pos;
    }
  bwrite(f, edges, sizeof(edges));
}

static void
write_trie(struct fastbuf *f, struct vertex *v, struct vertex *parent)
{
  struct stem_table_vertex w;
  ASSERT((ucw_off_t)v->pos == btell(f));
  w.parent = parent ? parent->pos : 0;
  w.parent_label = v->label;
  w.edge_hash_size = v->hash_size;
  for (struct variant *var=v->variants; var; var=var->vertex_next)
    w.edge_hash_size += 1<<24;
  bwrite(f, &w, sizeof(w));
  write_hash(f, v);
  for (struct variant *var=v->variants; var; var=var->vertex_next)
    bputl(f, var->lemma->pos);
  for (struct vertex *t=v->first_son; t; t=t->next_sibling)
    write_trie(f, t, v);
}

int main(int argc, char **argv)
{
  log_init(argv[0]);
  if (argc != 4)
    {
      fputs("Usage: stem-table-gen <src> <dest> <charset>\n", stderr);
      exit(1);
    }

  pool = mp_new(65536);
  lembuf_init(&lemmata);

  struct fastbuf *src = bopen(argv[1], O_RDONLY, 65536);
  byte buf[MAX_WORD_BYTES+1], w[MAX_WORD_BYTES+1];
  struct lemma *lm = NULL;
  while (bgets(src, buf, sizeof(buf)))
    {
      ASSERT(buf[0]);
      if (Cdigit(buf[0]))
	{
	  int d = 0;
	  byte *x = buf;
	  while (Cdigit(*x))
	    d = 10*d + *x++ - '0';
	  strcpy(w+d, x);
	  ASSERT(lm);
	  add_word(lm, w);
	}
      else
	{
	  lm = add_lemma(buf);
	  strcpy(w, buf);
	  add_word(lm, w);
	}
    }
  bclose(src);
  msg(L_INFO, "Read %d entries for %d lemmata", var_cnt, lm_cnt);
  msg(L_INFO, "Created trie with %d vertices", vert_cnt+1);

#ifdef LOCAL_DEBUG
  dump_lemmata();
  dump_trie(&trie_root, 0);
#endif

  struct fastbuf *dest = bopen(argv[2], O_WRONLY | O_CREAT | O_TRUNC, 65536);
  struct stem_table_hdr hdr;
  hdr.magic = STEM_TABLE_MAGIC;
  hdr.first_lemma = sizeof(hdr);
  hdr.root = assign_lemma_positions(hdr.first_lemma);
  ASSERT(strlen(argv[3]) < sizeof(hdr.charset));
  strcpy(hdr.charset, argv[3]);
  uns size = assign_vertex_positions(hdr.root, &trie_root);
  bwrite(dest, &hdr, sizeof(hdr));
  write_lemmata(dest);
  write_trie(dest, &trie_root, NULL);
  ASSERT((ucw_off_t)size == btell(dest));
  bclose(dest);
  msg(L_INFO, "Generated stemming table");

  return 0;
}
