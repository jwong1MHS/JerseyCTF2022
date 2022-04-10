/*
 *	Sherlock Indexer -- Lexicon Builder
 *
 *	(c) 2001--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2004 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/unaligned.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/hashfunc.h"
#include "ucw/unicode.h"
#include "sherlock/object.h"
#include "sherlock/tagged-text.h"
#include "charset/unicat.h"
#include "indexer/indexer.h"
#include "indexer/lexicon.h"
#include "indexer/params.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>

#define LH_MKLEX
#include "indexer/lexhash.h"

static enum word_class
lm_lookup(enum word_class orig_class, u16 *uni, uns ulen, word_id_t *idp, void *user UNUSED)
{
  struct verbum *v;

  if (orig_class != WC_NORMAL)
    return orig_class;
  v = lh_lookup(uni, ulen);
  v->u.count++;
  *idp = v;
  return v->id & 7;
}

static inline void
lm_got_word(uns pos UNUSED, uns cat UNUSED, word_id_t w UNUSED, void *user UNUSED)
{
}

static inline void
lm_got_complex(uns pos UNUSED, uns cat UNUSED, word_id_t wroot UNUSED, word_id_t wcont UNUSED, uns dir UNUSED, void *user UNUSED)
{
}

#include "indexer/lexmap.h"

static inline void
mklex_meta(byte *x)
{
  lm_doc_start(NULL);
  if (*x >= '0' && *x <= '3')
    x++;
  lm_map_text(x, x + str_len(x));
}

static void
mklex_metas(struct odes *o)
{
  struct oattr *a, *c;

  for (a=obj_find_attr(o, 'M'); a; a=a->same)
    mklex_meta(a->val);
  for (c=obj_find_attr(o, 'c' + OBJ_ATTR_SON); c; c=c->same)
    for (a=obj_find_attr(c->son, 'M'); a; a=a->same)
      mklex_meta(a->val);
}

static void
mklex_reftexts(struct odes *o)
{
  for (struct oattr *a=obj_find_attr(o, 'x'); a; a=a->same)
    {
      byte *t = a->val;
      for (uns i=0; i<3; i++)
	while (*t++ != ' ')
	  ;
      mklex_meta(t);
    }
}

#ifdef CUSTOM_MKLEX
static void
do_lm_map_text(char *start, char *end)
{
  lm_map_text(start, end);
}
#endif

static int
out_find(struct card_attr *attr, struct card_note *note)
{
  if (clist_empty(&subindices))
    return 1;
  uns ft, id;
#ifdef CONFIG_FILETYPE
  ft = CA_GET_FILE_TYPE(attr);
#else
  ft = 0;
#endif
  id = get_subindexing_id(fetch_id, note);
  CLIST_FOR_EACH(struct subindex *, sub, subindices)
    if ((sub->type_mask & (1 << ft)) &&
	(sub->id_mask & (1 << id)))
      return 1;
  return 0;
}

static void
mklex_card(struct card_info *info, struct odes *o)
{
  if (!out_find(&info->attr, &info->note))
    return;

  lm_doc_start(NULL);
  for (struct oattr *a=obj_find_attr(o, 'X'); a; a=a->same)
    {
      lm_map_text(a->val, a->val + str_len(a->val));
    }
  for (struct oattr *u = obj_find_attr(o, 'U' + OBJ_ATTR_SON); u; u=u->same)
    {
      struct odes *uu = u->son;
      mklex_metas(uu);
      for (struct oattr *r = obj_find_attr(uu, 'y' + OBJ_ATTR_SON); r; r=r->same)
	mklex_metas(r->son);
    }
  /*
   * Scan `x' brackets for metas, too. There are none during normal indexing,
   * however if we are updating an existing index and feeding stage2 of the indexer
   * with already preprocessed cards, such metas can exist.
   */
  for (struct oattr *x = obj_find_attr(o, 'x' + OBJ_ATTR_SON); x; x=x->same)
    mklex_metas(x->son);
#ifdef CUSTOM_MKLEX
  CUSTOM_MKLEX(o, do_lm_map_text);
#endif
  mklex_metas(o);
  mklex_reftexts(o);
  PROGRESS(fetch_id, "mklex: %d cards (%d%%), %d words in %d buckets",
	   fetch_id, (int)((float)fetch_id/fetch_num_ids*100),
	   lh_hash_count, lh_hash_size);
}

static inline void
lex_write_verbum(struct fastbuf *b, struct verbum *l)
{
  bputl(b, l->id);
  bputl(b, l->u.count);
  bput_context(b, 0);
  uns c = str_len(l->word);
  bputc(b, c);
  bwrite(b, l->word, c);
}

static void
lex_write(byte *name)
{
  struct fastbuf *b;

  b = bopen_file(name, O_WRONLY | O_CREAT | O_TRUNC, &indexer_fb_params);
  bputl(b, lh_hash_count);
  LH_WALK(l)
    lex_write_verbum(b, l);
  bclose(b);
}

static void
write_params(void)
{
  struct index_params params;
  params_load(&params);
  memcpy(&params.lex_config, &lexicon_config, sizeof(lexicon_config));
  params_save(&params);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  setproctitle_init(argc, argv);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }

  lm_init();
  lh_init();
  write_params();

  log(L_INFO, "Creating lexicon");
  fetch_cards(mklex_card);

  lex_write(index_name(fn_lex_raw));
  log(L_INFO, "Built lexicon with %d words", lh_hash_count);
  return 0;
}
