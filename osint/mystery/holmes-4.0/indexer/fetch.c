/*
 *	Sherlock Indexer -- Fetching of Cards for Second Indexer Pass
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2003--2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/mempool.h"
#include "ucw/url.h"
#include "sherlock/object.h"
#include "sherlock/attrset.h"
#include "sherlock/objread.h"
#include "indexer/indexer.h"

#include <string.h>
#include <fcntl.h>

uns fetch_id;
uns fetch_num_ids;

static void
fetch_cards_raw(void (*got_card)(struct card_info *info, struct odes *obj))
{
  struct mempool *pool;
  struct fastbuf *fb_infos;
  struct card_info info;
  struct get_buck gb;

  pool = mp_new(16384);
  get_buck_init(&gb);
  gb.pool = pool;
  fb_infos = index_bopen(fn_card_info, O_RDONLY, 1);
  fetch_num_ids = bfilesize(fb_infos) / sizeof(struct card_info);
  fetch_id = -1;
  while (breadb(fb_infos, &info, sizeof(info)))
    {
      fetch_id++;
      mp_flush(pool);
      if (!get_buck_next(&gb, info.attr.card))
	die("Ran out of buckets");
      got_card(&info, gb.o);
    }
  get_buck_cleanup(&gb);
  bclose(fb_infos);
  mp_delete(pool);
  log(L_INFO, "Processed %d objects", fetch_id+1);
}

void
fetch_cards(void (*got_card)(struct card_info *info, struct odes *obj))
{
  struct mempool *pool;
  struct fastbuf *fb_labels, *fb_infos;
  struct odes *o;
  u32 next_label_id;
  struct card_info info;
  struct odes *main_hdr, *url_hdr, *current_hdr;
  struct obj_read_state read_state;
  struct get_buck gb;

  if (raw_stage2_input)
    return fetch_cards_raw(got_card);

  pool = mp_new(16384);
  get_buck_init(&gb);
  gb.pool = pool;
  fb_labels = index_bopen(fn_labels, O_RDONLY, 1);
  fb_infos = index_bopen(fn_card_info, O_RDONLY, 1);
  fetch_num_ids = bfilesize(fb_infos) / sizeof(struct card_info);
  fetch_id = -1;
  next_label_id = bgetl(fb_labels);
  while (breadb(fb_infos, &info, sizeof(info)))
    {
      fetch_id++;
      ASSERT(!(info.attr.flags & (CARD_FLAG_EMPTY | CARD_FLAG_DUP)));	// should have been filtered out in stage 1
      mp_flush(pool);
      if (!get_buck_next(&gb, info.attr.card))
	die("Object pool changed unexpectedly");
      o = gb.o;

      /* Pick all per-URL attributes and move them to the main header */
      main_hdr = obj_new(pool);
      struct oattr **Oa = &o->attrs, *oa;
      while (oa = *Oa)
	if (oa->attr == 'U' || attr_set_match(&label_attr_set, oa))
	  {
	    *Oa = oa->next;				// delete from this chain
	    oa->next = main_hdr->attrs;			// prepend to main_hdr, no need to check the existence
	    main_hdr->attrs = oa;
	  }
	else
	  Oa = &oa->next;
      if (info.attr.flags & CARD_FLAG_MERGED)		// merged cards get all per-URL attributes from labels
	main_hdr = obj_new(pool);
      obj_add_son_ref(o, 'U' + OBJ_ATTR_SON, main_hdr);
      current_hdr = url_hdr = main_hdr;

      /* Read all labels and create the corresponding headers */
      u32 src_card = 0;
      u32 src_redir = 0;
      while (next_label_id == fetch_id)
	{
	  u32 scard = bgetl(fb_labels);
	  u32 sredir = bgetl(fb_labels);
	  uns cnt = bgetl(fb_labels);
	  uns flags = bgetc(fb_labels);
	  if (flags & LABEL_TYPE_URL)
	    {
	      if (scard != src_card)
		{
		  if (src_card)
		    current_hdr = url_hdr = obj_add_son(o, 'U' + OBJ_ATTR_SON);
		  src_card = scard;
		  src_redir = scard;
		}
	      if (sredir != src_redir)
		{
		  current_hdr = obj_add_son(url_hdr, 'y' + OBJ_ATTR_SON);
		  src_redir = sredir;
		}
	      obj_read_start(&read_state, current_hdr);
	    }
	  else
	    obj_read_start(&read_state, o);
	  get_attr_set_type(BUCKET_TYPE_V33);
	  ucw_off_t stop = btell(fb_labels) + cnt;
	  int last_attr = 0;
	  while (btell(fb_labels) < stop)
	    {
	      struct parsed_attr pa;
	      if (bget_attr(fb_labels, &pa) <= 0)
		die("Unexpected inconsistency of label file");
	      copy_parsed_attr(o->pool, &pa);
	      if ((flags & LABEL_FLAG_OVERRIDE) && pa.attr != last_attr)
		obj_set_attr(read_state.obj, pa.attr, NULL);
	      obj_read_attr_ref(&read_state, pa.attr, pa.val);
	      last_attr = pa.attr;
	    }
	  obj_read_end(&read_state);
	  next_label_id = bgetl(fb_labels);
	}
      if (info.note.useful_size)
	obj_set_attr_num(o, 'u', info.note.useful_size);
      if (info.attr.flags & CARD_FLAG_FRAMESET)
	{
	  /* Frameset to be deleted, but CARD_FLAG_EMPTY is no longer set,
	   * which means that some labels have been attached, so we index it,
	   * but without the X attribute.
	   */
	  obj_set_attr(o, 'X', NULL);
	}
      got_card(&info, o);
    }
  ASSERT(next_label_id == ~0U);
  get_buck_cleanup(&gb);
  bclose(fb_infos);
  bclose(fb_labels);
  mp_delete(pool);
  log(L_INFO, "Processed %d objects", fetch_id+1);
}

#ifdef TEST

#include "ucw/getopt.h"

static struct fastbuf *out;
static uns cnt;

static void got(struct card_info *info, struct odes *o)
{
  bprintf(out, "### new=%x orig=%x\n", cnt++, info->orig_card);
  obj_write(out, o, BUCKET_TYPE_PLAIN);
  bputc(out, '\n');
}

int main(int argc, char **argv)
{
  log_init(argv[0]);
  cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL);
  out = bfdopen_shared(1, 65536);
  fetch_cards(got);
  bclose(out);
  return 0;
}

#endif
