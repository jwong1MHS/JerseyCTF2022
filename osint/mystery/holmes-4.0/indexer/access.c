/*
 *	Sherlock Indexer -- Helper Functions for Data Structure Access
 *
 *	(c) 2004-2005 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/object.h"
#include "ucw/fastbuf.h"
#include "ucw/stkstring.h"
#include "indexer/indexer.h"
#include "indexer/params.h"

#include <fcntl.h>

/* Keeping card count */

uns card_count = ~0U;
uns skel_count = ~0U;

void
set_card_count(uns cc)
{
  if (card_count == ~0U)
    card_count = cc;
  else
    ASSERT(card_count == cc);
}

void
set_skel_count(uns sc)
{
  if (skel_count == ~0U)
    skel_count = sc;
  else
    ASSERT(skel_count == sc);
}

/* Dealing with notes */

struct partmap *notes_partmap, *notes_skel_partmap;

void
notes_part_map(uns rw)
{
  ASSERT(!notes_partmap);
  notes_partmap = partmap_open(index_name(fn_notes), rw);
  ASSERT(!(partmap_size(notes_partmap) % sizeof(struct card_note)));
  set_card_count(partmap_size(notes_partmap) / sizeof(struct card_note));
}

void
notes_part_unmap(void)
{
  ASSERT(notes_partmap);
  partmap_close(notes_partmap);
  notes_partmap = NULL;
}

void
notes_skel_part_map(uns rw)
{
  ASSERT(!notes_skel_partmap);
  notes_skel_partmap = partmap_open(index_name(fn_notes_skel), rw);
  ASSERT(!(partmap_size(notes_skel_partmap) % sizeof(struct card_note)));
  set_skel_count(partmap_size(notes_skel_partmap) / sizeof(struct card_note));
}

void
notes_skel_part_unmap(void)
{
  ASSERT(notes_skel_partmap);
  partmap_close(notes_skel_partmap);
  notes_skel_partmap = NULL;
}

/* Dealing with card attributes */

struct card_attr *attrs;
struct partmap *attrs_partmap;

void
attrs_map(uns rw)
{
  uns size;
  ASSERT(!attrs);
  attrs = mmap_file(index_name(fn_attributes), &size, rw);
  ASSERT(!(size % sizeof(struct card_attr)));
  set_card_count(size / sizeof(struct card_attr));
}

void
attrs_unmap(void)
{
  ASSERT(attrs);
  munmap_file(attrs, card_count * sizeof(struct card_attr));
  attrs = NULL;
}

void
attrs_part_map(uns rw)
{
  ASSERT(!attrs_partmap);
  attrs_partmap = partmap_open(index_name(fn_attributes), rw);
  ASSERT(!(partmap_size(attrs_partmap) % sizeof(struct card_attr)));
  set_card_count(partmap_size(attrs_partmap) / sizeof(struct card_attr));
}

void
attrs_part_unmap(void)
{
  ASSERT(attrs_partmap);
  partmap_close(attrs_partmap);
  attrs_partmap = NULL;
}

/* Dealing with index parameters */

void
params_load(struct index_params *params)
{
  struct fastbuf *b = bopen(index_name(fn_parameters), O_RDONLY, sizeof(struct index_params));
  int ok = breadb(b, params, sizeof(struct index_params));
  ASSERT(ok);
  bclose(b);
}

void
params_save(struct index_params *params)
{
  struct fastbuf *b = bopen(index_name(fn_parameters), O_CREAT | O_TRUNC | O_WRONLY, sizeof(struct index_params));
  bwrite(b, params, sizeof(struct index_params));
  bclose(b);
}

void
params_to_obj(struct index_params *params, struct odes *obj, uns final)
{
  obj_add_attr_format(obj, 'v', "%08x", params->version ? : INDEX_VERSION);
  obj_add_attr_num(obj, 't', params->ref_time);
  obj_add_attr_num(obj, 'n', params->objects_in);
  obj_add_attr_num(obj, 's', params->sites);
  obj_add_attr_num(obj, 'l', params->num_slices);
  obj_add_attr_num(obj, 'r', params->srand);
  if (final)
    {
      obj_add_attr_num(obj, 'm', params->cards_out);
    }
}

/* General copying procedure */

uns
alloc_read_ary(const char *name, const char *suffix, void *Ptr, uns count, uns record)
{
  void **ptr = Ptr;
  struct fastbuf *fb = index_bopen(stk_strcat(name, suffix), O_RDONLY, 0);
  if (count)
    ASSERT(bfilesize(fb) == count * record);
  else
  {
    ucw_off_t size = bfilesize(fb);
    ASSERT(!(size % record));
    count = size / record;
  }
  *ptr = big_alloc(count * record);
  if (count && !breadb(fb, *ptr, count * record))
    ASSERT(0);
  bclose(fb);
  return count;
}

void
write_free_ary(const char *name, const char *suffix, void *Ptr, uns count, uns record)
{
  void **ptr = Ptr;
  struct fastbuf *fb = index_bopen(stk_strcat(name, suffix), O_WRONLY | O_CREAT | O_TRUNC, 0);
  bwrite(fb, *ptr, count * record);
  big_free(*ptr, count * record);
  *ptr = NULL;
  bclose(fb);
}

