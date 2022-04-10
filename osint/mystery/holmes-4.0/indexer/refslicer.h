/*
 *	Sherlock Indexer -- Slice reference chains
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 *
 *	This procedure is used in ssort and wsort.
 */

#include "ucw/bbuf.h"
#include "ucw/unaligned.h"

static bb_t slice_buf;
static uns slice_start[HARD_MAX_SLICES+1];

static void
slice_init(void)
{
  ASSERT(num_slices && num_slices <= HARD_MAX_SLICES);
  if (num_slices > 1)
    {
      bb_init(&slice_buf);
      bb_grow(&slice_buf, 4096);

      struct index_params par;
      params_load(&par);
      ASSERT(num_slices == par.num_slices);
      get_slice_start(&par, slice_start);
      for (uns i=0; i<=num_slices; i++)
	ITRACEN(2, "slice_start[%u] = %x", i, slice_start[i]);
    }
}

static void
slice_cleanup(void)
{
  bb_done(&slice_buf);
}

static uns
load_chain(byte *p, struct fastbuf *src, uns oid)
{
  uns cnt;
  byte *start = p;

  PUT_U32(p, oid);
  p += 4;
  if (oid >> 28)
    cnt = oid >> 28;
  else
    {
      cnt = bget_utf8_32(src);
      p = utf8_32_put(p, cnt);
    }
  breadb(src, p, cnt);
  return p + cnt - start;
}

static uns
skip_chain(struct fastbuf *src)
{
  uns oid = bgetl(src), cnt, cnt2 = 0;
  if (oid >> 28)
    cnt = oid >> 28;
  else
    {
      cnt = bget_utf8_32(src);
      cnt2 = utf8_space(cnt);
    }
  bskip(src, cnt);
  return 4 + cnt + cnt2;
}

#define PREPARE_SLICE(oid)				\
  while (((oid) & OID_MASK) >= slice_start[i+1])	\
    {							\
      if (slice[i] < bptr)				\
	{						\
	  PUT_U32(bptr, 0);				\
	  bptr += 4;					\
	}						\
      slice[++i] = bptr;				\
    }

static inline uns
slice_chain(struct fastbuf *src, struct fastbuf *dest, uns max_size, uns word_id)
{
  uns rsize = bgetl(src);

  max_size -= 16*num_slices;
  if (rsize > max_size)
    {
      while (rsize > max_size)
	rsize -= skip_chain(src);
      msg(L_ERROR, "Too long reference chain for word #%x, some references has been removed", word_id);
    }

  ucw_off_t start = btell(dest);
  if (num_slices == 1)
    {
      while (rsize)
	rsize -= 4 + bbcopy_chain(src, dest, bgetl(src));
      bputl(dest, 0);
    }
  else
    {
      byte *bptr = bb_grow(&slice_buf, rsize + 4*num_slices);
      uns i = 0;
      byte *slice[HARD_MAX_SLICES+1];
      slice[0] = bptr;
      while (rsize)
	{
	  uns oid = bgetl(src);
	  PREPARE_SLICE(oid);
	  uns l = load_chain(bptr, src, oid);
	  rsize -= l;
	  bptr += l;
	}
      PREPARE_SLICE(~0U-1);

      uns mask = 0, last_i = 0;
      for (i=0; i<num_slices; i++)
	if (slice[i+1] > slice[i])
	  {
	    mask |= 1 << i;
	    last_i = i;
	  }
      bputc(dest, mask);
      for (i=0; i<last_i; i++)
	if (mask & (1 << i))
	  bput_utf8_32(dest, slice[i+1] - slice[i]);
      bwrite(dest, slice[0], bptr - slice[0]);
    }

  return btell(dest) - start;
}

#undef PREPARE_SLICE
