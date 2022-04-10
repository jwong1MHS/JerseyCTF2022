/*
 *	Sherlock Indexer -- Merging two reference chains
 *
 *	(c) 2001--2003 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 *
 *	This procedure is used in ssort and wsort.
 */

#define REFCHAIN_UNIFY_WORKSPACE (sizeof(struct refchain_write_heap) + sizeof(uns))
#define OID_MASK	((1<<28) - 1)

struct refchain_write_heap {
  void *pos, *end;
  u32 ID, id;
};

static inline uns
bbcopy_chain(struct fastbuf *src, struct fastbuf *dest, uns ID)
{
  uns cnt, cnt2 = 0;
  bputl(dest, ID);
  if (ID >> 28)
    cnt = ID >> 28;
  else
    {
      cnt = bget_utf8_32(src);
      bput_utf8_32(dest, cnt);
      cnt2 = utf8_space(cnt);
    }
  bbcopy(src, dest, cnt);
  return cnt + cnt2;
}

static void
refchain_write_merged(uns n, u32 **len, void **src, struct fastbuf *dest, void *buf)
{
  struct refchain_write_heap {
    void *pos, *end;
    u32 ID, id;
  } *info = buf;
  uns *heap = (void *)(info + n);

  uns sum = 0;
  for (uns i = 0; i < n; i++)
    {
      heap[i + 1] = i;
      info[i].ID = get_u32(src[i]);
      info[i].id = info[i].ID & OID_MASK;
      info[i].pos = src[i] + 4;
      info[i].end = src[i] + *len[i];
      sum += *len[i];
    }
  bputl(dest, sum);

#define LESS(x, y) (info[x].id < info[y].id)
  HEAP_INIT(uns, heap, n, LESS, HEAP_SWAP);
  while (1)
    {
      struct refchain_write_heap *i = info + heap[1];
      bputl(dest, i->ID);
      void *p = i->pos;
      if (i->ID >> 28)
        p += i->ID >> 28;
      else
        {
	  uns cnt;
          p = utf8_32_get(p, &cnt);
	  p += cnt;
        }
      bwrite(dest, i->pos, p - i->pos);
      i->pos = p;
      if (i->pos == i->end)
        {
	  if (n == 2)
	    {
	      i = info + heap[2];
	      bputl(dest, i->ID);
	      bwrite(dest, i->pos, i->end - i->pos);
	      return;
	    }
	  HEAP_DELMIN(uns, heap, n, LESS, HEAP_SWAP);
	}
      else
        {
	  i->ID = get_u32(i->pos);
	  i->id = i->ID & OID_MASK;
	  i->pos += 4;
	  HEAP_INCREASE(uns, heap, n, LESS, HEAP_SWAP, 1);
	}
    }
#undef LESS
}

static void
refchain_copy_merged(uns n, u32 **len, struct fastbuf **src, struct fastbuf *dest)
{
  uns heap[n + 1];
  struct info {
    uns len;
    u32 ID, id;
  } info[n];
  
  uns sum = 0;
  for (uns i = 0; i < n; i++)
    {
      heap[i + 1] = i;
      info[i].ID = bgetl(src[i]);
      info[i].id = info[i].ID & OID_MASK;
      info[i].len = *len[i] - 4;
      sum += *len[i];
    }
  bputl(dest, sum);

#define LESS(x, y) (info[x].id < info[y].id)
  HEAP_INIT(uns, heap, n, LESS, HEAP_SWAP);
  while (1)
    {
      struct fastbuf *s = src[heap[1]];
      struct info *i = info + heap[1];
      i->len -= bbcopy_chain(s, dest, i->ID);
      if (!i->len)
        {
	  if (n == 2)
	    {
	      s = src[heap[2]]; 
	      i = info + heap[2];
	      bputl(dest, i->ID);
	      bbcopy(s, dest, i->len);
	      return;
	    }
	  HEAP_DELMIN(uns, heap, n, LESS, HEAP_SWAP);
	}
      else
        {
	  i->ID = bgetl(s);
	  i->id = i->ID & OID_MASK;
	  i->len -= 4;
	  HEAP_INCREASE(uns, heap, n, LESS, HEAP_SWAP, 1);
	}
    }
#undef LESS
}
