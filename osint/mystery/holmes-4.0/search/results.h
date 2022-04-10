/*
 *	Sherlock Search Engine -- A template for result heap
 *
 *	(c) 1997--2006 Martin Mares <mj@ucw.cz>
 */

#define R_NOTE struct R(note)
#define R_HEAP struct R(heap)

struct R(note) {
  struct result_note n;
#ifdef CONFIG_SITES
  R_NOTE *hash_next;			/* Next in site hash chain */
#endif
  uns heap_pos;				/* Back-link to the heap */
  R_NOTE_FIELDS
};

struct R(heap) {
  uns max_matches;
  R_NOTE **heap;
  uns num_matches;
  R_NOTE *first_note, *free_note;
  uns site_max;
#ifdef CONFIG_SITES
  uns site_compressed;			/* Counter of site-compressed items */
  R_NOTE **site_hash;
  uns site_hash_mask;
#endif
};

#ifdef R_EMIT_CODE

static inline int
R(lt)(R_NOTE *a, R_NOTE *b)
{
  return a->n.q < b->n.q || a->n.q == b->n.q && a->n.sec_sort_key > b->n.sec_sort_key;
}

#define R_HEAP_LESS(a,b) R(lt)(a,b)
#define R_HEAP_SWAP(h,a,b,t) (t=h[a], h[a]=h[b], h[b]=t, h[a]->heap_pos=a, h[b]->heap_pos=b)

static void R(init)(struct mempool *pool, R_HEAP *h)
{
  /* Expects max_matches and site_max to be set */
  h->num_matches = 0;
  h->heap = mp_alloc(pool, sizeof(R_NOTE *) * (h->max_matches+1));
  h->first_note = mp_alloc(pool, sizeof(R_NOTE) * (h->max_matches+1) + 1);
  h->free_note = h->first_note++;
#ifdef CONFIG_SITES
  if (h->site_max)
    {
      h->site_hash_mask = 1;
      while (h->site_hash_mask < h->max_matches)
	h->site_hash_mask *= 2;
      h->site_hash = mp_alloc_zero(pool, sizeof(R_NOTE *) * h->site_hash_mask);
      h->site_hash_mask--;
    }
  h->site_compressed = 0;
#endif
}

static R_NOTE *R(get_note)(R_HEAP *h)
{
  return h->free_note;
}

static int R(insert)(R_HEAP *h, R_NOTE *note)
{
#ifdef CONFIG_SITES
  if (h->site_max)			/* Handle site compression */
    {
      R_SITE_TYPE site = R_GET_SITE(note);
      R_NOTE *t, **tt, *tn, *replace;
      tt = &h->site_hash[site & h->site_hash_mask];
      while ((t = *tt) && R_GET_SITE(t) < site)
	tt = &t->hash_next;
      /* tt is the place to connect the new note to */
      if (t && R_GET_SITE(t) == site)
	{
	  t->n.site_compressed += note->n.site_compressed;
	  if (h->site_max > 1)
	    {
	      tn = t->hash_next;
	      if (tn && R_GET_SITE(tn) == site)
		{
		  tn->n.site_compressed = ++t->n.site_compressed;
		  h->site_compressed++;
		  if (R(lt)(note, tn))
		    {
		      HDBG("\tSite2: Worse than both for site %08x (compressed=%d)", (uns)site, t->n.site_compressed);
		      return 0;
		    }
		  t->hash_next = tn->hash_next;
		  replace = tn;
		}
	      else
		{
		  HDBG("\tSite2: Second for site %08x (compressed=%d)", (uns)site, t->n.site_compressed);
		  replace = NULL;
		}
	      if (R(lt)(note, *tt))
		tt = &(*tt)->hash_next;
	    }
	  else				/* Only 1 doc per site allowed */
	    {
	      t->n.site_compressed++;
	      h->site_compressed++;
	      if (R(lt)(note, t))
		{
		  HDBG("\tSite1: Worse for site %08x (compressed=%d)", (uns)site, t->n.site_compressed);
		  return 0;
		}
	      *tt = t->hash_next;
	      replace = t;
	    }
	  note->n.site_compressed = t->n.site_compressed;
	}
      else
	{
	  HDBG("\tSite: First for site %08x (compressed=%d)", (uns)site, note->n.site_compressed);
	  replace = NULL;
	}
      if (replace)
	{
	  HDBG("\tSite: replacing %p", replace);
	  HEAP_DELETE(R_NOTE *, h->heap, h->num_matches, R_HEAP_LESS, R_HEAP_SWAP, replace->heap_pos);
	  h->free_note = replace;
	}
      else if (h->num_matches >= h->max_matches)
	{
	  R_NOTE *min, **mm;
	  if (R(lt)(note, h->heap[1]))
	    {
	      HDBG("\tSite: worse than minimum (lost compressed=%d)", note->n.site_compressed);
	      return 0;
	    }
	  min = h->free_note = h->heap[1];
	  HDBG("\tSite: replacing minimum %p from site %08x", min, (uns)(R_GET_SITE(min)));
	  HEAP_DELMIN(R_NOTE *, h->heap, h->num_matches, R_HEAP_LESS, R_HEAP_SWAP);
	  mm = &h->site_hash[R_GET_SITE(min) & h->site_hash_mask];
	  while (*mm != min)
	    {
	      ASSERT(*mm);
	      mm = &(*mm)->hash_next;
	    }
	  *mm = min->hash_next;
	  if (tt == &min->hash_next)
	    tt = mm;
	}
      else
	h->free_note = h->first_note++;
      HDBG("\tSite: hashed node %p for site %08x", note, (uns)site);
      note->hash_next = *tt;
      *tt = note;
    }
  else					/* No site compression */
#endif
    {
      if (h->num_matches >= h->max_matches)
	{
	  if (note->n.q < h->heap[1]->n.q) /* Q too low -> throw away */
	    {
	      HDBG("\tWorse than Q=%d", h->heap[1]->n.q);
	      return 0;
	    }
	  h->free_note = h->heap[1];
	  HEAP_DELMIN(R_NOTE *, h->heap, h->num_matches, R_HEAP_LESS, R_HEAP_SWAP);
	  HDBG("\tDeleted minimum with Q=%d", h->free_note->n.q);
	}
      else
	h->free_note = h->first_note++;
    }
  h->heap[++h->num_matches] = note;
  note->heap_pos = h->num_matches;
  HEAP_INSERT(R_NOTE *, h->heap, h->num_matches, R_HEAP_LESS, R_HEAP_SWAP);
  return 1;
}

static R_NOTE * UNUSED R(delete_min)(R_HEAP *h)
{
  if (!h->num_matches)
    return NULL;
  else
    {
      HEAP_DELMIN(R_NOTE *, h->heap, h->num_matches, R_HEAP_LESS, R_HEAP_SWAP);
      return h->heap[h->num_matches+1];		/* A little trick */
    }
}

#endif

#undef R
#undef R_NOTE_FIELDS
#undef R_SITE_TYPE
#undef R_GET_SITE

#undef R_NOTE
#undef R_HEAP
#undef R_HEAP_LESS
#undef R_HEAP_SWAP
