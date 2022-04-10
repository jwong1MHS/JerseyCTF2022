/*
 *	Sherlock Indexer -- Handling of Equivalence Classes
 *
 *	(c) 2002--2004 Martin Mares <mj@ucw.cz>
 */

/*
 * The equivalence classes are stored as Tarjan's Union-Find Trees
 * with path compression, but no union by size/rank => the amortized
 * complexity of operations is not O(alpha(n)) as in the original
 * version, but they still should perform very well indeed.
 */

static u32 *merges;
static uns merges_size;

static inline void merges_map(uns rw)
{
  merges = mmap_file(index_name(fn_merges), &merges_size, rw);
  ASSERT(!(merges_size % 4));
  set_card_count(merges_size/4);
}

static inline void merges_unmap(void)
{
  munmap_file(merges, merges_size);
}

static inline uns
merges_find_root(uns x)
{
  uns y = x, z;

  while ((int)merges[y] >= 0)
    y = merges[y];
  /* Perform path compression */
  while ((int)merges[x] >= 0)
    {
      z = merges[x];
      merges[x] = y;
      x = z;
    }
  return y;
}

static inline int
merges_union(uns src, uns dest)
{
  src = merges_find_root(src);
  dest = merges_find_root(dest);
  if (src == dest)
    return 1;
  else
    {
      merges[src] = dest;
      return 0;
    }
}

static inline uns
merges_follow(uns x)
{
  /* Simplified version of merges_find_root() for flattened merge arrays */
  return (merges[x] == ~0U) ? x : merges[x];
}
