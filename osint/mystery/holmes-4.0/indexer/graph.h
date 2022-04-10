/*
 *	Sherlock Link Graphs
 *
 *	(c) 2006 Robert Spalek <robert@ucw.cz>
 */

#ifndef _SHERLOCK_INDEXER_GRAPH_H
#define _SHERLOCK_INDEXER_GRAPH_H

#include "ucw/ff-binary.h"

/* Graph edge types */

#define ETYPE_NORMAL 0
#define ETYPE_INTERSITE 0x20000000
#define ETYPE_REDIRECT 0x40000000
#define ETYPE_FRAME 0x80000000
#define ETYPE_IMAGE 0xc0000000		/* also serves as a mask for link type */
#define ETYPE_MASK 0xe0000000
#define ETYPE_SHIFT 29

static inline void
bput_graph_hdr(struct fastbuf *fb, u32 node, u32 deg)
{
  if (deg == 1)
    bputl(fb, node);
  else if (deg < 0x100)
  {
    bputl(fb, node | (1 << ETYPE_SHIFT));
    bputc(fb, deg);
  }
  else if (deg < 0x10000)
  {
    bputl(fb, node | (2 << ETYPE_SHIFT));
    bputw(fb, deg);
  }
  else
  {
    bputl(fb, node | (3 << ETYPE_SHIFT));
    bputl(fb, deg);
  }
}

static inline int
bget_graph_hdr(struct fastbuf *fb, u32 *node, u32 *deg)
{
  u32 n = bgetl(fb);
  if (n == 0xffffffff)
  {
    *node = n;
    *deg = 0;
    return 0;
  }
  *node = n & ~ETYPE_MASK;
  u32 t = n >> ETYPE_SHIFT;
  switch (t)
  {
    case 0: *deg = 1; break;
    case 1: *deg = bgetc(fb); break;
    case 2: *deg = bgetw(fb); break;
    default: *deg = bgetl(fb);
  }
  return 1;
}

#endif
