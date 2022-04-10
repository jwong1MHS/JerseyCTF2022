#include "ucw/lib.h"
#include "ucw/fastbuf.h"
#include "ucw/bbuf.h"
#include "sherlock/sherlock.h"
#include "indexer/graph.h"

#include <stdlib.h>
#include <fcntl.h>

int
main(int argc UNUSED, char **argv)
{
  uns start = strtoul(argv[1], NULL, 16);
  struct fastbuf *in = bopen(argv[2], O_RDONLY, 1<<16);
  u32 node, deg;
  ucw_off_t pos;
  static bb_t positions;
  uns max = 0;
  while (pos = btell(in), bget_graph_hdr(in, &node, &deg))
  {
    ASSERT(node >= start);
    node -= start;
    bskip(in, deg * sizeof(u32));
    bb_grow(&positions, (node+1) * BYTES_PER_O);
    byte *p = positions.ptr + node * BYTES_PER_O;
    PUT_O(p, pos+1);
    max = MAX(max, node+1);
  }
  for (uns i=0; i<max; i++)	// so that uninitialized is set to -1
  {
    byte *p = positions.ptr + i * BYTES_PER_O;
    ucw_off_t pos = GET_O(p);
    PUT_O(p, pos-1);
  }
  bclose(in);
  struct fastbuf *out = bopen(argv[3], O_WRONLY | O_CREAT | O_TRUNC, 1<<16);
  bwrite(out, positions.ptr, max * BYTES_PER_O);
  bclose(out);
  return 0;
}
