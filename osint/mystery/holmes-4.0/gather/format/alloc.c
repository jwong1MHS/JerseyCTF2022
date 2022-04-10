/*
 *	Sherlock Gatherer: Limited Memory Allocation for Parsers
 *
 *	(c) 2005 Martin Mares <mj@ucw.cz>
 *
 *	The multiplexer is split to two parts: format.c and parse.c
 *	to avoid linking of all the parsing machinery where only the
 *	list of known content types and encodings is needed.
 */

#include "sherlock/sherlock.h"
#include "gather/gather.h"

static void
parser_check_alloc(uns size)
{
  if ((gthis->parser_malloced += size) > max_parser_alloc)
    gerror(2406, "Parser memory limit exceeded");
}

void *
parser_malloc(uns size)
{
  parser_check_alloc(size);
  return xmalloc(size);
}

void *
parser_realloc(void *ptr, uns size)
{
  /* We don't know the original size of the allocation, so we cheat.
   * However, we believe that all parsers are intelligent enough to reallocate
   * in exponential steps, so the total multiplicative error is O(1).
   */
  parser_check_alloc(size);
  return xrealloc(ptr, size);
}

void *
parser_malloc_zero(uns size)
{
  parser_check_alloc(size);
  return xmalloc_zero(size);
}
