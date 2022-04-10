/*
 *	Sherlock Gatherer: The Bit Bucket
 *
 *	(c) 2002 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "gather/gather.h"

int
sink_parse(char **args UNUSED)
{
  gthis->text = fbmem_create(16384);
  return 1;
}
