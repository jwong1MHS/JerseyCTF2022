/*
 *	Low priority CPU consuming script
 *
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#include "ucw/lib.h"
#include "ucw/conf.h"
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

static int threads = 1;

static void *
thread(void *p UNUSED)
{
  while (1) { }
}

int
main(int argc, char **argv)
{
  if (argc > 1)
    threads = atoi(argv[1]);
  nice(1000000);
  if (threads > 1)
    {
      pthread_t t[threads];
      for (int i = 1; i < threads; i++)
        pthread_create(t + i, NULL, thread, NULL);
    }
  while (1) { }
}
