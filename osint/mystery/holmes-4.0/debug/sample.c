/*
 *	Simple Random Sampling (select <k> lines from the input stream at random)
 *
 *	(c) 2006 Martin Mares <mj@ucw.cz>
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#include "ucw/lib.h"
#include "ucw/fastbuf.h"
#include "ucw/bbuf.h"
#include "ucw/conf.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <getopt.h>

static void NONRET
usage(void)
{
  fputs("\
Usage: sample [options] <sample_size> < input > output\n\
\n\
-o, --keep-order	Keep order of input lines\n\
", stderr);
  exit(1);
}

static char *shortopts = "o";
static struct option longopts[] = {
  { "keep-order",		0, 0, 'o' },
  { NULL,			0, 0, 0 }
};

struct sample {
  u64 sort_key;
  byte *text;
};

static uns n, line_len, samples_count;
static u64 line_num;
static struct sample *samples;
static bb_t line;
static uns keep_order;

#define SWAP(i, j) do { struct sample s = samples[i]; samples[i] = samples[j]; samples[j] = s; } while(0)

#define ASORT_PREFIX(x) samples_##x
#define ASORT_KEY_TYPE u64
#define ASORT_ELT(i) samples[i].sort_key
#define ASORT_SWAP(i, j) SWAP(i, j)
#include "ucw/sorter/array-simple.h"

static inline void
new_sample(uns index)
{
  samples[index].text = xmalloc(line_len);
  memcpy(samples[index].text, line.ptr, line_len);
  samples[index].sort_key = line_num;
}

int
main(int argc, char **argv)
{
  int opt;
  while ((opt = getopt_long(argc, argv, shortopts, longopts, NULL)) >= 0)
    switch (opt)
      {
	case 'o':
	  keep_order++;
	  break;
	default:
	  usage();
	  break;
      }

  if (argc != optind + 1)
    usage();
  if (cf_parse_int(argv[optind], (int*)&n) || !n || n > ~0U / sizeof(struct sample))
    {
      fprintf(stderr, "Invalid sample size %s\n", argv[1]);
      return 1;
    }

  srandom(time(NULL) ^ getpid());
  samples = xmalloc(n * sizeof(*samples));

  bb_init(&line);
  struct fastbuf *f = bfdopen_shared(0, 16384);
  u64 index;
  while (line_len = bgets_bb(f, &line, ~0U))
    {
      if (unlikely(!++line_num))
	die("Too many lines");
      if (line_num <= n)
        new_sample(line_num - 1);
      else if ((index = random_max_u64(line_num)) < n)
        {
          xfree(samples[index].text);
          new_sample(index);
        }
    }
  bclose(f);
  bb_done(&line);
  samples_count = MIN(n, line_num);

  if (keep_order)
    samples_sort(samples_count);
  else
    for (uns i = 1; i < samples_count; i++)
      {
        uns j = random_max(i + 1);
        SWAP(i, j);
      }

  f = bfdopen_shared(1, 16384);
  for (uns i = 0; i < samples_count; i++)
    bputsn(f, samples[i].text);
  bclose(f);

  return 0;
}
