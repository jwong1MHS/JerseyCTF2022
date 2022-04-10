/*
 *	Sherlock Filter Engine -- Tutorial
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 *
 *	See doc/filter for details.
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "filter/filter.h"

#include <stdio.h>

/* List of custom filter variables */
static struct filter_variable custom_vars[] = {
  { "i", F_LVC_RAW, F_ET_INT, 0 },
  { "s", F_LVC_RAW, F_ET_STRING, 0 },
  { NULL, 0, 0, 0 }
};

/* Shared variables between the C source and the filter */
struct args {
  int i;
  char *s;
};

/* Bind the structure to custom_vars. If you do not bind a variable,
 * its value will be always undefined. */
static struct filter_binding custom_bind[] = {
  { "i", OFFSETOF(struct args, i) },
  { "s", OFFSETOF(struct args, s) },
  { NULL, 0 },
};

/* A simple custom function to reverse a string */
static void
func_reverse(struct filter_args *args, struct filter_value *ret, struct filter_value arg[MAX_FUNC_ARGS])
{
  /* If the argument is udefined, the result will be also */
  if (ret->undef = arg[0].undef)
    return;
  /* Allocate a new string */
  char *s = ret->v.s = mp_strdup(args->pool, arg[0].v.s);
  int l = strlen(s);
  for (int i = l / 2; i > 0; i--)
    {
      char x = s[i - 1];
      s[i - 1] = s[l - i];
      s[l - i] = x;
    }
}

/* List of custom filter functions with their parameters and types of results */
static struct filter_function custom_func[] = {
  { "reverse", 1, {F_ET_STRING}, F_ET_STRING, func_reverse },
  { NULL, 0, {}, 0, NULL }
};

int
main(int argc, char **argv)
{
  /* No configuration, no options (except the default ones) */
  log_init(argv[0]);
  cf_def_file = NULL;
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 || argc != optind + 1)
    {
      fprintf(stderr, "Usage: filter-example filter <in >out\n");
      return 1;
    }

  /* Initialize buffered I/O */
  struct fastbuf *in = bfdopen_shared(0, 1024);
  struct fastbuf *out = bfdopen_shared(1, 1024);

  /* Parse the filter program. You have to supply a table of variables, their binding
   * to the data structure you will pass to the filter when running it, and a table
   * of custom functions. */
  struct filter *filter = filter_load(argv[optind], custom_vars, custom_bind, custom_func);

  /* Create a filter interpreter context. One is usually enough, multi-threaded programs
   * need one per thread. */
  struct filter_args *intr = filter_intr_new(filter);

  /* Create a memory pool used for temporary data during execution. Please note that if
   * you bind any string variable for writing, the value can be allocated from this pool,
   * so you have to copy it somewhere else before flushing the pool. */
  struct mempool *mp = mp_new(1024);

  /* A structure used for passing data between your program and the filter. */
  struct args args;

  /* Execute the filter for each line from the stdin */
  char *line;
  while (line = bgets_mp(in, mp))
    {
      /* Set up shared variables */
      args.s = line;
      args.i = F_UNDEF_INT;

      /* Run the filter */
      intr->pool = mp;
      intr->raw = &args;
      intr->attr = NULL;
      int accepted = filter_intr_run(intr);

      /* Show the results */
      bprintf(out, "%s %s -> %s, %s\n",
	accepted ? "Accepted" : "Rejected", line,
	(args.i == F_UNDEF_INT) ? "undef" : mp_printf(mp, "%d", args.i),
	(args.s == F_UNDEF_STRING) ? "undef" : args.s);

      /* Flush allocated memory */
      mp_flush(mp);
    }

  /* Clean up */
  mp_delete(mp);
  filter_intr_delete(intr);
  filter_delete(filter);
  bclose(out);
  bclose(in);

  return 0;
}
