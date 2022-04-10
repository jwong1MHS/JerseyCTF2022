/*
 *	Sherlock Shepherd Daemon -- Merging of Indexer Feedback
 *
 *	(c) 2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "gather/shepherd/shepherd.h"
#include "indexer/indexer.h"

#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>

static inline void
next_feedback(struct fastbuf *f, struct feedback_gatherer *fb, struct footprint *fp)
{
  if (breadb(f, fb, sizeof(*fb)))
    memcpy(fp, fb->footprint, 16);
  else
    *fp = MAX_FOOTPRINT;
}

static struct fastbuf *
merge(struct fastbuf *oldi, struct fastbuf *feedback)
{
  struct url_state s;
  struct feedback_gatherer fb;
  struct footprint fb_fp;
  struct fastbuf *newi = temp_state_file();
  uns cnt = 0, cnt_fb = 0;

  next_feedback(feedback, &fb, &fb_fp);
  while (breadb(oldi, &s, sizeof(s)))
    {
      if (s.flags & USF_ROBOTS)
	{
	  /* Robot files and skeys always have their weight forced to maximum */
	  s.weight = 255;
	  s.flags |= USF_TRUE_WEIGHT;
	}
      else
	{
	  int m;
	  while ((m = fp_cmp(&fb_fp, &s.fp)) < 0)
	    next_feedback(feedback, &fb, &fb_fp);
	  s.type &= ~UST_NO_TARGET;
	  s.flags &= ~(USF_TRUE_WEIGHT | USF_UNREF);
	  if (!m)
	    {
	      s.weight = fb.weight;
	      s.flags |= USF_TRUE_WEIGHT;
	      if (!(fb.flags & CARD_NOTE_IS_LINKED))
	        s.flags |= USF_UNREF;
	      if ((fb.flags & (CARD_NOTE_REDIRECT | CARD_NOTE_HAS_TARGET)) == CARD_NOTE_REDIRECT)
		s.type |= UST_NO_TARGET;
	      cnt_fb++;
	    }
	  cnt++;
	}
      bwrite(newi, &s, sizeof(s));
    }

  log(L_INFO, "Merged feedback to %d of %d entries", cnt_fb, cnt);
  return newi;
}

static void NONRET
usage(void)
{
  fprintf(stderr, "Usage: shep-feedback [<config-options>] <state> <feedback-file>\n");
  exit(1);
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) > 0 || optind != argc-2)
    usage();

  byte *state = argv[optind];
  byte *feedback_file = argv[optind+1];
  struct fastbuf *feedback = bopen(feedback_file, O_RDONLY, 65536);

  log(L_INFO, "Sorting original index");
  struct fastbuf *old_index = read_state_file(state, "index");
  old_index = url_state_by_fp_sort(old_index, NULL);

  log(L_INFO, "Merging feedback to index");
  struct fastbuf *new_index = merge(old_index, feedback);
  bclose(old_index);
  bclose(feedback);
  put_state_file(state, "index", new_index, 0);
  state_flags_set(state, STATE_FLAG_SORTED);

  return 0;
}
