/*
 *	Sherlock Indexer -- Computation of Site ID's from their hashes
 *
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "ucw/fastbuf.h"
#include "sherlock/index.h"
#include "indexer/indexer.h"
#include "indexer/sites.h"
#include "indexer/params.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>

struct site_resolver
{
  site_hash_t hash;
  uns id;
};

#define HASH_NODE	struct site_resolver
#define HASH_PREFIX(x)	sr_##x
#define HASH_KEY_MEMORY	hash
#define HASH_KEY_SIZE	SITE_HASH_SIZE
#define HASH_WANT_LOOKUP
#define HASH_WANT_CLEANUP
#define HASH_ZERO_FILL
#define HASH_DEFAULT_SIZE	16384
#define HASH_AUTO_POOL	16384

#include "ucw/hashtable.h"

static struct site_resolver **sites;

#define ASORT_PREFIX(x)	sa_##x
#define ASORT_KEY_TYPE	struct site_resolver *
#define ASORT_ELT(i)	sites[i]
#define ASORT_LT(x,y)	(memcmp(x->hash, y->hash, SITE_HASH_SIZE) < 0)

#include "ucw/sorter/array-simple.h"

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }

  log(L_INFO, "Extracting site hashes");
  struct fastbuf *fb_notes = index_bopen(fn_notes, O_RDONLY, 1);
  struct card_note note;
  sr_init();
  while (breadb(fb_notes, &note, sizeof(struct card_note)))
    sr_lookup(note.site_hash);
  brewind(fb_notes);

  uns count = 0;
  HASH_FOR_ALL(sr, node)
    count++, node;
  HASH_END_FOR;
  sites = big_alloc(count * sizeof(struct site_resolver *));
  uns i = 0;
  HASH_FOR_ALL(sr, node)
    sites[i++] = node;
  HASH_END_FOR;

  log(L_INFO, "Sorting %d site hashes", count);
  sa_sort(count);

  log(L_INFO, "Writing site IDs");
  struct fastbuf *fb_sites = index_bopen(fn_sites, O_CREAT | O_TRUNC | O_WRONLY, 1);
  site_hash_t hash0;
  bzero(&hash0, sizeof(hash0));
  bwrite(fb_sites, &hash0, SITE_HASH_SIZE);
  for (i=0; i<count; i++)
  {
    sites[i]->id = i+1;
    bwrite(fb_sites, sites[i]->hash, SITE_HASH_SIZE);
  }
  bclose(fb_sites);
  big_free(sites, count * sizeof(struct site_resolver *));

  attrs_part_map(1);
  i = 0;
  while (bread(fb_notes, &note, sizeof(struct card_note)))
  {
    struct site_resolver *sr = sr_lookup(note.site_hash);
    bring_attr(i++)->site_id = sr->id;
  }
  attrs_part_unmap();
  bclose(fb_notes);
  sr_cleanup();

  struct index_params params;
  params_load(&params);
  params.sites = count+1;
  params_save(&params);

  return 0;
}
