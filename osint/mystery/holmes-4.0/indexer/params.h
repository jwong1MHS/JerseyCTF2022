/*
 *	Sherlock: The index parameter file
 *
 *	(c) 2003--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2005 Robert Spalek <robert@ucw.cz>
 */

#ifndef _SHERLOCK_INDEXER_PARAMS_H
#define _SHERLOCK_INDEXER_PARAMS_H

#include "indexer/lexicon.h"

struct index_params {
  u32 version;				/* INDEX_VERSION */
  ucw_time_t ref_time;			/* Reference time (for document ages etc.) */
  struct lexicon_config lex_config;
  u32 objects_in;			/* Number of objects on the input of the indexer */
  u32 sites;				/* Number of unique sites */
  uns srand;				/* Random number generator setting for signatures */
  u32 database_version;			/* Database version of this index */
  u16 type_mask;			/* Split indices: data type mask */
  u16 id_mask;				/* Split indices: document ID mask */
  u32 num_slices;			/* Number of slices per reference chain */
  u32 cards_out;			/* Number of cards generated */
};

static inline void
get_slice_start(struct index_params *par, uns *slice_start)
{
  // slice i contains OIDs from the range [ slice_start[i], slice_start[i+1] )
  for (uns i = 0; i <= par->num_slices; i++)
    slice_start[i] = (u64)i * par->cards_out / par->num_slices;
}

struct odes;

void params_to_obj(struct index_params *par, struct odes *obj, uns final);

#endif
