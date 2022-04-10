/*
 *	Sherlock Indexer -- Merging Documents According to Signatures
 *
 *	(c) 2002--2004, Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "sherlock/object.h"
#include "ucw/bitarray.h"
#include "indexer/indexer.h"
#include "indexer/merges.h"
#include "indexer/matcher.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <alloca.h>

#define MAX_SIGNATURES	128
#define	COMPARED	8
struct sign_key
{
	area_t area;
	u32 cardid;
	u32 sign[MAX_SIGNATURES];
};

struct signature
{
	u32 cardid;
	u32 sign[0];
};

/* Opened files, computed constants, and counters.  */
static bitarray_t untouchable;
static struct fastbuf *matches;
static uns distinct_limit, record_size, starting_key, key_size, sign_size;
static uns skipped, similar;

#ifdef CONFIG_AREAS
static u32 *card_areas;
#endif

/*** Sorting the Signatures file.  ***/

#define SORT_PREFIX(x) sign_##x
#define SORT_KEY struct sign_key
#define SORT_KEY_SIZE(k) key_size
#define SORT_INPUT_FILE
#define SORT_OUTPUT_FB

static inline int
sign_compare(struct sign_key *a, struct sign_key *b)
{
	int i;
#ifdef	CONFIG_AREAS
	COMPARE(a->area, b->area);
#endif
	for (i=0; i<COMPARED; i++)
		COMPARE(a->sign[starting_key + i], b->sign[starting_key + i]);
	return 0;
}

static int
sign_read_key(struct fastbuf *f, struct sign_key *k)
{
	uns cardid;
	while ((cardid = bgetl(f)) != ~0U)
	{
		if (!bit_array_isset(untouchable, cardid) && (int)merges[cardid] < 0)
		{
			k->cardid = cardid;
			breadb(f, k->sign, sign_size);
#ifdef	CONFIG_AREAS
			k->area = card_areas[cardid];
#endif
			return 1;
		}
		/* Untouchable or already marked as a duplicate.  */
		bskip(f, sign_size);
		skipped++;
	}
	return 0;
}

static void
sign_write_key(struct fastbuf *f, struct sign_key *k)
{
	bputl(f, k->cardid);
	bwrite(f, k->sign, sign_size);
}

#include "ucw/sorter/sorter.h"

static uns
find_random_key(uns *forbidden_keys, uns count)
{
	while (1)
	{
		uns key, i;
		key = random() % (matcher_signatures - COMPARED + 1);
		for (i=0; i<count; i++)
			if (key == forbidden_keys[i])
				break;
		if (i >= count)
			return key;
	}
}

/*** Processing the sorted Signatures file.  ***/

static inline int
similar_documents(struct signature *a, struct signature *b)
{
	uns distinct = 0, i;
	for (i=0; i<matcher_signatures; i++)
		if (a->sign[i] != b->sign[i])
		{
			if (distinct >= distinct_limit)
				return 0;
			distinct++;
		}
	if (matches)
	{
		char tmp[128];
		sprintf(tmp, "%x\t%x\t%d\n", a->cardid, b->cardid, distinct);
		bputs(matches, tmp);
	}
	return 1;
}

static int
process_block(struct fastbuf *f, void *buffer, uns cached)
{
	struct signature *first = buffer, *curr, *last = buffer;
	uns read = 0;
	int eof = 0;
	int i, j;

	if (!cached && !breadb(f, buffer, record_size))
		return 0;
	buffer += record_size;
	read++;
	ASSERT(matcher_block > 1);
	while (read < matcher_block)
	{
		if (!breadb(f, buffer, record_size))
		{
			eof = 1;
			break;
		}
		curr = last = buffer;
		if (curr->sign[starting_key] != first->sign[starting_key]
#ifdef	CONFIG_AREAS
		|| card_areas[curr->cardid] != card_areas[first->cardid]
#endif
		    )
			break;
		buffer += record_size;
		read++;
	}
	buffer = first;
	for (i=1; i<(int) read; i++)
	{
		curr = buffer + i * record_size;
		for (j=i-1; j>=0; j--)
		{
			first = buffer + j * record_size;
			if (similar_documents(first, curr))
			{
				similar++;
				merges_union(first->cardid, curr->cardid);
				break;
			}
		}
	}
	/* If !eof, we have read one signature more than we wanted (either we
	 * have reached a new key hash, or the buffer is full and we want to
	 * overlap the last record).  */
	if (!eof && last != buffer)
		memcpy(buffer, last, record_size);
	return !eof;
}

/*
 * This procedure reads a signature file sorted on a hash chosen by random and
 * tries to identify similar documents.  Since similar documents have about 90%
 * of the hashes equal, it is highly probable that the sorting key belongs to
 * this set at least in one of the passes (99% for 2 passes, 99.9% for 3
 * passes).  Hence it suffices to process separately documents for every value
 * of the sorting key.  We read signatures according to a fixed key into memory
 * and find similarities.  It would be ideal to read all of them, but since
 * the algorithm is quadratic, we process the signatures in blocks of size 64
 * --- it does not matter so much.
 */
static void
process_file(struct fastbuf *signatures)
{
	void *buffer = alloca(matcher_block * record_size);
	uns cached = 0;
	while (process_block(signatures, buffer, cached++));
}

static void
find_similarities(void)
{
	uns forbid[matcher_passes];
	uns i;

	srand(time(NULL));
	if (matcher_signatures < 2*COMPARED)
		die("Too small value Matcher.Signatures = %d, minimal value is %d", matcher_signatures, 2*COMPARED);
	if (matcher_signatures > MAX_SIGNATURES)
		die("Too large value Matcher.Signatures = %d, maximal value is %d", matcher_signatures, MAX_SIGNATURES);
	distinct_limit = matcher_signatures - matcher_threshold;
	sign_size = matcher_signatures * sizeof(u32);
	record_size = sizeof(u32) + sign_size;
	key_size = OFFSETOF(struct sign_key, sign) + sign_size;

	for (i=0; i<matcher_passes; i++)
	{
		struct fastbuf *signatures;

		starting_key = forbid[i] = find_random_key(forbid, i);
		skipped = 0;
		signatures = sign_sort(index_name(fn_signatures), NULL);
		log(L_INFO, "Sorted signatures on the %d-th hash, skipped %d documents", starting_key, skipped);

		similar = 0;
		process_file(signatures);
		bclose(signatures);
		if (similar)
			log(L_INFO, "Found %d similar documents", similar);
	}
}

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

	if (!matcher_signatures || !index_name_defined(fn_signatures))
		return 0;

	log(L_INFO, "Browsing attributes");
	merges_map(1);
	untouchable = big_alloc_zero(BIT_ARRAY_BYTES(card_count));
	attrs_part_map(0);
	for (uns i=0; i<card_count; i++)
		if (bring_attr(i)->flags & (CARD_FLAG_EMPTY | CARD_FLAG_FRAMESET))
			bit_array_set(untouchable, i);
#ifdef CONFIG_AREAS
	READ_ATTR(card_areas, area);
#endif
	attrs_part_unmap();

	/* Images and audio files are untouchable, too */
	notes_part_map(0);
	for (uns i=0; i<card_count; i++)
		if (bring_note(i)->flags & (CARD_NOTE_IMAGE | CARD_NOTE_AUDIO))
			bit_array_set(untouchable, i);
	notes_part_unmap();

	log(L_INFO, "Merging documents according to signatures");
	if (fn_matches)
		matches = index_bopen(fn_matches, O_CREAT | O_TRUNC | O_WRONLY, 1);
	else
		matches = NULL;

	find_similarities();

	big_free(untouchable, BIT_ARRAY_BYTES(card_count));
	merges_unmap();
	if (matches)
		bclose(matches);

	return 0;
}
