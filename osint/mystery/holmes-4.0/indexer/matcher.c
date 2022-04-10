/*
 *	Matcher -- functions for computing signatures
 *
 *	(c) 2002--2004, Robert Spalek <robert@ucw.cz>
 *
 *	Based on the article by Andrei Z. Broder (1997):
 *		On the resemblance and containment of documents
 *		http://citeseer.nj.nec.com/broder97resemblance.html
 */

#undef	LOCAL_DEBUG

#include "sherlock/sherlock.h"

#include <stdlib.h>
#include <string.h>

#include "sherlock/index.h"
#include "sherlock/tagged-text.h"
#include "ucw/hashfunc.h"
#include "sherlock/object.h"
#include "indexer/indexer.h"
#include "indexer/matcher.h"
#include "indexer/lexicon.h"

static void lm_init(void);

void
matcher_init(void)
{
	lm_init();
}

static u32
gcd32(u32 a, u32 b)
{
	while (b)
	{
		u32 c = a % b;
		a = b;
		b = c;
	}
	return a;
}

#define	BIG_RANDOM	((random() & 0x7fffffff) | 0x80000000)
static struct hash_permuter *
permuter_new(void)
{
	struct hash_permuter *pert;
	uns i;
	if (!matcher_signatures)
		return NULL;
	pert = xmalloc_zero(matcher_signatures * sizeof(struct hash_permuter));
	for (i=0; i<matcher_signatures; i++)
	{
		uns iteration = 0;
		while (1)
		{
			uns j;
			iteration++;
			pert[i].mult = BIG_RANDOM | 1;
			for (j=0; j<i; j++)
				if (gcd32(pert[i].mult, pert[j].mult) > 1)
					break;
			if (j >= i)
				break;
		}
		DBG("%d-th modulo %08x found at the %d-th iteration", i, pert[i].mult, iteration);
	}
	return pert;
}

/* A random perturbation of an integer hash.  The permutation is simulated by a
 * linear congruent pseudorandom generator chosen by random. :-)  */

static inline u32 CONST
hash_modify(u32 hash, u32 mult)
{
	return hash * mult;
}

static void
update_minima(u32 *min, u32 hash, struct hash_permuter *pert)
{
	uns i;
	for (i=0; i<matcher_signatures; i += 2)
	{
		u32 tmp = hash_modify(hash, pert[i].mult);
		if (tmp < min[i])
			min[i] = tmp;
		if (tmp > min[i+1])
			min[i+1] = tmp;
	}
}

/* A cyclic hash is a linear combination of CONTEXT consecutive integers shifted
 * by SHIFT bits.  To compute a sequence of all cyclic hashes, it suffices to
 * update the last HASH by the following function instead of recomputing it
 * from scratch.  */

/* How big shift shall be taken when deleting an integer from a cyclic hash.  */
#define	CYCLIC_DELETE_SHIFT(context, shift)	(context * shift % (sizeof(u32)*8))

/* The shift of the hash between consecutive words.  */
#define	CYCLIC_SHIFT	13

static inline u32 CONST
cyclic_hash_update(u32 hash, uns shift, uns delete_shift, u32 added, u32 deleted)
{
	return ROL(hash, shift) ^ added ^ ROL(deleted, delete_shift);
}

/*
 * Call-backs called by the cutter-into-words indexer/lexmap.h
 */

typedef int word_id_t;

static enum word_class
lm_lookup(enum word_class orig_class, u16 *uni, uns ulen, word_id_t *idp UNUSED, struct matcher_context *ctxt)
{
	u32 new_hash;

	if (orig_class != WC_NORMAL)
		return orig_class;
	new_hash = hash_block((void *) uni, ulen * sizeof(u16));
		/* The SHIFT_BITS==7, hence it is efficient for UCS-2 strings too.  */
	if (ctxt->sign_total_words >= ctxt->sign_context)
	{
		ctxt->sign_cyclic_hash = cyclic_hash_update(ctxt->sign_cyclic_hash,
			CYCLIC_SHIFT, ctxt->sign_delete_shift,
			new_hash, ctxt->sign_last_word_hashes[ctxt->sign_deleting]);
		ctxt->sign_last_word_hashes[ctxt->sign_deleting++] = new_hash;
		if (ctxt->sign_deleting >= ctxt->sign_context)
			ctxt->sign_deleting = 0;
	}
	else
	{
		ctxt->sign_cyclic_hash = cyclic_hash_update(ctxt->sign_cyclic_hash,
			CYCLIC_SHIFT, 0, new_hash, 0);
		ctxt->sign_last_word_hashes[ctxt->sign_total_words] = new_hash;
	}
	ctxt->sign_total_words++;
	update_minima(ctxt->sign_min, ctxt->sign_cyclic_hash, ctxt->perm);
	return WC_IGNORED;
}

static inline void
lm_got_word(uns pos UNUSED, uns cat UNUSED, word_id_t w UNUSED, struct matcher_context *ctxt UNUSED)
{
	/* We do not need this call-back.  */
}

static inline void
lm_got_complex(uns pos UNUSED, uns cat UNUSED, word_id_t root UNUSED, word_id_t w UNUSED, uns dir UNUSED, struct matcher_context *ctxt UNUSED)
{
	/* We do not need this call-back.  */
}

/* Import a simplified version of the cutter-into-words.  */
#define LM_MULTI
#include "indexer/lexmap.h"

/*
 * 1.  Properties of a hash
 *
 * Let us assume the cyclic hash is a 32-bit fixed point random number.  If we
 * approximate it by a random real number X in interval <0,1), then its
 * distribution function is D_1(x)=x.  It is straightforward, that the minimum
 * Y of n independent numbers has the distribution function D_n(x)=1-(1-x)^n.
 * The density of the random variable Y is d_n(x)=n*(1-x)^(n-1).
 *
 * Further, the expected value of Y is 1/(n+1).  This means that, most
 * probably, the log_2(n+1) upper bits of the appropriate hash will be zero.
 * For a huge document comprising of, say, 8000 words, the hash will contain
 * about 32-13=19 significant digits with a high probability.
 *
 * Since the probabilistic distribution of Y is not uniform, it is not
 * apparent, what is the probability that 2 independent hashes Y_1, Y_2 are
 * equal.  Numeric calculations show, that, for n>30, P(Y_1=Y_2) ~~ (n+1)/2^33,
 * i.e. P(Y_1=Y_2) ~~ 1/2Exp(Y), which is the same as in the uniform case.
 *
 * We conclude that two distinct documents with fewer than 8000 words have
 * equal hashes with the probability less than 1:milion.  If the sequences of
 * hashes of two documents are equal, so are the original documents with a high
 * probability.
 *
 * Let the documents have N words and let us choose M particular words.  The
 * probability that the minimal hash belongs to this subset, is obviously M/N.
 * Therefore is two documents are equal with precision P percent (i.e. their
 * cyclic hashes are equal at P*N/100 places), the probability is obtaining
 * equal hashes is P/100 (neglecting the 1:milion probability).
 *
 * 2.  Producing many hashes
 *
 * The signature of a document comprises of several independent hashes.  Let X
 * be the original hash.  We define X_i = X * a_i mod 2^32.  Let us assume
 * a_i's are prime to each other and they all are prime to 2^32.  Hence each of
 * them has an inversion a_i^-1 in this ring.
 *
 * We prove that the secondary hashes X_i's are *almost* independent.  The
 * exact independence can not be achieved, since one can be computed exactly
 * from the another one.  However it holds, that P(X_j < t_j) ~~ P(X_j < t_j |
 * X_i < t_i) for each i, j and reasonably large thresholds t_i, t_j.  This is
 * because X_j = (a_j*a_i^-1) * X_i mod 2^32, and hence the lower interval X_i
 * in <0,t_i) is distributed almost uniform in <0,1) when mapped to X_j.
 *
 * We conclude that the presented mapping yields almost independent hashes.
 * This can be combined with the previous section.  By comparing two signatures
 * and counting the number of equal hashes, we can estimate the similarity P of
 * the two documents.
 *
 * This function returns the number of words the document has.  It is not
 * re-entrant.
 */
uns
matcher_compute_minima(u32 *min, struct matcher_context *ctxt, struct oattr *oa)
{
	ctxt->sign_cyclic_hash = 0;
	if (ctxt->sign_context != matcher_context)
	{
		ctxt->sign_context = matcher_context;
		ctxt->sign_last_word_hashes = xrealloc(ctxt->sign_last_word_hashes, ctxt->sign_context * sizeof(u32));
		ctxt->sign_delete_shift = CYCLIC_DELETE_SHIFT(matcher_context, CYCLIC_SHIFT);
	}
	ctxt->sign_deleting = ctxt->sign_total_words = 0;
	ctxt->sign_min = min;

	for (uns i=0; i<matcher_signatures; i += 2)
	{
		min[i] = ~0U;
		min[i+1] = 0;
	}
	struct lm_state lm;
	lmap_doc_start(&lm, ctxt);
	while (oa)
	{
		lmap_map_text(&lm, oa->val, oa->val + str_len(oa->val));
		oa = oa->same;
	}
	for (uns i=1; i<matcher_signatures; i += 2)
	  min[i] ^= ~0U;
	return ctxt->sign_total_words;
}

struct matcher_context *
matcher_new(struct matcher_context *clone)
{
	struct matcher_context *ctxt = xmalloc_zero(sizeof(*ctxt));
	if (clone)
		ctxt->perm = clone->perm;
	else
		ctxt->perm = permuter_new();
	return ctxt;
}
