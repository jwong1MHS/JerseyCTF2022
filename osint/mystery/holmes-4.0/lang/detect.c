/*
 *	Language detector
 *
 * 	(c) 2003, Robert Spalek <robert@ucw.cz>
 * 	(c) 2006--2007, Pavel Charvat <pchar@ucw.cz>
 *
 * 	Inspired by open-source program `text_cat'
 * 		http://odur.let.rug.nl/~vannoord/TextCat/
 * 	based on the text categorization algorithm presented in
 * 		Cavnar, W. B.  and J. M. Trenkle,
 * 		``N-Gram-Based Text Categorization''
 * 	In Proceedings of Third Annual Symposium on Document Analysis and
 * 	Information Retrieval, Las Vegas, NV, UNLV Publications/Reprographics,
 * 	pp. 161-175, 11-13 April 1994.
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/math.h"
#include "sherlock/tagged-text.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "ucw/unicode.h"
#include "ucw/fastbuf.h"
#include "lang/lang.h"
#include "lang/detect.h"
#include "charset/unicat.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/***** Configuration *****/

uns lang_detect_nr_langs;
struct lang_detect_lang_flag lang_detect_lang_flags[MAX_DETECTED];

uns lang_detect_nr_sequences, lang_detect_max_sequences, lang_detect_total_seq_len;
struct lang_detect_sequence **lang_detect_sequences;

uns lang_detect_mode;			/* Configured here, but used outside */
uns lang_detect_min_doc_length;
char *lang_detect_tables_file;

static byte *
reset(uns n UNUSED, byte **pars, void *ptr UNUSED)
{
	/*
	 * XXX: Journaling of the configuration relies on people using Reset properly.
	 * However, the detector config files are auto-generated, so it's probably
	 * a fair assumption.
	 */

	uns max;
	byte *err = cf_parse_int(pars[0], &max);
	if (err)
		return err;

	CF_JOURNAL_VAR(lang_detect_nr_langs);
	CF_JOURNAL_VAR(lang_detect_lang_flags);
	CF_JOURNAL_VAR(lang_detect_nr_sequences);
	CF_JOURNAL_VAR(lang_detect_max_sequences);
	CF_JOURNAL_VAR(lang_detect_total_seq_len);
	CF_JOURNAL_VAR(lang_detect_sequences);

	lang_detect_nr_langs = 0;
	bzero(lang_detect_lang_flags, sizeof(lang_detect_lang_flags));
	lang_detect_nr_sequences = 0;
	lang_detect_max_sequences = max;
	lang_detect_total_seq_len = 0;
	lang_detect_sequences = cf_malloc(lang_detect_max_sequences * sizeof(struct lang_detect_sequence *));
	return NULL;
}

static byte *
add_language(uns n UNUSED, byte **pars, byte *ptr)
{
	if (lang_detect_nr_sequences)
		return "Too late to tie a language";
	if (lang_detect_nr_langs >= MAX_DETECTED)
		return "Too many languages";
	int lang = lang_name_to_code(pars[0]);
	if (lang < 0)
		return "Unknown language";
	ASSERT(lang >= 0 && lang < MAX_DETECTED);
	uns is_accented = !ptr;
	for (uns i=0; i<lang_detect_nr_langs; i++)
		if (lang_detect_lang_flags[i].id == lang
		&& lang_detect_lang_flags[i].is_accented == is_accented)
			return "The language variant has already been tied";
	uns s, t, f;
	byte *err;
	if ((err = cf_parse_int(pars[1], &s)) || (err = cf_parse_int(pars[2], &t)) || (err = cf_parse_int(pars[3], &f)))
		return err;
	struct lang_detect_lang_flag *r = lang_detect_lang_flags + lang_detect_nr_langs++;
	r->id = lang;
	r->is_accented = is_accented;
	r->nr_seq = s;
	r->rel_threshold = t;
	r->freq_threshold = f;
	return NULL;
}

static byte *
add_sequence(uns n, byte **pars, byte *ptr UNUSED)
{
	if (!lang_detect_nr_langs)
		return "No language has been tied";
	if (n != lang_detect_nr_langs+1)
		return "TIED_LANG_COUNT+1 fields expected";
	uns ul = utf8_strlen(pars[0]);
	if (ul<1 || ul>MAX_SEQ_LENGTH)
		return "The sequence must be from 1 to MAX_SEQ_LENGTH characters long";
	struct lang_detect_sequence *seq;
	for (uns i=0; i<lang_detect_nr_sequences; i++)
		if (!strcmp(lang_detect_sequences[i]->text, pars[0]))
			return "The sequence has already been defined";
	seq = cf_malloc_zero(sizeof(struct lang_detect_sequence));
	seq->text = pars[0];
	seq->len = ul;
	lang_detect_total_seq_len += ul;
	for (uns i=0; i<lang_detect_nr_langs; i++)
	{
		byte *err;
		int f;
		if (err = cf_parse_int(pars[i+1], &f))
			return err;
		if (f < 0)
			return "Order cannot be a non-negative number";
		if (f > (int) lang_detect_lang_flags[i].nr_seq)
			return "Order cannot be higher than the number of sequences of the language";
		seq->order[i] = f;
	}
	if (lang_detect_nr_sequences >= lang_detect_max_sequences)
		return "Too many sequences in comparison with the upper-bound";
	lang_detect_sequences[lang_detect_nr_sequences++] = seq;
	return NULL;
}

static struct cf_section lang_detect_config = {
	CF_ITEMS {
		CF_PARSER("Reset", NULL, reset, 1),
		CF_PARSER("AddNormalLanguage", NULL, add_language, 4),
		CF_PARSER("AddCutAccentLanguage", "1", add_language, 4),
		CF_PARSER("SequenceFrequencies", NULL, add_sequence, CF_ANY_NUM),
		CF_UNS("MinDocumentLength", &lang_detect_min_doc_length),
		CF_STRING("IncludeTables", &lang_detect_tables_file),
		CF_UNS("Mode", &lang_detect_mode),
		CF_END
	}
};

static void CONSTRUCTOR
lang_detect_init(void)
{
	cf_declare_section("LangDetect", &lang_detect_config, 0);
}

/* Depth limit of the automaton... the algorithm is optimized for very small depths */
#define AUT_MAX_DEPTH	8

struct kmp_struct;

static void *kmp_alloc(struct kmp_struct *kmp, uns size);

static inline void
kmp_free(struct kmp_struct *kmp UNUSED, void *ptr UNUSED)
{}

/* Main structures and construction routines */
#define KMP_PREFIX(x) kmp_##x
#define KMP_USE_UTF8
#define KMP_CONTROL_CHAR ':'
#define KMP_VARS struct kmp_state **end[AUT_MAX_DEPTH], **start[AUT_MAX_DEPTH]; uns depth;
#define KMP_STATE_VARS uns id, count;
#define KMP_ADD_EXTRA_ARGS uns id
#define KMP_ADD_NEW(kmp,src,s)							\
	do{									\
		if (s->len > AUT_MAX_DEPTH)					\
			die("AUT_MAX_DEPTH in lang/detect.c is too small");	\
		kmp->u.depth = MAX(kmp->u.depth, s->len);			\
		s->u.id = id;							\
	}while(0)
#define KMP_GIVE_ALLOC
#include "ucw/kmp.h"

struct lang_detect {
	struct kmp_struct aut;
	struct lang_detect_results results;
	struct mempool *pool;
	uns len;
};

static void *
kmp_alloc(struct kmp_struct *kmp, uns size)
{
	return mp_alloc(SKIP_BACK(struct lang_detect, aut, kmp)->pool, size);
}

void
lang_detect_load_tables(void)
{
	if (!lang_detect_tables_file)
		die("Set LangDetect.IncludeTables to the file with language recognition tables");
	if (cf_load(lang_detect_tables_file))
		die("Cannot load language recognition tables (%s)", lang_detect_tables_file);
}

struct lang_detect *
lang_detect_alloc(struct mempool *pool)
{
	pool = pool ? : cf_pool;
	struct lang_detect *ld = mp_alloc_zero(pool, sizeof(*ld));
	ld->pool = pool;

	/* Build a new automaton from the sequences.  */
	kmp_init(&ld->aut);
	for (uns i = 0; i < lang_detect_nr_sequences; i++)
		kmp_add(&ld->aut, lang_detect_sequences[i]->text, i);
	kmp_build(&ld->aut);
	if (lang_detect_nr_sequences)
		for (uns i = 0; i < ld->aut.u.depth; i++)
			ld->aut.u.start[i] = ld->aut.u.end[i] = mp_alloc(pool, lang_detect_nr_sequences * sizeof(void *));
	ld->results.sf = lang_detect_nr_sequences ?
		mp_alloc(pool, lang_detect_nr_sequences * sizeof(struct sequence_freq)) : NULL;
	ld->results.variances = lang_detect_nr_sequences ?
		mp_alloc(pool, lang_detect_nr_sequences * sizeof(uns)) : NULL;

	return ld;
}

void
lang_detect_start(struct lang_detect *ld)
{
  ld->len = 0;
}

/* Zero-terminated string source */

static inline void
found_chain(struct kmp_struct *kmp, struct kmp_state *s)
{
	if (!s->u.count++)
		*((kmp->u.end[s->len - 1])++) = s;
}

static inline uns
format_char(uns w)
{
	return (w >= 0x80000000 || !Ualpha(w)) ? ':' : Utolower(w);
}

#define KMPS_PREFIX(x) kmp_##x
#define KMPS_KMP_PREFIX(x) GLUE_(kmp,x)
#define KMPS_FOUND_CHAIN(kmp,src,s) found_chain(kmp, s->out)
#define KMPS_ADD_CONTROLS
#define KMPS_MERGE_CONTROLS
#define KMPS_VARS struct lang_detect *ld;
#define KMPS_STEP(kmp,src,s) s->u.ld->len++
#define KMPS_GET_CHAR(kmp,src,s) ({			\
	uns w, result;					\
	GET_TAGGED_CHAR(src, w);			\
	if (!w)						\
		result = 0;				\
	else						\
	{						\
		s->c = format_char(w);			\
		result = 1;				\
	}						\
	result; })
#include "ucw/kmp-search.h"

void
lang_detect_add_string(struct lang_detect *ld, byte *str)
{
	struct kmp_search s;
	s.u.ld = ld;
	kmp_search(&ld->aut, &s, str);
}

/* Fastbuf source */

#define KMPS_PREFIX(x) kmp_fastbuf_##x
#define KMPS_KMP_PREFIX(x) GLUE_(kmp,x)
#define KMPS_FOUND_CHAIN(kmp,src,s) found_chain(kmp, s->out)
#define KMPS_ADD_CONTROLS
#define KMPS_MERGE_CONTROLS
#define KMPS_SOURCE struct fastbuf *
#define KMPS_VARS struct lang_detect *ld;
#define KMPS_STEP(kmp,src,s) s->u.ld->len++
#define KMPS_GET_CHAR(kmp,src,s) ({			\
	uns w, result;					\
	w = bget_tagged_char(src);			\
	if ((int)w == -1)				\
		result = 0;				\
	else						\
	{						\
		s->c = format_char(w);			\
		result = 1;				\
	}						\
	result; })
#include "ucw/kmp-search.h"

void
lang_detect_add_fastbuf(struct lang_detect *ld, struct fastbuf *fb)
{
	struct kmp_fastbuf_search s;
	s.u.ld = ld;
	kmp_fastbuf_search(&ld->aut, &s, fb);
}

#define	ASORT_PREFIX(x)	seq_freq_##x
#define	ASORT_KEY_TYPE	uns
#define	ASORT_ELT(i)	-array[i].occur
#define	ASORT_SWAP(i,j)	do { struct sequence_freq tmp=array[j]; array[j]=array[i]; array[i]=tmp; } while(0)
#define	ASORT_EXTRA_ARGS	, struct sequence_freq *array
#include "ucw/sorter/array-simple.h"

#define	TRACE(mask,par...)	if (0) fprintf(stderr, mask "\n",##par)

static uns
probability_of_language(uns id, struct sequence_freq *sorted_freqs, uns count)
{
	uns result = 0;
	TRACE("Computing frequence of language %d, I found %d/%d sequences:", id, count, lang_detect_nr_sequences);
	if (count > lang_detect_lang_flags[id].nr_seq)
	{
		count = lang_detect_lang_flags[id].nr_seq;
		TRACE("The count cut down to %d", lang_detect_lang_flags[id].nr_seq);
	}
	for (uns i=0; i<count; i++)
	{
		uns expected_pos = lang_detect_sequences[ sorted_freqs[i].id ]->order[id];
		if (expected_pos)
		{
			result += abs(expected_pos - i);
			TRACE("\tSeq #%d with %d occ. should be at #%d, += difference %d",
				i, sorted_freqs[i].occur, expected_pos, abs(expected_pos - i));
		}
		else
		{
			result += lang_detect_lang_flags[id].nr_seq;
			TRACE("\tSeq #%d with %d occ. should not be present, += penalty %d",
				i, sorted_freqs[i].occur, lang_detect_lang_flags[id].nr_seq);
		}
	}
	TRACE("Total variance is %d", result);
	return result;
}

struct lang_detect_results *
lang_detect_compute(struct lang_detect *ld)
{
	ld->results.nonzero_seq = 0;
	ld->results.total_occur = 0;
	for (uns i = ld->aut.u.depth; i--; )
		while (ld->aut.u.end[i] != ld->aut.u.start[i])
		{
			struct kmp_state *s = *(--(ld->aut.u.end[i]));
		        ld->results.sf[ld->results.nonzero_seq].id = s->u.id;
			ld->results.sf[ld->results.nonzero_seq++].occur = s->u.count;
			ld->results.total_occur += s->u.count;
			DBG("Found %d occurences of %s (id=%d)", s->u.count, lang_detect_sequences[s->u.id]->text, s->u.id);
			if (s->next)
				if (s->next->u.count)
					s->next->u.count += s->u.count;
				else
				{
					s->next->u.count = s->u.count;
					*((ld->aut.u.end[s->next->len - 1])++) = s->next;
				}
			s->u.count = 0;
		}

	ASSERT(ld->results.nonzero_seq <= lang_detect_nr_sequences);
	seq_freq_sort(ld->results.nonzero_seq, ld->results.sf);

	if (ld->results.total_occur >= lang_detect_min_doc_length
	&& lang_detect_nr_langs >= 1)
	{
		ld->results.lang1 = 0;
		for (uns i=0; i<lang_detect_nr_langs; i++)
		{
			ld->results.variances[i] = probability_of_language(i, ld->results.sf, ld->results.nonzero_seq);
			if (i > 0 && ld->results.variances[i] < ld->results.variances[ ld->results.lang1 ])
				ld->results.lang1 = i;
		}
		ld->results.lang2 = lang_detect_nr_langs;
		for (uns i=0; i<lang_detect_nr_langs; i++)
		{
			/* Do not compare it with the other accented variant of the same language.  */
			if (lang_detect_lang_flags[i].id != lang_detect_lang_flags[ ld->results.lang1 ].id
			&& (ld->results.lang2 == (int) lang_detect_nr_langs
			|| ld->results.variances[i] < ld->results.variances[ ld->results.lang2 ]))
				ld->results.lang2 = i;
		}
		if (ld->results.lang2 == (int) lang_detect_nr_langs)
			ld->results.lang2 = ld->results.lang1;
		ld->results.ratio = 1000 * ld->results.variances[ ld->results.lang2 ] / ld->results.variances[ ld->results.lang1 ];
		ld->results.min_ratio = lang_detect_lang_flags[ ld->results.lang1 ].rel_threshold;
		uns occur = 0;
		for (uns i=0; i<ld->results.nonzero_seq; i++)
		  if (lang_detect_sequences[ ld->results.sf[i].id ]->order[ ld->results.lang1 ])
		    occur += ld->results.sf[i].occur;
		ld->results.freq = ld->len ? 1000 * occur / ld->len : 0;
		ld->results.min_freq = lang_detect_lang_flags[ ld->results.lang1 ].freq_threshold;
	}
	else
	{
		ld->results.lang1 = ld->results.lang2 = -1;
		for (uns i=0; i<lang_detect_nr_langs; i++)
			ld->results.variances[i] = 0;
		ld->results.ratio = 0;
		ld->results.min_ratio = 1;
	}
	return &ld->results;
}

uns
lang_detect_choose_best(struct lang_detect *ld)
{
	struct lang_detect_results *res = lang_detect_compute(ld);
	DBG("ratio=%d/%d freq=%d/%d", res->ratio, res->min_ratio, res->freq, res->min_freq);
	if (res->lang1 < 0 || res->ratio < res->min_ratio || res->freq < res->min_freq)
		return LANG_UNKNOWN;
	else
		return lang_detect_lang_flags[ res->lang1 ].id;
}
