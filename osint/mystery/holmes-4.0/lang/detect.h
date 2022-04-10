/*
 *	Language detector
 *
 * 	(c) 2003, Robert Spalek <robert@ucw.cz>
 */

#include "lang/lang.h"

#define	MAX_SEQ_LENGTH	4
	/* maximum hard-length of a scanned sequence */
#define	MAX_DETECTED	(2*MAX_LANGUAGES)
	/* because every language can containt coefficients for both accented
	 * and unaccented variant */

struct lang_detect_lang_flag
{
	int id;
	byte is_accented;		/* just for debugging purposes, used nowhere */
	uns nr_seq, rel_threshold, freq_threshold;
};
struct lang_detect_sequence
{
	uns len;
	int order[MAX_LANGUAGES];
	byte *text;
};

struct sequence_freq {
	uns id, occur;
};

struct kmp_result;
struct lang_detect_results
{
	uns total_occur;		/* number of sequences found */
	uns nonzero_seq;		/* sequences with non-zero number of occurences */
	struct sequence_freq *sf;	/* sorted array of frequences of all sequences */
	uns *variances;			/* of the languages */
	int lang1, lang2;		/* best 2 languages or -1 if the detector has failed */
	int ratio;			/* between the variance of the 2 best languages */
	int min_ratio;			/* if ratio < min_ratio, then the guesses does not know */
	int freq;			/* number of sequences per character */
	int min_freq;			/* if frac < min_frac, then the guesses does not know */
};

extern uns lang_detect_nr_langs;
extern struct lang_detect_lang_flag lang_detect_lang_flags[];

extern uns lang_detect_nr_sequences, lang_detect_max_sequences, lang_detect_total_seq_len;
extern struct lang_detect_sequence **lang_detect_sequences;

extern uns lang_detect_mode;
extern uns lang_detect_min_doc_length;
extern char *lang_detect_tables_file;

struct lang_detect;

void lang_detect_load_tables(void);
struct lang_detect *lang_detect_alloc(struct mempool *pool);
void lang_detect_start(struct lang_detect *ld);
void lang_detect_add_string(struct lang_detect *ld, byte *str);
void lang_detect_add_fastbuf(struct lang_detect *ld, struct fastbuf *fb);
struct lang_detect_results *lang_detect_compute(struct lang_detect *ld);
uns lang_detect_choose_best(struct lang_detect *ld);
