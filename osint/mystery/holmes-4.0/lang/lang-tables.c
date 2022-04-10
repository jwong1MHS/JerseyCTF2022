/*
 *	Language detector --- Computing detection tables
 *
 * 	(c) 2003, Robert Spalek <robert@ucw.cz>
 *	(c) 2006, Martin Mares <mj@ucw.cz>
 *	(c) 2007, Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "ucw/fastbuf.h"
#include "sherlock/object.h"
#include "ucw/url.h"
#include "sherlock/tagged-text.h"
#include "ucw/unicode.h"
#include "lang/lang.h"
#include "lang/detect.h"
#include "charset/unicat.h"
#include "indexer/indexer.h"
#include "filter/filter.h"

#define	PROFILE_TOD
#include "ucw/profile.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

/***** Configuration *****/

#define TRACE(t,x,y...) do { if (trace >= t) fprintf(stderr, x,##y); } while (0)

enum lang_flag { LF_UNSET, LF_ACCENT, LF_NOACCENT };
enum seq_flag { SF_NORMAL, SF_CUTACCENT };

static uns trace = 0, my_progress = 0;
static char *my_filter_name;
static char *bucket_state_file, *freq_file, *coef_file, *threshold_file;
static uns training_ratio = 500;
static uns seq_length = 2;
static uns nr_best = 20;
static uns accent_lang_set, noaccent_lang_set;
static double freq_threshold = 0.5;

static enum lang_flag lang_flag[MAX_LANGUAGES];
static uns detect_lang_count[3];		/* number of languages */
static int lang2pos[MAX_LANGUAGES][2];		/* position of the language in the frequency array */
static int pos2lang[MAX_LANGUAGES*2];		/* language at the given position */
static enum seq_flag pos2flag[MAX_LANGUAGES*2];	/* and its flag */

static struct cf_section lang_tables_config = {
	CF_ITEMS {
		CF_UNS("Trace", &trace),
		CF_UNS("Progress", &my_progress),
		CF_STRING("Filter", &my_filter_name),
		CF_STRING("BucketStateFile", &bucket_state_file),
		CF_STRING("FrequencyFile", &freq_file),
		CF_STRING("CoefficientFile", &coef_file),
		CF_STRING("ThresholdFile", &threshold_file),
		CF_UNS("TrainingRatio", &training_ratio),
		CF_UNS("MaxSequenceLength", &seq_length),
		CF_UNS("NumberOfBestSeq", &nr_best),
		CF_DOUBLE("FreqThreshold", &freq_threshold),
		CF_PARSER("AccentLanguages", &accent_lang_set, lang_cf_parse_set, CF_ANY_NUM),
		CF_PARSER("NoAccentLanguages", &noaccent_lang_set, lang_cf_parse_set, CF_ANY_NUM),
		CF_END
	}
};

static void
compute_translation_tables(void)
{
	bzero(detect_lang_count, sizeof(detect_lang_count));
	for (uns i=0; i<MAX_LANGUAGES; i++)
	{
		int af = (accent_lang_set & (1 << i));
		int nf = (noaccent_lang_set & (1 << i));
		if (af && nf)
			die("Language `%s' declared as both accent and noaccent", lang_code_to_name(i));
		if (af)
			lang_flag[i] = LF_ACCENT;
		else if (nf)
			lang_flag[i] = LF_NOACCENT;
		detect_lang_count[ lang_flag[i] ]++;
	}
	memset(lang2pos, -1, sizeof(lang2pos));
	memset(pos2lang, -1, sizeof(pos2lang));
	memset(pos2flag, -1, sizeof(pos2lang));
	/*
	 * Frequencies of a particular sequence in different languages are
	 * ordered in this way:
	 *	1. in accented languages	-- without cuting accents
	 *	2. in no-accented languages	/
	 *	3. in accented languages	-- with cut accents
	 * so that we can drop 2+3 when the sequence contains an accent.
	 */
	uns count = 0;
	uns shift = detect_lang_count[LF_ACCENT] + detect_lang_count[LF_NOACCENT];
	for (uns i=0; i<MAX_LANGUAGES; i++)
		if (lang_flag[i] == LF_ACCENT)
		{
			lang2pos[i][SF_NORMAL] = count;
			pos2lang[ count ] = i;
			pos2flag[ count ] = SF_NORMAL;
			lang2pos[i][SF_CUTACCENT] = shift + count;
			pos2lang[ shift + count ] = i;
			pos2flag[ shift + count ] = SF_CUTACCENT;
			count++;
		}
	ASSERT(count == detect_lang_count[LF_ACCENT]);
	count = 0;
	shift = detect_lang_count[LF_ACCENT];
	for (uns i=0; i<MAX_LANGUAGES; i++)
		if (lang_flag[i] == LF_NOACCENT)
		{
			lang2pos[i][SF_NORMAL] = shift + count;
			pos2lang[ shift + count ] = i;
			pos2flag[ shift + count ] = SF_NORMAL;
			count++;
		}
	ASSERT(count == detect_lang_count[LF_NOACCENT]);
}

/***** Hash-table of sequence frequencies *****/

struct seq_node
{
	uns* freq;		/* array of frequencies in all detected languages, see compute_translation_tables() */
	int weight;		/* only for the 2nd pass (computing the coefficients) */
	byte is_accented;	/* set once because the computation is expensive */
	byte text[1];
};

static inline uns
is_accented(byte *str)
{
	while (*str)
	{
		uns c;
		str = utf8_get(str, &c);
		if (c != Uunaccent(c))
			return 1;
	}
	return 0;
}

static inline uns
size_of_seq_node(struct seq_node *n)
{
	if (n->is_accented)
		return detect_lang_count[LF_ACCENT];
	else
		return 2*detect_lang_count[LF_ACCENT] + detect_lang_count[LF_NOACCENT];
}

static struct mempool *seq_pool;
static uns seq_total_count;

static void
seq_init_data(struct seq_node *n)
{
	n->is_accented = is_accented(n->text);
	n->freq = mp_alloc_zero(seq_pool, size_of_seq_node(n) * sizeof(uns));
	n->weight = 0;
	seq_total_count++;
}

#define	HASH_NODE	struct seq_node
#define	HASH_PREFIX(x)	seq_##x
#define	HASH_KEY_ENDSTRING	text
#define	HASH_GIVE_INIT_DATA
#define	HASH_WANT_LOOKUP
#define	HASH_WANT_FIND
//#define	HASH_CONSERVE_SPACE
#define	HASH_USE_POOL	seq_pool
#include "ucw/hashtable.h"

/***** Parser of a tagged text that updates the hash-table *****/

static int freq_pos;			/* which index of the array to update */
static enum lang_flag doc_lang_flag;
static enum seq_flag doc_seq_flag;

static uns count;			/* the number of characters in the buffer */
static byte buf[6*MAX_SEQ_LENGTH+1];	/* last SEQ_LENGTH characters in UTF-8 encoding */
static uns pos[MAX_SEQ_LENGTH+1];	/* start positions of the last SEQ_LENGTH characters + the tail */
static byte non_letter;			/* was the last character non-letter?  */

static void
doc_start(uns lang_code, enum seq_flag flag)
{
	freq_pos = lang2pos[lang_code][flag];
	ASSERT(freq_pos >= 0);
	doc_lang_flag = lang_flag[lang_code];
	doc_seq_flag = flag;

	count = 0;
	bzero(buf, sizeof(buf));
	bzero(pos, sizeof(pos));
	non_letter = 0;
}

static void
increase_frequencies(void)
{
	for (uns i=0; i<count; i++)
	{
		struct seq_node *n = seq_lookup(buf + pos[i]);
		ASSERT(n);
		n->freq[freq_pos]++;
	}
}

static void
add_character(uns c)
{
	if (!Ualpha(c))
	{
		/* Translate sequences of control characters into one
		 * KMP_CONTROL_CHAR.  */
		if (non_letter)
			return;
		non_letter = 1;
		c = ':';
	}
	else
	{
		non_letter = 0;
		if (doc_seq_flag == SF_CUTACCENT)
			c = Uunaccent(c);
		else if (doc_lang_flag == LF_NOACCENT && c != Uunaccent(c))
		{
			/* Occasional accented characters in noaccented
			 * languages are treated as a special non-indexed
			 * character.  Reset the buffer.  */
			count = 0;
			non_letter = 1;
			return;
		}
		c = Utolower(c);
	}
	/* Rotate the buffer if necessary.  */
	ASSERT(count <= seq_length);
	if (count == seq_length)
	{
		uns pos1 = pos[1];
		memmove(buf, buf + pos1, pos[count] - pos1 + 1);
		for (uns i=0; i<count; i++)
			pos[i] = pos[i+1] - pos1;
		count--;
	}
	/* Append the new character.  */
	byte *end_buf = buf + pos[count];
	end_buf = utf8_put(end_buf, c);
	*end_buf = 0;
	pos[++count] = end_buf - buf;
	/* Increment the frequencies.  */
	increase_frequencies();
}

static void
doc_line(byte *line)
{
	byte *line_end = line + strlen(line);
	add_character(':');
	for (byte *pos=line; pos<line_end; )
	{
		uns c;
		GET_TAGGED_CHAR(pos, c);
		if (c >= 0x80000000)
			add_character(':');
		else
			add_character(c);
	}
	add_character(':');
}

static void
doc_end(void)
{
}

/***** Filters *****/

struct my_filter_data {
	byte *url;
	struct url url_s;
	byte *language;
};

struct filter_binding my_bindings[] = {
	/* URL and its parts */
	{ "url",		OFFSETOF(struct my_filter_data, url) },
	{ "protocol",		OFFSETOF(struct my_filter_data, url_s.protocol) },
	{ "host",		OFFSETOF(struct my_filter_data, url_s.host) },
	{ "port",		OFFSETOF(struct my_filter_data, url_s.port) },
	{ "path",		OFFSETOF(struct my_filter_data, url_s.rest) },
	{ "username",		OFFSETOF(struct my_filter_data, url_s.user) },
	{ "password",		OFFSETOF(struct my_filter_data, url_s.pass) },
	/* Attributes */
	{ "language",		OFFSETOF(struct my_filter_data, language) },
	{ NULL,			0 }
};

static struct filter_args *
my_filter_init(void)
{
	if (!my_filter_name || !my_filter_name[0])
		return NULL;
	return filter_intr_new(
		filter_load(my_filter_name, filter_builtin_vars, my_bindings, filter_builtin_func)
	);
}

static void
my_filter_done(struct filter_args *a)
{
	if (a)
	{
		filter_delete(a->filter);
		filter_intr_delete(a);
	}
}

static byte *
my_filter_get_language(struct filter_args *a, byte *url, struct odes *obj, struct mempool *pool)
{
	if (!a)
		return obj_find_aval(obj, 'l');
	struct my_filter_data d;
	byte buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];

	d.url = url;
	if (url_canon_split(d.url, buf1, buf2, &d.url_s))
		die("filter: error parsing URL");
	d.language = obj_find_aval(obj, 'l');
	a->attr = obj;
	a->raw = &d;
	a->pool = pool;
	if (filter_intr_run(a))
		return d.language;
	else
		return NULL;
}

/***** Computing the sequence frequencies *****/

static void
compute_frequencies(struct filter_args *fa)
{
	ASSERT(training_ratio > 0);
	ASSERT(seq_length >= 1 && seq_length <= MAX_SEQ_LENGTH);

	uns count = 0, training = 0, testing = 0;
	seq_pool = mp_new(1 << 16);
	seq_init();
	seq_total_count = 0;

	/* Randomly divide buckets into a training set and a test set and
	 * process all documents from the training set.  */
	printf("Computing frequencies of sequences of length 1..%d for %d+%d languages\n",
		seq_length, detect_lang_count[LF_ACCENT], detect_lang_count[LF_NOACCENT]);
	struct mempool *obj_pool = mp_new(1 << 12);
	struct get_buck gb;
	get_buck_init(&gb);
	gb.pool = obj_pool;
	struct fastbuf *b_buck = bopen(bucket_state_file, O_CREAT | O_TRUNC | O_WRONLY, 1024);
	for (; get_buck_next(&gb, ~0U); mp_flush(obj_pool))
	{
		struct odes *obj = gb.o;
		if (my_progress > 0 && !(count % my_progress))
			fprintf(stderr, "Processed %d buckets, training %d, testing %d\r", count, training, testing);
		bprintf(b_buck, "%08x ", gb.oid);
		count++;

		/* Parse the language of the object.  */
		byte *url = obj_find_aval(obj, 'U');
		byte *lang = my_filter_get_language(fa, url, obj, obj_pool);
		int lang_code = lang ? lang_primary_language(lang) : -1;
		if (lang_code >= 0 && !lang_flag[lang_code])
			lang_code = -1;
		if (!strcmp(url + strlen(url) - 11, "/robots.txt"))
		  lang_code = -1;
		lang = lang_code >= 0 ? lang_code_to_name(lang_code) : (byte*) "??";

		byte train = 0;
		bprintf(b_buck, "%2s ", lang);
		if (lang_code >= 0)
		{
			train = (random_max(1000) < training_ratio);
			if (train)
			{
				training++;
				for (enum seq_flag flag = SF_NORMAL; flag <= SF_CUTACCENT; flag++)
				{
					if (flag == SF_CUTACCENT && lang_flag[lang_code] == LF_NOACCENT)
						break;
					/* Process the bucket.  */
					doc_start(lang_code, flag);
					for (struct oattr *a=obj_find_attr(obj, 'X'); a; a=a->same)
						doc_line(a->val);
					doc_end();
				}
			}
			else
				testing++;
		}
		bprintf(b_buck, "%c ", lang_code >= 0 ? (train ? 'R' : 'T') : '0');
		bprintf(b_buck, ".. %6d %5d %s\n", 0, 0, url);	/* two dots and two numbers for the 3rd pass */
	}
	bclose(b_buck);
	get_buck_cleanup(&gb);
	mp_delete(obj_pool);
	printf("Processed %d buckets, training %d, testing %d\nFound %d distinct sequences\n\n",
		count, training, testing, seq_total_count);

	/* Dump the sequence frequencies.  */
	struct fastbuf *b_freq = bopen(freq_file, O_CREAT | O_TRUNC | O_WRONLY, 1024);
	bprintf(b_freq, "%d\n", seq_total_count);
	HASH_FOR_ALL(seq, n)
	{
		bputs(b_freq, n->text);
		bputc(b_freq, ' ');
		uns size = size_of_seq_node(n);
		for (uns i=0; i<size; i++)
			bprintf(b_freq, "%d ", n->freq[i]);
		bputc(b_freq, '\n');
	}
	HASH_END_FOR;
	bclose(b_freq);

	mp_delete(seq_pool);
}

/***** Computing the coefficients *****/

static void
compute_weights(struct seq_node **seq, uns count, uns lang)
{
	for (uns i=0; i<count; i++)
		seq[i]->weight = lang < size_of_seq_node(seq[i]) ? seq[i]->freq[lang] : 0;
}

#define	ASORT_PREFIX(x)	seq_weight_##x
#define	ASORT_KEY_TYPE	int
#define	ASORT_ELT(i)	-array[i]->weight
#define	ASORT_SWAP(i,j)	do { struct seq_node *tmp=array[j]; array[j]=array[i]; array[i]=tmp; } while(0)
#define	ASORT_EXTRA_ARGS	, struct seq_node **array
#include "ucw/sorter/array-simple.h"

#define	MAXBUF	1024
static void
compute_coefficients(void)
{
	uns count;
	struct seq_node **seq;
	uns i, j;

	seq_pool = mp_new(1 << 16);

	/* Read all sequences.  */
	struct fastbuf *b_freq = bopen(freq_file, O_RDONLY, 1024);
	byte buf[MAXBUF];
	bgets(b_freq, buf, MAXBUF);
	count = atoi(buf);
	seq = mp_alloc(seq_pool, count * sizeof(struct seq_node *));
	for (i=0; i<count; i++)
	{
		byte *c;
		c = bgets(b_freq, buf, MAXBUF);
		ASSERT(c);
		c = strchr(buf, ' ');
		seq[i] = mp_alloc(seq_pool, sizeof(struct seq_node) + (c-buf));
		memcpy(seq[i]->text, buf, c-buf);
		seq[i]->text[c-buf] = 0;
		seq[i]->is_accented = is_accented(seq[i]->text);

		uns size = size_of_seq_node(seq[i]);
		seq[i]->freq = mp_alloc(seq_pool, size * sizeof(uns));
		c++;
		for (j=0; j<size; j++)
		{
			uns n;
			if (sscanf(c, "%d %n", seq[i]->freq + j, &n) != 1)
				ASSERT(0);
			c += n;
		}
		ASSERT(!*c);
	}
	ASSERT(bgetc(b_freq) < 0);
	bclose(b_freq);
	printf("Read frequencies of %d sequences\n", count);

	/* Compute total numbers so that we know what to divide by.  */
	uns langs = 2*detect_lang_count[LF_ACCENT] + detect_lang_count[LF_NOACCENT];
	uns *total[langs];
	for (i=0; i<langs; i++)
		total[i] = mp_alloc_zero(seq_pool, seq_length * sizeof(uns));
	for (i=0; i<count; i++)
	{
		uns size = size_of_seq_node(seq[i]);
		for (j=0; j<size; j++)
			total[j][ utf8_strlen(seq[i]->text)-1 ] += seq[i]->freq[j];
	}
	printf("Total number of sequences of given language and length:\n");
	for (i=0; i<langs; i++)
	{
		uns lang_code = pos2lang[i];
		printf("Language#%d: %d (%s)%s: ", i, lang_code, lang_code_to_name(lang_code),
			pos2flag[i] == SF_NORMAL ? "" : " CUTACCENT");
		for (j=0; j<seq_length; j++)
			printf("%d ", total[i][j]);
		printf("\n");
	}

	/* For each language variant, find the most typical sequences.  */
	seq_init();
	seq_total_count = 0;
	uns freq[langs];
	for (i=0; i<langs; i++)
	{
		compute_weights(seq, count, i);
		seq_weight_sort(count, seq);
		freq[i] = 0;

		uns lang_code = pos2lang[i];
		TRACE(1, "Language#%d: %d (%s)%s\n", i, lang_code, lang_code_to_name(lang_code),
			pos2flag[i] == SF_NORMAL ? "" : " CUTACCENT");
		for (j=0; j<nr_best && j<count && seq[j]->weight; j++)
		{
			TRACE(1, "%3d. %4s %6d %8d", j, seq[j]->text, seq[j]->weight, seq[j]->freq[i]);
			if (seq_find(seq[j]->text))
				TRACE(1, " ALREADY TAKEN\n");
			else
				TRACE(1, " TAKING\n");
			struct seq_node *seqh = seq_lookup(seq[j]->text);
			freq[i] += seq[j]->freq[i];
			seqh->freq[i] = j+1;
		}
		freq[i] = total[i][0] ? (uns)(freq[i] * freq_threshold * 1000 / total[i][0]) : 0;
		TRACE(1, "\n");
	}

	/* Dump the hash-table of best sequences.  */
	struct fastbuf *b_coef = bopen(coef_file, O_CREAT | O_TRUNC | O_WRONLY, 1024);
	time_t t;
	time(&t);
	struct tm *tm = localtime(&t);
	strftime(buf, MAXBUF, "%Y-%m-%d %H:%M:%S", tm);
	bprintf(b_coef, "LangDetect {\n\
#\tGenerated at %s by lang-tables\n\n\
Reset\t\t\t%d\n\
MinDocumentLength\t%d\n\n", buf, seq_total_count, lang_detect_min_doc_length);
	for (i=0; i<langs; i++)
		bprintf(b_coef, "Add%sLanguage\t%s\t%d\t1000\t%d\n",
			pos2flag[i] == SF_NORMAL ? "Normal" : "CutAccent",
			lang_code_to_name(pos2lang[i]), nr_best, freq[i]);
	bprintf(b_coef, "\n#Languages:\t\t\t");
	for (i=0; i<langs; i++)
		bprintf(b_coef, "%s%s\t", lang_code_to_name(pos2lang[i]),
			pos2flag[i] == SF_NORMAL ? "" : "-CA");
	bputc(b_coef, '\n');
	HASH_FOR_ALL(seq, n)
	{
		bprintf(b_coef, "SequenceFrequencies\t%s\t", n->text);
		uns size = size_of_seq_node(n);
		for (i=0; i<size; i++)
			bprintf(b_coef, "%d\t", n->freq[i]);
		for (; i<langs; i++)
			bprintf(b_coef, "0\t");
		bputc(b_coef, '\n');
	}
	HASH_END_FOR;
	bputsn(b_coef, "}");
	bclose(b_coef);
	printf("Exported %d best sequences\n\n", seq_total_count);

	mp_delete(seq_pool);
}

/***** Testing the language detector *****/

enum doc_mode { DM_UNKNOWN, DM_TRAIN, DM_TEST,					DM_MAX };
enum outcome_type { OT_UNKNOWN=1, OT_SHORT, OT_CORRECT, OT_WRONG, OT_GUESS,	OT_MAX };
static byte *dm_names[] = { "Unknown", "Training", "Test" };
static byte *ot_names[] = { NULL, "unknown", "short", "correct", "wrong", "guess" };

struct threshold_record
{
	uns lang;		/* that was identified on a document */
	enum seq_flag flag;	/* with this flag */
	enum outcome_type outcome;	/* was it OK? */
	uns best;		/* variance of the best language for the document */
	int ratio;		/* variance of the 2nd best language is RATIO-times bigger */
	uns total;		/* number of detected sequences */
	uns url_len;		/* URL_LEN bytes follow this record */
};

#define	SORT_PREFIX(x) threshold_##x
#define	SORT_KEY_REGULAR struct threshold_record
#define SORT_DATA_SIZE(k) ((k).url_len)
#define	SORT_INPUT_FILE
#define	SORT_OUTPUT_FB

static inline int
threshold_compare(struct threshold_record *a, struct threshold_record *b)
{
	COMPARE(a->lang, b->lang);
	COMPARE(a->flag, b->flag);
	COMPARE(a->outcome, b->outcome);
	COMPARE(b->ratio, a->ratio);
	return 0;
}

#include "ucw/sorter/sorter.h"

static void
test_detector(uns just_test_set)
{
	uns count = 0;
	uns stat[DM_MAX][OT_MAX];
	uns lang_docs[MAX_LANGUAGES];
	lang_detect_load_tables();
	struct lang_detect *lang_detect = lang_detect_alloc(NULL);

	bzero(stat, sizeof(stat));
	bzero(lang_docs, sizeof(lang_docs));

	prof_t cnt;
	prof_init(&cnt);

	struct mempool *obj_pool = mp_new(1 << 12);
	struct get_buck gb;
	gb.pool = obj_pool;
	get_buck_init(&gb);
	struct fastbuf *b_buck = just_test_set ? bopen(bucket_state_file, O_RDWR, 1024) : NULL;
	struct fastbuf *b_threshold = bopen(threshold_file, O_CREAT | O_TRUNC | O_WRONLY, 1024);
	prof_start(&cnt);
	for (; get_buck_next(&gb, ~0U); mp_flush(obj_pool))
	{
		enum doc_mode mode;
		byte doc_lang[3];
		if (b_buck)
		{
			/* Third pass: check the test-set.  */
			byte buf[30];
			uns read = bread(b_buck, buf, 30);
			ASSERT(read == 30 && buf[8]==' ' && buf[11]==' ' && buf[13]==' ' && buf[16]==' ' && buf[23]==' ' && buf[29]==' ');
			mode = buf[12]=='0' ? DM_UNKNOWN : buf[12]=='R' ? DM_TRAIN : DM_TEST;
			memcpy(doc_lang, buf+9, 2);
			doc_lang[2] = 0;
		}
		else
		{
			/* Just detect languages of all buckets.  */
			mode = DM_UNKNOWN;
			doc_lang[0] = 0;
		}

		if (my_progress > 0 && !(count % my_progress))
			fprintf(stderr, "Processed %d buckets\r", count);
		count++;

		/* Discard the recent object and read a new one.  */
		struct odes *obj = gb.o;
		ASSERT(obj);

		/* Run the language detector.  */
		lang_detect_start(lang_detect);
		for (struct oattr *a=obj_find_attr(obj, 'X'); a; a=a->same)
			lang_detect_add_string(lang_detect, a->val);
		struct lang_detect_results *results = lang_detect_compute(lang_detect);

		/* Find the most probable language.  */
		enum outcome_type outcome;
		uns lang_code1, lang_code2;
		byte *lang_name1, *lang_name2;
		byte *url = obj_find_aval(obj, 'U');
		if (results->total_occur < lang_detect_min_doc_length)
		{
			lang_code1 = lang_code2 = 0;
			lang_name1 = lang_name2 = "--";
			outcome = OT_SHORT;
		}
		else if (results->lang1 >= 0 && results->ratio >= results->min_ratio)
		{
			lang_code1 = lang_detect_lang_flags[results->lang1].id;
			lang_code2 = lang_detect_lang_flags[results->lang2].id;
			ASSERT(lang_code1 < MAX_LANGUAGES && lang_code2 < MAX_LANGUAGES);
			lang_name1 = lang_code_to_name(lang_code1);
			lang_name2 = lang_code_to_name(lang_code2);
			if (mode == DM_UNKNOWN)
				outcome = OT_GUESS;
			else
				outcome = strcasecmp(lang_name1, doc_lang) ? OT_WRONG : OT_CORRECT;
			struct threshold_record tr = { lang_code1,
				lang_detect_lang_flags[ results->lang1 ].is_accented ? SF_NORMAL : SF_CUTACCENT,
				outcome, results->variances[results->lang1], results->ratio,
				results->total_occur, strlen(url) };
			bwrite(b_threshold, &tr, sizeof(struct threshold_record));
			bwrite(b_threshold, url, tr.url_len);
		}
		else
		{
			lang_code1 = lang_code2 = 0;
			lang_name1 = lang_name2 = lang_code_to_name(0);
			outcome = OT_UNKNOWN;
		}
		stat[mode][outcome]++;
		stat[mode][0]++;
		lang_docs[lang_code1]++;

		/* Trace the weights of all languages and the result.  */
		TRACE(1, "URL   : %s, Length: %d\n  Prob: ", url, results->total_occur);
		for (uns i=0; i<lang_detect_nr_langs; i++)
			TRACE(1, "%d ", results->variances[i]);
		TRACE(1, "===> #%d (%s, %d); 2nd: #%d (%s, %d)\n",
			results->lang1, lang_name1, lang_code1,
			results->lang2, lang_name2, lang_code2);

		if (b_buck)
		{
			/* Modify the bucket-processing log-file.  */
			ucw_off_t curr_pos = btell(b_buck);
			bflush(b_buck);
			bsetpos(b_buck, curr_pos-16);
			bprintf(b_buck, "%2.2s %6d %5d", lang_name1, results->total_occur, results->ratio);
			bflush(b_buck);
			bsetpos(b_buck, curr_pos);

			while (bgetc(b_buck) != '\n');
		}
		else
		{
			/* Output the chosen language.  */
			printf("%08x %2.2s %6d %5d %s\n", gb.oid, lang_name1, results->total_occur, results->ratio, obj_find_aval(obj, 'U'));
		}
	}
	prof_stop(&cnt);
	if (b_buck)
	{
		ASSERT(bgetc(b_buck) < 0);
		bclose(b_buck);
	}
	bclose(b_threshold);
	get_buck_cleanup(&gb);
	mp_delete(obj_pool);

	printf("\nTested %d buckets\n", count);
	for (enum doc_mode mode = 0; mode < DM_MAX; mode++)
	{
		printf("%s documents (total %d):\n", dm_names[mode], stat[mode][0]);
		for (enum outcome_type outcome = 1; outcome < OT_MAX; outcome++)
			if (stat[mode][outcome])
				printf("\t%d (%5.2f%%) %s\n", stat[mode][outcome],
					(stat[mode][outcome] + 0.)/stat[mode][0] * 100, ot_names[outcome]);
		if (stat[mode][OT_WRONG])
			printf("\tCorrect occured %.2f-times more than wrong\n",
				(stat[mode][OT_CORRECT] + 0.)/stat[mode][OT_WRONG]);
		printf("\n");
	}
	printf("Detected documents:\n");
	for (uns i=0; i<MAX_LANGUAGES; i++)
	{
		if (lang_docs[i])
			printf("%d documents of language %s (%d)\n",
				lang_docs[i], lang_code_to_name(i), i);
	}
	printf("Testing took %s seconds\n", PROF_STR(cnt));
}

/***** Determining thresholds *****/

static void
print_weights(void)
{
	/* Browse the threshold data.  */
	struct fastbuf *b_threshold = threshold_sort(threshold_file, NULL);
	struct threshold_record tr;
	while ( bread(b_threshold, &tr, sizeof(struct threshold_record)) )
	{
		printf("Language %d (%s%s), outcome %s, ratio %d, best %d, total %d, url ",
				tr.lang, lang_code_to_name(tr.lang),
				tr.flag == SF_CUTACCENT ? "-CA" : "",
				ot_names[tr.outcome], tr.ratio, tr.best, tr.total);
		byte url[tr.url_len+1];
		bread(b_threshold, url, tr.url_len);
		url[tr.url_len] = 0;
		puts(url);
	}
	bclose(b_threshold);
}

/***** Main program *****/

static char *short_opts = CF_SHORT_OPTS "afctwdURr:FL";
static char *help = "\
Usage: lang-tables [<options>]\n\
\n\
Options:\n"
CF_USAGE
"-a\tPerform phases 1--3\n\
-f\tPhase 1: compute frequency tables\n\
-c\tPhase 2: choose language coefficients\n\
-t\tPhase 3: test the accuracy\n\
-w\tPhase 4: print sorted acceptance weights to determine thresholds\n\
-d\tAlternative: detect languages on the whole bucket-file\n\n\
-U\tUnlink temporary files\n\
-R\tcall srand(time)\n\
-r seed\tcall srand(seed)\n\
-F\tcall the filter that can modify the language data\n\
-L\treload detector configuration (needed only together with -t)\n\
";

static void NONRET
usage(void)
{
	fputs(help, stderr);
	exit(1);
}

int
main(int argc, char **argv)
{
#define	COMP_FREQ	1
#define	COMP_COEF	2
#define	DO_TESTS	4
#define	PRINT_WEIGHTS	8
#define	DETECT_LANG	16
#define	OPT_UNLINK	1
#define	OPT_FILTERS	2
#define	OPT_RELOAD	4
	byte to_compute = 0, options = 0;
	int opt;
	uns seed = 1;

	cf_declare_section("LangTables", &lang_tables_config, 0);
	log_init(argv[0]);
	while ((opt = cf_getopt(argc, argv, short_opts, CF_NO_LONG_OPTS, NULL)) >= 0)
		switch (opt)
		{
			case 'a':
				to_compute |= COMP_FREQ | COMP_COEF | DO_TESTS;
				break;
			case 'f':
				to_compute |= COMP_FREQ;
				break;
			case 'c':
				to_compute |= COMP_COEF;
				break;
			case 't':
				to_compute |= DO_TESTS;
				break;
			case 'w':
				to_compute |= PRINT_WEIGHTS;
				break;
			case 'd':
				to_compute |= DETECT_LANG;
				break;
			case 'U':
				options |= OPT_UNLINK;
				break;
			case 'R':
				seed = time(NULL);
				break;
			case 'r':
				seed = atoi(optarg);
				break;
			case 'F':
				options |= OPT_FILTERS;
				break;
			case 'L':
				options |= OPT_RELOAD;
				break;
			default:
				usage();
		}
	if (optind < argc || !to_compute)
		usage();
	printf("Random seed is %d\n", seed);
	srand(seed);

	compute_translation_tables();
	if (to_compute & COMP_FREQ)
	{
		struct filter_args *a;
		if (options & OPT_FILTERS)
			a = my_filter_init();
		else
			a = NULL;
		compute_frequencies(a);
		my_filter_done(a);
	}
	if (to_compute & COMP_COEF)
		compute_coefficients();
	if (to_compute & COMP_COEF
	|| options & OPT_RELOAD)
		lang_detect_tables_file = coef_file;
	if (to_compute & DO_TESTS)
		test_detector(1);
	if (to_compute & PRINT_WEIGHTS)
		print_weights();
	if (options & OPT_UNLINK)
	{
		unlink(bucket_state_file);
		unlink(freq_file);
		/* Do not delete coef_file, since this is the output of the
		 * utility nor threshold_file, since it can be examined later.  */
	}
	if (to_compute & DETECT_LANG)
		test_detector(0);

	return 0;
}
