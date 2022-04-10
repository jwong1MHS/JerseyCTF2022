/*
 *	Sherlock Search Engine -- Card Files
 *
 *	(c) 1997--2007 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2006 Robert Spalek <robert@ucw.cz>
 *	(c) 2006 Pavel Charvat <pchar@ucw.cz>
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/hashfunc.h"
#include "ucw/heap.h"
#include "ucw/url.h"
#include "ucw/unicode.h"
#include "ucw/workqueue.h"
#include "ucw/lfs.h"
#include "sherlock/index.h"
#include "sherlock/tagged-text.h"
#include "sherlock/lizard-fb.h"
#include "sherlock/object.h"
#include "charset/unicat.h"
#include "lang/lang.h"
#include "search/sherlockd.h"
#include "search/lexicon.h"
#include "search/fulltext.h"
#include "search/refs.h"
#include "indexer/sites.h"
#include "indexer/alphabet.h"

struct kmp_struct;
static inline void *kmp_alloc(struct kmp_struct *kmp, uns size);
static inline void kmp_free(struct kmp_struct *kmp UNUSED, void *ptr UNUSED) {}

#define KMP_PREFIX(x) kmp_##x
#define KMP_USE_ASCII
#define KMP_TOLOWER
#define KMP_WANT_SEARCH
#define KMP_GIVE_ALLOC
#define KMP_VARS struct mempool *pool;
#define KMPS_GET_CHAR(kmp,src,s) ((src == s->u.src_end) ? 0 : ((s->c = Clocase(*(src)++)), 1))
#define KMPS_VARS uns best_len; char *best_pos, *src_end;
#define KMPS_INIT(kmp,src,s) s->u.best_len = 0
#define KMPS_FOUND_CHAIN(kmp,src,s) do{ if (s->out->len > s->u.best_len) { s->u.best_len = s->out->len; s->u.best_pos = src; } }while(0)
#include "ucw/kmp.h"

static inline void *
kmp_alloc(struct kmp_struct *kmp, uns size)
{
  return mp_alloc(kmp->u.pool, size);
}

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <alloca.h>

#define	TRACE(mask...)	do { if (q->debug & DEBUG_DUMPING) log(L_DEBUG, mask); } while (0)

/***** Card Header *****/

static void
show_card_attr(struct query *q, struct database *db, oid_t oid, struct card_attr *ca)
{
  send_reply("B%s", db->name);
  send_reply("O%08x", oid);
  send_reply("w%d", ca->weight);
#ifdef CONFIG_SITES
  u64 hash = site_find_hash(&db->sites, ca->site_id);
  send_reply("T%0*llx", 2*SITE_HASH_SIZE, (long long) hash);
  if (q->debug & DEBUG_CARD_INFO)
    send_reply(".Tsite_id=%x", ca->site_id);
#endif
#ifdef CONFIG_MERGING_HASHES
  uns mh = 0;
  for (uns i=0; i<SHERLOCK_MERGING_HASH_SIZE; i++)
    mh = (mh << 8) | ca->merging_hash[i];
  send_reply("g%0*x", 2*SHERLOCK_MERGING_HASH_SIZE, mh);
#endif
#ifdef CONFIG_AREAS
  send_reply("n%d", ca->area);
#endif
#ifdef CONFIG_LANG
  if (CA_GET_FILE_LANG(ca))
    send_reply("l%s", lang_code_to_name(CA_GET_FILE_LANG(ca)));
#endif
  CUSTOM_MATCH_SHOW(q, ca, send_reply);

  if (q->debug & DEBUG_CARD_INFO)
    {
#ifdef CONFIG_LASTMOD
      send_reply(".Cage=%d", ca->age);
#endif
#define INT_ATTR(id,kw,gf,pf) send_reply(".C" #id "=%d", gf(ca));
#define SMALL_SET_ATTR INT_ATTR
#define LATE_INT_ATTR INT_ATTR
#define LATE_SMALL_SET_ATTR SMALL_SET_ATTR
      EXTENDED_ATTRS
#undef SMALL_SET_ATTR
#undef INT_ATTR
#undef LATE_INT_ATTR
#undef LATE_SMALL_SET_ATTR
    }
}

static void
show_card_note(struct query *q, struct result_note *note)
{
  if (!note)
    return;
  send_reply("Q%d", note->q);
#ifdef CONFIG_SITES
  if (note->site_compressed)
    send_reply("m%d", note->site_compressed);
#endif
  if (q->debug & DEBUG_CARD_INFO)
    send_reply(".K%08x", note->sec_sort_key);
}

/***** Lexical Mapping *****/

static struct ref_context *ft_context;
static struct ft_results ft;

static struct ft_word *
ft_find_word_by_pos(uns pos)
{
#define ORIG_LT_POS(ary,i,x) ft.words[i].pos < (x)
#include "ucw/binsearch.h"
	uns w = BIN_SEARCH_FIRST_GE_CMP(whatever,ft.num_words,pos,ORIG_LT_POS);
	if (w < ft.num_words)
		return ft.words + w;
	else
		return NULL;
}

static struct ft_word *
ft_find_word_by_orig(byte *orig, struct ft_word *last_word)
{
	if (last_word[1].orig == orig)		// avoid a log-factor during linear reading
		return last_word+1;
#define ORIG_LT_ORIG(ary,i,x) ft.words[i].orig < (x)
#include "ucw/binsearch.h"
	uns w = BIN_SEARCH_FIRST_GE_CMP(whatever,ft.num_words,orig,ORIG_LT_ORIG);
	if (w < ft.num_words)
		return ft.words + w;
	else
		return NULL;
}

static void
ft_parse(byte *text, byte *text_end, ref_pos_t *lexmap_pos, enum ft_type input_type)
{
	ft.text = text;
	ft.text_end = text_end;
	ft.input_type = input_type;
	ft.pos = (lexmap_pos ? *lexmap_pos : POS_NOWHERE);
	ft_match(ft_context, &ft);
	if (lexmap_pos)
		*lexmap_pos = MIN(ft.pos, POS_NOWHERE);
}

/***** Search automaton for highlighted words *****/

static struct kmp_struct *ft_kmp;

static int
is_ascii(byte *str)
{
	while (*str)
		if (*str++ >= 0x80)
			return 0;
	return 1;
}

static struct kmp_struct *
kmp_create(struct query *q)
{
	if (!highlight_substring)
		return NULL;

	byte buf[MAX_WORD_BYTES+1];
	struct variant *v;
	uns words_len = 0;
	CLIST_FOR_EACH(struct database *, db, databases)
	{
		if (!restore_interims(q, q->results, db))
			continue;
		struct word **w = q->words;
		for (uns i=0; i<q->nwords; i++)
			if (!w[i]->is_string && w[i]->word_class != WC_COMPLEX)
				SLIST_WALK(v, w[i]->variants)
				{
					word_extract_variant(w[i], v, buf);
					if (is_ascii(buf) && strlen(buf) >= highlight_substring)
						words_len += strlen(buf);
				}
	}

	struct kmp_struct *kmp = mp_alloc(q->pool, sizeof(struct kmp_struct));
	kmp->u.pool = q->pool;
	kmp_init(kmp);
	CLIST_FOR_EACH(struct database *, db, databases)
	{
		if (!restore_interims(q, q->results, db))
			continue;
		struct word **w = q->words;
		for (uns i=0; i<q->nwords; i++)
			if (!w[i]->is_string && w[i]->word_class != WC_COMPLEX)
				SLIST_WALK(v, w[i]->variants)
				{
					word_extract_variant(w[i], v, buf);
					if (is_ascii(buf) && strlen(buf) >= highlight_substring)
						kmp_add(kmp, buf);
				}
	}
	kmp_build(kmp);
	return kmp;
}

/***** Dumper of a block of text *****/

static byte tm_letter[2] = "XM";
const char * const wt_names[9] = { WORD_TYPE_USER_NAMES, NULL };
const char * const mt_names[17] = { META_TYPE_USER_NAMES, NULL };
const char * const st_names[9] = { STRING_TYPE_USER_NAMES, NULL };

struct context_interval
{
	byte *first_char, *last_char;			/* pointers to the text */
	uns start_type;					/* type at the beginning */
	int context_len;				/* length of the context in printable characters */
};

#define	DUMP_INTERTAG_SPACES	0			/* whether to insert spaces between tags */

#define	MAXLINE	72					/* maximal length of output lines */
#define	DUMP_CONST(sp, txt)	dump_word(&line_len, txt, sizeof(txt)-1, sp)
#define	DUMP_MASK(sp, mask...)	do { \
	byte tmpbuf[64]; \
	uns tmplen = sprintf(tmpbuf, mask); \
	dump_word(&line_len, tmpbuf, tmplen, sp); \
} while (0)

static void
dump_word(uns *line_len, byte *start, uns strlen, uns space)
{
	if (!strlen)
	{
		/* Empty string means flushing the buffer.  */
		if (*line_len)
			*line_len = 0, send_reply_string("\n");
	}
	else
	{
		/* The text is broken into lines only at a space in the word mode.  */
		if (ft.input_type == FT_TEXT && space && *line_len >= MAXLINE)
			*line_len = 0, send_reply_string("\n");
		if (!*line_len)
			*line_len = 1, send_reply_block(&tm_letter[ft.input_type != FT_TEXT], 1);
		else if (space)
			(*line_len)++, send_reply_string(" ");
		send_reply_block(start, strlen);
		*line_len += strlen;
	}
}

static inline byte *
html_escape(uns c)
{
	switch (c)
	{
		case '<':
			return "&lt;";
		case '>':
			return "&gt;";
		case '&':
			return "&amp;";
		default:
			return NULL;
	}
}

static void
dump_text_interval(byte *label, struct context_interval *range)
{
	const char * const *type_names = (ft.input_type == FT_TEXT ? wt_names : mt_names);
	byte *text;
	uns line_len;
	uns space;
	uns type;
	uns whole_text = range->first_char == ft.text && range->last_char == ft.text_end;

	text = range->first_char;
	type = range->start_type;
	space = 1;
	line_len = 0;
	if (label)
		dump_word(&line_len, label, strlen(label), 1);
	if (!whole_text)
		DUMP_MASK(1, "<block c=%d-%d l=%d>", (uns)(range->first_char - ft.text), (uns)(range->last_char - ft.text), range->context_len);
	DUMP_MASK(DUMP_INTERTAG_SPACES, "<%s>", type_names[type]);
	struct ft_word *last_word = ft.words-1;
	while (text < range->last_char)
	{
		uns c = 0, rep_c = 0, rep_max = 0, rep_n = 0;
		int empty = 1;
		byte *bow = text, *eow = text;
		while (text < range->last_char)
		{
			GET_TAGGED_CHAR(text, c);
			if (c >= 0x80000000
			|| (alpha_class[c] != AC_ALPHA
			  && alpha_class[c] != AC_DIGIT
			  && alpha_class[c] != AC_LIGATURE))	// assuming that the expansion of the ligature consists of only alphanumerical characters
				break;
			if (c == rep_c)
				if (++rep_n > rep_max)
					rep_max = rep_n;
			rep_c = c;
			empty = 0;
			eow = text;
		}
		if (!empty)
		{
			int sign = 0;
			/* We can reach EOF here due to the existence of non-indexed words.  */
			struct ft_word *w = ft_find_word_by_orig(bow, last_word);
			last_word = w;
			if (w && w->orig == bow && w->weight)
			{
				if (w->weight > 1)
					sign = 2;
				else
					sign = 1;
			}
			if (rep_max < filter_repeated_alpha || sign)
			{
				if (sign == 2)
					DUMP_CONST(space, "<best>");
				else if (sign == 1)
					DUMP_CONST(space, "<found>");
				if (sign)
					space = 0;
				dump_word(&line_len, bow, eow - bow, space);
				if (sign == 2)
					DUMP_CONST(0, "</best>");
				else if (sign == 1)
					DUMP_CONST(0, "</found>");
				space = 0;
			}
		}
		if (c >= 0x80000000)
		{
			if (c < 0x80010000)
			{
				uns new_type = c & 0x0f;
				if (new_type != type)
					DUMP_MASK(DUMP_INTERTAG_SPACES, "</%s>", type_names[type]);
				if (c & 0x10 && eow > range->first_char)
					DUMP_CONST(DUMP_INTERTAG_SPACES, "<break>");
				if (new_type != type)
					DUMP_MASK(DUMP_INTERTAG_SPACES, "<%s>", type_names[new_type]);
				type = new_type;
			}
			else
				ASSERT(0);
			space = 1;
		}
		else if (Uspace(c))
		{
			space = 1;
		}
		else if (eow < range->last_char)			/* slash, ... */
		{
			byte *t = text;
			uns cnt = 0, next;
			do
			{
				cnt++;
				text = t;
				GET_TAGGED_CHAR(t, next);
			}
			while (t < range->last_char && next == c);
			if (cnt >= filter_repeated_nonalpha)
				space = 1;
			else
			{
				byte *printing = html_escape(c), buf[7];
				uns len;
				if (printing)
					len = strlen(printing);
				else
				{
					printing = buf;
					printing = utf8_put(printing, c);
					len = printing - buf;
					printing = buf;
				}
				while (cnt--)
				{
					dump_word(&line_len, printing, len, space);
					space = 0;
				}
			}
		}
	}
	DUMP_MASK(1, "</%s>", type_names[type]);
	if (!whole_text)
		DUMP_CONST(DUMP_INTERTAG_SPACES, "</block>");
	dump_word(&line_len, NULL, 0, 0);
}

/***** Computing intervals that will be dumped *****/

#define	IS_SENTENCE_BREAK(c)	((c) == '.' || (c) == '?' || (c) == '!')
#define	IS_WORD_BREAK(c)	(Uspace(c) || (c) == ',' || (c) == ';' || (c) == ':' || (c) == '-' || (c) == '/')

#define	PREV_TAGGED_CHAR(pos, pos1, limit, c) ({\
	byte *pos2;\
	pos1 = pos;\
	do { pos1--; }\
	while (pos1 > limit && *pos1 >= 0x80 && *pos1 < 0xc0);\
	do { pos2 = pos1; GET_TAGGED_CHAR(pos1, c); }\
	while (pos1 < pos);\
	pos1 = pos2;\
})

static void
contint_delete(struct context_interval *ints, uns maxints, uns i)
{
	for (; i<maxints-1 && ints[i].first_char; i++)
		ints[i] = ints[i+1];
	bzero(ints + i, sizeof(struct context_interval));
}

static void
contint_insert(struct context_interval *ints, uns maxints, uns i)
{
	ASSERT(!ints[maxints-1].first_char);
	for (uns j=maxints-1; j>i; j--)
		ints[j] = ints[j-1];
	bzero(ints + i, sizeof(struct context_interval));
}

static uns
printable_characters(byte *pos, byte *pos_end)
{
	uns len = 0;
	while (pos < pos_end)
	{
		uns c;
		GET_TAGGED_CHAR(pos, c);
		if (c < 0x8000000)
			len++;
	}
	return len;
}

static byte *
skip_spaces(byte *pos, byte *pos_end)
{
	while (pos < pos_end)
	{
		char *pos0 = pos;
		uns c;
		GET_TAGGED_CHAR(pos, c);
		if (c < 0x80000000 && c != ' ')
			return pos0;
	}
	return pos;
}

static uns
contint_add(struct query *q, byte *pos, byte *pos_end, struct context_interval *ints, uns maxints, int available)
{
	ASSERT(pos && pos_end && pos <= pos_end);
	ASSERT(pos >= ft.text && pos_end <= ft.text_end);
	TRACE("  contint_add: pos %d+%d, available %d, maxlen %d",
		(uns)(pos - ft.text), (uns)(pos_end - pos), available, (uns)(ft.text_end - ft.text));
#define INT(in) (int)((in)->first_char - ft.text), (int)((in)->last_char - ft.text), (in)->context_len
	if (available < (int)(pos_end - pos))
	{
		TRACE("    No room for that");
		return 0;
	}

	/* Decide whether we hit an existing interval and compute the bounds
	 * for the stretching operation.  The interval array is sorted and the
	 * interval are pairwise disjoint and non-empty.  */
	struct context_interval tmp_int, *I = NULL;
	byte *softl = ft.text;
	byte *softr = ft.text_end;
	for (uns i = 0; i < maxints && ints[i].first_char; i++)
	{
		if (pos >= ints[i].first_char && pos < ints[i].last_char
		|| pos_end > ints[i].first_char && pos_end <= ints[i].last_char
		|| pos < ints[i].first_char && pos_end > ints[i].last_char)
		{
			I = ints + i;
			TRACE("    Already contained by interval %d: (%d,%d)=%d", i, INT(I));
		}
		else if (ints[i].last_char <= pos)
		{
			ASSERT(ints[i].last_char > softl);
			softl = ints[i].last_char;
		}
		else if (ints[i].first_char >= pos_end)
		{
			softr = ints[i].first_char;
			break;
		}
	}
	uns total_added = 0;
	if (!I)
	{
		/* We might already have reached the maximum number of
		 * intervals, but ignore this for now, since the new interval
		 * might be merged during stretching.  */
		I = &tmp_int;
		I->first_char = pos;
		I->last_char = pos_end;
		I->start_type = FT_TYPE_UNSET;
		I->context_len = printable_characters(pos, pos_end);
		TRACE("    Temporarily created new interval (%d,%d)=%d", INT(I));
		total_added = I->context_len;
	}
	else
	{
		/* Enlarge the original interval if needed.  */
		if (pos < I->first_char)
		{
			uns sub_len = printable_characters(pos, I->first_char);
			total_added += sub_len;
			I->context_len += sub_len;
			I->first_char = pos;
			TRACE("    Extended to (%d,%d)=%d", INT(I));
		}
		if (pos_end > I->last_char)
		{
			uns sub_len = printable_characters(I->last_char, pos_end);
			total_added += sub_len;
			I->context_len += sub_len;
			I->last_char = pos_end;
			TRACE("    Extended to (%d,%d)=%d", INT(I));
		}
	}
	available -= total_added;

	/* Try to stretch the current interval backward.  */
	int added = 0;
	int limit1 = available/3;
	int limit2 = available/2;
	if (I->last_char == ft.text_end)
	{
		limit1 = available*2/3;
		limit2 = available;
	}
	byte *may_break = NULL;
	byte *nice_break = NULL;
	uns c;
	pos = I->first_char;
	while (pos > softl && added < limit2)
	{
		byte *pos1;
		PREV_TAGGED_CHAR(pos, pos1, ft.text, c);
		if (c < 0x80000000)
		{
			added++;
			if (IS_WORD_BREAK(c))
				may_break = pos;
			else if (added >= limit1 && IS_SENTENCE_BREAK(c))
				nice_break = pos;
		}
		else if (c < 0x80010000)
			may_break = pos;
		pos = pos1;
	}
	if (pos == softl)
		may_break = pos;
	if (nice_break)
		may_break = nice_break;
	if (may_break && pos < I->first_char)
	{
		added = printable_characters(may_break, I->first_char);
		available -= added;
		total_added += added;
		I->context_len += added;
		I->first_char = may_break;
		TRACE("    Stretched to the left side by %d characters -> (%d,%d)=%d", added, INT(I));
	}

	/* Find out the new starting type.  */
	pos = I->first_char;
	GET_TAGGED_CHAR(pos, c);
	while (1)
	{
		byte *pos1;
		ASSERT(pos > ft.text);
		PREV_TAGGED_CHAR(pos, pos1, ft.text, c);
		if (c >= 0x80000000 && c < 0x80001000)
		{
			I->start_type = c & 0x0f;
			break;
		}
		pos = pos1;
	}

	/* Try to stretch the current interval forward.  */
	added = 0;
	limit1 = available*2/3;
	limit2 = available;
	pos = I->last_char;
	may_break = nice_break = NULL;
	while (pos < softr && added < limit2)
	{
		byte *pos1;
		uns c;
		pos1 = pos;
		GET_TAGGED_CHAR(pos, c);
		if (c < 0x80000000)
		{
			added++;
			if (IS_WORD_BREAK(c))
			{
				if (Uspace(c))
					may_break = pos1;
				else
					may_break = pos;
			}
			else if (added >= limit1 && IS_SENTENCE_BREAK(c))
				nice_break = pos;
		}
		else if (c < 0x80001000)
			may_break = pos1;
	}
	if (pos == softr)
		may_break = pos;
	if (nice_break)
		may_break = nice_break;
	if (may_break && (added = printable_characters(I->last_char, may_break)))
	{
		available -= added;
		total_added += added;
		I->context_len += added;
		I->last_char = may_break;
		TRACE("    Stretched to the right side by %d characters -> (%d,%d)=%d", added, INT(I));
	}

	byte *ext_softl = skip_spaces(softl, softr);
	if ((I->first_char == softl || I->first_char == ext_softl) && softl > ft.text)
	{
		/* Merge to the left.  */
		uns i;
		for (i = 0; ints[i].last_char != softl; i++);
		uns pad = printable_characters(softl, ext_softl);
		ints[i].last_char = I->last_char;
		ints[i].context_len += I->context_len + pad;
		TRACE("    Merged with interval %d on the left side (padding %d) -> (%d,%d)=%d", i, pad, INT(&ints[i]));
		available -= pad;
		total_added += pad;
		if (I != &tmp_int)
		{
			ASSERT(I == ints + (i+1));
			contint_delete(ints, maxints, i+1);
		}
		I = ints+i;
	}
	byte *lc = skip_spaces(I->last_char, softr);
	if ((I->last_char == softr || lc == softr) && softr < ft.text_end)
	{
		/* Merge to the right.  */
		uns i;
		for (i = 0; ints[i].first_char != lc; i++);
		uns pad = printable_characters(I->last_char, lc);
		ints[i].first_char = I->first_char;
		ints[i].context_len += I->context_len + pad;
		ints[i].start_type = I->start_type;
		TRACE("    Merged with interval %d on the right side (padding %d) -> (%d,%d)=%d", i, pad, INT(&ints[i]));
		available -= pad;
		total_added += pad;
		if (I != &tmp_int)
		{
			ASSERT(I == ints + (i-1));
			contint_delete(ints, maxints, i-1);
		}
		I = ints+i;
	}
	if (I == &tmp_int)
	{
		/* New interval.  */
		uns i, right_neighbour = 0;
		for (i = 0; i < maxints && ints[i].first_char; i++)
			if (ints[i].first_char < tmp_int.first_char)
				right_neighbour = i + 1;
		if (i >= maxints)
		{
			TRACE("    Cannot add a new interval, since the maximum %d has already been reached", maxints);
			return 0;
		}
		contint_insert(ints, maxints, right_neighbour);
		ints[right_neighbour] = tmp_int;
		TRACE("    Added a new interval (%d,%d)=%d before %d", INT(I), right_neighbour);
	}
	return total_added;
#undef INT
}

static void
dump_context(struct query *q, uns max_intervals, int context)
{
	/* Select only positions pointing to our position-space.  */
	byte *type_title = ft.input_type != FT_TEXT ? "meta" : "text";
	TRACE("Dumping %s into length %d and %d intervals", type_title, context, max_intervals);
	uns count = 0;
	for (uns i=0; i<NUM_BESTS && ft.bests[i].pos != POS_NOWHERE; i++)
	{
		TRACE("    Best %d: pos %d, Q %d", count, ft.bests[i].pos, ft.bests[i].weight);
		count++;
	}

	/* Verbose debug messages.  */
	if (0)
	{
		TRACE("  Length of text: %d", (int)(ft.text_end-ft.text));
		char mask[24];
		sprintf(mask, "    Text: %%%d.%ds", (uns)(ft.text_end-ft.text), (uns)(ft.text_end-ft.text));
		TRACE(mask, ft.text);
		for (uns i=0; i<ft.num_words; i++)
		{
			struct ft_word *w = ft.words + i;
			char mask[24];
			sprintf(mask, "    Word %%3d: l%%2d c%%x \"%%%d.%ds\"", w->olen, w->olen);
			TRACE(mask, w->pos, w->olen, w->type, w->orig);
		}
	}

#define	ADD_CONTEXT(start, end, how_much, message) { \
	uns added_context = contint_add(q, start, end, interval, max_intervals, how_much); \
	context -= added_context; \
	if (added_context > 0) \
		TRACE message; \
}
	/* Find optimal context intervals.  */
	struct context_interval interval[max_intervals];
	bzero(interval, max_intervals * sizeof(struct context_interval));
	if (count > 0)
	{
		int context1 = context / count;
		uns i;
		/* Distribute the available context approximately uniformely
		 * among the best matches.  */
		for (i=0; i<count && context > 0; i++)
		{
			struct ft_word *w = ft_find_word_by_pos(ft.bests[i].pos);
			ADD_CONTEXT(w->orig, w->orig + w->olen, context1,
				("    -> Added best interval of length %d", added_context));
		}
		/* If we still have an available context, try to spend it
		 * again, preferring the former intervals.  */
		for (i=0; i < max_intervals && interval[i].first_char && context > 0; i++)
			ADD_CONTEXT(interval[i].last_char, interval[i].last_char, context/2,
				("    -> Stretched best interval %d by length %d", i, added_context));
	}
	if (!interval[0].first_char)
	{
		if (ft.num_words)
		{
			struct ft_word *w = ft.words;
			ADD_CONTEXT(w->orig, w->orig + w->olen, context,
				("    -> Added beginning of text of length %d", added_context));
		}
		else if (ft.text_end > ft.text)	/* might consist of non-words only */
		{
			byte *pos = ft.text;
			uns c;
			GET_TAGGED_CHAR(pos, c);	/* skip the first change of type */
			if (pos < ft.text_end)
			{
				byte *pos1 = pos;
				GET_TAGGED_CHAR(pos1, c);
				ADD_CONTEXT(pos, pos1, context,
					("    -> Added beginning of non-word text of length %d", added_context));
			}
		}
	}
	while (context > 0)
	{
		int save_context = context;
		for (uns i=0; i < max_intervals && interval[i].first_char && context > 0; i++)
			ADD_CONTEXT(interval[i].last_char, interval[i].last_char, context,
				("    -> Finally, stretched interval %d by length %d", i, added_context));
		if (context == save_context)
			break;
	}

	/* Dump the titles and the highlighted context.  */
	uns end = 0;
	for (uns i=0; i<max_intervals && interval[i].first_char; i++)
		if (interval[i].last_char == ft.text_end)
			end = 1;
	byte label[40];
	int see = simplified_eval_expr(ft_context->query->prep_expr, ft.bool_mask);
	sprintf(label, "<%s w=%x m=%d e=%d>", type_title, ft.word_mask, see >= 0, end);
	for (uns i=0; i<max_intervals && interval[i].first_char; i++)
	{
		TRACE("  => Result: Interval %d of type %s, bytes %d-%d context %d", i,
			(ft.input_type == FT_TEXT ? wt_names : mt_names) [interval[i].start_type],
			(uns)(interval[i].first_char - ft.text),
			(uns)(interval[i].last_char - ft.text),
			interval[i].context_len);
		dump_text_interval(i ? NULL : label, interval + i);
	}
}

/***** Dumper of URL records *****/

struct save_lm_pos {		/* here we remember the word-numbering of the lex-mapper */
	ref_pos_t meta[MT_MAX];
};

#define BT_SHIFT	28
#define BT_COUNT	6
#define BT_LETTERS	"MUycx?"
#define BT_MAIN		(0 << BT_SHIFT)
#define BT_URL		(1 << BT_SHIFT)
#define BT_REDIRECT	(2 << BT_SHIFT)
#define BT_CATALOG	(3 << BT_SHIFT)
#define BT_REFTEXT	(4 << BT_SHIFT)
#define BT_USER		(5 << BT_SHIFT)
#define BT_MASK		(0xf << BT_SHIFT)
#define BT_BASIS	(1 << (BT_SHIFT-1))

struct bracket {
	uns sons;		// bracket_type | number of sub-brackets
	byte *start, *end;	// of the original text stream
	int weight;		// final weight with all the factors taken into account
	struct save_lm_pos head_pos, tail_pos;
	struct bracket *son[0];
};

static struct bracket *
parse_brackets(struct query *q, uns type, byte **start, byte *end, struct save_lm_pos *saved_pos)
{
	struct bracket nn = {
		.sons = 0,
		.start = *start,
		//.end
		.weight = (type != BT_USER ? 1000 : 0),
		.head_pos = *saved_pos,
		.tail_pos = *saved_pos,
		//.son[]
	};
	int sub_weight = 0;	// maximum weight of a sub-bracket
	int has_tail = 0;
	uns occur[2] = { 0, 0 };	// mask of occurences of each word in URL's and metas

	static struct mempool *ll_pool = NULL;
	if (type == BT_MAIN)
	{
		if (ll_pool)
			mp_flush(ll_pool);
		else
			ll_pool = mp_new(1024);
	}
	struct ll_brackets {
		struct ll_brackets *next;
		struct bracket *b;
	} *first = NULL, **last = &first;

	struct parsed_attr pa;
	byte *start_attr, *attr;
	for (attr = *start; start_attr=attr, get_attr(&attr, end, &pa) >= 0; )
	{
		if (pa.attr == '(')
		{
			uns subtype;
			if (type == BT_USER)
				subtype = BT_USER;
			else
			{
				switch (pa.val[0])
				{
					case 'U':	subtype = BT_URL; break;
					case 'y':	subtype = BT_REDIRECT; break;
					case 'c':	subtype = BT_CATALOG; break;
					case 'x':	subtype = BT_REFTEXT; break;
					default:	ASSERT((pa.val[0] >= '0' && pa.val[0] <= '9') || pa.val[0] == 'f');
							subtype = BT_USER; break;
				}
				ASSERT(subtype > type);
			}
			struct bracket *s = parse_brackets(q, subtype, &attr, end, saved_pos);
			s->start = start_attr;					// add the opening parenthese
			sub_weight = MAX(sub_weight, s->weight);

			struct ll_brackets *nb = mp_alloc(ll_pool, sizeof(struct ll_brackets));
			nb->next = NULL;
			nb->b = s;
			*last = nb;
			last = &nb->next;
			nn.sons++;
		}
		else if (!pa.attr)
		{
			ASSERT(type == BT_MAIN);
			break;
		}
		else if (pa.attr == ')')
		{
			ASSERT(type != BT_MAIN);
			ASSERT(pa.len == 0);
			break;
		}
		/* otherwise it is a text attribute either before the 1st sub-bracket or after the last one */
		else if (type != BT_USER) switch (pa.attr)
		{
			case 'y':
			case 'U':
				ft_parse(pa.val, pa.val + pa.len, NULL, FT_URL);
				occur[0] |= ft.word_mask;
				nn.weight -= pa.len;
				break;
			case 'W':
				nn.weight += 4 * atoi(pa.val+1);	// skip one-letter type before the pagerank
				break;
			case 'M':
				/* Meta information corresponding to the URL-record.
				 * It contains NO type change.
				 *
				 * Some metas in the card might have not been indexed by chewer (see
				 * Chewer.GiantBanMeta), however we will not disturb anything by
				 * indexing them now, since each meta-type is either completely
				 * indexed or completely unindexed.
				 * */
				; byte *a = pa.val;
				if (*a >= '0' && *a <= '3')
					a++;
				ASSERT(*a >= 0x90 && *a < 0xa0);
				uns mt = *a & 0x0f;
				if (!has_tail)
				{
					nn.tail_pos = *saved_pos;
					has_tail = 1;
				}
				ft_parse(pa.val, pa.val + pa.len, saved_pos->meta + mt, FT_META);
				ft.text = a;
				occur[1] |= ft.word_mask;
				uns best_matches = 0;
				for (uns i=0; i<ft.num_words; i++)
					if (ft.words[i].weight > 1)
						best_matches++;
				if (best_matches)
					nn.weight += (5 * q->dbase->meta_weights[mt][0] + 200) * best_matches;
				break;
		}
	}

	struct bracket *n = xmalloc(sizeof(struct bracket) + nn.sons * sizeof(struct bracket *));
	*n = nn;
	n->end = *start = attr;
	uns idx = 0;
	while (first)
	{
		n->son[idx++] = first->b;
		first = first->next;
	}
	ASSERT(idx == nn.sons);

	for (uns i=0; i<n->sons; i++)
	{
		n->son[i]->weight -= 1000 * i / n->sons;
	}
	n->weight += sub_weight;
	char tmp[HARD_MAX_WORDS * 20] = "", *dump = tmp;
	for (uns i=0; i<max_words; i++)
	{
		if (occur[0] & (1<<i))
		{
			n->weight += 5000;
			dump += sprintf(dump, "U%d ", i);
		}
#define	WEIGHT_FOUND_META	3500
		if (occur[1] & (1<<i))
		{
			n->weight += WEIGHT_FOUND_META;
			dump += sprintf(dump, "M%d ", i);
		}
	}

	uns head = n->sons ? n->son[0]->start - n->start : 0;
	uns tail = n->sons ? n->end - n->son[n->sons-1]->end : 0;
	TRACE("Parsed %c-bracket of length %d=%d+%dsons+%d and weight1 %d%s%s",
		BT_LETTERS[type >> BT_SHIFT], (int)(n->end - n->start), head, n->sons, tail, n->weight,
		((occur[0] || occur[1]) ? ", occurences: " : ""), tmp);

	n->sons |= type;
	return n;
}

static void
trace_brackets(struct query *q, struct bracket *n, uns level, uns id)
{
	byte tmp[level+1];
	for (uns i=0; i<level; i++)
	  tmp[i] = '.';
	tmp[level] = 0;
	TRACE("%sFinal weight2 of the %d-th %c-bracket is %d", tmp, id, BT_LETTERS[(n->sons & BT_MASK) >> BT_SHIFT], n->weight);
	for (uns i=0; i<(n->sons & ~BT_MASK); i++)
		trace_brackets(q, n->son[i], level+1, i+1);
}

static void
free_brackets(struct bracket *n)
{
	for (uns i=0; i<(n->sons & ~BT_MASK); i++)
		free_brackets(n->son[i]);
	xfree(n);
}

static void
dump_html_escaped_word(byte *start, byte *end)
{
	while (start < end)
	{
		byte *c = start;
		byte *seq_name = NULL;
		while (c < end && !(seq_name = html_escape(*c) ))
			c++;
		if (c > start)
			send_reply_block(start, c-start);
		if (seq_name)
		{
			send_reply_string(seq_name);
			c++;
		}
		start = c;
	}
}

static void
dump_highlighted_url(byte *start, byte *end, byte *tag_name)
{
	/* Deescape the URL to not confuse e.g. ~ == %7E with words.  */
	byte original_url[end-start+1], deescaped_url[end-start+1];
	memcpy(original_url, start, end-start);
	original_url[end-start] = 0;
	url_deescape(original_url, deescaped_url);
	url_enescape_friendly(deescaped_url, original_url);	// it will stretch to at most original length
	start = original_url;
	end = start + strlen(start);

	ft_parse(start, end, NULL, FT_URL);
	/* The input is just an ordinary ASCII-text without control sequences.  */
	byte buf[20];
	sprintf(buf, "M<%s>", tag_name);
	send_reply_string(buf);
	byte *dumped = start;
	for (uns i=0; i<ft.num_words; i++)
	{
		struct ft_word *w = ft.words + i;
		dump_html_escaped_word(dumped, w->orig);
		if (w->weight)
			send_reply_string("<found>");
		if (w->weight || !highlight_substring)
			send_reply_block(w->orig, w->olen);
		else
		{
			byte *c = w->orig, *c_end = w->orig + w->olen;
			while (c < c_end)
			{
				struct kmp_search search;
				search.u.best_pos = NULL; /* not necessary, only hides warning */
				search.u.src_end = c_end;
				kmp_search(ft_kmp, &search, c);
				if (!search.u.best_len)
				{
					send_reply_block(c, c_end-c);
					break;
				}
				else
				{
					byte *p = search.u.best_pos - search.u.best_len;
					send_reply_block(c, p-c);
					send_reply_string("<match>");
					send_reply_block(p, search.u.best_len);
					send_reply_string("</match>");
					c = search.u.best_pos;
				}
			}
		}
		if (w->weight)
			send_reply_string("</found>");
		dumped = w->orig + w->olen;
	}
	dump_html_escaped_word(dumped, end);
	send_reply("</%s>", tag_name);
}

static void
dump_lines_between(struct query *q, struct save_lm_pos *saved_pos, byte *start, byte *end, struct bracket *n)
{
	struct parsed_attr pa;
	for (byte *record = start; get_attr(&record, end, &pa) > 0; )
	{
		if (pa.attr == 'M')
		{
			byte *a = pa.val;
			if (*a >= '0' && *a <= '3')
				a++;
			ASSERT(*a >= 0x90 && *a < 0xa0);
			uns meta_type = *a & 0x0f;
			ft_parse(pa.val, pa.val + pa.len, saved_pos->meta + meta_type, FT_META);
			ft.text = a;
			dump_context(q, 2, q->meta_chars[meta_type]);
		}
		else if (pa.attr == 'X')
		{
			ref_pos_t pos = 0;
 			ft_parse(pa.val, pa.val + pa.len, &pos, FT_TEXT);
			dump_context(q, q->intervals, q->context_chars);
		}
		else
		{
			byte tmp = pa.attr;
			send_reply_block(&tmp, 1);
			send_reply_block(pa.val, pa.len);
			send_reply_string("\n");
		}

		if (pa.attr == 'U')
			dump_highlighted_url(pa.val, pa.val + pa.len, "url");
		else if (pa.attr == 'y' && (n->sons & BT_MASK) != BT_CATALOG)
			dump_highlighted_url(pa.val, pa.val + pa.len, "redirect");
		else if (pa.attr == 'b' && (n->sons & BT_MASK) != BT_CATALOG)
			dump_highlighted_url(pa.val, pa.val + pa.len, "frameof");
		else if (strchr(url_attributes, pa.attr))
		{
			byte tagname[9] = "custom-0";
			tagname[7] = pa.attr;
			dump_highlighted_url(pa.val, pa.val + pa.len, tagname);
		}
	}
}

#define	ASORT_PREFIX(x)	bracket_sons_##x
#define	ASORT_KEY_TYPE	uns
#define	ASORT_ELT(i)	((br->son[i]->sons & BT_MASK) | (BT_BASIS - br->son[i]->weight))
#define	ASORT_SWAP(i,j)	do { struct bracket *tmp=br->son[j]; br->son[j]=br->son[i]; br->son[i]=tmp; } while(0)
#define	ASORT_EXTRA_ARGS	, struct bracket *br
#include "ucw/sorter/array-simple.h"

static void
dump_brackets(struct query *q, struct bracket *n)
{
  	if ((n->sons & BT_MASK) == BT_USER)
	{
		struct parsed_attr pa;
		for (byte *record = n->start; get_attr(&record, n->end, &pa) > 0; )
		{
		  	send_reply_block((byte *)&pa.attr, 1);
			send_reply_block(pa.val, pa.len);
			send_reply_string("\n");
		}
		return;
	}
	uns sons = n->sons & ~BT_MASK;
	if (!sons)
	{
		dump_lines_between(q, &n->head_pos, n->start, n->end, n);
		return;
	}
	byte *head = n->son[0]->start;
	byte *tail = n->son[sons-1]->end;
	uns limits[BT_COUNT] = { 0, q->url_max, global_max_redir_brack, global_max_cat_brack, ~0U, ~0U };

	bracket_sons_sort(sons, n);		// by ascending bracket-type and then by descending weight

	dump_lines_between(q, &n->head_pos, n->start, head, n);
	uns end;
	for (uns i=0; i<sons; i=end)
	{
		uns t = n->son[i]->sons & BT_MASK;
		for (end=i; end<sons && (n->son[end]->sons & BT_MASK) == t; end++);
		uns stop = i + MIN(end-i, limits[t >> BT_SHIFT]);
		for (uns j=i; j<stop; j++)
		{
			if (t == BT_REFTEXT && q->context_chars != CONTEXT_FULL && n->son[j]->weight < WEIGHT_FOUND_META)
				break;
			dump_brackets(q, n->son[j]);
		}
	}
	dump_lines_between(q, &n->tail_pos, tail, n->end, n);
}

/***** Fetching of cards *****/

struct card_fetch {
  struct work w;
  struct card_attr *ca;
  struct database *db;
  void *card;
  uns done;
};

static struct work_queue fetch_wqueue;
static struct worker_pool fetch_wpool;

static inline ucw_off_t
fetch_pos(struct card_fetch *f)
{
  return (ucw_off_t) f->ca->card << CARD_POS_SHIFT;
}

static inline uns
fetch_len(struct card_fetch *f)
{
  return ((f->ca+1)->card - f->ca->card) << CARD_POS_SHIFT;
}

static inline int
fetch_fd(struct card_fetch *f)
{
  return f->db->fd_cards;
}

static void
fetch_single(struct worker_thread *t UNUSED, struct work *work)
{
  /* Called by worker threads to service fetch requests */
  struct card_fetch *f = (struct card_fetch *) work;
  uns len = fetch_len(f);
  if ((uns)ucw_pread(fetch_fd(f), f->card, len, fetch_pos(f)) != len)
    f->card = NULL;
}

static void
fetch_start(struct query *q)
{
  struct card_fetch *fetches = q->card_fetches;
  uns n = q->ncard_fetches;
  ASSERT(fetches);
  if (!n)
    return;

  prof_t *o = profiler_switch(&prof_resf);
  if (fetch_threads)
    {
      ASSERT(!fetch_wqueue.nr_running);
      for (uns i=0; i<n; i++)
        {
	  struct card_fetch *f = &fetches[i];
	  f->w.go = fetch_single;
	  f->card = mp_alloc(q->pool, fetch_len(f));
	  log_fetch(q, fetch_fd(f), fetch_pos(f), fetch_len(f));
	}
      for (uns i=0; i<n; i++)
        work_submit(&fetch_wqueue, &fetches[i].w);
    }
  else
    {
      struct mmap_request mmaps[n];
      for (uns i=0; i<n; i++)
        {
	  struct card_fetch *f = &fetches[i];
          struct mmap_request *m = &mmaps[i];
          m->u.req.fd = fetch_fd(f);
          m->u.req.start = fetch_pos(f);
          m->u.req.end = m->u.req.start + fetch_len(f);
          m->userdata = i;
	  f->done = 1;
	}
      if (mmap_regions(q, mmaps, n) < 0)
        {
	  /* If mmapping fails, return with all cards unread, but done */
          log(L_ERROR, "Error mapping index cards for query result display");
          return;
        }
      for (uns i=0; i<n; i++)
	fetches[mmaps[i].userdata].card = mmaps[i].u.map.start;
    }
  profiler_switch(o);
}

static void
fetch_wait(struct card_fetch *f)
{
  if (fetch_threads)
    {
      if (!f->done)
	{
	  prof_t *o = profiler_switch(&prof_resf);
	  while (!f->done)
	    {
	      struct card_fetch *ok = (struct card_fetch *)work_wait(&fetch_wqueue);
	      ASSERT(ok);
	      ok->done = 1;
	    }
	  profiler_switch(o);
	}
    }
  else
    ASSERT(f->done);
}

static byte *
fetch_decompress(struct card_fetch *f, byte **end)
{
  byte *ptr;
  uns type;

  fetch_wait(f);
  if (!f->card)
    {
      log(L_ERROR, "Cannot load object %08x from database %s", (uns)(f->ca - f->db->card_attrs), f->db->name);
      return NULL;
    }

  int len = lizard_memread(liz_buf, f->card, &ptr, &type);
  if (len < 0)
    die("Cannot decompress object %08x from database %s: %m", (uns)(f->ca - f->db->card_attrs), f->db->name);
  get_attr_set_type(type);
  *end = ptr + len;
  return ptr;
}

void
prefetch_results_cleanup(struct query *q)
{
  /* There still could be some prefetches running, so better wait for all of them. */
  for (uns i=0; i<q->ncard_fetches; i++)
    fetch_wait(&q->card_fetches[i]);
  ASSERT(!fetch_threads || !fetch_wqueue.nr_running);
}

static void
card_fetch_init(void)
{
  if (fetch_threads)
    {
      fetch_wpool.num_threads = fetch_threads;
      worker_pool_init(&fetch_wpool);
      work_queue_init(&fetch_wpool, &fetch_wqueue);
    }
}

/***** The body of the card-dumper *****/

static void
show_card(struct query *q, struct card_attr *ca, struct result_note *note, byte *card_start, byte *card_end)
{
	oid_t oid;
	struct database *db = attr_to_db(ca, &oid);
	TRACE("*** Dumping card %08x from db %s ***", (uns)oid, db->name);

	ft_context = ft_init_card(q, db, ca);
	ASSERT(ft_context);
	show_card_attr(q, db, oid, ca);
	show_card_note(q, note);

	struct save_lm_pos saved_pos;
	bzero(&saved_pos, sizeof(struct save_lm_pos));
	struct bracket *br = parse_brackets(q, BT_MAIN, &card_start, card_end, &saved_pos);
	trace_brackets(q, br, 0, 0);
	dump_brackets(q, br);
	free_brackets(br);

	for (uns i=0; i<MT_MAX; i++)
		if (saved_pos.meta[i] > 0)
			TRACE("Total metas of type %d (%s) in card: %d", i, mt_names[i], saved_pos.meta[i]);

	send_reply_string("\n");
}

int
check_result_set(struct query *q)
{
  uns max_res = 0;
  uns res_cnt = 0;

  switch (q->out_mode)
    {
    case OUT_STATS:
      max_res = 1;
      break;
    case OUT_LIST:
    case OUT_SHOW:
      ASSERT(q->out_range);
      SLIST_FOR_EACH(struct set_node *, rng, *q->out_range)
	{
	  if (rng->min < 1)
	    {
	      add_err("-106 Matches are numbered starting with 1");
	      return 1;
	    }
	  max_res = MAX(max_res, rng->max);
	  if (q->out_mode == OUT_SHOW)
	    res_cnt += rng->max - rng->min + 1;
	}
      break;
    case OUT_DUMP:
      ASSERT(q->out_range);
      if (!q->db_list)
	{
	  add_err("-122 Missing DB specification");
	  return 1;
	}
      SLIST_FOR_EACH(struct dump_node *, n, *q->out_range)
	{
	  struct db_node *d = slist_head(q->db_list);
	  for (int i=0; d && i<n->db_id; i++)
	    d = slist_next(&d->n);
	  if (!d)
	    {
	      add_err("-122 The %d-th database not declared", n->db_id);
	      return 1;
	    }
	  n->db = d->db;
	  if (n->id >= n->db->num_ids)
	    {
	      add_err("-122 Card ID #%x is out of range for database %s", n->id, n->db->name);
	      return 1;
	    }
	  res_cnt++;
	}
      break;
    default:
      ASSERT(0);
    }

  if (max_res > max_matches)
    {
      add_err("-106 Too many documents requested, only first %d matches available.", max_matches);
      return 1;
    }
  if (res_cnt > max_output_matches)
    {
      add_err("-106 At most %d matches can be shown at once", max_output_matches);
      return 1;
    }
  q->needed_results = max_res;
  q->results_to_show = res_cnt;
  return 0;
}

static void
show_results_list(struct query *q)
{
  struct results *res = q->results;

  SLIST_FOR_EACH(struct set_node *, rng, *q->out_range)
    {
      int from = rng->min - 1;
      int to = MIN(rng->max, res->num_matches) - 1;
      while (from <= to)
	{
	  struct result_note *n = &res->matches[from++];
	  oid_t oid;
	  struct database *db = attr_to_db(n->attr, &oid);
	  show_card_attr(q, db, oid, n->attr);
	  show_card_note(q, n);
	  send_reply_string("\n");
	}
    }
}

static void
show_results_full(struct query *q)
{
  struct results *res = q->results;
  struct result_note *notes[q->results_to_show];
  uns nres = 0;

  if (!res->num_matches)
    return;

  SLIST_FOR_EACH(struct set_node *, rng, *q->out_range)
    {
      uns from = rng->min - 1;
      uns to = MIN(rng->max, res->num_matches) - 1;
      while (from <= to)
	notes[nres++] = &res->matches[from++];
    }
  ASSERT(nres <= q->results_to_show);
  q->card_fetches = mp_alloc_zero(q->pool, sizeof(struct card_fetch) * nres);
  q->ncard_fetches = nres;
  for (uns i=0; i<nres; i++)
    {
      struct card_fetch *f = &q->card_fetches[i];
      f->ca = notes[i]->attr;
      f->db = attr_to_db(f->ca, NULL);
    }
  fetch_start(q);

  ft_kmp = kmp_create(q);
  for (uns i=0; i<nres; i++)
    {
      struct card_fetch *f = &q->card_fetches[i];
      byte *ptr, *end;
      if (!(ptr = fetch_decompress(f, &end)))
	return;
      show_card(q, notes[i]->attr, notes[i], ptr, end);
    }
}

static void
dump_results_prefetch(struct query *q)
{
  q->card_fetches = mp_alloc_zero(q->pool, sizeof(struct card_fetch) * q->results_to_show);
  SLIST_FOR_EACH(struct dump_node *, n, *q->out_range)
    {
      struct card_fetch *f = &q->card_fetches[q->ncard_fetches++];
      f->ca = n->db->card_attrs + n->id;
      f->db = n->db;
    }
  fetch_start(q);
}

static void
dump_results(struct query *q)
{
  if (!q->card_fetches)
    dump_results_prefetch(q);
  ft_kmp = kmp_create(q);
  uns i = 0;
  SLIST_FOR_EACH(struct dump_node *, n, *q->out_range)
    {
      struct card_fetch *f = &q->card_fetches[i++];
      byte *ptr, *end;
      if (!(ptr = fetch_decompress(f, &end)))
	return;
      show_card(q, f->ca, NULL, ptr, end);
    }
}

void
prefetch_results(struct query *q)
{
  /* In threaded mode, we can parallelize card prefetch for DUMPs with query analysis. */
  if (q->out_mode == OUT_DUMP && fetch_threads)
    dump_results_prefetch(q);
}

void
show_results(struct query *q)
{
  profiler_switch(&prof_results);
  switch (q->out_mode)
    {
    case OUT_STATS:
      break;
    case OUT_LIST:
      show_results_list(q);
      break;
    case OUT_SHOW:
      show_results_full(q);
      break;
    case OUT_DUMP:
      dump_results(q);
      break;
    default:
      ASSERT(0);
    }
}

void
cards_init(void)
{
  alphabet_init();
}

void
cards_init_process(void)
{
  card_fetch_init();
}
