/*
 *	Sherlock Filter Engine --- trie data structure
 *
 *	(c) 2003, Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/mempool.h"
#include "ucw/hashfunc.h"
#include "ucw/chartype.h"
#include "ucw/fastbuf.h"
#include "ucw/clists.h"
#include "ucw/string.h"
#include "filter/filter.h"
#include "filter/parse.tab.h"

#include <string.h>

static int
can_be_in_trie(struct filter_case *cs, uns icase, byte **String)
{
	struct filter_cond *cond = cs->cond;
	ASSERT(cond->cat == F_CC_EXPR);
	if (cs->type == F_ET_STRING
	&& cond->o.expr.op == EPAT
	&& !cond->o.expr.neg
	&& cond->o.expr.icase == icase
	&& cond->o.expr.r->cat == F_EC_CONST)
	{
		ASSERT(cond->o.expr.r->type == F_ET_STRING);
		ASSERT(!cs->undef);
		ASSERT(!cond->o.expr.r->undef);
		if (String)
			*String = cond->o.expr.r->o.s;
		return 1;
	}
	return 0;
}

struct len_string
{
	cnode n;
	uns number;
	byte *s;
	uns l;
};

static inline uns
nth_bit(struct len_string *string, uns icase, uns suffix, uns bitnr)
/* Takes into account the case sensitivity and the direction.  */
{
	uns idx = bitnr/8, mask = bitnr%8;
	byte c;
	if (suffix)
		idx = string->l-1 - idx;
	mask = 1<<mask;
	c = string->s[idx];
	if (icase)
		c = Cupcase(c);
	return c&mask ? 1 : 0;
}

static uns gl_icase, gl_suffix;
	/* To avoid passing too many parameters along recursive calls, 2 of
	 * them are stored in global variables.  This causes that building the
	 * trie is not re-entrant.  */

static int
strings_differ(clist *strings, uns bitnr)
/* Returns 2 iff some string has its length exactly bitnr/8,
 * 1 if some strings differ at the bit position bitnr,
 * and 0 otherwise.  */
{
	uns cnt[2] = { 0, 0 };
	void *s;
	CLIST_WALK(s, *strings)
	{
		struct len_string *str = s;
		ASSERT(str->l >= bitnr/8);
		if (str->l == bitnr/8)
		{
			ASSERT(!(bitnr%8));
			return 2;
		}
		cnt[ nth_bit(str, gl_icase, gl_suffix, bitnr) ]++;
	}
	ASSERT(cnt[0] || cnt[1]);
	return cnt[0] && cnt[1];
}

static struct filter_trie_record *
create_subtrie(struct mempool *mp, clist *strings, uns bitnr, int **cmdid_pool, uns *number)
/* Parameters:
 * ===========
 * mp			memory pool where the trie records are allocated from
 * strings		the list of strings the trie is being built for
 * bitnr		compare the strings from the BITNR-th bit up
 * *cmdid_pool		we can use this array as a pool for command ID lists,
 * 			we have to increment *cmdid_pool after a record is allocated
 * *number		the number of processed strings is returned here (for checking only)
 *
 * Global variables:
 * =================
 * gl_icase, gl_suffix	is the trie case sensitive and shall we take suffixed instead of prefixes?
 *
 * The trie record is returned back to the caller.
 */
{
	struct filter_trie_record *r;
	struct len_string *prefix = NULL;
	void *s, *s1;
	uns exactlen_strings = 0, longer_strings = 0;

	/* Compute the number of strings of length bitnr/8 and the number of
	 * longer strings (which are also renumbered).  */
	*number = 0;
	if (clist_empty(strings))
		return NULL;
	CLIST_WALK_DELSAFE(s, *strings, s1)
	{
		struct len_string *str = s;
		ASSERT(str->l >= bitnr/8);
		if (str->l == bitnr/8)
		{
			ASSERT(!(bitnr%8));
			(*cmdid_pool)[exactlen_strings++] = str->number;
			clist_remove(s);
			prefix = str;
		}
		else
			longer_strings++;
	}
	ASSERT(exactlen_strings > 0 || longer_strings > 0);

	if (exactlen_strings > 0)
	{
		/* If some strings have length bitnr/8, create a trie-record
		 * for the bit position bitnr.  */
		ASSERT(prefix->l == bitnr/8);
		r = mp_alloc(mp, sizeof(struct filter_trie_record) + prefix->l + 1);
		memcpy(r->prefix, prefix->s, prefix->l);
		r->prefix[prefix->l] = 0;
		r->cmdids = *cmdid_pool;
		/* Terminate the command ID list.  */
		(*cmdid_pool)[exactlen_strings] = -1;
		*cmdid_pool += exactlen_strings + 1;
		*number = exactlen_strings;
	}
	else
	{
		uns orig_bitnr = bitnr;
		/* Otherwise look for the first bit-position at which the
		 * strings differ.  */
		for ( ; ; bitnr++)
		{
			uns res = strings_differ(strings, bitnr);
			if (res == 2)
			{
				/* Contains strings of length bitnr/8.  */
				ASSERT(bitnr > orig_bitnr);
				void *ret = create_subtrie(mp, strings, bitnr, cmdid_pool, number);
				ASSERT(*number == longer_strings);
				return ret;
			}
			else if (res == 1)
				/* All strings are longer than bitnr/8 and they
				 * differ at bitnr.  */
				break;
		}
		r = mp_alloc(mp, sizeof(struct filter_trie_record));
		r->cmdids = NULL;
	}
	r->bitnr = bitnr;

	/* Process both subtrees.  */
	clist sons[2];
	uns sons_length[2] = { 0, 0 };
	clist_init(sons + 0);
	clist_init(sons + 1);
	CLIST_WALK_DELSAFE(s, *strings, s1)
	{
		struct len_string *str = s;
		ASSERT(str->l > bitnr/8);
		uns bit = nth_bit(str, gl_icase, gl_suffix, bitnr);
		clist_remove(s);
		clist_add_tail(sons + bit, s);
		sons_length[bit]++;
	}
	if (!exactlen_strings)
		ASSERT(!clist_empty(sons + 0) && !clist_empty(sons + 1));
	for (uns bit=0; bit<2; bit++)
	{
		/* Create a son trie-record and accumulate the returned
		 * counters.  */
		uns number1;
		r->son[bit] = create_subtrie(mp, sons + bit, bitnr+1, cmdid_pool, &number1);
		ASSERT(number1 == sons_length[bit]);
		*number += number1;
	}
	return r;
}

struct filter_trie_table *
filter_trie_new(struct mempool *mp, struct filter_cmd *case_cmd, uns icase)
{
	ASSERT(case_cmd->op == SWITCH);
	uns count = 0;
	for (struct filter_case *curr = case_cmd->c.swit.cases; curr; curr=curr->next)
		if (can_be_in_trie(curr, icase, NULL))
			count++;
	if (count < filter_trie_limit)
		return NULL;

	struct filter_trie_table *trie = mp_alloc(mp, sizeof(struct filter_trie_table));
	trie->icase = icase;
	trie->cmds = count;
	trie->cmd = mp_alloc(mp, count * sizeof(struct filter_cmd *));
	trie->cmdid = mp_alloc_zero(mp, 4*count * sizeof(int *));		/* once for prefix, once for suffix; 2*count due to storing -1's */
	trie->results = mp_alloc_zero(mp, count * sizeof(struct filter_trie_result));

	int *cmdid_tail = trie->cmdid;
	gl_icase = icase;
	for (gl_suffix=0; gl_suffix<2; gl_suffix++)
	{
		/* Extract the strings from the struct filter_case's and (in
		 * the 2nd pass only) delete them from the original list.  */
		struct filter_case **cs = &case_cmd->c.swit.cases;
		struct len_string strings[trie->cmds];
		clist string_list;
		clist_init(&string_list);
		uns i = 0, empty_strings = 0;
		while (*cs)
		{
			byte *string;
			if (!can_be_in_trie(*cs, icase, &string))
			{
				cs = &(*cs)->next;
				continue;
			}
			strings[i].number = i;
			/* 1st pass: find a prefix and move to another case of the switch command.
			 * 2nd pass: find a suffix and delete the case of the switch command.  */
			if (!gl_suffix)
			{
				trie->cmd[i] = *cs;
				strings[i].l = strcspn(string, "*?");
				strings[i].s = string;
				cs = &(*cs)->next;
			}
			else
			{
				ASSERT(trie->cmd[i] == *cs);
				byte *c = string + strlen(string);
				while (c > string && c[-1] != '*' && c[-1] != '?')
					c--;
				strings[i].l = strlen(c);
				strings[i].s = c;
				*cs = (*cs)->next;
			}
			/* If the prefix/suffix is longer than 0, add it to the link-list.  */
			if (strings[i].l > 0)
				clist_add_tail(&string_list, &strings[i].n);
			else
			{
				trie->results[i].init_passed++;
				empty_strings++;
			}
			/* 2nd pass: check if the pattern is of the form abc*xyz.  */
			trie->results[i].trivial_pattern += strings[i].l;
			if (gl_suffix)
			{
				if (trie->results[i].trivial_pattern == strlen(string)-1
				&& string[ strlen(string)-1 - strings[i].l ] == '*')
					trie->results[i].trivial_pattern = 1;
				else
					trie->results[i].trivial_pattern = 0;
			}
			i++;
		}
		ASSERT(i == trie->cmds && empty_strings <= trie->cmds);

		/* Build the trie for the extracted strings.  */
		trie->root[ gl_suffix ] = create_subtrie(mp, &string_list, 0, &cmdid_tail, &i);
		ASSERT(cmdid_tail <= trie->cmdid + 4*trie->cmds);
		ASSERT(i + empty_strings == trie->cmds);
	}

	/* Finally, post-process the tables of results.  */
	clist_init(&trie->tests);
	clist_init(&trie->incremented_tests);
	clist_init(&trie->passed_tests);
	for (uns i=0; i<trie->cmds; i++)
	{
		trie->results[i].number = i;
		trie->results[i].passed = trie->results[i].init_passed;
		if (trie->results[i].init_passed == 2)
			clist_add_tail(&trie->passed_tests, &trie->results[i].n);
		else
			clist_add_tail(&trie->tests, &trie->results[i].n);
	}

	return trie;
}

static void
search_trie(struct filter_trie_table *trie, struct filter_trie_record *r, uns suffix, struct len_string *string)
{
	if (!r || r->bitnr/8 > string->l)
		return;
	if (r->cmdids)
	{
		ASSERT(!(r->bitnr%8));
		uns prefix_len = r->bitnr/8;
		byte *s = suffix
			? string->s + string->l - prefix_len
			: string->s;
		int res = trie->icase
			? strncasecmp(r->prefix, s, prefix_len)
			: strncmp(r->prefix, s, prefix_len);
		if (!res)
		{
			for (int *id=r->cmdids; *id != -1; id++)
			{
				uns nr = ++trie->results[ *id ].passed;
				clist_remove(&trie->results[ *id ].n);
				if (nr == 1)
					clist_add_tail(&trie->incremented_tests, &trie->results[ *id ].n);
				else if (nr == 2)
					clist_add_tail(&trie->passed_tests, &trie->results[ *id ].n);
				else
					ASSERT(0);
			}
		}
	}
	if (r->bitnr/8 == string->l)
		return;
	search_trie(trie, r->son[ nth_bit(string, trie->icase, suffix, r->bitnr) ],
		suffix, string);
}

#define	ASORT_PREFIX(x)	filter_trie_result_##x
#define	ASORT_KEY_TYPE	uns
#define	ASORT_ELT(i)	array[i]->number
#define	ASORT_SWAP(i,j)	do { struct filter_trie_result *tmp=array[j]; array[j]=array[i]; array[i]=tmp; } while(0)
#define	ASORT_EXTRA_ARGS	, struct filter_trie_result **array
#include "ucw/sorter/array-simple.h"

void
filter_trie_search(struct filter_trie_table *trie, byte *string, struct filter_cases *res)
/* Returns the test-case with the lowest number that is fulfilled.  */
{
	struct len_string str = { .s = string, .l = strlen(string) };
	search_trie(trie, trie->root[0], 0, &str);
	search_trie(trie, trie->root[1], 1, &str);

	struct filter_trie_result *results[trie->cmds];
	uns count = 0;
	void *s, *s1;
	CLIST_WALK(s, trie->passed_tests)
		results[ count++ ] = s;
	ASSERT(count <= trie->cmds);
	CLIST_WALK_DELSAFE(s, trie->incremented_tests, s1)
	{
		struct filter_trie_result *res = s;
		ASSERT(res->passed != res->init_passed);
		res->passed = res->init_passed;
		clist_remove(&res->n);
		clist_add_tail(&trie->tests, &res->n);
	}
	CLIST_WALK_DELSAFE(s, trie->passed_tests, s1)
	{
		struct filter_trie_result *res = s;
		if (res->passed != res->init_passed)
		{
			res->passed = res->init_passed;
			clist_remove(&res->n);
			clist_add_tail(&trie->tests, &res->n);
		}
	}
	/* Due to moves between the link-lists, the tests might not be ordered well.  */
	filter_trie_result_sort(count, results);

	for (uns i=0; i<count; i++)
	{
		struct filter_case *cas = trie->cmd[ results[i]->number ];
		byte *pattern = cas->cond->o.expr.r->o.s;
		if (results[i]->trivial_pattern
		|| trie->icase && str_match_pattern_nocase(pattern, string)
		|| !trie->icase && str_match_pattern(pattern, string))
			filter_cases_add(res, cas);
	}
}

static void
dump_trie(struct fastbuf *b, struct filter_trie_record *r, uns level, int value)
{
	if (!r)
		return;
	for (uns i=0; i<level; i++)
		bputs(b, ". ");
	if (value >= 0)
		bprintf(b, "%d: ", value);
	bprintf(b, "bitnr=%d ", r->bitnr);
	if (r->cmdids)
	{
		bprintf(b, "prefix=%s ==> ", r->prefix);
		for (int *id=r->cmdids; *id != -1; id++)
			bprintf(b, "%d ", *id);
	}
	bprintf(b, "\n");
	dump_trie(b, r->son[0], level+1, 0);
	dump_trie(b, r->son[1], level+1, 1);
}

void
filter_trie_dump(struct fastbuf *b, struct filter *f UNUSED, struct filter_trie_table *trie)
{
	bprintf(b, "Trie: icase=%d commands=%d\n", trie->icase, trie->cmds);
	for (uns i=0; i<trie->cmds; i++)
	{
		bprintf(b, "Command %d: %s, passes %d, %strivial\n", i,
			trie->cmd[i]->cond->o.expr.r->o.s,
			trie->results[i].init_passed,
			trie->results[i].trivial_pattern ? "" : "non");
		ASSERT(trie->results[i].number == i);
		ASSERT(trie->results[i].passed == trie->results[i].init_passed);
	}
	bprintf(b, "Prefix trie:\n");
	dump_trie(b, trie->root[0], 0, -1);
	bprintf(b, "Suffix trie:\n");
	dump_trie(b, trie->root[1], 0, -1);
}
