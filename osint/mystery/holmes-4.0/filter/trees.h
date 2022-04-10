/*
 *	Sherlock Filter Engine --- tree data structure
 *
 * 	This is not an ordinary header file, but an internal included source
 * 	file.  Define the following macros:
 *
 * 	NAME
 * 	NAME_
 * 	STRUCT_DECL
 * 	PTR_DECL
 * 	STRUCT_ATTR
 * 	FORMAT
 * 	FULL_INTERVAL
 * 	TREE_KEY_{ENDSTRING,ATOMIC}, TREE_NOCASE, TREE_ATOMIC_TYPE
 * 	FILTER_TYPE
 * 	RAW_ATTR
 * 	ICASE_UNSIGN_FLAG
 * 	DUMP_TITLE
 *
 *	(c) 2004--2005, Robert Spalek <robert@ucw.cz>
 */

struct NAME_(list) {
	struct filter_case *cas;
	struct NAME_(list) *next;
};

struct NAME_(node) {
	struct NAME_(list) *eq;
	struct NAME_(list) *sub;
	STRUCT_DECL;
};

static void
NAME_(dump_key)(struct fastbuf *fb, struct NAME_(node) *n)
{
	bprintf(fb, "key=%" FORMAT " ", n->STRUCT_ATTR);
}

static void
NAME_(dump_data)(struct fastbuf *fb UNUSED, struct NAME_(node) *n)
{
	for (struct NAME_(list) *l = n->eq; l; l = l->next)
		bprintf(fb, " eq=%p", l->cas);
	for (struct NAME_(list) *l = n->sub; l; l = l->next)
		bprintf(fb, " sub=%p", l->cas);
}

static inline void
NAME_(init_data)(struct NAME_(node) *n)
{
	n->eq = n->sub = NULL;
}

#define	TREE_NODE	struct NAME_(node)
#define	TREE_PREFIX(x)	NAME_(x)
#define TREE_GIVE_INIT_DATA
#define	TREE_WANT_LOOKUP
#define	TREE_WANT_DUMP
#include "ucw/redblack.h"

static PTR_DECL NAME_(full_int)[2] = { FULL_INTERVAL };

struct NAME_(tree1) {
	struct NAME_(tree) tree;
	struct filter_case *cases;
};

static inline void
NAME_(list_add)(struct mempool *mp, struct NAME_(list) **list, struct filter_case *cas)
{
	struct NAME_(list) *l = mp_alloc(mp, sizeof(*l));
	l->cas = cas;
	l->next = *list;
	*list = l;
}

static inline void
NAME_(list_add_buck)(struct mempool *mp, NAME_(bucket) *buck, uns son, struct filter_case *cas)
{
	NAME_(list_add)(mp, &buck->n.eq, cas);
	if (buck->son[son])
		NAME_(list_add)(mp, &buck->son[son]->n.sub, cas);
}

struct NAME_(tree) *
NAME_(tree_new)(struct mempool *mp, struct filter_cmd *case_cmd)
{
	ASSERT(case_cmd->op == SWITCH);
	uns count = 0;
	for (struct filter_case *curr = case_cmd->c.swit.cases; curr; curr=curr->next)
		if (can_be_in_tree(curr, FILTER_TYPE, ICASE_UNSIGN_FLAG, NULL))
			count++;
	if (count < filter_tree_limit)
		return NULL;

	union filter_raw_value2 str[2];
	struct NAME_(tree) *tree = mp_alloc(mp, sizeof(struct NAME_(tree1)));
	struct NAME_(tree1) *tree1 = (void *)tree;
	tree1->cases = NULL;
	NAME_(init)(tree);
	for (struct filter_case *curr = case_cmd->c.swit.cases; curr; curr=curr->next)
	{
		str[0].RAW_ATTR = NAME_(full_int)[0];
		str[1].RAW_ATTR = NAME_(full_int)[1];
		if (can_be_in_tree(curr, FILTER_TYPE, ICASE_UNSIGN_FLAG, str))
		{
			if ((PTR_DECL) str[0].RAW_ATTR != NAME_(full_int)[0])
				NAME_(lookup)(tree, str[0].RAW_ATTR);
			if ((PTR_DECL) str[1].RAW_ATTR != NAME_(full_int)[1])
				NAME_(lookup)(tree, str[1].RAW_ATTR);
		}
	}
	struct filter_case **cs = &case_cmd->c.swit.cases, *cas;
	while (cas = *cs)
	{
		str[0].RAW_ATTR = NAME_(full_int)[0];
		str[1].RAW_ATTR = NAME_(full_int)[1];
		if (!can_be_in_tree(cas, FILTER_TYPE, ICASE_UNSIGN_FLAG, str))
		{
			cs = &(*cs)->next;
			continue;
		}
		*cs = (*cs)->next;
		cas->next = tree1->cases;
		tree1->cases = cas;
		if ((PTR_DECL) str[0].RAW_ATTR == NAME_(full_int)[0] && (PTR_DECL) str[1].RAW_ATTR == NAME_(full_int)[1])
			NAME_(list_add)(mp, &tree->root->n.sub, cas);
		else
		{
			NAME_(stack_entry) lstack[64], rstack[64];
			uns ldepth = ~0U, rdepth = ~0U;
			if ((PTR_DECL) str[0].RAW_ATTR != NAME_(full_int)[0])
			{
				ldepth = NAME_(fill_stack)(lstack, ARRAY_SIZE(lstack), tree->root, str[0].RAW_ATTR, 0);
				ASSERT(lstack[ldepth].buck);
			}
			if ((PTR_DECL) str[1].RAW_ATTR != NAME_(full_int)[1])
			{
				rdepth = NAME_(fill_stack)(rstack, ARRAY_SIZE(rstack), tree->root, str[1].RAW_ATTR, 1);
				ASSERT(rstack[rdepth].buck);
			}
			if (!~ldepth)
			{
				for (uns i = 0; i <= rdepth; i++)
					if (i == rdepth || rstack[i].son)
						NAME_(list_add_buck)(mp, rstack[i].buck, 0, cas);
			}
			else if (!~rdepth)
			{
				for (uns i = 0; i <= ldepth; i++)
					if (i == ldepth || !lstack[i].son)
						NAME_(list_add_buck)(mp, lstack[i].buck, 1, cas);
			}
			else
			{
				uns depth = 0;
				while (depth < ldepth && depth < rdepth && lstack[depth].son == rstack[depth].son)
					depth++;
				NAME_(list_add)(mp, &lstack[depth].buck->n.eq, cas);
				for (uns i = depth + 1; i <= ldepth; i++)
					if (lstack[i].buck && (i == ldepth || !lstack[i].son))
						NAME_(list_add_buck)(mp, lstack[i].buck, 1, cas);
				for (uns i = depth + 1; i <= rdepth; i++)
					if (rstack[i].buck && (i == rdepth || rstack[i].son))
						NAME_(list_add_buck)(mp, rstack[i].buck, 0, cas);
			}
		}
	}
	return tree;
}

static inline void
NAME_(match_list)(struct NAME_(list) *list, struct filter_cases *res)
{
	for (; list; list = list->next)
		filter_cases_add(res, list->cas);
}

void
NAME_(tree_search)(struct NAME_(tree) *tree, PTR_DECL val, struct filter_cases *res)
{
	NAME_(stack_entry) stack[64];
	uns depth = NAME_(fill_stack)(stack, ARRAY_SIZE(stack), tree->root, val, 0);
	if (stack[depth].buck)
	{
		NAME_(match_list)(stack[depth].buck->n.eq, res);
		depth++;
	}
	while (depth--)
		NAME_(match_list)(stack[depth].buck->n.sub, res);
}

void
NAME_(tree_dump)(struct fastbuf *fb, struct filter *f UNUSED, struct NAME_(tree) *tree, uns level)
{
	if (!tree)
		return;
	filter_dump_spaces(fb, level);
	bprintf(fb, "# Binary search tree for " DUMP_TITLE "\n");
	for (struct filter_case *cas = ((struct NAME_(tree1) *)tree)->cases; cas; cas = cas->next)
	{
		filter_dump_spaces(fb, level);
		bputs(fb, "case ");
		filter_dump_condition(fb, f, cas->cond);
		bputs(fb, ":\n");
		filter_dump_commands(fb, f, cas->positive, level+1);
	}
	if (filter_trace > 0)
	{
		bputs(fb, "/*\n");
		NAME_(dump)(fb, tree);
		bputs(fb, "*/\n");
	}
}

#undef	NAME
#undef	NAME_
#undef	STRUCT_DECL
#undef	PTR_DECL
#undef	STRUCT_ATTR
#undef	FORMAT
#undef	FULL_INTERVAL
#undef	FILTER_TYPE
#undef	RAW_ATTR
#undef	ICASE_UNSIGN_FLAG
#undef	DUMP_TITLE
