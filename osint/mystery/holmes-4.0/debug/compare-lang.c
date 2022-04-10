/*
 *	A simple utility that compares languages in two indices
 *
 *	(c) 2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "ucw/fastbuf.h"
#include "ucw/unaligned.h"
#include "ucw/bbuf.h"
#include "ucw/getopt.h"
#include "lang/lang.h"
#include "indexer/indexer.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

struct fp_node {
	struct fingerprint fp;
	uns id;
};

static inline int
fp_nodes_compare(struct fp_node *a, struct fp_node *b)
{
	return memcmp(&a->fp, &b->fp, sizeof(a->fp));
}

static inline uns
fp_nodes_hash(struct fp_node *a)
{
	return get_u32_be(&a->fp);
}

static void
fp_nodes_write_merged(struct fastbuf *f, struct fp_node **keys, void **data UNUSED, uns n UNUSED, void *buf UNUSED)
{
	bwrite(f, *keys, sizeof(**keys));
}

#define SORT_PREFIX(x) fp_nodes_##x
#define SORT_KEY_REGULAR struct fp_node
#define SORT_HASH_BITS 32
#define SORT_UNIFY
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#include "ucw/sorter/sorter.h"

static struct fastbuf *
resolve_urls(struct fastbuf *urls)
{
	struct fastbuf *fps = index_bopen_tmp(1);
	struct fp_node node;
	bzero(&node, sizeof(node));
	bb_t bb;
	bb_init(&bb);
	while (bgets_bb(urls, &bb, ~0U)) {
		url_fingerprint(bb.ptr, &node.fp);
		bwrite(fps, &node, sizeof(node));
		node.id++;
	}
	bb_done(&bb);
	msg(L_INFO, "Resolved %u URLs", node.id);
	brewind(fps);
	brewind(urls);
	return fp_nodes_sort(fps, NULL);
}

struct card_node {
	u32 id;
	u32 cardid;
	byte lang;
};

static inline int
cards_by_cardid_compare(struct card_node *a, struct card_node *b)
{
	COMPARE(a->cardid, b->cardid);
	return 0;
}

#define SORT_PREFIX(x) cards_by_cardid_##x
#define SORT_KEY_REGULAR struct card_node
#define SORT_UNIQUE
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#include "ucw/sorter/sorter.h"

static inline int
cards_by_id_compare(struct card_node *a, struct card_node *b)
{
	COMPARE(a->id, b->id);
	return 0;
}

#define SORT_PREFIX(x) cards_by_id_##x
#define SORT_KEY_REGULAR struct card_node
#define SORT_UNIQUE
#define SORT_INPUT_FB
#define SORT_OUTPUT_FB
#include "ucw/sorter/sorter.h"

static struct fastbuf *
extract_cards(struct fastbuf *fp_nodes, char *index)
{
	msg(L_INFO, "Extracting cards from directory %s", index);
	
	fn_directory = index;
	struct fp_node fp_node;
	struct card_node card_node;
	struct card_print card_print;
	bzero(&card_node, sizeof(card_node));
	struct fastbuf *fingerprints = index_bopen(fn_fingerprints, O_RDONLY, 1);
	struct fastbuf *card_nodes = index_bopen_tmp(1);
	if (breadb(fp_nodes, &fp_node, sizeof(fp_node)))
		while (breadb(fingerprints, &card_print, sizeof(card_print))) {
      			int c = memcmp(&fp_node.fp, &card_print.fp, sizeof(fp_node.fp));
      			if (!c && card_print.cardid < 0x80000000) {
      				card_node.id = fp_node.id;
      				card_node.cardid = card_print.cardid;
				bwrite(card_nodes, &card_node, sizeof(card_node));
      			}
      			if (c <= 0 && !breadb(fp_nodes, &fp_node, sizeof(fp_node)))
      				break;
      		}
	msg(L_INFO, "Matched %d cards", (uns)(btell(card_nodes) / sizeof(card_node)));
	bclose(fingerprints);
	brewind(card_nodes);
	brewind(fp_nodes);
	
	card_nodes = cards_by_cardid_sort(card_nodes, NULL);
	struct fastbuf *in = card_nodes;
	card_nodes = index_bopen_tmp(1);
	struct fastbuf *attrs = index_bopen(fn_attributes, O_RDONLY, 1);
	struct card_attr card_attr;
	uns cardid = 0;
	while (breadb(in, &card_node, sizeof(card_node))) {
		while (cardid++ < card_node.cardid)
			bskip(attrs, sizeof(card_attr));
		breadb(attrs, &card_attr, sizeof(card_attr));
		card_node.lang = CA_GET_FILE_LANG(&card_attr); 
		bwrite(card_nodes, &card_node, sizeof(card_node));
	}
	bclose(attrs);
	bclose(in);
	brewind(card_nodes);
		
	return cards_by_id_sort(card_nodes, NULL);
}

static void
compare_indices(struct fastbuf *urls, struct fastbuf *fb1, struct fastbuf *fb2)
{
	msg(L_INFO, "Printing statistics");
	struct fastbuf *out = bopen_file("tmp/compare-lang-list", O_WRONLY | O_CREAT | O_TRUNC, &indexer_stream_params);
	struct card_node n1, n2;
	bb_t bb;
	bb_init(&bb);
	if (!breadb(fb1, &n1, sizeof(n1)))
		n1.id = ~0U;
	if (!breadb(fb2, &n2, sizeof(n2)))
		n2.id = ~0U;
	uns table[LANG_NONE + 2][LANG_NONE + 2];
	uns cnt1[LANG_NONE + 2], cnt2[LANG_NONE + 2];
	bzero(table, sizeof(table));
	uns id = 0;
	while (bgets_bb(urls, &bb, ~0U)) {
		uns lang1 = LANG_NONE + 1;
		uns lang2 = LANG_NONE + 1;
		if (n1.id == id)
			bprintf(out, "%-2s ", lang_code_to_name(lang1 = n1.lang));
		else
			bputs(out, "-- ");
		if (n2.id == id)
			bprintf(out, "%-2s ", lang_code_to_name(lang2 = n2.lang));
		else
			bputs(out, "-- ");
		if (n1.id == id)
			bprintf(out, "%08x ", n1.cardid);
		else
			bputs(out, "-------- ");
		if (n2.id == id)
			bprintf(out, "%08x ", n2.cardid);
		else
			bputs(out, "-------- ");
		bputsn(out, bb.ptr);
		if (n1.id == id && !breadb(fb1, &n1, sizeof(n1)))
			n1.id = ~0U;
		if (n2.id == id && !breadb(fb2, &n2, sizeof(n2)))
			n2.id = ~0U;
		lang1 = MIN(lang1, LANG_NONE + 1);
		lang2 = MIN(lang2, LANG_NONE + 1);
		table[lang1][lang2]++;
		cnt1[lang1]++;
		cnt2[lang2]++;
		id++;
	}
	bb_done(&bb);
	bclose(out);
	bclose(fb1);
	bclose(fb2);
	bclose(urls);
	if (id) {
		printf("  ");
		for (uns lang2 = 0; lang2 <= LANG_NONE + 1; lang2++)
			if (cnt2[lang2])
				printf(" %8s", (lang2 <= LANG_NONE) ? (char *)lang_code_to_name(lang2) : "--");
		printf("\n");
		for (uns lang1 = 0; lang1 <= LANG_NONE + 1; lang1++)
			if (cnt1[lang1]) {
				printf("%-2s", (lang1 <= LANG_NONE) ? (char *)lang_code_to_name(lang1) : "--");
				for (uns lang2 = 0; lang2 <= LANG_NONE + 1; lang2++)
					if (cnt2[lang2])
						printf(" %8u", table[lang1][lang2]);
				printf("\n");
			}
	}
}

int
main(int argc, char **argv)
{
	log_init(argv[0]);
	if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 || optind + 3 != argc) {
		fputs("Usage: compare-lang <index1> <index2> <urls>\n", stderr);
		exit(1);
	}

	char *index1 = argv[optind++];
	char *index2 = argv[optind++];
	char *fn_urls = argv[optind++];

	url_key_init();
	
	struct fastbuf *urls = bopen_file(fn_urls, O_RDONLY, &indexer_stream_params);
	struct fastbuf *resolved = resolve_urls(urls);
	struct fastbuf *cards1 = extract_cards(resolved, index1);
	struct fastbuf *cards2 = extract_cards(resolved, index2);
	bclose(resolved);
	compare_indices(urls, cards1, cards2);

	return 0;
}
