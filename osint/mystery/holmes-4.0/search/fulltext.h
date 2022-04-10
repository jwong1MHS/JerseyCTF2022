/*
 *	Sherlock Search Engine -- Fulltext Matching
 *
 *	(c) 2005--2006 Martin Mares <mj@ucw.cz>
 */

#ifndef _SEARCH_FULLTEXT_H
#define _SEARCH_FULLTEXT_H

struct ft_word {
  byte *orig;				/* Pointer to original text */
  ref_pos_t pos;			/* Position (as counted by lexmapper) */
  byte olen;				/* Length of original text */
  byte type;				/* WT_xxx or FT_TYPE_UNSET */
  u16 weight;				/* Match weight at this position (0=no match, 1=highlight only) */
};

#define FT_TYPE_UNSET 0xff

struct ft_best {
  uns pos;				/* Position with encoded meta type, POS_NOWHERE if this slot is empty */
  u16 weight;				/* Match weight */
  byte word_index;			/* Which word matched */
  byte rfu;
};

enum ft_type {
  FT_TEXT,
  FT_META,
  FT_URL
};

struct ft_results {
  /* Input */
  byte *text;				/* The text to be evaluated */
  byte *text_end;
  enum ft_type input_type;
  ref_pos_t pos;			/* Starting position or POS_NOWHERE (updated on return) */

  /* Output */
  struct ft_word *words;		/* All words found */
  uns num_words;
  uns word_mask;			/* Which words have matched */
  uns bool_mask;			/* Which boolean IDs have matched */
  struct ft_best bests[NUM_BESTS];	/* The best matches to dump */

  /* Internal */
  uns max_words;
  uns meta_subtype;
  uns best_break[HARD_MAX_WORDS];
};

void fulltext_init(void);
struct ref_context *ft_init_card(struct query *q, struct database *db, struct card_attr *attr);
void ft_match(struct ref_context *c, struct ft_results *res);

#endif
