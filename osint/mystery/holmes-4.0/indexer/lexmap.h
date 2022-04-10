/*
 *	Sherlock Indexer -- Lexical Mapping
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2005 Robert Spalek <robert@ucw.cz>
 *	(c) 2005 Tomas Valla <tom@ucw.cz>
 *
 *	Feature selection switches:
 *	   - LM_SIMPLE if the input is a string without Sherlock tags and ligatures
 *	   - LM_SEARCH_WILDCARDS to enable a special mode for sherlockd wildcards
 *	   - LM_MAP_URL to include functions for mapping of URL's as well
 *	   - LM_TRACK_TEXT if you want lexmap to keep track of position of
 *	     the words in the original text. Expects the text to contain
 *	     no URL brackets.
 *	   - LM_MULTI if you want to run multiple instances of the lexmapper
 *	     in parallel. In this case, you should call lmap_* instead of lm_*
 *	     and always pass a pointer to struct lm_state as the first argument.
 *
 *	You should supply:
 *	   - word_id_t: a type representing a single word
 *	     If word_id_t is a compound type, you have to define WORD_ID_NULL
 *	     and WORD_ID_DEFINED_P(x).
 *	and optionally:
 *	   - LM_SCAN_CHAR(u) to read one character from input to u, return 0 on eof, 1 otherwise
 *	   - LM_MAP_ARGS as the arguments of lm_map_text() which LM_SCAN_CHAR will see and could use
 *
 *	You call:
 *	   - lm_init() before doing anything else
 *	   - lm_doc_start() at the start of each document
 *	   - lm_map_text() to process data returned by LM_SCAN_CHAR
 *	   - lm_map_break() if you want to insert an explicit sentence break
 *
 *	We call:
 *	   - lm_lookup() to look up and categorize a word
 *	   - lm_got_word() for each word found together with its position
 *	   - lm_got_complex() for each word complex
 *
 *	We guarantee that lm_lookup() is called only on words no longer than
 *	MAX_WORD_CHARS Unicode characters.
 */

#include "ucw/chartype.h"
#include "ucw/unicode.h"
#include "charset/unicat.h"
#include "indexer/alphabet.h"

#include <string.h>

/* Internal state and various macros */

typedef struct lm_state {
  uns current_cat;
  uns pos;
  uns garb_cnt;
  word_id_t context_base, last_word;
  uns context_cat;
  void *user_data;
} lm_state;

#ifndef LM_SCAN_CHAR
#ifdef LM_SIMPLE
#define LM_SCAN_CHAR(u) (text = utf8_get(text, &u), u)
#define LM_MAP_ARGS const byte *text
#else
#define LM_SCAN_CHAR(u) ((text < stop) ? (text = get_tagged_char(text, &u), 1) : 0)
#define LM_MAP_ARGS const byte *text, const byte *stop
#endif
#endif

#ifndef LM_MAP_ARGS
#define LM_MAP_ARGS void
#endif

#ifdef LM_TRACK_TEXT
#define TRACK_TEXT(e) e
#else
#define TRACK_TEXT(e) do { } while(0)
#endif

#ifndef WORD_ID_NULL
#define WORD_ID_NULL (word_id_t)0
#define WORD_ID_DEFINED_P(x) (x)
#endif

static inline void
lmap_doc_start(lm_state *lm, void *user_data)
{
  lm->garb_cnt = 0;
  lm->last_word = lm->context_base = WORD_ID_NULL;
  lm->current_cat = WT_TEXT;
  lm->pos = 0;
  lm->user_data = user_data;
}

static inline void
lmap_set_pos(lm_state *lm, uns pos)
{
  /* Beware that this does not restore other state varibles.  You should only
   * call this procedure after lm_doc_start().  */
  lm->pos = pos;
}

static inline uns
lmap_get_pos(lm_state *lm)
{
  return lm->pos;
}

static void
lmap_map_word(lm_state *lm, u16 *uni, uns ulen, enum word_class class, const byte *ostart UNUSED, const uns olen UNUSED)
{
  word_id_t thisw = WORD_ID_NULL;

#ifdef LM_TRACK_TEXT
  class = lm_lookup(class, uni, ulen, &thisw, ostart, olen, lm->user_data);
#else
  class = lm_lookup(class, uni, ulen, &thisw, lm->user_data);
#endif
#ifdef LOCAL_DEBUG
  {
    byte w[MAX_WORD_BYTES+1], *p=w;
    uns i;
    for (i=0; i<ulen; i++)
      p = utf8_put(p, uni[i]);
    *p = 0;
    DBG("step: @%d <%s> class=%d cat=%d", lm->pos, w, class, lm->current_cat);
  }
#endif
  /*
   *  This complex automaton parses sequences of word classes, recognizes
   *  word complexes (words with context) and calculates word positions.
   */
  switch (class)
    {
    case WC_IGNORED:
    case WC_COMPLEX:
      break;
    case WC_NORMAL:
      if (lm->garb_cnt)
	{
	  if (lm->pos)
	    lm->pos += MIN(lm->garb_cnt, lex_max_gap);
	  lm->garb_cnt = 0;
	}
      else if (WORD_ID_DEFINED_P(lm->context_base))
	lm_got_complex(lm->pos-1, lm->context_cat, lm->context_base, thisw, 1, lm->user_data);
      lm->context_base = WORD_ID_NULL;
      lm_got_word(lm->pos, lm->current_cat, thisw, lm->user_data);
      lm->last_word = thisw;
      lm->pos++;
      break;
    case WC_BREAK:
      lm->garb_cnt += lex_max_gap;
      /* Fall thru */
    case WC_GARBAGE:
      lm->garb_cnt++;
      lm->context_base = lm->last_word = WORD_ID_NULL;
      break;
    case WC_CONTEXT:
      if (lm->garb_cnt)
	{
	  /* because of "... gap gap gap complex [complex] word" */
	  if (lm->pos)
	    lm->pos += MIN(lm->garb_cnt, lex_max_gap);
	  lm->garb_cnt = 0;
	}
      else
        {
	  if (WORD_ID_DEFINED_P(lm->context_base))
	    lm_got_complex(lm->pos-1, lm->current_cat, lm->context_base, thisw, 1, lm->user_data);
	  if (WORD_ID_DEFINED_P(lm->last_word))
	    lm_got_complex(lm->pos, lm->current_cat, thisw, lm->last_word, 0, lm->user_data);
	}
      lm->context_base = lm->last_word = thisw;
      lm->context_cat = lm->current_cat;
      lm->pos++;
      break;
    default:
      ASSERT(0);
    }
}

static inline void
lmap_map_break(lm_state *lm)
{
  lmap_map_word(lm, NULL, 0, WC_BREAK, NULL, 0);
}

#define	ANALYSED_CHARS 0x91
enum char_category { cc_lower, cc_upper, cc_digit, cc_base, cc_ctrl, cc_ox90, cc_ok, cc_end };
static byte char_analysis[ANALYSED_CHARS];

static void
lm_init(void)
{
  alphabet_init();
  for (uns c=0; c<ANALYSED_CHARS; c++)
    {
      uns i;
      if (c<=0x20 || c>=0x80 && c!=0x90)
	i = cc_ok;
      else if (Clower(c))
	i = cc_lower;
      else if (Cupper(c))
	i = cc_upper;
      else if (Cdigit(c))
	i = cc_digit;
      else if (c == 0x90)
	i = cc_ox90;
      else if (c == '+' || c == '/' || c == '=')
	i = cc_base;
      else
	i = cc_ctrl;
      char_analysis[c] = i;
    }
}

static ALWAYS_INLINE int
lmap_sequence_valid(u16 *uni, uns len)
{
  byte cnts[cc_end];
  uns c, act_cat, i;
  uns last_cat = cc_ok;
  uns cat_changes = 0;
  byte *msg;

  /* Check for non-ASCII characters, if present, the string is surely NOT some
   * encoded string (base64, uuencode, base85, base32, xxencode, binhex, btoa).
   * */
  bzero(&cnts, sizeof(cnts));
  if (len >= lex_max_ctrl_len)
    for (i=0; c = uni[i]; i++)
      {
	c = uni[i];
	if (c >= ANALYSED_CHARS)
	  return 1;
	act_cat = char_analysis[c];
	cnts[act_cat] = 1;
	if (act_cat != last_cat)
	  cat_changes++;
	last_cat = act_cat;
      }

  if (cnts[cc_upper] + cnts[cc_lower] + cnts[cc_digit] >= 3 && !cnts[cc_ctrl] && !cnts[cc_ox90]
      && cat_changes >= len/4)			/* base64, base is common */
    msg = "base64";
  else if (cnts[cc_upper] + cnts[cc_digit] + (cnts[cc_base]|cnts[cc_ctrl]) >= 3 && !cnts[cc_lower] && !cnts[cc_ox90]
	   && cat_changes >= len/3)			/* uuencode */
    msg = "uuencode";
  else if (cnts[cc_upper] + cnts[cc_lower] + cnts[cc_digit] + (cnts[cc_base]|cnts[cc_ctrl]) + cnts[cc_ox90] >= 4
	   && cat_changes >= len/2)			/* binhex, btoa, base85 */
    msg = "base85";
  else
    return 1;

  DBG("Throwing out word of length %d with %d category changes containing low/upp/dig/bas/ctr/x90=%d/%d/%d/%d/%d/%d, judged as %s",
      len, cat_changes, cnts[cc_lower], cnts[cc_upper], cnts[cc_digit], cnts[cc_base], cnts[cc_ctrl], cnts[cc_ox90], msg);
  return 0;
}

static void
lmap_map_sequence(lm_state *lm, u16 *uni, uns len, const byte **cpos UNUSED)
{
  uns start, u, flags, i, wlen;
  enum word_class class;

  if (!len)
     return;

#ifndef LM_SIMPLE
  uni[len] = 0;
  if (!lmap_sequence_valid(uni, len))
    {
      lmap_map_word(lm, NULL, 0, WC_GARBAGE, NULL, 0);
      return;
    }
#endif

  start = flags = 0;
  uni[len] = ' ';
  uni[len+1] = 0;
  for (i=0; u = uni[i]; i++)
    {
      uns cat = alpha_class[u];
#ifdef LM_SEARCH_WILDCARDS
      if ((u == '*' && wildcard_asterisks) || (u == '?' && wildcard_qmarks))
	cat = AC_ALPHA;
#endif
      switch ((enum alphabet_class) cat)
	{
	case AC_ALPHA:
	case AC_DIGIT:
	  flags |= 1 << cat;
	  break;
	case AC_SINGLETON:
	default:
	  wlen = i - start;
	  if (wlen)
	    {
	      uns max = lex_max_len;
	      if (flags == ((1 << AC_ALPHA) | (1 << AC_DIGIT)))
		max = lex_max_mixed_len;
	      else if (flags == (1 << AC_DIGIT))
		max = lex_max_num_len;
	      if (wlen > max)
		class = WC_GARBAGE;
	      else
		class = WC_NORMAL;
#ifdef LM_TRACK_TEXT
	      lmap_map_word(lm, uni+start, i-start, class, cpos[start], cpos[i]-cpos[start]);
#else
	      lmap_map_word(lm, uni+start, i-start, class, NULL, 0);
#endif
	    }
	  if (cat == AC_SINGLETON)
#ifdef LM_TRACK_TEXT
	    lmap_map_word(lm, uni+i, 1, WC_NORMAL, cpos[i], cpos[i+1]-cpos[i]);
#else
	    lmap_map_word(lm, uni+i, 1, WC_NORMAL, NULL, 0);
#endif
	  start = i+1;
	  flags = 0;
	}
    }
  if (alpha_class[uni[len-1]] == AC_BREAK)
    lmap_map_break(lm);
}

#ifdef LM_SIMPLE

static int
lmap_map_text(lm_state *lm, LM_MAP_ARGS)
{
  u16 uni[MAX_WORD_CHARS+1];
  uns u, l;

  l = 0;
  while (LM_SCAN_CHAR(u))
    {
      if (alpha_class[u] == AC_SPACE)
	{
	  lmap_map_sequence(lm, uni, l, NULL);
	  l = 0;
	}
      else if (l < MAX_WORD_CHARS)
	uni[l++] = u;
      else
	return 0;
    }
  lmap_map_sequence(lm, uni, l, NULL);
  return 1;
}

#else

static void
lmap_map_text(lm_state *lm, LM_MAP_ARGS)
{
  u16 uni[MAX_WORD_CHARS+1];
  const byte *cpos[MAX_WORD_CHARS+1];
  const u16 *lig;
  uns u, l, r;

  l = 0;
  for (;;)
    {
      TRACK_TEXT(cpos[l] = text);
      if (!LM_SCAN_CHAR(u))
	break;
    restart:
      if (u < 0x80000000)
	{
	  switch ((enum alphabet_class) alpha_class[u])
	    {
	    case AC_SPACE:
	      lmap_map_sequence(lm, uni, l, cpos);
	      l = 0;
	      break;
	    case AC_ALPHA:
	    case AC_DIGIT:
	    case AC_PUNCT:
	    case AC_BREAK:
	    case AC_SINGLETON:
	      if (l < MAX_WORD_CHARS)
		uni[l++] = u;
	      else
		{
		over:
		  lmap_map_word(lm, NULL, 0, WC_GARBAGE, NULL, 0);
		  l = 0;
		  r = 1;
		  while ( ((u < 0x80000000 && alpha_class[u] != AC_SPACE && alpha_class[u] != AC_SINGLETON) || (u >= 0x80010000)) && r )
		    {
		      TRACK_TEXT(cpos[0] = text);
		      r = LM_SCAN_CHAR(u);
		    }
		  if (r)
		    goto restart;
		  else
		    return;
		}
	      break;
	    case AC_LIGATURE:
	      lig = Uexpand_lig(u);
	      TRACK_TEXT(const byte *lig_start = cpos[l]);
	      while (*lig)
		{
		  if (l >= MAX_WORD_CHARS)
		    goto over;
		  TRACK_TEXT(cpos[l] = lig_start);
		  uni[l++] = *lig++;
		}
	      break;
	    default: ASSERT(0);
	    }
	}
      else if (u < 0x80010000)		/* Word type tag, breaks words */
	{
	  lmap_map_sequence(lm, uni, l, cpos);
	  l = 0;
	  lm->current_cat = u & 0x0f;
	  if (u & 0x10)
	    lmap_map_break(lm);
	}
      /* else it's a bracket which we ignore */
    }
  TRACK_TEXT(cpos[l] = text);
  lmap_map_sequence(lm, uni, l, cpos);
}

#endif

#ifdef LM_MAP_URL

static int
lmap_map_url(lm_state *lm, LM_MAP_ARGS)
{
  u16 uni[MAX_WORD_CHARS+1];
  const byte *cpos[MAX_WORD_CHARS+1];
  uns u, l;

  l = 0;
  while (1)
    {
      TRACK_TEXT(cpos[l] = text);
      if (!LM_SCAN_CHAR(u))
	break;
      uns c = alpha_class[u];
      if (c == AC_SPACE || u=='/' || u=='?' || u=='&' || u=='.')
	{
	  lmap_map_sequence(lm, uni, l, cpos);
	  l = 0;
	}
      else if (l < MAX_WORD_CHARS)
	uni[l++] = u;
      else
	return 0;
    }
  TRACK_TEXT(cpos[l] = text);
  lmap_map_sequence(lm, uni, l, cpos);
  return 1;
}

#endif

#ifndef LM_MULTI
static struct lm_state lm_default_state;
#define lm_doc_start(x) lmap_doc_start(&lm_default_state, x)
#define lm_get_pos() lmap_get_pos(&lm_default_state)
#define lm_set_pos(p) lmap_set_pos(&lm_default_state, p)
#define lm_map_text(x...) lmap_map_text(&lm_default_state, x)
#define lm_map_url(x...) lmap_map_url(&lm_default_state, x)
#endif
