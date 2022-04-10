/*
 *	Sherlock: Custom Parts of Configuration
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2004--2005 Robert Spalek <robert@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

/* Versions */

#define SHERLOCK_VERSION_SUFFIX ""	/* String appended to version number to identify custom versions */
#define CUSTOM_INDEX_TYPE 0x01		/* A byte identifying our flavor of index format */
#define CUSTOM_INDEX_VERSION 0x01	/* Remember to increase after each change of custom parts of the index */

/* Structures we'll need in function parameters */

struct card_attr;
struct odes;

/* Word types (at most 8 of them + WT_MAX) */

#define WT_TEXT			0	/* Ordinary text */
#define WT_EMPH			1	/* Emphasized text */
#define WT_SMALL		2	/* Small font */
#define WT_SMALL_HEADING	3	/* Heading */
#define WT_BIG_HEADING		4	/* Larger heading */
#define WT_ALT			5	/* Alternate texts for graphical elements */
#define WT_MAX			6

/* Descriptive names used for user output */
#define WORD_TYPE_USER_NAMES			\
   "text", "emph", "small", "hdr1", "hdr2", "alt", "word6", "word7"

/* Keywords for word type names */
#define WORD_TYPE_NAMES				\
	T(TEXT, 1 << WT_TEXT)			\
	T(EMPH, 1 << WT_EMPH)			\
	T(SMALL, 1 << WT_SMALL)			\
	T(HDR, (1 << WT_SMALL_HEADING) | (1 << WT_BIG_HEADING))  \
	T(HDR1, 1 << WT_SMALL_HEADING)		\
	T(HDR2, 1 << WT_BIG_HEADING)		\
	T(ALT, 1 << WT_ALT)			\

/*
 * These types are always matched with/without accents if accent mode is set to "auto",
 * regardless of accentedness of the current document.
 */
#define WORD_TYPES_AUTO_ACCENT_ALWAYS_STRICT 0
#define WORD_TYPES_AUTO_ACCENT_ALWAYS_STRIP 0

/* These types belong to all languages */
#define WORD_TYPES_ALL_LANGS 0

/* These types don't belong to any language, so neither language matching nor lemmatization affects them */
#define WORD_TYPES_NO_LANG 0

/* Word types have been decremented between card versions v1 and v2 */
#define WT_CONVERT_v1_v2(wt) ({ ASSERT(wt); wt-1; })

/* Meta information types (at most 16 of them + MT_MAX) */

#define MT_TITLE		0	/* Document title */
#define MT_KEYWORD		1	/* Keyword from the document */
#define MT_MISC			2	/* Unclassified metas */
#define MT_URL_KEYWD		3	/* Keywords extracted from URL */
#define MT_FILE			4	/* Part of file name */
#define MT_EXT			5	/* External texts (link texts) */
#define MT_MAX			6

#define META_TYPE_USER_NAMES			\
   "title", "keywd", "meta", "urlword", "file", "ext", "meta6", "meta7",	\
   "meta8", "meta9", "meta10", "meta11", "meta12", "meta13", "meta14", "meta15"

/* Keywords for meta type names */
#define META_TYPE_NAMES				\
	T(TITLE, 1 << MT_TITLE)			\
	T(KEYWD, 1 << MT_KEYWORD)		\
	T(META, 1 << MT_MISC)			\
	T(URLWORD, 1 << MT_URL_KEYWD)		\
	T(FILE, 1 << MT_FILE)			\
	T(EXT, 1 << MT_EXT)			\

#define META_TYPES_AUTO_ACCENT_ALWAYS_STRICT 0
#define META_TYPES_AUTO_ACCENT_ALWAYS_STRIP ((1 << MT_FILE) | (1 << MT_URL_KEYWD))
#define META_TYPES_ALL_LANGS (1 << MT_EXT)
#define META_TYPES_NO_LANG ((1 << MT_URL_KEYWD) | (1 << MT_FILE))

/* String types (at most 8 of them + ST_MAX) */

#define ST_REF			0	/* URL reference */
#define ST_URL			1	/* URL of the document */
#define ST_HOST			2	/* Host name */
#define ST_DOMAIN		3	/* Domain name */
#define ST_IP			4	/* IP address */
#define ST_MAX			5

#define STRING_TYPE_USER_NAMES							\
   "ref", "URL", "host", "domain", "ip", "type5", "type6", "type7"

#define STRING_TYPE_NAMES			\
	T(URL, 1 << ST_URL)			\
	T(HOST, 1 << ST_HOST)			\
	T(IP, 1 << ST_IP)			\
	T(DOMAINONLY, 1 << ST_DOMAIN)		\
	T(DOMAIN, (1 << ST_DOMAIN) | (1 << ST_HOST)) \
	T(REF, 1 << ST_REF)			\
	T(LINK, 1 << ST_REF)

#define STRING_TYPES_URL ((1 << ST_URL) | (1 << ST_REF))
/* These must be indexed in lowercase form */
#define STRING_TYPES_CASE_INSENSITIVE ((1 << ST_HOST) | (1 << ST_DOMAIN))

static inline void
custom_index_strings(struct odes *o UNUSED, void (*f)(char *text, uns type) UNUSED)
{
  /*
   * Call f for all strings you want to add to the index.
   * ST_URL, ST_HOST, ST_DOMAIN, ST_REF and ST_IP are indexed automatically
   * if they are defined.
   */
}

/*
 *  Definitions of custom attributes:
 *
 *  First of all, you need to define your own card_attr fields which will
 *  contain your attributes: CUSTOM_CARD_ATTRS lists them.
 *  Please order the attributes by decreasing size to get optimum padding.
 *
 *  Then define custom_create_attrs() which will get the object description
 *  and set your card_attr fields accordingly.
 *
 *  Finally, you have to define CUSTOM_ATTRS with matching rules:
 *
 *  INT_ATTR(id, keyword, get_func, parse_func) -- unsigned integer attribute
 *
 *  id		C identifier of the attribute
 *  keywd	search server keyword for the attribute
 *  int get_func(struct card_attr *ca)
 *		get attribute value from the card_attr
 *  char *parse_func(u32 *dest, char *value, uns intval)
 *		parse value in query (returns error message or NULL)
 *		for KEYWD = "string", it gets value="string", intval=0
 *		for KEYWD = num, it gets value=NULL, intval=num.
 *
 *  SMALL_SET_ATTR(id, keyword, get_func, parse_func)
 *    -- integers 0..31 with set matching
 *
 *  A good place for definitions of the functions is lib/custom.c.
 *
 *  You can also use LATE_INT_ATTR and LATE_SMALL_SET_ATTR instead
 *  which makes the attribute to be matched as late as possible, so that
 *  mismatched documents will still get caught by CUSTOM_EARLY_STATS,
 *  at the expense of slightly slower searching.
 */

/* No custom attributes defined yet */

#define CUSTOM_CARD_ATTRS
#define CUSTOM_ATTRS

/* The following function fills type_flags if CONFIG_FILETYPE and all custom attributes */

void custom_create_attrs(struct odes *odes, struct card_attr *ca);

/*
 *  Definitions of custom merging rules:
 *
 *	CUSTOM_MERGE(struct card_attr *merged, struct card_attr *dup)
 *				is called by the indexer whenever a card is considered duplicate
 *				and merged with another card. Here `merged' is the surviving card,
 *				`dup' the one which is going to be dropped. This is useful if you
 *				need to merge custom attributes in a special way (otherwise,
 *				the attributes of the `merged' card are used).
 *	CUSTOM_PROPAGATE_IMAGE_ATTRS(struct card_attr *referer, struct card_attr *img)
 *				is called whenever a link from a normal card `referer' to an image card
 *				`img', possibly after traversing a chain of redirects, is encountered.
 *				Again, this can be useful for propagating values of custom attributes.
 *	CUSTOM_MKLEX(struct odes *odes, void (*map)(char *start, *end))
 *				is called in the mklex and allows the customization to
 *				make its own additional lexicon mappings.
 *	CUSTOM_CHEW_PREPROC(struct odes *odes, struct card_attr *ca)
 *				is called in the chewer before all other manipulations with an object.
 *	CUSTOM_CHEW_ATTRS(struct odes *odes, struct card_attr *ca)
 *				is called in the chewer before `ca' is written.
 *				This is useful if you need to make updates to this structure
 *				in the second phase of indexation.
 */

/*
 *  Definition of custom statistics:
 *
 *  Here you can introduce your own statistics for the search server
 *  (e.g., you can count the number of documents matched for each value of a given custom attribute)
 *  by defining the following macros (optional):
 *
 *	CUSTOM_STAT_VARS	declarations of all statistics you want to collect. These will
 *				become a part of struct stats. (Queries are processed in multiple
 *				parts and each of them can have its own struct stats. For each
 *				database, these parts will be combined by calling CUSTOM_MERGE_STATS.)
 *	CUSTOM_INIT_STATS(struct query *q, struct stats *s)
 *				initialize statistics in a given struct stats
 *	CUSTOM_LATE_STATS(struct query *q, struct stats *s, struct card_attr *a)
 *				update variables when a document is matched
 *	CUSTOM_EARLY_STATS(q,s,a) update variables when a document matches everything except
 *				possibly for the LATE_xxx attributes.
 *	CUSTOM_MERGE_STATS(q,f,t) merge statistics f with t and store the result to t
 *	CUSTOM_SHOW_STATS(struct query *q, struct stats *s, void (*add)(char *fmt, ...))
 *				print the statistics -- `add' is a printf-like function
 *				which you should call for adding a line to the search server
 *				reply (it will appear in the per-database block).
 *
 *  The same system is used for per-filetype statistics, which are either late or early,
 *  depending on the CONFIG_COUNT_ALL_FILETYPES switch; see sherlock/index.h for how is this done.
 */

/*
 *  Definition of custom matching rules:
 *
 *  You also can define matchers which are even more general than custom attributes
 *  and which can influence not only acceptance of documents, but also their weight.
 *  This is controlled by the following macros (each of them optional):
 *
 *	CUSTOM_MATCH_VARS	declarations of per-query variables which will appear in struct query.
 *				These variables must not be modified by CUSTOM_MATCH.
 *	CUSTOM_MATCH_INIT(struct query *q)
 *				initialize there variables in a given struct query
 *	CUSTOM_MATCH_PARSE	a list of parsing rules for custom keywords; for each keyword, include
 *				CUSTOM_MATCH_KWD(id, keyword, char (*parse)(struct query *q, enum custom_op op, char *value, uns intval).
 *				Whenever KEYWORD <op> "value" is seen in the query, parse(q,op,value,0) is called
 *				and it's expected to return either NULL or an error message. For KEYWORD <op> number,
 *				parse(q,op,NULL,number) is called likewise.
 *	char *CUSTOM_MATCH_CACHE_KEY(struct query *q, struct mempool *mp)
 *				generate a unique textual representation of the custom attributes for
 *				use as a cache lookup key. The string should be allocated on the given
 *				memory pool or a static memory buffer. NULL result means an empty string.
 *				Keep in mind that the entire key is currently limited to 4KB,
 *				but this limitation can be removed if necessary.
 *	int CUSTOM_MATCH(struct query *q, struct ref_context *c, struct card_attr *ca, int Q)
 *				called to determine whether a single card (given by its attributes) matches.
 *				Return 1 if accepted or 0 otherwise and possibly set the Q bonus for the card (initially 0).
 *				For all keywords you define in CUSTOM_MATCH_PARSE, you can set c-><id>_value
 *				[SORTBY <keyword> will be recognized and will use this field as the sort key.]
 *				Matching should have no side-effects, use CUSTOM_xxx_STATS if you need side-effects.
 *	int CUSTOM_MATCH_ANY(struct query *q, struct ref_context *c, struct card_attr *ca, int Q)
 *				variant of CUSTOM_MATCH called in negative queries for cards with no matched words.
 *				If not defined, CUSTOM_MATCH is used instead.
 *	CUSTOM_MATCH_SHOW(struct query *q, struct card_attr *ca, void (*add)(char *fmt, ...))
 *				print extra attributes to card description -- like CUSTOM_SHOW_STATS.
 */

/*
 *  Per-thread storage (optional):
 *
 *	CUSTOM_REF_VARS		declare custom variables in struct ref_context (one per thread).
 *	CUSTOM_REF_INIT(struct ref_context *c)
 *				called sequentially for each ref_context after it has been initialized.
 *				Results can be safely used in custom matchers (possibly threaded).
 *	CUSTOM_REF_CLEANUP(struct ref_context *c)
 *				called sequentially, can be used to cleanup internal structures
 *				initialized by CUSTOM_REF_INIT
 */

/*
 *  Custom initialization (optional):
 *
 *	CUSTOM_INIT()		run after sherlockd initializes itself and before it calls any other CUSTOM_xxx macros.
 *	CUSTOM_FETCH(struct odes *o)
 *				called in the indexer for each fetched object and allows
 *				the user to modify some of its attributes.
 */

/*
 *  Custom analysers (optional):
 *
 *	CUSTOM_ANALYSERS	define new custom analysers, for example "AN_MODULE(an_custom1) AN_MODULE(an_custom2)".
 *				Each analyser must export its interface in "struct analyser" as defined
 *				in analyser/analyser.h.
 */
