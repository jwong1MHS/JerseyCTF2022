/*
 *	Sherlock Indexer -- Digestive Processes
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2003--2006 Robert Spalek <robert@ucw.cz>
 *	(c) 2006--2007 Pavel Charvat <pchar@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/clists.h"
#include "ucw/simple-lists.h"
#include "ucw/getopt.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/mempool.h"
#include "ucw/url.h"
#include "ucw/unicode.h"
#include "ucw/hashfunc.h"
#include "ucw/chartype.h"
#include "sherlock/object.h"
#include "sherlock/attrset.h"
#include "sherlock/conf.h"
#include "sherlock/tagged-text.h"
#include "sherlock/lizard-fb.h"
#include "charset/unicat.h"
#include "analyser/analyser.h"
#include "indexer/indexer.h"
#include "indexer/lexicon.h"
#include "indexer/params.h"

#ifdef CONFIG_IMAGES_SIM
#include "images/images.h"
#include "images/object.h"
#include "images/signature.h"
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <alloca.h>

/*
 *  Configuration
 */

static uns word_buf_size = 65536;
static uns string_buf_size = 65536;
static uns string_cats = ~0U;
static uns string_max = 4096;
static uns meta_limit = HARD_META_LIMIT;
static uns word_limit = HARD_WORD_LIMIT;
static uns excerpt_max = ~0U;
static uns doc_buf_size = 65536;
static uns giant_ban_meta, giant_penalty;
static uns type_weights[8];
static uns swindler_threshold = ~0U;
static uns no_contents_penalty, no_contents_threshold;
static struct clist hypertext_types;
static uns no_links_penalty;
static uns no_title_penalty;
static uns max_title_len = ~0U;
static uns min_compression;
static uns max_urls = ~0U;
static uns max_redirects = ~0U;

static byte *
chewer_commit(void *ptr UNUSED)
{
  if (meta_limit > HARD_META_LIMIT)
    return cf_printf("MetaLimit=%d>%d is too large", meta_limit, HARD_META_LIMIT);
  if (word_limit > HARD_WORD_LIMIT)
    return cf_printf("WordLimit=%d>%d is too large", word_limit, HARD_WORD_LIMIT);
  return NULL;
}

static struct cf_section word_weights_config = {};
static byte *wt_names[] = { WORD_TYPE_USER_NAMES, NULL };

static struct cf_section chewer_config = {
  CF_COMMIT(chewer_commit),
  CF_ITEMS {
    CF_UNS("WordBufSize", &word_buf_size),
    CF_UNS("StringBufSize", &string_buf_size),
    CF_BITMAP_LOOKUP("StringCats", &string_cats, ((const char* const []) {STRING_TYPE_USER_NAMES, NULL}) ),
    CF_UNS("StringMax", &string_max),
    CF_UNS("WordLimit", &word_limit),
    CF_UNS("MetaLimit", &meta_limit),
    CF_UNS("ExcerptMax", &excerpt_max),
    CF_UNS("DocBufSize", &doc_buf_size),
    CF_BITMAP_LOOKUP("GiantBanMeta", &giant_ban_meta, ((const char* const []) {META_TYPE_USER_NAMES, NULL}) ),
    CF_UNS("GiantPenalty", &giant_penalty),
    CF_SECTION("TypeWeights", type_weights, &word_weights_config),
    CF_UNS("SwindlerThreshold", &swindler_threshold),
    CF_UNS("NoContentsPenalty", &no_contents_penalty),
    CF_UNS("NoContentsThreshold", &no_contents_threshold),
    CF_LIST("HyperTextTypes", &hypertext_types, &cf_string_list_config),
    CF_UNS("NoLinksPenalty", &no_links_penalty),
    CF_UNS("NoTitlePenalty", &no_title_penalty),
    CF_UNS("MaxTitleLength", &max_title_len),
    CF_UNS("MinCompression", &min_compression),
    CF_UNS("MaxURLs", &max_urls),
    CF_UNS("MaxRedirects", &max_redirects),
    CF_END
  }
};

static void CONSTRUCTOR chewconf_init(void)
{
  cf_generate_word_type_config(&word_weights_config, wt_names, 1, 0);
  cf_declare_section("Chewer", &chewer_config, 0);
}

/*
 *  Output indices and their state
 *
 *  Except for output routines, we use a common set of in-memory structures
 *  for all indices and we encode the destination index in topmost 2 bits
 *  of the card id.
 */

struct out_index {
  uns type_mask;
  uns id_mask;
  uns card_cnt;
  struct fastbuf *card_attrs;
  struct fastbuf *cards_out;
  uns cards_compr, cards_uncompr;
  ucw_off_t total_uncompr;
  struct fastbuf *string_index;
  u64 string_cnt, string_dropped_cnt;
  struct fastbuf *word_index;
  u64 word_cnt;
  struct fastbuf *card_prints;
  byte *name;
  byte *directory;
  byte *log_prefix;
#ifdef CONFIG_IMAGES_SIM
  uns image_signatures_count;
  struct fastbuf *image_signatures;
#endif
};

#define CARD_ID_MASK 0x1fffffff
#define INDEX_ID_SHIFT 29

static inline uns id_to_card(uns id) { return id & CARD_ID_MASK; }
static inline uns id_to_idx(uns id) { return id >> INDEX_ID_SHIFT; }
static inline uns make_id(uns idx, uns cid) { ASSERT(cid <= CARD_ID_MASK); return (idx << INDEX_ID_SHIFT) | cid; }

static struct out_index out_index[HARD_MAX_SUBINDICES];
static uns num_out_indices;

static struct out_index *current_out;
static uns card_id;

static void
out_init(void)
{
  if (!fn_directory)
    die("Indexer.Directory not set");
  if (!clist_empty(&subindices))
    {
      num_out_indices = 0;
      struct subindex *sub;
      CLIST_WALK(sub, subindices)
	{
	  ASSERT(num_out_indices < HARD_MAX_SUBINDICES);
	  struct out_index *out = &out_index[num_out_indices++];
	  out->name = sub->name;
	  out->log_prefix = mp_strcat(cf_pool, out->name, ": ");
	  out->directory = mp_multicat(cf_pool, fn_directory, "/", out->name, NULL);
	  out->type_mask = sub->type_mask;
	  out->id_mask = sub->id_mask;
	}
    }
  else
    {
      num_out_indices = 1;
      struct out_index *out = &out_index[0];
      out->name = "main";
      out->log_prefix = "";
      out->directory = fn_directory;
      out->type_mask = ~0U;
      out->id_mask = ~0U;
    }
}

static int
out_find(struct card_attr *attr, struct card_note *note)
{
  uns ft, id;
#ifdef CONFIG_FILETYPE
  ft = CA_GET_FILE_TYPE(attr);
#else
  ft = 0;
#endif
  id = get_subindexing_id(fetch_id, note);

  for (uns i=0; i<num_out_indices; i++)
    if ((out_index[i].type_mask & (1 << ft)) &&
	(out_index[i].id_mask & (1 << id)))
      return i;
  return -1;
}

/*
 *  Encoding of reference chains, see also section WordIndex in doc/file-formats.
 */

static inline uns
mt_space(uns pos)
{
  /* Metas have position stored absolutely, because they are stored in a mixed
   * order behind the words.  */
  if (pos < (1<<6))
    return 2;
  else if (pos < (1<<13))
    return 3;
  else
    return 4;
}

static inline uns
wt_space(uns delta, uns type)
{
  /* Words are indexed first and they are stored using delta encoding.
   * Positions start from 1 and deltas are always positive, hence 0 is reserved
   * for positions behind the edge.  Strings have their own chains and
   * positions are not important for them, hence it is set to 0.  */
  if (type < 2 && delta < (1<<6))
    return 1;
  else if (delta < (1<<11))
    return 2;
  else if (delta < (1<<18))
    return 3;
  else
    return 4;
}

static inline uns
refchain_size(uns size1)
{
  return 4 + (size1 > 15 ? utf8_space(size1) : 0) + size1;
}

static inline void
bput_mt(struct fastbuf *f, uns pos, uns type)
{
  if (pos < (1<<6))
  {
    bputc(f, 0xe0 | (type >> 2));
    bputc(f, (type << 6) | pos);
  }
  else if (pos < (1<<13))
  {
    bputc(f, 0xf0 | (type & 0x03) | (type >> 3) & 0x04);
    bputw(f, (type >> 2) & 0x07 | (pos << 3));
  }
  else
  {
    bputc(f, 0xfc | type & 1);
    bputc(f, (type >> 2) | (type << 6) & 0x80 | (pos >> 12) & 0x70);
    bputw(f, pos);
  }
}

static inline void
bput_wt(struct fastbuf *f, uns delta, uns type)
{
  if (type < 2 && delta < (1<<6))
    bputc(f, (type << 6) | delta);
  else if (delta < (1<<11))
  {
    bputc(f, 0x80 | type | (delta >> 5) & 0x38);
    bputc(f, delta);
  }
  else if (delta < (1<<18))
  {
    bputc(f, 0xc0 | type | (delta >> 13) & 0x18);
    bputw(f, delta);
  }
  else
  {
    bputc(f, 0xf8 | type & 3);
    bputc(f, (type << 5) & 0x80 | (delta >> 16));
    bputw(f, delta);
  }
}

static inline void
bput_oid_size(struct fastbuf *f, uns oid, uns size)
{
  if (size <= 15)
    bputl(f, oid | (size << 28));
  else
    {
      bputl(f, oid);
      bput_utf8_32(f, size);
    }
}

/*
 *  Indexing of strings
 */

struct sentry {
  struct fingerprint fp;
  u32 id;
  u32 type;				// contains size1 stored as Hack2 (like Hack1)
};

static struct sentry *string_buf;
static uns sbuf_count, sbuf_size, sbuf_limit;
static uns string_runs;

static void
string_init(void)
{
  sbuf_size = string_buf_size / sizeof(struct sentry);
  if (sbuf_size < string_max)
    {
      log(L_WARN, "StringMax entries don't fit in StringBufSize, increasing StringBufSize to %d", string_max * (uns)sizeof(struct sentry));
      sbuf_size = string_max;
    }
  string_buf = big_alloc(sbuf_size * sizeof(struct sentry));
  sbuf_limit = sbuf_size - string_max;
  ITRACE("Allocated string pool with %d entries", sbuf_size);
}

static void
string_open(struct out_index *out)
{
  out->string_index = index_bopen(fn_string_index, O_WRONLY | O_CREAT | O_TRUNC, 1);
}

static inline int
string_cmp(const struct sentry *a, const struct sentry *b)
{
  int e = memcmp(&a->fp, &b->fp, sizeof(struct fingerprint));
  COMPARE_LT(e, 0);
  COMPARE_LT(a->id, b->id);
  COMPARE_LT(a->type, b->type);
  return 0;
}

#define ASORT_PREFIX(x) string_##x
#define ASORT_KEY_TYPE struct sentry
#define ASORT_LT(x,y) string_cmp(&(x), &(y))
#include "ucw/sorter/array.h"

static void
string_flush(void)
{
  if (!sbuf_count)
    return;
  string_sort(string_buf, sbuf_count);

  uns i = 0;
  while (i < sbuf_count)
    {
      /* calculate chain length */
      uns start = i++;
      uns to_hack = start;
      uns size = 0, size1 = wt_space(0, string_buf[start].type);
      struct sentry *first = &string_buf[start];
      for (; i<sbuf_count; i++)
	{
	  struct sentry *e = &string_buf[i];
	  struct sentry *prev = e-1;
	  if (memcmp(&first->fp, &e->fp, sizeof(struct fingerprint)) ||
	      id_to_idx(e->id ^ prev->id))
	    break;
	  else if (e->id != prev->id)
	    {
	      string_buf[to_hack].type |= size1 << 4;	// Hack2
	      size += refchain_size(size1);
	      to_hack = i;
	      size1 = wt_space(0, e->type);
	    }
	  else if (e->type != prev->type)
	    size1 += wt_space(0, e->type);
	}
      string_buf[to_hack].type |= size1 << 4;		// Hack2
      size += refchain_size(size1);

      /* output the chain */
      struct out_index *out = &out_index[id_to_idx(first->id)];
      struct fastbuf *fb = out->string_index;
      bwrite(fb, &first->fp, sizeof(struct fingerprint));
      bputl(fb, size);
      ucw_off_t expected_end = btell(fb) + size;
      struct sentry *e = &string_buf[start];
      struct sentry *clast = &string_buf[i];
      while (e < clast)
	{
	  size1 = e->type >> 4;				// Hack2
	  bput_oid_size(fb, id_to_card(e->id), size1);
	  e->type &= 0xf;				// Hack2
	  bput_wt(fb, 0, e->type);
	  for (e++; e<clast && e->id == (-1)[e].id; e++)
	    if (e->type != (-1)[e].type)
	      bput_wt(fb, 0, e->type);
	}
      ASSERT(btell(fb) == expected_end);
      out->string_cnt += i - start;
    }
  string_runs++;
  sbuf_count = 0;
}

static void
string_close(struct out_index *out)
{
  bclose(out->string_index);
  log(L_INFO, "%sGenerated %lld strings in %d runs; %lld strings dropped", out->log_prefix, (long long) out->string_cnt, string_runs, (long long) out->string_dropped_cnt);
}

static void
string_add(char *s, uns type)
{
  if (!(string_cats & (1 << type)))
    return;
  if (unlikely(sbuf_count >= sbuf_size))
    {
      current_out->string_dropped_cnt++;
      return;
    }

  struct sentry *e = &string_buf[sbuf_count++];
  fingerprint(s, &e->fp);
  e->id = card_id;
  e->type = type;
}

struct str_list {
  uns attr, type;
};

static void
string_add_attrs(struct odes *o, struct str_list *strs)
{
  while (strs->attr)
    {
      for (struct oattr *a=obj_find_attr(o, strs->attr); a; a=a->same)
	{
	  byte *x = a->val;
	  while (*x && *x != ' ')
	    x++;
	  if (*x)
	    {
	      *x = 0;
	      string_add(a->val, strs->type);
	      *x = ' ';
	    }
	  else
	    string_add(a->val, strs->type);
	}
      strs++;
    }
}

static void
string_card(struct odes *o)
{
  for (struct oattr *u = obj_find_attr(o, 'U' + OBJ_ATTR_SON); u; u=u->same)
    {
      char *url = obj_find_aval(u->son, 'U');
      ASSERT(url);
      char buf1[MAX_URL_SIZE], buf2[MAX_URL_SIZE];
      struct url ur;
#ifdef ST_URL
      string_add(url, ST_URL);
#endif
#if defined(ST_HOST) && defined(ST_DOMAIN)
      if (!url_canon_split(url, buf1, buf2, &ur) && ur.host)
	{
	  string_add(ur.host, ST_HOST);
	  char *dot = strrchr(ur.host, '.');
	  if (dot && dot > ur.host)
	    {
	      dot--;
	      while (dot > ur.host)
		{
		  if (*dot == '.')
		    string_add(dot+1, ST_DOMAIN);
		  dot--;
		}
	    }
	}
#endif
#ifdef ST_URL
      for (struct oattr *r = obj_find_attr(u->son, 'y' + OBJ_ATTR_SON); r; r=r->same)
	string_add_attrs(r->son, (struct str_list []) {
	    { 'y', ST_URL },
	    { 0, 0 }
	    });
#endif
#ifdef ST_IP
      {
	byte *s = obj_find_aval(u->son, 'k');
	if (s)
	  {
	    uns v;
	    sscanf(s, "%x", &v);
	    byte buf[16];
	    sprintf(buf, "%d.%d.%d.%d", (v >> 24) & 255, (v >> 16) & 255, (v >> 8) & 255, v & 255);
	    string_add(buf, ST_IP);
	  }
      }
#endif
    }

#ifdef ST_REF
  string_add_attrs(o, (struct str_list []) {
    { 'A', ST_REF },
    { 'F', ST_REF },
    { 'I', ST_REF },
    { 'R', ST_REF },
    { 'Y', ST_REF },
    { 'a', ST_REF },
    { 'd', ST_REF },
    { 'f', ST_REF },
    { 0, 0 }
    });
#endif

  custom_index_strings(o, string_add);

  if (sbuf_count >= sbuf_limit)
    string_flush();
}

/*
 *  Preprocessing of documents
 */

static byte *doc_buf;
static uns doc_length, doc_length_max;
static uns upgrade_counter, trim_counter;
static uns char_counter[WT_MAX], translate_type[WT_MAX];
static uns has_title, is_hypertext;

static void
preproc_init(void)
{
  doc_buf = big_alloc(doc_buf_size+1) + 1;
  doc_buf[-1] = 0;
}

static void
preproc_end(void)
{
  log(L_INFO, "Upgraded %d objects, trimmed %d objects (longest was %d chars)", upgrade_counter, trim_counter, doc_length_max);
}

static void
preproc_card(struct odes *o)
{
  byte *w = doc_buf;
  byte *stop = doc_buf + doc_buf_size - 5; /* 5 = space or category changer + UTF-8 character + trailing zero */

  byte *version = obj_find_aval(o, 'v');
  if (!version)
    {
      if (obj_find_attr(o, 'X'))
	die("Obsolete v0-card %08x found", fetch_id);
      else
	version = "2";
    }
  if (*version < '1' || *version > '2')
    die("Weird v%s-card %08x found", version, fetch_id);
  uns upgrade_wt UNUSED = 0;
  if (*version == '1')
#ifdef WT_CONVERT_v1_v2
    {
      upgrade_wt = 1;
      upgrade_counter++;
    }
#else
    die("Obsolete v1-card %08x found", fetch_id);
#endif

  bzero(char_counter, sizeof(char_counter));
  struct oattr *oa = obj_find_attr(o, 'X');
  byte *r;
  byte *wsp = w;
  uns c;
  uns cat = WT_TEXT;
  uns written_cat = ~0U; /* Always start with the category changer. We assume this invariant in search server. */
  for (; oa; oa=oa->same)
    {
      r = oa->val;
      c = ' ';
      do
        {
	  /* Space */
	  if (Cspace(c))
	    {
	      if (wsp != w) /* Merge multiple spaces & ignore leading spaces */
	        {
	          *w++ = ' ';
	          wsp = w;
		}
	    }
	  /* Printable character (could be slightly improved by removing few ifs) */
	  else if ((c ^ 0x80) >= 0x40)
	    {
	      if (cat != written_cat)
	        {
		  if (wsp == w && w != doc_buf) /* Ignore trailing spaces */
		    w--;
		  *w++ = cat | 0x80;
		  wsp = w;
		  written_cat = cat &= 0x0f;
		}
	      char_counter[cat]++;
	      *w++ = c;
	      if (c >= 0x80)
		do
		  {
		    ASSERT(*r & 0x80);
		    *w++ = *r++;
		    c <<= 1;
		  }
		while (c & 0x40);
              if (unlikely(w >= stop))
	        {
		  w = wsp;			/* Remove the partial word */
		  trim_counter++;
		  obj_add_attr(o, '.', "Trimmed text");
		  goto done;
		}
	    }
	  /* Category changer */
	  else if (c < 0xa0)
	    {
	      cat = (cat & 0x10) | (c & 0x1f);
#ifdef WT_CONVERT_v1_v2
	      if (upgrade_wt)	// Upgrade v1->v2: change the word-type
		cat = (cat & 0x10) | WT_CONVERT_v1_v2(cat & 0x0f);
#endif
	      if (wsp != w)
	        {
	          *w++ = ' ';
	          wsp = w;
		}
	    }
	  /* Brackets are to be removed */
	  else if (c < 0xb0)
	    {
	      ASSERT(*r);
	      r++;
	    }
	}
      while (c = *r++);
    }
done:
  if (wsp == w && w != doc_buf)
    w--;
  *w = 0;
  doc_length = w - doc_buf;
  if (doc_length > doc_length_max)
    doc_length_max = doc_length;
}

static void
preproc_reftexts(struct odes *o)
{
  for (struct oattr *rt = obj_find_attr(o, 'x'); rt; rt=rt->same)
    {
      byte *c = strchr(rt->val, ' ');
      *c++ = 0;
      uns wt, cnt, rd, len;
      rd = sscanf(c, "%d %d %n", &wt, &cnt, &len);
      ASSERT(rd == 2);
      c += len;

      struct odes *q = obj_add_son(o, 'x' + OBJ_ATTR_SON);
      obj_set_attr(q, 'M', c);
      obj_set_attr(q, 'z', rt->val);
      byte buf[64];
      sprintf(buf, "x%d %d", wt, cnt);
      obj_set_attr(q, 'W', buf);
    }
  obj_set_attr(o, 'x', NULL);
}

static uns
average_weight(void)
{
  uns cnt=0, wt=0;
  for (uns i=0; i<WT_MAX; i++)
    if (type_weights[i])
    {
      cnt += char_counter[i];
      wt += char_counter[i] * type_weights[i];
    }
  if (cnt)
    wt /= cnt;
  return wt;
}

static void
detect_swindler(struct odes *o)
{
  /* By default, we do not translate any type */
  for (uns i=0; i<WT_MAX; i++)
    translate_type[i] = i;

  /* Compute the initial average weight */
  uns wt = average_weight();
  uns is_swindler = (wt > swindler_threshold);

#undef	DEBUG_SWINDLERS
#ifdef	DEBUG_SWINDLERS
  static uns id = 0;
  id++;
  byte buf[500];
  uns len = sprintf(buf, "Frequencies of document %x: ", id);
  uns total_cnt = 0;
  for (uns i=0; i<WT_MAX; i++)
    if (char_counter[i])
    {
      len += sprintf(buf+len, "%s:%d ", wt_names[i], char_counter[i]);
      total_cnt += char_counter[i];
    }
  len += sprintf(buf+len, "TOTAL:%d avg-wt:%d", total_cnt, wt);
  if (is_swindler)
    len += sprintf(buf+len, " SWINDLER!");
  puts(buf);
#endif

  if (!is_swindler)
    return;

  do
  {
    int i;
    /* Find a used type with the biggest weight and the type the nearest
     * smaller weight */
    int max = -1, max2 = -1;
    for (i=0; i<WT_MAX; i++)
      if (char_counter[i] && (max<0 || type_weights[i] > type_weights[max]))
	max = i;
    ASSERT(max >= 0 && type_weights[max] > 0);
    for (i=0; i<WT_MAX; i++)
      if (type_weights[i] < type_weights[max]
	  && (max2<0 || type_weights[i] >= type_weights[max2]))
	max2 = i;
    /* Beware!  There's a little magic with > and >= on type_weights, do not change it!  */
    ASSERT(max2 >= 0);
    if (!type_weights[max2])
      die("Invalid configuration of Chewer.TypeWeights, you must have a type with weight smaller than SwindlerThreshold");
    /* Remap the type */
#ifdef	DEBUG_SWINDLERS
    printf("Type %s with frequency %d remapped to %s, ", wt_names[max], char_counter[max], wt_names[max2]);
#endif
    for (i=0; i<WT_MAX; i++)
      if (translate_type[i] == (uns)max)
      {
	translate_type[i] = max2;
	char_counter[max2] += char_counter[i];
	char_counter[i] = 0;
      }
    wt = average_weight();
#ifdef	DEBUG_SWINDLERS
    printf("new average weight is %d\n", wt);
#endif
  }
  while (wt > swindler_threshold);

  byte mappings[WT_MAX * 32], *ptr = mappings;
  for (uns i=0; i<WT_MAX; i++)
    if (translate_type[i] != i)
      ptr += sprintf(ptr, "%s->%s ", wt_names[i], wt_names[translate_type[i]]);
  obj_add_attr_format(o, '.', "Penalized by 0: swindling, remapped %s", mappings);
#ifdef  DEBUG_SWINDLERS
  printf("New types: %s\n", mappings);
#endif
}

static void
penalize_card(struct fastbuf *cards_mem, struct card_attr *attr, struct card_note *note, uns is_image, uns is_in_catalog)
{
  bput_attr_format(cards_mem, 'W', "s%d", note->weight_scanner);
#ifdef CONFIG_WEIGHTS
  bput_attr_format(cards_mem, 'W', "d%d", note->weight_dynamic);
#endif
  bput_attr_format(cards_mem, 'W', "m%d", note->weight_merged);

  int wt = attr->weight;
  if (note->flags & CARD_NOTE_GIANT)
  {
    /* Beware of changing the text "Penalized by " and ": giant class", this message is detected in centrum/indexer/patch-index.c */
    bput_attr_format(cards_mem, '.', "Penalized by %d: giant class", giant_penalty);
    wt -= giant_penalty;
  }
  if (!is_image && !is_in_catalog && !(note->flags & CARD_NOTE_AUDIO))
  {
    if (no_contents_penalty && note->useful_size < no_contents_threshold)
    {
      bput_attr_format(cards_mem, '.', "Penalized by %d: no contents (%d chars)", no_contents_penalty, note->useful_size);
      wt -= no_contents_penalty;
    }
  }
  if (is_hypertext && !is_in_catalog)
  {
    if (no_links_penalty && !(note->flags & CARD_NOTE_HAS_LINKS))
    {
      bput_attr_format(cards_mem, '.', "Penalized by %d: no links", no_links_penalty);
      wt -= no_links_penalty;
    }
#ifdef	MT_TITLE
    if (no_title_penalty && !has_title)
    {
      bput_attr_format(cards_mem, '.', "Penalized by %d: no title", no_title_penalty);
      wt -= no_title_penalty;
    }
#endif
  }
  if (note->card_bonus)
  {
    bput_attr_format(cards_mem, '.', "Penalized by %d: filters", -note->card_bonus);
    wt += note->card_bonus;
  }
  attr->weight = CLAMP(wt, 0, 255);
  bput_attr_format(cards_mem, 'W', "p%d", attr->weight);
}

/*
 *  Indexing of words
 */

#define LENT_QUANTUM 3
  // if you increase LENT_QUANTUM above 15, modify Hack1
#define LENT_BITE 4096

typedef struct lentry {
  struct lentry *next;
  u32 id;
  u32 count;				// contains size1 stored as Hack1
  ref_pos_t pos[LENT_QUANTUM];
  byte type[LENT_QUANTUM];
} lentry;

#define GBUF_TYPE	struct verbum *
#define GBUF_PREFIX(x)	uw_##x
#define GBUF_TRACE(msg...) ITRACEN(2, msg)
#include "ucw/gbuf.h"

static uns lentry_count, lentry_limit;
static struct verbum **context_words;
static uw_t used_words_buf;
static uns nr_used_words;
static struct verbum **used_words;
static struct mempool *word_pool;
static uns word_runs;
static u64 word_entries;
static uns meta_static_part;
static uns numerus_verba;

#define LH_CHEWER
#include "indexer/lexhash.h"

static void
lex_load(void)
{
  struct fastbuf *b;
  struct verbum *v, **ctxt_last=NULL;

  b = index_bopen(fn_lex_ordered, O_RDONLY, 1);
  numerus_verba = bgetl(b);
  if (lex_context_slots)
    {
      context_words = big_alloc_zero(sizeof(struct verbum *) * lex_context_slots);
      ctxt_last = alloca(sizeof(struct verbum *) * lex_context_slots);
    }
  for (uns i=0; i<numerus_verba; i++)
    {
      u32 id = bgetl(b);
      u32 cnt = bgetl(b);
      uns ctxt = bget_context(b);
      enum word_class class = id & 7;
      uns len = bgetc(b);
      if (class == WC_COMPLEX)
	v = ctxt_last[ctxt]++;
      else
	{
	  byte buf[MAX_WORD_BYTES+1];
	  breadb(b, buf, len);
	  buf[len] = 0;
	  v = lh_insert(buf, 0);
	  if (!v)
	    die("Malformed lexicon: Duplicate word <%s>", buf);
	  if (class == WC_CONTEXT)
	    ctxt_last[ctxt] = context_words[ctxt] = big_alloc_zero(sizeof(struct verbum)*2*lex_context_slots);
	}
      v->id = id;
      v->u.count = cnt;
      PUT_CONTEXT(&v->context_class, ctxt);
    }
  bclose(b);
  lh_rehash(lh_hash_count);		/* Sort the chains by counts */
  LH_WALK(v)
    v->u.first_lent = NULL;
  log(L_INFO, "Read lexicon with %d words (%d total entries)", lh_hash_count, numerus_verba);
}

static void
word_flush_single(uns word_id, lentry *head)
{
  lentry *f, *g;

  /* Need to untangle the chains belonging to different destination indices and reverse them */
  static lentry *chains[HARD_MAX_SUBINDICES];	/* Kept clean between calls */
  while (head)
    {
      f = head->next;
      uns idx = id_to_idx(head->id);
      head->next = chains[idx];
      chains[idx] = head;
      head = f;
    }

  /* Scan all destination indices and generate reference chains */
  for (uns idx=0; idx<num_out_indices; idx++)
    if (chains[idx])
      {
	struct out_index *out = &out_index[idx];
	struct fastbuf *fb = out->word_index;
	head = chains[idx];
	chains[idx] = NULL;

	/* Calculate block size */
	uns size = 0;
	f = head;
	while (f)
	  {
	    g = f;
	    uns size1 = 0;
	    uns last_pos = 0;
	    while (f && f->id == g->id)
	      {
		for (uns i=0; i<f->count; i++)
		  if (f->type[i] & 0x80)
		    size1 += mt_space(f->pos[i]);
		  else
		    {
		      uns delta = f->pos[i] ? f->pos[i] - last_pos : 0;
		      size1 += wt_space(delta, f->type[i]);
		      last_pos = f->pos[i];
		      f->pos[i] = delta;	// rewrite to delta
		    }
		f = f->next;
	      }
	    size += refchain_size(size1);
	    g->count |= size1 << 4;		// Hack1: we do not want to compute size1 again when dumping
	  }

	/* Write header */
	bputl(fb, word_id);
	bputl(fb, size);
	ucw_off_t expos = btell(fb) + size;

	/* Write body */
	f = head;
	while (f)
	  {
	    uns size1 = f->count >> 4;	// remove Hack1
	    f->count &= 0xf;
	    bput_oid_size(fb, id_to_card(f->id), size1);
	    g = f;
	    while (f && g->id == f->id)
	      {
		for (uns i=0; i<f->count; i++)
		  {
		    //log(L_DEBUG, "Dumping W%x O%x %c%x P%x", id, id_to_card(f->id), (f->type[i] & 0x80) ? 'M' : 'W', f->type[i] & 0x7f, f->pos[i]);
		    if (f->type[i] & 0x80)
		      bput_mt(fb, f->pos[i], f->type[i]);
		    else
		      bput_wt(fb, f->pos[i], f->type[i]);
		  }
		f = f->next;
	      }
	    out->word_cnt++;
	  }

	/* Sanity check */
	if (unlikely(expos != btell(fb)))
	  die("word_flush_single: Internal error, size mismatch by %d", (int)(btell(fb) - expos));
      }
}

#define ASORT_PREFIX(x) word_##x
#define ASORT_KEY_TYPE struct verbum *
#define ASORT_LT(x,y) ((x)->id < (y)->id)
#include "ucw/sorter/array.h"

static void
word_flush(int final)
{
  struct verbum **w, *v;

  used_words = used_words_buf.ptr;			// save one dereference
  word_sort(used_words, nr_used_words);
  used_words[nr_used_words] = NULL;
  for (w=used_words; v = *w; w++)
    {
      word_flush_single(v->id/8, v->u.first_lent);
      v->u.first_lent = NULL;
    }

  mp_flush(word_pool);
  word_runs++;
  word_entries += lentry_count;
  lentry_count = 0;
  nr_used_words = 0;

  if (final)
    ITRACE("Words used %lld entries, that is %lld bytes", (long long)word_entries, (long long)(word_entries * sizeof(lentry)));
}

static inline int
word_check(struct verbum *v, uns type)
{
  lentry *e = v->u.first_lent;

  while (e && e->id == card_id)
    {
      int i = e->count - 1;
      while (i >= 0)
	{
	  if (e->pos[i])
	    return 0;
	  else if (e->type[i] == type)
	    return 1;
	  i--;
	}
      e = e->next;
    }
  return 0;
}

static inline void
word_add(struct verbum *v, uns type, int pos)
{
  lentry *e = v->u.first_lent;
  if (!e || e->id != card_id || e->count >= LENT_QUANTUM)
    {
      /* Trick: we allocate only lentries, so it will be always aligned */
      e = mp_alloc_fast_noalign(word_pool, sizeof(lentry));
      if (!v->u.first_lent)
        {
	  uw_grow(&used_words_buf, nr_used_words+2);
	  used_words_buf.ptr[nr_used_words++] = v;
	}
      e->next = v->u.first_lent;
      v->u.first_lent = e;
      e->id = card_id;
      e->count = 0;
      lentry_count++;
    }
  e->type[e->count] = type;
  e->pos[e->count++] = pos+1;	// positions start from 1, and 0 means behind the edge
}

static enum word_class
lm_lookup(enum word_class orig_class, u16 *uni, uns ulen, word_id_t *idp, void *user UNUSED)
{
  struct verbum *v;

  if (orig_class != WC_NORMAL)
    return orig_class;
  v = lh_lookup(uni, ulen);
  *idp = v;
  return v->id & 7;
}

static void
lm_got_word(uns pos, uns cat, word_id_t w, void *user UNUSED)
{
  if (meta_static_part)
    {
      if (pos < meta_limit)
	word_add(w, meta_static_part, pos);
#ifdef MT_TITLE
      if (cat == MT_TITLE)
	has_title = 1;
#endif
      return;
    }
  cat = translate_type[cat];
  if (pos >= word_limit)
    {
      if (!word_check(w, cat))
	word_add(w, cat, -1);
    }
  else
    word_add(w, cat, pos);
}

#ifdef CONFIG_CONTEXTS
static inline void
lm_got_complex(uns pos, uns cat, word_id_t root, word_id_t w, uns dir, void *user UNUSED)
{
  struct verbum *v = context_words[root->context_class] + w->context_class + (dir ? lex_context_slots : 0);
  lm_got_word(pos, cat, v, user);
}
#else
static inline void
lm_got_complex(uns pos UNUSED, uns cat UNUSED, word_id_t root UNUSED, word_id_t w UNUSED, uns dir UNUSED, void *user UNUSED)
{
}
#endif

#include "indexer/lexmap.h"

struct meta_info {
  struct meta_info *next;
  uns static_part;
  byte *text;
};
static struct meta_info *meta_first[16], *meta_last[16];
static struct fastbuf *analyse_meta_fb;
static struct mempool *meta_pool;

static inline void
word_meta_preprocess(struct oattr *av, struct mempool *pool)
{
#ifdef MT_TITLE
  byte *t = av->val;
  if (*t >= '0' && *t <= '3')
    return;
  ASSERT(*t >= 0x90 && *t < 0xa0);
  if ((*t & 0x0f) == MT_TITLE && strlen(t) > max_title_len)
    {
      byte *cp = mp_alloc(pool, strlen(t)+2);
      *cp = '1';
      strcpy(cp+1, t);
      av->val = cp;
    }
#endif
}

static inline int
word_meta_add(byte *t, struct meta_info *m, uns permit_types)
{
  if (*t >= '0' && *t <= '3')
    m->static_part = (*t++ - '0');
  else
    m->static_part = 0;
  ASSERT(*t >= 0x90 && *t < 0xa0);
  uns type = *t & 0x0f;
  if (!(permit_types & (1 << type)))
    return 0;
  m->static_part |= 0x80 | (type << 2);	// 10tt ttww
  if (meta_first[type])
    meta_last[type]->next = m;
  else
    meta_first[type] = m;
  meta_last[type] = m;
  m->next = NULL;
  m->text = t;
  return 1;
}

static void
word_meta(struct odes *o, struct card_note *note)
{
  uns permit_types = (note->flags & CARD_NOTE_GIANT) ? ~giant_ban_meta : ~0U;
  bzero(meta_first, sizeof(meta_first));
  mp_flush(meta_pool);
  struct meta_info *mi = mp_alloc_fast(meta_pool, sizeof(*mi));

  if (analyse_meta_fb)
    fbgrow_reset(analyse_meta_fb);

#define ADD_META(a) if (word_meta_add(a->val, mi, permit_types)) mi=mp_alloc_fast(meta_pool, sizeof(*mi))
#define DO_METAS(x) for (struct oattr *a=obj_find_attr(x, 'M'); a; a=a->same) ADD_META(a)
#define DO_CAT(y) for (struct oattr *c=obj_find_attr(y, 'c' + OBJ_ATTR_SON); c; c=c->same) DO_METAS(c->son)

  for (struct oattr *u = obj_find_attr(o, 'U' + OBJ_ATTR_SON); u; u=u->same)
  {
    DO_METAS(u->son);
    DO_CAT(u->son);
    for (struct oattr *r=obj_find_attr(u->son, 'y' + OBJ_ATTR_SON); r; r=r->same)
      {
	DO_METAS(r->son);
	DO_CAT(r->son);
      }
  }
  for (struct oattr *a=obj_find_attr(o, 'M'); a; a=a->same)
    {
      word_meta_preprocess(a, o->pool);
      ADD_META(a);
    }
  for (struct oattr *a=obj_find_attr(o, 'x' + OBJ_ATTR_SON); a; a=a->same)
    {
      struct oattr *b=obj_find_attr(a->son, 'M');
      ASSERT(b);
      ADD_META(b);
    }
#undef DO_CAT
#undef DO_METAS
#undef ADD_META

  has_title = 0;
  for (uns type=0; type<16; type++)
    if (meta_first[type])
      {
	lm_doc_start(NULL);
	for (struct meta_info *m=meta_first[type]; m; m=m->next)
	  {
	    meta_static_part = m->static_part;
	    lm_map_text(m->text, m->text + str_len(m->text));
	    if (analyse_meta_fb)
	      {
		bputc(analyse_meta_fb, 0x90 + type);
		bputs(analyse_meta_fb, m->text);
	      }
	  }
      }
}

static void
word_card(struct odes *o, struct card_note *note)
{
  lm_doc_start(NULL);
  meta_static_part = 0;
  lm_map_text(doc_buf, doc_buf + doc_length);
  word_meta(o, note);
  if (lentry_count >= lentry_limit)
    word_flush(0);
}

static void
word_init(void)
{
  lh_init();
  lm_init();
  lex_load();
  word_pool = mp_new(sizeof(lentry) * LENT_BITE);
  meta_pool = mp_new(4096);
  lentry_limit = word_buf_size / (sizeof(lentry) + sizeof(struct lentry *));
  uw_init(&used_words_buf);
  uw_grow(&used_words_buf, lentry_limit);
  nr_used_words = 0;
  ITRACE("Allocated word pool, lentry_limit=%d", lentry_limit);
}

static void
word_open(struct out_index *out)
{
  out->word_index = index_bopen(fn_word_index, O_WRONLY | O_CREAT | O_TRUNC, 1);
}

static void
word_close(struct out_index *out)
{
  bclose(out->word_index);
  log(L_INFO, "%sGenerated %lld word refs in %d runs", out->log_prefix, (long long) out->word_cnt, word_runs);
}

/*
 *  Processing of cards
 */

static struct fastbuf *cards_mem;
static uns url_trim_cnt, redir_trim_cnt, cards_largest;

static void
cards_init(void)
{
  put_attr_set_type(BUCKET_TYPE_V33);
  lizard_set_type(BUCKET_TYPE_V33_LIZARD, min_compression / 100.);
  cards_mem = fbgrow_create(2*excerpt_max);
}

static void
cards_open(struct out_index *out)
{
  out->cards_out = index_bopen(fn_cards, O_WRONLY | O_CREAT | O_TRUNC, 1);
  lizard_bwrite(out->cards_out, "", 0);

  out->card_attrs = index_bopen(fn_card_attrs, O_WRONLY | O_CREAT | O_TRUNC, 1);
  struct card_attr a;
  bzero(&a, sizeof(a));			/* Create dummy document 0 to make all read ID's be >0 */
  bwrite(out->card_attrs, &a, sizeof(a));
  out->card_cnt++;
}

static void
card_write_start(struct card_attr *attr)
{
  fbgrow_reset(cards_mem);

  struct out_index *out = current_out;
  uns align = (1 << CARD_POS_SHIFT) - 1;
  ucw_off_t pos = btell(out->cards_out);
  while (pos & align)
    {
      bputc(out->cards_out, 0);
      pos++;
    }
  if ((u64)(pos >> CARD_POS_SHIFT) >= 0xffffffff)
    die("Card file too large after %08x. You need to increase CARD_POS_SHIFT in sherlock/index.h.", fetch_id);
  attr->card = pos >> CARD_POS_SHIFT;
}

static void
card_write_end(void)
{
  struct out_index *out = current_out;
  uns len_in = btell(cards_mem);
  cards_largest = MAX(cards_largest, len_in);
  fbgrow_rewind(cards_mem);
  uns type = lizard_bbcopy_compress(out->cards_out, cards_mem, len_in);
  if (type == BUCKET_TYPE_V33_LIZARD)
    out->cards_compr++;
  else
    out->cards_uncompr++;
  out->total_uncompr += len_in;
}

static void
cards_close(struct out_index *out)
{
  struct card_attr a;

  bzero(&a, sizeof(a));			/* Append fake attribute marking end of card file */
  card_write_start(&a);
  bwrite(out->card_attrs, &a, sizeof(a));
  ucw_off_t total_compr = btell(out->cards_out);
  bclose(out->cards_out);
  bclose(out->card_attrs);
  log(L_INFO, "%sGenerated %d cards: %d compressed, %d uncompressed, compressed to %d%%", out->log_prefix,
      out->card_cnt, out->cards_compr, out->cards_uncompr, (uns) ( 100. * total_compr / (out->total_uncompr ? : 1)));
}

static void
cards_end(void)
{
  log(L_INFO, "Trimmed URL list for %d cards, redirect list for %d; largest card has %d bytes",
      url_trim_cnt, redir_trim_cnt, cards_largest);
  bclose(cards_mem);
}

static void
probe_content_type(struct odes *o)
{
  byte *ctype = obj_find_aval(o, 'T');
  if (is_hypertext || !ctype)
    return;
  CLIST_FOR_EACH(simp_node *, n, hypertext_types)
    if (!strcmp(ctype, n->s))
      {
	is_hypertext = 1;
	break;
      }
}

static void
card_card(struct odes *o, struct card_attr *attr, struct card_note *note)
{
  /*
   * First of all, dump all headers and other nested parts.
   * CAVEAT: search/cards.c expects them to appear before all other attributes!
   */

  is_hypertext = 0;
  uns is_in_catalog = 0;
  uns url_count = 0;
  uns urls_trimmed = 0, redirs_trimmed = 0;

  card_write_start(attr);

  for (struct oattr *u = obj_find_attr(o, 'U' + OBJ_ATTR_SON); u; u=u->same)
    {
      struct odes *uu = u->son;
      obj_move_attr_to_head(uu, 'U');		// For purely aesthetic reasons

      uns is_cat = 0;
      probe_content_type(uu);
      if (obj_find_aval(uu, 'c' + OBJ_ATTR_SON))
	is_cat = 1;

      struct oattr *first_r = obj_find_attr(uu, 'y' + OBJ_ATTR_SON);
      struct oattr *r, **pr = &first_r;		// First redirect is sure to stay
      uns redir_count = 0;
      while (r = *pr)
	{
	  redir_count++;
	  obj_move_attr_to_head(r->son, 'y');
	  if (obj_find_aval(r->son, 'c' + OBJ_ATTR_SON))
	    is_cat = 1;
	  else if (redir_count > max_redirects)
	    {
	      *pr = r->same;
	      redirs_trimmed++;
	      continue;
	    }
	  pr = &r->same;
	}

      is_in_catalog |= is_cat;
      url_count++;
      if (url_count <= max_urls || is_cat)
	{
	  bput_attr_push(cards_mem, 'U');
	  bput_object_nocheck(cards_mem, uu);
	  bput_attr_pop(cards_mem);
	}
      else
	urls_trimmed++;
    }
  if (urls_trimmed)
  {
    url_trim_cnt++;
    bput_attr_format(cards_mem, '.', "Trimmed %u URLs", urls_trimmed);
  }
  if (redirs_trimmed)
  {
    redir_trim_cnt++;
    bput_attr_format(cards_mem, '.', "Trimmed %u redirects", redirs_trimmed);
  }

  /* Dump reftexts */
  bput_oattr_nocheck(cards_mem, obj_find_attr(o, 'x' + OBJ_ATTR_SON));

  /* Then dump all other attributes (subobjects first because of the "CAVEAT" above) */
  for (struct oattr *at=o->attrs; at; at=at->next)
    if (at->attr >= OBJ_ATTR_SON && attr_set_match(&card_attr_set, at))
      bput_oattr_nocheck(cards_mem, at);
  for (struct oattr *at=o->attrs; at; at=at->next)
    if (at->attr < OBJ_ATTR_SON && attr_set_match(&card_attr_set, at))
      bput_oattr_nocheck(cards_mem, at);

  /* Perform final penalization and dump notes on evolution of the weight */
  if (!raw_stage2_input)
    penalize_card(cards_mem, attr, note, !!obj_find_attr(o, 'N'), is_in_catalog);

  /* Document contents, but limited to the useful part */
  uns l = excerpt_max;
  if (l && doc_length)
  {
    if (l < doc_length)
    {
      uns i, maxl=MIN(l+256, doc_length);
      for (i=l; i<maxl; i++)
	if (doc_buf[i] == ' ')
	  break;
      if (i < maxl)
	l = i;
    }
    else
      l = doc_length;
    bput_attr_large(cards_mem, 'X', doc_buf, l);
  }

  card_write_end();
}

/*
 *  Processing of card fingerprints
 */

static void
prints_open(struct out_index *out)
{
  out->card_prints = index_maybe_bopen(fn_card_prints, O_WRONLY | O_CREAT | O_TRUNC, 1);
}

static void
prints_close(struct out_index *out)
{
  bclose(out->card_prints);
}

static void
prints_add(byte *url)
{
  struct card_print e;
  fingerprint(url, &e.fp);
  e.cardid = id_to_card(card_id);
  bwrite(current_out->card_prints, &e, sizeof(e));
}

static void
prints_card(struct odes *o)
{
  if (!current_out->card_prints)
    return;

  for (struct oattr *u = obj_find_attr(o, 'U' + OBJ_ATTR_SON); u; u=u->same)
    {
      byte *url;
      if (url = obj_find_aval(u->son, 'U'))
	prints_add(url);
      for (struct oattr *r = obj_find_attr(u->son, 'y' + OBJ_ATTR_SON); r; r=r->same)
	if (url = obj_find_aval(r->son, 'y'))
	  prints_add(url);
    }
}

/*
 *  Interface to the analysers
 */

static struct fastbuf analyse_text_fb;
static struct an_context an_context;
static struct mempool *an_pool;

#define GBUF_TYPE	struct odes *
#define GBUF_PREFIX(x)	ob_##x
#include "ucw/gbuf.h"
static ob_t url_block_buf;

static void
analyse_init(void)
{
  an_pool = mp_new(0x2000);
  analyser_init_hook(AN_HOOK_CHEWER);
  analyser_init(&an_context, AN_HOOK_CHEWER, AN_NEED_TEXT | AN_NEED_METAS | AN_NEED_ALL_URLS, NULL);
  if (an_context.need_mask & AN_NEED_METAS)
    analyse_meta_fb = fbgrow_create(4096);
  ob_init(&url_block_buf);
}

static void
analyse_end(void)
{
  bclose(analyse_meta_fb);
  analyser_log_stats(&an_context);
  analyser_cleanup(&an_context);
  mp_delete(an_pool);
}

static void
analyse_card(struct odes *o)
{
  struct oattr *ua = obj_find_attr(o, 'U' + OBJ_ATTR_SON);
  struct an_iface ai = {
    .obj = o,
    .url_block = ua->son,
    .pool = an_pool
  };

  if (an_context.need_mask & AN_NEED_ALL_URLS)
    {
      uns i = 0;
      for (; ua; ua=ua->same)
	{
	  ob_grow(&url_block_buf, i+1);
	  url_block_buf.ptr[i++] = ua->son;
	  for (struct oattr *ra = obj_find_attr(ua->son, 'y' + OBJ_ATTR_SON); ra; ra=ra->same)
	    {
	      ob_grow(&url_block_buf, i+1);
	      url_block_buf.ptr[i++] = ra->son;
	    }
	}
      ob_grow(&url_block_buf, i+1);
      url_block_buf.ptr[i] = NULL;
      ai.all_urls = url_block_buf.ptr;
    }

  uns need = analyser_need(&an_context, &ai);
  if (need)
    {
      if (need & AN_NEED_TEXT)
	{
	  fbbuf_init_read(&analyse_text_fb, doc_buf, doc_length, 0);
	  ai.text = &analyse_text_fb;
	}
      if (need & AN_NEED_METAS)
	{
	  fbgrow_rewind(analyse_meta_fb);
	  ai.metas = analyse_meta_fb;
	}
      analyser_run_needed(&an_context, &ai);
    }
  mp_flush(an_pool);
}

/*
 *  Processing of index parameters
 */

static struct index_params global_params;

static void
params_init(void)
{
  params_load(&global_params);
}

static void
params_close(struct out_index *out)
{
  struct index_params par = global_params;
  par.type_mask = out->type_mask;
  par.id_mask = out->id_mask;
  par.cards_out = out->card_cnt;
  params_save(&par);
}

/*
 *  Processing of image signatures
 */

#ifdef CONFIG_IMAGES_SIM

static void
images_open(struct out_index *out)
{
  out->image_signatures = index_bopen(fn_image_signatures_unsorted, O_WRONLY | O_CREAT | O_TRUNC, 1);
  out->image_signatures_count = 0;
  bputl(out->image_signatures, 0);
}

static void
images_close(struct out_index *out)
{
  brewind(out->image_signatures);
  bputl(out->image_signatures, out->image_signatures_count);
  bclose(out->image_signatures);
  log(L_INFO, "Written %u image signatures", out->image_signatures_count);
}

static void
images_card(struct odes *o)
{
  struct image_signature sig;
  if (get_image_obj_signature(&sig, o) && sig.len)
    {
      struct fastbuf *image_signatures = current_out->image_signatures;
      bputl(image_signatures, id_to_card(card_id));
      bwrite(image_signatures, &sig, image_signature_size(sig.len));
      current_out->image_signatures_count++;
    }
}

#else
static inline void images_open(struct out_index *out UNUSED) {}
static inline void images_close(struct out_index *out UNUSED) {}
static inline void images_card(struct odes *o UNUSED) {}
#endif

/*
 *  Processing of admin export
 */

static struct fastbuf *admin_out;

static void
admin_init(void)
{
  if (admin_out = index_maybe_bopen(fn_admin_export, O_CREAT | O_TRUNC | O_WRONLY, 1))
    {
      struct odes *o = obj_new(cf_pool);
      params_to_obj(&global_params, o, 0);
      bput_attr_format(admin_out, 'v', "%08x", ADMIN_EXPORT_VERSION);
      bput_attr_push(admin_out, 'p');
      obj_write(admin_out, o, BUCKET_TYPE_PLAIN);
      bput_attr_pop(admin_out);
      bputc(admin_out, '\n');
    }
}

static void
admin_end(void)
{
  if (admin_out)
    bclose(admin_out);
}

static void
admin_card(int idx, uns id, struct card_info *info)
{
  if (admin_out)
    {
      struct card_note *cn = &info->note;
      struct card_attr *ca = &info->attr;
      struct admin_export n = {
	.id = id,
	.subindex = idx,
	.flags = ca->flags,
	.note_flags = cn->flags,
	.weight = ca->weight,
	.weight_scanner = cn->weight_scanner,
#ifdef CONFIG_WEIGHTS
	.weight_dynamic = cn->weight_dynamic,
#endif
	.weight_merged = cn->weight_merged,
#ifdef CONFIG_LASTMOD
	.age = ca->age,
#endif
#ifdef CONFIG_FILETYPE
	.file_flags = ca->type_flags,
	.file_type = CA_GET_FILE_TYPE(ca),
	.file_lang = CA_GET_FILE_LANG(ca),
#endif
#ifdef CONFIG_SITES
	.site = ca->site_id,
#endif
      };
      memcpy(n.footprint, info->note.footprint, 16);
      bwrite(admin_out, &n, sizeof(n));
    }
}

/*
 *  Main loop
 */

static uns num_dropped_cards;

static void
chew_card(struct card_info *info, struct odes *o)
{
  struct card_attr *attr = &info->attr;
  struct card_note *note = &info->note;

  int idx = out_find(attr, note);
  if (idx < 0)
    {
      num_dropped_cards++;
      return;
    }
  current_out = &out_index[idx];
  card_id = make_id(idx, current_out->card_cnt);

#ifdef CUSTOM_CHEW_PREPROC
  CUSTOM_CHEW_PREPROC(o, attr);
#endif
  preproc_card(o);
  if (!raw_stage2_input)
    preproc_reftexts(o);
  detect_swindler(o);
  prints_card(o);
  word_card(o, note);
  analyse_card(o);
  string_card(o);
  card_card(o, attr, note);
  images_card(o);
#ifdef CUSTOM_CHEW_ATTRS
  CUSTOM_CHEW_ATTRS(o, attr);
#endif
  bwrite(current_out->card_attrs, attr, sizeof(*attr));
  admin_card(idx, current_out->card_cnt, info);

  PROGRESS(fetch_id, "chewer: %d cards (%d%%)",
	   fetch_id, (int)((float)fetch_id/fetch_num_ids*100));
  current_out->card_cnt++;
}

int
main(int argc, char **argv)
{
  log_init(argv[0]);
  setproctitle_init(argc, argv);
  if (cf_getopt(argc, argv, CF_SHORT_OPTS, CF_NO_LONG_OPTS, NULL) >= 0 ||
      optind < argc)
  {
    fputs("This program supports only the following command-line arguments:\n" CF_USAGE, stderr);
    exit(1);
  }

  out_init();
  preproc_init();
  word_init();
  string_init();
  params_init();
  analyse_init();
  admin_init();
  cards_init();
  byte *orig_directory = fn_directory;
  for (uns i=0; i<num_out_indices; i++)
    {
      struct out_index *out = &out_index[i];
      ITRACE("Opening index %s in %s", out->name, out->directory);
      current_out = out;
      fn_directory = out->directory;
      word_open(out);
      string_open(out);
      cards_open(out);
      prints_open(out);
      images_open(out);
    }
  fn_directory = orig_directory;

  log(L_INFO, "Chewing cards and creating indices with %d slices", num_slices);
  fetch_cards(chew_card);

  if (!raw_stage2_input)
    preproc_end();
  word_flush(1);
  string_flush();
  for (uns i=0; i<num_out_indices; i++)
    {
      struct out_index *out = &out_index[i];
      ITRACE("Closing index %s", out->name);
      current_out = out;
      fn_directory = out->directory;
      word_close(out);
      string_close(out);
      cards_close(out);
      prints_close(out);
      images_close(out);
      params_close(out);
    }
  cards_end();
  analyse_end();
  admin_end();

  if (num_dropped_cards)
    log(L_INFO, "%d cards did not belong to any subindex", num_dropped_cards);

  return 0;
}
