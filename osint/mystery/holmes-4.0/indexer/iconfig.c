/*
 *	Sherlock Indexer -- Configuration
 *
 *	(c) 2001--2006 Martin Mares <mj@ucw.cz>
 *	(c) 2002--2006 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/fastbuf.h"
#include "ucw/simple-lists.h"
#include "sherlock/attrset.h"
#include "indexer/indexer.h"

#include <stdio.h>
#include <string.h>

char *fn_directory;
char *fn_parameters;
char *fn_fingerprints;
char *fn_fp_splits;
char *fn_labels_by_id;
char *fn_attributes;
char *fn_checksums;
char *fn_links;
char *fn_urls;
char *fn_url_index;
char *fn_skel_urls;
char *fn_ref_texts;
char *fn_graph_obj;
char *fn_graph_skel;
char *fn_sites;
char *fn_labels;
char *fn_merges;
char *fn_signatures;
char *fn_matches;
char *fn_word_index;
char *fn_string_index;
char *fn_lexicon;
char *fn_lex_raw;
char *fn_lex_ordered;
char *fn_lex_words;
char *fn_lex_by_freq;
char *fn_stems;
char *fn_stems_ordered;
char *fn_references;
char *fn_string_map;
char *fn_string_hash;
char *fn_cards;
char *fn_card_attrs;
char *fn_notes;
char *fn_notes_skel;
char *fn_keywords;
char *fn_feedback_gath;
char *fn_card_prints;
char *link_attrs = "";
char *indexer_filter_name;
char *fn_lex_classes;
char *fn_blacklist;
char *fn_card_info;
char *fn_admin_export;
clist indexer_sources;
uns string_avg_bucket = 1024;
uns indexer_fb_size = 65536;
struct fb_params indexer_fb_params;
struct fb_params indexer_stream_params;
uns progress;
uns progress_screen;
uns progress_status_line;
uns sort_delete_src;
uns ref_max_length = 256;
uns ref_min_length = 1;
uns ref_max_count = 1;
uns matcher_signatures = 0;
uns matcher_context = 4;
uns matcher_min_words = 0;
uns matcher_threshold = 0;
uns matcher_passes = 3;
uns matcher_block = 64;
uns max_num_objects = ~0;
uns min_summed_size = 0;
uns frameset_to_redir;
uns default_weight = 128;
uns raw_stage2_input;
uns ic_connect_timeout;
uns ic_reply_timeout;
uns ic_retry_count;
uns ic_retry_delay;
uns ic_send_feedback;
clist subindices;
uns indexer_trace;
uns num_slices = 1;
uns indexer_threads = 1;
uns indexer_thread_stack_size;
uns reject_empty;
uns resolve_trace;
uns resolve_threads;
uns resolve_prefetch = 4;
uns resolve_batch_size = 4096;
double resolve_max_hash_density = 0.35;

/* Duplicate images */
char *fn_image_thumbnails;
/* Similar images */
char *fn_image_signatures_unsorted;
char *fn_image_signatures;
char *fn_image_clusters;
uns image_sig_max_cluster_count = 1000;

struct attr_set
  label_attr_set,
  link_attr_set,
  ref_link_attr_set,
  override_label_attr_set,
  override_body_attr_set,
  card_attr_set;

static clist label_attr, link_attr, ref_link_attr, override_label_attr, override_body_attr, card_attr;

static char *
iconfig_commit(void *ptr UNUSED)
{
  attr_set_commit(&label_attr_set, &label_attr);
  attr_set_commit(&link_attr_set, &link_attr);
  attr_set_commit(&ref_link_attr_set, &ref_link_attr);
  attr_set_commit(&override_label_attr_set, &override_label_attr);
  attr_set_commit(&override_body_attr_set, &override_body_attr);
  attr_set_commit(&card_attr_set, &card_attr);

  uns nr = 0;
  struct cnode *n, *n2;
  CLIST_WALK(n, subindices) {
    nr++;
    for (n2 = n->next; n2 != &subindices.head; n2 = n2->next) {
      if (!strcmp(((struct subindex *)n)->name, ((struct subindex *)n2)->name))
	return cf_printf("SubIndex %s defined more than once", ((struct subindex *)n)->name);
    }
  }
  if (nr > HARD_MAX_SUBINDICES)
    return "Too many subindices (see HARD_MAX_SUBINDICES)";

  return NULL;
}

static struct cf_section subindex_config = {
  CF_TYPE(struct subindex),
#define F(x)	PTR_TO(struct subindex, x)
  CF_ITEMS {
    CF_STRING("Name", F(name)),
    CF_UNS("TypeMask", F(type_mask)),
    CF_UNS("IDMask", F(id_mask)),
    CF_END
  }
#undef F
};

static char *
matcher_commit(void *ptr UNUSED)
{
  if (matcher_signatures % 2)
    return "Matcher.Signatures must be even";
  return NULL;
}

static char *
resolve_commit(void *ptr UNUSED)
{
  if (!(resolve_max_hash_density >= 0.1 && resolve_max_hash_density <= 0.9))
    return "Resolve.MaxHashDensity is out of the supported [0.1, 0.9] range";
  if (resolve_threads > resolve_prefetch)
    return "Resolve.Prefetch must not be lower than the number of threads";
  return NULL;
}

static struct cf_section iconfig = {
  CF_COMMIT(iconfig_commit),
  CF_ITEMS {
    CF_STRING("Directory", &fn_directory),
    CF_LIST("Source", &indexer_sources, &cf_string_list_config),
    CF_UNS("Trace", &indexer_trace),
    CF_STRING("Parameters", &fn_parameters),
    CF_STRING("Fingerprints", &fn_fingerprints),
    CF_STRING("FPSplits", &fn_fp_splits),
    CF_STRING("LabelsByID", &fn_labels_by_id),
    CF_STRING("Attributes", &fn_attributes),
    CF_STRING("Checksums", &fn_checksums),
    CF_STRING("Links", &fn_links),
    CF_STRING("URLList", &fn_urls),
    CF_STRING("URLIndex", &fn_url_index),
    CF_STRING("URLSkelList", &fn_skel_urls),
    CF_STRING("RefTexts", &fn_ref_texts),
    CF_STRING("ObjectGraph", &fn_graph_obj),
    CF_STRING("SkeletonGraph", &fn_graph_skel),
    CF_STRING("Sites", &fn_sites),
    CF_STRING("Labels", &fn_labels),
    CF_STRING("Merges", &fn_merges),
    CF_STRING("Signatures", &fn_signatures),
    CF_STRING("Matches", &fn_matches),
    CF_STRING("WordIndex", &fn_word_index),
    CF_STRING("StringIndex", &fn_string_index),
    CF_STRING("Lexicon", &fn_lexicon),
    CF_STRING("LexRaw", &fn_lex_raw),
    CF_STRING("LexOrdered", &fn_lex_ordered),
    CF_STRING("LexWords", &fn_lex_words),
    CF_STRING("LexByFreq", &fn_lex_by_freq),
    CF_STRING("LexClasses", &fn_lex_classes),
    CF_STRING("Stems", &fn_stems),
    CF_STRING("StemsOrdered", &fn_stems_ordered),
    CF_STRING("References", &fn_references),
    CF_STRING("StringMap", &fn_string_map),
    CF_STRING("StringHash", &fn_string_hash),
    CF_STRING("Cards", &fn_cards),
    CF_STRING("CardAttributes", &fn_card_attrs),
    CF_STRING("Notes", &fn_notes),
    CF_STRING("NotesSkel", &fn_notes_skel),
    CF_STRING("Keywords", &fn_keywords),
    CF_STRING("FeedbackGatherer", &fn_feedback_gath),
    CF_STRING("CardPrints", &fn_card_prints),
    CF_STRING("Blacklist", &fn_blacklist),
    CF_STRING("CardInfo", &fn_card_info),
    CF_STRING("AdminExport", &fn_admin_export),
    CF_LIST("LabelAttrs", &label_attr, &attr_set_cf),
    CF_LIST("OverrideLabelAttrs", &override_label_attr, &attr_set_cf),
    CF_LIST("OverrideBodyAttrs", &override_body_attr, &attr_set_cf),
    CF_LIST("CardAttrs", &card_attr, &attr_set_cf_sub),
    CF_LIST("LinkAttrs", &link_attr, &attr_set_cf_sub),
    CF_LIST("RefLinkTypes", &ref_link_attr, &attr_set_cf_sub),
    CF_STRING("Filter", &indexer_filter_name),
    CF_UNS("StringAvgBucket", &string_avg_bucket),
    CF_UNS("FileBufSize", &indexer_fb_size),
    CF_SECTION("FileAccess", &indexer_fb_params, &fbpar_cf),
    CF_SECTION("StreamFileAccess", &indexer_stream_params, &fbpar_cf),
    CF_UNS("Progress", &progress),
    CF_UNS("ProgressScreen", &progress_screen),
    CF_UNS("ProgressStatusLine", &progress_status_line),
    CF_UNS("SortDeleteSrc", &sort_delete_src),
    CF_UNS("RefMaxLength", &ref_max_length),
    CF_UNS("RefMinLength", &ref_min_length),
    CF_UNS("RefMaxCount", &ref_max_count),
    CF_UNS("MaxObjects", &max_num_objects),
    CF_UNS("MinSummedSize", &min_summed_size),
    CF_UNS("FramesetToRedir", &frameset_to_redir),
    CF_UNS("DefaultWeight", &default_weight),
    CF_UNS("RawStage2Input", &raw_stage2_input),
    CF_LIST("SubIndex", &subindices, &subindex_config),
    CF_UNS("Slices", &num_slices),
    CF_UNS("Threads", &indexer_threads),
    CF_UNS("ThreadStackSize", &indexer_thread_stack_size),
    CF_UNS("RejectEmpty", &reject_empty),
    CF_END
  }
};

static struct cf_section matcher_config = {
  CF_COMMIT(matcher_commit),
  CF_ITEMS {
    CF_UNS("Signatures", &matcher_signatures),
    CF_UNS("Context", &matcher_context),
    CF_UNS("MinWords", &matcher_min_words),
    CF_UNS("Threshold", &matcher_threshold),
    CF_UNS("Passes", &matcher_passes),
    CF_UNS("Block", &matcher_block),
    CF_END
  }
};

static struct cf_section resolve_config = {
  CF_COMMIT(resolve_commit),
  CF_ITEMS {
    CF_UNS("Trace", &resolve_trace),
    CF_UNS("Threads", &resolve_threads),
    CF_UNS("Prefetch", &resolve_prefetch),
    CF_UNS("BatchSize", &resolve_batch_size),
    CF_DOUBLE("MaxHashDensity", &resolve_max_hash_density),
    CF_END
  }
};

#ifdef CONFIG_SHEPHERD_PROTOCOL
static struct cf_section iconnect_config = {
  CF_ITEMS {
    CF_UNS("ConnectTimeout", &ic_connect_timeout),
    CF_UNS("ReplyTimeout", &ic_reply_timeout),
    CF_UNS("RetryCount", &ic_retry_count),
    CF_UNS("RetryDelay", &ic_retry_delay),
    CF_UNS("SendFeedback", &ic_send_feedback),
    CF_END
  }
};
#endif

#ifdef CONFIG_IMAGES
static struct cf_section images_config = {
  CF_ITEMS {
    CF_STRING("ImageSignaturesUnsorted", &fn_image_signatures_unsorted),
    CF_STRING("ImageSignatures", &fn_image_signatures),
    CF_STRING("ImageClusters", &fn_image_clusters),
    CF_STRING("ImageThumbnails", &fn_image_thumbnails),
    CF_UNS("SigMaxClusterCount", &image_sig_max_cluster_count),
    CF_END
  }
};
#endif

static void CONSTRUCTOR iconf_init(void)
{
  cf_declare_section("Indexer", &iconfig, 0);
  cf_declare_section("Matcher", &matcher_config, 0);
  cf_declare_section("Resolve", &resolve_config, 0);
#ifdef CONFIG_SHEPHERD_PROTOCOL
  cf_declare_section("IConnect", &iconnect_config, 0);
#endif
#ifdef CONFIG_IMAGES
  cf_declare_section("ImageIndexer", &images_config, 0);
#endif
}

char *
index_name(const char *file)
{
  if (!file)
    die("Missing indexer file name declarations in section Indexer");
  if (!fn_directory)
    die("Indexer.Directory not set");
  if (file[0] == '/')
    return (char *)file;
  return cf_printf("%s/%s", fn_directory, file);
}

int
index_name_defined(const char *file)
{
  return file && *file && strcmp(file, "-");
}
