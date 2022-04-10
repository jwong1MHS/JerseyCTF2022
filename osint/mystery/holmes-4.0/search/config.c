/*
 *	Sherlock Search Engine -- Configuration
 *
 *	(c) 1997--2007 Martin Mares <mj@ucw.cz>
 *	(c) 2006 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/chartype.h"
#include "ucw/ipaccess.h"
#include "ucw/unicode.h"
#include "ucw/fastbuf.h"
#include "sherlock/conf.h"
#include "search/sherlockd.h"

#include <string.h>
#include <stdlib.h>

char *log_name;
char *status_name;
uns log_incoming;
uns log_rejected;
uns log_requests;
uns log_replies;
uns log_fetches;
uns port = 4444;
uns listen_queue = 16;
uns connection_timeout = 60;
char *control_password = "";
clist databases;
uns num_matches = 100;
uns max_matches = 1000;
uns cache_size = 1;
uns max_output_matches = ~0;
uns max_words;
uns max_phrases;
uns max_nears;
uns max_bools;
uns max_word_matches;
uns global_accent_mode;
uns wildcard_asterisks;
uns wildcard_qmarks;
uns max_wildcard_zone = ~0;
uns min_wildcard_prefix_len;
uns global_allow_approx;
uns global_context_chars;
uns global_meta_chars[16];
uns global_intervals;
uns highlight_substring;
char *url_attributes;
uns global_site_max;
uns global_url_max = 0x7fffffff;
uns global_max_redir_brack = 0x7fffffff;
uns global_max_cat_brack = 0x7fffffff;
uns global_partial_answers;
uns global_morphing;
uns global_spelling;
uns global_synonyming;
uns global_syn_expand;
uns doc_weight_scale = 1;
uns word_weight_scale = 1;
uns word_bonus;
uns default_word_types;
static uns default_wt = 0xff;
static uns default_mt = 0xffff;
static uns default_st = 0;
uns global_debug;
uns mem_map_zone_size = 16;
uns mem_map_elide_gaps = 16384;
uns mem_map_prefetch = 1;
uns prox_penalty;
int prox_limit;
uns query_watchdog;
uns global_sorting;
uns global_sort_reverse;
uns magic_complexes;
uns magic_merge_words;
uns magic_merge_classes;
uns magic_keyphrases;
uns magic_keyphrases_classes;
uns magic_keyphrases_string_types;
uns magic_keyphrases_bonus;
uns magic_near;
uns near_bonus_word;
uns near_penalty_gap;
uns near_bonus_connect;
uns near_min_weight;
uns near_max_weight;
uns misaccent_penalty;
uns blind_match_penalty;
uns morph_penalty;
uns stem_penalty;
uns synonymum_penalty;
uns spell_good_freq;
uns spell_min_len;
uns spell_margin;
uns spell_add_penalty;
uns spell_del_penalty;
uns spell_mod_penalty;
uns spell_xpos_penalty;
uns spell_accent_penalty;
uns spell_common_penalty;
uns spell_dwarf;
uns spell_dwarf_margin;
clist spell_common_pairs;
clist spell_phrases;
clist spell_kb_trans;
uns second_best_reduce = 1;
uns magic_merge_bonus;
clist access_list;
uns hydra_processes;
uns slice_threads;
uns max_image_sims;
uns image_sim_max_weight;
uns image_sim_slope;
uns fetch_threads;
uns filter_repeated_nonalpha = ~0U;
uns filter_repeated_alpha = ~0U;

static char *
cf_database_init(struct database *db)
{
  memset(db->meta_weights, -1, sizeof(db->meta_weights));
  for (uns i=0; i<16; i++)
    db->meta_weights[i][0] = 0;
  return NULL;
}

static char *
cf_database_commit(struct database *db)
{
  if (!db->name)
    return "Missing database name";
  if (!db->directory)
    return "Missing database directory";
  if (!db->parts)
    return "No parts defined";
  CLIST_FOR_EACH(struct database *, db2, databases)
    if (db2 != db && !strcmp(db->name, db2->name))
      return "Database of this name already defined";
  CF_JOURNAL_VAR(db->meta_weights);
  for (uns i=0; i<16; i++)
    for (uns j=0; j<4; j++)
      if (db->meta_weights[i][j] == ~0U && j > 0)
	db->meta_weights[i][j] = db->meta_weights[i][j-1];
  return NULL;
}

static char *
cf_default_sort_by(uns n UNUSED, char **w, void *p UNUSED)
{
  int x;
  char *c = w[0];
  CF_JOURNAL_VAR(global_sort_reverse);
  CF_JOURNAL_VAR(global_sorting);
  if (*c == '-')
    {
      global_sort_reverse = ~0U;
      c++;
    }
  else
    global_sort_reverse = 0;
  if ((x = lookup_custom_attr(c)) < 0)
    return "Unknown attribute";
  global_sorting = x;
  return NULL;
}

static char *
cf_search_init(void *ptr UNUSED)
{
  memset(global_meta_chars, -1, sizeof(global_meta_chars));
  return NULL;
}

static char *
cf_search_commit(void *ptr UNUSED)
{
  if ((default_wt || default_mt) && default_st)
    return "If DefaultStringTypes are nonempty, DefaultWordTypes and DefaultMetaTypes must both be empty";
  CF_JOURNAL_VAR(default_word_types);
  default_word_types = (default_mt << 16) | default_wt | (default_st ? 0x8000 | default_st : 0);
  if (clist_empty(&databases))
    return "Missing database defition";
  uns count = 0;
  CLIST_FOR_EACH(struct database *, db, databases)
    count++;
  if (count >= HARD_MAX_DATABASES)
    return "Too many databases defined (HARD_MAX_DATABASES exceeded)";
  uns max = 0, cnt = 0;
  for (uns i=0; i<16; i++)
    if (global_meta_chars[i] != ~0U)
      {
        max = MAX(max, global_meta_chars[i]);
	cnt++;
      }
  if (cnt)
    {
      CF_JOURNAL_VAR(global_meta_chars);
      for (uns i=0; i<16; i++)
        if (global_meta_chars[i] == ~0U)
          global_meta_chars[i] = max;
    }
  return NULL;
}

static struct cf_section word_weights_config = {};
static struct cf_section meta_weights_config = {};
static struct cf_section string_weights_config = {};
static struct cf_section meta_chars_config = {};

#define F(x) PTR_TO(struct database, x)
static struct cf_section database_config = {
  CF_TYPE(struct database),
  CF_INIT(cf_database_init),
  CF_COMMIT(cf_database_commit),
  CF_ITEMS {
    CF_STRING("Name", F(name)),
    CF_STRING("Directory", F(directory)),
    CF_BITMAP_LOOKUP("Parts", F(parts), ((const char * const []) {"words", "strings", "prints", "image-signatures", NULL })),
    CF_SECTION("WordWeights", F(word_weights[0]), &word_weights_config),
    CF_SECTION("MetaWeights", F(meta_weights[0][0]), &meta_weights_config),
    CF_SECTION("StringWeights", F(string_weights[0]), &string_weights_config),
    CF_UNS("IsOptional", F(is_optional)),
    CF_STRING_DYN("Blacklists", F(blacklists), CF_ANY_NUM),
    CF_END
  }
};
#undef F

static void
init_database_config(void)
{
  cf_generate_word_type_config(&word_weights_config, (byte*[]) { WORD_TYPE_USER_NAMES, NULL }, 1, 1);
  cf_generate_word_type_config(&string_weights_config, (byte*[]) { STRING_TYPE_USER_NAMES, NULL }, 1, 1);
  byte *mt_user_names[] = {META_TYPE_USER_NAMES, NULL};
  cf_generate_word_type_config(&meta_weights_config, mt_user_names, 4, 1);
  cf_generate_word_type_config(&meta_chars_config, mt_user_names, 1, 0);
}

static struct cf_section spell_pair_config = {
  CF_TYPE(struct spell_pair),
  CF_ITEMS {
    CF_USER("X", PTR_TO(struct spell_pair, x), &cf_type_unichar),
    CF_USER("Y", PTR_TO(struct spell_pair, y), &cf_type_unichar),
    CF_END
  }
};

#define F(x) PTR_TO(struct spell_phrase, x)
static struct cf_section spell_phrase_config = {
  CF_TYPE(struct spell_phrase),
  CF_ITEMS {
    CF_STRING("Src", F(src)),
    CF_STRING("Dest", F(dest)),
    CF_UNS("Penalty", F(penalty)),
    CF_END
  }
};
#undef F

#define F(x) PTR_TO(struct spell_kb_tran, x)
static struct cf_section spell_kb_tran_config = {
  CF_TYPE(struct spell_kb_tran),
  CF_ITEMS {
    CF_UNS("Penalty", F(penalty)),
    CF_LIST("Pairs", F(pairs), &spell_pair_config),
    CF_END
  }
};
#undef F

static struct cf_section search_config = {
  CF_INIT(cf_search_init),
  CF_COMMIT(cf_search_commit),
  CF_ITEMS {
    CF_STRING("LogFile", &log_name),
    CF_STRING("StatusFile", &status_name),
    CF_UNS("LogIncoming", &log_incoming),
    CF_UNS("LogRejected", &log_rejected),
    CF_UNS("LogRequests", &log_requests),
    CF_UNS("LogReplies", &log_replies),
    CF_UNS("LogFetches", &log_fetches),
    CF_UNS("Port", &port),
    CF_UNS("ListenQueue", &listen_queue),
    CF_LIST("Access", &access_list, &ipaccess_cf),
    CF_UNS("ConnTimeOut", &connection_timeout),
    CF_STRING("ControlPassword", &control_password),
    CF_LIST("Database", &databases, &database_config),
    CF_UNS("CacheSize", &cache_size),
    CF_UNS("NumMatches", &num_matches),
    CF_UNS("MaxMatches", &max_matches),
    CF_UNS("MaxOutputObjects", &max_output_matches),
    CF_UNS("MaxWords", &max_words),
    CF_UNS("MaxPhrases", &max_phrases),
    CF_UNS("MaxNears", &max_nears),
    CF_UNS("MaxBools", &max_bools),
    CF_UNS("MaxWordMatches", &max_word_matches),
    CF_UNS("AccentMode", &global_accent_mode),
    CF_UNS("WildcardAsterisks", &wildcard_asterisks),
    CF_UNS("WildcardQMarks", &wildcard_qmarks),
    CF_UNS("MaxWildcardZone", &max_wildcard_zone),
    CF_UNS("MinWildcardPrefix", &min_wildcard_prefix_len),
    CF_UNS("AllowApprox", &global_allow_approx),
    CF_UNS("DocWeightScale", &doc_weight_scale),
    CF_UNS("WordWeightScale", &word_weight_scale),
    CF_UNS("ContextChars", &global_context_chars),
    CF_SECTION("MetaChars", global_meta_chars, &meta_chars_config),
    CF_UNS("Intervals", &global_intervals),
    CF_UNS("HighlightSubstring", &highlight_substring),
    CF_STRING("URLAttributes", &url_attributes),
    CF_UNS("SiteMax", &global_site_max),
    CF_UNS("URLMax", &global_url_max),
    CF_UNS("MaxRedirectBrackets", &global_max_redir_brack),
    CF_UNS("MaxCatalogBrackets", &global_max_cat_brack),
    CF_UNS("PartialAnswers", &global_partial_answers),
    CF_BITMAP_LOOKUP("DefaultWordTypes", &default_wt, wt_names),
    CF_BITMAP_LOOKUP("DefaultMetaTypes", &default_mt, mt_names),
    CF_BITMAP_LOOKUP("DefaultStringTypes", &default_st, st_names),
    CF_UNS("Debug", &global_debug),
    CF_UNS("WordBonus", &word_bonus),
    CF_UNS("MemMapZone", &mem_map_zone_size),
    CF_UNS("MemMapElideGaps", &mem_map_elide_gaps),
    CF_UNS("MemMapPrefetch", &mem_map_prefetch),
    CF_UNS("ProxPenalty", &prox_penalty),
    CF_INT("ProxLimit", &prox_limit),
    CF_UNS("QueryWatchdog", &query_watchdog),
    CF_PARSER("DefaultSortBy", NULL, cf_default_sort_by, 1),
    CF_UNS("MagicComplexes", &magic_complexes),
    CF_UNS("MagicMergeWords", &magic_merge_words),
    CF_BITMAP_LOOKUP("MagicMergeMetaTypes", &magic_merge_classes, mt_names),
    CF_UNS("MagicKeyphrases", &magic_keyphrases),
    CF_BITMAP_LOOKUP("MagicKeyphrasesMetaTypes", &magic_keyphrases_classes, mt_names),
    CF_BITMAP_LOOKUP("MagicKeyphrasesStringTypes", &magic_keyphrases_string_types, st_names),
    CF_UNS("MagicKeyphrasesBonus", &magic_keyphrases_bonus),
    CF_UNS("MagicNear", &magic_near),
    CF_UNS("NearBonusWord", &near_bonus_word),
    CF_UNS("NearPenaltyGap", &near_penalty_gap),
    CF_UNS("NearBonusConnect", &near_bonus_connect),
    CF_UNS("NearMinWeight", &near_min_weight),
    CF_UNS("NearMaxWeight", &near_max_weight),
    CF_UNS("BlindMatchPenalty", &blind_match_penalty),
    CF_UNS("MisaccentPenalty", &misaccent_penalty),
    CF_UNS("StemPenalty", &stem_penalty),
    CF_UNS("MorphPenalty", &morph_penalty),
    CF_UNS("Morphing", &global_morphing),
    CF_UNS("Spelling", &global_spelling),
    CF_UNS("SpellGoodFreq", &spell_good_freq),
    CF_UNS("SpellMinLen", &spell_min_len),
    CF_UNS("SpellMargin", &spell_margin),
    CF_UNS("SpellAddPenalty", &spell_add_penalty),
    CF_UNS("SpellDelPenalty", &spell_del_penalty),
    CF_UNS("SpellModPenalty", &spell_mod_penalty),
    CF_UNS("SpellXposPenalty", &spell_xpos_penalty),
    CF_UNS("SpellAccentPenalty", &spell_accent_penalty),
    CF_UNS("SpellCommonPenalty", &spell_common_penalty),
    CF_UNS("SpellDwarf", &spell_dwarf),
    CF_UNS("SpellDwarfMargin", &spell_dwarf_margin),
    CF_UNS("SynonymumPenalty", &synonymum_penalty),
    CF_UNS("Synonyming", &global_synonyming),
    CF_UNS("SynExpand", &global_syn_expand),
    CF_UNS("SecondBestReduce", &second_best_reduce),
    CF_LIST("SpellCommonPairs", &spell_common_pairs, &spell_pair_config),
    CF_LIST("SpellPhrases", &spell_phrases, &spell_phrase_config),
    CF_LIST("SpellKBTranslations", &spell_kb_trans, &spell_kb_tran_config),
    CF_UNS("MagicMergeBonus", &magic_merge_bonus),
    CF_UNS("HydraProcesses", &hydra_processes),
    CF_UNS("SliceThreads", &slice_threads),
    CF_UNS("MaxImageSims", &max_image_sims),
    CF_UNS("ImageSimMaxWeight", &image_sim_max_weight),
    CF_UNS("ImageSimSlope", &image_sim_slope),
    CF_UNS("FetchThreads", &fetch_threads),
    CF_UNS("FilterRepeatedNonAlpha", &filter_repeated_nonalpha),
    CF_UNS("FilterRepeatedAlpha", &filter_repeated_alpha),
    CF_END
  }
};

static void CONSTRUCTOR ss_conf_init(void)
{
  init_database_config();
  cf_declare_section("Search", &search_config, 0);
}
