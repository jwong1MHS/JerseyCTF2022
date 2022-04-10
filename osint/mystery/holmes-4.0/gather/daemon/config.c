/*
 *	Sherlock Gatherer Daemon -- Configuration
 *
 *	(c) 1997--2002 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "gather/daemon/gatherd.h"
#include "ucw/conf.h"

char *log_name;
uns max_bucket_file_size;
uns max_host_count;
uns max_threads = 8;
uns trace_threads = 0;
uns min_server_delay = 5;
uns rec_err_dly1 = 60;
uns rec_err_limit = 3;
uns rec_err_dly2 = 3600;
uns trace_refs = 0;
uns max_rec_err = ~0;
uns auto_enqueue_root = 0;
uns ignore_refs = 0;
uns queue_cache_size = 16;
uns soft_max_obj_count = ~0;
uns hard_max_obj_count = ~0;
uns urldb_cache_size = 16;
uns md5db_cache_size = 16;
uns max_run_time = 3600;
uns dump_full_objects = 0;
uns auto_sync = 0;
uns max_resolvers = 1;
uns host_hash_size = 256;
uns key_hash_size = 256;
uns doc_change_mix = 128;
uns trickster_err_prob;
uns trickster_step = 1;
uns allow_hard_shutdown = 0;
int compress_level;
char *host_file_name = "not/configured";
char *host_bak_name = "not/configured";
char *queue_file_name = "not/configured";
char *lock_name = "not/configured";
char *urldb_name = "not/configured";
char *md5db_name = "not/configured";

static struct cf_section gatherd_config = {
  CF_ITEMS {
    CF_STRING("LogFile", &log_name),
    CF_UNS("MaxBucketFileSize", &max_bucket_file_size),
    CF_UNS("MaxHostCount", &max_host_count),
    CF_UNS("MaxThreads", &max_threads),
    CF_UNS("TraceThreads", &trace_threads),
    CF_UNS("MinServerDelay", &min_server_delay),
    CF_UNS("RecErrDelay1", &rec_err_dly1),
    CF_UNS("RecErrDelay2", &rec_err_dly2),
    CF_UNS("RecErrLimit", &rec_err_limit),
    CF_UNS("TraceRefs", &trace_refs),
    CF_UNS("MaxRecErr", &max_rec_err),
    CF_UNS("AutoGoRoot", &auto_enqueue_root),
    CF_UNS("IgnoreRefs", &ignore_refs),
    CF_UNS("SoftMaxObj", &soft_max_obj_count),
    CF_UNS("HardMaxObj", &hard_max_obj_count),
    CF_UNS("QueueCacheSize", &queue_cache_size),
    CF_UNS("URLDbCacheSize", &urldb_cache_size),
    CF_UNS("MD5DbCacheSize", &md5db_cache_size),
    CF_UNS("MaxRunTime", &max_run_time),
    CF_UNS("DumpFullObjs", &dump_full_objects),
    CF_UNS("AutoSync", &auto_sync),
    CF_UNS("MaxResolvers", &max_resolvers),
    CF_UNS("HostHashSize", &host_hash_size),
    CF_UNS("KeyHashSize", &key_hash_size),
    CF_STRING("HostFile", &host_file_name),
    CF_STRING("HostFileBak", &host_bak_name),
    CF_STRING("QueueFile", &queue_file_name),
    CF_STRING("LockFile", &lock_name),
    CF_STRING("URLDBFile", &urldb_name),
    CF_STRING("MD5DBFile", &md5db_name),
    CF_UNS("DocChangeMix", &doc_change_mix),
    CF_UNS("TricksterErrProb", &trickster_err_prob),
    CF_UNS("TricksterStep", &trickster_step),
    CF_UNS("HardShutdown", &allow_hard_shutdown),
    CF_INT("Compress", &compress_level),
    CF_END
  }
};

static void CONSTRUCTOR
read_config(void)
{
  cf_declare_section("GatherD", &gatherd_config, 0);
}
