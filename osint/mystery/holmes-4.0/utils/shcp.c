/*
|| shcp.c - sending large files to multiple destinations
||
|| (c) 2005-2006 Vladimir Jelen <vladimir.jelen@netcentrum.cz>
||
|| How this works:
||
|| host1 <------- host2(if using reverse direction)
||       \
||        \-----> host3 -------> host4
||         \
||          \---> host5 -------> host6
||                      \
||                       \-----> host7
||
|| Each host start his child hosts(including hosts for reverse direction) with
|| fork and executing ssh, and each host open his files.
||
|| Comunication between hosts:
||
|| 1) send headers from client to server(pipe):
||	8 b		protocol version
|| 2) send headers from server to client(pipe):
||	8 b		protocol version
||	4 b		port
|| 3) connect client to server
|| 4) send headers from sender to receiver
||	4 b		file count
||
|| Then for each file we do:
||
|| 1) send headers from sender to receiver
||	4 b		file path length
||	x b		path
||	8 b		file size
|| 	MD5_HEX_SIZE b	random key
|| 2) transfer file over tcp connection
|| 3) send footers from sender to receiver
||	MD5_SIZE b	md5 of random key and adler of file
*/
#include "sherlock/sherlock.h"
#include "ucw/conf.h"
#include "ucw/getopt.h"
#include "ucw/lfs.h"
#include "ucw/md5.h"
#include "ucw/lizard.h"
#include "ucw/stkstring.h"
#include "ucw/clists.h"
#include "ucw/chartype.h"
#include "ucw/mempool.h"
#include "ucw/string.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <time.h>
#include <libgen.h>

#define PROTOCOL_V  "SHCP0003"

enum dsc_type {
  SHCP_SERVER, SHCP_CLIENT, SHCP_FILE
};

enum dsc_function {
  SHCP_SEND, SHCP_RECEIVE
};

struct argnode {
  cnode n;
  byte *s;
};

struct arglist {
  int count;
  int length;
  clist list;
  char **array;
  char *str;
};

struct host_info {
  char *user;
  char *host;
  char *path;
  char *file;

  struct arglist args;

  int ready;
  pthread_t thread;
  int retval;

  enum dsc_type type;
  enum dsc_function function;

  int descriptor;  // file or socket descriptor
  int pipedsc[2];

  struct sockaddr_in sa;
};

struct file_info {
  byte *host;
  byte *path;
  s64 size;
  u32 adler;
  byte key[MD5_HEX_SIZE];
  byte md5[MD5_SIZE];
};

static char *rundir = "run";
static char *ssh_program = "ssh";
static char *shcp_program = "bin/shcp";

static int buff_size = 65536;
static int tcpb_size = 65536;

static int read_pos;
static int write_pos = 1;
static byte *rw_buffer[2];

static int trace;
static int odirect;
static unsigned int limit;
static unsigned int limit_usec;
static unsigned int header_timeout = 60;
static unsigned int timeout = 60;
#define MAX_SSH_OPTIONS 20
static char *ssh_options[MAX_SSH_OPTIONS+1];
static struct timeval limit_init_time;
static s64 limit_init_size;
static int file_count;
static struct file_info *file_list;
static struct file_info *file_current;
static struct mempool *shcp_mp;
static pid_t process_pid;
static char hostname[255];

static pthread_mutex_t rw_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t read_cond = PTHREAD_COND_INITIALIZER;
static pthread_cond_t write_cond = PTHREAD_COND_INITIALIZER;

#define TRACE(a,b...) do { if(trace) log(L_DEBUG,"[%s:%d] " a,hostname,(int)process_pid,##b); } while(0)
#define LOG(a,b,c...) do { log(a,"[%s:%d] " b,hostname,(int)process_pid,##c); } while(0)
#define DIE(a,b...) do { die("[%s:%d] " a,hostname,(int)process_pid,##b); } while(0)

static void
add_ssh_option(byte *b)
{
  /* Using lists would be too clumsy, dynamic arrays would reallocate for each
   * element added. Scanning the array each time is harmless.  */
  uns i = 0;
  while (ssh_options[i])
    i++;
  if (i >= MAX_SSH_OPTIONS)
    die("Too many SSH options");
  ssh_options[i] = cf_strdup(b);
}

static struct cf_section shcp_config = {
  CF_ITEMS {
    CF_STRING("SshProgram", &ssh_program),
    CF_STRING("ShcpProgram", &shcp_program),
    CF_INT("SockBufferSize", &tcpb_size),
    CF_INT("FileBufferSize", &buff_size),
    CF_INT("Trace", &trace),
    CF_INT("DirectFileAccess", &odirect),
    CF_UNS("Limit", &limit),
    CF_STRING("RunDir", &rundir),
    CF_UNS("HeaderTimeout", &header_timeout),
    CF_UNS("Timeout", &timeout),
    CF_STRING_ARY("SshOption", ssh_options, MAX_SSH_OPTIONS),
    CF_END
  }
};

static void CONSTRUCTOR
shcp_config_init(void)
{
  cf_declare_section("Shcp", &shcp_config, 0);
}

/*------------------------------------------------------------------------------
|| Small helper functions
*/

static void
usage(byte *msg, ...)
{
  va_list args;
  va_start(args, msg);
  if (msg) {
    fprintf(stderr, "Invalid parameters: ");
    vfprintf(stderr, msg, args);
    fprintf(stderr, "\nTry 'shcp -h' for more information.\n");
  }
  else
    fprintf(stderr, "\
Usage: shcp [<options>] <src> <dest>\n\
\n\
Options:\n\
" CF_USAGE "\
-v, --verbose\t\tVerbose mode\n\
-d, --direct\t\tUse direct writes (bypass write-back caches)\n\
-l, --limit <speed>\tLimit transfer speed (in MB/s)\n\
-e, --rundir <path>\tDirectory we should change to on the remotes (default: run)\n\
-s, --ssh <path>\tProgram to use for connecting to the remotes (default: ssh)\n\
-c, --cmd <path>\tHow shcp is called at the remotes (default: bin/shcp)\n\
-t, --sockbuf <size>\tSet buffer size for socket writes (in bytes)\n\
-b, --diskbuf <size>\tSet buffer size for disk writes (must be a multiple of -t, in bytes)\n\
-a, --addopt <option>\tAdd option passed to ssh\n\
-h, --help\t\tPrint this help\n\
\n\
Source:\n\
  [host:]file\t\t\t\ttransfer a single local or remote file\n\
  { [host:]file1 [host:]file2 ... }\ttransfer multiple files\n\
\n\
Destination:\n\
  [host:](file|dir)\t\t\tcopy to a single destination\n\
  <dest> [<dest> ...]\t\t\tcopy to several destinations in parallel\n\
  host:(file|dir) { <dest> }\t\tchain transfer: copy to a single remote destination and\n\
\t\t\t\t\task the remote server to pass the received data to <dest>\n\
\n\
Examples:\n\
  bin/shcp sherlock5:db/objects db/objects\n\
  bin/shcp { index.new/cards index.new/card-attrs } sherlock11:index.new sherlock12:index.new\n\
  bin/shcp -vd -l 10 { index.new/main/* } sherlock11:index.new { sherlock12:index.new { sherlock13:index.new } }\n\
");
  exit(1);
}

static int
strcmp_null_safe(const char *s1, const char *s2)
{
  if(s1 == NULL && s2 == NULL)
    return 0;
  else if(s1 == NULL)
    return -1;
  else if(s2 == NULL)
    return 1;
  else
    return strcmp(s1, s2);
}

static void *
xvalloc(uns size)
{
  void *m = valloc(size);
  if(!m)
    DIE("Cannot allocate %d bytes of memory: %m", size);
  return m;
}

static void
limit_init(void)
{
  gettimeofday(&limit_init_time, NULL);
  limit_init_size = 0;
}

static void
limit_wait(s64 size)
{
  struct timeval limit_now;
  s64 estimate, limit_diff;

  limit_init_size += size;
  estimate = limit_init_size / limit_usec;
  gettimeofday(&limit_now, NULL);

  limit_diff = (limit_now.tv_sec - limit_init_time.tv_sec) * (s64)1000000 + (limit_now.tv_usec - limit_init_time.tv_usec);
  if(limit_diff < estimate) {
    struct timespec tm;
    tm.tv_sec = (estimate - limit_diff) / 1000000;
    tm.tv_nsec = ((estimate - limit_diff) % 1000000) * 1000;
    nanosleep(&tm, NULL);
  }
}

static double
dsec_diff(struct timeval a, struct timeval b)
{
  return (b.tv_sec - a.tv_sec) + (b.tv_usec - a.tv_usec) / (double)1000000;
}

static int
is_regular_file(const byte *name)
{
  ucw_stat_t st;

  if(ucw_stat(name, &st) < 0)
    return -1;
  else if(S_ISREG(st.st_mode))
    return 1;

  return 0;
}

static int
is_dir(const byte *name)
{
  ucw_stat_t st;

  if(!ucw_stat(name, &st)) {
    if(S_ISDIR(st.st_mode))
      return 1;
  }
  return 0;
}

static void
arglist_init(struct arglist *a)
{
  clist_init(&a->list);
  a->count = 0;
  a->length = 1;
}

static void
addarg(struct arglist *a, char *format, ...)
{
  va_list args;
  struct argnode *tmp;

  tmp = mp_alloc(shcp_mp, sizeof(struct argnode));

  va_start(args, format);
  tmp->s = mp_vprintf(shcp_mp, format, args);
  va_end(args);

  clist_add_tail(&a->list, &tmp->n);
  a->count++;
  a->length += strlen(tmp->s) + 1;
}

static void
arglist_finish(struct arglist *a)
{
  int i = 0;
  byte *s;
  struct argnode *tmp;

  a->array = xmalloc(sizeof(char *) * (a->count + 1));
  a->str = xmalloc(a->length);
  s = a->str;
  CLIST_WALK(tmp, a->list) {
    a->array[i] = tmp->s;
    s += sprintf(s, "%s ", tmp->s);
    i++;
  }
  a->array[i] = NULL;
}

static byte *
construct_path(byte *dir, byte *filename)
{
  byte *pos = strrchr(filename, '/');
  return mp_multicat(shcp_mp, dir, dir[strlen(dir) - 1] == '/' ? "" : "/", pos ? pos + 1 : filename, NULL);
}

static void
alarm_handler(int UNUSED whatsit)
{
  DIE("Timed out");
}

/*------------------------------------------------------------------------------
|| Read / write functions
*/

static void
sread(struct host_info *hi, void *buff, uns len)
{
  ASSERT(hi->type != SHCP_FILE);
  switch(careful_read(hi->pipedsc[0], buff, len)) {
    case -1:
      DIE("Error when reading pipe from %s: %m", hi->host);
    case 0:
      DIE("Error when reading pipe from %s: unexpected EOF", hi->host);
  }
}

static void
swrite(struct host_info *hi, void *buff, uns len)
{
  ASSERT(hi->type != SHCP_FILE);
  switch(careful_write(hi->pipedsc[1], buff, len)) {
    case -1:
      DIE("Error when writing pipe to %s: %m", hi->host);
    case 0:
      DIE("Error when writing pipe to %s: pipe is full!", hi->host);
  }
}

static u32
sread_u32(struct host_info *hi)
{
  u32 tmp;
  sread(hi, &tmp, 4);
  return ntohl(tmp);
}

static void
swrite_u32(struct host_info *hi, u32 num)
{
  num = htonl(num);
  swrite(hi, &num, 4);
}

static void
shcp_read_write(struct host_info *rh, struct host_info **wh, void *buff, int len)
{
  int wlen, rlen;
  void *rb, *wb;
  struct host_info **p;

  rb = wb = buff;
  wlen = len;

  while(wlen > 0) {
    alarm(timeout);
    if(rh->type == SHCP_FILE) {
      rlen = MIN(wlen, buff_size);
      if(read(rh->descriptor, rb, buff_size) < MIN(wlen, buff_size))
        DIE("Error reading file %s: %m", rh->path);
    }
    else {
      rlen = MIN(wlen, tcpb_size);
      if(careful_read(rh->descriptor, rb, rlen) < 1)
        DIE("Error reading socket from %s: %m", rh->host);
    }

    wlen -= rlen;
    rb += rlen;

    if(*wh) {
      while(rlen > 0) {
        for(p = wh; *p != NULL; p++) {
          if(careful_write((*p)->descriptor, wb, MIN(rlen, tcpb_size)) < 1)
            DIE("Error writing socket to %s: %m", (*p)->host);
        }
        rlen -= tcpb_size;
        wb += tcpb_size;
        if(limit)
          limit_wait(tcpb_size);
      }
    }
  }
  file_current->adler = adler32_update(file_current->adler, buff, len);
}

static void *
shcp_write_thread(void *ptr)
{
  int pos;
  s64 size = file_current->size;
  struct host_info *hi = ptr;

  while(size > 0) {
    pthread_mutex_lock(&rw_mutex);
    hi->ready = 1;
    pthread_cond_signal(&write_cond);
    pthread_cond_wait(&read_cond, &rw_mutex);
    pos = write_pos;
    pthread_mutex_unlock(&rw_mutex);

    if(careful_write(hi->descriptor, rw_buffer[pos], buff_size) <= 0)
      DIE("Error writing file %s: %m", file_current->path);

    size -= buff_size;
  }

  pthread_exit(NULL);
}

/*------------------------------------------------------------------------------
|| Socket / file opening functions
*/

static void
open_sock(struct host_info *sk)
{
  socklen_t len;
  struct hostent *he;

  sk->sa.sin_family = AF_INET;
  if((sk->descriptor = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
    DIE("Cannot open socket: %m");

  switch(sk->type) {
    case SHCP_SERVER:
      len = sizeof(sk->sa);
      sk->sa.sin_port = 0;
      sk->sa.sin_addr.s_addr = htonl(INADDR_ANY);
      if(bind(sk->descriptor, (struct sockaddr *)&(sk->sa), len) < 0)
        DIE("Cannot bind socket: %m");
      if(getsockname(sk->descriptor, (struct sockaddr *)&(sk->sa), &len) < 0)
        DIE("Cannot determine local address: %m");
      if(listen(sk->descriptor, 1) < 0)
        DIE("Cannot listen on a socket: %m");
      break;

    case SHCP_CLIENT:
      if(!(he = gethostbyname(sk->host)))
        DIE("Cannot get address of host %s: %s", sk->host, hstrerror(h_errno));
      memcpy(&sk->sa.sin_addr, he->h_addr, 4);
      if(connect(sk->descriptor, (struct sockaddr *)&(sk->sa), sizeof(sk->sa)) < 0)
        DIE("Cannot connect to server %s port %d': %m", sk->host, sk->sa.sin_port);
      break;

    default:
      ASSERT(0);
  }
}

static void
start_server(struct host_info *hi)
{
  int tmp;
  struct hostent *he;
  socklen_t len = sizeof(hi->sa);

  ASSERT(hi->type == SHCP_SERVER);

  if((tmp = accept(hi->descriptor, (struct sockaddr *)&(hi->sa), &len)) < 0)
    DIE("Cannot accept connection: %m");
  if(he = gethostbyaddr(&hi->sa.sin_addr.s_addr, sizeof(hi->sa.sin_addr.s_addr), AF_INET))
    hi->host = mp_strdup(shcp_mp, he->h_name);
  else {
    TRACE("Cannot get hostname of client: %s. Using ip address for logging.", hstrerror(h_errno));
    hi->host = mp_strdup(shcp_mp, inet_ntoa(hi->sa.sin_addr));
  }
  close(hi->descriptor);
  hi->descriptor = tmp;
}

static void
open_host(struct host_info *hi)
{
  hi->ready = 0;
  if(hi->type == SHCP_SERVER || hi->type == SHCP_CLIENT)
    open_sock(hi);
}

static void
open_file(struct host_info *hi, struct file_info *fi)
{
  int rw_flags;

  ASSERT(hi);
  ASSERT(fi);
  ASSERT(hi->type == SHCP_FILE);

  rw_flags = (hi->function == SHCP_SEND) ? O_WRONLY | O_CREAT | O_TRUNC : O_RDONLY;

  if(odirect)
#ifdef CONFIG_DIRECT_IO
    rw_flags |= O_DIRECT;
#else
    die("O_DIRECT not supported on this platform or direct I/O disabled by configure -CONFIG_DIRECT_IO");
#endif

  if(hi->function == SHCP_SEND) {
    if(is_dir(hi->path))
      hi->file = construct_path(hi->path, fi->path);
    else if(file_count > 1)
      DIE("Transferring multiple files, but target %s is not directory", hi->path);
    else
      hi->file = hi->path;
  }
  else
    hi->file = fi->path;

  if((hi->descriptor = ucw_open(hi->file, rw_flags, 0666)) < 0)
    DIE("Can't open file %s: %m", hi->file);
}

/*------------------------------------------------------------------------------
|| Writing / reading of headers / footers
*/

static void read_protocol_version(struct host_info *hi)
{
  byte proto[8], *our = PROTOCOL_V;
  sread(hi, proto, 8);
  for (uns i=0; i<8; i++)
    if (!Calnum(proto[i]))
      DIE("Invalid protocol identification received: %s", stk_hexdump(proto, 8));
  if (memcmp(proto, our, 8))
    DIE("Remote server uses an incompatible protocol: %.8s, need %s", proto, our);
}

static void
read_connection_header(struct host_info *hi)
{
  ASSERT(hi);
  ASSERT(hi->type == SHCP_CLIENT);

  swrite(hi, PROTOCOL_V, 8);
  read_protocol_version(hi);
  hi->sa.sin_port = sread_u32(hi);
}

static void
write_connection_header(struct host_info *hi)
{
  ASSERT(hi);
  ASSERT(hi->type == SHCP_SERVER);

  read_protocol_version(hi);
  swrite(hi, PROTOCOL_V, 8);
  swrite_u32(hi, hi->sa.sin_port);
}

static void
read_header(struct host_info *hi)
{
  ASSERT(hi);
  ASSERT(hi->function == SHCP_RECEIVE);

  if(hi->type != SHCP_FILE)
    file_count = sread_u32(hi);
}

static void
write_header(struct host_info *hi)
{
  ASSERT(hi);
  ASSERT(hi->function == SHCP_SEND);

  if(hi->type != SHCP_FILE)
    swrite_u32(hi, file_count);
}

static void
read_file_header(struct host_info *hi, struct file_info *fi)
{
  u32 length;
  byte keybin[MD5_SIZE];

  ASSERT(hi);
  ASSERT(fi);
  ASSERT(hi->function == SHCP_RECEIVE);

  fi->adler = adler32_update(0, NULL, 0);
  hi->path = fi->path;

  switch(hi->type) {
    case SHCP_FILE:
      open_file(hi, fi);
      if((fi->size = ucw_seek(hi->descriptor, 0, SEEK_END)) < 0)
        DIE("Can't get stats for file %s: %m", hi->path);
      ucw_seek(hi->descriptor, 0, SEEK_SET);
      randomkey(keybin, MD5_SIZE);
      mem_to_hex(fi->key, keybin, MD5_SIZE, MEM_TO_HEX_UPCASE);
      TRACE("Creating file headers - size: %lld bytes, key: %s", (long long) fi->size, fi->key);
      break;

    case SHCP_SERVER:
    case SHCP_CLIENT:
      sread(hi, &length, 4);
      fi->path = xmalloc(length);
      sread(hi, fi->path, length);
      sread(hi, &fi->size, 8);
      sread(hi, fi->key, MD5_HEX_SIZE);
      TRACE("Getting file headers - size: %lld bytes, key: %s", (long long) fi->size, fi->key);
      break;
  }
}

static void
write_file_header(struct host_info *hi, struct file_info *fi)
{
  u32 length;

  ASSERT(hi);
  ASSERT(fi);
  ASSERT(hi->function == SHCP_SEND);

  switch(hi->type) {
    case SHCP_FILE:
      open_file(hi, fi);
      pthread_create(&hi->thread, NULL, shcp_write_thread, (void*) hi);
      break;

    case SHCP_SERVER:
    case SHCP_CLIENT:
      length = strlen(fi->path) + 1;
      swrite(hi, &length, 4);
      swrite(hi, fi->path, length);
      swrite(hi, &fi->size, 8);
      swrite(hi, fi->key, MD5_HEX_SIZE);
      break;
  }
}

static void
read_file_footer(struct host_info *hi, struct file_info *fi)
{
  byte sig[MD5_SIZE];
  md5_context md5c;

  ASSERT(hi);
  ASSERT(fi);
  ASSERT(hi->function == SHCP_RECEIVE);

  md5_init(&md5c);
  md5_update(&md5c, fi->key, strlen(fi->key));
  md5_update(&md5c, (byte *) &fi->adler, sizeof(fi->adler));
  memcpy(fi->md5, md5_final(&md5c), MD5_SIZE);

  if(hi->type != SHCP_FILE) {
    sread(hi, sig, MD5_SIZE);
    if(memcmp(fi->md5, sig, MD5_SIZE))
      DIE("File checksum mismatch");
    TRACE("File footer read. Checksum is OK.");
  }
}

static void
write_file_footer(struct host_info *hi, struct file_info *fi)
{
  ASSERT(hi);
  ASSERT(fi);
  ASSERT(hi->function == SHCP_SEND);

  if(hi->type == SHCP_FILE) {
    pthread_join(hi->thread, NULL);
    ucw_ftruncate(hi->descriptor, fi->size);
    close(hi->descriptor);
  }
  else
    swrite(hi, fi->md5, MD5_SIZE);
}

/*------------------------------------------------------------------------------
|| Parse & fork host functions
*/

static byte *
parse_get_user(byte *s)
{
  char *pos, *tmp;

  if(pos = strchr(s, '@')) {
    *pos = '\0';
    tmp = mp_strdup(shcp_mp, s);
    *pos = '@';
    return tmp;
  }
  else
    return NULL;
}

static byte *
parse_get_host(byte *s)
{
  char *start, *pos, *tmp;

  if(!(start = strchr(s, '@')))
    start = s;
  else
    start++;

  if(pos = strchr(start, ':')) {
    *pos = '\0';
    tmp = mp_strdup(shcp_mp, start);
    *pos = ':';
    return tmp;
  }
  else
    return NULL;
}

static byte *
parse_get_path(byte *s)
{
  char *pos;

  if(pos = strchr(s, ':'))
    return mp_strdup(shcp_mp, pos + 1);

  return mp_strdup(shcp_mp, s);
}

static void
parse_host(struct host_info *hi, char *s, int is_receiver) {
  int i;

  hi->user = parse_get_user(s);
  hi->host = parse_get_host(s);
  hi->path = parse_get_path(s);

  if(hi->host) {
    arglist_init(&hi->args);
    addarg(&hi->args, ssh_program);
    for (uns i=0; ssh_options[i]; i++)
      addarg(&hi->args, "%s", ssh_options[i]);
    if(hi->user)
      addarg(&hi->args, "%s@%s", hi->user, hi->host);
    else
      addarg(&hi->args, "%s", hi->host);
    addarg(&hi->args, "cd");
    addarg(&hi->args, "%s", rundir);
    addarg(&hi->args, "&&");
    addarg(&hi->args, shcp_program);
    for (uns i=0; ssh_options[i]; i++) {
      addarg(&hi->args, "-a");
      addarg(&hi->args, "\"%s\"", ssh_options[i]);
    }
    if(is_receiver) {
      hi->type = SHCP_CLIENT;
      hi->function = SHCP_SEND;
      addarg(&hi->args, "-r");
    }
    else {
      hi->type = SHCP_CLIENT;
      hi->function = SHCP_RECEIVE;
      addarg(&hi->args, "{");
      for(i = 0; i < file_count; i++)
        addarg(&hi->args, "%s", file_list[i].path);
      addarg(&hi->args, "}");
      addarg(&hi->args, "-i");
    }
    if(trace)
      addarg(&hi->args, "-v");
    if(limit) {
      addarg(&hi->args, "-l");
      addarg(&hi->args, "%d", limit);
    }
    addarg(&hi->args, "-e");
    addarg(&hi->args, "%s", rundir);
    addarg(&hi->args, "-s");
    addarg(&hi->args, ssh_program);
    addarg(&hi->args, "-c");
    addarg(&hi->args, shcp_program);
    addarg(&hi->args, "-t");
    addarg(&hi->args, "%d", tcpb_size);
    addarg(&hi->args, "-b");
    addarg(&hi->args, "%d", buff_size);
    if(is_receiver)
      addarg(&hi->args, hi->path);
  }
  else {
    hi->type = SHCP_FILE;
    hi->function = SHCP_SEND;
  }
}

static struct host_info *
parse_hosts(int *host_c, int is_receiver, int send_back, int argc, char **argv) {
  int i, host_i, start, stop, pos, level, first_file;
  struct host_info *hl;

  send_back = send_back && 1;
  *host_c = host_i = 1 + send_back;
  first_file = pos = level = 0;
  file_count = 1;

  if(optind >= argc)
    usage("Too few arguments");
  start = stop = optind;

  // parse files
  if(!is_receiver) {
    if(!strcmp(argv[start], "{")) {
      for(i = ++start; i < argc; i++) {
        if(!strcmp(argv[i], "}"))
          break;
        if(!strcmp(argv[i], "{") || i >= argc - 1)
          usage("Invalid input file/host format");
      }
      stop = i;
      if((file_count = stop - start) < 1)
        usage("Invalid input file/host format");
    }

    file_list = xmalloc_zero(sizeof(struct file_info) * file_count);
    for(i = 0; i < file_count; i++) {
      file_list[i].host = parse_get_host(argv[start + i]);
      file_list[i].path = parse_get_path(argv[start + i]);
      if(strcmp_null_safe(file_list[i].host, file_list[0].host))
        usage("All files must be from same host");
      if(!file_list[i].host) {
        int tmp = is_regular_file(file_list[i].path);
        if(tmp < 0)
          DIE("Can't get stats for file %s: %m", file_list[i].path);
        else if(!tmp)
          DIE("File %s is not a regular file", file_list[i].path);
      }
    }
    first_file = start;
    start = stop + 1;
  }

  // parse hosts
  for(i = start; i < argc; i++) {
    if(!strcmp(argv[i], "{")) {
      level++;
      pos = i;
      if(i == start)
        usage("Invalid output file/host format");
    }
    else if(!strcmp(argv[i], "}")) {
      level--;
      if(level < 0 || i == (pos + 1))
        usage("Invalid output file/host format");
    }
    else if(level == 0)
      (*host_c)++;
  }

  if(level != 0 || *host_c < 2)
    usage("Invalid output file/host format");

  hl = xmalloc(sizeof(struct host_info) * (*host_c));
  for(i = 0; i < *host_c; i++)
    hl[i].host = NULL;

  pos = 0;

  for(i = start; i < argc; i++) {
    if(!strcmp(argv[i], "{")) {
      if(!hl[pos].host)
        usage("Invalid output file/host format - cannot use '{' after file without host");
      if(!level++)
        continue;
    }
    else if (!strcmp(argv[i], "}")) {
      if(!--level)
        continue;
    }
    else if(!level) {
      parse_host(&hl[host_i], argv[i], 1);
      pos = host_i++;
      continue;
    }
    addarg(&hl[pos].args, argv[i]);
  }

  if(send_back) {
    hl[0].type = SHCP_FILE;
    hl[0].function = SHCP_RECEIVE;
    hl[1].type = SHCP_SERVER;
    hl[1].function = SHCP_SEND;
  }
  else if(!is_receiver && file_list[0].host) {
    parse_host(&hl[0], argv[first_file], 0);
    xfree(file_list);
  }
  else {
    hl[0].type = is_receiver ? SHCP_SERVER : SHCP_FILE;
    hl[0].function = SHCP_RECEIVE;
  }

  return hl;
}

static void
fork_hosts(struct host_info *hl, int host_c)
{
  int i, j;
  int comm[2];

  for(i = 0; i < host_c; i++) {
    if(hl[i].type == SHCP_SERVER) {
      hl[i].pipedsc[0] = 0;
      hl[i].pipedsc[1] = 1;
    }
    else if(hl[i].type == SHCP_CLIENT) {
      if(socketpair(AF_UNIX, SOCK_STREAM, 0, comm))
        DIE("Cannot open socketpair: %m");

      switch(fork()) {
        case -1:
          DIE("Fork error: %m");
          break;

        // child
        case 0:
          close(0);
          close(1);
          for(j = 0; j < i; j++) {
            if(hl[j].type != SHCP_FILE) {
              close(hl[j].pipedsc[0]);
              close(hl[j].pipedsc[1]);
            }
          }

          dup2(comm[0], 0);
          dup2(comm[0], 1);
          close(comm[0]);
          close(comm[1]);

          arglist_finish(&hl[i].args);
          TRACE("Executing cmd: %s", hl[i].args.str);
          if(execvp(ssh_program, (char **)hl[i].args.array) == -1)
            DIE("Cannot execute ssh program %s: %m", ssh_program);
      }

      // parent
      hl[i].pipedsc[0] = comm[1];
      hl[i].pipedsc[1] = comm[1];
      close(comm[0]);
    }
  }
}

static void
connect_hosts(struct host_info **hi)
{
  for(; *hi != NULL; hi++) {
    if((*hi)->type == SHCP_CLIENT)
      read_connection_header(*hi);
    open_host(*hi);
    if((*hi)->type == SHCP_SERVER) {
      write_connection_header(*hi);
      start_server(*hi);
    }
  }
}

static int is_recv_callback(struct host_info *hi) { return (hi->function == SHCP_RECEIVE); }
static int is_send_callback(struct host_info *hi) { return (hi->function == SHCP_SEND); }
static int is_recv_file_callback(struct host_info *hi) { return (hi->type == SHCP_FILE && hi->function == SHCP_RECEIVE); }
static int is_send_file_callback(struct host_info *hi) { return (hi->type == SHCP_FILE && hi->function == SHCP_SEND); }
static int is_send_sock_callback(struct host_info *hi) { return (hi->type != SHCP_FILE && hi->function == SHCP_SEND); }

static struct host_info **
generate_host_info_list(struct host_info *hi, int hc, int (*ptr)(struct host_info *))
{
  int i, count;
  struct host_info **tmp;

  for(i = 0, count = 0; i < hc; i++)
    if(ptr(&hi[i]))
      count++;

  tmp = mp_alloc(shcp_mp, sizeof(struct host_info *) * (count + 1));

  for(i = 0, count = 0; i < hc; i++)
    if(ptr(&hi[i]))
      tmp[count++] = &hi[i];

  tmp[count] = NULL;

  return tmp;
}

static int
host_info_list_count(struct host_info **hl)
{
  int count = 0;
  struct host_info **p;

  for(p = hl; *p != NULL; p++)
    count++;
  return count;
}

/*------------------------------------------------------------------------------
|| Main
*/

int
main(int argc, char **argv)
{
  int i, opt, is_receiver, send_back, host_c, status, retval;
  s64 size;
  pid_t pid;
  byte exit_msg[EXIT_STATUS_MSG_SIZE];

  double speed, tdiff;
  struct host_info *host_list, **hi_recv_list, **hi_send_list, **hi_sock_list, **hi_file_recv_list, **hi_file_send_list, **p;
  struct timeval start_t, end_t;

  static const byte shortopts[] = CF_SHORT_OPTS "hrivl:ds:c:t:b:e:a:";
  static struct option longopts[] = {
    CF_LONG_OPTS
    { "addopt",		1, 0, 'a' },
    { "diskbuf",	1, 0, 'b' },
    { "cmd",		1, 0, 'c' },
    { "direct",		0, 0, 'd' },
    { "rundir",		1, 0, 'e' },
    { "help",		0, 0, 'h' },
    { "limit",		1, 0, 'l' },
    { "ssh",		1, 0, 's' },
    { "sockbuf",	1, 0, 't' },
    { "verbose",	0, 0, 'v' },
    { NULL,		0, 0, 0 }
  };

  process_pid = getpid();
  if(gethostname(hostname, 255))
    die("Cannot get host name: %m");
  shcp_mp = mp_new(4096);
  is_receiver = send_back = size = 0;
  log_init("shcp");

  while((opt = cf_getopt(argc, argv, shortopts, longopts, NULL)) >= 0) {
    switch(opt) {
      case 'r':
        is_receiver = 1;
        break;

      case 'i':
        send_back = 1;

      case 'v':
        trace = 1;
        break;

      case 'l':
        limit = atoi(optarg);
        break;

      case 'd':
        odirect = 1;
        break;

      case 's':
        ssh_program = optarg;
        break;

      case 'c':
        shcp_program = optarg;
        break;

      case 't':
        tcpb_size = atoi(optarg);
        break;

      case 'b':
        buff_size = atoi(optarg);
        break;

      case 'e':
        rundir = optarg;
        break;

      case 'a':
        add_ssh_option(optarg);
        break;

      case 'h':
      default:
        usage(NULL);
    }
  }

  if(buff_size % tcpb_size)
    usage("Buffer size must be a multiple of TCP buffer size");

  if(limit > 0) {
    limit_usec = (limit * 1024 * 1024) / 1000000;
    LOG(L_INFO, "Limiting transfer speed to %d MB/s (%d B/usec)", limit, limit_usec);
  }

  signal(SIGALRM, alarm_handler);
  alarm(header_timeout);

  host_list = parse_hosts(&host_c, is_receiver, send_back, argc, argv);
  fork_hosts(host_list, host_c);
  hi_recv_list = generate_host_info_list(host_list, host_c, is_recv_callback);
  hi_send_list = generate_host_info_list(host_list, host_c, is_send_callback);
  hi_sock_list = generate_host_info_list(host_list, host_c, is_send_sock_callback);
  hi_file_recv_list = generate_host_info_list(host_list, host_c, is_recv_file_callback);
  hi_file_send_list = generate_host_info_list(host_list, host_c, is_send_file_callback);
  connect_hosts(hi_send_list);
  connect_hosts(hi_recv_list);

  ASSERT(host_info_list_count(hi_recv_list) == 1);

  read_header(*hi_recv_list);
  for(p = hi_sock_list; *p != NULL; p++)
    write_header(*p);

  if((*hi_recv_list)->type != SHCP_FILE)
    file_list = xmalloc(sizeof(struct file_info) * file_count);

  setproctitle_init(argc, argv);

  for(i = 0; i < 2; i++)
    rw_buffer[i] = xvalloc(buff_size);

  TRACE("Starting transfer with values - tcpb_size: %d bytes, buff_size %d bytes", tcpb_size, buff_size);

  for(i = 0; i < file_count; i++) {
    gettimeofday(&start_t, NULL);
    file_current = &file_list[i];

    read_file_header(*hi_recv_list, file_current);
    for(p = hi_send_list; *p != NULL; p++)
      write_file_header(*p, file_current);

    if(limit)
      limit_init();

    if(*hi_file_recv_list)
      LOG(L_INFO, "Sending file %s (%lld bytes)", (*hi_file_recv_list)->file, (long long) file_current->size);
    if(trace)
      for(p = hi_file_send_list; *p != NULL; p++)
        LOG(L_INFO, "Receiving file %s (%lld bytes)", (*p)->file, (long long) file_current->size);

    size = file_current->size;
    while(size > 0) {
      int sz = MIN(buff_size, size);

      shcp_read_write(*hi_recv_list, hi_sock_list, rw_buffer[read_pos], sz);

      pthread_mutex_lock(&rw_mutex);

      for(p = hi_file_send_list; *p != NULL; p++) {
        if(!(*p)->ready) {
          pthread_cond_wait(&write_cond, &rw_mutex);
          p--;
          continue;
        }
        (*p)->ready = 0;
      }

      write_pos = read_pos;
      read_pos = !read_pos;

      pthread_cond_broadcast(&read_cond);
      pthread_mutex_unlock(&rw_mutex);

      size -= sz;
      setproctitle("shcp file %d/%d (%s): %.1f%%", i + 1, file_count, file_current->path, (1 - size / (double)file_current->size) * 100);
    }

    read_file_footer(*hi_recv_list, file_current);
    for(p = hi_send_list; *p != NULL; p++)
      write_file_footer(*p, file_current);

    gettimeofday(&end_t, NULL);

    tdiff = dsec_diff(start_t, end_t);
    speed = (file_current->size / (double)(1024 * 1024)) / tdiff;

    if(trace || host_info_list_count(hi_send_list) == host_info_list_count(hi_file_send_list)) {
      if(hi_file_recv_list[0] && hi_file_send_list[0])
        LOG(L_INFO, "Transceived %lld bytes in %.1fs (%.1f MB/s)", (long long) file_current->size, tdiff, speed);
      else if(hi_file_recv_list[0])
        LOG(L_INFO, "Sent %lld bytes in %.1fs (%.1f MB/s)", (long long) file_current->size, tdiff, speed);
      else if(hi_file_send_list[0])
        LOG(L_INFO, "Received %lld bytes in %.1fs (%.1f MB/s)", (long long) file_current->size, tdiff, speed);
    }
  }

  retval = 0;
  while((pid = wait(&status)) > 0) {
    if(format_exit_status(exit_msg, status)) {
      LOG(L_ERROR, "Subprocess %d %s", (int) pid, exit_msg);
      retval = 1;
    }
  }

  return retval;
}
