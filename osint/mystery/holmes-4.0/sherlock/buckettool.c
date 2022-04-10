/*
 *	Sherlock Library -- Bucket Manipulation Tool
 *
 *	(c) 2001--2005 Martin Mares <mj@ucw.cz>
 *	(c) 2004 Robert Spalek <robert@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

#include "sherlock/sherlock.h"
#include "sherlock/bucket.h"
#include "sherlock/object.h"
#include "ucw/fastbuf.h"
#include "ucw/ff-binary.h"
#include "ucw/ff-unicode.h"
#include "ucw/lfs.h"
#include "ucw/getopt.h"
#include "ucw/mempool.h"
#include "ucw/lizard.h"
#include "ucw/bbuf.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

static int verbose;
static struct mempool *pool;
static struct buck2obj_buf *buck_buf;

static void
help(void)
{
  fprintf(stderr, "\
Usage: buckettool [<options>] <command>\n\
\n\
Options:\n"
CF_USAGE
"\
-r\t\tDo not parse V33 buckets, but print the raw content\n\
-v\t\tBe verbose\n\
\nCommands:\n\
-b <path>\tBucket file name instead of the default one\n\
-c\t\tConcatenate and dump all buckets\n\
-d <obj>\tDelete bucket\n\
-f\t\tAudit bucket file structure (fsck)\n\
-F\t\tAudit and fix bucket file structure\n\
-i[<type>]\tInsert buckets separated by blank lines\n\
-l\t\tList all buckets\n\
-L\t\tList all buckets including deleted ones\n\
-q\t\tQuick check of bucket file consistency\n\
-s\t\tShake down bucket file (without updating other structures!!!)\n\
-x <obj>\tExtract bucket\n\
");
  exit(1);
}

static oid_t
parse_id(char *c)
{
  char *e;
  oid_t o = strtoul(c, &e, 16);
  if (e && *e)
    die("Invalid object ID: %s", c);
  return o;
}

static void
list(int full)
{
  struct obuck_context c;
  uns flags = full ? OBUCK_FULL : 0;

  bucket_open(0);
  if (obuck_find_first(&bucket_file, &c, flags))
    do
      {
	if (c.hdr.oid == OBUCK_OID_DELETED)
	  printf("DELETED  %6d\n", c.hdr.length);
	else
	  printf("%08x %6d %08x\n", c.hdr.oid, c.hdr.length, c.hdr.type);
      }
    while (obuck_find_next(&bucket_file, &c, flags));
  bucket_close();
}

static void
delete(char *id)
{
  oid_t oid = parse_id(id);
  bucket_open(1);
  obuck_delete(&bucket_file, oid);
  bucket_close();
}

static void
dump_oattrs(struct fastbuf *out, struct oattr *oa)
{
  for (; oa; oa = oa->next)
    for (struct oattr *a=oa; a; a = a->same)
      if (a->attr < OBJ_ATTR_SON)
	bprintf(out, "%c%s\n", a->attr, a->val);
      else
        {
	  bprintf(out, "(%c\n", a->attr - OBJ_ATTR_SON);
	  dump_oattrs(out, a->son->attrs);
	  bputs(out, ")\n");
	}
}

static void
dump_parsed_bucket(struct fastbuf *out, struct obuck_header *h, struct fastbuf *b)
{
  struct odes *o_hdr, *o_body;
  mp_flush(pool);
  o_hdr = obj_new(pool);
  o_body = obj_new(pool);
  if (buck2obj_parse(buck_buf, h->type, h->length, b, o_hdr, NULL, o_body, 1) < 0)
    bprintf(out, ".Cannot parse bucket %x of type %x and length %d: %m\n", h->oid, h->type, h->length);
  else
    {
      dump_oattrs(out, o_hdr->attrs);
      bputc(out, '\n');
      dump_oattrs(out, o_body->attrs);
    }
}

static void
extract(char *id)
{
  struct fastbuf *b, *out;
  struct obuck_context c;

  c.hdr.oid = parse_id(id);
  bucket_open(0);
  obuck_find_by_oid(&bucket_file, &c, 0);
  out = bfdopen_shared(1, 65536);
  if (verbose)
    bprintf(out, "### %08x %6d %08x\n", c.hdr.oid, c.hdr.length, c.hdr.type);
  b = obuck_fetch(&bucket_file, &c);
  if (c.hdr.type < BUCKET_TYPE_V33 || !buck_buf)
    bbcopy_slow(b, out, ~0U);
  else
    dump_parsed_bucket(out, &c.hdr, b);
  bclose(b);
  bclose(out);
  bucket_close();
}

static void
insert(byte *arg)
{
  struct fastbuf *b, *in;
  byte buf[4096];
  struct obuck_header h;
  byte *e;
  u32 type;
  bb_t lizard_buf, compressed_buf;

  bb_init(&lizard_buf);
  bb_init(&compressed_buf);
  if (!arg)
    type = BUCKET_TYPE_PLAIN;
  else if (sscanf(arg, "%x", &type) != 1)
    die("Type `%s' is not a hexadecimal number", arg);
  if (type < 10)
    type += BUCKET_TYPE_PLAIN;
  put_attr_set_type(type);

  in = bfdopen_shared(0, 4096);
  bucket_open(1);
  do
    {
      uns lizard_filled = 0;
      uns in_body = 0;
      b = NULL;
      while ((e = bgets(in, buf, sizeof(buf))))
	{
	  if (!buf[0])
	  {
	    if (in_body || type < BUCKET_TYPE_V30)
	      break;
	    in_body = 1;
	  }
	  if (!b)
	    b = obuck_create(&bucket_file);
	  if (in_body == 1)
	  {
	    if (type < BUCKET_TYPE_V33)
	      bputs(b, "\n");
	    else
	      bput_attr_separator(b);
	    in_body = 2;
	  }
	  else if (type <= BUCKET_TYPE_V33 || !in_body)
	  {
	    bput_attr(b, buf[0], buf+1, e-buf-1);
	  }
	  else
	  {
	    ASSERT(type == BUCKET_TYPE_V33_LIZARD);
	    uns want_len = lizard_filled + (e-buf) + 6 + LIZARD_NEEDS_CHARS;	// +6 is the maximum UTF-8 length
	    bb_grow(&lizard_buf, want_len);
	    byte *ptr = lizard_buf.ptr + lizard_filled;
	    ptr = put_attr(ptr, buf[0], buf+1, e-buf-1);
	    lizard_filled = ptr - lizard_buf.ptr;
	  }
	}
      if (in_body && type == BUCKET_TYPE_V33_LIZARD)
      {
	bputl(b, lizard_filled
#if 0	//TEST error resilience: write wrong length
	    +1
#endif
	    );
	bputl(b, adler32(lizard_buf.ptr, lizard_filled)
#if 0	//TEST error resilience: write wrong checksum
	    +1
#endif
	    );
	uns want_len = lizard_filled * LIZARD_MAX_MULTIPLY + LIZARD_MAX_ADD;
	bb_grow(&compressed_buf, want_len);
	want_len = lizard_compress(lizard_buf.ptr, lizard_filled, compressed_buf.ptr);
#if 0	//TEST error resilience: tamper the compressed data by removing EOF
	compressed_buf[want_len-1] = 1;
#endif
	bwrite(b, compressed_buf.ptr, want_len);
      }
      if (b)
	{
	  obuck_create_end(&bucket_file, b, type, &h);
	  printf("%08x %d %08x\n", h.oid, h.length, h.type);
	}
    }
  while (e);
  bb_done(&lizard_buf);
  bb_done(&compressed_buf);
  bucket_close();
  bclose(in);
}

static void
cat(void)
{
  struct obuck_header h;
  struct fastbuf *b, *out;
  byte buf[1024];

  bucket_open(0);
  out = bfdopen_shared(1, 65536);
  while (b = obuck_slurp_pool(&bucket_file, &h, OBUCK_OID_ANY))
    {
      bprintf(out, "### %08x %6d %08x\n", h.oid, h.length, h.type);
      if (h.type < BUCKET_TYPE_V33 || !buck_buf)
      {
	int lf = 1, l;
	while ((l = bread(b, buf, sizeof(buf))))
	{
	  bwrite(out, buf, l);
	  lf = (buf[l-1] == '\n');
	}
	if (!lf)
	  bprintf(out, "\n# <missing EOL>\n");
      }
      else
	dump_parsed_bucket(out, &h, b);
      bputc(out, '\n');
    }
  bclose(out);
  bucket_close();
}

static void
fsck(int fix)
{
  int fd, i;
  struct obuck_header h, nh;
  ucw_off_t pos = 0;
  ucw_off_t end;
  oid_t oid;
  u32 chk;
  int errors = 0;
  int fatal_errors = 0;

  fd = ucw_open(bucket_file_name, O_RDWR);
  if (fd < 0)
    die("Unable to open the bucket file %s: %m", bucket_file_name);
  for(;;)
    {
      if (pos >= ((ucw_off_t) OBUCK_OID_FIRST_SPECIAL << OBUCK_SHIFT))
	{
	  printf("*** bucket file exceeds maximum allowed size, giving up\n");
	  fatal_errors++;
	  goto finish;
	}
      oid = pos >> OBUCK_SHIFT;
      i = ucw_pread(fd, &h, sizeof(h), pos);
      if (!i)
	break;
      if (i != sizeof(h))
	printf("%08x  incomplete header\n", oid);
      else if (h.magic == OBUCK_INCOMPLETE_MAGIC)
	printf("%08x  incomplete file\n", oid);
      else if (h.magic != OBUCK_MAGIC)
	printf("%08x  invalid header magic\n", oid);
      else if (h.oid != oid && h.oid != OBUCK_OID_DELETED)
	printf("%08x  invalid header backlink\n", oid);
      else
	{
	  end = (pos + sizeof(h) + h.length + 4 + OBUCK_ALIGN - 1) & ~(ucw_off_t)(OBUCK_ALIGN - 1);
	  if (ucw_pread(fd, &chk, 4, end-4) != 4)
	    printf("%08x  missing trailer\n", oid);
	  else if (chk != OBUCK_TRAILER)
	    printf("%08x  mismatched trailer\n", oid);
	  else
	    {
	      /* OK */
	      pos = end;
	      continue;
	    }
	}
      errors++;
      end = pos;
      do
	{
	  if (pos - end > 0x10000000)
	    {
	      printf("*** skipped for too long, giving up\n");
	      fatal_errors++;
	      goto finish;
	    }
	  end += OBUCK_ALIGN;
	  if (ucw_pread(fd, &nh, sizeof(nh), end) != sizeof(nh))
	    {
	      printf("*** unable to find next header\n");
	      if (fix)
		{
		  printf("*** truncating file\n");
		  ucw_ftruncate(fd, pos);
		}
	      else
		printf("*** would truncate the file here\n");
	      goto finish;
	    }
	}
      while (nh.magic != OBUCK_MAGIC ||
	     (nh.oid != (oid_t)(end >> OBUCK_SHIFT) && nh.oid != OBUCK_OID_DELETED));
      printf("*** match at oid %08x\n", (uns)(end >> OBUCK_SHIFT));
      if (fix)
	{
	  h.magic = OBUCK_MAGIC;
	  h.oid = OBUCK_OID_DELETED;
	  h.length = end - pos - sizeof(h) - 4;
	  ucw_pwrite(fd, &h, sizeof(h), pos);
	  chk = OBUCK_TRAILER;
	  ucw_pwrite(fd, &chk, 4, end-4);
	  printf("*** replaced the invalid chunk by a DELETED bucket of size %d\n", (uns)(end - pos));
	}
      else
	printf("*** would mark %d bytes as DELETED\n", (uns)(end - pos));
      pos = end;
    }
 finish:
  close(fd);
  if (!fix && errors || fatal_errors)
    exit(1);
}

static int
shake_kibitz(struct obuck_header *old, oid_t new, byte *buck UNUSED)
{
  if (verbose)
    {
      printf("%08x -> ", old->oid);
      if (new == OBUCK_OID_DELETED)
	puts("DELETED");
      else
	printf("%08x\n", new);
    }
  return 1;
}

static void
shake(void)
{
  bucket_open(1);
  obuck_shakedown(&bucket_file, shake_kibitz);
  bucket_close();
}

static void
quickcheck(void)
{
  bucket_open(1);
  bucket_close();
}

int
main(int argc, char **argv)
{
  int i, op;
  char *arg = NULL;
  uns raw = 0;

  log_init(NULL);
  op = 0;
  while ((i = cf_getopt(argc, argv, CF_SHORT_OPTS "b:cd:fFi::lLqrsvx:", CF_NO_LONG_OPTS, NULL)) != -1)
    if (i == '?' || op)
      help();
    else if (i == 'v')
      verbose++;
    else if (i == 'r')
      raw++;
    else if (i == 'b')
      bucket_set_name(optarg);
    else
      {
	op = i;
	arg = optarg;
      }
  if (optind < argc)
    help();

  if (!raw)
  {
    pool = mp_new(1<<14);
    buck_buf = buck2obj_alloc();
  }
  switch (op)
    {
    case 'l':
      list(0);
      break;
    case 'L':
      list(1);
      break;
    case 'd':
      delete(arg);
      break;
    case 'x':
      extract(arg);
      break;
    case 'i':
      insert(arg);
      break;
    case 'c':
      cat();
      break;
    case 'f':
      fsck(0);
      break;
    case 'F':
      fsck(1);
      break;
    case 'q':
      quickcheck();
      break;
    case 's':
      shake();
      break;
    default:
      help();
    }
  if (buck_buf)
  {
    buck2obj_free(buck_buf);
    mp_delete(pool);
  }

  return 0;
}
