/*
 *	Sherlock Gatherer -- Decompressors for GZIP and Deflate
 *
 *	(c) 1997 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2003 Robert Spalek <robert@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "gather/gather.h"
#include "ucw/ff-binary.h"

#include <string.h>
#include <zlib.h>

#define TRACE(x,y...) do { if (trace_decode) log(L_DEBUG, x,##y); } while (0)
#define XTRACE(x,y...) do { if (trace_decode > 1) log(L_DEBUG, x,##y); } while (0)

/* gzip flag byte */
#define ASCII_FLAG   0x01 /* bit 0 set: file probably ascii text */
#define HEAD_CRC     0x02 /* bit 1 set: header CRC present */
#define EXTRA_FIELD  0x04 /* bit 2 set: extra field present */
#define ORIG_NAME    0x08 /* bit 3 set: original file name present */
#define COMMENT      0x10 /* bit 4 set: file comment present */
#define RESERVED     0xE0 /* bits 5..7: reserved */

/*
 * Check the gzip header of a gz_stream opened for reading. Set the stream
 * mode to transparent if the gzip magic header is not present; set s->err
 * to Z_DATA_ERROR if the magic header is present but the rest of the header
 * is incorrect.
 */
static int
check_gzip_header(struct fastbuf *in)
{
	int method; /* method byte */
	int flags;  /* flags byte */
	uns len;
	int c = 0;

	/* Check the gzip magic header */
	if (bgetc(in) != 0x1f || bgetc(in) != 0x8b)
		return Z_DATA_ERROR;
	method = bgetc(in);
	flags = bgetc(in);
	if (method != Z_DEFLATED || (flags & RESERVED) != 0)
		return Z_DATA_ERROR;

	/* Discard time, xflags and OS code: */
	for (len = 0; len < 6; len++) c = bgetc(in);

	if ((flags & EXTRA_FIELD) != 0) { /* skip the extra field */
		len = bgetc(in);
		len += bgetc(in) << 8;
		/* len is garbage if EOF but the loop below will quit anyway */
		while (len-- != 0 && (c = bgetc(in)) >= 0) ;
	}
	if ((flags & ORIG_NAME) != 0) { /* skip the original file name */
		byte url[MAX_URL_SIZE];
		uns i = 0;
		while ((c = bgetc(in)) != 0 && c >= 0)
			if (i<MAX_URL_SIZE)
				url[i++] = c;
		url[i] = 0;
		gthis->file_name = gstrdup(url);
		gthis->content_type = NULL;	/* needed to clean the guess based on former file_name */
		TRACE("Changing filename after ungziping to %s", url);
	}
	if ((flags & COMMENT) != 0) {   /* skip the .gz file comment */
		while ((c = bgetc(in)) != 0 && c >= 0) ;
	}
	if ((flags & HEAD_CRC) != 0) {  /* skip the header crc */
		bgetw(in);
	}
	return c < 0 ? Z_DATA_ERROR : Z_OK;
}

#define	ERR(nr, txt)	do { err=nr; err_msg=txt; goto bye; } while(0)

int
gzip_parse(char **args UNUSED)
{
	struct fastbuf *in, *out;
	z_stream zs;
	int cnt;
	int err = 0;
	char *err_msg = NULL;
	uns size = 0;
	u32 crc;
	int eof = 0;

	in = fbmem_clone_read(gthis->contents);
	out = gthis->temp = fbmem_create(16384);

	bzero(&zs, sizeof(zs));
	if ((cnt = inflateInit2(&zs, -MAX_WBITS)) != Z_OK)
	{
		err = 2500;
		log(L_ERROR_R, "Inflate init error: %s", zs.msg);
		err_msg = "Inflate init error";
		goto bye0;
	}
	if ((cnt = check_gzip_header(in)) != Z_OK)
		ERR(2500, "Gzip header error");
	crc = crc32(0L, Z_NULL, 0);
	TRACE("Decode: Readed gzip header");

	zs.avail_out = bdirect_write_prepare(out, &zs.next_out);
	while (!eof)
	{
		byte *start = zs.next_out;
		if (!zs.avail_in)
		{
			if (zs.avail_in = bdirect_read_prepare(in, &zs.next_in))
				bdirect_read_commit(in, zs.next_in + zs.avail_in);
			XTRACE("Decode: read %d bytes", zs.avail_in);
		}
		cnt = inflate(&zs, Z_NO_FLUSH);
		crc = crc32(crc, start, (uns)(zs.next_out - start));
		if (cnt != Z_OK && cnt != Z_STREAM_END && cnt != Z_BUF_ERROR)
		{
			log(L_ERROR_R, "Inflate error: %s", zs.msg);
			ERR(2501, "Inflate error");
		}
		if (zs.total_out)
		{
			size += zs.total_out;
			bdirect_write_commit(out, zs.next_out);
			XTRACE("Decode: written %d bytes", (int) zs.total_out);
			if (max_decode_size && size >= max_decode_size)
			{
				log(L_WARN_R, "Cutting %d bytes long ungziped file (maximum is %d)", size, max_decode_size);
				eof = 2;
			}
			else
			{
				zs.avail_out = bdirect_write_prepare(out, &zs.next_out);
				zs.total_out = 0;
			}
		}
		if (cnt == Z_BUF_ERROR)
		{
incomplete_stream:
			log(L_WARN_R, "Incomplete stream, only %d bytes unpacked", size);
			eof = 2;
		}
		else if (cnt == Z_STREAM_END)
		{
			u32 crc1;
			int i;
			in->bptr -= zs.avail_in;
			zs.avail_in = 0;
			crc1 = bgetc(in);
			crc1 += bgetc(in) << 8;
			crc1 += bgetc(in) << 16;
			crc1 += bgetc(in) << 24;
			for (i=0; i<4; i++)
				if (bgetc(in) < 0)
					goto incomplete_stream;
			if (crc1 != crc)
				ERR(2501, "Gzip CRC error");
			inflateReset(&zs);
			crc = crc32(0L, Z_NULL, 0);
			TRACE("Decode: CRC of the block is OK");
			if (check_gzip_header(in) != Z_OK)
				eof = 1;
		}
	}
	TRACE("Decode: complete (%d bytes long)", size);
	if (eof == 2)
		gobj_truncate();

bye:
	inflateEnd(&zs);
bye0:
	bclose(in);
	if (!err)
	{
		switch_content_encoding();
		return 0;
	}
	else
		gerror(err, err_msg ? : "Unknown error");
}

int
deflate_parse(char **args UNUSED)
{
	struct fastbuf *in, *out;
	z_stream zs;
	int cnt;
	int err = 0;
	char *err_msg = NULL;
	uns size = 0;
	int eof = 0;

	in = fbmem_clone_read(gthis->contents);
	out = gthis->temp = fbmem_create(16384);

	bzero(&zs, sizeof(zs));
	if ((cnt = inflateInit2(&zs, -MAX_WBITS)) != Z_OK)
	{
		err = 2500;
		log(L_ERROR_R, "Inflate init error: %s", zs.msg);
		err_msg = "Inflate init error";
		goto bye0;
	}
	TRACE("Decode: Readed deflate header");

	zs.avail_out = bdirect_write_prepare(out, &zs.next_out);
	while (!eof)
	{
		if (!zs.avail_in)
		{
	  		if (zs.avail_in = bdirect_read_prepare(in, &zs.next_in))
				bdirect_read_commit(in, zs.next_in + zs.avail_in);
			XTRACE("Decode: read %d bytes", zs.avail_in);
		}
		cnt = inflate(&zs, Z_NO_FLUSH);
		if (cnt != Z_OK && cnt != Z_STREAM_END && cnt != Z_BUF_ERROR)
		{
			log(L_ERROR_R, "Inflate error: %s", zs.msg);
			ERR(2501, "Inflate error");
		}
		if (zs.total_out)
		{
			size += zs.total_out;
			bdirect_write_commit(out, zs.next_out);
			XTRACE("Decode: written %d bytes", (int) zs.total_out);
			if (max_decode_size && size >= max_decode_size)
			{
				log(L_WARN_R, "Cutting %d bytes long inflated file (maximum is %d)", size, max_decode_size);
				eof = 2;
			}
			else
			{
				zs.avail_out = bdirect_write_prepare(out, &zs.next_out);
				zs.total_out = 0;
			}
		}
		if (cnt == Z_BUF_ERROR)
		{
			log(L_WARN_R, "Incomplete stream, only %d bytes unpacked", size);
			eof = 2;
		}
		else if (cnt == Z_STREAM_END)
		{
			TRACE("Decode: correct end of stream");
			eof = 1;
		}
	}
	TRACE("Decode: complete (%d bytes long)", size);
	if (eof == 2)
		gobj_truncate();

bye:
	inflateEnd(&zs);
bye0:
	bclose(in);
	if (!err)
	{
		switch_content_encoding();
		return 0;
	}
	else
		gerror(err, err_msg ? : "Unknown error");
}
