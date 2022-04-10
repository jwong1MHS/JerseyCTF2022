/*
 *	Sherlock Gatherer -- Decompressors for ZIP
 *
 *	(c) 1997 Martin Mares <mj@ucw.cz>
 *	(c) 2001--2003 Robert Spalek <robert@ucw.cz>
 *	(c) 2008 Jakub Horak <thement@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "gather/gather.h"
#include "ucw/ff-binary.h"

#include <string.h>
#include <zlib.h>

#define TRACE(x,y...) do { if (trace_decode) log(L_DEBUG, x,##y); } while (0)

enum {
	METH_STORED = 0,
	METH_DEFLATE = 8
};

struct zip_parser {
	u32 crc32;
	uns offset, length;
	int method;
	struct fastbuf *in, *out;
	z_stream *zs;
};

/*
 * Structure of zip file:
 *
 * [ local file header 1 ]
 * < data 1 >
 * [ local file header 2 ]
 * < data 2 >
 * [ local file header 3 ]
 * < data 3 >
 * ...
 * [ central directory ]
 * [ file header 1 ]
 * [ file header 2 ]
 * [ file header 3 ]
 * ...
 * [ end-of-central-directory ]
 */

#define ERR(zp,no,msg...) { zip_cleanup(zp); gerror(no,msg); }
#define SHORT(zp) ERR(zp, 2501, "Unexpected EOF")

static void zip_init(struct zip_parser *zp)
{
	bzero(zp, sizeof(*zp));
}

static void zip_cleanup(struct zip_parser *zp)
{
	DBG("cleaning up");
	if (zp->in)
		bclose(zp->in);
	if (zp->out) {
		gthis->temp = NULL;
		bclose(zp->out);
	}
	if (zp->zs)
		inflateEnd(zp->zs);
}

static void parse_zip_header(struct zip_parser *zp)
{
	byte hdr[30];
	uns n_ent, max, off;
	ucw_off_t flen;

	flen = bfilesize(zp->in);
	if (flen < 22)
		SHORT(zp);
	DBG("input file length=%ld", (long) flen);

	bseek(zp->in, -22, SEEK_END);
	breadb(zp->in, hdr, 22);

	/* Although zip end-of-central-directory has fixed length,
	 * it is allowed to add variable length zip comment. Because it's
	 * rarely used nowaydays, we ignore this feature.
	 */

	if (get_u32_le(hdr) != 0x06054b50)
		ERR(zp, 2500, "Missing end-of-central directory signature");

	if (get_u16_le(hdr + 8) != get_u16_le(hdr+10))
		ERR(zp, 2502, "Multipart archives not supported");

	n_ent = get_u16_le(hdr + 8);
	off = get_u32_le(hdr + 16);
	if (flen < n_ent * 46 + off)
		SHORT(zp);
	DBG("n_ent=%d, off=%d", n_ent, off);
	bsetpos(zp->in, off);

	max = 0;
	while (n_ent--) {
		byte fhdr[46];
		uns size;

		if (bread(zp->in, fhdr, sizeof(fhdr)) != sizeof(fhdr))
			SHORT(zp);
		if (get_u32_le(fhdr) != 0x02014b50)
			ERR(zp, 2500, "File header expected, got %08x instead", get_u32_le(fhdr));
		if (get_u16_le(fhdr + 6) > 20)
			ERR(zp, 2502, "Need unsupported features to extract: 0x%04x", get_u16_le(fhdr+6));
		if (!bskip(zp->in, get_u16_le(fhdr + 28) + get_u16_le(fhdr + 30) + get_u16_le(fhdr + 32)))
			SHORT(zp);

		/* find largest file not exceeding max_decode_size */
		size = get_u32(fhdr + 24);
		if (size <= max || size >= max_decode_size)
			continue;
		max = size;

		zp->offset = get_u32_le(fhdr + 42);
		zp->length = get_u32_le(fhdr + 20);
		zp->crc32 = get_u32_le(fhdr + 16);
		zp->method = get_u16_le(fhdr + 10);
	}

	DBG("max.len=%d, max.off=%d, max=%d", zp->length, zp->offset, max);
	if (!max)
		ERR(zp, 2500, "No suitable files found in archive");

	if (zp->offset + 30 > flen)
		SHORT(zp);
	bseek(zp->in, zp->offset, SEEK_SET);
	breadb(zp->in, hdr, 30);

	if (get_u32_le(hdr) != 0x04034b50)
		ERR(zp, 2500, "Local file header expected");

	if (!bskip(zp->in, get_u16_le(hdr+26)+get_u16_le(hdr+28)))
		SHORT(zp);
}

static uns crc_copy(struct fastbuf *f, struct fastbuf *t, uns len, uns *crc)
{
	u32 c = crc32(0L, Z_NULL, 0);
	uns l = len;

	while (l) {
		byte *fptr, *tptr;
		uns favail, tavail, n;

		favail = bdirect_read_prepare(f, &fptr);
		if (!favail)
			return len - l;

		tavail = bdirect_write_prepare(t, &tptr);
		n = MIN(l, favail);
		n = MIN(n, tavail);
		memcpy(tptr, fptr, n);
		c = crc32(c, tptr, n);
		bdirect_read_commit(f, fptr + n);
		bdirect_write_commit(t, tptr + n);
		l -= n;
	}
	*crc = c;
	return len - l;
}

static void zip_restore(struct zip_parser *zp)
{
	u32 crc = 0;

	TRACE("File is stored, no decompression needed");
	if (zp->length != crc_copy(zp->in, zp->out, zp->length, &crc))
		SHORT(zp);
	if (crc != zp->crc32)
		ERR(zp, 2501, "ZIP CRC error");
}

static void zip_inflate(struct zip_parser *zp)
{
	int cnt;
	z_stream zs;
	u32 crc;
	uns size = 0;

	bzero(&zs, sizeof(zs));
	if ((cnt = inflateInit2(&zs, -MAX_WBITS)) != Z_OK) {
		log(L_ERROR_R, "Inflate init error: %s", zs.msg);
		ERR(zp, 2501, "Inflate init error");
	}
	zp->zs = &zs;

	crc = crc32(0L, Z_NULL, 0);
	zs.avail_out = bdirect_write_prepare(zp->out, &zs.next_out);
	while (1) {
		byte *start = zs.next_out;
		if (!zs.avail_in) {
			if (zs.avail_in = bdirect_read_prepare(zp->in, &zs.next_in))
				bdirect_read_commit(zp->in, zs.next_in + zs.avail_in);
			DBG("Decode: read %d bytes", zs.avail_in);
		}
		cnt = inflate(&zs, Z_NO_FLUSH);
		crc = crc32(crc, start, (uns)(zs.next_out - start));
		if (cnt != Z_OK && cnt != Z_STREAM_END && cnt != Z_BUF_ERROR) {
			log(L_ERROR_R, "Inflate error: %s", zs.msg);
			ERR(zp, 2501, "Inflate error");
		}
		if (zs.total_out) {
			size += zs.total_out;
			bdirect_write_commit(zp->out, zs.next_out);
			DBG("Decode: written %d bytes", (int) zs.total_out);
			if (max_decode_size && size >= max_decode_size) {
				log(L_WARN_R, "Cutting %d bytes long unzipped file (maximum is %d)", size, max_decode_size);
				gobj_truncate();
				break;
			} else {
				zs.avail_out = bdirect_write_prepare(zp->out, &zs.next_out);
				zs.total_out = 0;
			}
		}
		if (cnt == Z_BUF_ERROR) {
			log(L_WARN_R, "Incomplete stream, only %d bytes unpacked", size);
			gobj_truncate();
			break;
		} else if (cnt == Z_STREAM_END) {
			DBG("ZIP CRC: %08x, computed CRC: %08x", zp->crc32, crc);
			if (crc != zp->crc32)
				ERR(zp, 2501, "ZIP CRC error");
			TRACE("Decode: CRC of the block is OK");
			break;
		}
	}
	zp->zs = NULL;
	inflateEnd(&zs);
	TRACE("Decode: complete (%d bytes long)", size);
}

int zip_parse(char **args UNUSED)
{
	struct zip_parser zp;

	zip_init(&zp);
	zp.in = fbmem_clone_read(gthis->contents);
	zp.out = gthis->temp = fbmem_create(16384);

	parse_zip_header(&zp);

	if (zp.method == METH_STORED)
		zip_restore(&zp);
	else if (zp.method == METH_DEFLATE)
		zip_inflate(&zp);
	else
		ERR(&zp, 2502, "Don't know how to unpack method %d", zp.method);

	bclose(zp.in);
	switch_content_type("zip");
	return 0;
}

