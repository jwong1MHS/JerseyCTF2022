/*
 *  Sherlock PDF Parser: Encrypted Documents
 *
 *  (c) 2002--2003 Milan Vancura <milan@ucw.cz>
 *  (c) 2003--2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/md5.h"
#include "gather/gather.h"
#include "gather/format/pdf/pdf.h"

#include <string.h>

int encrypted;
byte decryptkey[21];
uns decryptkey_length;
struct rc4_state rc4_state, rc4_state_stream;

void
rc4_setup(struct rc4_state *s, u32 obj, u32 gen) {
    byte rc4key[16];
    int i, l;
    byte *m, j, k, a;

    TRACE(100, "Setting up RC4 decription for object %d,%d", obj, gen);

    m = decryptkey + decryptkey_length;
    m[0]=obj;
    m[1]=(obj >> 8);
    m[2]=(obj >> 16);
    m[3]=gen;
    m[4]=(gen >> 8);
    l = decryptkey_length + 5;
    md5_hash_buffer(rc4key, decryptkey, l);
    l = MIN(l, 16);

    s->x = 0;
    s->y = 0;
    m = s->m;

    for(i=0; i< 256; i++) m[i] = i;

    j=k=0;

    for(i=0; i<256; i++) {
        a=m[i];
        j=j+a+rc4key[k];
        m[i]=m[j];
	m[j]=a;
        if(++k>=l) k=0;
    }
}

int
rc4_conv(struct rc4_state *rs, void *dest, void *src, u32 n) {
    u32 i;
    byte *s=src, *d=dest;
    byte *m=rs->m, a, b;

    for(i=0;i<n;i++) {
	a=m[++rs->x];
	rs->y+=a;
	m[rs->x]=b=m[rs->y];
	m[rs->y]=a;
	d[i]=s[i]^m[(a+b) & 0xFF];
    }
    return n;
}

void
copy_and_decrypt(void *dest, void *src, uns n) {
    if((encrypted&3)==3) {
        struct rc4_state rs = rc4_state;
        rc4_conv(&rs, dest, src, n);
    } else
        memcpy(dest, src, n);
}

void
decrypt_init(OBJECT encrypt, OBJECT fileid) {
    OBJECT filter, frev, opass, upass, privs, fver, len;
    OBJ_GET encryptdict[]={
      {"Filter",&filter}, {"R",&frev}, {"O",&opass}, {"U",&upass}, {"P",&privs}, {"V",&fver},
      {"Length",&len}, {NULL,NULL}};
    int ver, rev, length;

    if (encrypt.type != OT_DICT)
      gerror(2220, "/Encrypt object of incorrect type");
    if (fileid.type != OT_ARRAY || fileid.value.a->obj.type != OT_STRING || fileid.value.a->obj.value.str.l != 16)
      gerror(2220, "/ID object of incorrect type");
    parse_dict(encrypt.value.d, encryptdict);
    if (filter.type != OT_NAME || memcmp(filter.value.s, "Standard", 9))
      gerror(2221, "Encrypt: Unknown filter");
    if ((fver.type != OT_INT && fver.type != OT_UNKNOWN) ||
	frev.type != OT_INT ||
	opass.type != OT_STRING ||
	upass.type != OT_STRING ||
	privs.type != OT_INT)
      gerror(2220, "Encrypt: malformed dictionary");

    ver = (fver.type == OT_UNKNOWN) ? 0 : fver.value.n;
    if (ver != 1 && ver != 2)
      gerror(2221, "Encrypt: version %d not supported", ver);
    if (ver == 2 && len.type == OT_INT)
      length = len.value.n;
    else
      length = 40;
    if (length < 32 || length > 128 || length % 8)
      gerror(2221, "Encrypt: length %d not supported", length);
    rev = frev.value.n;
    if (rev != 2 && rev != 3)
      gerror(2221, "Encrypt: revision %d not supported", rev);
    if (opass.value.str.l != 32 || upass.value.str.l != 32)
      gerror(2221, "Encrypt: unknown password length");
    TRACE(10,"(decrypt_init) Initializing encryption: length=%d, ver=%d, rev=%d, rights=%x", length, ver, rev, privs.value.n);

    if (respect_user_rights && (privs.value.n & ((rev == 3) ? 0x420 : 0x20) == 0))
      gerror(2224, "Text extraction forbidden");

    /* Key initialization for empty password (see PDF Reference, section 3.5.2) */
    /* FIXME: Should check validity of the password first */
    byte md5[16], temp[84];
    static const byte password_padding[32] = {
      0x28,0xbf,0x4e,0x5e,0x4e,0x75,0x8a,0x41,0x64,0x00,0x4e,0x56,0xff,0xfa,0x01,0x08,
      0x2e,0x2e,0x00,0xb6,0xd0,0x68,0x3e,0x80,0x2f,0x0c,0xa9,0xfe,0x64,0x53,0x69,0x7a };
    memcpy(temp, password_padding, 32);
    memcpy(temp+32, opass.value.str.i, 32);
    u32 priv32 = privs.value.n;
    temp[64] = priv32;
    temp[65] = priv32 >> 8;
    temp[66] = priv32 >> 16;
    temp[67] = priv32 >> 24;
    memcpy(temp+68, fileid.value.a->obj.value.str.i, 16);
    md5_hash_buffer(md5, temp, 84);
    if (rev == 3)
      for (uns i=0; i<50; i++)
	{
	  memcpy(decryptkey, md5, 16);
          md5_hash_buffer(md5, decryptkey, 16);
	}
    decryptkey_length = length/8;
    memcpy(decryptkey, md5, decryptkey_length);

    encrypted=3;
}
