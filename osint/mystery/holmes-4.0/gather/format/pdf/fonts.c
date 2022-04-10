/*
 *  Sherlock PDF Parser: Fonts and encodings
 *
 *  (c) 2002--2003 Milan Vancura <milan@ucw.cz>
 *  (c) 2003--2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "ucw/prime.h"
#include "gather/format/pdf/pdf.h"
#include "gather/format/pdf/glyphs.h"

#include <stdio.h>

static struct font *fonttop;

static s32
so2i(OBJECT obj, byte lmin, byte lmax, byte *olen) {
    s32 ret=0;
    int i;

    if(obj.type!=OT_STRING || obj.value.str.l<lmin || obj.value.str.l>lmax)
	return -1;
    if(olen)
	*olen=obj.value.str.l;
    for(i=0;i<obj.value.str.l;i++)
	ret=ret*256+obj.value.str.i[i];
    return ret;
}

static struct unichar tempbuf[TEMPBUFLEN];
u16 *unistr[UNICODESTRINGCNT];
int unistrp;

static struct fencoding *
enc_hash(struct unichar *buf, uns len, int bytes) {
    struct unichar *tt,*bend=buf+len;
    struct fencoding *out;

    if(!len || bytes>2)
	return NULL;
    out=mp_alloc(global_pool,sizeof(struct fencoding));
    out->bytes=bytes;
    if(bytes==1) {
	int min=255,max=0;

	for(tt=buf;tt<bend;tt++) {
	    if(tt->fcode<min)
		min=tt->fcode;
	    if(tt->fcode>max)
		max=tt->fcode;
	}
	TRACE(200,"(enc_hash) MINMAX %d, %d",min,max);
	ASSERT(max<256);
	out->len=max-min+1;
	out->start=min;
	out->table=mp_alloc_zero(global_pool,out->len*sizeof(struct unichar));
	for(tt=buf;tt<bend;tt++)
	    (out->table)[tt->fcode-min]=*tt;
	return out;
    } else {			/* bytes==2 */
	int ts,skip,fc;
	struct unichar *ht;

	ts=next_table_prime(MAX(24,len*14/10));
	skip=ts/4;
	ASSERT(skip && skip!=ts);
	ht=mp_alloc_zero(global_pool,ts*sizeof(struct unichar));
	for(tt=buf;tt<bend;tt++) {
	    fc=tt->fcode % ts;
	    while(ht[fc].fcode)
		fc=(fc+skip)%ts;
	    ht[fc]=*tt;
	    TRACE(200,"(enc_hash) (2 bytes) Stored %d at %d(%d)",tt->unicode,fc,tt->fcode);
	}
	out->len=ts;
	out->start=skip;
	out->table=ht;
	return out;
    }
}

static int
parse_one_unichar(ARRAY_ENTRY *temp, struct unichar *tt) {
    s32 u;

    if(temp->obj.value.str.l<=2) {
	if(!in_unirange(u=so2i(temp->obj,1,2,NULL))) {
	    TRACE(200,"(uni_parse) Unicode <%.4x> out of range",u);
	    tt--;
	    return 1;
	}
	tt->unicode=u;
	TRACE(200,"(uni_parse) Unicode char <%.4x> -> <%.4x> stored",tt->fcode,tt->unicode);
    } else {
	int j,jj;
	if(unistrp>=UNICODESTRINGCNT || temp->obj.value.str.l>UNICODESTRMAXLEN) {
	    TRACE(0,"Max. count of ToUnicode strings or length of the current string exceeded application's limits");
	    tt--;
	    return 1;
	}
	tt->unicode=0xe000+unistrp;
	temp->obj.value.str.l-=temp->obj.value.str.l%2;
	unistr[unistrp]=mp_alloc_zero(global_pool,temp->obj.value.str.l+2);
	for(j=0,jj=0;j<temp->obj.value.str.l;j+=2)
	    if(in_unirange(u=temp->obj.value.str.i[j]*256+temp->obj.value.str.i[j+1]))
		unistr[unistrp][jj++]=u;
	unistr[unistrp][jj]=0;
	TRACE(200,"(uni_parse) Unicode string <%.4x> -> <%.4x> stored",tt->fcode,tt->unicode);
	unistrp++;
    }
    return 0;
}

static struct fencoding *
uni_parse(void) {
    ARRAY_ENTRY *start=NULL,*cur=NULL,*temp=NULL,*ta=NULL;
    struct fencoding *enc;

    int i;
    struct unichar *tt=tempbuf;
    s32 s,e;
    byte lcode;

    TRACE(200,"ToUnicode stream parsing");
    /*
     * Parse the input stream and store encoding table in tempbuf.
     */
    lcode=0;
    while(in_check_nschar(SP_ANYWCOMM)>=0) {
	if(!cur)
	    cur=start=mp_alloc_zero(page_pool,sizeof(ARRAY_ENTRY));
	else
	    cur=cur->next=mp_alloc_zero(page_pool,sizeof(ARRAY_ENTRY));
	cur->next=NULL;
	cur->obj=get_obj();
	if(cur->obj.type==OT_UTOK) { /* an operator means action */
	    TRACE(200,"ToUnicode op %s",cur->obj.value.s);
	    if(!memcmp(cur->obj.value.s,"endbfrange",11)) {
/*
 * possible syntax of one argument of bfrange:
 *
 * <from> <to> <UNI>  range from-to filled with values UNI..UNI+to-from
 * <from> <to> <uni_string>                     strings, last byte incremented
 * <from> <to> [ <UNI | uni_string> ... ]       values from the array
 */
		for(temp=start;temp && temp!=cur;temp=temp->next) {
		    if((s=so2i(temp->obj,lcode?lcode:1,lcode?lcode:2,&lcode))<0)
			return NULL;
		    if(!(temp=temp->next))
			return NULL;
		    if((e=so2i(temp->obj,lcode,lcode,&lcode))<0)
			return NULL;
		    if(!(temp=temp->next))
			return NULL;
		    TRACE(200,"(uni_parse) bfrange %d-%d (%d) type %d",s,e,e-s+1,temp->obj.type);
		    if(temp->obj.type==OT_STRING) {
			if(!temp->obj.value.str.l)
			    continue;
			if(temp->obj.value.str.l>1)
			    temp->obj.value.str.l-=temp->obj.value.str.l%2;
			for(i=0;i<e-s+1 && tt<tempbuf+TEMPBUFLEN;i++,tt++) {
			    tt->fcode=s+i;
			    if(parse_one_unichar(temp,tt))
				continue;
			    temp->obj.value.str.i[temp->obj.value.str.l-1]++;
			    TRACE(200,"added range part: %d %d",tt->fcode,tt->unicode);
			}
			if(i<e-s+1)
			    return NULL;
		    } else if(temp->obj.type==OT_ARRAY) {
			for(i=0,ta=temp->obj.value.a;tt<tempbuf+TEMPBUFLEN && i<e-s+1 && ta;i++,ta=ta->next,tt++) {
			    if(ta->obj.type!=OT_STRING || ta->obj.value.str.l<2)
				return NULL;
			    tt->fcode=s+i;
			    if(parse_one_unichar(ta,tt))
				continue;;
			}
			if(i<e-s+1)
			    return NULL;
		    } else
			return NULL;
		}
	    } else if(!memcmp(cur->obj.value.s,"endbfchar",10)) {
/*
 * possible syntax of one argument of fbchar:
 * <char> <UNI | uni_str>
 */

		for(temp=start;temp && temp!=cur && tt<tempbuf+TEMPBUFLEN;temp=temp->next,tt++) {
		    if((s=so2i(temp->obj,lcode?lcode:1,lcode?lcode:2,&lcode))<0)
			return NULL;
		    if(!(temp=temp->next))
			return NULL;
		    if(temp->obj.type!=OT_STRING)
			return NULL;
		    tt->fcode=s;
		    if(parse_one_unichar(temp,tt))
			continue;;
		}
	    }
	    cur=start=NULL;
	}
    }
/*
 * create encoding structure
 */
    enc=enc_hash(tempbuf,tt-tempbuf,lcode);
    if(enc) {
	TRACE(200,"End of parsing ToUnicode stream, %d entries",enc->len);
	HTRACE(200,enc->table,enc->len *sizeof(struct unichar), "Encoding table (%d), values %dbit",enc->len,enc->bytes*8);
    } else
	TRACE(200,"ERROR while parsing ToUnicode stream");
    return enc;
}

/* Five built-in encoding vectors */
static struct fencoding *
builtinenc(byte *enc) {
    if(!memcmp(enc,"StandardEncoding",17))
	return &StandardEncoding;
    if(!memcmp(enc,"MacRomanEncoding",17))
	return &MacRomanEncoding;
    if(!memcmp(enc,"WinAnsiEncoding",16))
	return &WinAnsiEncoding;
    if(!memcmp(enc,"PDFDocEncoding",15))
	return &PDFDocEncoding;
    if(!memcmp(enc,"MacExpertEncoding",18))
	return &MacExpertEncoding;
    return NULL;
}

/*
 * FIXME:
 * Apply the full algorithm published by Adobe at URL:
 * http://partners.adobe.com/asn/developer/typeforum/unicodegn.html
 */

static struct fencoding *
addencdiffs(struct unichar *tt, uns start, uns end, ARRAY_ENTRY *sdiff) {
    ARRAY_ENTRY *cur;
    byte code;
    byte *n;
    const struct glyph *g;
    struct fencoding *oenc;

    for(cur=sdiff;cur && cur->obj.type==OT_INT;) {
	code=cur->obj.value.n;
	TRACE(200,"(addencdiffs) Block sdiff at %d",code);
	for(cur=cur->next;cur && cur->obj.type==OT_NAME;cur=cur->next) {
	    TRACE(200,"(addencdiffs) Got name %s",cur->obj.value.s);
	    for(n=cur->obj.value.s;*n;n++)
		if(*n=='.')
		    *n=0;
	    g=is_glyph(cur->obj.value.s,0);
	    TRACE(200,"(addencdiffs) Name %s = glyph %.4x placed at %d",cur->obj.value.s,g ? g->unival : 0xfffff,code);
	    if(g) {
		tt[code].fcode=code;
		tt[code].unicode=g->unival;
		start=MIN(code,start);
		end=MAX(code,end);
	    }
	    code++;
	}
    }
    oenc=mp_alloc(global_pool,sizeof(struct fencoding));
    oenc->bytes=1;
    oenc->len=end-start+1;
    oenc->start=start;
    oenc->table=mp_alloc(global_pool,(end-start+1) * sizeof(struct unichar));
    memmove(oenc->table,tempbuf+start,(end-start+1) * sizeof(struct unichar));
    HTRACE(200,oenc->table,oenc->len *sizeof(struct unichar), "Encoding table (%d), values %dbit",oenc->len,oenc->bytes*8);
    return oenc;
}

/*
 * This code is as terrible as Adobe specification about Encoding vectors :-|
 * (see chapter 5 in PDF Reference Manual and additional notes placed in the
 * whole document)
 */

static struct fencoding *
enc_parse(OBJECT enc,OBJECT subtype,OBJECT basefont) {
    OBJECT baseenc, diffs;
    OBJ_GET encdict[]={{"BaseEncoding",&baseenc},{"Differences",&diffs},{NULL,NULL}};
    struct fencoding *benc;

    obj_deref(&enc);

    TRACE(200,"(enc_parse) Std 14?");
    /* 14 built-in fonts */
    if(enc.type==OT_UNKNOWN && subtype.type==OT_NAME && !memcmp(subtype.value.s,"Type1",6) && basefont.type==OT_NAME) {
	if(!strcmp(basefont.value.s,"Times-Roman") || !strcmp(basefont.value.s,"Times-Bold") ||
		!strcmp(basefont.value.s,"Times-Italic") || !strcmp(basefont.value.s,"Times-BoldItalic") ||
		!strcmp(basefont.value.s,"Helvetica") || !strcmp(basefont.value.s,"Helvetica-Bold") ||
		!strcmp(basefont.value.s,"Helvetica-Oblique") || !strcmp(basefont.value.s,"Helvetica-BoldOblique") ||
		!strcmp(basefont.value.s,"Courier") || !strcmp(basefont.value.s,"Courier-Bold") ||
		!strcmp(basefont.value.s,"Courier-Oblique") || !strcmp(basefont.value.s,"Courier-BoldOblique"))
	    return &StandardEncoding;
	if(!strcmp(basefont.value.s,"Symbol"))
	    return &SymbolEncoding;
	if(!strcmp(basefont.value.s,"ZapfDingbats"))
	    return NULL;    /* no unicode values */
	/*
	 * FIXME: In future versions, we may add font file parsing and setting
	 * up the encoding vector according this information.
	 */
	return NULL;
    }

    TRACE(200,"(enc_parse) Std Enc Name?");
    if(enc.type==OT_NAME)
	return builtinenc(enc.value.s);

    TRACE(200,"(enc_parse) Dict?");
    if(enc.type==OT_DICT) {
	parse_dict(enc.value.d,encdict);
	TRACE(200,"(enc_parse) BaseEnc %d, Diffs %d",baseenc.type,diffs.type);
	if(baseenc.type==OT_NAME) {
	    switch(diffs.type) {
		case OT_UNKNOWN:
		    return builtinenc(baseenc.value.s);
		case OT_ARRAY:
		    bzero(tempbuf,256);
		    if (!(benc=builtinenc(baseenc.value.s)))
		        return NULL;
		    memmove(tempbuf+benc->start,benc->table,benc->len*sizeof(struct unichar));
		    benc=addencdiffs(tempbuf,benc->start,benc->len,diffs.value.a);
		    return benc;
		default:
		    return NULL;
	    }
	} else {
	    TRACE(200,"(enc_parse) Diffs.type-OT_ARRAY=%d",diffs.type-OT_ARRAY);
	    if(diffs.type!=OT_ARRAY)
		return NULL;
	    TRACE(200,"(enc_parse) calling addencdiffs");
	    benc=addencdiffs(tempbuf,0,0,diffs.value.a);
	    if(benc)
		HTRACE(200,benc->table,benc->len *sizeof(struct unichar), "Encoding table (%d), values %dbit",benc->len,benc->bytes*8);
	    return benc;
	}
    }
    return NULL;
}

struct fencoding *
parse_fontenc(uns fobj) {
    OBJECT touni,encoding,subtype,basefont;
    OBJ_GET fontdict[]={{"ToUnicode",&touni},{"Encoding",&encoding},
	{"Subtype",&subtype},{"BaseFont",&basefont},{NULL,NULL}};
    struct font *t;
    OBJECT fn;

    for(t=fonttop;t;t=t->next) {
	if(t->obj==fobj)
	    break;
    }
    if(t)
	return t->enc;
    fn=get_i_obj(fobj);
    if(fn.type!=OT_DICT)
	return NULL;
    t=mp_alloc(global_pool,sizeof(struct font));
    parse_dict(fn.value.d,fontdict);
    if(touni.type==OT_OBJREF && check_i_stream(touni.value.n)==0) {
	TRACE(200,"ToUnicode object #%d",touni.value.n);
	stream_array[0]=touni.value.n;
	stream_array[1]=0;
	set_input_method(pdf_stream_in);
	t->enc=uni_parse();
	set_input_method(pdf_in);
	t->next=fonttop;
	t->obj=fobj;
	fonttop=t;
	return fonttop->enc;
    }
    TRACE(200,"Encoding object #%d",encoding.type==OT_OBJREF?(uns)encoding.value.n:fobj);
    t->enc=enc_parse(encoding,subtype,basefont);
    t->next=fonttop;
    t->obj=fobj;
    fonttop=t;
    return fonttop->enc;
}
