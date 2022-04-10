/*
 *  Sherlock Gatherer: PDF parser
 *
 *  (c) 2002--2003 Milan Vancura <milan@ucw.cz>
 *  (c) 2003--2004 Martin Mares <mj@ucw.cz>
 */

/*
 * TODO:
 *
 *    + split the whole jumbo file
 *    + out_pdftext_string() - text of bookmarks, anotations etc. can be coded
 *      in Unicode (PLRM->Syntax->Common data structures->PDF Text string)
 *    + code cleanups: better order of functions in a file, create lookup
 *      function for searching in a table of strings etc.
 *    + even the stream filters we implement can have parameters
 *    + format version can be overriden in Root->Version [3.4.1]
 *    + PDF-1.5: object streams and xref streams [3.4.7]
 *    + sf_func: accept abbreviations according to [H9]
 */

/*
 * Warning: All tracing messages (Trace 10000) can cause HUGE log files.
 *          Example: Log of parsing the PDF Reference Manual > 0.5GB
 *          => use appropriate Trace level &&/|| raise Trace level for debugged
 *          parts of code only
 */

#undef LOCAL_DEBUG

#include "sherlock/sherlock.h"
#include "sherlock/index.h"
#include "sherlock/math.h"
#include "ucw/conf.h"
#include "ucw/chartype.h"
#include "ucw/mempool.h"
#include "ucw/hashfunc.h"
#include "ucw/ff-unicode.h"
#include "ucw/unicode.h"
#include "charset/unicat.h"
#include "gather/gather.h"
#include "gather/format/pdf/pdf.h"

#include <stdlib.h>
#include <string.h>
#include <setjmp.h>
#include <alloca.h>

/* Configuration parameters */

uns pdf_trace;
uns xreft_size=1000;
uns respect_user_rights=1;
uns pdf_warnings=1;

static struct cf_section pdf_config = {
  CF_ITEMS {
    CF_UNS("Trace", &pdf_trace),
    CF_UNS("XrefTabSize", &xreft_size),
    CF_UNS("RespectUserRights", &respect_user_rights),
    CF_UNS("Warnings", &pdf_warnings),
    CF_END
  }
};

static void CONSTRUCTOR
pdf_init(void) {
  cf_declare_section("PDF", &pdf_config, 0);
}

static struct fastbuf *pdf_out, *meta_out;

/*
 * Memory allocation.
 */

struct mempool *page_pool;		/* Page-local stuff; discarded in out_pages() */
struct mempool *global_pool;		/* Global stuff; discarded at the end of parsing */
struct mempool *sf_pool;		/* Stream functions' data */

static jmp_buf recoverable_error_jump;

static void
pdf_rec_error(void) {
    longjmp(recoverable_error_jump, 1);
}

static void
pdf_truncate(void) {
    gobj_truncate();
    pdf_rec_error();
}

static inline void
check_overflow(void) {
    if (btell(pdf_out) > (ucw_off_t) max_decode_size)
	pdf_truncate();
}

/* Resources and graphics states */

static struct resources *restop;
static struct gstate *gstop;

static struct gsdata gsnull={-1,NULL,-1,-1,trm_nochange};

static struct gstate *
parse_gstate(uns gobj, struct fontname *resfonttop) {
    OBJECT gs,font;
    OBJ_GET gsdict[]={{"Font",&font},{NULL,NULL}};
    struct gstate *gg;
    struct fontname *fn;

    for(gg=gstop;gg;gg=gg->next)
	if(gg->obj=gobj)
	    break;
    if(gg) {
	TRACE(200,"(parse_gstate) Returning already known value");
	return gg;
    }
    gs=get_i_obj(gobj);
    if(gs.type!=OT_DICT)
	return NULL;
    parse_dict(gs.value.d,gsdict);
    gg=mp_alloc(global_pool,sizeof(struct gstate));
    gg->obj=gobj;
    gg->next=gstop;
    gstop=gg;
    if(font.type==OT_ARRAY && font.value.a && font.value.a->obj.type==OT_OBJREF) {
	for(fn=resfonttop;fn;fn=fn->next)
	    if(fn->obj==(uns)font.value.a->obj.value.n)
		break;
	if(fn) {
	    gg->data=mp_alloc(global_pool,sizeof(struct gsdata));
	    gg->data[0]=gsnull;
	    gg->data->tf=fn->enc;
	} else {
	    gg->data=&gsnull;
	}
    } else {
	gg->data=&gsnull;
    }
    return gg;
}

static struct resources *
parse_resources(OBJECT res, uns robj) {
    OBJECT fonts,gstate;
    OBJ_GET resdict[]={{"Font",&fonts},{"ExtGState",&gstate},{NULL,NULL}};
    struct resources *t;
    struct fontname *fn;
    struct gsname *gn;
    struct gstate *gs;
    DICT_ENTRY *d;
    int i;

    if(res.type==OT_OBJREF)
	robj = res.value.n;
    for(t=restop;t;t=t->next) {
	if(t->obj==robj)
	    break;
    }
    if(t)
	return t;

    if(res.type==OT_OBJREF)
	res=get_i_obj(robj);
    if(res.type!=OT_DICT)
	return NULL;

    t=mp_alloc_zero(global_pool,sizeof(struct resources));
    t->next=restop;
    t->obj=robj;
    restop=t;
    parse_dict(res.value.d,resdict);
    obj_deref(&fonts);
    if(fonts.type==OT_DICT) {
	for(d=fonts.value.d;d;d=d->next) {
	    if(d->obj.type!=OT_OBJREF)
		continue;
	    i=strlen(d->name)+1;
	    fn=mp_alloc_zero(global_pool,sizeof(struct fontname));
	    fn->next=restop->fonts;
	    fn->obj=d->obj.value.n;
	    fn->namehash=hash_string(d->name);
	    fn->name=mp_alloc(global_pool,i);
	    memcpy(fn->name,d->name,i);
	    restop->fonts=fn;
	}
	for(fn=restop->fonts;fn;fn=fn->next)
	    fn->enc=parse_fontenc(fn->obj);
    }
    obj_deref(&gstate);
    if(gstate.type==OT_DICT) {
	for(d=gstate.value.d;d;d=d->next) {
	    if(d->obj.type!=OT_OBJREF)
		continue;
	    i=strlen(d->name)+1;
	    gn=mp_alloc(global_pool,sizeof(struct gsname));
	    gn->next=restop->gstates;
	    gn->namehash=hash_string(d->name);
	    gn->name=mp_alloc(global_pool,i);
	    memcpy(gn->name,d->name,i);
	    gn->obj=d->obj.value.n;
	    restop->gstates=gn;
	    TRACE(200,"(parse_resources) GState: %s(%d)=obj %d",d->name,gn->namehash,gn->obj);
	}
	for(gn=restop->gstates;gn;gn=gn->next) {
	    gs=parse_gstate(gn->obj,restop->fonts);
	    gn->gsd=gs ? gs->data : NULL;
	    if(gn->gsd)
		TRACE(200,"(parse_resources) GState data: %d={obj %d, ctm_sy %d, tf %p, tfs %d, tm_sy %d, trm %d}", gn->namehash, gs->obj, gn->gsd->ctm_sy,gn->gsd->tf, gn->gsd->tfs, gn->gsd->tm_sy, gn->gsd->trm);
	    else
		TRACE(200,"(parse_resources) GState data: NULL");
	}
    }
    return restop;
}

/*
 * Graphic State stack; default value is stored in [0] element
 */

static struct gsdata gstack[GS_MAX_DEPTH]={{1,NULL,0,1,trm_fill}}, *gsmax=gstack+GS_MAX_DEPTH, *gscur;

static void
change_font(byte *fname, OBJECT fsize, struct fontname *fntop) {
    struct fontname *fn;
    uns fnh;

    if(!fname || !fname[0] || !fntop ||!gscur || (fsize.type!=OT_INT && fsize.type!=OT_REAL)) {
	TRACE(200,"(change_font) RETURN fname(%s), fntop(%p), fsize.type(%d)",fname,fntop,fsize.type);
	return;
    }
    fnh=hash_string(fname);
    for(fn=fntop;fn;fn=fn->next)
	if(fn->namehash==fnh && !strcmp(fn->name,fname))
	    break;
    if(fn) {
	gscur->tf=fn->enc;
	if((fnh=obj_int_real(fsize)))
	    gscur->tfs=fnh;
	TRACE(200,"(change_font) Font set to font obj #%d",fn->obj);
    } else {
	TRACE(200,"(change_font) Unknown font name");
	gscur->tf=NULL;
    }
}

static void
change_gs(byte *gsname, struct gsname *gntop) {
    struct gsname *gn;
    uns gnh,gnl;

    if(!gsname || !gntop ||!gscur)
	return;
    gnl=strlen(gsname)+1;
    if(gnl<2)
	return;
    gnh=hash_string(gsname);
    for(gn=gntop;gn;gn=gn->next)
	if(gn->namehash==gnh && !memcmp(gn->name,gsname,gnl))
	    break;
    if(gn && gn->gsd) {
	if(gn->gsd->ctm_sy>=0)
	    gscur->ctm_sy=gn->gsd->ctm_sy;
	if(gn->gsd->tf)
	    gscur->tf=gn->gsd->tf;
	if(gn->gsd->tfs>=0)
	    gscur->tfs=gn->gsd->tfs;
	if(gn->gsd->tm_sy>=0)
	    gscur->tm_sy=gn->gsd->tm_sy;
	if(gn->gsd->trm>=0)
	    gscur->trm=gn->gsd->trm;
    } else {
	TRACE(0,"Unknown GS name");
	*gscur=gstack[0];
    }
}

static uns current_class;				/* Current class in current output stream */
static uns queued_class;				/* Class of the next character we'll write */
#define WC_TYPE_MASK	0x0f				/* Lower bits are word type number WT_... or MT_... */
#define WC_BREAK	0x10				/* Force sentence break */
#define WC_SPACE	0x20				/* Force word break */
#define WC_META		0x80				/* We're writing meta-information */
static uns wordbuf[MAX_WORD_CHARS];
static uns wordlen;

/* Some of the standard types may be missing, assume WT_TEXT then */
#ifndef WT_BIG_HEADING
#define WT_BIG_HEADING WT_TEXT
#endif
#ifndef WT_SMALL_HEADING
#define WT_SMALL_HEADING WT_TEXT
#endif
#ifndef WT_SMALL
#define WT_SMALL WT_TEXT
#endif
#ifndef WT_ALT
#define WT_ALT WT_TEXT
#endif

static void
flush_word(void) {
    struct fastbuf *s_out = (queued_class & WC_META) ? meta_out : pdf_out;

    TRACE(200, "(flush_word) Wordlen=%d current_class=%x queued_class=%x", wordlen, current_class, queued_class);
    if (!wordlen)
	return;
    if (wordlen >= MAX_WORD_CHARS) {
	/* Oversize words are ignored and replaced by breaks */
	wordlen = 0;
	queued_class |= WC_BREAK;
	return;
    }
    if (current_class != queued_class) {
	if ((current_class ^ queued_class) & (WC_TYPE_MASK | WC_BREAK))
	    bputc(s_out, 0x80 + (queued_class & (WC_TYPE_MASK | WC_BREAK)));
	else
	    bputc(s_out, ' ');
	queued_class = current_class = queued_class & (WC_TYPE_MASK | WC_META);
    }
    for (uns i=0; i<wordlen; i++)
	bput_utf8(s_out, wordbuf[i]);
    if (pdf_trace >= 200) {
	byte buf[MAX_WORD_BYTES+1], *p=buf;
	for (uns i=0; i<wordlen; i++)
	    p = utf8_put(p, wordbuf[i]);
	*p++ = 0;
	LOG("(flush_word) out word: %s", buf);
    }
    check_overflow();
    wordlen=0;
}

static void
switch_class(uns class) {
    flush_word();
    if (class & (WC_META | WC_TYPE_MASK)) {
	if ((class ^ queued_class) & WC_META) {
	    class |= WC_BREAK;
	    current_class = class & WC_META;
	}
	queued_class = (queued_class & ~(WC_META | WC_TYPE_MASK)) | (class & (WC_META | WC_TYPE_MASK));
    }
    queued_class |= class & (WC_BREAK | WC_SPACE);
}

static inline void
word_break(void) {
    switch_class(WC_SPACE);
}

static void
add_uchar(uns unicode) {
    int i;

    if(unicode>=0xe000 && unicode<0xe000+UNICODESTRINGCNT && unistr[unicode-0xe000]) {
	TRACE(200,"(add_uchar) char means unicode string");
	for(i=0;unistr[unicode-0xe000][i];i++)
	    add_uchar(unistr[unicode-0xe000][i]);
	TRACE(200,"(add_uchar) end of unicode string");
	return;
    }
    if(!in_unirange(unicode)) {
	TRACE(200,"(add_uchar) RETURN char out of unirange %.4x",unicode);
	return;
    }

    if (Uprint(unicode) && !Uspace(unicode)) {
	/*
	 * What about some non-word chars? (c),(R),<TM> etc.
	 */
	TRACE(200,"(add_uchar) add word char %.4x",unicode);
	if (wordlen < MAX_WORD_CHARS)
	    wordbuf[wordlen++] = unicode;
    } else {
	TRACE(200,"(add_uchar) End of word");
	word_break();
    }
}

static void
output_enc_string(OBJECT in, struct fencoding *sf) {
    uns c,uc;
    int i;
    struct unichar cc;

    if(sf->bytes==1) {
	TRACE(200,"(output_enc_string) printing the string; encoding table %d-%d",sf->start,sf->start+sf->len);
	PRINTOBJ(200,in);
	for(i=0;i<in.value.str.l;i++) {
	    c=in.value.str.i[i];
	    if(c<sf->start || c>=sf->start+sf->len) {
		TRACE(200,"(output_enc_string) invalid input char %d",c);
		continue;
	    }
	    cc=sf->table[c-sf->start];
	    if(cc.fcode)
		add_uchar(cc.unicode);
	}
    } else { /* bytes==2 */
	for(i=0;i<in.value.str.l;i+=2) {
	    uc=c=in.value.str.i[i]*256+in.value.str.i[i+1];
	    do {
		cc=sf->table[c%sf->len];
		c=(c+sf->start)%sf->len;
	    } while(cc.fcode>0 && cc.fcode!=uc);
	    if(cc.fcode)
		add_uchar(cc.unicode);
	}
    }
}

static void
out_enc_string(OBJECT in) {
    struct fencoding *sf=gscur->tf;
    uns c, class;

    if(in.type!=OT_STRING) {
	TRACE(200,"(out_enc_string) RETURN !OT_STRING");
	return;
    }
    if(!sf) {
	TRACE(200,"(out_enc_string) RETURN !sf");
	return;
    }
    /*
     * don't output invisible text
     */
    if(gscur->trm==trm_nothing || gscur->trm==trm_nochange) {
	TRACE(200,"(out_enc_string) RETURN invisible text");
	return;
    }
    /*
     * guess string class from the text size
     */
    c=gscur->ctm_sy * gscur->tm_sy * gscur->tfs;
    TRACE(200,"(out_enc_string) Text size: %d*%d*%d=%d",gscur->ctm_sy,gscur->tm_sy,gscur->tfs,c);
    if(c>13)
	class=WT_BIG_HEADING;
    else if(c>11)
	class=WT_SMALL_HEADING;
    else if(c>8)
	class=WT_TEXT;
    else if(c>0)
	class=WT_SMALL;
    else {
	TRACE(200,"(out_enc_string) RETURN strange text size %d",c);
	return;
    }
    switch_class(class);
    output_enc_string(in, sf);
}

static inline void
out_pdfstring(OBJECT obj, uns class) {
    switch_class(class | WC_BREAK);
    output_enc_string(obj, &PDFDocEncoding);
}

/* parse content stream */

static void
cs_gettexts(struct resources *res) {
    OBJECT obj;
    ARRAY_ENTRY *start=NULL,*cur=NULL,*prev=NULL;

    int i,h,textmode=0;
    byte *s;
    s32 tm[4],tms;

    TRACE(200,"Content stream out text");
    gstack[1]=gstack[0];	/* initializing gs current state */
    gscur=gstack+1;

    while(in_check_nschar(SP_ANYWCOMM) >= 0) {
	if(!cur) {
	    cur=start=mp_alloc_zero(page_pool,sizeof(ARRAY_ENTRY));
	} else {
	    prev=cur;
	    cur=cur->next=mp_alloc_zero(page_pool,sizeof(ARRAY_ENTRY));
	}
	cur->next=NULL;
	cur->obj=get_obj();
	if(cur->obj.type==OT_UTOK) { /* an operator */
	    /*
	     * No operator we are intersted in is longer than 2 chars
	     */
	    for(h=0,i=0,s=cur->obj.value.s;*s && i<2;s++)
		h=h*256+*s;
	    if(*s)
		h=0;
	    switch(h) {
		case 25453:       /* cm */
		    TRACE(200,"(cs_gettexts - cm)");
		    for(cur=start,i=0;i<3 && cur;i++)
		       cur= cur->next;
		    if(!cur)
			break;
		    if(cur->obj.type==OT_INT)
			gscur->ctm_sy*=cur->obj.value.n;
		    if(cur->obj.type==OT_REAL)
			gscur->ctm_sy*=cur->obj.value.r;
		    break;
		case 113:       /* q */
		    TRACE(200,"(cs_gettexts - q)");
		    if(gscur>gsmax) {
			TRACE(0,"Gstack overflow");
			break;
		    }
		    gscur[1]=gscur[0];
		    gscur++;
		    break;
		case 81:        /* Q */
		    TRACE(200,"(cs_gettexts - Q)");
		    if(gscur<=gstack+1) {
			TRACE(0,"Gstack underflow");
			break;
		    }
		    gscur--;
		    break;
		case 26483:       /* gs */
		    TRACE(200,"(cs_gettexts - gs)");
		    if(start->obj.type==OT_NAME)
			change_gs(start->obj.value.s,res->gstates);
		    break;
		case 21618:       /* Tr */
		    TRACE(200,"(cs_gettexts - Tr)");
		    if(textmode && start->obj.type==OT_INT && start->obj.value.n>trm_nochange && start->obj.value.n<trm_max)
			gscur->trm=start->obj.value.n;
		    break;
		case 21613:       /* Tm */
		    TRACE(200,"(cs_gettexts - Tm)");
		    for(cur=start,i=0;i<4 && cur;tm[i++]=obj_int_real(cur->obj),cur=cur->next);
		    if(!cur)
			break;
		    tms=hypot(tm[0],tm[1]);
		    tms=tms?ABS(tm[1]*tm[2]-tm[0]*tm[3])/tms:0;
		    if(tms)
			TRACE(200,"(cs_gettexts - Tm) scale=%d",tms);
		    else
			TRACE(200,"(cs_gettexts - Tm) matrix=[%d,%d,%d,%d]",tm[0],tm[1],tm[2],tm[3]);
		    gscur->tm_sy=tms;
		    if(!(cur=cur->next))
			break;
		    /*
		     * finding word boundaries - little magic
		     * idea: if shift is greater than charsize, flush_word
		     *       if shift_x is greater than charsize*5 or
		     *        if shift_y is greater than charsize, schedule
		     *        sentence break
		     *  FIXME:
		     *       (check the units of translation move in PLRM, it
		     *       looks unit is 1/1000 of charsize but who knows?)
		     */
		    i=ABS(obj_int_real(cur->obj));
		    if(i>gscur->tm_sy*gscur->tfs*1000) {
			if(i>gscur->tm_sy*gscur->tfs*5*1000) {
			    TRACE(200,"(cs_gettexts - Tm) shift=%d, tm: %d, tfs: %d, all: %d", i, gscur->tm_sy, gscur->tfs, gscur->tm_sy*gscur->tfs*5);
			    switch_class(WC_BREAK);
			}
			word_break();
		    }
		    if(!(cur=cur->next))
			break;
		    i=ABS(obj_int_real(cur->obj));
		    if(i>gscur->tm_sy*gscur->tfs*1000)
			switch_class(WC_BREAK);
		    break;
		case 16980:       /* BT */
		    TRACE(200,"(cs_gettexts - BT)");
		    textmode=1;
		    gscur->tm_sy=1;	/* Specification says BT resets Tm matrix */
		    break;
		case 17748:       /* ET */
		    TRACE(200,"(cs_gettexts - ET)");
		    textmode=0;
		    break;
		case 16969:       /* BI */
		    TRACE(200,"(cs_gettexts - BI)");
		    /* not nice trick but BI..ID..EI is a not-nice
		     * exception from Content Stream syntax
		     */
		    do {
			if(skip_space(SP_ANYWCOMM) < 0)
			    break;
			obj=get_obj();
		    } while(obj.type!=OT_UTOK || memcmp(obj.value.s,"ID",3));
		    do {
			if(skip_space(SP_ANYWDELIM) < 0)
			    break;
			obj=get_tok();
		    } while(memcmp(obj.value.s,"EI",3));
		    break;
		case 21606:       /* Tf */
		    TRACE(200,"(cs_gettexts - Tf)");
		    if(textmode && start->obj.type==OT_NAME)
			change_font(start->obj.value.s,start->next->obj,res->fonts);
		    else
			TRACE(200,"(cs_gettexts - Tf) syntax error: textmode %d, obj.type %d",textmode,start->obj.type);
		    break;
		case 21604:       /* Td */
		    TRACE(200,"(cs_gettexts - Td)");
		    if(textmode)
			word_break();
		    break;
		case 21572:       /* TD */
		    TRACE(200,"(cs_gettexts - TD)");
		    if(textmode)
			word_break();
		    break;
		case 21546:       /* T* */
		    TRACE(200,"(cs_gettexts -  T*)");
		    if(textmode)
			word_break();
		    break;
		case 39:        /* ' */
		    TRACE(200,"(cs_gettexts - ')");
		    if(textmode) {
			word_break();
			out_enc_string(start->obj);
		    }
		    break;
		case 21610:       /* Tj */
		    TRACE(200,"(cs_gettexts - Tj)");
		    if(textmode)
			out_enc_string(start->obj);
		    break;
		case 34:        /* " */
		    TRACE(200,"(cs_gettexts - \")");
		    if(textmode && start->next && start->next->next)
			out_enc_string(start->next->next->obj);
		    break;
		case 21578:       /* TJ */
		    TRACE(200,"(cs_gettexts - TJ) textmode=%d, obj.type=%d",textmode,start->obj.type);
		    if(textmode && start->obj.type==OT_ARRAY)
			for(cur=start->obj.value.a;cur;) {
			    out_enc_string(cur->obj);
			    if(!(cur=cur->next))
				break;
			    /*
			     * "kern" bigger than average char width means
			     * flush_word()
			     */
			    if(obj_int_real(cur->obj)<=-100)
				word_break();
			    cur=cur->next;
			}
		    break;
		default: ;
	    }
	    cur=start=NULL;
	    mp_flush(page_pool);
	}
    }
    word_break();
    TRACE(200,"End of parsing Content stream (out text)");
}

static void
out_info(void) {
    OBJECT info,author,title;
    OBJ_GET infodict[]={{"Author",&author},{"Title",&title},{NULL,NULL}};

    if(!pdf_inforef)
	return;
    info=get_i_obj(pdf_inforef);
    PRINTOBJ(100,info);
    if(info.type!=OT_DICT)
	return;
    parse_dict(info.value.d,infodict);
#ifdef MT_MISC
    obj_deref(&author);
    if(author.type==OT_STRING) {
	out_pdfstring(author, WC_META | MT_MISC | WC_BREAK);
    }
#endif
#ifdef MT_TITLE
    obj_deref(&title);
    if(title.type==OT_STRING) {
	out_pdfstring(title, WC_META | MT_TITLE | WC_BREAK);
    }
#endif
}

static uns bookmarkcnt;

static s32
out_bookmarks(s32 outlines) {
    OBJECT root,first,next,title;
    OBJ_GET bookrootdict[]={{"First",&first},{"Next",&next},{"Title",&title},{NULL,NULL}};
    s32 n;

    if(++bookmarkcnt>MAX_TREE_SIZE) {
	TRACE(100,"Maximum amount of bookmarks reached");
	pdf_truncate();
    }
    root=get_i_obj(outlines);
    if(root.type!=OT_DICT)
	return 0;
    parse_dict(root.value.d,bookrootdict);
    obj_deref(&title);
    if(title.type==OT_STRING)
	out_pdfstring(title, WT_SMALL_HEADING | WC_BREAK);
    if(first.type==OT_OBJREF) {
	n=first.value.n;
	while((n=out_bookmarks(n))>0);
    }
    return next.type==OT_OBJREF ? next.value.n : 0;
}

static char *Annots[26]={
    "Popup",
    "",
    "Ink",
    "",
    "Highlight",
    "",
    "Movie",
    "Squiggly",
    "Line",
    "",
    "Widget",
    "Stamp",
    "Text",
    "FreeText",
    "Link",
    "Sound",
    "",
    "",
    "Circle",
    "Underline",
    "",
    "FileAttachment",
    "StrikeOut",
    "Square",
    "",
    ""
};

static void
out_annots(OBJECT annots) {
    OBJECT type,subtype,contents,action,filespec,uri;
    OBJ_GET annotdict[]={{"Type",&type},{"Subtype",&subtype},{"Contents",&contents},{"A",&action},{NULL,NULL}};
    OBJ_GET actiondict[]={{"Type",&type},{"S",&subtype},{"F",&filespec},{"URI",&uri},{NULL,NULL}};
    OBJ_GET filespecdict[]={{"FS",&subtype},{"F",&uri},{NULL,NULL}};
    int h;
    int oclass;
    byte *b;
    ARRAY_ENTRY *ai;

    obj_deref(&annots);
    if(annots.type!=OT_ARRAY)
	return;
    for(ai=annots.value.a;ai;ai=ai->next) {
	obj_deref(&ai->obj);
	if(ai->obj.type!=OT_DICT)
	    continue;
	parse_dict(ai->obj.value.d,annotdict);
	if(subtype.type!=OT_NAME)
	    return;
	for(h=0,b=subtype.value.s;*b>0;h=(h+(*b++)-96)%26);
	if((h==11 || h==18) && *subtype.value.s>'S') h++;
	if(h<0 || memcmp(subtype.value.s,Annots[h],b-subtype.value.s) || !Annots[h])
	    return;
	obj_deref(&contents);
	if(contents.type==OT_STRING) {
	    oclass=(h==0 || h==6 || h==10 || h==14 || h==15) ? WT_ALT : WT_SMALL;
	    TRACE(100,"Annots (%d):%*s",h,contents.value.str.l,contents.value.str.i);
	    out_pdfstring(contents,oclass);
	}
	if(h==14) { /* Link */
	    obj_deref(&action);
	    if(action.type==OT_DICT) {
		parse_dict(action.value.d,actiondict);
		if(subtype.type==OT_NAME && !memcmp(subtype.value.s,"URI",4)) {
		    obj_deref(&uri);
		    if(uri.type==OT_STRING) {
			uri.value.str.i[uri.value.str.l]=0;
			gobj_add_ref('R',uri.value.str.i);
		    }
		} else if(subtype.type==OT_NAME && !memcmp(subtype.value.s,"GoToR",6)) {
		    obj_deref(&filespec);
		    if(filespec.type==OT_DICT) {
			parse_dict(filespec.value.d,filespecdict);
			if(subtype.type==OT_NAME && !memcmp(subtype.value.s,"URL",4)) {
			    obj_deref(&uri);
			    if(uri.type==OT_STRING) {
				uri.value.str.i[uri.value.str.l]=0;
				gobj_add_ref('R',uri.value.str.i);
			    }
			}
		    }
		}
	    }
	}
    }
}

static uns pagescnt;

static void
out_pages(s32 pagesref, struct resources *resref) {
    /* resref==NULL means no resources known up to this level of page tree
     * if this is a leaf (Page) and still resref==NULL, skip this page
     */
    OBJECT root,type,nres,kids,annots,contents,contentest;
    OBJ_GET rootdict[]={{"Type",&type},{"Resources",&nres},
	{"Kids",&kids},
	{"Annots",&annots},{"Contents",&contents},
	{NULL,NULL}};
    ARRAY_ENTRY *ai;
    uns i;

    switch_class(WC_BREAK | WT_TEXT);
    if(++pagescnt>=MAX_TREE_SIZE) {
	TRACE(100,"Maximum amount of pages reached");
	pdf_truncate();
    }
    if(pagesref<=0)
	return;
    root=get_i_obj(pagesref);
    if(root.type!=OT_DICT)
	return;
    parse_dict(root.value.d,rootdict);
    if(nres.type!=OT_UNKNOWN)
	resref=parse_resources(nres,pagesref);
    if (type.type != OT_NAME)
      return;
    if (!memcmp(type.value.s,"Page",5)) {	/* Type Page */
	TRACE(200,"(out_pages) Page; obj #%d",pagesref);
	if (!resref) {
	    TRACE(200,"(out_pages) RETURN !resref");
	    return;
	}
	out_annots(annots);
	contentest=contents;
	obj_deref(&contentest);
	if(contentest.type==OT_ARRAY)
	    contents=contentest;
	if (contents.type!=OT_ARRAY && contents.type!=OT_OBJREF) {
	    TRACE(200,"(out_pages) RETURN !contents (type %d)",contents.type);
	    return;
	}
	if(contents.type==OT_ARRAY) {
	    uns lca;

	    for(ai=contents.value.a,lca=0;ai && ai->obj.type==OT_OBJREF;ai=ai->next,lca++);
	    lca=lca>MAX_STREAMS ? MAX_STREAMS : lca;
	    for(ai=contents.value.a,i=0;i<lca;ai=ai->next,i++)
		stream_array[i]=ai->obj.value.n;
	    stream_array[i]=0;
	} else {	/* contents.type==OT_OBJREF */
	    stream_array[0]=contents.value.n;
	    stream_array[1]=0;
	}
	set_input_method(pdf_stream_in);
	cs_gettexts(resref);
	set_input_method(pdf_in);
    } else if (!memcmp(type.value.s,"Pages",6)) {
	int kidsc,i;
	s32 *kidsa;
	ARRAY_ENTRY *ai;

	TRACE(200,"(out_pages) Pages; obj #%d",pagesref);
	obj_deref(&kids);
	if (kids.type!=OT_ARRAY)
	    return;
	for(kidsc=0,ai=kids.value.a;ai;ai=ai->next,kidsc++);
	if(kidsc>MAX_SUB_PAGES)
	    kidsc=MAX_SUB_PAGES;
	kidsa = alloca(sizeof(s32)*kidsc);
	for(i=0,ai=kids.value.a;i<kidsc && ai;ai=ai->next) {
	    if(ai->obj.type!=OT_OBJREF)
		continue;
	    kidsa[i]=ai->obj.value.n;
	    i++;
	}
	kidsc=i;
	for(i=0;i<kidsc;out_pages(kidsa[i++],resref));
    }
    mp_flush(page_pool);
}

static void
out_document(void) {
    OBJECT root,bookmarkroot,pageroot,baseurl;
    OBJ_GET rootdict[]={{"Outlines",&bookmarkroot},{"Pages",&pageroot},{"Base",&baseurl},{NULL,NULL}};

    if(!pdf_rootref)
	return;
    root=get_i_obj(pdf_rootref);
    if(root.type!=OT_DICT)
	return;
    parse_dict(root.value.d,rootdict);
    obj_deref(&baseurl);
    if(baseurl.type==OT_STRING)
	gthis->base_url = gobj_parse_url(&gthis->base_url_s, baseurl.value.str.i, "base", 0);

    mp_flush(page_pool);
    bookmarkcnt=0;
    if(bookmarkroot.type==OT_OBJREF)
	out_bookmarks(bookmarkroot.value.n);
    mp_flush(page_pool);

    pagescnt=0;
    if(pageroot.type==OT_OBJREF)
	out_pages(pageroot.value.n,NULL);
}

int
pdf_parse(char **args UNUSED) {
    if(gthis->truncated)
	gerror(2205, "PDF file truncated");

    pdf_in = fbmem_clone_read(gthis->contents);
    pdf_out = gthis->text = fbmem_create(16384);
    meta_out = gthis->meta = fbmem_create(256);
    current_class = queued_class = 0;

    if (!page_pool) {
	page_pool=mp_new(16384);
	global_pool=mp_new(16384);
	sf_pool=mp_new(2048);
    }

    if (!setjmp(recoverable_error_jump)) {
	pdf_setup();
	out_info();
	out_document();
    }

    bclose(pdf_in);
    flush_word();

    mp_delete(page_pool);
    mp_delete(global_pool);
    mp_delete(sf_pool);
    page_pool = NULL;

    return 1;
}
