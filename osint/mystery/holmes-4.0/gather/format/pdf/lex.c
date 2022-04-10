/*
 *  Sherlock PDF Parser: Lexical analysis and low-level parsing
 *
 *  (c) 2002--2003 Milan Vancura <milan@ucw.cz>
 *  (c) 2003--2005 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "gather/gather.h"
#include "gather/format/pdf/pdf.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static struct fastbuf fb_stream, *input;
struct fastbuf *pdf_in, *pdf_stream_in=&fb_stream;
static byte stringbuf[STRINGBUFLEN];

ucw_off_t pdf_filesize;

s32 pdf_rootref, pdf_inforef;

static OBJ_START **xreftroot;

static OBJ_START
obj_off(uns i) {
    OBJ_START out={0,0};
    uns j=i/xreft_size;

    if(j>=xreft_size || !xreftroot[j])
	return out;
    return xreftroot[j][i%xreft_size];
}

/*
 * Basic input calls
 *
 * there are input calls defined. They can read from PDF file (pdf_in) or from
 * PDF stream object (pdf_stream_in). Function set_input_method() switches between
 * these two.
 */

int
ingetc(struct fastbuf *in) {
    int c;
    c=bgetc(in);
    if(c<0 && in==pdf_in)
	gerror(2200,"(ingetc) Unexpected EOF");
    if(c<0 && in==pdf_stream_in)
	TRACE(10001,"End of stream");
    return c;
}

#define in_get_char()	    ingetc(input)
#define in_check_char()	    bpeekc(input)
#define in_unget_char()	    bungetc(input)
#define in_read(b,l)	    bread(input,b,l)
#define in_tell()	    ((int)btell(input))

int
in_check_nschar(int spacetype) {
    if(skip_space(spacetype)<0)
	return -1;
    return in_check_char();
}

/*
 * input method selector
 *
 * note that after switching to pdf_stream_in, pdf_stream_in is initialized
 */

void
set_input_method_raw(struct fastbuf *inf)
{
    ASSERT(inf==pdf_in || inf==pdf_stream_in);
    input=inf;
}

void
set_input_method(struct fastbuf *inf) {
    ASSERT(inf==pdf_in || inf==pdf_stream_in);
    if(inf==pdf_stream_in)
	stream_init(pdf_stream_in);
    input=inf;
}

int
pdf_check_nschar(int spacetype) {
    struct fastbuf *tmp=input;

    input=pdf_in;
    skip_space(spacetype);
    input=tmp;
    return pdf_check_char();
}

#define pdf_seek_end(pos)   bseek(pdf_in, pos, SEEK_END)

void
pdf_seek(ucw_off_t pos) {
    if(pos<0 || pos>=pdf_filesize)
	gerror(2220,"(pdf_seek) Seek out of file");
    bsetpos(pdf_in, pos);
}

/* advanced input calls */

static ucw_off_t seekposbuf[MAX_SEEK_POS];
static int seekpos=0;

void
restorefilepos(void) {
    if(seekpos<=0)
	gerror(2222,"(restorefilepos) Empty filepos buffer (seekpos=%d)",seekpos);
    seekpos--;
    pdf_seek(seekposbuf[seekpos]);
}

void
savefilepos(void) {
    seekposbuf[seekpos]=pdf_tell();
    seekpos++;
    if(seekpos>=MAX_SEEK_POS)
	gerror(2222,"(savefilepos) Seekpos buffer overflow (filepos: %d)",pdf_tell());
}

int
in_get_line(char * buf) {
    int c;
    int i=0;

    do {
	c=in_get_char();
	if(c>=0 && buf)
	    buf[i]=c;
	i++;
    } while (c>=0 && c!=10 && c!=13 && i<MAX_BUFF_SIZE);
    if(c<0)
	return -1;
    if(buf)
	buf[i-1]=0;
    if(c==13) {
	c=in_get_char();
	if(c<0)
	    return -1;
	if(c!=10)
	    in_unget_char();
	else
	    i++;
    }
    return i;
}

static int
is_numchar(int c) {
    return(c>='0' && c<='9');
}

static int
is_intchar(int c) {
    return(is_numchar(c) || c=='+' || c=='-');
}

int
is_space(int c, uns type) {
    return (((type & SP_EOL) && (c==10 || c==13)) ||
	    ((type & SP_NOEOL) && (c==0 || c==9 || c==12 || c==' ')) ||
	    ((type & SP_DELIM) && (c=='(' || c==')' || c=='<' || c=='>' || c=='[' || c==']' || c=='{' || c=='}' || c=='/' || c=='%')) ||
	    ((type & SP_COMMENT) && (c=='%')));
}

int
skip_space(uns type) {
    int c;
    int i=0;

    while(is_space(c=in_get_char(),type) && c>=0) {
	TRACE(10001,"(skip_space) char %d",c);
	if((type & SP_COMMENT) && c=='%') {
	    if(in_get_line(NULL)<0)
		break;
	}
	i++;
    }
    TRACE(10001,"(skip_space) end char %d",c);
    if(c<0)
	return -1;
    in_unget_char();
    return (i>0);
}

OBJECT
get_tok(void) {
    OBJECT obj;
    int c;
    byte *b=stringbuf, *e=stringbuf+STRINGBUFLEN;

    obj.type=OT_UTOK;
    while(!is_space(c=in_get_char(),SP_ANYWDELIM) && c>=0 && b<e)
	*b++=c;
    if(c>=0)
	in_unget_char();
    *b=0;
    c=b-stringbuf+1;
    if(c<0 || c>=STRINGBUFLEN)
	gerror(2203,"(get_tok) stringbuf overflow: %d",c);
    obj.value.s=mp_alloc(page_pool,b-stringbuf+1);
    memcpy(obj.value.s,stringbuf,b-stringbuf+1);
    return obj;
}

#if 0
static s32
getint(void) {
    OBJECT obj;
    s32 val;
    char *b,*ptr;

    obj=get_tok();
    if(!is_intchar(*obj.value.s))
	gerror(2200,"(getint) Wrong int number: %s",obj.value.s);
    for(b=obj.value.s+1;is_numchar(*b);b++);
    if(*b)
	gerror(2200,"(getint) Wrong int number: %s (%c)",obj.value.s,*b);
    val=strtol(obj.value.s,&ptr,10);
    if(ptr!=b)
	gerror(2200,"(getint) Wrong int number: %s",obj.value.s);
    return val;
}
#endif

static OBJECT
get_int_real(void) {
    OBJECT out;
    int i=0,c;
    char *b=stringbuf, *e=stringbuf+STRINGBUFLEN;
    char *ptr;

    if((is_intchar(c=in_get_char()) || c=='.') && (i+=(c=='.'))<2)
	*b++=c;
    else
	gerror(2200,"(get_int_real) Wrong int/real number (start)");
    while((is_numchar(c=in_get_char()) || c=='.') && (i+=(c=='.'))<2 && b<e)
	*b++=c;
    if(c>=0)
	in_unget_char();
    *b=0;
    if(i>=1) {
	out.value.r=strtod(stringbuf,&ptr);
	out.type=OT_REAL;
    } else {
	out.value.n=strtol(stringbuf,&ptr,10);
	out.type=OT_INT;
    }
    if(ptr!=b)
	gerror(2200,"(get_int_real) Wrong int/real number: %s",stringbuf);
    return out;
}

static inline uns
hexnibble(uns n) {
    if(is_numchar(n))
	n-='0';
    else if(n>='a' && n<='f')
	n=n+10-'a';
    else if(n>='A' && n<='F')
	n=n+10-'A';
    else
	gerror(2223,"(hexnibble) Wrong HEX char '%c'",n);
    return n;
}

uns
hexbyte(uns hi, uns lo) {
    return 16*hexnibble(hi)+hexnibble(lo);
}

static OBJECT
get_hexastring(void) {
    OBJECT obj;
    int c,d;
    byte *b=stringbuf,*e=stringbuf+STRINGBUFLEN;

    obj.type=OT_STRING;
    do {
	if(skip_space(SP_ANYSPACE)<0)
	    break;
	c=in_get_char();
	if(c=='>' || c<0)
	    break;
	if(skip_space(SP_ANYSPACE)<0)
	    break;
	d=in_get_char();
	*b++=(d=='>' || d<0) ? hexbyte(c,'0') : hexbyte(c,d);
	if(b>=e)
	    gerror(2203,"(get_hexastring) stringbuf overflow at %d",in_tell());
    } while(d!='>' && d>=0);

    obj.value.str.i=mp_alloc(page_pool,b-stringbuf+1);
    obj.value.str.l=b-stringbuf;
    copy_and_decrypt(obj.value.str.i, stringbuf, obj.value.str.l);

    return obj;
}

static OBJECT
get_string(void) {
    OBJECT obj;
    int c='(';
    int parenlev=1;	    /* level of nesting in () */
    int i;
    byte *b=stringbuf, *e=stringbuf+STRINGBUFLEN;

    obj.type=OT_STRING;
    in_get_char();	    /* '(' */
    for(;parenlev;b++) {
	if(b>=e)
	    gerror(2203,"(get_string) stringbuf overflow at %d",in_tell());
	c=in_get_char();
	if(c<0)
	    break;
	*b=c;
	switch(c) {
	    case '(': parenlev++; break;
	    case ')': parenlev--; break;
	    case '\r':
		c=in_get_char();
		if(c!='\n')
		    in_unget_char();
	    case '\n':
		*b='\n';
		break;
	    case '\\':
		c=in_get_char();
		switch(c) { /* table of special chars */
		    case -1:
			parenlev=0;
			break;
		    case '\r':
			c=in_get_char();
			if(c!='\n')
			    in_unget_char();
		    case '\n':
			b--;
			break;
		    case 'n': *b=c='\n'; break;
		    case 'r': *b=c='\r'; break;
		    case 't': *b=c='\t'; break;
		    case 'b': *b=c='\b'; break;
		    case 'f': *b=c='\f'; break;
		    case '0': /* octal code */
		    case '1':
		    case '2':
		    case '3':
		    case '4':
		    case '5':
		    case '6':
		    case '7':
			*b=0;
			i=0;
			while(c>='0' && c<='7' && i<3 && *b<32) {
			    *b=*b*8+c-'0';
			    c=in_get_char();
			    i++;
			}
			if(c<0) {
			    parenlev=0;	/* break from for cycle */
			    break;
			}
			in_unget_char();
			break;
		    default: *b=c; break;
		}
		break;
	    default: ;
	}
    }
    if(c==')')
	b--;
    obj.value.str.i=mp_alloc(page_pool,b-stringbuf+1);
    obj.value.str.l=b-stringbuf;
    copy_and_decrypt(obj.value.str.i, stringbuf, obj.value.str.l);
    return obj;
}

static OBJECT
get_name(void) {
    OBJECT obj;
    int c,d;
    byte *b=stringbuf, *e=stringbuf+STRINGBUFLEN;

    obj.type=OT_NAME;
    in_get_char(); /* '/' */
    while(!is_space(c=in_get_char(),SP_ANYWDELIM) && c>=0 && b<e) {
	*b++=c;
	if(c=='#') {
	    c=in_get_char();
	    d=in_get_char();
	    if(d<0) {
		b--;
		break;
	    }
	    if((b[-1]=hexbyte(c,d))==0)
		gerror(2200,"(get_name) NULL byte not allowed in object name");
	    /*
	     * XXX: This is not forbidden by the standard, but using control
	     * characters in names is evil and it could break our log messages,
	     * so we are going to disallow it for the time being.
	     */
	    if(b[-1] < 0x20)
		gerror(2200,"(get_name) control character found in object name");
	}
    }
    if(c>=0)
	in_unget_char();
    *b=0;
    obj.value.s=mp_alloc(page_pool,b-stringbuf+1);
    memcpy(obj.value.s,stringbuf,b-stringbuf+1);

    return obj;
}

static OBJECT
check_ref(OBJECT in1) {
    OBJECT in2;
    int c;

    savefilepos();
    c=in_check_nschar(SP_ANYWCOMM);
    if(is_numchar(c) || c=='+') {
	in2=get_int_real();
	if(in2.type==OT_INT) {
	    c=in_check_nschar(SP_ANYWCOMM);
	    c=in_get_char();
	    if(c=='R' && is_space(in_check_char(),SP_ANYWDELIM)) {
		c=in1.value.n;
	        if((uns)c<xreft_size*xreft_size && xreftroot[c/xreft_size] && xreftroot[c/xreft_size][c%xreft_size].position>0)
		    in1.type=OT_OBJREF;
		else
		    in1.type=OT_NULL;
	    }
	}
    }
    if(in1.type==OT_INT)
	restorefilepos();
    else
	seekpos--;  /* forgetpos() */
    return in1;
}

static OBJECT get_dict(void);
static OBJECT get_array(void);

OBJECT
get_obj(void) {
    int c;
    OBJECT obj;

    c=in_check_nschar(SP_ANYWCOMM);
    TRACE(500,"get_obj: filepos=%d; TYPECHAR=%c",in_tell(),c);
    switch(c) {
	case -1:
	    gerror(2200,"(get_obj) Unexpected EOF");
	    break;
	case '0':
	case '1':
	case '2':
	case '3':
	case '4':
	case '5':
	case '6':
	case '7':
	case '8':
	case '9':
	case '.':
	case '-':
	case '+':
	    obj=get_int_real();
	    break;
	case '(':
	    obj=get_string();
	    break;
	case '[':
	    in_get_char();
	    obj=get_array();
	    break;
	case '<':
	    /* test next char */
	    c=in_get_char();
	    c=in_check_char();
	    TRACE(500,"get_obj: TYPECHAR2=%c",c);
	    if(c=='<') {
		in_get_char();
		obj=get_dict();
	    } else {
		obj=get_hexastring();
	    }
	    break;
	case '/':
	    obj=get_name();
	    break;
	default:
	    if(!is_space(c,SP_ANYWDELIM)) {
		obj=get_tok();
		if(!memcmp(obj.value.s,"null",5)) {
		    obj.type=OT_NULL;
		    obj.value.n=0;
		    break;
		}
		if(!memcmp(obj.value.s,"true",5)) {
		    obj.type=OT_BOOL;
		    obj.value.n=1;
		    break;
		}
		if(!memcmp(obj.value.s,"false",6)) {
		    obj.type=OT_BOOL;
		    obj.value.n=0;
		    break;
		}
	    } else {
		gerror(2200,"(get_obj) Unknown token type (char %d at %d)",c,in_tell());
	    }
    }
    if(input==pdf_in && obj.type==OT_INT) {
	TRACE(500,"get_obj: check_ref(%d)", obj.value.n);
	obj=check_ref(obj);
    }
    return obj;
}

OBJECT
get_i_obj(s32 n) {
    OBJECT out;
    OBJ_START oi=obj_off(n);

    if(oi.position==0) {
	out.type=OT_NULL;
	out.value.n=n;
	return out;
    }
    if((encrypted&3)==3)
	rc4_setup(&rc4_state, n, oi.generation);
    pdf_seek(oi.position);
    out = get_obj();
    if (out.type != OT_INT || out.value.n != n)
	gerror(2220, "(get_i_obj) Can't find object #%d", n);
    out = get_obj();
    if (out.type != OT_INT || out.value.n != oi.generation)
	gerror(2220, "(get_i_obj) Wrong G number of object #%d", n);
    out = get_obj();
    if (out.type != OT_UTOK || strcmp(out.value.s, "obj"))
	gerror(2220, "(get_i_obj) Keyword \"obj\" expected (obj #%d)", n);
    return get_obj();
}

static int po_shift=0;

void
printobj(OBJECT obj) {
    ARRAY_ENTRY *ap;
    DICT_ENTRY *dp;

    switch(obj.type) {
	case OT_BOOL:
	    LOG("%*s(bool: %s)",po_shift,"",obj.value.n?"true":"false");
	    break;
	case OT_INT:
	    LOG("%*s(int: %d)",po_shift,"",obj.value.n);
	    break;
	case OT_REAL:
	    LOG("%*s(real: %f)",po_shift,"",obj.value.r);
	    break;
	case OT_OBJREF:
	    LOG("%*s(ref: (%d %d R))",po_shift,"",obj.value.n,obj_off(obj.value.n).generation);
	    break;
	case OT_STRING:
	    LOG("%*s(string: (%d %.*s))",po_shift,"",obj.value.str.l,obj.value.str.l,obj.value.str.i);
	    break;
	case OT_NAME:
	    LOG("%*s(name: /%s)",po_shift,"",obj.value.s);
	    break;
	case OT_ARRAY:
	    LOG("%*s(array)",po_shift,"");
	    po_shift+=3;
	    for (ap=obj.value.a;ap;ap=ap->next)
		printobj(ap->obj);
	    po_shift-=3;
	    break;
	case OT_DICT:
	    LOG("%*s(dict)",po_shift,"");
	    po_shift+=3;
	    for (dp=obj.value.d;dp;dp=dp->next) {
		LOG("%*s/%s: ",po_shift,"",dp->name);
		po_shift+=3;
		printobj(dp->obj);
		po_shift-=3;
	    }
	    po_shift-=3;
	    break;
	case OT_STREAM:
	    LOG("%*s(stream)",po_shift,"");
	    break;
	case OT_NULL:
	    if(obj.value.n)
		LOG("%*s(unknown ref to obj %d)",po_shift,"",obj.value.n);
	    else
		LOG("%*s(null)",po_shift,"");
	    break;
	case OT_UTOK:
	    LOG("%*s(token: %s)",po_shift,"",obj.value.s);
	    break;
	default:
	    gerror(2203,"(printobj) Unknown object: %d",obj.type);
    }
}

static OBJECT
get_array(void) {
    OBJECT obj;
    ARRAY_ENTRY *start=NULL,*cur=NULL;

    TRACE(200,"Array:");
    while(in_check_nschar(SP_ANYWCOMM)!=']') {
	if(!cur)
	    cur=start=mp_alloc_zero(page_pool,sizeof(ARRAY_ENTRY));
	else
	    cur=cur->next=mp_alloc_zero(page_pool,sizeof(ARRAY_ENTRY));
	cur->obj=get_obj();
	PRINTOBJ(200,cur->obj);
    }
    in_get_char();
    if(!cur)			    /* empty dict */
	TRACE(200,"(empty)");
    else
	cur->next=NULL;		    /* the last element's next pointer is a terminator */
    TRACE(200,"End of Array");

    obj.type=OT_ARRAY;
    obj.value.a=start;
    return obj;
}

static OBJECT
get_dict(void) {
    OBJECT obj;
    DICT_ENTRY *start=NULL,*cur=NULL;
    int c;

    TRACE(200,"Dict:");
    while(in_check_nschar(SP_ANYWCOMM)!='>') {
	if(!cur)
	    cur=start=mp_alloc_zero(page_pool,sizeof(DICT_ENTRY));
	else
	    cur=cur->next=mp_alloc_zero(page_pool,sizeof(DICT_ENTRY));
	obj=get_obj();		    /* key - must be a name object */
	PRINTOBJ(200,obj);
	if(obj.type!=OT_NAME)
	    gerror(2200,"(get_dict) Key in the dictionary isn't a name object! (%d)",obj.type);
	cur->name=obj.value.s;
	obj=get_obj();		    /* value */
	PRINTOBJ(200,obj);
	cur->obj=obj;
    }
    in_get_char();
    c=in_get_char();
    if(c!='>')
	gerror(2200,"(get_dict) Syntax error at the end of dictionary (char %c)",c);
    if(!cur)			    /* empty dict */
	TRACE(200,"(empty)");
    else
	cur->next=NULL;		    /* the last element's next pointer is a terminator */
    TRACE(200,"End of Dict");

    obj.type=OT_DICT;
    obj.value.d=start;
    return obj;
}

void
parse_dict(DICT_ENTRY *dict_start,OBJ_GET *items) {
    int j;
    DICT_ENTRY *dp;

    for(j=0;items[j].name;items[j++].obj->type=OT_UNKNOWN);
    for (dp=dict_start;dp;dp=dp->next) {
	for(j=0;items[j].name;j++) {
	    if(strncmp(dp->name,items[j].name,127)==0) {
		*items[j].obj = dp->obj;
		break;
	    }
	}
    }
}

static void
getxrefsec(s32 start) {
    char buf[MAX_BUFF_SIZE], *ptr;
    uns i,j,o_first, o_count;
    OBJECT trailer,root,info,encrypt,fileid,prev;
    OBJ_GET trailerdict[]={{"Root",&root},{"Prev",&prev},{"Info",&info},{"Encrypt",&encrypt},{"ID",&fileid},{NULL,NULL}};
    int prevxrefsect=0;
    int xb,xi;
    int saveencr;

    TRACE(20,"xref pos: %d",start);
    pdf_seek(start);
    in_get_line(buf);
    if(memcmp(buf,"xref",5))
	gerror(2220,"(getxrefsec) 'xref' expected at pos %d",start);
    do {
	/*
	 * start of xref subsection: first line with start and count numbers and
	 * than line for each object follows
	 */
	in_get_line(buf);
	o_first=strtol(buf,&ptr,10);
	o_count=strtol(ptr,&ptr,10);
	TRACE(200,"Subsection start at %d with %d items",o_first,o_count);

	for(i=o_first;i<o_first+o_count;i++) {
	    if((j=in_get_line(buf))!=20)
		gerror(2220,"(getxrefsec) wrong length of xref line: %d (object %d)",j,i);
	    if(i>=xreft_size*xreft_size)
		gerror(2220,"(getxrefsec) Object number out of range: %d",i);
	    if(i==0)
		continue;
	    xb=i/xreft_size;
	    xi=i%xreft_size;
	    if(!xreftroot[xb])
		xreftroot[xb]=mp_alloc_zero(global_pool,sizeof(OBJ_START)*xreft_size);
	    if(xreftroot[xb][xi].position!=0)    /* already known object? */
		continue;
	    xreftroot[xb][xi].position  =strtol(buf,&ptr,10);
	    xreftroot[xb][xi].generation=strtol(ptr,&ptr,10);
	    switch(ptr[1]) {
		case 'n': break;	    /* everything is done */
		case 'f': xreftroot[xb][xi].position=-1;  /* a flag that this object was deleted */
		      break;
		default:  gerror(2220,"(getxrefsec) 'xref' table corrupted. 'n' or 'f' expected, '%c' found",ptr[1]);
	    }
	    TRACE(200,"Object %d stored: (%d,%d)",i,xreftroot[xb][xi].position,xreftroot[xb][xi].generation);
	}
    } while (is_numchar(pdf_check_char()));

    /* xref subsections are done, now look for the trailer */

    skip_space(SP_ANYSPACE);
    pdf_read(buf,7);
    if(memcmp(buf,"trailer",7) || skip_space(SP_ANYWCOMM)==0)
	gerror(2220,"(getxrefsec) Trailer keyword not found at %d",pdf_tell());
    TRACE(200,"The trailer dictionary at %d",pdf_tell());

    saveencr=encrypted;
    encrypted&=~2;		/* Trailer with file ID and Encrypt objects are never encrypted */

    trailer=get_obj();

    if(trailer.type!=OT_DICT)
	gerror(2220,"(getxrefsec) Trailer isn't a dictionary, but type %d",trailer.type);

    TRACE(10,"Trailer:");
    PRINTOBJ(10,trailer);
    parse_dict(trailer.value.d,trailerdict);
    obj_deref(&encrypt);
    obj_deref(&fileid);

    encrypted=saveencr;

    if(encrypt.type!=OT_UNKNOWN && encrypt.type!=OT_NULL)
	decrypt_init(encrypt,fileid);
    if(prev.type!=OT_UNKNOWN)
	    if(prev.type==OT_INT)
		    prevxrefsect=prev.value.n;
	    else
		    gerror(2220,"(getxrefsec) Can't locate Prev xref table");
    if(root.type==OT_OBJREF || root.type==OT_NULL) {
	TRACE(20,"Root: %d",root.value.n);
	if(!pdf_rootref && root.value.n)
	    pdf_rootref=root.value.n;
    }
    if(info.type==OT_OBJREF || info.type==OT_NULL) {
	TRACE(20,"Info: %d",info.value.n);
	if(!pdf_inforef && info.value.n)
	    pdf_inforef=info.value.n;
    }

    mp_flush(page_pool);

    if(prevxrefsect)
	getxrefsec(prevxrefsect);
}

static void
check_header(void)
{
  byte buf[16];
  pdf_seek(0);
  pdf_read(buf, 15);
  if (memcmp(buf, "%PDF-", 5))
    gerror(2200, "Missing PDF header");

  byte *ver = buf+5;
  byte *c = ver;
  while (c < buf+15 && (*c >= '0' && *c <= '9' || *c == '.'))
    c++;
  *c = 0;
  if (ver[0] != '1' || ver[1] != '.')
    gerror(2200, "PDF version %s not supported", ver);
  if (ver[2] > '5' || ver[3])
    pdf_warn("Format version %s unknown, but proceeding", ver);
}

static void
getxref(void) {
    char buf[MAX_BUFF_SIZE];
    int i;
    s32 pos;

    pdf_seek_end(-LOOK4STARTXREF);
    pdf_read(buf,LOOK4STARTXREF);
    TRACE(100,"BUF: %s",buf);

    buf[LOOK4STARTXREF]=0;
     /* 12=strlen("startxref 1 %%EOF") */
    for(i=LOOK4STARTXREF-12;i>0 && memcmp("startxref",buf+i,9);i--);
    if(i<=0)
	gerror(2220,"(getxref) 'startxref' not found");
    pos=i+=9; /* length("startxref") */
    while(is_space(buf[i],SP_ANYSPACE))
	i++;
    if(pos==i)
	gerror(2220,"(getxref) 'startxref ' not found");
    if((pos=atol(buf+i))==0)
	gerror(2220,"(getxref) no value of 'startxref'");

    xreftroot=(OBJ_START **)mp_alloc_zero(global_pool,sizeof(void *) * xreft_size);

    getxrefsec(pos);	/* file offset of the first xref section */
}

void
pdf_setup(void)
{
    pdf_seek_end(0);
    pdf_filesize=pdf_tell();
    set_input_method(pdf_in);

    if(pdf_filesize<LOOK4STARTXREF)
	gerror(2200,"(getxref) PDF file too small");
    check_header();
    getxref();
}
