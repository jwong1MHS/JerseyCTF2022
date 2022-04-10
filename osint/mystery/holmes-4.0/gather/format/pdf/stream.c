/*
 *  Sherlock PDF Parser: Decoding of streams
 *
 *  (c) 2002--2003 Milan Vancura <milan@ucw.cz>
 *  (c) 2003--2004 Martin Mares <mj@ucw.cz>
 */

#include "sherlock/sherlock.h"
#include "ucw/fastbuf.h"
#include "ucw/mempool.h"
#include "gather/gather.h"
#include "gather/format/pdf/pdf.h"

#include <zlib.h>

struct fchain;

typedef int(*fcf_t)(struct fchain *);

struct fchain {
    fcf_t	    func;
    byte	    buf[STREAMBLKLEN];
    byte	    *data;
    byte	    *end;
    struct fchain   *prev;
    uintptr_t	    priv;
};

static struct fchain *sf_top;
static uns *sa_top;
uns stream_array[MAX_STREAMS+1];

static int
stream_refill(struct fastbuf *f) {
    struct fchain *c=sf_top;

    if(!sf_top)
	    return 0;
    if(c->data==c->end)
	c->func(c);
    TRACE(10001,"(stream_refill) Buffer %d-%d",(uns)(c->data-c->buf),(uns)(c->end-c->buf));
    if(c->data<c->end) {
	f->buffer=f->bptr=c->data;
	f->bufend=f->bstop=c->end;
	f->pos+=c->end-c->data;
	c->data=c->end;
	return 1;
    } else {
	TRACE(200,"(stream_refill) Stream object #%d([%d])",*sa_top,(uns)(sa_top-stream_array));
	set_input_method_raw(pdf_in);
	if(*sa_top && !check_i_stream(*sa_top++)) {
	    set_input_method_raw(pdf_stream_in);
	    return stream_refill(f);
	} else {
	    set_input_method_raw(pdf_stream_in);
	    return 0;
	}
    }
}

void
stream_init(struct fastbuf *f) {
    bzero(f,sizeof(*f));
    f->refill=stream_refill;
    f->name="PDF stream";
    sa_top=stream_array;
    if(check_i_stream(*sa_top++))
	sf_top=NULL;
}

/* stream block_filter functions */

static int
sf_get_nschar(int sptype, struct fchain *g) {
    int c,r=0;

    while(r==0) {
	if(g->data==g->end)
	    r=g->func(g);
	while(g->data < g->end) {
	    c=*g->data++;
	    if(!is_space(c,sptype))
		return c;
	}
    }
    return -1;
}

static inline void
bufshift(struct fchain *f, int i) {
    memmove(f->buf,f->data,i);
    f->end=f->buf+i;
    f->data=f->buf;
}

static int
sf_read(struct fchain *cur) {
    int l=cur->end-cur->data;
    int m;

    bufshift(cur,l);

    TRACE(10001, "(sf_read) before: Buffer %d-%d, remains: %d",(uns)(cur->data-cur->buf),(uns)(cur->end-cur->buf),(int)cur->priv);
    if(cur->priv==0)
	return 1;
    l=MIN(STREAMBLKLEN-l,(int)cur->priv);
    m=pdf_read(cur->end,l);
    ASSERT(m);
    cur->end+=m;
    cur->priv-=m;
    TRACE(10001, "(sf_read) after: Buffer %d-%d, remains: %d",(uns)(cur->data-cur->buf),(uns)(cur->end-cur->buf),(int)cur->priv);
    return 0;
}

static int
sf_ahex(struct fchain *cur) {
    int c,d=cur->end-cur->data;
    int ret=0;

    bufshift(cur,d);
    while(cur->end < cur->buf+STREAMBLKLEN) {
	if((c=sf_get_nschar(SP_ANYSPACE,cur->prev))<0) {
	    ret=1;
	    break;
	}
	if((d=sf_get_nschar(SP_ANYSPACE,cur->prev))<0) {
	    *cur->end++=hexbyte(c,'0');
	    ret=1;
	    break;
	} else {
	    *cur->end++=hexbyte(c,d);
	}
    }
    TRACE(100, "(sf_ahex) Buffer %d-%d",(uns)(cur->data-cur->buf),(uns)(cur->end-cur->buf));
    return ret;
}

static int
sf_inflate(struct fchain *cur) {
    z_stream *d_stream = (z_stream *) cur->priv;
    int err=Z_OK;

    bufshift(cur,cur->end-cur->data);
    if (!d_stream) {
	d_stream = mp_alloc_zero(sf_pool, sizeof(*d_stream));
	cur->priv = (uintptr_t) d_stream;
	err=inflateInit(d_stream);
	if(err!=Z_OK)
	    gerror(2223,"(sf_inflate) InflateInit error: %d",err);
    } else if (d_stream->next_in==NULL) {
	TRACE(100, "(sf_inflate) Signalling EOF");
	return 1;
    }

    d_stream->next_out=cur->end;
    d_stream->avail_out=cur->buf+STREAMBLKLEN-cur->end;

    for(;d_stream->avail_out>0 && err==Z_OK;) {
	if (cur->prev->data >= cur->prev->end)
	    cur->prev->func(cur->prev);
	d_stream->next_in=cur->prev->data;
	d_stream->avail_in=cur->prev->end-cur->prev->data;
	TRACE(100,"(sf_inflate) before decompression: avail_in=%d, total_in=%ld, avail_out=%d, total_out=%ld",d_stream->avail_in,d_stream->total_in,d_stream->avail_out,d_stream->total_out);
	err=inflate(d_stream,Z_SYNC_FLUSH);
	cur->prev->data=d_stream->next_in;
	cur->end=d_stream->next_out;
    }

    TRACE(100, "(sf_inflate) Buffer %d-%d, zlib_err: %d",(uns)(cur->data-cur->buf),(uns)(cur->end-cur->buf),err);
    if(err==Z_STREAM_END) {
	if((err=inflateEnd(d_stream))!=Z_OK)
	     gerror(2223,"(sf_inflate) Error while inflateEnd: %d",err);
	d_stream->next_in=NULL;
	return (cur->end <= cur->data);
    }
    if(err!=Z_OK)
	gerror(2223,"(sf_inflate) Error while inflate: %d",err);
    return 0;
}

static int
sf_a85(struct fchain *cur) {
    int n,c=cur->end-cur->data;
    u32 out;

    if (cur->priv)
	return 1;
    bufshift(cur,c);
    while(cur->end < cur->buf+STREAMBLKLEN-4) {
	out=0;
	for(n=0;n<5;n++) {
	    c=sf_get_nschar(SP_ANYSPACE,cur->prev);
	    if(c<0)
		gerror(2223,"(sf_a85) Input error: unexpected EOD");
	    if(c=='z' && n==0) {
		cur->end[0]=cur->end[1]=cur->end[2]=cur->end[3]=0;
		cur->end+=4;
		break;
	    }
	    if(c=='~')
		if(sf_get_nschar(SP_ANYSPACE,cur->prev)=='>' && sf_get_nschar(SP_ANYSPACE,cur->prev)<0) {
		    cur->priv=1;
		    return 1;
		}
	    if(c<'!' || c>'u')
		gerror(2223,"(sf_a85) Input error: unexpected char %c",c);
	    out=out*85+c-'!';
	}
	if(c=='z' && n==0)
	    continue;
	for(c=n;c<5;c++)
	    out=out*85;
	for(c=0;c<n-1;c++) {
	    cur->end[n-2-c]=out&255;
	    out>>=8;
	}
	cur->end+=n-1;
    }
    TRACE(100, "(sf_a85) Buffer %d-%d",(uns)(cur->data-cur->buf),(uns)(cur->end-cur->buf));
    return 0;
}

static int
sf_rc4(struct fchain *cur) {
    int avail_in=0,avail_out,n;

    bufshift(cur,cur->end-cur->data);
    if (!cur->priv) {
	TRACE(100, "(sf_rc4) Signalling EOF");
	return 1;
    }

    avail_out=cur->buf+STREAMBLKLEN-cur->end;

    for(;avail_out>0;) {
	if (cur->prev->data >= cur->prev->end)
	    cur->prev->func(cur->prev);
	if(!(avail_in=cur->prev->end-cur->prev->data))
	    break;
	TRACE(100,"(sf_rc4) before decryption: avail_in=%d, avail_out=%d",avail_in,avail_out);
	n=MIN(avail_out,avail_in);
	rc4_conv((struct rc4_state *) cur->priv,cur->end,cur->prev->data,n);
	cur->prev->data+=n;
	cur->end+=n;
	avail_out-=n;
    }

#if 0
    HTRACE(100,cur->data,(u32) (cur->end-cur->data),"(rc4 log)");
#endif

    TRACE(100, "(sf_rc4) Buffer %d-%d, avail_in: %d",(uns)(cur->data-cur->buf),(uns)(cur->end-cur->buf),avail_in);
    if(!avail_in) {
	cur->priv=(uintptr_t) NULL;
	return (cur->end <= cur->data);
    }
    return 0;
}

static fcf_t
sf_func(byte *fname) {
    if(!strcmp(fname, "ASCIIHexDecode"))
	return sf_ahex;
    if(!strcmp(fname, "ASCII85Decode"))
	return sf_a85;
    if(!strcmp(fname, "FlateDecode"))
	return sf_inflate;
    return NULL;
}

static int
check_stream(s32 l) {
    char buf[10];
    int i;

    skip_space(SP_ANYWCOMM);
    pdf_read(buf,6);
    if(memcmp(buf,"stream",6))
	return -1;
    switch(pdf_get_char()) {
	case '\r':
	    if(pdf_get_char()!='\n')
		return -1;
	    break;
	case '\n':
	    break;
	default:
	    return -1;
    }

    i=pdf_tell();
    pdf_seek(i+l);
    skip_space(SP_ANYWCOMM);
    pdf_read(buf,9);
    if(memcmp(buf,"endstream",9) || skip_space(SP_ANYWCOMM)==0)
	return -1;
    pdf_seek(i);
    return 0;
}

int
check_i_stream(s32 n) {
    OBJECT obj,length,filter,dpar,extfile;
    OBJ_GET streamdict[]={{"Length",&length},{"Filter",&filter},{"DecodeParms",&dpar},{"F",&extfile},{NULL,NULL}};
    ARRAY_ENTRY atmp,*ai;
    fcf_t sff;
    struct fchain *tf;

    sf_top=NULL;
    encrypted|=0x01;
    obj=get_i_obj(n);
    if(obj.type!=OT_DICT)
	return -1;
    parse_dict(obj.value.d,streamdict);
    if(extfile.type!=OT_UNKNOWN || dpar.type!=OT_UNKNOWN)
	return -1;

    /* deref length without any wrong side-effect */
    savefilepos();
    struct rc4_state rc4_backup = rc4_state;

    obj_deref(&length);
    if(length.type!=OT_INT)
	return -1;

    rc4_state = rc4_backup;
    restorefilepos();

    if(filter.type!=OT_UNKNOWN && filter.type!=OT_NAME && filter.type!=OT_ARRAY)
	return -1;
    mp_flush(sf_pool);
    sf_top=mp_alloc(sf_pool,sizeof(struct fchain));
    sf_top->func=sf_read;
    sf_top->end=sf_top->data=sf_top->buf;
    sf_top->priv=length.value.n;
    if((encrypted&0x03)==3) {
	tf=mp_alloc(sf_pool,sizeof(struct fchain));
	tf->prev=sf_top;
	sf_top=tf;
	tf->end=tf->data=tf->buf;
	rc4_state_stream = rc4_state;
	tf->priv=(uintptr_t) &rc4_state_stream;
	tf->func=sf_rc4;
	TRACE(200,"(check_i_stream) sff=RC4DECRYPT");
	encrypted&=~1;		/* streams are encrypted _outside_, no RC4 decryption inside */
    }
    if(filter.type!=OT_UNKNOWN) {
	if(filter.type==OT_ARRAY) {
	    ai=filter.value.a;
	} else {
	    atmp.obj=filter;
	    atmp.next=NULL;
	    ai=&atmp;
	}
	for(;ai;ai=ai->next) {
	    if(ai->obj.type!=OT_NAME)
		return -1;
	    if(!(sff=sf_func(ai->obj.value.s))) {
		pdf_warn("Unknown stream filter %s", ai->obj.value.s);
		return -1;
	    }
	    TRACE(200,"(check_i_stream) sff=%s",sff==sf_ahex?"ASCII HEX":(sff==sf_a85?"ASCII 85":"INFLATE"));
	    tf=mp_alloc(sf_pool,sizeof(struct fchain));
	    tf->prev=sf_top;
	    sf_top=tf;
	    tf->end=tf->data=tf->buf;
	    tf->priv=0;
	    tf->func=sff;
	}
    }
    return check_stream(length.value.n);
}
