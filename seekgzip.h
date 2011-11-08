#ifndef __SEEKGZIP_H__
#define __SEEKGZIP_H__

struct tag_seekgzip_t; typedef struct tag_seekgzip seekgzip_t;

enum {
    SEEKGZIP_SUCCESS=0,
    SEEKGZIP_ERROR=-1024,
    SEEKGZIP_OPENERROR,
    SEEKGZIP_READERROR,
    SEEKGZIP_WRITEERROR,
    SEEKGZIP_DATAERROR,
    SEEKGZIP_OUTOFMEMORY,
    SEEKGZIP_IMCOMPATIBLE,
    SEEKGZIP_ZLIBERROR,
};

int
seekgzip_build(
    const char *filename
    );

seekgzip_t*
seekgzip_open(
    const char *filename,
    int *errorcode
    );

void
seekgzip_close(
    seekgzip_t* zs
    );

void
seekgzip_seek(
    seekgzip_t *zs,
    off_t offset
    );

off_t
seekgzip_tell(
    seekgzip_t *zs
    );

int
seekgzip_read(
    seekgzip_t* zs,
    void *buffer,
    int size
    );

int
seekgzip_error(
    seekgzip_t* sgz
    );

#endif/*__SEEKGZIP_H__*/

