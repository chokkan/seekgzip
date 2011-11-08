#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <zlib.h>
#include "seekgzip.h"

#define SEEKGZIP_OPTIMIZATION

/*===== Begin of the portion of zran.c =====*/

/* zran.c -- example of zlib/gzip stream indexing and random access
 * Copyright (C) 2005 Mark Adler
 * For conditions of distribution and use, see copyright notice in zlib.h
   Version 1.0  29 May 2005  Mark Adler */

/* Illustrate the use of Z_BLOCK, inflatePrime(), and inflateSetDictionary()
   for random access of a compressed file.  A file containing a zlib or gzip
   stream is provided on the command line.  The compressed stream is decoded in
   its entirety, and an index built with access points about every SPAN bytes
   in the uncompressed output.  The compressed file is left open, and can then
   be read randomly, having to decompress on the average SPAN/2 uncompressed
   bytes before getting to the desired block of data.

   An access point can be created at the start of any deflate block, by saving
   the starting file offset and bit of that block, and the 32K bytes of
   uncompressed data that precede that block.  Also the uncompressed offset of
   that block is saved to provide a referece for locating a desired starting
   point in the uncompressed stream.  build_index() works by decompressing the
   input zlib or gzip stream a block at a time, and at the end of each block
   deciding if enough uncompressed data has gone by to justify the creation of
   a new access point.  If so, that point is saved in a data structure that
   grows as needed to accommodate the points.

   To use the index, an offset in the uncompressed data is provided, for which
   the latest accees point at or preceding that offset is located in the index.
   The input file is positioned to the specified location in the index, and if
   necessary the first few bits of the compressed data is read from the file.
   inflate is initialized with those bits and the 32K of uncompressed data, and
   the decompression then proceeds until the desired offset in the file is
   reached.  Then the decompression continues to read the desired uncompressed
   data from the file.

   Another approach would be to generate the index on demand.  In that case,
   requests for random access reads from the compressed data would try to use
   the index, but if a read far enough past the end of the index is required,
   then further index entries would be generated and added.

   There is some fair bit of overhead to starting inflation for the random
   access, mainly copying the 32K byte dictionary.  So if small pieces of the
   file are being accessed, it would make sense to implement a cache to hold
   some lookahead and avoid many calls to extract() for small lengths.

   Another way to build an index would be to use inflateCopy().  That would
   not be constrained to have access points at block boundaries, but requires
   more memory per access point, and also cannot be saved to file due to the
   use of pointers in the state.  The approach here allows for storage of the
   index in a file.
 */

#define SPAN 1048576L       /* desired distance between access points */
#define WINSIZE 32768U      /* sliding window size */
#define CHUNK 16384         /* file input buffer size */

/* access point entry */
struct point {
    off_t out;          /* corresponding offset in uncompressed data */
    off_t in;           /* offset in input file of first full byte */
    int bits;           /* number of bits (1-7) from byte at in - 1, or 0 */
    unsigned char window[WINSIZE];  /* preceding 32K of uncompressed data */
};

/* access point list */
struct access {
    int have;           /* number of list entries filled in */
    int size;           /* number of list entries allocated */
    struct point *list; /* allocated list */
};

/* Deallocate an index built by build_index() */
static void free_index(struct access *index)
{
    if (index != NULL) {
        free(index->list);
        free(index);
    }
}

/* Add an entry to the access point list.  If out of memory, deallocate the
   existing list and return NULL. */
static struct access *addpoint(struct access *index, int bits,
    off_t in, off_t out, unsigned left, unsigned char *window)
{
    struct point *next;

    /* if list is empty, create it (start with eight points) */
    if (index == NULL) {
        index = (struct access*)malloc(sizeof(struct access));
        if (index == NULL) return NULL;
        index->list = (struct point*)malloc(sizeof(struct point) << 3);
        if (index->list == NULL) {
            free(index);
            return NULL;
        }
        index->size = 8;
        index->have = 0;
    }

    /* if list is full, make it bigger */
    else if (index->have == index->size) {
        index->size <<= 1;
        next = (struct point*)realloc(index->list, sizeof(struct point) * index->size);
        if (next == NULL) {
            free_index(index);
            return NULL;
        }
        index->list = next;
    }

    /* fill in entry and increment how many we have */
    next = index->list + index->have;
    next->bits = bits;
    next->in = in;
    next->out = out;
    if (left)
        memcpy(next->window, window + WINSIZE - left, left);
    if (left < WINSIZE)
        memcpy(next->window + left, window, WINSIZE - left);
    index->have++;

    /* return list, possibly reallocated */
    return index;
}

#ifdef  SEEKGZIP_OPTIMIZATION
struct point *findpoint(struct access *index, off_t offset)
{
    int half, len = index->have;
    struct point *first = &index->list[0], *middle;

    /* equivalent to std::upper_bound() */
    while (0 < len) {
        half = (len >> 1);
        middle = first + half;
        if (offset < middle->out) {
            len = half;
        } else {
            first = middle + 1;
            len = len - half - 1;
        }
    }

    /* decrement the point */
    return (first == &index->list[0] ? NULL : first-1);
}
#endif/*SEEKGZIP_OPTIMIZATION*/

/* Make one entire pass through the compressed stream and build an index, with
   access points about every span bytes of uncompressed output -- span is
   chosen to balance the speed of random access against the memory requirements
   of the list, about 32K bytes per access point.  Note that data after the end
   of the first zlib or gzip stream in the file is ignored.  build_index()
   returns the number of access points on success (>= 1), Z_MEM_ERROR for out
   of memory, Z_DATA_ERROR for an error in the input file, or Z_ERRNO for a
   file read error.  On success, *built points to the resulting index. */
static int build_index(FILE *in, off_t span, struct access **built)
{
    int ret;
    off_t totin, totout;        /* our own total counters to avoid 4GB limit */
    off_t last;                 /* totout value of last access point */
    struct access *index;       /* access points being generated */
    z_stream strm;
    unsigned char input[CHUNK];
    unsigned char window[WINSIZE];

    /* initialize inflate */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;
    ret = inflateInit2(&strm, 47);      /* automatic zlib or gzip decoding */
    if (ret != Z_OK)
        return ret;

    /* inflate the input, maintain a sliding window, and build an index -- this
       also validates the integrity of the compressed data using the check
       information at the end of the gzip or zlib stream */
    totin = totout = last = 0;
    index = NULL;               /* will be allocated by first addpoint() */
    strm.avail_out = 0;
    do {
        /* get some compressed data from input file */
        strm.avail_in = fread(input, 1, CHUNK, in);
        if (ferror(in)) {
            ret = Z_ERRNO;
            goto build_index_error;
        }
        if (strm.avail_in == 0) {
            ret = Z_DATA_ERROR;
            goto build_index_error;
        }
        strm.next_in = input;

        /* process all of that, or until end of stream */
        do {
            /* reset sliding window if necessary */
            if (strm.avail_out == 0) {
                strm.avail_out = WINSIZE;
                strm.next_out = window;
            }

            /* inflate until out of input, output, or at end of block --
               update the total input and output counters */
            totin += strm.avail_in;
            totout += strm.avail_out;
            ret = inflate(&strm, Z_BLOCK);      /* return at end of block */
            totin -= strm.avail_in;
            totout -= strm.avail_out;
            if (ret == Z_NEED_DICT)
                ret = Z_DATA_ERROR;
            if (ret == Z_MEM_ERROR || ret == Z_DATA_ERROR)
                goto build_index_error;
            if (ret == Z_STREAM_END)
                break;

            /* if at end of block, consider adding an index entry (note that if
               data_type indicates an end-of-block, then all of the
               uncompressed data from that block has been delivered, and none
               of the compressed data after that block has been consumed,
               except for up to seven bits) -- the totout == 0 provides an
               entry point after the zlib or gzip header, and assures that the
               index always has at least one access point; we avoid creating an
               access point after the last block by checking bit 6 of data_type
             */
            if ((strm.data_type & 128) && !(strm.data_type & 64) &&
                (totout == 0 || totout - last > span)) {
                index = addpoint(index, strm.data_type & 7, totin,
                                 totout, strm.avail_out, window);
                if (index == NULL) {
                    ret = Z_MEM_ERROR;
                    goto build_index_error;
                }
                last = totout;
            }
        } while (strm.avail_in != 0);
    } while (ret != Z_STREAM_END);

    /* clean up and return index (release unused entries in list) */
    (void)inflateEnd(&strm);
    index = (struct access*)realloc(index, sizeof(struct point) * index->have);
    index->size = index->have;
    *built = index;
    return index->size;

    /* return error */
  build_index_error:
    (void)inflateEnd(&strm);
    if (index != NULL)
        free_index(index);
    return ret;
}

/* Use the index to read len bytes from offset into buf, return bytes read or
   negative for error (Z_DATA_ERROR or Z_MEM_ERROR).  If data is requested past
   the end of the uncompressed data, then extract() will return a value less
   than len, indicating how much as actually read into buf.  This function
   should not return a data error unless the file was modified since the index
   was generated.  extract() may also return Z_ERRNO if there is an error on
   reading or seeking the input file. */
static int extract(FILE *in, struct access *index, off_t offset,
                  unsigned char *buf, int len)
{
    int ret, skip;
    z_stream strm;
    struct point *here;
    unsigned char input[CHUNK];
    unsigned char discard[WINSIZE];

    /* proceed only if something reasonable to do */
    if (len < 0)
        return 0;

    /* find where in stream to start */
#ifdef  SEEKGZIP_OPTIMIZATION
    here = findpoint(index, offset);
    if (here == NULL) {
        /* possibly out of range. */
        return 0;
    }
#else
    here = index->list;
    ret = index->have;
    while (--ret && here[1].out <= offset)
        here++;
#endif/*SEEKGZIP_OPTIMIZATION*/

    /* initialize file and inflate state to start there */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;
    ret = inflateInit2(&strm, -15);         /* raw inflate */
    if (ret != Z_OK)
        return ret;
    ret = fseeko(in, here->in - (here->bits ? 1 : 0), SEEK_SET);
    if (ret == -1)
        goto extract_ret;
    if (here->bits) {
        ret = getc(in);
        if (ret == -1) {
            ret = ferror(in) ? Z_ERRNO : Z_DATA_ERROR;
            goto extract_ret;
        }
        (void)inflatePrime(&strm, here->bits, ret >> (8 - here->bits));
    }
    (void)inflateSetDictionary(&strm, here->window, WINSIZE);

    /* skip uncompressed bytes until offset reached, then satisfy request */
    offset -= here->out;
    strm.avail_in = 0;
    skip = 1;                               /* while skipping to offset */
    do {
        /* define where to put uncompressed data, and how much */
        if (offset == 0 && skip) {          /* at offset now */
            strm.avail_out = len;
            strm.next_out = buf;
            skip = 0;                       /* only do this once */
        }
        if (offset > WINSIZE) {             /* skip WINSIZE bytes */
            strm.avail_out = WINSIZE;
            strm.next_out = discard;
            offset -= WINSIZE;
        }
        else if (offset != 0) {             /* last skip */
            strm.avail_out = (unsigned)offset;
            strm.next_out = discard;
            offset = 0;
        }

        /* uncompress until avail_out filled, or end of stream */
        do {
            if (strm.avail_in == 0) {
                strm.avail_in = fread(input, 1, CHUNK, in);
                if (ferror(in)) {
                    ret = Z_ERRNO;
                    goto extract_ret;
                }
                if (strm.avail_in == 0) {
                    ret = Z_DATA_ERROR;
                    goto extract_ret;
                }
                strm.next_in = input;
            }
            ret = inflate(&strm, Z_NO_FLUSH);       /* normal inflate */
            if (ret == Z_NEED_DICT)
                ret = Z_DATA_ERROR;
            if (ret == Z_MEM_ERROR || ret == Z_DATA_ERROR)
                goto extract_ret;
            if (ret == Z_STREAM_END)
                break;
        } while (strm.avail_out != 0);

        /* if reach end of stream, then don't keep trying to get more */
        if (ret == Z_STREAM_END)
            break;

        /* do until offset reached and requested data read, or stream ends */
    } while (skip);

    /* compute number of uncompressed bytes read after offset */
    ret = skip ? 0 : len - strm.avail_out;

    /* clean up and return bytes read or error */
  extract_ret:
    (void)inflateEnd(&strm);
    return ret;
}

/*===== End of the portion of zran.c =====*/



static char *get_index_file(const char *target)
{
    char *idx = (char*)malloc(strlen(target) + 4 + 1);
    if (idx == NULL) {
        return NULL;
    }
    strcpy(idx, target);
    strcat(idx, ".idx");
    return idx;    
}

static int write_uint32(gzFile gz, uint32_t v)
{
    return gzwrite(gz, &v, sizeof(v));
}

static uint32_t read_uint32(gzFile gz)
{
    uint32_t v;
    gzread(gz, &v, sizeof(v));
    return v;
}

struct tag_seekgzip
{
    FILE *fp;
    struct access index;
    off_t offset;
    int errorcode;
};

int seekgzip_build(const char *target)
{
    int i, len, ret = SEEKGZIP_SUCCESS;
    FILE *fp = NULL;
    struct access *index = NULL;
    char *target_idx = NULL;
    gzFile gz = NULL;

    // Open the target gzip file.
    fp = fopen(target, "rb");
    if (fp == NULL) {
        ret = SEEKGZIP_OPENERROR;
        goto force_exit;
    }

    // Build an index for the file.
    len = build_index(fp, SPAN, &index);
    if (len < 0) {
        switch (len) {
        case Z_MEM_ERROR:
            ret = SEEKGZIP_OUTOFMEMORY;
        case Z_DATA_ERROR:
            ret = SEEKGZIP_DATAERROR;
        case Z_ERRNO:
            ret = SEEKGZIP_READERROR;
        default:
            ret = SEEKGZIP_ERROR;
        }
        goto force_exit;
    }

    // Close the target file.
    fclose(fp);
    fp = NULL;

    // Prepare the name for the index file.
    target_idx = get_index_file(target);
    if (target_idx == NULL) {
        ret = SEEKGZIP_OUTOFMEMORY;
        goto force_exit;
    }

    // Open the index file for writing.
    gz = gzopen(target_idx, "wb");
    if (gz == NULL) {
        ret = SEEKGZIP_OPENERROR;
        goto force_exit;
    }

    // Write a header.
    gzwrite(gz, "ZSEK", 4);
    write_uint32(gz, (uint32_t)sizeof(off_t));
    write_uint32(gz, (uint32_t)index->have);

    // Write out entry points.
    for (i = 0;i < index->have;++i) {
        gzwrite(gz, &index->list[i].out, sizeof(off_t));
        gzwrite(gz, &index->list[i].in, sizeof(off_t));
        gzwrite(gz, &index->list[i].bits, sizeof(int));
        gzwrite(gz, index->list[i].window, WINSIZE);
    }

force_exit:
    if (gz != NULL) {
        gzclose(gz);
    }
    if (target_idx != NULL) {
        free(target_idx);
    }
    if (index != NULL) {
        free_index(index);
    }
    if (fp != NULL) {
        fclose(fp);
    }

    return ret;
}

seekgzip_t* seekgzip_open(const char *target, int *errorcode)
{
    int i, ret = SEEKGZIP_SUCCESS;
    FILE *fp = NULL;
    gzFile gz = NULL;
    char *target_idx = NULL;
    seekgzip_t *zs = NULL;

    // Open the target gzip file for reading.
    fp = fopen(target, "rb");
    if (fp == NULL) {
        ret = SEEKGZIP_OPENERROR;
        goto error_exit;
    }

    // Prepare the name for the index file.
    target_idx = get_index_file(target);
    if (target_idx == NULL) {
        ret = SEEKGZIP_OUTOFMEMORY;
        goto error_exit;
    }

    // Open the index file for reading.
    gz = gzopen(target_idx, "rb");
    if (gz == NULL) {
        ret = SEEKGZIP_OPENERROR;
        goto error_exit;
    }

    // Read the magic string.
    ret = SEEKGZIP_IMCOMPATIBLE;
    if (gzgetc(gz) != 'Z') goto error_exit;
    if (gzgetc(gz) != 'S') goto error_exit;
    if (gzgetc(gz) != 'E') goto error_exit;
    if (gzgetc(gz) != 'K') goto error_exit;
    ret = SEEKGZIP_SUCCESS;

    // Check the size of off_t.
    if (read_uint32(gz) != sizeof(off_t)) {
        ret = SEEKGZIP_IMCOMPATIBLE;
        goto error_exit;
    }

    // Allocate a seekgzip_t instance.
    zs = (seekgzip_t*)malloc(sizeof(seekgzip_t));
    if (zs == NULL) {
        ret = SEEKGZIP_OUTOFMEMORY;
        goto error_exit;
    }
    memset(zs, 0, sizeof(*zs));

    // Read the number of entry points.
    zs->index.have = zs->index.size = read_uint32(gz);

    // Allocate an array for entry points.
    zs->index.list = (struct point*)malloc(sizeof(struct point) * zs->index.have);
    if (zs->index.list == NULL) {
        ret = SEEKGZIP_OUTOFMEMORY;
        goto error_exit;
    }

    // Read entry points.
    for (i = 0;i < zs->index.have;++i) {
        gzread(gz, &zs->index.list[i].out, sizeof(off_t));
        gzread(gz, &zs->index.list[i].in, sizeof(off_t));
        gzread(gz, &zs->index.list[i].bits, sizeof(int));
        gzread(gz, zs->index.list[i].window, WINSIZE);
    }

    // Close the index filiiiie.
    if (gzclose(gz) != 0) {
        ret = SEEKGZIP_ZLIBERROR;
        goto error_exit;
    }

    free(target_idx);

    zs->fp = fp;
    zs->offset = 0;
    zs->errorcode = 0;

    if (errorcode != NULL) {
        *errorcode = 0;
    }
    return zs;

error_exit:
    if (zs != NULL) {
        if (zs->index.list != NULL) {
            free(zs->index.list);
        }
        free(zs);
    }
    if (gz != NULL) {
        gzclose(gz);
    }
    if (target_idx != NULL) {
        free(target_idx);
    }
    if (fp != NULL) {
        fclose(fp);
    }

    if (errorcode != NULL) {
        *errorcode = ret;
    }
    return NULL;
}

void seekgzip_close(seekgzip_t* zs)
{
    if (zs != NULL) {
        if (zs->fp != NULL) {
            fclose(zs->fp);
        }
        if (zs->index.list != NULL) {
            free(zs->index.list);
        }
        free(zs);
    }
}

void seekgzip_seek(seekgzip_t *zs, off_t offset)
{
    zs->offset = offset;
}

off_t seekgzip_tell(seekgzip_t *zs)
{
    return zs->offset;
}

int seekgzip_read(seekgzip_t* zs, void *buffer, int size)
{
    int len = extract(zs->fp, &zs->index, zs->offset, (unsigned char*)buffer, size);
    if (0 < len) {
        zs->offset += len;
    }
    return len;
}

int seekgzip_error(seekgzip_t* sgz)
{
    return sgz->errorcode;
}

#ifdef BUILD_UTILITY

static void seekgzip_error(int ret)
{
    switch (ret) {
    case SEEKGZIP_ERROR:
        fprintf(stderr, "ERROR: An unknown error occurred.\n");
        break;
    case SEEKGZIP_OPENERROR:
        fprintf(stderr, "ERROR: Failed to open a file.\n");
        break;
    case SEEKGZIP_READERROR:
        fprintf(stderr, "ERROR: Failed to read a file.\n");
        break;
    case SEEKGZIP_WRITEERROR:
        fprintf(stderr, "ERROR: Failed to write a file.\n");
        break;
    case SEEKGZIP_DATAERROR:
        fprintf(stderr, "ERROR: The file is corrupted.\n");
        break;
    case SEEKGZIP_OUTOFMEMORY:
        fprintf(stderr, "ERROR: Out of memory.\n");
        break;
    case SEEKGZIP_IMCOMPATIBLE:
        fprintf(stderr, "ERROR: The imcompatible file.\n");
        break;
    case SEEKGZIP_ZLIBERROR:
        fprintf(stderr, "ERROR: An error occurred in zlib.\n");
        break;
    }
}

int main(int argc, char *argv[])
{
    int ret = 0;

    if (argc != 3) {
        printf("This utility maintains an index for random (seekable) access of a gzip file.\n");
        printf("USAGE:\n");
        printf("    %s -b <FILE>\n", argv[0]);
        printf("        Build an index file \"$FILE.idx\" for the gzip file $FILE.\n");
        printf("    %s <FILE> [BEGIN:END]\n", argv[0]);
        printf("        Output the content of the gzip file $FILE of offset range [BEGIN:END).\n");
        return 0;

    } else if (strcmp(argv[1], "-b") == 0) {
        const char *target = argv[2];

        printf("Building an index: %s.idx\n", target);
        printf("Filesize up to: %d bit\n", sizeof(off_t) * 8);

        ret = seekgzip_build(target);
        if (ret != 0) {
            seekgzip_error(ret);
            return 1;
        }
        return 0;

    } else {
        char *arg = argv[2], *p = NULL;
        off_t begin = 0, end = (off_t)-1;
        seekgzip_t* zs = seekgzip_open(argv[1], NULL);
        if (zs == NULL) {
            fprintf(stderr, "ERROR: Failed to open the index file.\n");
            return 1;
        }

        p = strchr(arg, '-');
        if (p == NULL) {
            begin =(off_t)strtoull(arg, NULL, 10);
            end = begin+1;
        } else if (p == arg) {
            begin = 0;
            end = (off_t)strtoull(p+1, NULL, 10);
        } else if (p == arg + strlen(arg) - 1) {
            *p = 0;
            begin = (off_t)strtoull(arg, NULL, 10);
        } else {
            *p++ = 0;
            begin =(off_t)strtoull(arg, NULL, 10);
            end =(off_t)strtoull(p, NULL, 10);
        }

        seekgzip_seek(zs, begin);

        while (begin < end) {
            int read;
            char buffer[CHUNK];
            off_t size = (end - begin);
            if (CHUNK < size) {
                size = CHUNK;
            }
            read = seekgzip_read(zs, buffer, (int)size);
            if (0 < read) {
                fwrite(buffer, read, sizeof(char), stdout);
                begin += read;
            } else if (read == 0) {
                break;
            } else {
                fprintf(stderr, "ERROR: An error occurred while reading the gzip file.\n");
                ret = 1;
                break;
            }
        }
    
        seekgzip_close(zs);
        return ret;
    }
}

#endif
