#include "postgres_fe.h"
#include "common/zpq_stream.h"
#include "c.h"
#include "pg_config.h"

static int zpq_algorithm_impl;

typedef struct
{
	char    (*name)(void);
	ZpqStream* (*create)(zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg);
	ssize_t (*read)(ZpqStream *zs, void *buf, size_t size,
								   void *source, size_t source_size);
	ssize_t (*write)(ZpqStream *zs, void const *buf, size_t size,
									void *target, size_t target_size);
	void    (*free)(ZpqStream *zs);
	char const* (*error)(ZpqStream *zs);
	size_t  (*buffered)(ZpqStream *zs);
} ZpqAlgorithm;


#if HAVE_LIBZSTD

#include <malloc.h>
#include <zstd.h>

#define ZSTD_BUFFER_SIZE (8*1024)
#define ZSTD_COMPRESSION_LEVEL 1

typedef struct ZstdStream
{
	ZSTD_CStream*  tx_stream;
	ZSTD_DStream*  rx_stream;
	ZSTD_outBuffer tx;
	ZSTD_inBuffer  rx;
	size_t         tx_not_flushed; /* Amount of datas in internal zstd buffer */
	size_t         tx_buffered;    /* Data which is consumed by zpq_read but not yet sent */
	zpq_tx_func    tx_func;
	zpq_rx_func    rx_func;
	void*          arg;
	char const*    rx_error;    /* Decompress error message */
	size_t         tx_total;
	size_t         tx_total_raw;
	size_t         rx_total;
	size_t         rx_total_raw;
	char           tx_buf[ZSTD_BUFFER_SIZE];
	char           rx_buf[ZSTD_BUFFER_SIZE];
} ZstdStream;

static ZpqStream*
zstd_create(zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg)
{
	ZstdStream* zs = (ZstdStream*)malloc(sizeof(ZstdStream));
	zs->tx_stream = ZSTD_createCStream();
	ZSTD_initCStream(zs->tx_stream, ZSTD_COMPRESSION_LEVEL);
	zs->rx_stream = ZSTD_createDStream();
	ZSTD_initDStream(zs->rx_stream);
	zs->tx.dst = zs->tx_buf;
	zs->tx.pos = 0;
	zs->tx.size = ZSTD_BUFFER_SIZE;
	zs->rx.src = zs->rx_buf;
	zs->rx.pos = 0;
	zs->rx.size = 0;
	zs->rx_func = rx_func;
	zs->tx_func = tx_func;
	zs->tx_buffered = 0;
	zs->tx_not_flushed = 0;
	zs->rx_error = NULL;
	zs->arg = arg;
	zs->tx_total = zs->tx_total_raw = 0;
	zs->rx_total = zs->rx_total_raw = 0;
	return (ZpqStream*)zs;
}

static ssize_t
zstd_read(ZpqStream *zstream, void *buf, size_t size,
							  void *source, size_t source_size)
{
	ZstdStream* zs = (ZstdStream*)zstream;
	ssize_t rc;
	ZSTD_outBuffer out;
	out.dst = buf;
	out.pos = 0;
	out.size = size;

	while (1)
	{
		rc = ZSTD_decompressStream(zs->rx_stream, &out, &zs->rx);
		if (ZSTD_isError(rc))
		{
			zs->rx_error = ZSTD_getErrorName(rc);
			return ZPQ_DECOMPRESS_ERROR;
		}
		/* Return result if we fill requested amount of bytes or read operation was performed */
		if (out.pos != 0)
		{
			zs->rx_total_raw += out.pos;
			return out.pos;
		}
		if (zs->rx.pos == zs->rx.size)
		{
			zs->rx.pos = zs->rx.size = 0; /* Reset rx buffer */
		}
		rc = zs->rx_func(zs->arg, (char*)zs->rx.src + zs->rx.size, ZSTD_BUFFER_SIZE - zs->rx.size);
		if (rc > 0) /* read fetches some data */
		{
			zs->rx.size += rc;
			zs->rx_total += rc;
		}
		else /* read failed */
		{
			zs->rx_total_raw += out.pos;
			return rc;
		}
	}
}

static ssize_t
zstd_write(ZpqStream *zstream, void const *buf, size_t size,
							   void *target, size_t target_size)
{
	ZstdStream* zs = (ZstdStream*)zstream;
	ssize_t rc;
	ZSTD_inBuffer in_buf;
	in_buf.src = buf;
	in_buf.pos = 0;
	in_buf.size = size;

	do
	{
		if (zs->tx.pos == 0) /* Compress buffer is empty */
		{
			zs->tx.dst = zs->tx_buf; /* Reset pointer to the beginning of buffer */

			if (in_buf.pos < size) /* Has something to compress in input buffer */
				ZSTD_compressStream(zs->tx_stream, &zs->tx, &in_buf);

			if (in_buf.pos == size) /* All data is compressed: flushed internal zstd buffer */
			{
				zs->tx_not_flushed = ZSTD_flushStream(zs->tx_stream, &zs->tx);
			}
		}
		rc = zs->tx_func(zs->arg, zs->tx.dst, zs->tx.pos);
		if (rc > 0)
		{
			zs->tx.pos -= rc;
			zs->tx.dst = (char*)zs->tx.dst + rc;
			zs->tx_total += rc;
		}
		else
		{
			zs->tx_buffered = zs->tx.pos;
			zs->tx_total_raw += in_buf.pos;
			return rc;
		}
	} while (zs->tx.pos == 0 && (in_buf.pos < size || zs->tx_not_flushed)); /* repeat sending data until first partial write */

	zs->tx_total_raw += in_buf.pos;
	zs->tx_buffered = zs->tx.pos;
	return in_buf.pos;
}

static void
zstd_free(ZpqStream *zstream)
{
	ZstdStream* zs = (ZstdStream*)zstream;
	if (zs != NULL)
	{
		ZSTD_freeCStream(zs->tx_stream);
		ZSTD_freeDStream(zs->rx_stream);
		free(zs);
	}
}

static char const*
zstd_error(ZpqStream *zstream)
{
	ZstdStream* zs = (ZstdStream*)zstream;
	return zs->rx_error;
}

static size_t
zstd_buffered(ZpqStream *zstream)
{
	ZstdStream* zs = (ZstdStream*)zstream;
	return zs != NULL ? zs->tx_buffered + zs->tx_not_flushed : 0;
}

static char
zstd_name(void)
{
	return 'f';
}

#endif

#if HAVE_LIBZ

#include <malloc.h>
#include <zlib.h>

/*#define ZLIB_BUFFER_SIZE 8192*/
#define ZLIB_COMPRESSION_LEVEL 1

/*typedef struct ZlibStream*/
/*{*/
	/*z_stream tx;*/
	/*z_stream rx;*/

	/*zpq_tx_func    tx_func;*/
	/*zpq_rx_func    rx_func;*/
	/*void*          arg;*/

	/*size_t         tx_buffered;*/

	/*Bytef          tx_buf[ZLIB_BUFFER_SIZE];*/
	/*Bytef          rx_buf[ZLIB_BUFFER_SIZE];*/
/*} ZlibStream;*/

static ZpqStream*
zlib_create(zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg)
{
	int rc;
	ZlibStream* zs = (ZlibStream*)malloc(sizeof(ZlibStream));
	memset(&zs->tx, 0, sizeof(zs->tx));
	zs->tx.next_out = zs->tx_buf;
	zs->tx.avail_out = ZLIB_BUFFER_SIZE;
	zs->tx_buffered = 0;
	rc = deflateInit(&zs->tx, ZLIB_COMPRESSION_LEVEL);
	if (rc != Z_OK)
	{
		free(zs);
		return NULL;
	}
	Assert(zs->tx.next_out == zs->tx_buf && zs->tx.avail_out == ZLIB_BUFFER_SIZE);

	memset(&zs->rx, 0, sizeof(zs->tx));
	zs->rx.next_in = zs->rx_buf;
	zs->rx.avail_in = ZLIB_BUFFER_SIZE;
	rc = inflateInit(&zs->rx);
	if (rc != Z_OK)
	{
		free(zs);
		return NULL;
	}
	Assert(zs->rx.next_in == zs->rx_buf && zs->rx.avail_in == ZLIB_BUFFER_SIZE);
	zs->rx.avail_in = 0;

	zs->rx_func = rx_func;
	zs->tx_func = tx_func;
	zs->arg = arg;

	return (ZpqStream*)zs;
}

static ssize_t
zlib_read(ZpqStream *zstream, void *buf, size_t size, void *source, size_t source_size)
{
	ZlibStream* zs = (ZlibStream*)zstream;
	int rc = 0;
	zs->rx.next_out = buf;
	zs->rx.avail_out = size;

	if (source_size > 0)
	{
		zs->rx.next_in = zs->rx_buf;
		zs->rx.avail_in = source_size;
	}

	while (1)
	{
		if (zs->rx.avail_in > 0) /* If there is some data in receiver buffer, then decompress it */
		{
			rc = inflate(&zs->rx, Z_SYNC_FLUSH);
			if (rc != Z_OK)
			{
				return ZPQ_DECOMPRESS_ERROR;
			}
			if (zs->rx.avail_out != size)
			{
				return size - zs->rx.avail_out;
			}
		}
	}
}

static ssize_t
zlib_write(ZpqStream *zstream, void const *buf, size_t size, void *target, size_t target_size)
{
	ZlibStream* zs = (ZlibStream*)zstream;
    int rc;
	zs->tx.next_in = (Bytef *)buf;
	zs->tx.avail_in = size;
	zs->tx.next_out = target;
	zs->tx.avail_out = target_size;

	do
	{
		if (zs->tx.avail_out == ZLIB_BUFFER_SIZE) /* Compress buffer is empty */
		{
			if (zs->tx.avail_in != 0) /* Has something in input buffer */
			{
				rc = deflate(&zs->tx, Z_SYNC_FLUSH);
				Assert(rc == Z_OK);
			}
		}
	} while (zs->tx.avail_out == ZLIB_BUFFER_SIZE && zs->tx.avail_in != 0); /* repeat sending data until first partial write */

	zs->tx_buffered = 0;

	return ZLIB_BUFFER_SIZE - zs->tx.avail_out;
}

static void
zlib_free(ZpqStream *zstream)
{
	ZlibStream* zs = (ZlibStream*)zstream;
	if (zs != NULL)
	{
		inflateEnd(&zs->rx);
		deflateEnd(&zs->tx);
		free(zs);
	}
}

static char const*
zlib_error(ZpqStream *zstream)
{
	ZlibStream* zs = (ZlibStream*)zstream;
	return zs->rx.msg;
}

static size_t
zlib_buffered(ZpqStream *zstream)
{
	ZlibStream* zs = (ZlibStream*)zstream;
	return zs != NULL ? zs->tx_buffered : 0;
}

static char
zlib_name(void)
{
	return 'z';
}

#endif

static ZpqAlgorithm const zpq_algorithms[] = 
{
#if HAVE_LIBZSTD
	{zstd_name, zstd_create, zstd_read, zstd_write, zstd_free, zstd_error, zstd_buffered},
#endif
#if HAVE_LIBZ
	{zlib_name, zlib_create, zlib_read, zlib_write, zlib_free, zlib_error, zlib_buffered},
#endif
	{NULL}
};

ZpqStream*
zpq_create(zpq_tx_func tx_func, zpq_rx_func rx_func, void *arg)
{
	return zpq_algorithms[zpq_algorithm_impl].create(tx_func, rx_func, arg);
}

ssize_t
zpq_read(ZpqStream *zs, void *buf, size_t size,
						void *source, size_t source_size)
{
	return zpq_algorithms[zpq_algorithm_impl].read(zs, buf, size,
													   source, source_size);
}

ssize_t
zpq_write(ZpqStream *zs, void const *buf, size_t size,
						 void *target, size_t target_size)
{
	return zpq_algorithms[zpq_algorithm_impl].write(zs, buf, size,
														target, target_size);
}

void
zpq_free(ZpqStream *zs)
{
	zpq_algorithms[zpq_algorithm_impl].free(zs);
}

char const*
zpq_error(ZpqStream *zs)
{
	return zpq_algorithms[zpq_algorithm_impl].error(zs);
}


size_t
zpq_buffered(ZpqStream *zs)
{
	return zpq_algorithms[zpq_algorithm_impl].buffered(zs);
}

void
zpq_get_supported_algorithms(char algorithms[ZPQ_MAX_ALGORITHMS])
{
	int i;
	for (i = 0; zpq_algorithms[i].name != NULL; i++)
	{
		Assert(i < ZPQ_MAX_ALGORITHMS);
		algorithms[i] = zpq_algorithms[i].name();
	}
	Assert(i < ZPQ_MAX_ALGORITHMS);
	algorithms[i] = '\0';
}


bool
zpq_set_algorithm(char name)
{
	int i;
	if (name != ZPQ_NO_COMPRESSION)
	{
		for (i = 0; zpq_algorithms[i].name != NULL; i++)
		{
			if (zpq_algorithms[i].name() == name)
			{
				zpq_algorithm_impl = i;
				return true;
			}
		}
	}
	return false;
}

