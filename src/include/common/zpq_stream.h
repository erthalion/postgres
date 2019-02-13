/*
 * zpq_stream.h
 *     Streaiming compression for libpq
 */

#ifndef ZPQ_STREAM_H
#define ZPQ_STREAM_H

#include <stdlib.h>

#define ZPQ_IO_ERROR (-1)
#define ZPQ_DECOMPRESS_ERROR (-2)
#define ZPQ_MAX_ALGORITHMS (8)
#define ZPQ_NO_COMPRESSION 'n'

#define ZPQ_READ_BUFFER 0
#define ZPQ_WRITE_BUFFER 1

#if HAVE_LIBZ || HAVE_LIBZSTD
#define USE_COMPRESSION
#endif

struct ZpqStream;
typedef struct ZpqStream ZpqStream;

typedef ssize_t(*zpq_tx_func)(void* arg, void const* data, size_t size);
typedef ssize_t(*zpq_rx_func)(void* arg, void* data, size_t size);

ZpqStream* zpq_create(zpq_tx_func tx_func, zpq_rx_func rx_func, void* arg);
ssize_t zpq_read(ZpqStream* zs, void* buf, size_t size,
								void *source, size_t source_size);
ssize_t zpq_write(ZpqStream* zs, void const* buf, size_t size,
								 void *target, size_t target_size);
char const* zpq_error(ZpqStream* zs);
size_t zpq_buffered(ZpqStream* zs);
void* zpq_buffer(ZpqStream* zs, int type);
size_t zpq_buffer_size(ZpqStream* zs, int type);
size_t zpq_read_drain(ZpqStream* zs, void *ptr, size_t len);
void zpq_free(ZpqStream* zs);

void zpq_get_supported_algorithms(char algorithms[ZPQ_MAX_ALGORITHMS]);
bool zpq_set_algorithm(char name);

#endif
