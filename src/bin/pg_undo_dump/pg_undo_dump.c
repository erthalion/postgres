/*-------------------------------------------------------------------------
 *
 * pg_undo_dump.c - decode and display UNDO log
 *
 * Copyright (c) 2013-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_undo_dump/pg_undo_dump.c
 *-------------------------------------------------------------------------
 */

#define FRONTEND 1
#include "postgres.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/transam.h"
#include "access/undolog.h"
#include "access/undopage.h"
#include "access/undorecordset.h"
#include "common/logging.h"
#include "common/fe_memutils.h"
#include "rmgrdesc.h"

#include "getopt_long.h"

static const char *progname;

static bool show_chunks = false;
static bool show_metadata = false;
static bool show_records = false;

typedef struct UndoSegFile
{
	char	   *name;
	UndoLogNumber logno;
	UndoLogOffset offset;
} UndoSegFile;

/*
 * Open the file in the valid target directory.
 *
 * return a read only fd
 */
static int
open_file_in_directory(const char *directory, UndoSegFile *seg)
{
	int			fd = -1;
	char		fpath[MAXPGPATH];

	Assert(directory != NULL);

	snprintf(fpath, MAXPGPATH, "%s/%06X.%010zX",
			 directory, seg->logno, seg->offset);

	fd = open(fpath, O_RDONLY | PG_BINARY, 0);

	if (fd < 0)
		pg_log_error("could not open file \"%s\": %s",
					 seg->name, strerror(errno));
	return fd;
}

static void
usage(void)
{
	printf(_("%s decodes and displays PostgreSQL undo logs for debugging.\n\n"),
		   progname);
	printf(_("Usage:\n  %s [OPTION]... DATADIR\n\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" [-D, --pgdata=]DATADIR  data directory\n"));
	printf(_("  -c, --chunks           show chunk headers\n"));
	/* TODO Rename to -s / --slots ? */
	printf(_("  -m, --metadata         show metadata of the undo log files\n"));
	printf(_("  -r, --records          show undo log records\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf(_("\nReport bugs to <pgsql-bugs@lists.postgresql.org>.\n"));
}

/*
 * Each log consists of multiple segments, which need to be processed by the
 * offset. To ensure that the logs are sorted separately, sort first by logno,
 * then by offset.
 */
static int
undo_seg_compare(const void *s1, const void *s2)
{
	UndoSegFile *seg1 = (UndoSegFile *) s1;
	UndoSegFile *seg2 = (UndoSegFile *) s2;

	if (seg1->logno != seg2->logno)
		return seg1->logno > seg2->logno ? 1 : -1;

	if (seg1->offset != seg2->offset)
		return seg1->offset > seg2->offset ? 1 : -1;

	return 0;
}

/* Add new record types here if they are introduced. */
typedef union
{
	XactUndoRecordSetHeader xact;
	char		foo[4];
}			TypeHeader;

static void
print_chunk_info(UndoRecPtr start, UndoRecordSetChunkHeader *hdr,
				 UndoRecordSetType type, TypeHeader * type_header)
{
	UndoLogNumber logno = UndoRecPtrGetLogNo(start);
	UndoLogOffset off = UndoRecPtrGetOffset(start);

	printf("logno: %X, start: %010zX, prev: ", logno, off);
	if (hdr->previous_chunk != InvalidUndoRecPtr)
	{
		UndoLogNumber logno_prev = UndoRecPtrGetLogNo(hdr->previous_chunk);
		UndoLogOffset off_prev = UndoRecPtrGetOffset(hdr->previous_chunk);

		printf("%X.%010zX, ", logno_prev, off_prev);
	}
	else
		printf("<invalid>, ");
	printf("size: %zu, ", hdr->size);

	if (type_header)
	{
		if (type == URST_TRANSACTION)
		{
			XactUndoRecordSetHeader *hdr;
			TransactionId xid;

			hdr = &type_header->xact;
			xid = XidFromFullTransactionId(hdr->fxid);
			printf("type: transaction (xid=%u, dboid=%u)",
				   xid, hdr->dboid);
		}
		else if (type == URST_FOO)
		{
			printf("type: foo (%s)", type_header->foo);
		}
		else
		{
			pg_log_error("unrecognized URS type %d", type);
			return;
		}
	}

	printf("\n");
}

static Size rec_buf_usage = 0;
static Size rec_buf_off = 0;	/* The next record to process. */

/*
 * Add data to a "record buffer" and process as much as we can. data_start
 * tells where in the log the data starts.
 *
 * Pass NULL to only process the remaining data.
 */
static void
process_records(char *data, int size, UndoRecPtr data_start)
{
	static char *rec_buffer = NULL;
	static Size rec_buf_size = 0;
	static UndoRecPtr current_rec_start = InvalidUndoRecPtr;
	int			length_size = sizeof(((UndoNode *) NULL)->length);
	int			rmid_size = sizeof(((UndoNode *) NULL)->rmid);
	int			type_size = sizeof(((UndoNode *) NULL)->type);
	StringInfo	buf = makeStringInfo();

	/* Enlarge the buffer if needed. */
	if (data && (rec_buf_usage + size) > rec_buf_size)
	{
		rec_buf_size = rec_buf_usage + size;

		if (rec_buffer == NULL)
			rec_buffer = (char *) palloc(rec_buf_size);
		else
			rec_buffer = (char *) repalloc(rec_buffer, rec_buf_size);
	}

	/*
	 * Update the current record pointer. This should happen when called the
	 * first time for a chunk.
	 */
	if (current_rec_start == InvalidUndoRecPtr)
		current_rec_start = data_start;

	/* Append the data to the buffer. */
	if (data)
	{
		memcpy(rec_buffer + rec_buf_usage, data, size);
		rec_buf_usage += size;
	}

	while ((rec_buf_usage - rec_buf_off) >= length_size)
	{
		Size		rec_len;
		Size		rec_off_tmp = rec_buf_off;
		uint8		rmid,
					rec_type;
		WrittenUndoNode node;
		const RmgrDescData *desc;

		/* Read the record length info. */
		memcpy(&rec_len, rec_buffer + rec_off_tmp, length_size);
		rec_off_tmp += length_size;

		/* Read rmid if it's there. */
		if ((rec_buf_usage - rec_off_tmp) < rmid_size)
			break;
		memcpy(&rmid, rec_buffer + rec_off_tmp, rmid_size);
		rec_off_tmp += rmid_size;

		/* Read record type if it's there. */
		if ((rec_buf_usage - rec_off_tmp) < type_size)
			break;
		memcpy(&rec_type, rec_buffer + rec_off_tmp, type_size);
		rec_off_tmp += type_size;

		/* Process the record if it's there. */
		if ((rec_buf_usage - rec_off_tmp) < rec_len)
			break;

		node.location = current_rec_start;
		node.n.rmid = rmid;
		node.n.type = rec_type;
		node.n.length = rec_len;
		node.n.data = rec_buffer + rec_off_tmp;

		desc = &RmgrDescTable[rmid];
		resetStringInfo(buf);
		desc->undo_desc(buf, &node);
		printf("rmgr: %-11s len: %7zu, urp: %06X.%010zX, %s\n",
			   desc->rm_name,
			   rec_len,
			   (UndoLogNumber) UndoRecPtrGetLogNo(current_rec_start),
			   UndoRecPtrGetOffset(current_rec_start),
			   buf->data);

		rec_buf_off = rec_off_tmp + rec_len;
		current_rec_start += rec_len;
	}

	if (data == NULL)
	{
		/* Get ready to accept new record start pointer. */
		current_rec_start = InvalidUndoRecPtr;
		rec_buf_usage = 0;
		rec_buf_off = 0;
	}

	/* Move the unprocessed data to the beginning of the buffer. */
	if (data && rec_buf_off > 0)
	{
		memmove(rec_buffer, rec_buffer + rec_buf_off,
				rec_buf_usage - rec_buf_off);
		rec_buf_usage -= rec_buf_off;
		rec_buf_off = 0;
	}
}

/*
 * Process segments of a single log file. Return prematurely if any error is
 * encountered.
 *
 * prev_chunk is in/out argument that helps to maintain the pointer to the
 * previous chunk across calls.
 */
static void
process_log(const char *dir_path, UndoSegFile *first, int count)
{
	int			i;
	UndoSegFile *seg = first;
	UndoLogOffset off_expected = 0;
	char		buf[UndoLogSegmentSize];
	UndoRecPtr	current_chunk = InvalidUndoRecPtr;
	UndoRecordSetChunkHeader chunk_hdr;
	int			chunk_hdr_bytes_left = 0;
	UndoRecordSetType urs_type = URST_INVALID;
	int			type_hdr_size,
				type_hdr_bytes_left = 0;
	UndoLogOffset chunk_bytes_left = 0;
	TypeHeader	type_hdr;

	/* This is very unlikely, but easy to check. */
	if (count > (UndoLogMaxSize / UndoLogSegmentSize))
	{
		pg_log_error("log %d has too many segments", first->logno);
		return;
	}

	for (i = 0; i < count; i++)
	{
		int			seg_file;
		int			nread,
					j;
		char	   *p;

		/*
		 * Since the UNDO log is a continuous stream of changes, any hole
		 * terminates processing.
		 */
		if (seg->offset != off_expected)
		{
			pg_log_error("segment %010zX missing in log %d", off_expected,
						 seg->logno);
			return;
		}

		/* Open the segment file and read it. */
		seg_file = open_file_in_directory(dir_path, seg);
		if (seg_file < 0)
			return;

		nread = read(seg_file, buf, UndoLogSegmentSize);
		if (nread != UndoLogSegmentSize)
		{
			if (nread <= 0)
				pg_log_error("could not read from log file %s: %s",
							 seg->name, strerror(errno));
			else
				pg_log_error("could only read %d bytes out of %zu from log file %s: %s",
							 nread, UndoLogSegmentSize, seg->name,
							 strerror(errno));
			return;
		}

		/* Process the pages of the segment. */
		p = buf;
		for (j = 0; j < UndoLogSegmentSize / BLCKSZ; j++)
		{
			UndoPageHeaderData pghdr;
			int			page_offset = 0;
			int			page_usage;
			uint16		first_rec;
			uint16		first_chunk = 0;
			int			chunk_bytes_to_skip;

			/* The segment is not loaded aligned. */
			memcpy(&pghdr, p, SizeOfUndoPageHeaderData);

			page_usage = pghdr.ud_insertion_point;

			/*
			 * TODO Consider continuing the scan but only checking that all
			 * the remaining pages (including headers?) are uninitialized.
			 */
			if (page_usage == 0)
				break;

			if (page_usage < SizeOfUndoPageHeaderData || page_usage > BLCKSZ)
			{
				pg_log_error("page %d of the log segment \"%s\" has invalid ud_insertion_point: %d",
							 j, seg->name, page_usage);
				return;
			}

			first_rec = pghdr.ud_first_record;
			if (first_rec != 0 &&
				(first_rec < SizeOfUndoPageHeaderData ||
				 first_rec >= pghdr.ud_insertion_point))
			{
				pg_log_error("page %d of the log segment \"%s\" has invalid ud_first_record: %d",
							 j, seg->name, pghdr.ud_first_record);
				return;
			}

			/* The log should start with a chunk. */
			if (i == 0 && j == 0)
			{
				/* Check as much as we can of the page header. */
				if (pghdr.ud_first_chunk != SizeOfUndoPageHeaderData)
				{
					pg_log_error("the initial segment (\"%s\") does not start with a chunk immediately following the page header",
								 seg->name);
					return;
				}
				first_chunk = pghdr.ud_first_chunk;

				if (pghdr.ud_continue_chunk != InvalidUndoRecPtr)
				{
					pg_log_error("chunk continues on the initial segment \"%s\"",
								 seg->name);
					return;
				}

				current_chunk = MakeUndoRecPtr(seg->logno, seg->offset +
											   pghdr.ud_first_chunk);
				chunk_hdr_bytes_left = SizeOfUndoRecordSetChunkHeader;

				/*
				 * The size of the header of the first page cannot be included
				 * in any chunk size.
				 */
				chunk_bytes_to_skip = 0;
			}
			else
			{
				UndoRecPtr	cont = pghdr.ud_continue_chunk;

				/*
				 * If not at the beginning of the log, we should always keep
				 * track of the current chunk.
				 */
				Assert(current_chunk != InvalidUndoRecPtr);

				/*
				 * We're at the start of the current page, current_chunk must
				 * have been initialized earlier.
				 */
				Assert(current_chunk <
					   MakeUndoRecPtr(seg->logno, seg->offset + j * BLCKSZ));

				if (cont == InvalidUndoRecPtr || cont != current_chunk)
				{
					UndoLogNumber logno = UndoRecPtrGetLogNo(cont);
					UndoLogOffset offset = UndoRecPtrGetOffset(cont);

					pg_log_error("page %d of the log segment \"%s\" has invalid ud_continue_chunk %06X.%010zX",
								 j, seg->name, logno, offset);
				}

				/*
				 * The page header size must eventually be subtracted from
				 * chunk_bytes_left because it's included in the chunk size.
				 * However, since chunk_bytes_left is unsigned, we do not
				 * subtract anything from it if it's still zero.  This can
				 * happen if we're still reading the chunk header or the
				 * type-specific header. (The underflow should not be a
				 * problem because the chunk size will eventually be added,
				 * but it seems ugly and it makes debugging less convenient.)
				 */
				if (chunk_bytes_left > 0)
				{
					chunk_bytes_left -= SizeOfUndoPageHeaderData;
					chunk_bytes_to_skip = 0;
				}
				else
					chunk_bytes_to_skip = SizeOfUndoPageHeaderData;
			}

			page_offset = SizeOfUndoPageHeaderData;

			/* Process the page data. */
			while (page_offset < page_usage)
			{
				int			done,
							read_now;

				/*
				 * At any moment we're reading either the chunk or chunk
				 * header, but never both.
				 */
				Assert(!(chunk_hdr_bytes_left > 0 && chunk_bytes_left > 0));

				if (chunk_hdr_bytes_left > 0)
				{
					/*
					 * Retrieve the remaining part of the header that fits on
					 * the current page.
					 */
					done = SizeOfUndoRecordSetChunkHeader - chunk_hdr_bytes_left;
					read_now = Min(chunk_hdr_bytes_left, page_usage - page_offset);
					memcpy((char *) &chunk_hdr + done, p + page_offset, read_now);
					chunk_hdr_bytes_left -= read_now;
					page_offset += read_now;

					/*
					 * If we have got the whole header, get the chunk size,
					 * otherwise continue reading the header.
					 */
					if (chunk_hdr_bytes_left == 0)
					{
						if (chunk_hdr.size < SizeOfUndoRecordSetChunkHeader ||
						/* TODO Check the "usable bytes" instead. */
							chunk_hdr.size > UndoLogMaxSize)
						{
							UndoLogNumber logno = UndoRecPtrGetLogNo(current_chunk);
							UndoLogOffset offset = UndoRecPtrGetOffset(current_chunk);

							pg_log_error("chunk starting at %06X.%010zX has invalid size %zu",
										 logno, offset, chunk_hdr.size);
							return;
						}

						/*
						 * Invalid previous_chunk indicates that this is the
						 * first chunk of the undo record set. Read the type
						 * specific header if we can recognize it.
						 */
						if (chunk_hdr.previous_chunk == InvalidUndoRecPtr)
						{
							if (chunk_hdr.type == URST_TRANSACTION ||
								chunk_hdr.type == URST_FOO)
							{
								type_hdr_size = get_urs_type_header_size(chunk_hdr.type);
								type_hdr_bytes_left = type_hdr_size;
								urs_type = chunk_hdr.type;
								continue;
							}
							else if (chunk_hdr.type != URST_INVALID)
							{
								UndoLogNumber logno = UndoRecPtrGetLogNo(current_chunk);
								UndoLogOffset offset = UndoRecPtrGetOffset(current_chunk);

								pg_log_error("chunk starting at %06X.%010zX has invalid type header %d",
											 logno, offset, chunk_hdr.type);
								return;
							}
						}

						print_chunk_info(current_chunk, &chunk_hdr, URST_INVALID,
										 NULL);

						chunk_bytes_left = chunk_hdr.size - SizeOfUndoRecordSetChunkHeader;

						/*
						 * Account for the page header if we could not do so
						 * earlier.
						 */
						if (chunk_bytes_to_skip > 0)
						{
							chunk_bytes_left -= chunk_bytes_to_skip;
							chunk_bytes_to_skip = 0;
						}
					}
					else
						continue;
				}
				else if (type_hdr_bytes_left > 0)
				{
					done = type_hdr_size - type_hdr_bytes_left;
					read_now = Min(type_hdr_bytes_left, page_usage - page_offset);
					memcpy((char *) &type_hdr + done, p + page_offset, read_now);
					type_hdr_bytes_left -= read_now;
					page_offset += read_now;

					/* Have the whole type header? */
					if (type_hdr_bytes_left == 0)
					{
						if (show_chunks)
							print_chunk_info(current_chunk, &chunk_hdr, urs_type,
											 &type_hdr);

						chunk_bytes_left = chunk_hdr.size - SizeOfUndoRecordSetChunkHeader -
							type_hdr_size;

						/*
						 * Account for the page header if we could not do so
						 * earlier.
						 */
						if (chunk_bytes_to_skip > 0)
						{
							chunk_bytes_left -= chunk_bytes_to_skip;
							chunk_bytes_to_skip = 0;
						}
					}
					else
						continue;
				}

				/* Process the current chunk. */
				if (chunk_bytes_left > 0)
				{
					int			read_now;

					read_now = Min(chunk_bytes_left, page_usage - page_offset);

					/* Pass the data to the record processing code. */
					if (show_records && chunk_hdr.type == URST_TRANSACTION)
					{
						UndoRecPtr	data_start;

						data_start = MakeUndoRecPtr(seg->logno, seg->offset +
													j * BLCKSZ + page_offset);

						process_records(p + page_offset, read_now, data_start);
					}

					chunk_bytes_left -= read_now;
					page_offset += read_now;
				}

				/*
				 * If done with the current chunk, prepare to read the next
				 * one.
				 */
				if (chunk_bytes_left == 0)
				{
					/* Process the remaining records of the chunk. */
					process_records(NULL, 0, InvalidUndoRecPtr);

					chunk_hdr_bytes_left = SizeOfUndoRecordSetChunkHeader;
					/* The following chunk is becoming the current one. */
					current_chunk = MakeUndoRecPtr(seg->logno, seg->offset +
												   j * BLCKSZ + page_offset);

					/*
					 * Save the offset of the first chunk start, to check the
					 * value stored in the header.
					 */
					if (first_chunk == 0)
						first_chunk = page_offset;
				}
			}

			/* Check ud_first_chunk. */
			if (pghdr.ud_first_chunk != first_chunk)
			{
				/*
				 * current_chunk is where the next chunk should start, but
				 * that chunk might not exist yet. In such a case,
				 * ud_first_chunk is still zero and should not be checked.
				 */
				if (UndoRecPtrGetPageOffset(current_chunk) != page_offset)
				{
					pg_log_error("page %d of the log segment \"%s\" has invalid ud_first_chunk: %d",
								 j, seg->name, pghdr.ud_first_chunk);
					return;
				}
#ifdef USE_ASSERT_CHECKING
				else
					Assert(pghdr.ud_first_chunk == 0);
#endif							/* USE_ASSERT_CHECKING */
			}

			p += BLCKSZ;
		}

		/* Process the remaining records of the segment. */
		process_records(NULL, 0, InvalidUndoRecPtr);

		if (rec_buf_usage > rec_buf_off)
			pg_log_error("not all record data could be processed");

		close(seg_file);
		off_expected += UndoLogSegmentSize;
		seg++;
	}
}

static void
process_logs(const char *dir_path)
{
	DIR		   *dir;
	struct dirent *de;
	UndoSegFile *segments,
			   *seg,
			   *log_start;
	int			nseg,
				nseg_max,
				i;

	dir = opendir(dir_path);
	if (dir == NULL)
	{
		pg_log_error("could not open directory \"%s\": %m", dir_path);
		exit(1);
	}

	nseg_max = 8;
	nseg = 0;
	segments = (UndoSegFile *) palloc(nseg_max * sizeof(UndoSegFile));

	/* First, collect information on all segments. */
	while (errno = 0, (de = readdir(dir)) != NULL)
	{
		UndoLogNumber logno;
		UndoLogOffset offset;
		int			offset_high;
		int			offset_low;

		if ((strcmp(de->d_name, ".") == 0) ||
			(strcmp(de->d_name, "..") == 0))
			continue;

		if (strlen(de->d_name) != 17 ||
			sscanf(de->d_name, "%06X.%02X%08X",
				   &logno, &offset_high, &offset_low) != 3)
		{
			pg_log_info("unexpected file \"%s\" in \"%s\"", de->d_name, dir_path);
			continue;
		}

		offset = ((UndoLogOffset) offset_high << 32) | offset_low;

		if (nseg >= nseg_max)
		{
			nseg_max *= 2;
			segments = (UndoSegFile *) repalloc(segments,
												nseg_max * sizeof(UndoSegFile));
		}
		seg = &segments[nseg++];
		seg->name = pg_strdup(de->d_name);
		seg->logno = logno;
		seg->offset = offset;
	}

	if (errno)
	{
		pg_log_error("could not read directory \"%s\": %m", dir_path);
		goto cleanup;
	}

	if (closedir(dir))
	{
		pg_log_error("could not close directory \"%s\": %m", dir_path);
		goto cleanup;
	}

	/*
	 * The segments need to be processed in the offset order, so sort them.
	 */
	qsort((void *) segments, nseg, sizeof(UndoSegFile), undo_seg_compare);

	/* Process the per-log sequences. */
	seg = log_start = segments;
	for (i = 0; i < nseg; i++)
	{
		/* Reached the end or a new log? */
		if (i == nseg || seg->logno != log_start->logno)
		{
			process_log(dir_path, log_start, seg - log_start);
			log_start = seg;
		}

		seg++;
	}
	if (seg > log_start)
		process_log(dir_path, log_start, seg - log_start);

cleanup:
	for (i = 0; i < nseg; i++)
	{
		seg = &segments[i];
		pfree(seg->name);
	}
	pfree(segments);
}

static void
process_metadata(const char *dir_path)
{
	DIR		   *dir;
	struct dirent *de;
	XLogRecPtr	last = InvalidXLogRecPtr;
	int			fd = -1;
	char		fpath[MAXPGPATH];
	UndoLogNumber next_logno,
				num_logs;
	int			i,
				nread;
	pg_crc32c	crc;

	dir = opendir(dir_path);
	if (dir == NULL)
	{
		pg_log_error("could not open directory \"%s\": %m", dir_path);
		exit(1);
	}

	/* First, select the latest checkpoint */
	while (errno = 0, (de = readdir(dir)) != NULL)
	{
		XLogRecPtr	current;

		if ((strcmp(de->d_name, ".") == 0) ||
			(strcmp(de->d_name, "..") == 0))
			continue;

		if (strlen(de->d_name) != 16 ||
			sscanf(de->d_name, "%016" INT64_MODIFIER "X", &current) != 1)
		{
			pg_log_info("unexpected file \"%s\" in \"%s\"", de->d_name, dir_path);
			continue;
		}

		if (last == InvalidXLogRecPtr || current > last)
			last = current;
	}

	if (last == InvalidXLogRecPtr)
	{
		pg_log_error("No slot metadata found in \"%s\"", dir_path);
		exit(1);
	}

	snprintf(fpath, MAXPGPATH, "pg_undo/%016" INT64_MODIFIER "X", last);

	fd = open(fpath, O_RDONLY | PG_BINARY, 0);

	if (fd < 0)
	{
		pg_log_error("could not open file \"%s\": %s", fpath,
					 strerror(errno));
		exit(1);
	}

	nread = read(fd, &next_logno, sizeof(next_logno));
	if (nread != sizeof(next_logno))
	{
		pg_log_error("could not read next_logno");
		exit(1);
	}

	nread = read(fd, &num_logs, sizeof(num_logs));
	if (nread != sizeof(num_logs))
	{
		pg_log_error("could not read num_logs");
		exit(1);
	}

	for (i = 0; i < num_logs; i++)
	{
		UndoLogMetaData slot;

		nread = read(fd, &slot, sizeof(UndoLogMetaData));
		if (nread == 0)
			break;
		if (nread != sizeof(UndoLogMetaData))
		{
			if (nread <= 0)
				pg_log_error("could not read from file %s: %s", fpath,
							 strerror(errno));
			else
				pg_log_error("could only read %d bytes out of %zu from file %s: %s",
							 nread, sizeof(UndoLogMetaData), fpath,
							 strerror(errno));
			exit(1);
		}

		/* TODO Print out all the fields. */
		printf("logno: %d, discard: " UndoRecPtrFormat
			   ", insert: " UndoRecPtrFormat ", size: %lu \n",
			   slot.logno,
			   MakeUndoRecPtr(slot.logno, slot.discard),
			   MakeUndoRecPtr(slot.logno, slot.insert),
			   slot.size);
	}

	nread = read(fd, &crc, sizeof(pg_crc32c));
	if (nread == sizeof(pg_crc32c))
	{
		/* TODO Verify CRC. */
	}
	else
	{
		if (nread < 0)
			pg_log_error("could not read from file %s: %s", fpath,
						 strerror(errno));
		else
			pg_log_error("could only read %d bytes out of %zu from file %s: %s",
						 nread, sizeof(pg_crc32c), fpath,
						 strerror(errno));

		pg_log_warning("CRC not found");
	}

	close(fd);
}

int
main(int argc, char **argv)
{
	char	   *DataDir = NULL;

	static struct option long_options[] = {
		{"pgdata", required_argument, NULL, 'D'},
		{"chunks", no_argument, NULL, 'c'},
		{"metadata", no_argument, NULL, 'm'},
		{"records", no_argument, NULL, 'r'},
		{NULL, 0, NULL, 0}
	};

	int			option;
	int			optindex = 0;

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_undo_dump"));
	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage();
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_undo_dump (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((option = getopt_long(argc, argv, "D:cmr",
								 long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case 'D':
				DataDir = optarg;
				break;

			case 'c':
				show_chunks = true;
				break;

			case 'm':
				show_metadata = true;
				break;

			case 'r':
				show_records = true;
				break;

			default:
				goto bad_argument;
		}
	}

	if ((optind + 1) < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind + 1]);
		goto bad_argument;
	}

	if (DataDir == NULL && optind < argc)
		DataDir = argv[optind++];

	if (DataDir == NULL)
	{
		pg_log_error("no data directory specified");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	/*
	 * The 'l' option is not required to show logs, but if 'r' or 's' is
	 * specified,
	 */
	if (show_metadata)
	{
		if (show_chunks)
		{
			pg_log_error("the '-c' and '-m' options are mutually exclusive");
			exit(1);
		}

		if (show_records)
		{
			pg_log_error("the '-r' and '-m' options are mutually exclusive");
			exit(1);
		}
	}

	/* List of logs is the default output. */
	if (!(show_chunks || show_records || show_metadata))
		show_chunks = true;

	if (chdir(DataDir) < 0)
	{
		pg_log_error("could not change directory to \"%s\": %m",
					 DataDir);
		exit(1);
	}

	if (show_chunks || show_records)
		/* TODO Process tablespaces too. */
		process_logs("base/undo");
	else
		process_metadata("pg_undo");

	return EXIT_SUCCESS;

bad_argument:
	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
	return EXIT_FAILURE;
}
