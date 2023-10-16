/*-------------------------------------------------------------------------
 *
 * queryjumble.h
 *	  Query normalization and fingerprinting.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/nodes/queryjumble.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef QUERYJUMBLE_H
#define QUERYJUMBLE_H

#include "nodes/parsenodes.h"
#include "nodes/nodeFuncs.h"

/*
 * Struct for tracking locations/lengths of constants during normalization
 */
typedef struct LocationLen
{
	int			location;		/* start offset in query text */
	int			length;			/* length in bytes, or -1 to ignore */

	/*
	 * Indicates the constant represents the beginning or the end of a merged
	 * constants interval. The value shows how many constants were merged away
	 * (up to a power of 10), or in other words the order of manitude for
	 * number of merged constants (i.e. how many digits it has). Otherwise the
	 * value is 0, indicating that no merging was performed.
	 */
	int			magnitude;
} LocationLen;

/*
 * Working state for computing a query jumble and producing a normalized
 * query string
 */
typedef struct JumbleState
{
	/* Jumble of current query tree */
	unsigned char *jumble;

	/* Number of bytes used in jumble[] */
	Size		jumble_len;

	/* Array of locations of constants that should be removed */
	LocationLen *clocations;

	/* Allocated length of clocations array */
	int			clocations_buf_size;

	/* Current number of valid entries in clocations array */
	int			clocations_count;

	/* highest Param id we've seen, in order to start normalization correctly */
	int			highest_extern_param_id;
} JumbleState;

/* Values for the compute_query_id GUC */
enum ComputeQueryIdType
{
	COMPUTE_QUERY_ID_OFF,
	COMPUTE_QUERY_ID_ON,
	COMPUTE_QUERY_ID_AUTO,
	COMPUTE_QUERY_ID_REGRESS
};

/* GUC parameters */
extern PGDLLIMPORT int compute_query_id;
extern PGDLLIMPORT bool query_id_const_merge;
extern PGDLLIMPORT bool query_id_const_merge_threshold;

extern const char *CleanQuerytext(const char *query, int *location, int *len);
extern JumbleState *JumbleQuery(Query *query);
extern void EnableQueryId(void);

extern PGDLLIMPORT bool query_id_enabled;

/*
 * Returns whether query identifier computation has been enabled, either
 * directly in the GUC or by a module when the setting is 'auto'.
 */
static inline bool
IsQueryIdEnabled(void)
{
	if (compute_query_id == COMPUTE_QUERY_ID_OFF)
		return false;
	if (compute_query_id == COMPUTE_QUERY_ID_ON)
		return true;
	return query_id_enabled;
}

#endif							/* QUERYJUMBLE_H */
