/*-------------------------------------------------------------------------
 *
 * toyam_handler.c
 *	  a toy table access method code
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/toytable/toyam_handler.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"

#include "access/multixact.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/catalog.h"
#include "catalog/storage.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "storage/bufmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(toytableam_handler);

typedef struct
{
	TableScanDescData scan;

	int			tupidx;
} ToyScanDescData;
typedef ToyScanDescData *ToyScanDesc;

static const TupleTableSlotOps *
toyam_slot_callbacks(Relation relation)
{
	return &TTSOpsVirtual;
}

static TableScanDesc toyam_scan_begin(Relation rel,
							 Snapshot snapshot,
							 int nkeys, struct ScanKeyData *key,
							 ParallelTableScanDesc pscan,
							 bool allow_strat,
							 bool allow_sync,
							 bool allow_pagemode,
							 bool is_bitmapscan,
							 bool is_samplescan,
							 bool temp_snap)
{
	ToyScanDesc tscan;

	tscan = palloc0(sizeof(ToyScanDescData));
	tscan->scan.rs_rd = rel;
	tscan->scan.rs_snapshot = snapshot;
	tscan->scan.rs_nkeys = nkeys;
	tscan->scan.rs_bitmapscan = is_bitmapscan;
	tscan->scan.rs_samplescan = is_samplescan;
	tscan->scan.rs_allow_strat = allow_strat;
	tscan->scan.rs_allow_sync = allow_sync;
	tscan->scan.rs_temp_snap = temp_snap;
	tscan->scan.rs_parallel = pscan;

	tscan->tupidx = 0;

	return &tscan->scan;
}

static void
toyam_scan_end(TableScanDesc scan)
{
	pfree(scan);
}

static void
toyam_scan_rescan(TableScanDesc scan, struct ScanKeyData *key,
				  bool set_params, bool allow_strat,
				  bool allow_sync, bool allow_pagemode)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static bool
toyam_scan_getnextslot(TableScanDesc scan,
					   ScanDirection direction,
					   TupleTableSlot *slot)
{
	ToyScanDesc tscan = (ToyScanDesc) scan;

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;

	/*
	 * Return a constant 1 rows. Every int4 attribute gets
	 * a running count, everything else is NULL.
	 */
	if (tscan->tupidx < 10)
	{
		TupleDesc desc = RelationGetDescr(tscan->scan.rs_rd);

		tscan->tupidx++;

		for (AttrNumber attno = 1; attno <= desc->natts; attno++)
		{
			Form_pg_attribute att = &desc->attrs[attno - 1];
			Datum		d;
			bool		isnull;

			if (att->atttypid == INT4OID)
			{
				d = tscan->tupidx;
				isnull = false;
			}
			else
			{
				d = (Datum) 0;
				isnull = true;
			}

			slot->tts_values[attno - 1] = d;
			slot->tts_isnull[attno - 1] = isnull;
		}

		ItemPointerSet(&slot->tts_tid, 1, tscan->tupidx);
		slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
		slot->tts_flags &= ~TTS_FLAG_EMPTY;

		return true;
	}
	else
		return false;
}

static Size
toyam_parallelscan_estimate(Relation rel)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static Size
toyam_parallelscan_initialize(Relation rel,
							  ParallelTableScanDesc pscan)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static void
toyam_parallelscan_reinitialize(Relation rel,
								ParallelTableScanDesc pscan)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static struct IndexFetchTableData *
toyam_index_fetch_begin(Relation rel)
{
	IndexFetchTableData *tfetch = palloc0(sizeof(IndexFetchTableData));

	tfetch->rel = rel;

	return tfetch;
}

static void
toyam_index_fetch_reset(struct IndexFetchTableData *data)
{
}

static void
toyam_index_fetch_end(struct IndexFetchTableData *data)
{
	pfree(data);
}

static bool
toyam_index_fetch_tuple(struct IndexFetchTableData *scan,
						ItemPointer tid,
						Snapshot snapshot,
						TupleTableSlot *slot,
						bool *call_again, bool *all_dead)
{
	TupleDesc desc = RelationGetDescr(scan->rel);
	int			tupidx;

	if (ItemPointerGetBlockNumber(tid) != 1)
		return false;

	tupidx = ItemPointerGetOffsetNumber(tid);
	if (tupidx < 1 || tupidx > 10)
		return false;

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;

	/* Return same data as toyam_scan_getnextslot does */
	for (AttrNumber attno = 1; attno <= desc->natts; attno++)
	{
		Form_pg_attribute att = &desc->attrs[attno - 1];
		Datum		d;
		bool		isnull;

		if (att->atttypid == INT4OID)
		{
			d = tupidx;
			isnull = false;
		}
		else
		{
			d = (Datum) 0;
			isnull = true;
		}

		slot->tts_values[attno - 1] = d;
		slot->tts_isnull[attno - 1] = isnull;
	}

	ItemPointerSet(&slot->tts_tid, 1, tupidx);
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;

	return true;
}

static bool
toyam_tuple_fetch_row_version(Relation rel,
							  ItemPointer tid,
							  Snapshot snapshot,
							  TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static void
toyam_tuple_get_latest_tid(Relation rel,
						   Snapshot snapshot,
						   ItemPointer tid)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static bool
toyam_tuple_satisfies_snapshot(Relation rel,
							   TupleTableSlot *slot,
							   Snapshot snapshot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static TransactionId
toyam_compute_xid_horizon_for_tuples(Relation rel,
									 ItemPointerData *items,
									 int nitems)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static TM_Result
toyam_tuple_lock(Relation rel,
				 ItemPointer tid,
				 Snapshot snapshot,
				 TupleTableSlot *slot,
				 CommandId cid,
				 LockTupleMode mode,
				 LockWaitPolicy wait_policy,
				 uint8 flags,
				 TM_FailureData *tmfd)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static void
toyam_relation_set_new_filenode(Relation rel,
								char persistence,
								TransactionId *freezeXid,
								MultiXactId *minmulti)
{
	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidMultiXactId;

	/*
	 * FIXME: We don't need this for anything. But index build calls
	 * RelationGetNumberOfBlocks, from index_update_stats(), and that
	 * fails if the underlying file doesn't exist.
	 */
	RelationCreateStorage(rel->rd_node, persistence);
}

static void
toyam_relation_nontransactional_truncate(Relation rel)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static void
toyam_relation_copy_data(Relation rel, RelFileNode newrnode)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static void
toyam_relation_copy_for_cluster(Relation NewHeap,
								Relation OldHeap,
								Relation OldIndex,
								bool use_sort,
								TransactionId OldestXmin,
								TransactionId FreezeXid,
								MultiXactId MultiXactCutoff,
								double *num_tuples,
								double *tups_vacuumed,
								double *tups_recently_dead)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static void
toyam_relation_vacuum(Relation onerel,
					  struct VacuumParams *params,
					  BufferAccessStrategy bstrategy)
{
	/* we've got nothing to do */
}

static bool
toyam_scan_analyze_next_block(TableScanDesc scan,
							  BlockNumber blockno,
							  BufferAccessStrategy bstrategy)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static bool
toyam_scan_analyze_next_tuple(TableScanDesc scan,
							  TransactionId OldestXmin,
							  double *liverows,
							  double *deadrows,
							  TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static double
toyam_index_build_range_scan(Relation heap_rel,
							 Relation index_rel,
							 struct IndexInfo *index_nfo,
							 bool allow_sync,
							 bool anyvisible,
							 bool progress,
							 BlockNumber start_blockno,
							 BlockNumber end_blockno,
							 IndexBuildCallback callback,
							 void *callback_state,
							 TableScanDesc scan)
{
	TupleTableSlot *slot;
	EState     *estate;

	estate = CreateExecutorState();
	slot = table_slot_create(heap_rel, NULL);

	if (!scan)
		scan = toyam_scan_begin(heap_rel,
								SnapshotAny,
								0, NULL,
								NULL,
								false,
								false,
								false,
								false,
								false,
								false);

	while (toyam_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		Datum           values[INDEX_MAX_KEYS];
		bool            isnull[INDEX_MAX_KEYS];
		HeapTuple		heapTuple;

		FormIndexDatum(index_nfo, slot, estate, values, isnull);

		/* Call the AM's callback routine to process the tuple */
		heapTuple = ExecCopySlotHeapTuple(slot);
		heapTuple->t_self = slot->tts_tid;
		callback(heap_rel, heapTuple, values, isnull, true,
				 callback_state);
		pfree(heapTuple);
	}

	toyam_scan_end(scan);
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);

	return 10;
}

static void
toyam_index_validate_scan(Relation heap_rel,
						  Relation index_rel,
						  struct IndexInfo *index_info,
						  Snapshot snapshot,
						  struct ValidateIndexState *state)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static void
toyam_relation_estimate_size(Relation rel, int32 *attr_widths,
							 BlockNumber *pages, double *tuples,
							 double *allvisfrac)
{
	*pages = 1;
	*tuples = 1;
	*allvisfrac = 1.0;
}

static bool
toyam_scan_sample_next_block(TableScanDesc scan,
							 struct SampleScanState *scanstate)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static bool
toyam_scan_sample_next_tuple(TableScanDesc scan,
					   struct SampleScanState *scanstate,
					   TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static const TableAmRoutine toyam_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = toyam_slot_callbacks,

	.scan_begin = toyam_scan_begin,
	.scan_end = toyam_scan_end,
	.scan_rescan = toyam_scan_rescan,
	.scan_getnextslot = toyam_scan_getnextslot,

	.parallelscan_estimate = toyam_parallelscan_estimate,
	.parallelscan_initialize = toyam_parallelscan_initialize,
	.parallelscan_reinitialize = toyam_parallelscan_reinitialize,

	.index_fetch_begin = toyam_index_fetch_begin,
	.index_fetch_reset = toyam_index_fetch_reset,
	.index_fetch_end = toyam_index_fetch_end,
	.index_fetch_tuple = toyam_index_fetch_tuple,

	.tuple_fetch_row_version = toyam_tuple_fetch_row_version,
	.tuple_get_latest_tid = toyam_tuple_get_latest_tid,
	.tuple_satisfies_snapshot = toyam_tuple_satisfies_snapshot,
	.compute_xid_horizon_for_tuples = toyam_compute_xid_horizon_for_tuples,

	.tuple_lock = toyam_tuple_lock,

	.relation_set_new_filenode = toyam_relation_set_new_filenode,
	.relation_nontransactional_truncate = toyam_relation_nontransactional_truncate,
	.relation_copy_data = toyam_relation_copy_data,
	.relation_copy_for_cluster = toyam_relation_copy_for_cluster,
	.relation_vacuum = toyam_relation_vacuum,

	.scan_analyze_next_block = toyam_scan_analyze_next_block,
	.scan_analyze_next_tuple = toyam_scan_analyze_next_tuple,
	.index_build_range_scan = toyam_index_build_range_scan,
	.index_validate_scan = toyam_index_validate_scan,

	.relation_estimate_size = toyam_relation_estimate_size,

	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,
	.scan_sample_next_block = toyam_scan_sample_next_block,
	.scan_sample_next_tuple = toyam_scan_sample_next_tuple,
};

Datum
toytableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&toyam_methods);
}
