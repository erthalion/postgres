/*-------------------------------------------------------------------------
 *
 * xactundo.c
 *	  management of undo record sets for transactions
 *
 * Undo records that need to be applied after a transaction or
 * subtransaction abort should be inserted using the functions defined
 * in this file; thus, every table or index access method that wants to
 * use undo for post-abort cleanup should invoke these interfaces.
 *
 * The reason for this design is that we want to pack all of the undo
 * records for a single transaction into one place, regardless of the
 * AM which generated them. That way, we can apply the undo actions
 * which pertain to that transaction in the correct order; namely,
 * backwards as compared with the order in which the records were
 * generated.
 *
 * Actually, we may use up to three undo record sets per transaction,
 * one per persistence level (permanent, unlogged, temporary). We
 * assume that it's OK to apply the undo records for each persistence
 * level independently of the others. At least insofar as undo records
 * describe page modifications to relations with a persistence level
 * matching the undo log in which undo pertaining to those modifications
 * is stored, this assumption seems safe, since the modifications
 * must necessarily touch disjoint sets of pages.
 *
 * All undo record sets of type URST_TRANSACTION are managed here;
 * the undo system supports at most one such record set per persistence
 * level per transaction.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/xactundo.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/undo.h"
#include "access/undolog.h"
#include "access/undoread.h"
#include "access/undopage.h"
#include "access/undorecordset.h"
#include "access/xact.h"
#include "access/xactundo.h"
#include "access/xlog_internal.h"
#include "catalog/pg_class.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"

/*
 * The capacity of the UndoRequestManager represents the maximum number of
 * in-progress or aborted transactions that have written undo which still needs
 * to be tracked.  Once an aborted transaction's undo actions have been
 * executed, it no longer counts against this limit.
 *
 * We could make the multiplier or the absolute value user-settable, but for
 * now we just hard-code the capacity as a fixed multiple of MaxBackends.
 * Hopefully, we'll never get very close to this limit, because if we do,
 * it means that the system is aborting transactions faster than the undo
 * machinery can perform the undo actions.
 */
#define UNDO_CAPACITY_PER_BACKEND		10

/*
 * If the UndoRequestManager is almost full, then we start refusing all
 * requests to perform undo in the background. Instead, the aborting
 * transactions will need to execute their own undo actions.  The point is
 * to avoid hitting the hard limit, at which stage we would have to start
 * refusing undo-writing transactions completely. This constant represents
 * the percentage of UndoRequestManager space that may be consumed before we
 * hit the soft limit.
 *
 * Note that this should be set so that the remaining capacity when the limit
 * is hit is at least MaxBackends; if this is done, it shouldn't be possible
 * to hit the hard limit unless the system crashes at least once while the
 * number of tracked transactions is already above the soft limit.  We set it
 * a bit lower than that here so as to make it unlikely that we'll hit the
 * hard limit even if there are multiple crashes.
 */
#define UNDO_SOFT_LIMIT_MULTIPLIER		0.85

static void
SerializeUndoData(StringInfo buf, UndoNode *undo_node)
{
	/* TODO: replace with actual serialization */
	appendBinaryStringInfo(buf, (char *) &undo_node->length, sizeof(((UndoNode *) NULL)->length));
	appendBinaryStringInfo(buf, (char *) &undo_node->rmid, sizeof(((UndoNode *) NULL)->rmid));
	appendBinaryStringInfo(buf, (char *) &undo_node->type, sizeof(((UndoNode *) NULL)->type));
	appendBinaryStringInfo(buf, undo_node->data, undo_node->length);
}

/* Per-subtransaction backend-private undo state. */
typedef struct XactUndoSubTransaction
{
	SubTransactionId nestingLevel;
	UndoRecPtr	start_location[NUndoPersistenceLevels];
	struct XactUndoSubTransaction *next;
} XactUndoSubTransaction;

/* Backend-private undo state (but with pointers into shared memory). */
typedef struct XactUndoData
{
	/* Is the transaction in the UNDO stage? */
	bool		is_undo;
	/* Has the transaction generated any undo log? */
	bool		has_undo;
	XactUndoSubTransaction *subxact;
	UndoRecPtr	end_location[NUndoPersistenceLevels];
	UndoRecordSet *record_set[NUndoPersistenceLevels];
} XactUndoData;

static XactUndoData XactUndo;
static XactUndoSubTransaction XactUndoTopState;

static void CollapseXactUndoSubTransactions(void);
static const char *UndoPersistenceLevelString(UndoPersistenceLevel plevel);

/*
 * Reset backend-local undo state.
 */
void
ResetXactUndo(void)
{
	int			i;

	XactUndo.is_undo = false;
	XactUndo.has_undo = false;
	XactUndo.subxact = &XactUndoTopState;
	XactUndoTopState.nestingLevel = 1;
	XactUndoTopState.next = NULL;

	for (i = 0; i < NUndoPersistenceLevels; ++i)
	{
		XactUndoTopState.start_location[i] = InvalidUndoRecPtr;
		XactUndo.end_location[i] = InvalidUndoRecPtr;
		XactUndo.record_set[i] = NULL;
	}
}

bool
XactHasUndo(void)
{
	return XactUndo.has_undo;
}

/*
 * Prepare to insert a transactional undo record.
 */
UndoRecPtr
PrepareXactUndoData(XactUndoContext *ctx, char persistence,
					UndoNode *undo_node)
{
	int			nestingLevel = GetCurrentTransactionNestLevel();
	UndoPersistenceLevel plevel = GetUndoPersistenceLevel(persistence);
	FullTransactionId fxid = GetTopFullTransactionId();
	UndoRecPtr	result;
	UndoRecPtr *sub_start_location;
	UndoRecordSet *urs;
	UndoRecordSize size;

	/* We should be connected to a database. */
	Assert(OidIsValid(MyDatabaseId));

	/* Remember that we've done something undo-related. */
	XactUndo.has_undo = true;

	/*
	 * If we've entered a subtransaction, spin up a new XactUndoSubTransaction
	 * so that we can track the start locations for the subtransaction
	 * separately from any parent (sub)transactions.
	 */
	if (nestingLevel > XactUndo.subxact->nestingLevel)
	{
		XactUndoSubTransaction *subxact;
		int			i;

		subxact = MemoryContextAlloc(TopMemoryContext,
									 sizeof(XactUndoSubTransaction));
		subxact->nestingLevel = nestingLevel;
		subxact->next = XactUndo.subxact;
		XactUndo.subxact = subxact;

		for (i = 0; i < NUndoPersistenceLevels; ++i)
			subxact->start_location[i] = InvalidUndoRecPtr;
	}

	/*
	 * Make sure we have an UndoRecordSet of the appropriate type open for
	 * this persistence level.
	 *
	 * These record sets are always associated with the toplevel transaction,
	 * not a subtransaction, in order to avoid fragmentation.
	 */
	urs = XactUndo.record_set[plevel];
	if (urs == NULL)
	{
		XactUndoRecordSetHeader hdr;

		hdr.fxid = fxid;
		hdr.dboid = MyDatabaseId;

		urs = UndoCreate(URST_TRANSACTION, persistence, 1,
						 sizeof(hdr), (char *) &hdr);
		XactUndo.record_set[plevel] = urs;
	}

	/* Remember persistence level. */
	ctx->plevel = plevel;

	/* Prepare serialized undo data. */
	initStringInfo(&ctx->data);
	SerializeUndoData(&ctx->data, undo_node);
	size = ctx->data.len;

	/*
	 * Find sufficient space for this undo insertion and lock the necessary
	 * buffers.
	 */
	result = UndoPrepareToInsert(urs, size);

	/*
	 * If this is the first undo for this persistence level in this
	 * subtransaction, record the start location.
	 */
	sub_start_location = &XactUndo.subxact->start_location[plevel];
	if (!UndoRecPtrIsValid(*sub_start_location))
		*sub_start_location = result;

	/*
	 * Remember this as the last end location.
	 */
	XactUndo.end_location[plevel] = UndoRecPtrPlusUsableBytes(result, size);

	return result;
}

/*
 * Insert transactional undo data.
 */
void
InsertXactUndoData(XactUndoContext *ctx, uint8 first_block_id)
{
	UndoRecordSet *urs = XactUndo.record_set[ctx->plevel];

	Assert(urs != NULL);
	UndoInsert(urs, ctx->data.data, ctx->data.len);
	UndoXLogRegisterBuffers(urs, first_block_id);
}

/*
 * Set page LSNs for just-inserted transactional undo data.
 */
void
SetXactUndoPageLSNs(XactUndoContext *ctx, XLogRecPtr lsn)
{
	UndoRecordSet *urs = XactUndo.record_set[ctx->plevel];

	Assert(urs != NULL);
	UndoPageSetLSN(urs, lsn);
}

/*
 * Clean up after inserting transactional undo data.
 */
void
CleanupXactUndoInsertion(XactUndoContext *ctx)
{
	UndoRecordSet *urs = XactUndo.record_set[ctx->plevel];

	UndoRelease(urs);
	pfree(ctx->data.data);
}

/*
 * Recreate UNDO during WAL replay.
 *
 * XXX: Should this live here? Or somewhere around the serialization code?
 */
UndoRecPtr
XactUndoReplay(XLogReaderState *xlog_record, UndoNode *undo_node)
{
	StringInfoData data;

	/* Prepare serialized undo data. */
	initStringInfo(&data);
	SerializeUndoData(&data, undo_node);

	return UndoReplay(xlog_record, data.data, data.len);
}

/*
 * PerformUndoRange
 *
 * Apply undo records between 'begin' and 'end'.
 */
void
PerformUndoActionsRange(UndoRecPtr begin, UndoRecPtr end,
						UndoPersistenceLevel plevel,
						int nestingLevel)
{
	UndoRSReaderState r;
	char		relpersistence;

	/* XXX Do we need a separate function for this conversion? */
	if (plevel == UNDOPERSISTENCE_TEMP)
		relpersistence = RELPERSISTENCE_TEMP;
	else if (plevel == UNDOPERSISTENCE_UNLOGGED)
		relpersistence = RELPERSISTENCE_UNLOGGED;
	else
	{
		Assert(plevel == UNDOPERSISTENCE_PERMANENT);
		relpersistence = RELPERSISTENCE_PERMANENT;
	}

	/*
	 * FIXME: provide correct persistence level - also
	 * UndoPersistenceLevelString
	 */
	UndoRSReaderInit(&r, begin, end, relpersistence, nestingLevel == 1);

	while (UndoRSReaderReadOneBackward(&r))
	{
		const RmgrData *rmgr;

		rmgr = &RmgrTable[r.node.n.rmid];
		rmgr->rm_undo(&r.node);
	}

	UndoRSReaderClose(&r);
}

/*
 * Perform undo actions.
 *
 * Caller must ensure that we have a valid transaction context so that it's
 * safe for us to do things that might fail.
 *
 * Our job is to apply all undo for transaction nesting levels greater than or
 * equal to the level supplied as an argument.
 */
void
PerformUndoActions(int nestingLevel)
{
	XactUndoSubTransaction *mysubxact = XactUndo.subxact;

	/* Sanity checks. */
	Assert(XactUndo.has_undo);
	Assert(mysubxact != NULL);

	/*
	 * Subtransaction's UNDO could have been cleaned up because the parent
	 * transaction is going to be rolled back.
	 */
	if (mysubxact->nestingLevel < nestingLevel)
		return;

	Assert(mysubxact->nestingLevel == nestingLevel);

	/*
	 * Invoke facilities to actually apply undo actions from here, passing the
	 * relevant information from the XactUndo so that they know what to do.
	 */
	for (UndoPersistenceLevel p = UNDOPERSISTENCE_PERMANENT;
		 p < NUndoPersistenceLevels; p++)
	{
		UndoRecPtr	start_location;
		UndoRecPtr	end_location;

		start_location = mysubxact->start_location[p];
		if (!UndoRecPtrIsValid(start_location))
			continue;
		end_location = XactUndo.end_location[p];

		elog(DEBUG1, "executing undo: persistence: %s, nestingLevel: %d, bytes: %lu start: %lu end: %lu",
			 UndoPersistenceLevelString(p),
			 nestingLevel,
			 end_location - start_location,
			 start_location, end_location
			);

		PerformUndoActionsRange(start_location, end_location, p,
								nestingLevel);
	}
}

/*
 * Perform the undo actions with no active transaction.
 */
void
PerformBackgroundUndo(UndoRecPtr begin, UndoRecPtr end,
					  UndoPersistenceLevel plevel)
{
	/* We pretend to do a regular transaction, but there's no DO in it. */
	StartTransactionCommand();

	/*
	 * Initialize XactUndo so that the undo actios can be applied simply by
	 * aborting the current transaction.
	 */
	ResetXactUndo();
	XactUndo.has_undo = true;
	XactUndo.subxact->start_location[plevel] = begin;
	XactUndo.end_location[plevel] = end;

	/*
	 * Abort the transaction. This includes the UNDO stage.
	 *
	 * XXX Downside of this approach is that if the undo fails, the background
	 * worker exits without processing the remaining sets. Is that o.k.?
	 */
	AbortCurrentTransaction();
}

/*
 * Post-commit cleanup of the undo state.
 *
 * NB: This code MUST NOT FAIL, since it is run as a post-commit cleanup step.
 * Don't put anything complicated in this function!
 */
void
AtCommit_XactUndo(void)
{
	/* Also exit quickly if we never did anything undo-related. */
	if (!XactUndo.has_undo)
		return;

	/* Reset state for next transaction. */
	ResetXactUndo();
}

/*
 * Post-abort cleanup of the undo state.
 */
void
AtAbort_XactUndo(void)
{
	bool		has_temporary_undo = false;

	/* Exit quickly if this transaction generated no undo. */
	if (!XactUndo.has_undo)
		return;

	/* This is a toplevel abort, so collapse all subtransaction state. */
	CollapseXactUndoSubTransactions();

	/* Figure out whether there any relevant temporary undo. */
	has_temporary_undo =
		UndoRecPtrIsValid(XactUndo.subxact->start_location[UNDOPERSISTENCE_TEMP]);

	if (XactUndo.is_undo)
	{
		/*
		 * Regrettably, we seem to have failed when attempting to perform undo
		 * actions.
		 */

		/*
		 * This can happen when aborting a transaction which hasn't failed
		 * itself but abort of its subtransaction was tried and it failed.
		 */
		if (proc_exit_inprogress)
		{
			/*
			 * Try to avoid additional errors by our proc_exit() hooks.
			 *
			 * Shared memory shouldn't probably be accessed at this stage and
			 * the backend is going to exit, so cleanup is not critical here.
			 */
			ResetXactUndo();

			/*
			 * When called from proc_exit(), elevel >= ERROR would cause
			 * recursion.
			 */
			elog(WARNING, "failed to undo transaction");
		}

		/*
		 * XXX. If we have any temporary undo, we're in big trouble, because
		 * we're unable to process it.  Should we throw FATAL?  Just leave the
		 * undo unapplied and somehow retry at a later point in the session?
		 */
		if (has_temporary_undo)
			elog(WARNING, "experience_intense_sadness");

		/* Reset the undo state, unless done above. */
		if (!proc_exit_inprogress)
			ResetXactUndo();
		return;
	}

	XactUndo.is_undo = true;
}

/*
 * Clean up of the undo state following a subtransaction commit.
 *
 * Like AtCommit_XactUndo, this must not fail.
 */
void
AtSubCommit_XactUndo(int level)
{
	XactUndoSubTransaction *cursubxact = XactUndo.subxact;
	XactUndoSubTransaction *nextsubxact = cursubxact->next;
	int			i;

	/* Exit quickly if the transaction or this subtransaction has no undo. */
	if (!XactUndo.has_undo || cursubxact->nestingLevel < level)
		return;

	/* If this fails, some other subtransaction failed to clean up properly. */
	Assert(cursubxact->nestingLevel == level);

	/* If this fails, things are really messed up. */
	Assert(nextsubxact->nestingLevel < cursubxact->nestingLevel);

	/*
	 * We might reach here after performing undo for a subtransaction that
	 * previously aborted. If so, we should discard the XactUndoSubTransaction
	 * which we were keeping around for that purpose.
	 */
	if (XactUndo.is_undo)
	{
		XactUndo.subxact = cursubxact->next;
		pfree(cursubxact);
		Assert(XactUndo.subxact->nestingLevel < level);
		XactUndo.is_undo = false;
		return;
	}

	/*
	 * If we have undo but our parent subtransaction doesn't, we can just
	 * adjust the nesting level of the current XactUndoSubTransaction.
	 */
	if (nextsubxact->nestingLevel < cursubxact->nestingLevel - 1)
	{
		cursubxact->nestingLevel--;
		return;
	}

	/* Merge our data with parent. */
	for (i = 0; i < NUndoPersistenceLevels; ++i)
		if (!UndoRecPtrIsValid(nextsubxact->start_location[i]))
			nextsubxact->start_location[i] = cursubxact->start_location[i];
	pfree(cursubxact);
	XactUndo.subxact = nextsubxact;
}

/*
 * Clean up of the undo state following a subtransaction abort.
 *
 * If the caller is unable or unwilling to perform foreground undo, he should
 * pass cleanup_only=true to this function.
 *
 * XXX. We need to avoid doing foreground undo for things that have
 * already been successfully undone as a result of previous subtransaction
 * aborts. That's not really this function's problem but we need to deal with
 * it somewhere.
 */
void
AtSubAbort_XactUndo(int level, bool cleanup_only)
{
	XactUndoSubTransaction *cursubxact = XactUndo.subxact;

	/* Exit quickly if the transaction or this subtransaction has no undo. */
	if (!XactUndo.has_undo || cursubxact->nestingLevel < level)
		return;

	/*
	 * If we fail when attempting to perform undo actions, it's impossible to
	 * continue with the parent (sub)transaction. We currently handle this by
	 * killing off the entire backend.
	 *
	 * Note that we need a defense here against reentering this function from
	 * within proc_exit and failing again.
	 */
	if (XactUndo.is_undo && !proc_exit_inprogress)
	{
		/*
		 * XXX. This is non-optimal.
		 *
		 * We don't necessarily need to kill the entire backend; it would
		 * probably be good enough to kill off the top-level transaction,
		 * maybe by somehow (how?) forcing the parent subtransaction to also
		 * fail (and thus retry our undo) and so forth until we either succeed
		 * during undo or get to the outermost level. Or perhaps we should
		 * force all of the transactions up to the top level into a failed
		 * state immediately (again, how?).
		 *
		 * Another thing that sucks about this is that throwing FATAL here
		 * will probably lose the original error message that might give the
		 * user some hint as to the cause of the failure. We probably need to
		 * improve that somehow.
		 */
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unable to continue transaction after undo failure")));
	}

	/*
	 * If the caller is unable or unwilling to perform foreground undo for
	 * this subtransaction, we can and should discard the state that would be
	 * used for that purpose at this stage.
	 */
	if (cleanup_only)
	{
		XactUndo.subxact = cursubxact->next;
		pfree(cursubxact);
		Assert(XactUndo.subxact->nestingLevel < level);
	}
	else
		XactUndo.is_undo = true;
}

/*
 * Get ready to PREPARE a transaction that has undo. Any errors must be
 * thrown at this stage.
 */
void
AtPrepare_XactUndo(GlobalTransaction gxact)
{
	UndoRecPtr	temp_undo_start;

	/* Exit quickly if this transaction generated no undo. */
	if (!XactUndo.has_undo)
		return;

	/*
	 * Whether PREPARE succeeds or fails, this session will no longer be in a
	 * transaction, so collapse all subtransaction state. This simplifies the
	 * check for temporary undo which follows.
	 */
	CollapseXactUndoSubTransactions();

	/*
	 * If we have temporary undo, we cannot PREPARE.
	 *
	 * The earlier check for operations on temporary objects will presumaby
	 * catch most problems, but there might be corner cases where temporary
	 * undo exists but those checks don't trip. So, to be safe, add another
	 * check here.
	 */
	temp_undo_start = XactUndo.subxact->start_location[UNDOPERSISTENCE_TEMP];
	if (UndoRecPtrIsValid(temp_undo_start))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a transaction that has temporary undo")));
}

/*
 * Post-PREPARE resource cleanup.
 *
 * It's too late for an ERROR at this point, so everything we do here must
 * be guaranteed to succeed.
 */
void
PostPrepare_XactUndo(void)
{
	/* Exit quickly if this transaction generated no undo. */
	if (!XactUndo.has_undo)
		return;

	/* And clear the undo state for the next transaction. */
	ResetXactUndo();
}

void
AtProcExit_XactUndo(void)
{
}

/*
 * Collapse the subtransaction stack.
 *
 * In effect, we're pretending that all subtransactions has committed, in
 * preparation for making some decision about the fate of the top-level
 * transaction.
 */
static void
CollapseXactUndoSubTransactions(void)
{
	while (XactUndo.subxact->next != NULL)
	{
		XactUndoSubTransaction *cursubxact = XactUndo.subxact;
		XactUndoSubTransaction *nextsubxact = cursubxact->next;
		int			i;

		for (i = 0; i < NUndoPersistenceLevels; ++i)
			if (!UndoRecPtrIsValid(nextsubxact->start_location[i]))
				nextsubxact->start_location[i] = cursubxact->start_location[i];
		pfree(cursubxact);
		XactUndo.subxact = nextsubxact;
	}
}

void
GetCurrentUndoRange(UndoRecPtr * begin, UndoRecPtr * end,
					UndoPersistenceLevel plevel)
{
	*begin = XactUndo.subxact->start_location[plevel];
	*end = XactUndo.end_location[plevel];
}

/*
 * Get undo persistence level as a C string.
 */
static const char *
UndoPersistenceLevelString(UndoPersistenceLevel plevel)
{
	switch (plevel)
	{
		case UNDOPERSISTENCE_PERMANENT:
			return "permanent";
		case UNDOPERSISTENCE_UNLOGGED:
			return "unlogged";
		case UNDOPERSISTENCE_TEMP:
			return "temporary";
	}

	pg_unreachable();
}
