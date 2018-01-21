/*-------------------------------------------------------------------------
 *
 * subscripting.h
 *		API for generic type subscripting
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/subscripting.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SUBSCRIPTING_H
#define SUBSCRIPTING_H

#include "parser/parse_node.h"
#include "nodes/primnodes.h"

struct ParseState;
struct SubscriptingRefState;

typedef SubscriptingRef * (*SubscriptingPrepare) (bool isAssignment, SubscriptingRef *sbsef);

typedef SubscriptingRef * (*SubscriptingValidate) (bool isAssignment, SubscriptingRef *sbsef,
												   struct ParseState *pstate);

typedef Datum (*SubscriptingFetch) (Datum source, struct SubscriptingRefState *sbsefstate);

typedef Datum (*SubscriptingAssign) (Datum source, struct SubscriptingRefState *sbsefstate);

typedef struct SbsRoutines
{
	SubscriptingRef			*sbsref;

	SubscriptingPrepare		prepare;
	SubscriptingValidate	validate;
	SubscriptingFetch		fetch;
	SubscriptingAssign		assign;

} SbsRoutines;


#endif							/* SUBSCRIPTING_H */
