/*-------------------------------------------------------------------------
 *
 * jsonbsubs.c
 *	  Subscripting support functions for jsonb.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsonbsubs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/execExpr.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/subscripting.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "utils/jsonb.h"
#include "utils/jsonfuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/*
 * Finish parse analysis of a SubscriptingRef expression for a jsonb.
 *
 * Transform the subscript expressions, coerce them to integers,
 * and determine the result type of the SubscriptingRef node.
 */
static void
jsonb_subscript_transform(SubscriptingRef *sbsref,
						  List *indirection,
						  ParseState *pstate,
						  bool isSlice,
						  bool isAssignment)
{
	List	   *upperIndexpr = NIL;
	List	   *lowerIndexpr = NIL;
	ListCell   *idx;

	/*
	 * Transform the subscript expressions, and separate upper and lower
	 * bounds into two lists.
	 *
	 * If we have a container slice expression, we convert any non-slice
	 * indirection items to slices by treating the single subscript as the
	 * upper bound and supplying an assumed lower bound of 1.
	 */
	foreach(idx, indirection)
	{
		A_Indices  *ai = lfirst_node(A_Indices, idx);
		Node	   *subexpr;

		if (isSlice)
		{
			if (ai->lidx)
			{
				subexpr = transformExpr(pstate, ai->lidx, pstate->p_expr_kind);
				subexpr = coerce_to_target_type(pstate,
												subexpr, exprType(subexpr),
												TEXTOID, -1,
												COERCION_ASSIGNMENT,
												COERCE_IMPLICIT_CAST,
												-1);
				if (subexpr == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("jsonb subscript must have text type"),
							 parser_errposition(pstate, exprLocation(subexpr))));
			}
			else
			{
				/* Slice with omitted lower bound, put NULL into the list */
				subexpr = NULL;
			}
			lowerIndexpr = lappend(lowerIndexpr, subexpr);
		}
		else
			Assert(ai->lidx == NULL && !ai->is_slice);

		if (ai->uidx)
		{
			subexpr = transformExpr(pstate, ai->uidx, pstate->p_expr_kind);
			subexpr = coerce_to_target_type(pstate,
											subexpr, exprType(subexpr),
											TEXTOID, -1,
											COERCION_ASSIGNMENT,
											COERCE_IMPLICIT_CAST,
											-1);
			if (subexpr == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("jsonb subscript must have text type"),
						 parser_errposition(pstate, exprLocation(subexpr))));
		}
		else
		{
			/* Slice with omitted upper bound, put NULL into the list */
			Assert(isSlice && ai->is_slice);
			subexpr = NULL;
		}
		upperIndexpr = lappend(upperIndexpr, subexpr);
	}

	/* ... and store the transformed lists into the SubscriptRef node */
	sbsref->refupperindexpr = upperIndexpr;
	sbsref->reflowerindexpr = lowerIndexpr;

	if (sbsref->reflowerindexpr != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("jsonb subscript does not support slices"),
				 parser_errposition(pstate, exprLocation(
						 ((Node *) linitial(sbsref->reflowerindexpr))))));

	/* Determine the result type of the subscripting operation; always jsonb */
	sbsref->refrestype = JSONBOID;
	sbsref->reftypmod = -1;
}

/*
 * During execution, process the subscripts in a SubscriptingRef expression.
 *
 * The subscript expressions are already evaluated in Datum form in the
 * SubscriptingRefState's arrays.  Check them as necessary.
 *
 * If any subscript is NULL, we throw error in assignment cases, or in fetch
 * cases set result to NULL and return false (instructing caller to skip the
 * rest of the SubscriptingRef sequence).
 */
static bool
jsonb_subscript_check_subscripts(ExprState *state,
								 ExprEvalStep *op,
								 ExprContext *econtext)
{
	SubscriptingRefState *sbsrefstate = op->d.sbsref_subscript.state;

	/* Process upper subscripts */
	for (int i = 0; i < sbsrefstate->numupper; i++)
	{
		if (sbsrefstate->upperprovided[i])
		{
			/* If any index expr yields NULL, result is NULL or error */
			if (sbsrefstate->upperindexnull[i])
			{
				if (sbsrefstate->isassignment)
					ereport(ERROR,
							(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							 errmsg("jsonb subscript in assignment must not be null")));
				*op->resnull = true;
				return false;
			}
		}
	}

	return true;
}

/*
 * Evaluate SubscriptingRef fetch for a jsonb element.
 *
 * Source container is in step's result variable (it's known not NULL, since
 * we set fetch_strict to true).
 */
static void
jsonb_subscript_fetch(ExprState *state,
					  ExprEvalStep *op,
					  ExprContext *econtext)
{
	SubscriptingRefState *sbsrefstate = op->d.sbsref.state;
	Jsonb		*jsonbSource;

	/* Should not get here if source jsonb (or any subscript) is null */
	Assert(!(*op->resnull));

	jsonbSource = DatumGetJsonbP(*op->resvalue);
	*op->resvalue = jsonb_get_element(jsonbSource,
									  sbsrefstate->upperindex,
									  sbsrefstate->numupper,
									  op->resnull,
									  false);
}

/*
 * Evaluate SubscriptingRef assignment for a jsonb element assignment.
 *
 * Input container (possibly null) is in result area, replacement value is in
 * SubscriptingRefState's replacevalue/replacenull.
 */
static void
jsonb_subscript_assign(ExprState *state,
					   ExprEvalStep *op,
					   ExprContext *econtext)
{
	SubscriptingRefState *sbsrefstate = op->d.sbsref.state;
	Jsonb		*jsonbSource;
	JsonbValue	*replacevalue;

	if (sbsrefstate->replacenull)
	{
		replacevalue = (JsonbValue *) palloc(sizeof(JsonbValue));
		replacevalue->type = jbvNull;
	}
	else
		replacevalue =
			JsonbToJsonbValue(DatumGetJsonbP(sbsrefstate->replacevalue));

	if (*op->resnull)
	{
		JsonbValue *newSource = (JsonbValue *) palloc(sizeof(JsonbValue));
		newSource->type = jbvObject;
		newSource->val.object.nPairs = 0;

		jsonbSource = JsonbValueToJsonb(newSource);
		*op->resnull = false;
	}
	else
		jsonbSource = DatumGetJsonbP(*op->resvalue);

	*op->resvalue = jsonb_set_element(jsonbSource,
									  sbsrefstate->upperindex,
									  sbsrefstate->numupper,
									  replacevalue);
	/* The result is never NULL, so no need to change *op->resnull */
}

/*
 * Compute old jsonb element value for a SubscriptingRef assignment
 * expression.  Will only be called if the new-value subexpression
 * contains SubscriptingRef or FieldStore.  This is the same as the
 * regular fetch case, except that we have to handle a null jsonb,
 * and the value should be stored into the SubscriptingRefState's
 * prevvalue/prevnull fields.
 */
static void
jsonb_subscript_fetch_old(ExprState *state,
						  ExprEvalStep *op,
						  ExprContext *econtext)
{
	SubscriptingRefState *sbsrefstate = op->d.sbsref.state;

	if (*op->resnull)
	{
		/* whole jsonb is null, so any element is too */
		sbsrefstate->prevvalue = (Datum) 0;
		sbsrefstate->prevnull = true;
	}
	else
	{
		Jsonb	*jsonbSource = DatumGetJsonbP(*op->resvalue);
		sbsrefstate->prevvalue = jsonb_get_element(jsonbSource,
									  			   sbsrefstate->upperindex,
									  			   sbsrefstate->numupper,
												   &sbsrefstate->prevnull,
												   false);
	}
}

/*
 * Set up execution state for a jsonb subscript operation. Opposite to the
 * arrays subscription, there is no limit for number of subscripts as jsonb
 * type itself doesn't have nesting limits.
 */
static void
jsonb_exec_setup(const SubscriptingRef *sbsref,
				 SubscriptingRefState *sbsrefstate,
				 SubscriptExecSteps *methods)
{
	Assert((sbsrefstate->numlower == 0));

	/* Should be impossible if parser is sane, but check anyway: */
	if (sbsrefstate->numlower != 0 &&
		sbsrefstate->numupper != sbsrefstate->numlower)
		elog(ERROR, "upper and lower index lists are not same length");

	/*
	 * Pass back pointers to appropriate step execution functions.
	 */
	methods->sbs_check_subscripts = jsonb_subscript_check_subscripts;
	methods->sbs_fetch = jsonb_subscript_fetch;
	methods->sbs_assign = jsonb_subscript_assign;
	methods->sbs_fetch_old = jsonb_subscript_fetch_old;
}

/*
 * jsonb_subscript_handler
 *		Subscripting handler for jsonb.
 *
 */
Datum
jsonb_subscript_handler(PG_FUNCTION_ARGS)
{
	static const SubscriptRoutines sbsroutines = {
		.transform = jsonb_subscript_transform,
		.exec_setup = jsonb_exec_setup,
		.fetch_strict = true,	/* fetch returns NULL for NULL inputs */
		.fetch_leakproof = true,	/* fetch returns NULL for bad subscript */
		.store_leakproof = false	/* ... but assignment throws error */
	};

	PG_RETURN_POINTER(&sbsroutines);
}
