/*-------------------------------------------------------------------------
 *
 * uniquekeys.c
 *	  Utilities for matching and building unique keys
 *
 * Portions Copyright (c) 2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/uniquekeys.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"
#include "rewrite/rewriteManip.h"


static List *gather_mergeable_baserestrictlist(RelOptInfo *rel);
static List *gather_mergeable_joinclauses(RelOptInfo *joinrel,
										  RelOptInfo *rel1,
										  RelOptInfo *rel2,
										  List *restirctlist,
										  JoinType jointype);
static bool match_index_to_baserestrictinfo(IndexOptInfo *unique_ind,
											List *restrictlist);

/*
 * This struct is used to help populate_joinrel_uniquekeys.
 *
 * added_to_joinrel is true if a uniquekey (from outerrel or innerrel)
 * has been added to joinrel.
 * useful is true if the exprs of the uniquekey still appears in joinrel.
 */
typedef struct UniqueKeyContextData
{
	UniqueKey	*uniquekey;
	bool	added_to_joinrel;
	bool	useful;
} *UniqueKeyContext;

static List *initililze_uniquecontext_for_joinrel(RelOptInfo *inputrel);
static bool innerrel_keeps_unique(PlannerInfo *root,
								  RelOptInfo *outerrel,
								  RelOptInfo *innerrel,
								  List *restrictlist,
								  bool reverse);
static List *get_exprs_from_uniquekey(RelOptInfo *joinrel,
									  RelOptInfo *rel1,
									  UniqueKey *ukey);
static bool clause_sides_match_join(RestrictInfo *rinfo,
									Relids outerrelids,
									Relids innerrelids);
static void add_uniquekey_from_index(RelOptInfo *rel,
									 IndexOptInfo *unique_index);
static void add_uniquekey_for_onerow(RelOptInfo *rel);
static bool add_combined_uniquekey(RelOptInfo *joinrel,
								   RelOptInfo *outer_rel,
								   RelOptInfo *inner_rel,
								   UniqueKey *outer_ukey,
								   UniqueKey *inner_ukey,
								   JoinType jointype);

/* Used for unique indexes checking for partitioned table */
static bool index_constains_partkey(RelOptInfo *partrel,  IndexOptInfo *ind);
static IndexOptInfo *simple_copy_indexinfo_to_parent(RelOptInfo *parentrel,
													 IndexOptInfo *from);
static bool simple_indexinfo_equal(IndexOptInfo *ind1, IndexOptInfo *ind2);
static void adjust_partition_unique_indexlist(RelOptInfo *parentrel,
											  RelOptInfo *childrel,
											  List **global_unique_index);

/* Helper function for grouped relation and distinct relation. */
static void add_uniquekey_from_sortgroups(PlannerInfo *root,
										  RelOptInfo *rel,
										  List *sortgroups);

/*
 * populate_baserel_uniquekeys
 *		Populate 'baserel' uniquekeys list by looking at the rel's unique index
 * add baserestrictinfo
 */
void
populate_baserel_uniquekeys(PlannerInfo *root,
							RelOptInfo *baserel,
							List *indexlist)
{
	ListCell *lc;
	List	*restrictlist;
	bool	return_one_row = false;
	List	*matched_uniq_indexes = NIL;

	Assert(baserel->rtekind == RTE_RELATION);

	if (root->parse->hasTargetSRFs)
		return;

	if (baserel->reloptkind == RELOPT_OTHER_MEMBER_REL)
		/*
		 * Set UniqueKey on member rel is useless, we have to recompute it at
		 * upper level, see populate_partitionedrel_uniquekeys for reference
		 */
		return;

	restrictlist = gather_mergeable_baserestrictlist(baserel);

	foreach(lc, indexlist)
	{
		IndexOptInfo *ind = (IndexOptInfo *) lfirst(lc);
		if (!ind->unique || !ind->immediate ||
			(ind->indpred != NIL && !ind->predOK))
			continue;

		if (match_index_to_baserestrictinfo(ind, restrictlist))
		{
			return_one_row = true;
			break;
		}

		if (ind->indexprs != NIL)
			/* XXX: ignore index on expression so far */
			continue;
		matched_uniq_indexes = lappend(matched_uniq_indexes, ind);
	}

	if (return_one_row)
	{
		add_uniquekey_for_onerow(baserel);
	}
	else
	{
		foreach(lc, matched_uniq_indexes)
			add_uniquekey_from_index(baserel, lfirst_node(IndexOptInfo, lc));
	}
}


/*
 * populate_partitioned_rel_uniquekeys
 * The unique index can only be used for UniqueKey based on:
 * 1). It must include partition key.
 * 2). It exists in all the related. partitions.
 */
void
populate_partitionedrel_uniquekeys(PlannerInfo *root,
								   RelOptInfo *rel,
								   List *childrels)
{
	ListCell	*lc;
	List	*global_uniq_indexlist = NIL;
	RelOptInfo *childrel;
	bool is_first = true;

	Assert(IS_PARTITIONED_REL(rel));

	if (root->parse->hasTargetSRFs)
		return;

	if (childrels == NIL)
		return;

	childrel = linitial_node(RelOptInfo, childrels);
	foreach(lc, childrel->indexlist)
	{
		IndexOptInfo *ind = lfirst(lc);
		IndexOptInfo *modified_index;
		if (!ind->unique || !ind->immediate ||
			(ind->indpred != NIL && !ind->predOK))
			continue;

		modified_index = simple_copy_indexinfo_to_parent(rel, ind);
		/*
		 * If the unique index doesn't contain partkey, then it is unique
		 * on this partition only, so it is useless for us.
		 */
		if (!index_constains_partkey(rel, modified_index))
			continue;
		global_uniq_indexlist = lappend(global_uniq_indexlist,  modified_index);
	}

	/* Fast path */
	if (global_uniq_indexlist == NIL)
		return;

	foreach(lc, childrels)
	{
		RelOptInfo *child = lfirst(lc);
		if (is_first)
		{
			is_first = false;
			continue;
		}
		adjust_partition_unique_indexlist(rel, child, &global_uniq_indexlist);
	}

	/* Now we have a list of unique index which are exactly same on all childrels,
	 * Set the UniqueKey just like it is non-partition table
	 */
	populate_baserel_uniquekeys(root, rel, global_uniq_indexlist);
}


/*
 * populate_distinctrel_uniquekeys
 */
void
populate_distinctrel_uniquekeys(PlannerInfo *root,
								RelOptInfo *inputrel,
								RelOptInfo *distinctrel)
{
	/* The unique key before the distinct is still valid. */
	distinctrel->uniquekeys = list_copy(inputrel->uniquekeys);
	add_uniquekey_from_sortgroups(root, distinctrel, root->parse->distinctClause);
}

/*
 * populate_grouprel_uniquekeys
 */
void
populate_grouprel_uniquekeys(PlannerInfo *root,
							 RelOptInfo *grouprel)
{
	Query *parse = root->parse;

	if (parse->hasTargetSRFs)
		return;

	if (parse->groupingSets)
		return;

	/* A Normal group by without grouping set. */
	if (parse->groupClause)
		add_uniquekey_from_sortgroups(root,
									  grouprel,
									  root->parse->groupClause);
	else
		/* It has aggregation but without a group by, so only one row returned */
		add_uniquekey_for_onerow(grouprel);
}

/*
 * simple_copy_uniquekeys
 * Using a function for the one-line code makes us easy to check where we simply
 * copied the uniquekey.
 */
void
simple_copy_uniquekeys(RelOptInfo *oldrel,
					   RelOptInfo *newrel)
{
	newrel->uniquekeys = oldrel->uniquekeys;
}

/*
 *  populate_unionrel_uniquekeys
 */
void
populate_unionrel_uniquekeys(PlannerInfo *root,
							  RelOptInfo *unionrel)
{
	ListCell	*lc;
	List	*exprs = NIL;

	Assert(unionrel->uniquekeys == NIL);

	foreach(lc, unionrel->reltarget->exprs)
	{
		exprs = lappend(exprs, lfirst(lc));
	}

	if (exprs == NIL)
		/* SQL: select union select; is valid, we need to handle it here. */
		return;
	unionrel->uniquekeys = lappend(unionrel->uniquekeys,
								   makeUniqueKey(exprs,
												 false, /* allows_multinull */
												 false  /* onerow */));
}

/*
 * populate_joinrel_uniquekeys
 *
 * populate uniquekeys for joinrel. We will check each relation to see if it's
 * UniqueKey is still valid via innerrel_keeps_unique, if so, we add it to
 * joinrel.  The multi_nullvals field will be changed from false to true
 * for some outer join cases and one-row UniqueKey needs to be converted to nomarl
 * UniqueKey for the same case as well.

 * For the uniquekey in either baserel which can't be unique after join, we still
 * check to see if combination of UniqueKeys from both side is still useful for us.
 * if yes, we add it to joinrel as well.
 */
void
populate_joinrel_uniquekeys(PlannerInfo *root, RelOptInfo *joinrel,
							RelOptInfo *outerrel, RelOptInfo *innerrel,
							List *restrictlist, JoinType jointype)
{
	ListCell *lc, *lc2;
	List	*clause_list = NIL;
	List	*outerrel_ukey_ctx;
	List	*innerrel_ukey_ctx;
	bool	inner_onerow, outer_onerow;

	if (root->parse->hasTargetSRFs)
		return;

	/* Care about the outerrel relation only for SEMI/ANTI join */
	if (jointype == JOIN_SEMI || jointype == JOIN_ANTI)
	{
		foreach(lc, outerrel->uniquekeys)
		{
			UniqueKey	*uniquekey = lfirst_node(UniqueKey, lc);
			if (list_is_subset(uniquekey->exprs, joinrel->reltarget->exprs))
				joinrel->uniquekeys = lappend(joinrel->uniquekeys, uniquekey);
		}
		return;
	}

	Assert(jointype == JOIN_LEFT || jointype == JOIN_FULL || jointype == JOIN_INNER);

	/* Fast path */
	if (innerrel->uniquekeys == NIL || outerrel->uniquekeys == NIL)
		return;

	inner_onerow = relation_is_onerow(innerrel);
	outer_onerow = relation_is_onerow(outerrel);

	outerrel_ukey_ctx = initililze_uniquecontext_for_joinrel(outerrel);
	innerrel_ukey_ctx = initililze_uniquecontext_for_joinrel(innerrel);

	clause_list = gather_mergeable_joinclauses(joinrel, outerrel, innerrel,
											   restrictlist, jointype);

	if (innerrel_keeps_unique(root, innerrel, outerrel, clause_list, true /* reverse */))
	{
		foreach(lc, outerrel_ukey_ctx)
		{
			UniqueKeyContext ctx = (UniqueKeyContext)lfirst(lc);

			if (!list_is_subset(ctx->uniquekey->exprs, joinrel->reltarget->exprs))
			{
				ctx->useful = false;
				continue;
			}

			/* Outer relation has one row, and the unique key is not duplicated after join,
			 * the joinrel will still has one row unless the jointype == JOIN_FULL.
			 */
			if (outer_onerow && jointype != JOIN_FULL)
			{
				add_uniquekey_for_onerow(joinrel);
				return;
			}
			else if (outer_onerow)
			{
				/* Full join case, the onerow becomes multi rows and multi_nullvals changes
				 * to true. We also need to set the exprs correctly since it is not one-row
				 * any more.
				 */
				ListCell *lc2;
				foreach(lc2, get_exprs_from_uniquekey(joinrel, outerrel, NULL))
				{
					joinrel->uniquekeys = lappend(joinrel->uniquekeys,
												  makeUniqueKey(lfirst(lc2),
																true, /* multi_nullvals */
																false /* onerow */));
				}
			}
			else
			{
				if (!ctx->uniquekey->multi_nullvals && jointype == JOIN_FULL)
					/* Change multi_nullvals to true due to the full join. */
					joinrel->uniquekeys = lappend(joinrel->uniquekeys,
												  makeUniqueKey(ctx->uniquekey->exprs,
																true,
																ctx->uniquekey->onerow));
				else
					/* Just reuse it */
					joinrel->uniquekeys = lappend(joinrel->uniquekeys,
												  ctx->uniquekey);
			}
			ctx->added_to_joinrel = true;
		}
	}

	if (innerrel_keeps_unique(root, outerrel, innerrel, clause_list, false))
	{
		foreach(lc, innerrel_ukey_ctx)
		{
			UniqueKeyContext ctx = (UniqueKeyContext)lfirst(lc);

			if (!list_is_subset(ctx->uniquekey->exprs, joinrel->reltarget->exprs))
			{
				ctx->useful = false;
				continue;
			}

			/*
			 * Inner relation has one row, and the unique key is not duplicated after join,
			 * the joinrel will still has one row unless the jointype not in
			 * (JOIN_FULL, JOIN_LEFT)
			 */
			if (inner_onerow && jointype != JOIN_FULL && jointype != JOIN_LEFT)
			{
				add_uniquekey_for_onerow(joinrel);
				return;
			}
			else if (inner_onerow)
			{
				/* Full join or left outer join case, the inner one row becomes to multi rows
				 * and multi_nullvals becomes to true. We also need to set the exprs correctly
				 * since it is not one-row any more.
				 */
				ListCell *lc2;
				foreach(lc2, get_exprs_from_uniquekey(joinrel, innerrel, NULL))
				{
					joinrel->uniquekeys = lappend(joinrel->uniquekeys,
												  makeUniqueKey(lfirst(lc2),
																true, /* multi_nullvals */
																false /* onerow */));
				}
			}
			else
			{
				if (!ctx->uniquekey->multi_nullvals &&
					(jointype == JOIN_FULL || jointype == JOIN_LEFT))
					/* Need to change multi_nullvals to true due to the outer join. */
					joinrel->uniquekeys = lappend(joinrel->uniquekeys,
												  makeUniqueKey(ctx->uniquekey->exprs,
																true,
																ctx->uniquekey->onerow));
				else
					joinrel->uniquekeys = lappend(joinrel->uniquekeys,
												  ctx->uniquekey);

			}
			ctx->added_to_joinrel = true;
		}
	}

	/* The combination of the UniqueKey from both sides is unique as well regardless
	 * of join type, but no bother to add it if its subset has been added to joinrel
	 * already or it is not useful for the joinrel.
	 */
	foreach(lc, outerrel_ukey_ctx)
	{
		UniqueKeyContext ctx1 = (UniqueKeyContext) lfirst(lc);
		if (ctx1->added_to_joinrel || !ctx1->useful)
			continue;
		foreach(lc2, innerrel_ukey_ctx)
		{
			UniqueKeyContext ctx2 = (UniqueKeyContext) lfirst(lc2);
			if (ctx2->added_to_joinrel || !ctx2->useful)
				continue;
			if (add_combined_uniquekey(joinrel, outerrel, innerrel,
									   ctx1->uniquekey, ctx2->uniquekey,
									   jointype))
				/* If we set a onerow UniqueKey to joinrel, we don't need other. */
				return;
		}
	}
}


/*
 * convert_subquery_uniquekeys
 *
 * Covert the UniqueKey in subquery to outer relation.
 */
void convert_subquery_uniquekeys(PlannerInfo *root,
								 RelOptInfo *currel,
								 RelOptInfo *sub_final_rel)
{
	ListCell	*lc;

	if (sub_final_rel->uniquekeys == NIL)
		return;

	if (relation_is_onerow(sub_final_rel))
	{
		add_uniquekey_for_onerow(currel);
		return;
	}

	Assert(currel->subroot != NULL);

	foreach(lc, sub_final_rel->uniquekeys)
	{
		UniqueKey *ukey = lfirst_node(UniqueKey, lc);
		ListCell	*lc;
		List	*exprs = NIL;
		bool	ukey_useful = true;

		/* One row case is handled above */
		Assert(ukey->exprs != NIL);
		foreach(lc, ukey->exprs)
		{
			Var *var;
			TargetEntry *tle = tlist_member(lfirst(lc),
											currel->subroot->processed_tlist);
			if (tle == NULL)
			{
				ukey_useful = false;
				break;
			}
			var = find_var_for_subquery_tle(currel, tle);
			if (var == NULL)
			{
				ukey_useful = false;
				break;
			}
			exprs = lappend(exprs, var);
		}

		if (ukey_useful)
			currel->uniquekeys = lappend(currel->uniquekeys,
										 makeUniqueKey(exprs,
													   ukey->multi_nullvals,
													   ukey->onerow));
	}
}

/*
 * innerrel_keeps_unique
 *
 * Check if Unique key of the innerrel is valid after join. innerrel's UniqueKey
 * will be still valid if innerrel's any-column mergeop outrerel's uniquekey
 * exists in clause_list.
 *
 * Note: the clause_list must be a list of mergeable restrictinfo already.
 */
static bool
innerrel_keeps_unique(PlannerInfo *root,
					  RelOptInfo *outerrel,
					  RelOptInfo *innerrel,
					  List *clause_list,
					  bool reverse)
{
	ListCell	*lc, *lc2, *lc3;

	if (outerrel->uniquekeys == NIL || innerrel->uniquekeys == NIL)
		return false;

	/* Check if there is outerrel's uniquekey in mergeable clause. */
	foreach(lc, outerrel->uniquekeys)
	{
		List	*outer_uq_exprs = lfirst_node(UniqueKey, lc)->exprs;
		bool clauselist_matchs_all_exprs = true;
		foreach(lc2, outer_uq_exprs)
		{
			Node *outer_uq_expr = lfirst(lc2);
			bool find_uq_expr_in_clauselist = false;
			foreach(lc3, clause_list)
			{
				RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc3);
				Node *outer_expr;
				if (reverse)
					outer_expr = rinfo->outer_is_left ? get_rightop(rinfo->clause) : get_leftop(rinfo->clause);
				else
					outer_expr = rinfo->outer_is_left ? get_leftop(rinfo->clause) : get_rightop(rinfo->clause);
				if (equal(outer_expr, outer_uq_expr))
				{
					find_uq_expr_in_clauselist = true;
					break;
				}
			}
			if (!find_uq_expr_in_clauselist)
			{
				/* No need to check the next exprs in the current uniquekey */
				clauselist_matchs_all_exprs = false;
				break;
			}
		}

		if (clauselist_matchs_all_exprs)
			return true;
	}
	return false;
}


/*
 * relation_is_onerow
 * Check if it is a one-row relation by checking UniqueKey.
 */
bool
relation_is_onerow(RelOptInfo *rel)
{
	UniqueKey *ukey;
	if (rel->uniquekeys == NIL)
		return false;
	ukey = linitial_node(UniqueKey, rel->uniquekeys);
	if (ukey->onerow)
	{
		/* Some helpful tiny check for UniqueKey. */

		/* 1. We will only store one UniqueKey for this rel. */
		Assert(list_length(rel->uniquekeys) == 1);
		/* 2. multi_nullvals must be false. */
		Assert(!ukey->multi_nullvals);
		/* 3. exprs must be NIL. */
		Assert(ukey->exprs == NIL);

	}
	return ukey->onerow;
}

/*
 * relation_has_uniquekeys_for
 *		Returns true if we have proofs that 'rel' cannot return multiple rows with
 *		the same values in each of 'exprs'.  Otherwise returns false.
 */
bool
relation_has_uniquekeys_for(PlannerInfo *root, List *pathkeys,
							bool allow_multinulls)
{
	ListCell *lc;
	List *exprs = NIL;

	/* For UniqueKey->onerow case, the uniquekey->exprs is empty as well
	 * so we can't rely on list_is_subset to handle this special cases
	 */
	if (pathkeys == NIL)
		return false;

	foreach(lc, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lc);
		EquivalenceClass *ec = pathkey->pk_eclass;
 		ListCell   *k;

 		foreach(k, ec->ec_members)
 		{
 			EquivalenceMember *mem = (EquivalenceMember *) lfirst(k);
			exprs = lappend(exprs, mem->em_expr);
 		}
	}

	foreach(lc, root->query_uniquekeys)
	{
		UniqueKey *ukey = lfirst_node(UniqueKey, lc);
		if (ukey->multi_nullvals && !allow_multinulls)
			continue;
		if (list_is_subset(ukey->exprs, exprs))
			return true;
	}
	return false;
}


/*
 * Examine the rel's restriction clauses for usable var = const clauses
 */
static List*
gather_mergeable_baserestrictlist(RelOptInfo *rel)
{
	List	*restrictlist = NIL;
	ListCell	*lc;
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/*
		 * Note: can_join won't be set for a restriction clause, but
		 * mergeopfamilies will be if it has a mergejoinable operator and
		 * doesn't contain volatile functions.
		 */
		if (restrictinfo->mergeopfamilies == NIL)
			continue;			/* not mergejoinable */

		/*
		 * The clause certainly doesn't refer to anything but the given rel.
		 * If either side is pseudoconstant then we can use it.
		 */
		if (bms_is_empty(restrictinfo->left_relids))
		{
			/* righthand side is inner */
			restrictinfo->outer_is_left = true;
		}
		else if (bms_is_empty(restrictinfo->right_relids))
		{
			/* lefthand side is inner */
			restrictinfo->outer_is_left = false;
		}
		else
			continue;

		/* OK, add to list */
		restrictlist = lappend(restrictlist, restrictinfo);
	}
	return restrictlist;
}


/*
 * gather_mergeable_joinclauses
 */
static List*
gather_mergeable_joinclauses(RelOptInfo *joinrel,
							 RelOptInfo *outerrel,
							 RelOptInfo *innerrel,
							 List *restrictlist,
							 JoinType jointype)
{
	List	*clause_list = NIL;
	ListCell	*lc;
	foreach(lc, restrictlist)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *)lfirst(lc);
		if (IS_OUTER_JOIN(jointype) &&
			RINFO_IS_PUSHED_DOWN(restrictinfo, joinrel->relids))
			continue;

		/* Ignore if it's not a mergejoinable clause */
		if (!restrictinfo->can_join ||
			restrictinfo->mergeopfamilies == NIL)
			continue;			/* not mergejoinable */

		/*
		 * Check if clause has the form "outer op inner" or "inner op outer",
		 * and if so mark which side is inner.
		 */
		if (!clause_sides_match_join(restrictinfo, outerrel->relids, innerrel->relids))
			continue;			/* no good for these input relations. */

		/* OK, add to list */
		clause_list = lappend(clause_list, restrictinfo);
	}
	return clause_list;
}


/*
 * Return true if uk = Const in the restrictlist
 */
static bool
match_index_to_baserestrictinfo(IndexOptInfo *unique_ind, List *restrictlist)
{
	int c = 0;

	/* A fast path to avoid the 2 loop scan. */
	if (list_length(restrictlist) < unique_ind->ncolumns)
		return false;

	for(c = 0;  c < unique_ind->ncolumns; c++)
	{
		ListCell	*lc;
		bool	found_in_restrictinfo = false;
		foreach(lc, restrictlist)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
			Node	   *rexpr;

			/*
			 * The condition's equality operator must be a member of the
			 * index opfamily, else it is not asserting the right kind of
			 * equality behavior for this index.  We check this first
			 * since it's probably cheaper than match_index_to_operand().
			 */
			if (!list_member_oid(rinfo->mergeopfamilies, unique_ind->opfamily[c]))
				continue;

			/*
			 * XXX at some point we may need to check collations here too.
			 * For the moment we assume all collations reduce to the same
			 * notion of equality.
			 */

			/* OK, see if the condition operand matches the index key */
			if (rinfo->outer_is_left)
				rexpr = get_rightop(rinfo->clause);
			else
				rexpr = get_leftop(rinfo->clause);

			if (match_index_to_operand(rexpr, c, unique_ind))
			{
				found_in_restrictinfo = true;
				break;
			}
		}
		if (!found_in_restrictinfo)
			return false;
	}
	return true;
}

/*
 * add_uniquekey_from_index
 *	We only add the Index Vars whose expr exists in rel->reltarget
 */
static void
add_uniquekey_from_index(RelOptInfo *rel, IndexOptInfo *unique_index)
{
	int	pos;
	List	*exprs = NIL;
	bool	multi_nullvals = false;

	/* Fast path.
	 * Check if the indexed columns are used in this relation, if not return fast.
	 */
	for(pos = 0; pos < unique_index->ncolumns; pos++)
	{
		int attno = unique_index->indexkeys[pos];
		if (bms_is_empty(rel->attr_needed[attno - rel->min_attr]))
			/* The indexed column is not needed in this relation. */
			return;
		if (!bms_is_member(attno - FirstLowInvalidHeapAttributeNumber,
						   rel->notnullattrs))
			multi_nullvals = true;
	}

	exprs = get_tlist_exprs(unique_index->indextlist, true);
	rel->uniquekeys = lappend(rel->uniquekeys,
							  makeUniqueKey(exprs,
											multi_nullvals,
											false));
}


/*
 * add_uniquekey_for_onerow
 * If we are sure that the relation only returns one row, then all the columns
 * are unique. However we don't need to create UniqueKey for every column, we
 * just set UniqueKey->onerow to true is OK and leave the exprs = NIL.
 */
void
add_uniquekey_for_onerow(RelOptInfo *rel)
{
	rel->uniquekeys = list_make1(makeUniqueKey(NIL, /* No need to set exprs */
											   false, /* onerow can't have multi_nullvals */
											   true));

}

/*
 * initililze_uniquecontext_for_joinrel
 * Return a List of UniqueKeyContext for an inputrel
 */
static List *
initililze_uniquecontext_for_joinrel(RelOptInfo *inputrel)
{
	List	*res = NIL;
	ListCell *lc;
	foreach(lc,  inputrel->uniquekeys)
	{
		UniqueKeyContext context;
		context = palloc(sizeof(struct UniqueKeyContextData));
		context->uniquekey = lfirst_node(UniqueKey, lc);
		context->added_to_joinrel = false;
		context->useful = true;
		res = lappend(res, context);
	}
	return res;
}

/*
 * clause_sides_match_join
 *	  Determine whether a join clause is of the right form to use in this join.
 *
 * We already know that the clause is a binary opclause referencing only the
 * rels in the current join.  The point here is to check whether it has the
 * form "outerrel_expr op innerrel_expr" or "innerrel_expr op outerrel_expr",
 * rather than mixing outer and inner vars on either side.  If it matches,
 * we set the transient flag outer_is_left to identify which side is which.
 */
static bool
clause_sides_match_join(RestrictInfo *rinfo, Relids outerrelids,
						Relids innerrelids)
{
	if (bms_is_subset(rinfo->left_relids, outerrelids) &&
		bms_is_subset(rinfo->right_relids, innerrelids))
	{
		/* lefthand side is outer */
		rinfo->outer_is_left = true;
		return true;
	}
	else if (bms_is_subset(rinfo->left_relids, innerrelids) &&
			 bms_is_subset(rinfo->right_relids, outerrelids))
	{
		/* righthand side is outer */
		rinfo->outer_is_left = false;
		return true;
	}
	return false;				/* no good for these input relations */
}

/*
 * get_exprs_from_uniquekey
 *	Unify the way of get List of exprs from a one-row UniqueKey or
 * normal UniqueKey. Return a List of exprs.
 *
 * rel1: The relation which you want to get the exprs.
 * ukey: The UniqueKey you want to get the exprs.
 */
static List *
get_exprs_from_uniquekey(RelOptInfo *joinrel, RelOptInfo *rel1, UniqueKey *ukey)
{
	ListCell *lc;
	bool onerow = rel1 != NULL && relation_is_onerow(rel1);

	List	*res = NIL;
	Assert(onerow || ukey);
	if (onerow)
	{
		/* Only cares about the exprs still exist in joinrel */
		foreach(lc, joinrel->reltarget->exprs)
		{
			Bitmapset *relids = pull_varnos(lfirst(lc));
			if (bms_is_subset(relids, rel1->relids))
			{
				res = lappend(res, list_make1(lfirst(lc)));
			}
		}
	}
	else
	{
		res = list_make1(ukey->exprs);
	}
	return res;
}

/*
 * Partitioned table Unique Keys.
 * The partition table unique key is maintained as:
 * 1. The index must be unique as usual.
 * 2. The index must contains partition key.
 * 3. The index must exist on all the child rel. see simple_indexinfo_equal for
 *    how we compare it.
 */

/* index_constains_partkey
 * return true if the index contains the partiton key.
 */
static bool
index_constains_partkey(RelOptInfo *partrel,  IndexOptInfo *ind)
{
	ListCell	*lc;
	int	i;
	Assert(IS_PARTITIONED_REL(partrel));

	for(i = 0; i < partrel->part_scheme->partnatts; i++)
	{
		Node *part_expr = linitial(partrel->partexprs[i]);
		bool found_in_index = false;
		foreach(lc, ind->indextlist)
		{
			Expr *index_expr = lfirst_node(TargetEntry, lc)->expr;
			if (equal(index_expr, part_expr))
			{
				found_in_index = true;
				break;
			}
		}
		if (!found_in_index)
			return false;
	}
	return true;
}

/*
 * simple_indexinfo_equal
 *
 * Used to check if the 2 index is same as each other. The index here
 * is COPIED from childrel and did some tiny changes(see
 * simple_copy_indexinfo_to_parent)
 */
static bool
simple_indexinfo_equal(IndexOptInfo *ind1, IndexOptInfo *ind2)
{
	Size oid_cmp_len = sizeof(Oid) * ind1->ncolumns;
	return ind1->ncolumns == ind2->ncolumns &&
		ind1->unique == ind2->unique &&
		memcmp(ind1->indexkeys, ind2->indexkeys, sizeof(int) * ind1->ncolumns) == 0 &&
		memcmp(ind1->opfamily, ind2->opfamily, oid_cmp_len) == 0 &&
		memcmp(ind1->opcintype, ind2->opcintype, oid_cmp_len) == 0 &&
		memcmp(ind1->sortopfamily, ind2->sortopfamily, oid_cmp_len) == 0 &&
		equal(ind1->indextlist, ind2->indextlist);
}


/*
 * The below macros are used for simple_copy_indexinfo_to_parent which is so
 * customized that I don't want to put it to copyfuncs.c. So copy it here.
 */
#define COPY_POINTER_FIELD(fldname, sz) \
	do { \
		Size	_size = (sz); \
		newnode->fldname = palloc(_size); \
		memcpy(newnode->fldname, from->fldname, _size); \
	} while (0)

#define COPY_NODE_FIELD(fldname) \
	(newnode->fldname = copyObjectImpl(from->fldname))

#define COPY_SCALAR_FIELD(fldname) \
	(newnode->fldname = from->fldname)


/*
 * simple_copy_indexinfo_to_parent (from partition)
 * Copy the IndexInfo from child relation to parent relation with some modification,
 * which is used to test:
 * 1. If the same index exists in all the childrels.
 * 2. If the parentrel->reltarget/basicrestrict info matches this index.
 */
static IndexOptInfo *
simple_copy_indexinfo_to_parent(RelOptInfo *parentrel,
								IndexOptInfo *from)
{
	IndexOptInfo *newnode = makeNode(IndexOptInfo);

	COPY_SCALAR_FIELD(ncolumns);
	COPY_SCALAR_FIELD(nkeycolumns);
	COPY_SCALAR_FIELD(unique);
	COPY_SCALAR_FIELD(immediate);
	/* We just need to know if it is NIL or not */
	COPY_SCALAR_FIELD(indpred);
	COPY_SCALAR_FIELD(predOK);
	COPY_POINTER_FIELD(indexkeys, from->ncolumns * sizeof(int));
	COPY_POINTER_FIELD(indexcollations, from->ncolumns * sizeof(Oid));
	COPY_POINTER_FIELD(opfamily, from->ncolumns * sizeof(Oid));
	COPY_POINTER_FIELD(opcintype, from->ncolumns * sizeof(Oid));
	COPY_POINTER_FIELD(sortopfamily, from->ncolumns * sizeof(Oid));
	COPY_NODE_FIELD(indextlist);

	/*
	 * Change relid from partition relid to parent relid so that  the later
	 * index match work.
	 */
	ChangeVarNodes((Node*) newnode->indextlist,
				   from->rel->relid,
				   parentrel->relid, 0);
	newnode->rel = parentrel;
	return newnode;
}

/*
 * adjust_partition_unique_indexlist
 *
 * global_unique_indexes: At the beginning, it contains the copy & modified
 * unique index from the first partition. And then check if each index in it still
 * exists in the following partitions. If no, remove it. at last, it has an
 * index list which exists in all the partitions.
 */
static void
adjust_partition_unique_indexlist(RelOptInfo *parentrel,
								  RelOptInfo *childrel,
								  List **global_unique_indexes)
{
	ListCell	*lc, *lc2;
	foreach(lc, *global_unique_indexes)
	{
		IndexOptInfo	*g_ind = lfirst_node(IndexOptInfo, lc);
		bool found_in_child = false;

		foreach(lc2, childrel->indexlist)
		{
			IndexOptInfo   *p_ind = lfirst_node(IndexOptInfo, lc2);
			IndexOptInfo   *p_ind_copy;
			if (!p_ind->unique || !p_ind->immediate ||
				(p_ind->indpred != NIL && !p_ind->predOK))
				continue;
			p_ind_copy = simple_copy_indexinfo_to_parent(parentrel, p_ind);
			if (simple_indexinfo_equal(p_ind_copy, g_ind))
			{
				found_in_child = true;
				break;
			}
		}

		if (!found_in_child)
			/* There is no same index on other childrel, remove it */
			*global_unique_indexes = foreach_delete_current(*global_unique_indexes, lc);
	}
}

/* Helper function for groupres/distinctrel */
static void
add_uniquekey_from_sortgroups(PlannerInfo *root, RelOptInfo *rel, List *sortgroups)
{
	Query *parse = root->parse;
	List	*exprs;

	/* XXX: If there are some vars which is not in current levelsup, the semantic is
	 * imprecise, should we avoid it? levelsup = 1 is just a demo, maybe we need to
	 * check every level other than 0, if so, we need write another pull_var_walker.
	 */
	List	*upper_vars = pull_vars_of_level((Node*)sortgroups, 1);

	if (upper_vars != NIL)
		return;

	exprs = get_sortgrouplist_exprs(sortgroups, parse->targetList);
	rel->uniquekeys = lappend(rel->uniquekeys,
							  makeUniqueKey(exprs,
											false, /* sortgroupclause can't be multi_nullvals */
											false));

}


/*
 * add_combined_uniquekey
 * The combination of both UniqueKeys is a valid UniqueKey for joinrel no matter
 * the jointype.
 * Note: This function is called when either single side of the UniqueKeys is not
 * valid any more after join.
 */
bool
add_combined_uniquekey(RelOptInfo *joinrel,
					   RelOptInfo *outer_rel,
					   RelOptInfo *inner_rel,
					   UniqueKey *outer_ukey,
					   UniqueKey *inner_ukey,
					   JoinType jointype)
{

	ListCell	*lc1, *lc2;

	/* Either side has multi_nullvals, the combined UniqueKey has multi_nullvals */
	bool multi_nullvals = outer_ukey->multi_nullvals || inner_ukey->multi_nullvals;

	/* If we have outer join, it implies we will have mutli_nullvals */
	multi_nullvals = multi_nullvals || IS_OUTER_JOIN(jointype);

	/* The only case we can get onerow joinrel after join */
	if  (outer_ukey->onerow && inner_ukey->onerow && jointype == JOIN_INNER)
	{
		add_uniquekey_for_onerow(joinrel);
		return true;
	}

	foreach(lc1, get_exprs_from_uniquekey(joinrel, outer_rel, outer_ukey))
	{
		foreach(lc2, get_exprs_from_uniquekey(joinrel, inner_rel, inner_ukey))
		{
			List *exprs = list_concat_copy(lfirst_node(List, lc1), lfirst_node(List, lc2));
			joinrel->uniquekeys = lappend(joinrel->uniquekeys,
										  makeUniqueKey(exprs,
														multi_nullvals,
														false));
		}
	}
	return false;
}

List*
build_uniquekeys(PlannerInfo *root, List *sortclauses)
{
	List *result = NIL;
	List *sortkeys;
	ListCell *l;

	sortkeys = make_pathkeys_for_uniquekeys(root,
											sortclauses,
											root->processed_tlist);

	/* Create a uniquekey and add it to the list */
	foreach(l, sortkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(l);
		EquivalenceClass *ec = pathkey->pk_eclass;
 		ListCell   *k;
		List *exprs = NIL;

 		foreach(k, ec->ec_members)
 		{
 			EquivalenceMember *mem = (EquivalenceMember *) lfirst(k);
			exprs = lappend(exprs, mem->em_expr);
 		}

		result = lappend(result, makeUniqueKey(exprs, false, false));
	}

	return result;
}
