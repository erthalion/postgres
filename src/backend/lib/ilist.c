/*-------------------------------------------------------------------------
 *
 * ilist.c
 *	  support for integrated/inline doubly- and singly- linked lists
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/lib/ilist.c
 *
 * NOTES
 *	  This file only contains functions that are too big to be considered
 *	  for inlining.  See ilist.h for most of the goodies.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/ilist.h"

/*
 * Delete 'node' from list.
 *
 * It is not allowed to delete a 'node' which is not in the list 'head'
 *
 * Caution: this is O(n); consider using slist_delete_current() instead.
 */
void
slist_delete(slist_head *head, slist_node *node)
{
	slist_node *last = &head->head;
	slist_node *cur;
	bool		found PG_USED_FOR_ASSERTS_ONLY = false;

	while ((cur = last->next) != NULL)
	{
		if (cur == node)
		{
			last->next = cur->next;
#ifdef USE_ASSERT_CHECKING
			found = true;
#endif
			break;
		}
		last = cur;
	}
	Assert(found);

	slist_check(head);
}

/*
 * Verify integrity of a doubly linked list
 */
dlist_head*
dlist_check_force(dlist_head *head)
{
	dlist_node *cur;

	if (head == NULL)
		elog(PANIC, "doubly linked list head address is NULL");

	if (head->head.next == NULL && head->head.prev == NULL)
		return head;					/* OK, initialized as zeroes */

	/* iterate in forward direction */
	for (cur = head->head.next; cur != &head->head; cur = cur->next)
	{
		if (cur == NULL ||
			cur->next == NULL ||
			cur->prev == NULL ||
			cur->prev->next != cur ||
			cur->next->prev != cur)
			elog(PANIC, "doubly linked list is corrupted");
	}

	/* iterate in backward direction */
	for (cur = head->head.prev; cur != &head->head; cur = cur->prev)
	{
		if (cur == NULL ||
			cur->next == NULL ||
			cur->prev == NULL ||
			cur->prev->next != cur ||
			cur->next->prev != cur)
			elog(PANIC, "doubly linked list is corrupted");
	}

	return head;
}

/*
 * Verify integrity of a singly linked list
 */
slist_head *
slist_check_force(slist_head *head)
{
	slist_node *cur;

	if (head == NULL)
		elog(PANIC, "singly linked list head address is NULL");

	/*
	 * there isn't much we can test in a singly linked list except that it
	 * actually ends sometime, i.e. hasn't introduced a cycle or similar
	 */
	for (cur = head->head.next; cur != NULL; cur = cur->next)
		;

	return head;
}

bool
dlist_is_member(dlist_head *head, dlist_node *node)
{
	dlist_iter iter;

	dlist_foreach(iter, head)
	{
		if (iter.cur == node)
			return true;
	}

	return false;
}
