/*-------------------------------------------------------------------------
 *
 * llvmjit_types.c
 *	  List of types needed by JIT emitting code.
 *
 * JIT emitting code often needs to access struct elements, create functions
 * with the correct signature etc. To allow synchronizing these types with a
 * low chance of definitions getting out of sync, this file lists types and
 * functions that directly need to be accessed from LLVM.
 *
 * When LlVM is first used in a backend, a bitcode version of this file, will
 * be loaded. The needed types and signatures will be stored into Struct*,
 * Type*, Func* variables.
 *
 * NB: This file will not be linked into the server, it's just converted to
 * bitcode.
 *
 *
 * Copyright (c) 2016-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/lib/llvmjit_types.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

/*
 * List of types needed for JITing. These have to be non-static, otherwise
 * clang/LLVM will omit them.  As this file will never be linked into
 * anything, that's harmless.
 */
size_t		TypeSizeT;


/*
 * To determine which attributes functions need to have (depends e.g. on
 * compiler version and settings) to be compatible for inlining, we simply
 * copy the attributes of this function.
 */
extern Datum AttributeTemplate(PG_FUNCTION_ARGS);
Datum
AttributeTemplate(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}


/*
 * To force signatures of functions used during JITing to be present,
 * reference the functions required. This again has to be non-static, to avoid
 * being removed as unnecessary.
 */
void	   *referenced_functions[] =
{
	strlen
};
