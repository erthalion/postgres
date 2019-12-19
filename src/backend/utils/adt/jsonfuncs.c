/*-------------------------------------------------------------------------
 *
 * jsonfuncs.c
 *		Functions to process JSON data types.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsonfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/execExpr.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/fmgroids.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/* Operations available for setPath */
#define JB_PATH_CREATE					0x0001
#define JB_PATH_DELETE					0x0002
#define JB_PATH_REPLACE					0x0004
#define JB_PATH_INSERT_BEFORE			0x0008
#define JB_PATH_INSERT_AFTER			0x0010
#define JB_PATH_CREATE_OR_INSERT \
	(JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER | JB_PATH_CREATE)

/* state for json_object_keys */
typedef struct OkeysState
{
	JsonLexContext *lex;
	char	  **result;
	int			result_size;
	int			result_count;
	int			sent_count;
} OkeysState;

/* state for iterate_json_values function */
typedef struct IterateJsonStringValuesState
{
	JsonLexContext *lex;
	JsonIterateStringValuesAction action;	/* an action that will be applied
											 * to each json value */
	void	   *action_state;	/* any necessary context for iteration */
	uint32		flags;			/* what kind of elements from a json we want
								 * to iterate */
} IterateJsonStringValuesState;

/* state for transform_json_string_values function */
typedef struct TransformJsonStringValuesState
{
	JsonLexContext *lex;
	StringInfo	strval;			/* resulting json */
	JsonTransformStringValuesAction action; /* an action that will be applied
											 * to each json value */
	void	   *action_state;	/* any necessary context for transformation */
} TransformJsonStringValuesState;

/* state for json_get* functions */
typedef struct GetState
{
	JsonLexContext *lex;
	text	   *tresult;
	char	   *result_start;
	bool		normalize_results;
	bool		next_scalar;
	int			npath;			/* length of each path-related array */
	char	  **path_names;		/* field name(s) being sought */
	int		   *path_indexes;	/* array index(es) being sought */
	bool	   *pathok;			/* is path matched to current depth? */
	int		   *array_cur_index;	/* current element index at each path
									 * level */
} GetState;

/* state for json_array_length */
typedef struct AlenState
{
	JsonLexContext *lex;
	int			count;
} AlenState;

/* state for json_each */
typedef struct EachState
{
	JsonLexContext *lex;
	Tuplestorestate *tuple_store;
	TupleDesc	ret_tdesc;
	MemoryContext tmp_cxt;
	char	   *result_start;
	bool		normalize_results;
	bool		next_scalar;
	char	   *normalized_scalar;
} EachState;

/* state for json_array_elements */
typedef struct ElementsState
{
	JsonLexContext *lex;
	const char *function_name;
	Tuplestorestate *tuple_store;
	TupleDesc	ret_tdesc;
	MemoryContext tmp_cxt;
	char	   *result_start;
	bool		normalize_results;
	bool		next_scalar;
	char	   *normalized_scalar;
} ElementsState;

/* state for get_json_object_as_hash */
typedef struct JHashState
{
	JsonLexContext *lex;
	const char *function_name;
	HTAB	   *hash;
	char	   *saved_scalar;
	char	   *save_json_start;
	JsonTokenType saved_token_type;
} JHashState;

/* hashtable element */
typedef struct JsonHashEntry
{
	char		fname[NAMEDATALEN]; /* hash key (MUST BE FIRST) */
	char	   *val;
	JsonTokenType type;
} JsonHashEntry;

/* structure to cache type I/O metadata needed for populate_scalar() */
typedef struct ScalarIOData
{
	Oid			typioparam;
	FmgrInfo	typiofunc;
} ScalarIOData;

/* these two structures are used recursively */
typedef struct ColumnIOData ColumnIOData;
typedef struct RecordIOData RecordIOData;

/* structure to cache metadata needed for populate_array() */
typedef struct ArrayIOData
{
	ColumnIOData *element_info; /* metadata cache */
	Oid			element_type;	/* array element type id */
	int32		element_typmod; /* array element type modifier */
} ArrayIOData;

/* structure to cache metadata needed for populate_composite() */
typedef struct CompositeIOData
{
	/*
	 * We use pointer to a RecordIOData here because variable-length struct
	 * RecordIOData can't be used directly in ColumnIOData.io union
	 */
	RecordIOData *record_io;	/* metadata cache for populate_record() */
	TupleDesc	tupdesc;		/* cached tuple descriptor */
	/* these fields differ from target type only if domain over composite: */
	Oid			base_typid;		/* base type id */
	int32		base_typmod;	/* base type modifier */
	/* this field is used only if target type is domain over composite: */
	void	   *domain_info;	/* opaque cache for domain checks */
} CompositeIOData;

/* structure to cache metadata needed for populate_domain() */
typedef struct DomainIOData
{
	ColumnIOData *base_io;		/* metadata cache */
	Oid			base_typid;		/* base type id */
	int32		base_typmod;	/* base type modifier */
	void	   *domain_info;	/* opaque cache for domain checks */
} DomainIOData;

/* enumeration type categories */
typedef enum TypeCat
{
	TYPECAT_SCALAR = 's',
	TYPECAT_ARRAY = 'a',
	TYPECAT_COMPOSITE = 'c',
	TYPECAT_COMPOSITE_DOMAIN = 'C',
	TYPECAT_DOMAIN = 'd'
} TypeCat;

/* these two are stolen from hstore / record_out, used in populate_record* */

/* structure to cache record metadata needed for populate_record_field() */
struct ColumnIOData
{
	Oid			typid;			/* column type id */
	int32		typmod;			/* column type modifier */
	TypeCat		typcat;			/* column type category */
	ScalarIOData scalar_io;		/* metadata cache for direct conversion
								 * through input function */
	union
	{
		ArrayIOData array;
		CompositeIOData composite;
		DomainIOData domain;
	}			io;				/* metadata cache for various column type
								 * categories */
};

/* structure to cache record metadata needed for populate_record() */
struct RecordIOData
{
	Oid			record_type;
	int32		record_typmod;
	int			ncolumns;
	ColumnIOData columns[FLEXIBLE_ARRAY_MEMBER];
};

/* per-query cache for populate_record_worker and populate_recordset_worker */
typedef struct PopulateRecordCache
{
	Oid			argtype;		/* declared type of the record argument */
	ColumnIOData c;				/* metadata cache for populate_composite() */
	MemoryContext fn_mcxt;		/* where this is stored */
} PopulateRecordCache;

/* per-call state for populate_recordset */
typedef struct PopulateRecordsetState
{
	JsonLexContext *lex;
	const char *function_name;
	HTAB	   *json_hash;
	char	   *saved_scalar;
	char	   *save_json_start;
	JsonTokenType saved_token_type;
	Tuplestorestate *tuple_store;
	HeapTupleHeader rec;
	PopulateRecordCache *cache;
} PopulateRecordsetState;

/* common data for populate_array_json() and populate_array_dim_jsonb() */
typedef struct PopulateArrayContext
{
	ArrayBuildState *astate;	/* array build state */
	ArrayIOData *aio;			/* metadata cache */
	MemoryContext acxt;			/* array build memory context */
	MemoryContext mcxt;			/* cache memory context */
	const char *colname;		/* for diagnostics only */
	int		   *dims;			/* dimensions */
	int		   *sizes;			/* current dimension counters */
	int			ndims;			/* number of dimensions */
} PopulateArrayContext;

/* state for populate_array_json() */
typedef struct PopulateArrayState
{
	JsonLexContext *lex;		/* json lexer */
	PopulateArrayContext *ctx;	/* context */
	char	   *element_start;	/* start of the current array element */
	char	   *element_scalar; /* current array element token if it is a
								 * scalar */
	JsonTokenType element_type; /* current array element type */
} PopulateArrayState;

/* state for json_strip_nulls */
typedef struct StripnullState
{
	JsonLexContext *lex;
	StringInfo	strval;
	bool		skip_next_null;
} StripnullState;

/* structure for generalized json/jsonb value passing */
typedef struct JsValue
{
	bool		is_json;		/* json/jsonb */
	union
	{
		struct
		{
			char	   *str;	/* json string */
			int			len;	/* json string length or -1 if null-terminated */
			JsonTokenType type; /* json type */
		}			json;		/* json value */

		JsonbValue *jsonb;		/* jsonb value */
	}			val;
} JsValue;

typedef struct JsObject
{
	bool		is_json;		/* json/jsonb */
	union
	{
		HTAB	   *json_hash;
		JsonbContainer *jsonb_cont;
	}			val;
} JsObject;

/* useful macros for testing JsValue properties */
#define JsValueIsNull(jsv) \
	((jsv)->is_json ?  \
		(!(jsv)->val.json.str || (jsv)->val.json.type == JSON_TOKEN_NULL) : \
		(!(jsv)->val.jsonb || (jsv)->val.jsonb->type == jbvNull))

#define JsValueIsString(jsv) \
	((jsv)->is_json ? (jsv)->val.json.type == JSON_TOKEN_STRING \
		: ((jsv)->val.jsonb && (jsv)->val.jsonb->type == jbvString))

#define JsObjectIsEmpty(jso) \
	((jso)->is_json \
		? hash_get_num_entries((jso)->val.json_hash) == 0 \
		: ((jso)->val.jsonb_cont == NULL || \
		   JsonContainerSize((jso)->val.jsonb_cont) == 0))

#define JsObjectFree(jso) \
	do { \
		if ((jso)->is_json) \
			hash_destroy((jso)->val.json_hash); \
	} while (0)

/* state for assignment of a single subscript */
typedef struct JsonbSubscriptState
{
	bool		exists;			/* does this element exist? */
	bool		is_array;		/* is it array or object? */
	int			array_size;		/* size of array */
	int			array_index;	/* index in array (negative means prepending) */
} JsonbSubscriptState;

/* state for subscript assignment */
typedef struct JsonbAssignState
{
	JsonbParseState *ps;		/* jsonb building state */
	JsonbIterator *iter;		/* source jsonb iterator */
	JsonbSubscriptState subscripts[MAX_SUBSCRIPT_DEPTH + 1]; /* per-subscript states */
} JsonbAssignState;

/* semantic action functions for json_object_keys */
static void okeys_object_field_start(void *state, char *fname, bool isnull);
static void okeys_array_start(void *state);
static void okeys_scalar(void *state, char *token, JsonTokenType tokentype);

/* semantic action functions for json_get* functions */
static void get_object_start(void *state);
static void get_object_end(void *state);
static void get_object_field_start(void *state, char *fname, bool isnull);
static void get_object_field_end(void *state, char *fname, bool isnull);
static void get_array_start(void *state);
static void get_array_end(void *state);
static void get_array_element_start(void *state, bool isnull);
static void get_array_element_end(void *state, bool isnull);
static void get_scalar(void *state, char *token, JsonTokenType tokentype);

/* common worker function for json getter functions */
static Datum get_path_all(FunctionCallInfo fcinfo, bool as_text);
static text *get_worker(text *json, char **tpath, int *ipath, int npath,
						bool normalize_results);
static Datum get_jsonb_path_all(FunctionCallInfo fcinfo, bool as_text);
static text *JsonbValueAsText(JsonbValue *v);

/* semantic action functions for json_array_length */
static void alen_object_start(void *state);
static void alen_scalar(void *state, char *token, JsonTokenType tokentype);
static void alen_array_element_start(void *state, bool isnull);

/* common workers for json{b}_each* functions */
static Datum each_worker(FunctionCallInfo fcinfo, bool as_text);
static Datum each_worker_jsonb(FunctionCallInfo fcinfo, const char *funcname,
							   bool as_text);

/* semantic action functions for json_each */
static void each_object_field_start(void *state, char *fname, bool isnull);
static void each_object_field_end(void *state, char *fname, bool isnull);
static void each_array_start(void *state);
static void each_scalar(void *state, char *token, JsonTokenType tokentype);

/* common workers for json{b}_array_elements_* functions */
static Datum elements_worker(FunctionCallInfo fcinfo, const char *funcname,
							 bool as_text);
static Datum elements_worker_jsonb(FunctionCallInfo fcinfo, const char *funcname,
								   bool as_text);

/* semantic action functions for json_array_elements */
static void elements_object_start(void *state);
static void elements_array_element_start(void *state, bool isnull);
static void elements_array_element_end(void *state, bool isnull);
static void elements_scalar(void *state, char *token, JsonTokenType tokentype);

/* turn a json object into a hash table */
static HTAB *get_json_object_as_hash(char *json, int len, const char *funcname);

/* semantic actions for populate_array_json */
static void populate_array_object_start(void *_state);
static void populate_array_array_end(void *_state);
static void populate_array_element_start(void *_state, bool isnull);
static void populate_array_element_end(void *_state, bool isnull);
static void populate_array_scalar(void *_state, char *token, JsonTokenType tokentype);

/* semantic action functions for get_json_object_as_hash */
static void hash_object_field_start(void *state, char *fname, bool isnull);
static void hash_object_field_end(void *state, char *fname, bool isnull);
static void hash_array_start(void *state);
static void hash_scalar(void *state, char *token, JsonTokenType tokentype);

/* semantic action functions for populate_recordset */
static void populate_recordset_object_field_start(void *state, char *fname, bool isnull);
static void populate_recordset_object_field_end(void *state, char *fname, bool isnull);
static void populate_recordset_scalar(void *state, char *token, JsonTokenType tokentype);
static void populate_recordset_object_start(void *state);
static void populate_recordset_object_end(void *state);
static void populate_recordset_array_start(void *state);
static void populate_recordset_array_element_start(void *state, bool isnull);

/* semantic action functions for json_strip_nulls */
static void sn_object_start(void *state);
static void sn_object_end(void *state);
static void sn_array_start(void *state);
static void sn_array_end(void *state);
static void sn_object_field_start(void *state, char *fname, bool isnull);
static void sn_array_element_start(void *state, bool isnull);
static void sn_scalar(void *state, char *token, JsonTokenType tokentype);

/* worker functions for populate_record, to_record, populate_recordset and to_recordset */
static Datum populate_recordset_worker(FunctionCallInfo fcinfo, const char *funcname,
									   bool is_json, bool have_record_arg);
static Datum populate_record_worker(FunctionCallInfo fcinfo, const char *funcname,
									bool is_json, bool have_record_arg);

/* helper functions for populate_record[set] */
static HeapTupleHeader populate_record(TupleDesc tupdesc, RecordIOData **record_p,
									   HeapTupleHeader defaultval, MemoryContext mcxt,
									   JsObject *obj);
static void get_record_type_from_argument(FunctionCallInfo fcinfo,
										  const char *funcname,
										  PopulateRecordCache *cache);
static void get_record_type_from_query(FunctionCallInfo fcinfo,
									   const char *funcname,
									   PopulateRecordCache *cache);
static void JsValueToJsObject(JsValue *jsv, JsObject *jso);
static Datum populate_composite(CompositeIOData *io, Oid typid,
								const char *colname, MemoryContext mcxt,
								HeapTupleHeader defaultval, JsValue *jsv, bool isnull);
static Datum populate_scalar(ScalarIOData *io, Oid typid, int32 typmod, JsValue *jsv);
static void prepare_column_cache(ColumnIOData *column, Oid typid, int32 typmod,
								 MemoryContext mcxt, bool need_scalar);
static Datum populate_record_field(ColumnIOData *col, Oid typid, int32 typmod,
								   const char *colname, MemoryContext mcxt, Datum defaultval,
								   JsValue *jsv, bool *isnull);
static RecordIOData *allocate_record_info(MemoryContext mcxt, int ncolumns);
static bool JsObjectGetField(JsObject *obj, char *field, JsValue *jsv);
static void populate_recordset_record(PopulateRecordsetState *state, JsObject *obj);
static void populate_array_json(PopulateArrayContext *ctx, char *json, int len);
static void populate_array_dim_jsonb(PopulateArrayContext *ctx, JsonbValue *jbv,
									 int ndim);
static void populate_array_report_expected_array(PopulateArrayContext *ctx, int ndim);
static void populate_array_assign_ndims(PopulateArrayContext *ctx, int ndims);
static void populate_array_check_dimension(PopulateArrayContext *ctx, int ndim);
static void populate_array_element(PopulateArrayContext *ctx, int ndim, JsValue *jsv);
static Datum populate_array(ArrayIOData *aio, const char *colname,
							MemoryContext mcxt, JsValue *jsv);
static Datum populate_domain(DomainIOData *io, Oid typid, const char *colname,
							 MemoryContext mcxt, JsValue *jsv, bool isnull);

/* functions supporting jsonb_delete, jsonb_set and jsonb_concat */
static JsonbValue *IteratorConcat(JsonbIterator **it1, JsonbIterator **it2,
								  JsonbParseState **state);
static Datum jsonb_get_element(Jsonb *jb, Datum *path, int npath,
							   bool *isnull, bool as_text);
static JsonbValue *setPath(JsonbIterator **it, Datum *path_elems,
						   bool *path_nulls, int path_len,
						   JsonbParseState **st, int level, JsonbValue *newval,
						   int op_type);
static void setPathObject(JsonbIterator **it, Datum *path_elems,
						  bool *path_nulls, int path_len, JsonbParseState **st,
						  int level, JsonbValue *newval, int op_type);
static void setPathArray(JsonbIterator **it, Datum *path_elems,
						 bool *path_nulls, int path_len, JsonbParseState **st,
						 int level,
						 JsonbValue *newval, uint32 nelems, int op_type);
static bool copyJsonbObject(JsonbParseState **st, JsonbIterator **it,
							const JsonbValue *key);

/* function supporting iterate_json_values */
static void iterate_values_scalar(void *state, char *token, JsonTokenType tokentype);
static void iterate_values_object_field_start(void *state, char *fname, bool isnull);

/* functions supporting transform_json_string_values */
static void transform_string_values_object_start(void *state);
static void transform_string_values_object_end(void *state);
static void transform_string_values_array_start(void *state);
static void transform_string_values_array_end(void *state);
static void transform_string_values_object_field_start(void *state, char *fname, bool isnull);
static void transform_string_values_array_element_start(void *state, bool isnull);
static void transform_string_values_scalar(void *state, char *token, JsonTokenType tokentype);

static SubscriptingRef *jsonb_subscript_prepare(bool isAssignment,
												SubscriptingRef *sbsref);

static SubscriptingRef *jsonb_subscript_validate(bool isAssignment,
												 SubscriptingRef *sbsref,
												 ParseState *pstate);
static Datum jsonb_subscript_fetch(Datum containerSource, SubscriptingRefState *sbstate);
static Datum jsonb_subscript_assign(Datum containerSource, SubscriptingRefState *sbstate);

/*
 * SQL function json_object_keys
 *
 * Returns the set of keys for the object argument.
 *
 * This SRF operates in value-per-call mode. It processes the
 * object during the first call, and the keys are simply stashed
 * in an array, whose size is expanded as necessary. This is probably
 * safe enough for a list of keys of a single object, since they are
 * limited in size to NAMEDATALEN and the number of keys is unlikely to
 * be so huge that it has major memory implications.
 */
Datum
jsonb_object_keys(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	OkeysState *state;
	int			i;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		Jsonb	   *jb = PG_GETARG_JSONB_P(0);
		bool		skipNested = false;
		JsonbIterator *it;
		JsonbValue	v;
		JsonbIteratorToken r;

		if (JB_ROOT_IS_SCALAR(jb))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot call %s on a scalar",
							"jsonb_object_keys")));
		else if (JB_ROOT_IS_ARRAY(jb))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot call %s on an array",
							"jsonb_object_keys")));

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		state = palloc(sizeof(OkeysState));

		state->result_size = JB_ROOT_COUNT(jb);
		state->result_count = 0;
		state->sent_count = 0;
		state->result = palloc(state->result_size * sizeof(char *));

		it = JsonbIteratorInit(&jb->root);

		while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
		{
			skipNested = true;

			if (r == WJB_KEY)
			{
				char	   *cstr;

				cstr = palloc(v.val.string.len + 1 * sizeof(char));
				memcpy(cstr, v.val.string.val, v.val.string.len);
				cstr[v.val.string.len] = '\0';
				state->result[state->result_count++] = cstr;
			}
		}

		MemoryContextSwitchTo(oldcontext);
		funcctx->user_fctx = (void *) state;
	}

	funcctx = SRF_PERCALL_SETUP();
	state = (OkeysState *) funcctx->user_fctx;

	if (state->sent_count < state->result_count)
	{
		char	   *nxt = state->result[state->sent_count++];

		SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(nxt));
	}

	/* cleanup to reduce or eliminate memory leaks */
	for (i = 0; i < state->result_count; i++)
		pfree(state->result[i]);
	pfree(state->result);
	pfree(state);

	SRF_RETURN_DONE(funcctx);
}


Datum
json_object_keys(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	OkeysState *state;
	int			i;

	if (SRF_IS_FIRSTCALL())
	{
		text	   *json = PG_GETARG_TEXT_PP(0);
		JsonLexContext *lex = makeJsonLexContext(json, true);
		JsonSemAction *sem;
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		state = palloc(sizeof(OkeysState));
		sem = palloc0(sizeof(JsonSemAction));

		state->lex = lex;
		state->result_size = 256;
		state->result_count = 0;
		state->sent_count = 0;
		state->result = palloc(256 * sizeof(char *));

		sem->semstate = (void *) state;
		sem->array_start = okeys_array_start;
		sem->scalar = okeys_scalar;
		sem->object_field_start = okeys_object_field_start;
		/* remainder are all NULL, courtesy of palloc0 above */

		pg_parse_json(lex, sem);
		/* keys are now in state->result */

		pfree(lex->strval->data);
		pfree(lex->strval);
		pfree(lex);
		pfree(sem);

		MemoryContextSwitchTo(oldcontext);
		funcctx->user_fctx = (void *) state;
	}

	funcctx = SRF_PERCALL_SETUP();
	state = (OkeysState *) funcctx->user_fctx;

	if (state->sent_count < state->result_count)
	{
		char	   *nxt = state->result[state->sent_count++];

		SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(nxt));
	}

	/* cleanup to reduce or eliminate memory leaks */
	for (i = 0; i < state->result_count; i++)
		pfree(state->result[i]);
	pfree(state->result);
	pfree(state);

	SRF_RETURN_DONE(funcctx);
}

static void
okeys_object_field_start(void *state, char *fname, bool isnull)
{
	OkeysState *_state = (OkeysState *) state;

	/* only collecting keys for the top level object */
	if (_state->lex->lex_level != 1)
		return;

	/* enlarge result array if necessary */
	if (_state->result_count >= _state->result_size)
	{
		_state->result_size *= 2;
		_state->result = (char **)
			repalloc(_state->result, sizeof(char *) * _state->result_size);
	}

	/* save a copy of the field name */
	_state->result[_state->result_count++] = pstrdup(fname);
}

static void
okeys_array_start(void *state)
{
	OkeysState *_state = (OkeysState *) state;

	/* top level must be a json object */
	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot call %s on an array",
						"json_object_keys")));
}

static void
okeys_scalar(void *state, char *token, JsonTokenType tokentype)
{
	OkeysState *_state = (OkeysState *) state;

	/* top level must be a json object */
	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot call %s on a scalar",
						"json_object_keys")));
}

/*
 * json and jsonb getter functions
 * these implement the -> ->> #> and #>> operators
 * and the json{b?}_extract_path*(json, text, ...) functions
 */


Datum
json_object_field(PG_FUNCTION_ARGS)
{
	text	   *json = PG_GETARG_TEXT_PP(0);
	text	   *fname = PG_GETARG_TEXT_PP(1);
	char	   *fnamestr = text_to_cstring(fname);
	text	   *result;

	result = get_worker(json, &fnamestr, NULL, 1, false);

	if (result != NULL)
		PG_RETURN_TEXT_P(result);
	else
		PG_RETURN_NULL();
}

Datum
jsonb_object_field(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	text	   *key = PG_GETARG_TEXT_PP(1);
	JsonbValue *v;
	JsonbValue	vbuf;

	if (!JB_ROOT_IS_OBJECT(jb))
		PG_RETURN_NULL();

	v = getKeyJsonValueFromContainer(&jb->root,
									 VARDATA_ANY(key),
									 VARSIZE_ANY_EXHDR(key),
									 &vbuf);

	if (v != NULL)
		PG_RETURN_JSONB_P(JsonbValueToJsonb(v));

	PG_RETURN_NULL();
}

Datum
json_object_field_text(PG_FUNCTION_ARGS)
{
	text	   *json = PG_GETARG_TEXT_PP(0);
	text	   *fname = PG_GETARG_TEXT_PP(1);
	char	   *fnamestr = text_to_cstring(fname);
	text	   *result;

	result = get_worker(json, &fnamestr, NULL, 1, true);

	if (result != NULL)
		PG_RETURN_TEXT_P(result);
	else
		PG_RETURN_NULL();
}

Datum
jsonb_object_field_text(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	text	   *key = PG_GETARG_TEXT_PP(1);
	JsonbValue *v;
	JsonbValue	vbuf;

	if (!JB_ROOT_IS_OBJECT(jb))
		PG_RETURN_NULL();

	v = getKeyJsonValueFromContainer(&jb->root,
									 VARDATA_ANY(key),
									 VARSIZE_ANY_EXHDR(key),
									 &vbuf);

	if (v != NULL && v->type != jbvNull)
		PG_RETURN_TEXT_P(JsonbValueAsText(v));

	PG_RETURN_NULL();
}

Datum
json_array_element(PG_FUNCTION_ARGS)
{
	text	   *json = PG_GETARG_TEXT_PP(0);
	int			element = PG_GETARG_INT32(1);
	text	   *result;

	result = get_worker(json, NULL, &element, 1, false);

	if (result != NULL)
		PG_RETURN_TEXT_P(result);
	else
		PG_RETURN_NULL();
}

Datum
jsonb_array_element(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	int			element = PG_GETARG_INT32(1);
	JsonbValue *v;
	JsonbValue	vbuf;

	if (!JB_ROOT_IS_ARRAY(jb))
		PG_RETURN_NULL();

	/* Handle negative subscript */
	if (element < 0)
	{
		uint32		nelements = JB_ROOT_COUNT(jb);

		if (-element > nelements)
			PG_RETURN_NULL();
		else
			element += nelements;
	}

	v = getIthJsonbValueFromContainer(&jb->root, element, &vbuf);
	if (v != NULL)
		PG_RETURN_JSONB_P(JsonbValueToJsonb(v));

	PG_RETURN_NULL();
}

Datum
json_array_element_text(PG_FUNCTION_ARGS)
{
	text	   *json = PG_GETARG_TEXT_PP(0);
	int			element = PG_GETARG_INT32(1);
	text	   *result;

	result = get_worker(json, NULL, &element, 1, true);

	if (result != NULL)
		PG_RETURN_TEXT_P(result);
	else
		PG_RETURN_NULL();
}

Datum
jsonb_array_element_text(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	int			element = PG_GETARG_INT32(1);
	JsonbValue *v;
	JsonbValue	vbuf;

	if (!JB_ROOT_IS_ARRAY(jb))
		PG_RETURN_NULL();

	/* Handle negative subscript */
	if (element < 0)
	{
		uint32		nelements = JB_ROOT_COUNT(jb);

		if (-element > nelements)
			PG_RETURN_NULL();
		else
			element += nelements;
	}

	v = getIthJsonbValueFromContainer(&jb->root, element, &vbuf);

	if (v != NULL && v->type != jbvNull)
		PG_RETURN_TEXT_P(JsonbValueAsText(v));

	PG_RETURN_NULL();
}

Datum
json_extract_path(PG_FUNCTION_ARGS)
{
	return get_path_all(fcinfo, false);
}

Datum
json_extract_path_text(PG_FUNCTION_ARGS)
{
	return get_path_all(fcinfo, true);
}

static inline bool
jsonb_get_array_index_from_cstring(char *indexstr, long *index)
{
	char	   *endptr;

	errno = 0;
	*index = strtol(indexstr, &endptr, 10);
	if (endptr == indexstr || *endptr != '\0' || errno != 0 ||
		*index > INT_MAX || *index < INT_MIN)
		return false;

	return true;
}

static inline bool
jsonb_get_array_index_from_text(Datum indextext, long *index)
{
	return jsonb_get_array_index_from_cstring(TextDatumGetCString(indextext), index);
}

/*
 * common routine for extract_path functions
 */
static Datum
get_path_all(FunctionCallInfo fcinfo, bool as_text)
{
	text	   *json = PG_GETARG_TEXT_PP(0);
	ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
	text	   *result;
	Datum	   *pathtext;
	bool	   *pathnulls;
	int			npath;
	char	  **tpath;
	int		   *ipath;
	int			i;

	/*
	 * If the array contains any null elements, return NULL, on the grounds
	 * that you'd have gotten NULL if any RHS value were NULL in a nested
	 * series of applications of the -> operator.  (Note: because we also
	 * return NULL for error cases such as no-such-field, this is true
	 * regardless of the contents of the rest of the array.)
	 */
	if (array_contains_nulls(path))
		PG_RETURN_NULL();

	deconstruct_array(path, TEXTOID, -1, false, 'i',
					  &pathtext, &pathnulls, &npath);

	tpath = palloc(npath * sizeof(char *));
	ipath = palloc(npath * sizeof(int));

	for (i = 0; i < npath; i++)
	{
		Assert(!pathnulls[i]);
		tpath[i] = TextDatumGetCString(pathtext[i]);

		/*
		 * we have no idea at this stage what structure the document is so
		 * just convert anything in the path that we can to an integer and set
		 * all the other integers to INT_MIN which will never match.
		 */
		if (*tpath[i] != '\0')
		{
			long		ind;

			if (jsonb_get_array_index_from_cstring(tpath[i], &ind))
				ipath[i] = (int) ind;
			else
				ipath[i] = INT_MIN;
		}
		else
			ipath[i] = INT_MIN;
	}

	result = get_worker(json, tpath, ipath, npath, as_text);

	if (result != NULL)
		PG_RETURN_TEXT_P(result);
	else
		PG_RETURN_NULL();
}

/*
 * get_worker
 *
 * common worker for all the json getter functions
 *
 * json: JSON object (in text form)
 * tpath[]: field name(s) to extract
 * ipath[]: array index(es) (zero-based) to extract, accepts negatives
 * npath: length of tpath[] and/or ipath[]
 * normalize_results: true to de-escape string and null scalars
 *
 * tpath can be NULL, or any one tpath[] entry can be NULL, if an object
 * field is not to be matched at that nesting level.  Similarly, ipath can
 * be NULL, or any one ipath[] entry can be INT_MIN if an array element is
 * not to be matched at that nesting level (a json datum should never be
 * large enough to have -INT_MIN elements due to MaxAllocSize restriction).
 */
static text *
get_worker(text *json,
		   char **tpath,
		   int *ipath,
		   int npath,
		   bool normalize_results)
{
	JsonLexContext *lex = makeJsonLexContext(json, true);
	JsonSemAction *sem = palloc0(sizeof(JsonSemAction));
	GetState   *state = palloc0(sizeof(GetState));

	Assert(npath >= 0);

	state->lex = lex;
	/* is it "_as_text" variant? */
	state->normalize_results = normalize_results;
	state->npath = npath;
	state->path_names = tpath;
	state->path_indexes = ipath;
	state->pathok = palloc0(sizeof(bool) * npath);
	state->array_cur_index = palloc(sizeof(int) * npath);

	if (npath > 0)
		state->pathok[0] = true;

	sem->semstate = (void *) state;

	/*
	 * Not all variants need all the semantic routines. Only set the ones that
	 * are actually needed for maximum efficiency.
	 */
	sem->scalar = get_scalar;
	if (npath == 0)
	{
		sem->object_start = get_object_start;
		sem->object_end = get_object_end;
		sem->array_start = get_array_start;
		sem->array_end = get_array_end;
	}
	if (tpath != NULL)
	{
		sem->object_field_start = get_object_field_start;
		sem->object_field_end = get_object_field_end;
	}
	if (ipath != NULL)
	{
		sem->array_start = get_array_start;
		sem->array_element_start = get_array_element_start;
		sem->array_element_end = get_array_element_end;
	}

	pg_parse_json(lex, sem);

	return state->tresult;
}

static void
get_object_start(void *state)
{
	GetState   *_state = (GetState *) state;
	int			lex_level = _state->lex->lex_level;

	if (lex_level == 0 && _state->npath == 0)
	{
		/*
		 * Special case: we should match the entire object.  We only need this
		 * at outermost level because at nested levels the match will have
		 * been started by the outer field or array element callback.
		 */
		_state->result_start = _state->lex->token_start;
	}
}

static void
get_object_end(void *state)
{
	GetState   *_state = (GetState *) state;
	int			lex_level = _state->lex->lex_level;

	if (lex_level == 0 && _state->npath == 0)
	{
		/* Special case: return the entire object */
		char	   *start = _state->result_start;
		int			len = _state->lex->prev_token_terminator - start;

		_state->tresult = cstring_to_text_with_len(start, len);
	}
}

static void
get_object_field_start(void *state, char *fname, bool isnull)
{
	GetState   *_state = (GetState *) state;
	bool		get_next = false;
	int			lex_level = _state->lex->lex_level;

	if (lex_level <= _state->npath &&
		_state->pathok[lex_level - 1] &&
		_state->path_names != NULL &&
		_state->path_names[lex_level - 1] != NULL &&
		strcmp(fname, _state->path_names[lex_level - 1]) == 0)
	{
		if (lex_level < _state->npath)
		{
			/* if not at end of path just mark path ok */
			_state->pathok[lex_level] = true;
		}
		else
		{
			/* end of path, so we want this value */
			get_next = true;
		}
	}

	if (get_next)
	{
		/* this object overrides any previous matching object */
		_state->tresult = NULL;
		_state->result_start = NULL;

		if (_state->normalize_results &&
			_state->lex->token_type == JSON_TOKEN_STRING)
		{
			/* for as_text variants, tell get_scalar to set it for us */
			_state->next_scalar = true;
		}
		else
		{
			/* for non-as_text variants, just note the json starting point */
			_state->result_start = _state->lex->token_start;
		}
	}
}

static void
get_object_field_end(void *state, char *fname, bool isnull)
{
	GetState   *_state = (GetState *) state;
	bool		get_last = false;
	int			lex_level = _state->lex->lex_level;

	/* same tests as in get_object_field_start */
	if (lex_level <= _state->npath &&
		_state->pathok[lex_level - 1] &&
		_state->path_names != NULL &&
		_state->path_names[lex_level - 1] != NULL &&
		strcmp(fname, _state->path_names[lex_level - 1]) == 0)
	{
		if (lex_level < _state->npath)
		{
			/* done with this field so reset pathok */
			_state->pathok[lex_level] = false;
		}
		else
		{
			/* end of path, so we want this value */
			get_last = true;
		}
	}

	/* for as_text scalar case, our work is already done */
	if (get_last && _state->result_start != NULL)
	{
		/*
		 * make a text object from the string from the previously noted json
		 * start up to the end of the previous token (the lexer is by now
		 * ahead of us on whatever came after what we're interested in).
		 */
		if (isnull && _state->normalize_results)
			_state->tresult = (text *) NULL;
		else
		{
			char	   *start = _state->result_start;
			int			len = _state->lex->prev_token_terminator - start;

			_state->tresult = cstring_to_text_with_len(start, len);
		}

		/* this should be unnecessary but let's do it for cleanliness: */
		_state->result_start = NULL;
	}
}

static void
get_array_start(void *state)
{
	GetState   *_state = (GetState *) state;
	int			lex_level = _state->lex->lex_level;

	if (lex_level < _state->npath)
	{
		/* Initialize counting of elements in this array */
		_state->array_cur_index[lex_level] = -1;

		/* INT_MIN value is reserved to represent invalid subscript */
		if (_state->path_indexes[lex_level] < 0 &&
			_state->path_indexes[lex_level] != INT_MIN)
		{
			/* Negative subscript -- convert to positive-wise subscript */
			int			nelements = json_count_array_elements(_state->lex);

			if (-_state->path_indexes[lex_level] <= nelements)
				_state->path_indexes[lex_level] += nelements;
		}
	}
	else if (lex_level == 0 && _state->npath == 0)
	{
		/*
		 * Special case: we should match the entire array.  We only need this
		 * at the outermost level because at nested levels the match will have
		 * been started by the outer field or array element callback.
		 */
		_state->result_start = _state->lex->token_start;
	}
}

static void
get_array_end(void *state)
{
	GetState   *_state = (GetState *) state;
	int			lex_level = _state->lex->lex_level;

	if (lex_level == 0 && _state->npath == 0)
	{
		/* Special case: return the entire array */
		char	   *start = _state->result_start;
		int			len = _state->lex->prev_token_terminator - start;

		_state->tresult = cstring_to_text_with_len(start, len);
	}
}

static void
get_array_element_start(void *state, bool isnull)
{
	GetState   *_state = (GetState *) state;
	bool		get_next = false;
	int			lex_level = _state->lex->lex_level;

	/* Update array element counter */
	if (lex_level <= _state->npath)
		_state->array_cur_index[lex_level - 1]++;

	if (lex_level <= _state->npath &&
		_state->pathok[lex_level - 1] &&
		_state->path_indexes != NULL &&
		_state->array_cur_index[lex_level - 1] == _state->path_indexes[lex_level - 1])
	{
		if (lex_level < _state->npath)
		{
			/* if not at end of path just mark path ok */
			_state->pathok[lex_level] = true;
		}
		else
		{
			/* end of path, so we want this value */
			get_next = true;
		}
	}

	/* same logic as for objects */
	if (get_next)
	{
		_state->tresult = NULL;
		_state->result_start = NULL;

		if (_state->normalize_results &&
			_state->lex->token_type == JSON_TOKEN_STRING)
		{
			_state->next_scalar = true;
		}
		else
		{
			_state->result_start = _state->lex->token_start;
		}
	}
}

static void
get_array_element_end(void *state, bool isnull)
{
	GetState   *_state = (GetState *) state;
	bool		get_last = false;
	int			lex_level = _state->lex->lex_level;

	/* same tests as in get_array_element_start */
	if (lex_level <= _state->npath &&
		_state->pathok[lex_level - 1] &&
		_state->path_indexes != NULL &&
		_state->array_cur_index[lex_level - 1] == _state->path_indexes[lex_level - 1])
	{
		if (lex_level < _state->npath)
		{
			/* done with this element so reset pathok */
			_state->pathok[lex_level] = false;
		}
		else
		{
			/* end of path, so we want this value */
			get_last = true;
		}
	}

	/* same logic as for objects */
	if (get_last && _state->result_start != NULL)
	{
		if (isnull && _state->normalize_results)
			_state->tresult = (text *) NULL;
		else
		{
			char	   *start = _state->result_start;
			int			len = _state->lex->prev_token_terminator - start;

			_state->tresult = cstring_to_text_with_len(start, len);
		}

		_state->result_start = NULL;
	}
}

static void
get_scalar(void *state, char *token, JsonTokenType tokentype)
{
	GetState   *_state = (GetState *) state;
	int			lex_level = _state->lex->lex_level;

	/* Check for whole-object match */
	if (lex_level == 0 && _state->npath == 0)
	{
		if (_state->normalize_results && tokentype == JSON_TOKEN_STRING)
		{
			/* we want the de-escaped string */
			_state->next_scalar = true;
		}
		else if (_state->normalize_results && tokentype == JSON_TOKEN_NULL)
		{
			_state->tresult = (text *) NULL;
		}
		else
		{
			/*
			 * This is a bit hokey: we will suppress whitespace after the
			 * scalar token, but not whitespace before it.  Probably not worth
			 * doing our own space-skipping to avoid that.
			 */
			char	   *start = _state->lex->input;
			int			len = _state->lex->prev_token_terminator - start;

			_state->tresult = cstring_to_text_with_len(start, len);
		}
	}

	if (_state->next_scalar)
	{
		/* a de-escaped text value is wanted, so supply it */
		_state->tresult = cstring_to_text(token);
		/* make sure the next call to get_scalar doesn't overwrite it */
		_state->next_scalar = false;
	}
}

Datum
jsonb_extract_path(PG_FUNCTION_ARGS)
{
	return get_jsonb_path_all(fcinfo, false);
}

Datum
jsonb_extract_path_text(PG_FUNCTION_ARGS)
{
	return get_jsonb_path_all(fcinfo, true);
}

static Datum
get_jsonb_path_all(FunctionCallInfo fcinfo, bool as_text)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
	Datum	   *pathtext;
	bool	   *pathnulls;
	bool		isnull;
	int			npath;
	Datum		res;

	/*
	 * If the array contains any null elements, return NULL, on the grounds
	 * that you'd have gotten NULL if any RHS value were NULL in a nested
	 * series of applications of the -> operator.  (Note: because we also
	 * return NULL for error cases such as no-such-field, this is true
	 * regardless of the contents of the rest of the array.)
	 */
	if (array_contains_nulls(path))
		PG_RETURN_NULL();

	deconstruct_array(path, TEXTOID, -1, false, 'i',
					  &pathtext, &pathnulls, &npath);

	res = jsonb_get_element(jb, pathtext, npath, &isnull, as_text);

	if (isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(res);
}

/* Initialize private jsonb subscripting state. */
static void
jsonb_subscript_init(SubscriptingRefState *sbstate, Datum container, bool isnull)
{
	Jsonb	   *jb = isnull ? NULL : DatumGetJsonbP(container);

	if (sbstate->isassignment)
	{
		JsonbAssignState *astate = palloc0(sizeof(*astate));
		JsonbSubscriptState *subscript = &astate->subscripts[0];

		astate->ps = NULL;

		if (jb)
		{
			JsonbValue	jbv;
			JsonbIteratorToken tok;

			astate->iter = JsonbIteratorInit(&jb->root);

			tok = JsonbIteratorNext(&astate->iter, &jbv, false);

			if (tok == WJB_BEGIN_ARRAY)
			{
				if (jbv.val.array.rawScalar)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("cannot assign to subscript of scalar jsonb")));

				subscript->is_array = true;
				subscript->array_size = jbv.val.array.nElems;
			}
			else
				subscript->is_array = false;

			subscript->exists = true;
		}
		else
		{
			astate->iter = NULL;
			subscript->exists = false;
		}

		sbstate->privatedata = astate;
	}
	else
	{
		JsonbValue *jbv;

		/* Initialize a binary JsonbValue and use it as a private state */
		if (jb)
		{
			jbv = palloc(sizeof(*jbv));

			jbv->type = jbvBinary;
			jbv->val.binary.data = &jb->root;
			jbv->val.binary.len = VARSIZE(jb) - VARHDRSZ;
		}
		else
			jbv = NULL;

		sbstate->privatedata = jbv;
	}
}

/*
 * Select subscript expression variant.
 *
 * There are two expression variants of jsonb subscripts:
 *   0th - unmodified expression
 *   1st - expression casted to int4 (optional, if type is numeric)
 *
 * If the current jsonb is an array then we select 1st variant, otherwise
 * default 0th variant is selected.
 */
static int
jsonb_subscript_selectexpr(SubscriptingRefState *sbstate, int num,
						   Oid subscriptType, Oid *exprTypes, int nExprs)
{
	bool		is_array = false;

	Assert(nExprs == 1);
	Assert(!OidIsValid(exprTypes[0]) || exprTypes[0] == INT4OID);

	if (sbstate->isassignment)
	{
		JsonbAssignState *astate = sbstate->privatedata;
		JsonbSubscriptState *subscript = &astate->subscripts[num];

		if (!subscript->exists)
		{
			/* NULL can be only in assignments, select int4 variant if available. */
			Assert(sbstate->isassignment);

			subscript->is_array = OidIsValid(exprTypes[0]);
			subscript->array_size = 0;
		}

		is_array = subscript->is_array;
	}
	else
	{
		JsonbValue *jbv = sbstate->privatedata;

		Assert(jbv);

		if (jbv->type == jbvBinary)
		{
			JsonbContainer *jbc = jbv->val.binary.data;

			if (JsonContainerIsArray(jbc) && !JsonContainerIsScalar(jbc))
				is_array = true;
		}
	}

	if (is_array && OidIsValid(exprTypes[0]))
		return 1;

	return 0;
}

/* Get the integer index from a subscript datum */
static int32
jsonb_subscript_get_array_index(Datum value, Oid typid, int num,
								int arraySize, bool isAssignment)
{
	long		lindex;

	if (typid == INT4OID)
		lindex = DatumGetInt32(value);
	else if (typid != TEXTOID)
		elog(ERROR, "invalid jsonb subscript type: %u", typid);
	else if (!jsonb_get_array_index_from_text(value, &lindex))
	{
		if (isAssignment)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("jsonb array subscript is not an integer: \"%s\"",
							TextDatumGetCString(value))));
		return -1;
	}

	if (lindex >= 0)
		return lindex;
	else /* handle negative subscript */
		return arraySize + lindex;
}

/* Get the string key from a subscript datum */
static char *
jsonb_subscript_get_object_key(Datum value, Oid typid, int *len)
{
	if (typid == TEXTOID)
	{
		*len = VARSIZE_ANY_EXHDR(value);

		return VARDATA_ANY(value);
	}
	else if (typid == INT4OID)
	{
		char		*key = DatumGetCString(DirectFunctionCall1(int4out, value));

		*len = strlen(key);

		return key;
	}
	else
	{
		elog(ERROR, "invalid jsonb subscript type: %u", typid);
		return NULL;
	}
}

/* Apply single susbscript to jsonb container */
static inline JsonbValue *
jsonb_subscript_apply(JsonbValue *jbv, Datum subscriptVal, Oid subscriptTypid,
					  int subscriptIdx)
{
	JsonbContainer *jbc;

	if (jbv->type != jbvBinary ||
		JsonContainerIsScalar(jbv->val.binary.data))
		return NULL;	/* scalar, extraction yields a null */

	jbc = jbv->val.binary.data;

	if (JsonContainerIsObject(jbc))
	{
		int			keylen;
		char	   *keystr = jsonb_subscript_get_object_key(subscriptVal,
															subscriptTypid,
															&keylen);

		return getKeyJsonValueFromContainer(jbc, keystr, keylen, jbv);
	}
	else if (JsonContainerIsArray(jbc) && !JsonContainerIsScalar(jbc))
	{
		int32		index = jsonb_subscript_get_array_index(subscriptVal,
															subscriptTypid,
															subscriptIdx,
															JsonContainerSize(jbc),
															false);

		if (index < 0)
			return NULL;

		return getIthJsonbValueFromContainer(jbc, index, jbv);
	}
	else
	{
		/* scalar, extraction yields a null */
		return NULL;
	}
}

static void
push_null_elements(JsonbParseState **ps, int num)
{
	JsonbValue	null;

	null.type = jbvNull;

	while (num-- > 0)
		pushJsonbValue(ps, WJB_ELEM, &null);
}

/* Perfrom one subscript assignment step */
static void
jsonb_subscript_step_assignment(SubscriptingRefState *sbstate, Datum value,
								Oid typid, int num, bool isupper)
{
	JsonbAssignState *astate = sbstate->privatedata;
	JsonbSubscriptState *subscript = &astate->subscripts[num];
	JsonbIteratorToken tok;
	JsonbValue	jbv;
	bool		last = num >= sbstate->numupper - 1;

	if (!subscript->exists)
	{
		/* Select the type of newly created container. */
		if (typid == INT4OID)
		{
			subscript->is_array = true;
			subscript->array_size = 0;
		}
		else if (typid == TEXTOID)
			subscript->is_array = false;
		else
			elog(ERROR, "invalid jsonb subscript type: %u", typid);
	}

	subscript[1].exists = false;

	if (subscript->is_array)
	{
		int32		i = 0;
		int32		index = jsonb_subscript_get_array_index(value, typid, num,
															subscript->array_size,
															true);

		pushJsonbValue(&astate->ps, WJB_BEGIN_ARRAY, NULL);

		subscript->array_index = index;

		if (index >= 0 && subscript->exists)
		{
			/* Try to copy preceding elements */
			for (; i < index; i++)
			{
				tok = JsonbIteratorNext(&astate->iter, &jbv, true);

				if (tok != WJB_ELEM)
					break;

				pushJsonbValue(&astate->ps, tok, &jbv);
			}

			/* Try to read replaced element */
			if (i >= index &&
				JsonbIteratorNext(&astate->iter, &jbv, last) != WJB_END_ARRAY)
				subscript[1].exists = true;
		}

		/* Fill the gap before the new element with nulls */
		if (i < index)
			push_null_elements(&astate->ps, index - i);
	}
	else
	{
		JsonbValue	key;

		key.type = jbvString;
		key.val.string.val = jsonb_subscript_get_object_key(value, typid,
															&key.val.string.len);

		pushJsonbValue(&astate->ps, WJB_BEGIN_OBJECT, NULL);

		if (subscript->exists &&
			copyJsonbObject(&astate->ps, &astate->iter, &key))
		{
			subscript[1].exists = true;		/* key is found */
			tok = JsonbIteratorNext(&astate->iter, &jbv, last);
		}

		pushJsonbValue(&astate->ps, WJB_KEY, &key);
	}

	/* If the value does exits, process and validate its type. */
	if (subscript[1].exists)
	{
		if (jbv.type == jbvArray)
		{
			subscript[1].is_array = true;
			subscript[1].array_size = jbv.val.array.nElems;
		}
		else
			subscript[1].is_array = false;

		if (!last && IsAJsonbScalar(&jbv))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot assign to subscript of scalar jsonb")));
	}
}

/* Perform one subscripting step */
static bool
jsonb_subscript_step(SubscriptingRefState *sbstate, int num, bool isupper)
{
	Datum		value;
	Oid			typid;

	if (isupper)
	{
		value = sbstate->upperindex[num];
		typid = sbstate->uppertypid[num];
	}
	else
		elog(ERROR, "jsonb subscript cannot be lower");

	if (sbstate->isassignment)
	{
		jsonb_subscript_step_assignment(sbstate, value, typid, num, isupper);

		return true;	/* always process next subscripts */
	}
	else
	{
		/*
		 * Perform one subscripting step by applying subscript value to current
		 * jsonb container and saving the result into private state.
		 */
		JsonbValue *jbv = sbstate->privatedata;

		Assert(jbv); 	/* NULL can only be in assignments */

		jbv = jsonb_subscript_apply(jbv, value, typid, num);

		sbstate->privatedata = jbv;

		/* Process next subscripts only if the result is not NULL */
		return jbv != NULL;
	}
}

static Datum
jsonb_get_element(Jsonb *jb, Datum *path, int npath, bool *isnull, bool as_text)
{
	JsonbValue		jbv;
	JsonbValue	   *jbvp = NULL;
	JsonbContainer *container = &jb->root;
	int				i;
	bool			have_object = false,
					have_array = false;

	*isnull = false;

	/* Identify whether we have object, array, or scalar at top-level */
	if (JsonContainerIsObject(container))
		have_object = true;
	else if (JsonContainerIsArray(container) && !JsonContainerIsScalar(container))
		have_array = true;
	else
	{
		Assert(JsonContainerIsArray(container) && JsonContainerIsScalar(container));
		/* Extract the scalar value, if it is what we'll return */
		if (npath <= 0)
			jbvp = getIthJsonbValueFromContainer(container, 0, &jbv);
	}

	/*
	 * If the array is empty, return the entire LHS object, on the grounds
	 * that we should do zero field or element extractions.  For the
	 * non-scalar case we can just hand back the object without much work. For
	 * the scalar case, fall through and deal with the value below the loop.
	 * (This inconsistency arises because there's no easy way to generate a
	 * JsonbValue directly for root-level containers.)
	 */
	if (npath <= 0 && jbvp == NULL)
	{
		if (as_text)
		{
			return PointerGetDatum(cstring_to_text(JsonbToCString(NULL,
															container,
															VARSIZE(jb))));
		}
		else
		{
			/* not text mode - just hand back the jsonb */
			PG_RETURN_JSONB_P(jb);
		}
	}

	for (i = 0; i < npath; i++)
	{
		if (have_object)
		{
			jbvp = getKeyJsonValueFromContainer(container,
												VARDATA(path[i]),
												VARSIZE(path[i]) - VARHDRSZ,
												jbvp);
		}
		else if (have_array)
		{
			long		lindex;
			uint32		index;

			if (!jsonb_get_array_index_from_text(path[i], &lindex))
			{
				*isnull = true;
				return PointerGetDatum(NULL);
			}

			if (lindex >= 0)
			{
				index = (uint32) lindex;
			}
			else
			{
				/* Handle negative subscript */
				uint32		nelements;

				/* Container must be array, but make sure */
				if (!JsonContainerIsArray(container))
					elog(ERROR, "not a jsonb array");

				nelements = JsonContainerSize(container);

				if (-lindex > nelements)
				{
					*isnull = true;
					return PointerGetDatum(NULL);
				}
				else
					index = nelements + lindex;
			}

			jbvp = getIthJsonbValueFromContainer(container, index, jbvp);
		}
		else
		{
			/* scalar, extraction yields a null */
			*isnull = true;
			return PointerGetDatum(NULL);
		}

		if (jbvp == NULL)
		{
			*isnull = true;
			return PointerGetDatum(NULL);
		}
		else if (i == npath - 1)
			break;

		if (jbvp->type == jbvBinary)
		{
			container = jbvp->val.binary.data;
			have_object = JsonContainerIsObject(container);
			have_array = JsonContainerIsArray(container);
			Assert(!JsonContainerIsScalar(container));
		}
		else
		{
			Assert(IsAJsonbScalar(jbvp));
			have_object = false;
			have_array = false;
		}
	}

	if (as_text)
	{
		if (jbvp->type == jbvNull)
		{
			*isnull = true;
			return PointerGetDatum(NULL);
		}

		return PointerGetDatum(JsonbValueAsText(jbvp));
	}
	else
	{
		Jsonb	   *res = JsonbValueToJsonb(jbvp);

		/* not text mode - just hand back the jsonb */
		PG_RETURN_JSONB_P(res);
	}
}

/*
 * Return the text representation of the given JsonbValue.
 */
static text *
JsonbValueAsText(JsonbValue *v)
{
	switch (v->type)
	{
		case jbvNull:
			return NULL;

		case jbvBool:
			return v->val.boolean ?
				cstring_to_text_with_len("true", 4) :
				cstring_to_text_with_len("false", 5);

		case jbvString:
			return cstring_to_text_with_len(v->val.string.val,
											v->val.string.len);

		case jbvNumeric:
			{
				Datum		cstr;

				cstr = DirectFunctionCall1(numeric_out,
										   PointerGetDatum(v->val.numeric));

				return cstring_to_text(DatumGetCString(cstr));
			}

		case jbvBinary:
			{
				StringInfoData jtext;

				initStringInfo(&jtext);
				(void) JsonbToCString(&jtext, v->val.binary.data,
									  v->val.binary.len);

				return cstring_to_text_with_len(jtext.data, jtext.len);
			}

		default:
			elog(ERROR, "unrecognized jsonb type: %d", (int) v->type);
			return NULL;
	}
}

/*
 * SQL function json_array_length(json) -> int
 */
Datum
json_array_length(PG_FUNCTION_ARGS)
{
	text	   *json = PG_GETARG_TEXT_PP(0);
	AlenState  *state;
	JsonLexContext *lex;
	JsonSemAction *sem;

	lex = makeJsonLexContext(json, false);
	state = palloc0(sizeof(AlenState));
	sem = palloc0(sizeof(JsonSemAction));

	/* palloc0 does this for us */
#if 0
	state->count = 0;
#endif
	state->lex = lex;

	sem->semstate = (void *) state;
	sem->object_start = alen_object_start;
	sem->scalar = alen_scalar;
	sem->array_element_start = alen_array_element_start;

	pg_parse_json(lex, sem);

	PG_RETURN_INT32(state->count);
}

Datum
jsonb_array_length(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);

	if (JB_ROOT_IS_SCALAR(jb))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot get array length of a scalar")));
	else if (!JB_ROOT_IS_ARRAY(jb))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot get array length of a non-array")));

	PG_RETURN_INT32(JB_ROOT_COUNT(jb));
}

/*
 * These next two checks ensure that the json is an array (since it can't be
 * a scalar or an object).
 */

static void
alen_object_start(void *state)
{
	AlenState  *_state = (AlenState *) state;

	/* json structure check */
	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot get array length of a non-array")));
}

static void
alen_scalar(void *state, char *token, JsonTokenType tokentype)
{
	AlenState  *_state = (AlenState *) state;

	/* json structure check */
	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot get array length of a scalar")));
}

static void
alen_array_element_start(void *state, bool isnull)
{
	AlenState  *_state = (AlenState *) state;

	/* just count up all the level 1 elements */
	if (_state->lex->lex_level == 1)
		_state->count++;
}

/*
 * SQL function json_each and json_each_text
 *
 * decompose a json object into key value pairs.
 *
 * Unlike json_object_keys() these SRFs operate in materialize mode,
 * stashing results into a Tuplestore object as they go.
 * The construction of tuples is done using a temporary memory context
 * that is cleared out after each tuple is built.
 */
Datum
json_each(PG_FUNCTION_ARGS)
{
	return each_worker(fcinfo, false);
}

Datum
jsonb_each(PG_FUNCTION_ARGS)
{
	return each_worker_jsonb(fcinfo, "jsonb_each", false);
}

Datum
json_each_text(PG_FUNCTION_ARGS)
{
	return each_worker(fcinfo, true);
}

Datum
jsonb_each_text(PG_FUNCTION_ARGS)
{
	return each_worker_jsonb(fcinfo, "jsonb_each_text", true);
}

static Datum
each_worker_jsonb(FunctionCallInfo fcinfo, const char *funcname, bool as_text)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	ReturnSetInfo *rsi;
	Tuplestorestate *tuple_store;
	TupleDesc	tupdesc;
	TupleDesc	ret_tdesc;
	MemoryContext old_cxt,
				tmp_cxt;
	bool		skipNested = false;
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken r;

	if (!JB_ROOT_IS_OBJECT(jb))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot call %s on a non-object",
						funcname)));

	rsi = (ReturnSetInfo *) fcinfo->resultinfo;

	if (!rsi || !IsA(rsi, ReturnSetInfo) ||
		(rsi->allowedModes & SFRM_Materialize) == 0 ||
		rsi->expectedDesc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that "
						"cannot accept a set")));

	rsi->returnMode = SFRM_Materialize;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);

	ret_tdesc = CreateTupleDescCopy(tupdesc);
	BlessTupleDesc(ret_tdesc);
	tuple_store =
		tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random,
							  false, work_mem);

	MemoryContextSwitchTo(old_cxt);

	tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
									"jsonb_each temporary cxt",
									ALLOCSET_DEFAULT_SIZES);

	it = JsonbIteratorInit(&jb->root);

	while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
	{
		skipNested = true;

		if (r == WJB_KEY)
		{
			text	   *key;
			HeapTuple	tuple;
			Datum		values[2];
			bool		nulls[2] = {false, false};

			/* Use the tmp context so we can clean up after each tuple is done */
			old_cxt = MemoryContextSwitchTo(tmp_cxt);

			key = cstring_to_text_with_len(v.val.string.val, v.val.string.len);

			/*
			 * The next thing the iterator fetches should be the value, no
			 * matter what shape it is.
			 */
			r = JsonbIteratorNext(&it, &v, skipNested);
			Assert(r != WJB_DONE);

			values[0] = PointerGetDatum(key);

			if (as_text)
			{
				if (v.type == jbvNull)
				{
					/* a json null is an sql null in text mode */
					nulls[1] = true;
					values[1] = (Datum) NULL;
				}
				else
					values[1] = PointerGetDatum(JsonbValueAsText(&v));
			}
			else
			{
				/* Not in text mode, just return the Jsonb */
				Jsonb	   *val = JsonbValueToJsonb(&v);

				values[1] = PointerGetDatum(val);
			}

			tuple = heap_form_tuple(ret_tdesc, values, nulls);

			tuplestore_puttuple(tuple_store, tuple);

			/* clean up and switch back */
			MemoryContextSwitchTo(old_cxt);
			MemoryContextReset(tmp_cxt);
		}
	}

	MemoryContextDelete(tmp_cxt);

	rsi->setResult = tuple_store;
	rsi->setDesc = ret_tdesc;

	PG_RETURN_NULL();
}


static Datum
each_worker(FunctionCallInfo fcinfo, bool as_text)
{
	text	   *json = PG_GETARG_TEXT_PP(0);
	JsonLexContext *lex;
	JsonSemAction *sem;
	ReturnSetInfo *rsi;
	MemoryContext old_cxt;
	TupleDesc	tupdesc;
	EachState  *state;

	lex = makeJsonLexContext(json, true);
	state = palloc0(sizeof(EachState));
	sem = palloc0(sizeof(JsonSemAction));

	rsi = (ReturnSetInfo *) fcinfo->resultinfo;

	if (!rsi || !IsA(rsi, ReturnSetInfo) ||
		(rsi->allowedModes & SFRM_Materialize) == 0 ||
		rsi->expectedDesc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that "
						"cannot accept a set")));

	rsi->returnMode = SFRM_Materialize;

	(void) get_call_result_type(fcinfo, NULL, &tupdesc);

	/* make these in a sufficiently long-lived memory context */
	old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);

	state->ret_tdesc = CreateTupleDescCopy(tupdesc);
	BlessTupleDesc(state->ret_tdesc);
	state->tuple_store =
		tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random,
							  false, work_mem);

	MemoryContextSwitchTo(old_cxt);

	sem->semstate = (void *) state;
	sem->array_start = each_array_start;
	sem->scalar = each_scalar;
	sem->object_field_start = each_object_field_start;
	sem->object_field_end = each_object_field_end;

	state->normalize_results = as_text;
	state->next_scalar = false;
	state->lex = lex;
	state->tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
										   "json_each temporary cxt",
										   ALLOCSET_DEFAULT_SIZES);

	pg_parse_json(lex, sem);

	MemoryContextDelete(state->tmp_cxt);

	rsi->setResult = state->tuple_store;
	rsi->setDesc = state->ret_tdesc;

	PG_RETURN_NULL();
}


static void
each_object_field_start(void *state, char *fname, bool isnull)
{
	EachState  *_state = (EachState *) state;

	/* save a pointer to where the value starts */
	if (_state->lex->lex_level == 1)
	{
		/*
		 * next_scalar will be reset in the object_field_end handler, and
		 * since we know the value is a scalar there is no danger of it being
		 * on while recursing down the tree.
		 */
		if (_state->normalize_results && _state->lex->token_type == JSON_TOKEN_STRING)
			_state->next_scalar = true;
		else
			_state->result_start = _state->lex->token_start;
	}
}

static void
each_object_field_end(void *state, char *fname, bool isnull)
{
	EachState  *_state = (EachState *) state;
	MemoryContext old_cxt;
	int			len;
	text	   *val;
	HeapTuple	tuple;
	Datum		values[2];
	bool		nulls[2] = {false, false};

	/* skip over nested objects */
	if (_state->lex->lex_level != 1)
		return;

	/* use the tmp context so we can clean up after each tuple is done */
	old_cxt = MemoryContextSwitchTo(_state->tmp_cxt);

	values[0] = CStringGetTextDatum(fname);

	if (isnull && _state->normalize_results)
	{
		nulls[1] = true;
		values[1] = (Datum) 0;
	}
	else if (_state->next_scalar)
	{
		values[1] = CStringGetTextDatum(_state->normalized_scalar);
		_state->next_scalar = false;
	}
	else
	{
		len = _state->lex->prev_token_terminator - _state->result_start;
		val = cstring_to_text_with_len(_state->result_start, len);
		values[1] = PointerGetDatum(val);
	}

	tuple = heap_form_tuple(_state->ret_tdesc, values, nulls);

	tuplestore_puttuple(_state->tuple_store, tuple);

	/* clean up and switch back */
	MemoryContextSwitchTo(old_cxt);
	MemoryContextReset(_state->tmp_cxt);
}

static void
each_array_start(void *state)
{
	EachState  *_state = (EachState *) state;

	/* json structure check */
	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot deconstruct an array as an object")));
}

static void
each_scalar(void *state, char *token, JsonTokenType tokentype)
{
	EachState  *_state = (EachState *) state;

	/* json structure check */
	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot deconstruct a scalar")));

	/* supply de-escaped value if required */
	if (_state->next_scalar)
		_state->normalized_scalar = token;
}

/*
 * SQL functions json_array_elements and json_array_elements_text
 *
 * get the elements from a json array
 *
 * a lot of this processing is similar to the json_each* functions
 */

Datum
jsonb_array_elements(PG_FUNCTION_ARGS)
{
	return elements_worker_jsonb(fcinfo, "jsonb_array_elements", false);
}

Datum
jsonb_array_elements_text(PG_FUNCTION_ARGS)
{
	return elements_worker_jsonb(fcinfo, "jsonb_array_elements_text", true);
}

static Datum
elements_worker_jsonb(FunctionCallInfo fcinfo, const char *funcname,
					  bool as_text)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	ReturnSetInfo *rsi;
	Tuplestorestate *tuple_store;
	TupleDesc	tupdesc;
	TupleDesc	ret_tdesc;
	MemoryContext old_cxt,
				tmp_cxt;
	bool		skipNested = false;
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken r;

	if (JB_ROOT_IS_SCALAR(jb))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot extract elements from a scalar")));
	else if (!JB_ROOT_IS_ARRAY(jb))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot extract elements from an object")));

	rsi = (ReturnSetInfo *) fcinfo->resultinfo;

	if (!rsi || !IsA(rsi, ReturnSetInfo) ||
		(rsi->allowedModes & SFRM_Materialize) == 0 ||
		rsi->expectedDesc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that "
						"cannot accept a set")));

	rsi->returnMode = SFRM_Materialize;

	/* it's a simple type, so don't use get_call_result_type() */
	tupdesc = rsi->expectedDesc;

	old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);

	ret_tdesc = CreateTupleDescCopy(tupdesc);
	BlessTupleDesc(ret_tdesc);
	tuple_store =
		tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random,
							  false, work_mem);

	MemoryContextSwitchTo(old_cxt);

	tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
									"jsonb_array_elements temporary cxt",
									ALLOCSET_DEFAULT_SIZES);

	it = JsonbIteratorInit(&jb->root);

	while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
	{
		skipNested = true;

		if (r == WJB_ELEM)
		{
			HeapTuple	tuple;
			Datum		values[1];
			bool		nulls[1] = {false};

			/* use the tmp context so we can clean up after each tuple is done */
			old_cxt = MemoryContextSwitchTo(tmp_cxt);

			if (as_text)
			{
				if (v.type == jbvNull)
				{
					/* a json null is an sql null in text mode */
					nulls[0] = true;
					values[0] = (Datum) NULL;
				}
				else
					values[0] = PointerGetDatum(JsonbValueAsText(&v));
			}
			else
			{
				/* Not in text mode, just return the Jsonb */
				Jsonb	   *val = JsonbValueToJsonb(&v);

				values[0] = PointerGetDatum(val);
			}

			tuple = heap_form_tuple(ret_tdesc, values, nulls);

			tuplestore_puttuple(tuple_store, tuple);

			/* clean up and switch back */
			MemoryContextSwitchTo(old_cxt);
			MemoryContextReset(tmp_cxt);
		}
	}

	MemoryContextDelete(tmp_cxt);

	rsi->setResult = tuple_store;
	rsi->setDesc = ret_tdesc;

	PG_RETURN_NULL();
}

Datum
json_array_elements(PG_FUNCTION_ARGS)
{
	return elements_worker(fcinfo, "json_array_elements", false);
}

Datum
json_array_elements_text(PG_FUNCTION_ARGS)
{
	return elements_worker(fcinfo, "json_array_elements_text", true);
}

static Datum
elements_worker(FunctionCallInfo fcinfo, const char *funcname, bool as_text)
{
	text	   *json = PG_GETARG_TEXT_PP(0);

	/* elements only needs escaped strings when as_text */
	JsonLexContext *lex = makeJsonLexContext(json, as_text);
	JsonSemAction *sem;
	ReturnSetInfo *rsi;
	MemoryContext old_cxt;
	TupleDesc	tupdesc;
	ElementsState *state;

	state = palloc0(sizeof(ElementsState));
	sem = palloc0(sizeof(JsonSemAction));

	rsi = (ReturnSetInfo *) fcinfo->resultinfo;

	if (!rsi || !IsA(rsi, ReturnSetInfo) ||
		(rsi->allowedModes & SFRM_Materialize) == 0 ||
		rsi->expectedDesc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that "
						"cannot accept a set")));

	rsi->returnMode = SFRM_Materialize;

	/* it's a simple type, so don't use get_call_result_type() */
	tupdesc = rsi->expectedDesc;

	/* make these in a sufficiently long-lived memory context */
	old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);

	state->ret_tdesc = CreateTupleDescCopy(tupdesc);
	BlessTupleDesc(state->ret_tdesc);
	state->tuple_store =
		tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random,
							  false, work_mem);

	MemoryContextSwitchTo(old_cxt);

	sem->semstate = (void *) state;
	sem->object_start = elements_object_start;
	sem->scalar = elements_scalar;
	sem->array_element_start = elements_array_element_start;
	sem->array_element_end = elements_array_element_end;

	state->function_name = funcname;
	state->normalize_results = as_text;
	state->next_scalar = false;
	state->lex = lex;
	state->tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
										   "json_array_elements temporary cxt",
										   ALLOCSET_DEFAULT_SIZES);

	pg_parse_json(lex, sem);

	MemoryContextDelete(state->tmp_cxt);

	rsi->setResult = state->tuple_store;
	rsi->setDesc = state->ret_tdesc;

	PG_RETURN_NULL();
}

static void
elements_array_element_start(void *state, bool isnull)
{
	ElementsState *_state = (ElementsState *) state;

	/* save a pointer to where the value starts */
	if (_state->lex->lex_level == 1)
	{
		/*
		 * next_scalar will be reset in the array_element_end handler, and
		 * since we know the value is a scalar there is no danger of it being
		 * on while recursing down the tree.
		 */
		if (_state->normalize_results && _state->lex->token_type == JSON_TOKEN_STRING)
			_state->next_scalar = true;
		else
			_state->result_start = _state->lex->token_start;
	}
}

static void
elements_array_element_end(void *state, bool isnull)
{
	ElementsState *_state = (ElementsState *) state;
	MemoryContext old_cxt;
	int			len;
	text	   *val;
	HeapTuple	tuple;
	Datum		values[1];
	bool		nulls[1] = {false};

	/* skip over nested objects */
	if (_state->lex->lex_level != 1)
		return;

	/* use the tmp context so we can clean up after each tuple is done */
	old_cxt = MemoryContextSwitchTo(_state->tmp_cxt);

	if (isnull && _state->normalize_results)
	{
		nulls[0] = true;
		values[0] = (Datum) NULL;
	}
	else if (_state->next_scalar)
	{
		values[0] = CStringGetTextDatum(_state->normalized_scalar);
		_state->next_scalar = false;
	}
	else
	{
		len = _state->lex->prev_token_terminator - _state->result_start;
		val = cstring_to_text_with_len(_state->result_start, len);
		values[0] = PointerGetDatum(val);
	}

	tuple = heap_form_tuple(_state->ret_tdesc, values, nulls);

	tuplestore_puttuple(_state->tuple_store, tuple);

	/* clean up and switch back */
	MemoryContextSwitchTo(old_cxt);
	MemoryContextReset(_state->tmp_cxt);
}

static void
elements_object_start(void *state)
{
	ElementsState *_state = (ElementsState *) state;

	/* json structure check */
	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot call %s on a non-array",
						_state->function_name)));
}

static void
elements_scalar(void *state, char *token, JsonTokenType tokentype)
{
	ElementsState *_state = (ElementsState *) state;

	/* json structure check */
	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot call %s on a scalar",
						_state->function_name)));

	/* supply de-escaped value if required */
	if (_state->next_scalar)
		_state->normalized_scalar = token;
}

/*
 * SQL function json_populate_record
 *
 * set fields in a record from the argument json
 *
 * Code adapted shamelessly from hstore's populate_record
 * which is in turn partly adapted from record_out.
 *
 * The json is decomposed into a hash table, in which each
 * field in the record is then looked up by name. For jsonb
 * we fetch the values direct from the object.
 */
Datum
jsonb_populate_record(PG_FUNCTION_ARGS)
{
	return populate_record_worker(fcinfo, "jsonb_populate_record",
								  false, true);
}

Datum
jsonb_to_record(PG_FUNCTION_ARGS)
{
	return populate_record_worker(fcinfo, "jsonb_to_record",
								  false, false);
}

Datum
json_populate_record(PG_FUNCTION_ARGS)
{
	return populate_record_worker(fcinfo, "json_populate_record",
								  true, true);
}

Datum
json_to_record(PG_FUNCTION_ARGS)
{
	return populate_record_worker(fcinfo, "json_to_record",
								  true, false);
}

/* helper function for diagnostics */
static void
populate_array_report_expected_array(PopulateArrayContext *ctx, int ndim)
{
	if (ndim <= 0)
	{
		if (ctx->colname)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("expected JSON array"),
					 errhint("See the value of key \"%s\".", ctx->colname)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("expected JSON array")));
	}
	else
	{
		StringInfoData indices;
		int			i;

		initStringInfo(&indices);

		Assert(ctx->ndims > 0 && ndim < ctx->ndims);

		for (i = 0; i < ndim; i++)
			appendStringInfo(&indices, "[%d]", ctx->sizes[i]);

		if (ctx->colname)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("expected JSON array"),
					 errhint("See the array element %s of key \"%s\".",
							 indices.data, ctx->colname)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("expected JSON array"),
					 errhint("See the array element %s.",
							 indices.data)));
	}
}

/* set the number of dimensions of the populated array when it becomes known */
static void
populate_array_assign_ndims(PopulateArrayContext *ctx, int ndims)
{
	int			i;

	Assert(ctx->ndims <= 0);

	if (ndims <= 0)
		populate_array_report_expected_array(ctx, ndims);

	ctx->ndims = ndims;
	ctx->dims = palloc(sizeof(int) * ndims);
	ctx->sizes = palloc0(sizeof(int) * ndims);

	for (i = 0; i < ndims; i++)
		ctx->dims[i] = -1;		/* dimensions are unknown yet */
}

/* check the populated subarray dimension */
static void
populate_array_check_dimension(PopulateArrayContext *ctx, int ndim)
{
	int			dim = ctx->sizes[ndim]; /* current dimension counter */

	if (ctx->dims[ndim] == -1)
		ctx->dims[ndim] = dim;	/* assign dimension if not yet known */
	else if (ctx->dims[ndim] != dim)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("malformed JSON array"),
				 errdetail("Multidimensional arrays must have "
						   "sub-arrays with matching dimensions.")));

	/* reset the current array dimension size counter */
	ctx->sizes[ndim] = 0;

	/* increment the parent dimension counter if it is a nested sub-array */
	if (ndim > 0)
		ctx->sizes[ndim - 1]++;
}

static void
populate_array_element(PopulateArrayContext *ctx, int ndim, JsValue *jsv)
{
	Datum		element;
	bool		element_isnull;

	/* populate the array element */
	element = populate_record_field(ctx->aio->element_info,
									ctx->aio->element_type,
									ctx->aio->element_typmod,
									NULL, ctx->mcxt, PointerGetDatum(NULL),
									jsv, &element_isnull);

	accumArrayResult(ctx->astate, element, element_isnull,
					 ctx->aio->element_type, ctx->acxt);

	Assert(ndim > 0);
	ctx->sizes[ndim - 1]++;		/* increment current dimension counter */
}

/* json object start handler for populate_array_json() */
static void
populate_array_object_start(void *_state)
{
	PopulateArrayState *state = (PopulateArrayState *) _state;
	int			ndim = state->lex->lex_level;

	if (state->ctx->ndims <= 0)
		populate_array_assign_ndims(state->ctx, ndim);
	else if (ndim < state->ctx->ndims)
		populate_array_report_expected_array(state->ctx, ndim);
}

/* json array end handler for populate_array_json() */
static void
populate_array_array_end(void *_state)
{
	PopulateArrayState *state = (PopulateArrayState *) _state;
	PopulateArrayContext *ctx = state->ctx;
	int			ndim = state->lex->lex_level;

	if (ctx->ndims <= 0)
		populate_array_assign_ndims(ctx, ndim + 1);

	if (ndim < ctx->ndims)
		populate_array_check_dimension(ctx, ndim);
}

/* json array element start handler for populate_array_json() */
static void
populate_array_element_start(void *_state, bool isnull)
{
	PopulateArrayState *state = (PopulateArrayState *) _state;
	int			ndim = state->lex->lex_level;

	if (state->ctx->ndims <= 0 || ndim == state->ctx->ndims)
	{
		/* remember current array element start */
		state->element_start = state->lex->token_start;
		state->element_type = state->lex->token_type;
		state->element_scalar = NULL;
	}
}

/* json array element end handler for populate_array_json() */
static void
populate_array_element_end(void *_state, bool isnull)
{
	PopulateArrayState *state = (PopulateArrayState *) _state;
	PopulateArrayContext *ctx = state->ctx;
	int			ndim = state->lex->lex_level;

	Assert(ctx->ndims > 0);

	if (ndim == ctx->ndims)
	{
		JsValue		jsv;

		jsv.is_json = true;
		jsv.val.json.type = state->element_type;

		if (isnull)
		{
			Assert(jsv.val.json.type == JSON_TOKEN_NULL);
			jsv.val.json.str = NULL;
			jsv.val.json.len = 0;
		}
		else if (state->element_scalar)
		{
			jsv.val.json.str = state->element_scalar;
			jsv.val.json.len = -1;	/* null-terminated */
		}
		else
		{
			jsv.val.json.str = state->element_start;
			jsv.val.json.len = (state->lex->prev_token_terminator -
								state->element_start) * sizeof(char);
		}

		populate_array_element(ctx, ndim, &jsv);
	}
}

/* json scalar handler for populate_array_json() */
static void
populate_array_scalar(void *_state, char *token, JsonTokenType tokentype)
{
	PopulateArrayState *state = (PopulateArrayState *) _state;
	PopulateArrayContext *ctx = state->ctx;
	int			ndim = state->lex->lex_level;

	if (ctx->ndims <= 0)
		populate_array_assign_ndims(ctx, ndim);
	else if (ndim < ctx->ndims)
		populate_array_report_expected_array(ctx, ndim);

	if (ndim == ctx->ndims)
	{
		/* remember the scalar element token */
		state->element_scalar = token;
		/* element_type must already be set in populate_array_element_start() */
		Assert(state->element_type == tokentype);
	}
}

/* parse a json array and populate array */
static void
populate_array_json(PopulateArrayContext *ctx, char *json, int len)
{
	PopulateArrayState state;
	JsonSemAction sem;

	state.lex = makeJsonLexContextCstringLen(json, len, true);
	state.ctx = ctx;

	memset(&sem, 0, sizeof(sem));
	sem.semstate = (void *) &state;
	sem.object_start = populate_array_object_start;
	sem.array_end = populate_array_array_end;
	sem.array_element_start = populate_array_element_start;
	sem.array_element_end = populate_array_element_end;
	sem.scalar = populate_array_scalar;

	pg_parse_json(state.lex, &sem);

	/* number of dimensions should be already known */
	Assert(ctx->ndims > 0 && ctx->dims);

	pfree(state.lex);
}

/*
 * populate_array_dim_jsonb() -- Iterate recursively through jsonb sub-array
 *		elements and accumulate result using given ArrayBuildState.
 */
static void
populate_array_dim_jsonb(PopulateArrayContext *ctx, /* context */
						 JsonbValue *jbv,	/* jsonb sub-array */
						 int ndim)	/* current dimension */
{
	JsonbContainer *jbc = jbv->val.binary.data;
	JsonbIterator *it;
	JsonbIteratorToken tok;
	JsonbValue	val;
	JsValue		jsv;

	check_stack_depth();

	if (jbv->type != jbvBinary || !JsonContainerIsArray(jbc))
		populate_array_report_expected_array(ctx, ndim - 1);

	Assert(!JsonContainerIsScalar(jbc));

	it = JsonbIteratorInit(jbc);

	tok = JsonbIteratorNext(&it, &val, true);
	Assert(tok == WJB_BEGIN_ARRAY);

	tok = JsonbIteratorNext(&it, &val, true);

	/*
	 * If the number of dimensions is not yet known and we have found end of
	 * the array, or the first child element is not an array, then assign the
	 * number of dimensions now.
	 */
	if (ctx->ndims <= 0 &&
		(tok == WJB_END_ARRAY ||
		 (tok == WJB_ELEM &&
		  (val.type != jbvBinary ||
		   !JsonContainerIsArray(val.val.binary.data)))))
		populate_array_assign_ndims(ctx, ndim);

	jsv.is_json = false;
	jsv.val.jsonb = &val;

	/* process all the array elements */
	while (tok == WJB_ELEM)
	{
		/*
		 * Recurse only if the dimensions of dimensions is still unknown or if
		 * it is not the innermost dimension.
		 */
		if (ctx->ndims > 0 && ndim >= ctx->ndims)
			populate_array_element(ctx, ndim, &jsv);
		else
		{
			/* populate child sub-array */
			populate_array_dim_jsonb(ctx, &val, ndim + 1);

			/* number of dimensions should be already known */
			Assert(ctx->ndims > 0 && ctx->dims);

			populate_array_check_dimension(ctx, ndim);
		}

		tok = JsonbIteratorNext(&it, &val, true);
	}

	Assert(tok == WJB_END_ARRAY);

	/* free iterator, iterating until WJB_DONE */
	tok = JsonbIteratorNext(&it, &val, true);
	Assert(tok == WJB_DONE && !it);
}

/* recursively populate an array from json/jsonb */
static Datum
populate_array(ArrayIOData *aio,
			   const char *colname,
			   MemoryContext mcxt,
			   JsValue *jsv)
{
	PopulateArrayContext ctx;
	Datum		result;
	int		   *lbs;
	int			i;

	ctx.aio = aio;
	ctx.mcxt = mcxt;
	ctx.acxt = CurrentMemoryContext;
	ctx.astate = initArrayResult(aio->element_type, ctx.acxt, true);
	ctx.colname = colname;
	ctx.ndims = 0;				/* unknown yet */
	ctx.dims = NULL;
	ctx.sizes = NULL;

	if (jsv->is_json)
		populate_array_json(&ctx, jsv->val.json.str,
							jsv->val.json.len >= 0 ? jsv->val.json.len
							: strlen(jsv->val.json.str));
	else
	{
		populate_array_dim_jsonb(&ctx, jsv->val.jsonb, 1);
		ctx.dims[0] = ctx.sizes[0];
	}

	Assert(ctx.ndims > 0);

	lbs = palloc(sizeof(int) * ctx.ndims);

	for (i = 0; i < ctx.ndims; i++)
		lbs[i] = 1;

	result = makeMdArrayResult(ctx.astate, ctx.ndims, ctx.dims, lbs,
							   ctx.acxt, true);

	pfree(ctx.dims);
	pfree(ctx.sizes);
	pfree(lbs);

	return result;
}

static void
JsValueToJsObject(JsValue *jsv, JsObject *jso)
{
	jso->is_json = jsv->is_json;

	if (jsv->is_json)
	{
		/* convert plain-text json into a hash table */
		jso->val.json_hash =
			get_json_object_as_hash(jsv->val.json.str,
									jsv->val.json.len >= 0
									? jsv->val.json.len
									: strlen(jsv->val.json.str),
									"populate_composite");
	}
	else
	{
		JsonbValue *jbv = jsv->val.jsonb;

		if (jbv->type == jbvBinary &&
			JsonContainerIsObject(jbv->val.binary.data))
		{
			jso->val.jsonb_cont = jbv->val.binary.data;
		}
		else
		{
			bool		is_scalar;

			is_scalar = IsAJsonbScalar(jbv) ||
				(jbv->type == jbvBinary &&
				 JsonContainerIsScalar(jbv->val.binary.data));
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 is_scalar
					 ? errmsg("cannot call %s on a scalar",
							  "populate_composite")
					 : errmsg("cannot call %s on an array",
							  "populate_composite")));
		}
	}
}

/* acquire or update cached tuple descriptor for a composite type */
static void
update_cached_tupdesc(CompositeIOData *io, MemoryContext mcxt)
{
	if (!io->tupdesc ||
		io->tupdesc->tdtypeid != io->base_typid ||
		io->tupdesc->tdtypmod != io->base_typmod)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(io->base_typid,
													 io->base_typmod);
		MemoryContext oldcxt;

		if (io->tupdesc)
			FreeTupleDesc(io->tupdesc);

		/* copy tuple desc without constraints into cache memory context */
		oldcxt = MemoryContextSwitchTo(mcxt);
		io->tupdesc = CreateTupleDescCopy(tupdesc);
		MemoryContextSwitchTo(oldcxt);

		ReleaseTupleDesc(tupdesc);
	}
}

/* recursively populate a composite (row type) value from json/jsonb */
static Datum
populate_composite(CompositeIOData *io,
				   Oid typid,
				   const char *colname,
				   MemoryContext mcxt,
				   HeapTupleHeader defaultval,
				   JsValue *jsv,
				   bool isnull)
{
	Datum		result;

	/* acquire/update cached tuple descriptor */
	update_cached_tupdesc(io, mcxt);

	if (isnull)
		result = (Datum) 0;
	else
	{
		HeapTupleHeader tuple;
		JsObject	jso;

		/* prepare input value */
		JsValueToJsObject(jsv, &jso);

		/* populate resulting record tuple */
		tuple = populate_record(io->tupdesc, &io->record_io,
								defaultval, mcxt, &jso);
		result = HeapTupleHeaderGetDatum(tuple);

		JsObjectFree(&jso);
	}

	/*
	 * If it's domain over composite, check domain constraints.  (This should
	 * probably get refactored so that we can see the TYPECAT value, but for
	 * now, we can tell by comparing typid to base_typid.)
	 */
	if (typid != io->base_typid && typid != RECORDOID)
		domain_check(result, isnull, typid, &io->domain_info, mcxt);

	return result;
}

/* populate non-null scalar value from json/jsonb value */
static Datum
populate_scalar(ScalarIOData *io, Oid typid, int32 typmod, JsValue *jsv)
{
	Datum		res;
	char	   *str = NULL;
	char	   *json = NULL;

	if (jsv->is_json)
	{
		int			len = jsv->val.json.len;

		json = jsv->val.json.str;
		Assert(json);
		if (len >= 0)
		{
			/* Need to copy non-null-terminated string */
			str = palloc(len + 1 * sizeof(char));
			memcpy(str, json, len);
			str[len] = '\0';
		}
		else
			str = json;			/* string is already null-terminated */

		/* If converting to json/jsonb, make string into valid JSON literal */
		if ((typid == JSONOID || typid == JSONBOID) &&
			jsv->val.json.type == JSON_TOKEN_STRING)
		{
			StringInfoData buf;

			initStringInfo(&buf);
			escape_json(&buf, str);
			/* free temporary buffer */
			if (str != json)
				pfree(str);
			str = buf.data;
		}
	}
	else
	{
		JsonbValue *jbv = jsv->val.jsonb;

		if (typid == JSONBOID)
		{
			Jsonb	   *jsonb = JsonbValueToJsonb(jbv); /* directly use jsonb */

			return JsonbPGetDatum(jsonb);
		}
		/* convert jsonb to string for typio call */
		else if (typid == JSONOID && jbv->type != jbvBinary)
		{
			/*
			 * Convert scalar jsonb (non-scalars are passed here as jbvBinary)
			 * to json string, preserving quotes around top-level strings.
			 */
			Jsonb	   *jsonb = JsonbValueToJsonb(jbv);

			str = JsonbToCString(NULL, &jsonb->root, VARSIZE(jsonb));
		}
		else if (jbv->type == jbvString)	/* quotes are stripped */
			str = pnstrdup(jbv->val.string.val, jbv->val.string.len);
		else if (jbv->type == jbvBool)
			str = pstrdup(jbv->val.boolean ? "true" : "false");
		else if (jbv->type == jbvNumeric)
			str = DatumGetCString(DirectFunctionCall1(numeric_out,
													  PointerGetDatum(jbv->val.numeric)));
		else if (jbv->type == jbvBinary)
			str = JsonbToCString(NULL, jbv->val.binary.data,
								 jbv->val.binary.len);
		else
			elog(ERROR, "unrecognized jsonb type: %d", (int) jbv->type);
	}

	res = InputFunctionCall(&io->typiofunc, str, io->typioparam, typmod);

	/* free temporary buffer */
	if (str != json)
		pfree(str);

	return res;
}

static Datum
populate_domain(DomainIOData *io,
				Oid typid,
				const char *colname,
				MemoryContext mcxt,
				JsValue *jsv,
				bool isnull)
{
	Datum		res;

	if (isnull)
		res = (Datum) 0;
	else
	{
		res = populate_record_field(io->base_io,
									io->base_typid, io->base_typmod,
									colname, mcxt, PointerGetDatum(NULL),
									jsv, &isnull);
		Assert(!isnull);
	}

	domain_check(res, isnull, typid, &io->domain_info, mcxt);

	return res;
}

/* prepare column metadata cache for the given type */
static void
prepare_column_cache(ColumnIOData *column,
					 Oid typid,
					 int32 typmod,
					 MemoryContext mcxt,
					 bool need_scalar)
{
	HeapTuple	tup;
	Form_pg_type type;

	column->typid = typid;
	column->typmod = typmod;

	tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for type %u", typid);

	type = (Form_pg_type) GETSTRUCT(tup);

	if (type->typtype == TYPTYPE_DOMAIN)
	{
		/*
		 * We can move directly to the bottom base type; domain_check() will
		 * take care of checking all constraints for a stack of domains.
		 */
		Oid			base_typid;
		int32		base_typmod = typmod;

		base_typid = getBaseTypeAndTypmod(typid, &base_typmod);
		if (get_typtype(base_typid) == TYPTYPE_COMPOSITE)
		{
			/* domain over composite has its own code path */
			column->typcat = TYPECAT_COMPOSITE_DOMAIN;
			column->io.composite.record_io = NULL;
			column->io.composite.tupdesc = NULL;
			column->io.composite.base_typid = base_typid;
			column->io.composite.base_typmod = base_typmod;
			column->io.composite.domain_info = NULL;
		}
		else
		{
			/* domain over anything else */
			column->typcat = TYPECAT_DOMAIN;
			column->io.domain.base_typid = base_typid;
			column->io.domain.base_typmod = base_typmod;
			column->io.domain.base_io =
				MemoryContextAllocZero(mcxt, sizeof(ColumnIOData));
			column->io.domain.domain_info = NULL;
		}
	}
	else if (type->typtype == TYPTYPE_COMPOSITE || typid == RECORDOID)
	{
		column->typcat = TYPECAT_COMPOSITE;
		column->io.composite.record_io = NULL;
		column->io.composite.tupdesc = NULL;
		column->io.composite.base_typid = typid;
		column->io.composite.base_typmod = typmod;
		column->io.composite.domain_info = NULL;
	}
	else if (type->typlen == -1 && OidIsValid(type->typelem))
	{
		column->typcat = TYPECAT_ARRAY;
		column->io.array.element_info = MemoryContextAllocZero(mcxt,
															   sizeof(ColumnIOData));
		column->io.array.element_type = type->typelem;
		/* array element typemod stored in attribute's typmod */
		column->io.array.element_typmod = typmod;
	}
	else
	{
		column->typcat = TYPECAT_SCALAR;
		need_scalar = true;
	}

	/* caller can force us to look up scalar_io info even for non-scalars */
	if (need_scalar)
	{
		Oid			typioproc;

		getTypeInputInfo(typid, &typioproc, &column->scalar_io.typioparam);
		fmgr_info_cxt(typioproc, &column->scalar_io.typiofunc, mcxt);
	}

	ReleaseSysCache(tup);
}

/* recursively populate a record field or an array element from a json/jsonb value */
static Datum
populate_record_field(ColumnIOData *col,
					  Oid typid,
					  int32 typmod,
					  const char *colname,
					  MemoryContext mcxt,
					  Datum defaultval,
					  JsValue *jsv,
					  bool *isnull)
{
	TypeCat		typcat;

	check_stack_depth();

	/*
	 * Prepare column metadata cache for the given type.  Force lookup of the
	 * scalar_io data so that the json string hack below will work.
	 */
	if (col->typid != typid || col->typmod != typmod)
		prepare_column_cache(col, typid, typmod, mcxt, true);

	*isnull = JsValueIsNull(jsv);

	typcat = col->typcat;

	/* try to convert json string to a non-scalar type through input function */
	if (JsValueIsString(jsv) &&
		(typcat == TYPECAT_ARRAY ||
		 typcat == TYPECAT_COMPOSITE ||
		 typcat == TYPECAT_COMPOSITE_DOMAIN))
		typcat = TYPECAT_SCALAR;

	/* we must perform domain checks for NULLs, otherwise exit immediately */
	if (*isnull &&
		typcat != TYPECAT_DOMAIN &&
		typcat != TYPECAT_COMPOSITE_DOMAIN)
		return (Datum) 0;

	switch (typcat)
	{
		case TYPECAT_SCALAR:
			return populate_scalar(&col->scalar_io, typid, typmod, jsv);

		case TYPECAT_ARRAY:
			return populate_array(&col->io.array, colname, mcxt, jsv);

		case TYPECAT_COMPOSITE:
		case TYPECAT_COMPOSITE_DOMAIN:
			return populate_composite(&col->io.composite, typid,
									  colname, mcxt,
									  DatumGetPointer(defaultval)
									  ? DatumGetHeapTupleHeader(defaultval)
									  : NULL,
									  jsv, *isnull);

		case TYPECAT_DOMAIN:
			return populate_domain(&col->io.domain, typid, colname, mcxt,
								   jsv, *isnull);

		default:
			elog(ERROR, "unrecognized type category '%c'", typcat);
			return (Datum) 0;
	}
}

static RecordIOData *
allocate_record_info(MemoryContext mcxt, int ncolumns)
{
	RecordIOData *data = (RecordIOData *)
	MemoryContextAlloc(mcxt,
					   offsetof(RecordIOData, columns) +
					   ncolumns * sizeof(ColumnIOData));

	data->record_type = InvalidOid;
	data->record_typmod = 0;
	data->ncolumns = ncolumns;
	MemSet(data->columns, 0, sizeof(ColumnIOData) * ncolumns);

	return data;
}

static bool
JsObjectGetField(JsObject *obj, char *field, JsValue *jsv)
{
	jsv->is_json = obj->is_json;

	if (jsv->is_json)
	{
		JsonHashEntry *hashentry = hash_search(obj->val.json_hash, field,
											   HASH_FIND, NULL);

		jsv->val.json.type = hashentry ? hashentry->type : JSON_TOKEN_NULL;
		jsv->val.json.str = jsv->val.json.type == JSON_TOKEN_NULL ? NULL :
			hashentry->val;
		jsv->val.json.len = jsv->val.json.str ? -1 : 0; /* null-terminated */

		return hashentry != NULL;
	}
	else
	{
		jsv->val.jsonb = !obj->val.jsonb_cont ? NULL :
			getKeyJsonValueFromContainer(obj->val.jsonb_cont, field, strlen(field),
										 NULL);

		return jsv->val.jsonb != NULL;
	}
}

/* populate a record tuple from json/jsonb value */
static HeapTupleHeader
populate_record(TupleDesc tupdesc,
				RecordIOData **record_p,
				HeapTupleHeader defaultval,
				MemoryContext mcxt,
				JsObject *obj)
{
	RecordIOData *record = *record_p;
	Datum	   *values;
	bool	   *nulls;
	HeapTuple	res;
	int			ncolumns = tupdesc->natts;
	int			i;

	/*
	 * if the input json is empty, we can only skip the rest if we were passed
	 * in a non-null record, since otherwise there may be issues with domain
	 * nulls.
	 */
	if (defaultval && JsObjectIsEmpty(obj))
		return defaultval;

	/* (re)allocate metadata cache */
	if (record == NULL ||
		record->ncolumns != ncolumns)
		*record_p = record = allocate_record_info(mcxt, ncolumns);

	/* invalidate metadata cache if the record type has changed */
	if (record->record_type != tupdesc->tdtypeid ||
		record->record_typmod != tupdesc->tdtypmod)
	{
		MemSet(record, 0, offsetof(RecordIOData, columns) +
			   ncolumns * sizeof(ColumnIOData));
		record->record_type = tupdesc->tdtypeid;
		record->record_typmod = tupdesc->tdtypmod;
		record->ncolumns = ncolumns;
	}

	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));

	if (defaultval)
	{
		HeapTupleData tuple;

		/* Build a temporary HeapTuple control structure */
		tuple.t_len = HeapTupleHeaderGetDatumLength(defaultval);
		ItemPointerSetInvalid(&(tuple.t_self));
		tuple.t_tableOid = InvalidOid;
		tuple.t_data = defaultval;

		/* Break down the tuple into fields */
		heap_deform_tuple(&tuple, tupdesc, values, nulls);
	}
	else
	{
		for (i = 0; i < ncolumns; ++i)
		{
			values[i] = (Datum) 0;
			nulls[i] = true;
		}
	}

	for (i = 0; i < ncolumns; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		char	   *colname = NameStr(att->attname);
		JsValue		field = {0};
		bool		found;

		/* Ignore dropped columns in datatype */
		if (att->attisdropped)
		{
			nulls[i] = true;
			continue;
		}

		found = JsObjectGetField(obj, colname, &field);

		/*
		 * we can't just skip here if the key wasn't found since we might have
		 * a domain to deal with. If we were passed in a non-null record
		 * datum, we assume that the existing values are valid (if they're
		 * not, then it's not our fault), but if we were passed in a null,
		 * then every field which we don't populate needs to be run through
		 * the input function just in case it's a domain type.
		 */
		if (defaultval && !found)
			continue;

		values[i] = populate_record_field(&record->columns[i],
										  att->atttypid,
										  att->atttypmod,
										  colname,
										  mcxt,
										  nulls[i] ? (Datum) 0 : values[i],
										  &field,
										  &nulls[i]);
	}

	res = heap_form_tuple(tupdesc, values, nulls);

	pfree(values);
	pfree(nulls);

	return res->t_data;
}

/*
 * Setup for json{b}_populate_record{set}: result type will be same as first
 * argument's type --- unless first argument is "null::record", which we can't
 * extract type info from; we handle that later.
 */
static void
get_record_type_from_argument(FunctionCallInfo fcinfo,
							  const char *funcname,
							  PopulateRecordCache *cache)
{
	cache->argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
	prepare_column_cache(&cache->c,
						 cache->argtype, -1,
						 cache->fn_mcxt, false);
	if (cache->c.typcat != TYPECAT_COMPOSITE &&
		cache->c.typcat != TYPECAT_COMPOSITE_DOMAIN)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
		/* translator: %s is a function name, eg json_to_record */
				 errmsg("first argument of %s must be a row type",
						funcname)));
}

/*
 * Setup for json{b}_to_record{set}: result type is specified by calling
 * query.  We'll also use this code for json{b}_populate_record{set},
 * if we discover that the first argument is a null of type RECORD.
 *
 * Here it is syntactically impossible to specify the target type
 * as domain-over-composite.
 */
static void
get_record_type_from_query(FunctionCallInfo fcinfo,
						   const char *funcname,
						   PopulateRecordCache *cache)
{
	TupleDesc	tupdesc;
	MemoryContext old_cxt;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/* translator: %s is a function name, eg json_to_record */
				 errmsg("could not determine row type for result of %s",
						funcname),
				 errhint("Provide a non-null record argument, "
						 "or call the function in the FROM clause "
						 "using a column definition list.")));

	Assert(tupdesc);
	cache->argtype = tupdesc->tdtypeid;

	/* If we go through this more than once, avoid memory leak */
	if (cache->c.io.composite.tupdesc)
		FreeTupleDesc(cache->c.io.composite.tupdesc);

	/* Save identified tupdesc */
	old_cxt = MemoryContextSwitchTo(cache->fn_mcxt);
	cache->c.io.composite.tupdesc = CreateTupleDescCopy(tupdesc);
	cache->c.io.composite.base_typid = tupdesc->tdtypeid;
	cache->c.io.composite.base_typmod = tupdesc->tdtypmod;
	MemoryContextSwitchTo(old_cxt);
}

/*
 * common worker for json{b}_populate_record() and json{b}_to_record()
 * is_json and have_record_arg identify the specific function
 */
static Datum
populate_record_worker(FunctionCallInfo fcinfo, const char *funcname,
					   bool is_json, bool have_record_arg)
{
	int			json_arg_num = have_record_arg ? 1 : 0;
	JsValue		jsv = {0};
	HeapTupleHeader rec;
	Datum		rettuple;
	JsonbValue	jbv;
	MemoryContext fnmcxt = fcinfo->flinfo->fn_mcxt;
	PopulateRecordCache *cache = fcinfo->flinfo->fn_extra;

	/*
	 * If first time through, identify input/result record type.  Note that
	 * this stanza looks only at fcinfo context, which can't change during the
	 * query; so we may not be able to fully resolve a RECORD input type yet.
	 */
	if (!cache)
	{
		fcinfo->flinfo->fn_extra = cache =
			MemoryContextAllocZero(fnmcxt, sizeof(*cache));
		cache->fn_mcxt = fnmcxt;

		if (have_record_arg)
			get_record_type_from_argument(fcinfo, funcname, cache);
		else
			get_record_type_from_query(fcinfo, funcname, cache);
	}

	/* Collect record arg if we have one */
	if (!have_record_arg)
		rec = NULL;				/* it's json{b}_to_record() */
	else if (!PG_ARGISNULL(0))
	{
		rec = PG_GETARG_HEAPTUPLEHEADER(0);

		/*
		 * When declared arg type is RECORD, identify actual record type from
		 * the tuple itself.
		 */
		if (cache->argtype == RECORDOID)
		{
			cache->c.io.composite.base_typid = HeapTupleHeaderGetTypeId(rec);
			cache->c.io.composite.base_typmod = HeapTupleHeaderGetTypMod(rec);
		}
	}
	else
	{
		rec = NULL;

		/*
		 * When declared arg type is RECORD, identify actual record type from
		 * calling query, or fail if we can't.
		 */
		if (cache->argtype == RECORDOID)
		{
			get_record_type_from_query(fcinfo, funcname, cache);
			/* This can't change argtype, which is important for next time */
			Assert(cache->argtype == RECORDOID);
		}
	}

	/* If no JSON argument, just return the record (if any) unchanged */
	if (PG_ARGISNULL(json_arg_num))
	{
		if (rec)
			PG_RETURN_POINTER(rec);
		else
			PG_RETURN_NULL();
	}

	jsv.is_json = is_json;

	if (is_json)
	{
		text	   *json = PG_GETARG_TEXT_PP(json_arg_num);

		jsv.val.json.str = VARDATA_ANY(json);
		jsv.val.json.len = VARSIZE_ANY_EXHDR(json);
		jsv.val.json.type = JSON_TOKEN_INVALID; /* not used in
												 * populate_composite() */
	}
	else
	{
		Jsonb	   *jb = PG_GETARG_JSONB_P(json_arg_num);

		jsv.val.jsonb = &jbv;

		/* fill binary jsonb value pointing to jb */
		jbv.type = jbvBinary;
		jbv.val.binary.data = &jb->root;
		jbv.val.binary.len = VARSIZE(jb) - VARHDRSZ;
	}

	rettuple = populate_composite(&cache->c.io.composite, cache->argtype,
								  NULL, fnmcxt, rec, &jsv, false);

	PG_RETURN_DATUM(rettuple);
}

/*
 * get_json_object_as_hash
 *
 * decompose a json object into a hash table.
 */
static HTAB *
get_json_object_as_hash(char *json, int len, const char *funcname)
{
	HASHCTL		ctl;
	HTAB	   *tab;
	JHashState *state;
	JsonLexContext *lex = makeJsonLexContextCstringLen(json, len, true);
	JsonSemAction *sem;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(JsonHashEntry);
	ctl.hcxt = CurrentMemoryContext;
	tab = hash_create("json object hashtable",
					  100,
					  &ctl,
					  HASH_ELEM | HASH_CONTEXT);

	state = palloc0(sizeof(JHashState));
	sem = palloc0(sizeof(JsonSemAction));

	state->function_name = funcname;
	state->hash = tab;
	state->lex = lex;

	sem->semstate = (void *) state;
	sem->array_start = hash_array_start;
	sem->scalar = hash_scalar;
	sem->object_field_start = hash_object_field_start;
	sem->object_field_end = hash_object_field_end;

	pg_parse_json(lex, sem);

	return tab;
}

static void
hash_object_field_start(void *state, char *fname, bool isnull)
{
	JHashState *_state = (JHashState *) state;

	if (_state->lex->lex_level > 1)
		return;

	/* remember token type */
	_state->saved_token_type = _state->lex->token_type;

	if (_state->lex->token_type == JSON_TOKEN_ARRAY_START ||
		_state->lex->token_type == JSON_TOKEN_OBJECT_START)
	{
		/* remember start position of the whole text of the subobject */
		_state->save_json_start = _state->lex->token_start;
	}
	else
	{
		/* must be a scalar */
		_state->save_json_start = NULL;
	}
}

static void
hash_object_field_end(void *state, char *fname, bool isnull)
{
	JHashState *_state = (JHashState *) state;
	JsonHashEntry *hashentry;
	bool		found;

	/*
	 * Ignore nested fields.
	 */
	if (_state->lex->lex_level > 1)
		return;

	/*
	 * Ignore field names >= NAMEDATALEN - they can't match a record field.
	 * (Note: without this test, the hash code would truncate the string at
	 * NAMEDATALEN-1, and could then match against a similarly-truncated
	 * record field name.  That would be a reasonable behavior, but this code
	 * has previously insisted on exact equality, so we keep this behavior.)
	 */
	if (strlen(fname) >= NAMEDATALEN)
		return;

	hashentry = hash_search(_state->hash, fname, HASH_ENTER, &found);

	/*
	 * found being true indicates a duplicate. We don't do anything about
	 * that, a later field with the same name overrides the earlier field.
	 */

	hashentry->type = _state->saved_token_type;
	Assert(isnull == (hashentry->type == JSON_TOKEN_NULL));

	if (_state->save_json_start != NULL)
	{
		int			len = _state->lex->prev_token_terminator - _state->save_json_start;
		char	   *val = palloc((len + 1) * sizeof(char));

		memcpy(val, _state->save_json_start, len);
		val[len] = '\0';
		hashentry->val = val;
	}
	else
	{
		/* must have had a scalar instead */
		hashentry->val = _state->saved_scalar;
	}
}

static void
hash_array_start(void *state)
{
	JHashState *_state = (JHashState *) state;

	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot call %s on an array", _state->function_name)));
}

static void
hash_scalar(void *state, char *token, JsonTokenType tokentype)
{
	JHashState *_state = (JHashState *) state;

	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot call %s on a scalar", _state->function_name)));

	if (_state->lex->lex_level == 1)
	{
		_state->saved_scalar = token;
		/* saved_token_type must already be set in hash_object_field_start() */
		Assert(_state->saved_token_type == tokentype);
	}
}


/*
 * SQL function json_populate_recordset
 *
 * set fields in a set of records from the argument json,
 * which must be an array of objects.
 *
 * similar to json_populate_record, but the tuple-building code
 * is pushed down into the semantic action handlers so it's done
 * per object in the array.
 */
Datum
jsonb_populate_recordset(PG_FUNCTION_ARGS)
{
	return populate_recordset_worker(fcinfo, "jsonb_populate_recordset",
									 false, true);
}

Datum
jsonb_to_recordset(PG_FUNCTION_ARGS)
{
	return populate_recordset_worker(fcinfo, "jsonb_to_recordset",
									 false, false);
}

Datum
json_populate_recordset(PG_FUNCTION_ARGS)
{
	return populate_recordset_worker(fcinfo, "json_populate_recordset",
									 true, true);
}

Datum
json_to_recordset(PG_FUNCTION_ARGS)
{
	return populate_recordset_worker(fcinfo, "json_to_recordset",
									 true, false);
}

static void
populate_recordset_record(PopulateRecordsetState *state, JsObject *obj)
{
	PopulateRecordCache *cache = state->cache;
	HeapTupleHeader tuphead;
	HeapTupleData tuple;

	/* acquire/update cached tuple descriptor */
	update_cached_tupdesc(&cache->c.io.composite, cache->fn_mcxt);

	/* replace record fields from json */
	tuphead = populate_record(cache->c.io.composite.tupdesc,
							  &cache->c.io.composite.record_io,
							  state->rec,
							  cache->fn_mcxt,
							  obj);

	/* if it's domain over composite, check domain constraints */
	if (cache->c.typcat == TYPECAT_COMPOSITE_DOMAIN)
		domain_check(HeapTupleHeaderGetDatum(tuphead), false,
					 cache->argtype,
					 &cache->c.io.composite.domain_info,
					 cache->fn_mcxt);

	/* ok, save into tuplestore */
	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphead);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphead;

	tuplestore_puttuple(state->tuple_store, &tuple);
}

/*
 * common worker for json{b}_populate_recordset() and json{b}_to_recordset()
 * is_json and have_record_arg identify the specific function
 */
static Datum
populate_recordset_worker(FunctionCallInfo fcinfo, const char *funcname,
						  bool is_json, bool have_record_arg)
{
	int			json_arg_num = have_record_arg ? 1 : 0;
	ReturnSetInfo *rsi;
	MemoryContext old_cxt;
	HeapTupleHeader rec;
	PopulateRecordCache *cache = fcinfo->flinfo->fn_extra;
	PopulateRecordsetState *state;

	rsi = (ReturnSetInfo *) fcinfo->resultinfo;

	if (!rsi || !IsA(rsi, ReturnSetInfo) ||
		(rsi->allowedModes & SFRM_Materialize) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that "
						"cannot accept a set")));

	rsi->returnMode = SFRM_Materialize;

	/*
	 * If first time through, identify input/result record type.  Note that
	 * this stanza looks only at fcinfo context, which can't change during the
	 * query; so we may not be able to fully resolve a RECORD input type yet.
	 */
	if (!cache)
	{
		fcinfo->flinfo->fn_extra = cache =
			MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(*cache));
		cache->fn_mcxt = fcinfo->flinfo->fn_mcxt;

		if (have_record_arg)
			get_record_type_from_argument(fcinfo, funcname, cache);
		else
			get_record_type_from_query(fcinfo, funcname, cache);
	}

	/* Collect record arg if we have one */
	if (!have_record_arg)
		rec = NULL;				/* it's json{b}_to_recordset() */
	else if (!PG_ARGISNULL(0))
	{
		rec = PG_GETARG_HEAPTUPLEHEADER(0);

		/*
		 * When declared arg type is RECORD, identify actual record type from
		 * the tuple itself.
		 */
		if (cache->argtype == RECORDOID)
		{
			cache->c.io.composite.base_typid = HeapTupleHeaderGetTypeId(rec);
			cache->c.io.composite.base_typmod = HeapTupleHeaderGetTypMod(rec);
		}
	}
	else
	{
		rec = NULL;

		/*
		 * When declared arg type is RECORD, identify actual record type from
		 * calling query, or fail if we can't.
		 */
		if (cache->argtype == RECORDOID)
		{
			get_record_type_from_query(fcinfo, funcname, cache);
			/* This can't change argtype, which is important for next time */
			Assert(cache->argtype == RECORDOID);
		}
	}

	/* if the json is null send back an empty set */
	if (PG_ARGISNULL(json_arg_num))
		PG_RETURN_NULL();

	/*
	 * Forcibly update the cached tupdesc, to ensure we have the right tupdesc
	 * to return even if the JSON contains no rows.
	 */
	update_cached_tupdesc(&cache->c.io.composite, cache->fn_mcxt);

	state = palloc0(sizeof(PopulateRecordsetState));

	/* make tuplestore in a sufficiently long-lived memory context */
	old_cxt = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);
	state->tuple_store = tuplestore_begin_heap(rsi->allowedModes &
											   SFRM_Materialize_Random,
											   false, work_mem);
	MemoryContextSwitchTo(old_cxt);

	state->function_name = funcname;
	state->cache = cache;
	state->rec = rec;

	if (is_json)
	{
		text	   *json = PG_GETARG_TEXT_PP(json_arg_num);
		JsonLexContext *lex;
		JsonSemAction *sem;

		sem = palloc0(sizeof(JsonSemAction));

		lex = makeJsonLexContext(json, true);

		sem->semstate = (void *) state;
		sem->array_start = populate_recordset_array_start;
		sem->array_element_start = populate_recordset_array_element_start;
		sem->scalar = populate_recordset_scalar;
		sem->object_field_start = populate_recordset_object_field_start;
		sem->object_field_end = populate_recordset_object_field_end;
		sem->object_start = populate_recordset_object_start;
		sem->object_end = populate_recordset_object_end;

		state->lex = lex;

		pg_parse_json(lex, sem);
	}
	else
	{
		Jsonb	   *jb = PG_GETARG_JSONB_P(json_arg_num);
		JsonbIterator *it;
		JsonbValue	v;
		bool		skipNested = false;
		JsonbIteratorToken r;

		if (JB_ROOT_IS_SCALAR(jb) || !JB_ROOT_IS_ARRAY(jb))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot call %s on a non-array",
							funcname)));

		it = JsonbIteratorInit(&jb->root);

		while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
		{
			skipNested = true;

			if (r == WJB_ELEM)
			{
				JsObject	obj;

				if (v.type != jbvBinary ||
					!JsonContainerIsObject(v.val.binary.data))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("argument of %s must be an array of objects",
									funcname)));

				obj.is_json = false;
				obj.val.jsonb_cont = v.val.binary.data;

				populate_recordset_record(state, &obj);
			}
		}
	}

	/*
	 * Note: we must copy the cached tupdesc because the executor will free
	 * the passed-back setDesc, but we want to hang onto the cache in case
	 * we're called again in the same query.
	 */
	rsi->setResult = state->tuple_store;
	rsi->setDesc = CreateTupleDescCopy(cache->c.io.composite.tupdesc);

	PG_RETURN_NULL();
}

static void
populate_recordset_object_start(void *state)
{
	PopulateRecordsetState *_state = (PopulateRecordsetState *) state;
	int			lex_level = _state->lex->lex_level;
	HASHCTL		ctl;

	/* Reject object at top level: we must have an array at level 0 */
	if (lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot call %s on an object",
						_state->function_name)));

	/* Nested objects require no special processing */
	if (lex_level > 1)
		return;

	/* Object at level 1: set up a new hash table for this object */
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(JsonHashEntry);
	ctl.hcxt = CurrentMemoryContext;
	_state->json_hash = hash_create("json object hashtable",
									100,
									&ctl,
									HASH_ELEM | HASH_CONTEXT);
}

static void
populate_recordset_object_end(void *state)
{
	PopulateRecordsetState *_state = (PopulateRecordsetState *) state;
	JsObject	obj;

	/* Nested objects require no special processing */
	if (_state->lex->lex_level > 1)
		return;

	obj.is_json = true;
	obj.val.json_hash = _state->json_hash;

	/* Otherwise, construct and return a tuple based on this level-1 object */
	populate_recordset_record(_state, &obj);

	/* Done with hash for this object */
	hash_destroy(_state->json_hash);
	_state->json_hash = NULL;
}

static void
populate_recordset_array_element_start(void *state, bool isnull)
{
	PopulateRecordsetState *_state = (PopulateRecordsetState *) state;

	if (_state->lex->lex_level == 1 &&
		_state->lex->token_type != JSON_TOKEN_OBJECT_START)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("argument of %s must be an array of objects",
						_state->function_name)));
}

static void
populate_recordset_array_start(void *state)
{
	/* nothing to do */
}

static void
populate_recordset_scalar(void *state, char *token, JsonTokenType tokentype)
{
	PopulateRecordsetState *_state = (PopulateRecordsetState *) state;

	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot call %s on a scalar",
						_state->function_name)));

	if (_state->lex->lex_level == 2)
		_state->saved_scalar = token;
}

static void
populate_recordset_object_field_start(void *state, char *fname, bool isnull)
{
	PopulateRecordsetState *_state = (PopulateRecordsetState *) state;

	if (_state->lex->lex_level > 2)
		return;

	_state->saved_token_type = _state->lex->token_type;

	if (_state->lex->token_type == JSON_TOKEN_ARRAY_START ||
		_state->lex->token_type == JSON_TOKEN_OBJECT_START)
	{
		_state->save_json_start = _state->lex->token_start;
	}
	else
	{
		_state->save_json_start = NULL;
	}
}

static void
populate_recordset_object_field_end(void *state, char *fname, bool isnull)
{
	PopulateRecordsetState *_state = (PopulateRecordsetState *) state;
	JsonHashEntry *hashentry;
	bool		found;

	/*
	 * Ignore nested fields.
	 */
	if (_state->lex->lex_level > 2)
		return;

	/*
	 * Ignore field names >= NAMEDATALEN - they can't match a record field.
	 * (Note: without this test, the hash code would truncate the string at
	 * NAMEDATALEN-1, and could then match against a similarly-truncated
	 * record field name.  That would be a reasonable behavior, but this code
	 * has previously insisted on exact equality, so we keep this behavior.)
	 */
	if (strlen(fname) >= NAMEDATALEN)
		return;

	hashentry = hash_search(_state->json_hash, fname, HASH_ENTER, &found);

	/*
	 * found being true indicates a duplicate. We don't do anything about
	 * that, a later field with the same name overrides the earlier field.
	 */

	hashentry->type = _state->saved_token_type;
	Assert(isnull == (hashentry->type == JSON_TOKEN_NULL));

	if (_state->save_json_start != NULL)
	{
		int			len = _state->lex->prev_token_terminator - _state->save_json_start;
		char	   *val = palloc((len + 1) * sizeof(char));

		memcpy(val, _state->save_json_start, len);
		val[len] = '\0';
		hashentry->val = val;
	}
	else
	{
		/* must have had a scalar instead */
		hashentry->val = _state->saved_scalar;
	}
}

/*
 * Semantic actions for json_strip_nulls.
 *
 * Simply repeat the input on the output unless we encounter
 * a null object field. State for this is set when the field
 * is started and reset when the scalar action (which must be next)
 * is called.
 */

static void
sn_object_start(void *state)
{
	StripnullState *_state = (StripnullState *) state;

	appendStringInfoCharMacro(_state->strval, '{');
}

static void
sn_object_end(void *state)
{
	StripnullState *_state = (StripnullState *) state;

	appendStringInfoCharMacro(_state->strval, '}');
}

static void
sn_array_start(void *state)
{
	StripnullState *_state = (StripnullState *) state;

	appendStringInfoCharMacro(_state->strval, '[');
}

static void
sn_array_end(void *state)
{
	StripnullState *_state = (StripnullState *) state;

	appendStringInfoCharMacro(_state->strval, ']');
}

static void
sn_object_field_start(void *state, char *fname, bool isnull)
{
	StripnullState *_state = (StripnullState *) state;

	if (isnull)
	{
		/*
		 * The next thing must be a scalar or isnull couldn't be true, so
		 * there is no danger of this state being carried down into a nested
		 * object or array. The flag will be reset in the scalar action.
		 */
		_state->skip_next_null = true;
		return;
	}

	if (_state->strval->data[_state->strval->len - 1] != '{')
		appendStringInfoCharMacro(_state->strval, ',');

	/*
	 * Unfortunately we don't have the quoted and escaped string any more, so
	 * we have to re-escape it.
	 */
	escape_json(_state->strval, fname);

	appendStringInfoCharMacro(_state->strval, ':');
}

static void
sn_array_element_start(void *state, bool isnull)
{
	StripnullState *_state = (StripnullState *) state;

	if (_state->strval->data[_state->strval->len - 1] != '[')
		appendStringInfoCharMacro(_state->strval, ',');
}

static void
sn_scalar(void *state, char *token, JsonTokenType tokentype)
{
	StripnullState *_state = (StripnullState *) state;

	if (_state->skip_next_null)
	{
		Assert(tokentype == JSON_TOKEN_NULL);
		_state->skip_next_null = false;
		return;
	}

	if (tokentype == JSON_TOKEN_STRING)
		escape_json(_state->strval, token);
	else
		appendStringInfoString(_state->strval, token);
}

/*
 * SQL function json_strip_nulls(json) -> json
 */
Datum
json_strip_nulls(PG_FUNCTION_ARGS)
{
	text	   *json = PG_GETARG_TEXT_PP(0);
	StripnullState *state;
	JsonLexContext *lex;
	JsonSemAction *sem;

	lex = makeJsonLexContext(json, true);
	state = palloc0(sizeof(StripnullState));
	sem = palloc0(sizeof(JsonSemAction));

	state->strval = makeStringInfo();
	state->skip_next_null = false;
	state->lex = lex;

	sem->semstate = (void *) state;
	sem->object_start = sn_object_start;
	sem->object_end = sn_object_end;
	sem->array_start = sn_array_start;
	sem->array_end = sn_array_end;
	sem->scalar = sn_scalar;
	sem->array_element_start = sn_array_element_start;
	sem->object_field_start = sn_object_field_start;

	pg_parse_json(lex, sem);

	PG_RETURN_TEXT_P(cstring_to_text_with_len(state->strval->data,
											  state->strval->len));

}

/*
 * SQL function jsonb_strip_nulls(jsonb) -> jsonb
 */
Datum
jsonb_strip_nulls(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	JsonbIterator *it;
	JsonbParseState *parseState = NULL;
	JsonbValue *res = NULL;
	JsonbValue	v,
				k;
	JsonbIteratorToken type;
	bool		last_was_key = false;

	if (JB_ROOT_IS_SCALAR(jb))
		PG_RETURN_POINTER(jb);

	it = JsonbIteratorInit(&jb->root);

	while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		Assert(!(type == WJB_KEY && last_was_key));

		if (type == WJB_KEY)
		{
			/* stash the key until we know if it has a null value */
			k = v;
			last_was_key = true;
			continue;
		}

		if (last_was_key)
		{
			/* if the last element was a key this one can't be */
			last_was_key = false;

			/* skip this field if value is null */
			if (type == WJB_VALUE && v.type == jbvNull)
				continue;

			/* otherwise, do a delayed push of the key */
			(void) pushJsonbValue(&parseState, WJB_KEY, &k);
		}

		if (type == WJB_VALUE || type == WJB_ELEM)
			res = pushJsonbValue(&parseState, type, &v);
		else
			res = pushJsonbValue(&parseState, type, NULL);
	}

	Assert(res != NULL);

	PG_RETURN_POINTER(JsonbValueToJsonb(res));
}

/*
 * SQL function jsonb_pretty (jsonb)
 *
 * Pretty-printed text for the jsonb
 */
Datum
jsonb_pretty(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);
	StringInfo	str = makeStringInfo();

	JsonbToCStringIndent(str, &jb->root, VARSIZE(jb));

	PG_RETURN_TEXT_P(cstring_to_text_with_len(str->data, str->len));
}

/*
 * SQL function jsonb_concat (jsonb, jsonb)
 *
 * function for || operator
 */
Datum
jsonb_concat(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb1 = PG_GETARG_JSONB_P(0);
	Jsonb	   *jb2 = PG_GETARG_JSONB_P(1);
	JsonbParseState *state = NULL;
	JsonbValue *res;
	JsonbIterator *it1,
			   *it2;

	/*
	 * If one of the jsonb is empty, just return the other if it's not scalar
	 * and both are of the same kind.  If it's a scalar or they are of
	 * different kinds we need to perform the concatenation even if one is
	 * empty.
	 */
	if (JB_ROOT_IS_OBJECT(jb1) == JB_ROOT_IS_OBJECT(jb2))
	{
		if (JB_ROOT_COUNT(jb1) == 0 && !JB_ROOT_IS_SCALAR(jb2))
			PG_RETURN_JSONB_P(jb2);
		else if (JB_ROOT_COUNT(jb2) == 0 && !JB_ROOT_IS_SCALAR(jb1))
			PG_RETURN_JSONB_P(jb1);
	}

	it1 = JsonbIteratorInit(&jb1->root);
	it2 = JsonbIteratorInit(&jb2->root);

	res = IteratorConcat(&it1, &it2, &state);

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}


/*
 * SQL function jsonb_delete (jsonb, text)
 *
 * return a copy of the jsonb with the indicated item
 * removed.
 */
Datum
jsonb_delete(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	text	   *key = PG_GETARG_TEXT_PP(1);
	char	   *keyptr = VARDATA_ANY(key);
	int			keylen = VARSIZE_ANY_EXHDR(key);
	JsonbParseState *state = NULL;
	JsonbIterator *it;
	JsonbValue	v,
			   *res = NULL;
	bool		skipNested = false;
	JsonbIteratorToken r;

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot delete from scalar")));

	if (JB_ROOT_COUNT(in) == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(&in->root);

	while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
	{
		skipNested = true;

		if ((r == WJB_ELEM || r == WJB_KEY) &&
			(v.type == jbvString && keylen == v.val.string.len &&
			 memcmp(keyptr, v.val.string.val, keylen) == 0))
		{
			/* skip corresponding value as well */
			if (r == WJB_KEY)
				(void) JsonbIteratorNext(&it, &v, true);

			continue;
		}

		res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
	}

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/*
 * SQL function jsonb_delete (jsonb, variadic text[])
 *
 * return a copy of the jsonb with the indicated items
 * removed.
 */
Datum
jsonb_delete_array(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(1);
	Datum	   *keys_elems;
	bool	   *keys_nulls;
	int			keys_len;
	JsonbParseState *state = NULL;
	JsonbIterator *it;
	JsonbValue	v,
			   *res = NULL;
	bool		skipNested = false;
	JsonbIteratorToken r;

	if (ARR_NDIM(keys) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot delete from scalar")));

	if (JB_ROOT_COUNT(in) == 0)
		PG_RETURN_JSONB_P(in);

	deconstruct_array(keys, TEXTOID, -1, false, 'i',
					  &keys_elems, &keys_nulls, &keys_len);

	if (keys_len == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(&in->root);

	while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
	{
		skipNested = true;

		if ((r == WJB_ELEM || r == WJB_KEY) && v.type == jbvString)
		{
			int			i;
			bool		found = false;

			for (i = 0; i < keys_len; i++)
			{
				char	   *keyptr;
				int			keylen;

				if (keys_nulls[i])
					continue;

				keyptr = VARDATA_ANY(keys_elems[i]);
				keylen = VARSIZE_ANY_EXHDR(keys_elems[i]);
				if (keylen == v.val.string.len &&
					memcmp(keyptr, v.val.string.val, keylen) == 0)
				{
					found = true;
					break;
				}
			}
			if (found)
			{
				/* skip corresponding value as well */
				if (r == WJB_KEY)
					(void) JsonbIteratorNext(&it, &v, true);

				continue;
			}
		}

		res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
	}

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/*
 * SQL function jsonb_delete (jsonb, int)
 *
 * return a copy of the jsonb with the indicated item
 * removed. Negative int means count back from the
 * end of the items.
 */
Datum
jsonb_delete_idx(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	int			idx = PG_GETARG_INT32(1);
	JsonbParseState *state = NULL;
	JsonbIterator *it;
	uint32		i = 0,
				n;
	JsonbValue	v,
			   *res = NULL;
	JsonbIteratorToken r;

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot delete from scalar")));

	if (JB_ROOT_IS_OBJECT(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot delete from object using integer index")));

	if (JB_ROOT_COUNT(in) == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(&in->root);

	r = JsonbIteratorNext(&it, &v, false);
	Assert(r == WJB_BEGIN_ARRAY);
	n = v.val.array.nElems;

	if (idx < 0)
	{
		if (-idx > n)
			idx = n;
		else
			idx = n + idx;
	}

	if (idx >= n)
		PG_RETURN_JSONB_P(in);

	pushJsonbValue(&state, r, NULL);

	while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		if (r == WJB_ELEM)
		{
			if (i++ == idx)
				continue;
		}

		res = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
	}

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/*
 * SQL function jsonb_set(jsonb, text[], jsonb, boolean)
 */
Datum
jsonb_set(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
	Jsonb	   *newjsonb = PG_GETARG_JSONB_P(2);
	JsonbValue *newval = JsonbToJsonbValue(newjsonb);
	bool		create = PG_GETARG_BOOL(3);
	JsonbValue *res = NULL;
	Datum	   *path_elems;
	bool	   *path_nulls;
	int			path_len;
	JsonbIterator *it;
	JsonbParseState *st = NULL;

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot set path in scalar")));

	if (JB_ROOT_COUNT(in) == 0 && !create)
		PG_RETURN_JSONB_P(in);

	deconstruct_array(path, TEXTOID, -1, false, 'i',
					  &path_elems, &path_nulls, &path_len);

	if (path_len == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(&in->root);

	res = setPath(&it, path_elems, path_nulls, path_len, &st,
				  0, newval, create ? JB_PATH_CREATE : JB_PATH_REPLACE);

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}


/*
 * SQL function jsonb_delete_path(jsonb, text[])
 */
Datum
jsonb_delete_path(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
	JsonbValue *res = NULL;
	Datum	   *path_elems;
	bool	   *path_nulls;
	int			path_len;
	JsonbIterator *it;
	JsonbParseState *st = NULL;

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot delete path in scalar")));

	if (JB_ROOT_COUNT(in) == 0)
		PG_RETURN_JSONB_P(in);

	deconstruct_array(path, TEXTOID, -1, false, 'i',
					  &path_elems, &path_nulls, &path_len);

	if (path_len == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(&in->root);

	res = setPath(&it, path_elems, path_nulls, path_len, &st,
				  0, NULL, JB_PATH_DELETE);

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/*
 * SQL function jsonb_insert(jsonb, text[], jsonb, boolean)
 */
Datum
jsonb_insert(PG_FUNCTION_ARGS)
{
	Jsonb	   *in = PG_GETARG_JSONB_P(0);
	ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
	Jsonb	   *newjsonb = PG_GETARG_JSONB_P(2);
	JsonbValue *newval = JsonbToJsonbValue(newjsonb);
	bool		after = PG_GETARG_BOOL(3);
	JsonbValue *res = NULL;
	Datum	   *path_elems;
	bool	   *path_nulls;
	int			path_len;
	JsonbIterator *it;
	JsonbParseState *st = NULL;

	if (ARR_NDIM(path) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("wrong number of array subscripts")));

	if (JB_ROOT_IS_SCALAR(in))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot set path in scalar")));

	deconstruct_array(path, TEXTOID, -1, false, 'i',
					  &path_elems, &path_nulls, &path_len);

	if (path_len == 0)
		PG_RETURN_JSONB_P(in);

	it = JsonbIteratorInit(&in->root);

	res = setPath(&it, path_elems, path_nulls, path_len, &st, 0, newval,
				  after ? JB_PATH_INSERT_AFTER : JB_PATH_INSERT_BEFORE);

	Assert(res != NULL);

	PG_RETURN_JSONB_P(JsonbValueToJsonb(res));
}

/*
 * Iterate over all jsonb objects and merge them into one.
 * The logic of this function copied from the same hstore function,
 * except the case, when it1 & it2 represents jbvObject.
 * In that case we just append the content of it2 to it1 without any
 * verifications.
 */
static JsonbValue *
IteratorConcat(JsonbIterator **it1, JsonbIterator **it2,
			   JsonbParseState **state)
{
	JsonbValue	v1,
				v2,
			   *res = NULL;
	JsonbIteratorToken r1,
				r2,
				rk1,
				rk2;

	r1 = rk1 = JsonbIteratorNext(it1, &v1, false);
	r2 = rk2 = JsonbIteratorNext(it2, &v2, false);

	/*
	 * Both elements are objects.
	 */
	if (rk1 == WJB_BEGIN_OBJECT && rk2 == WJB_BEGIN_OBJECT)
	{
		/*
		 * Append the all tokens from v1 to res, except last WJB_END_OBJECT
		 * (because res will not be finished yet).
		 */
		pushJsonbValue(state, r1, NULL);
		while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_END_OBJECT)
			pushJsonbValue(state, r1, &v1);

		/*
		 * Append the all tokens from v2 to res, include last WJB_END_OBJECT
		 * (the concatenation will be completed).
		 */
		while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_DONE)
			res = pushJsonbValue(state, r2, r2 != WJB_END_OBJECT ? &v2 : NULL);
	}

	/*
	 * Both elements are arrays (either can be scalar).
	 */
	else if (rk1 == WJB_BEGIN_ARRAY && rk2 == WJB_BEGIN_ARRAY)
	{
		pushJsonbValue(state, r1, NULL);

		while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_END_ARRAY)
		{
			Assert(r1 == WJB_ELEM);
			pushJsonbValue(state, r1, &v1);
		}

		while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_END_ARRAY)
		{
			Assert(r2 == WJB_ELEM);
			pushJsonbValue(state, WJB_ELEM, &v2);
		}

		res = pushJsonbValue(state, WJB_END_ARRAY, NULL /* signal to sort */ );
	}
	/* have we got array || object or object || array? */
	else if (((rk1 == WJB_BEGIN_ARRAY && !(*it1)->isScalar) && rk2 == WJB_BEGIN_OBJECT) ||
			 (rk1 == WJB_BEGIN_OBJECT && (rk2 == WJB_BEGIN_ARRAY && !(*it2)->isScalar)))
	{

		JsonbIterator **it_array = rk1 == WJB_BEGIN_ARRAY ? it1 : it2;
		JsonbIterator **it_object = rk1 == WJB_BEGIN_OBJECT ? it1 : it2;

		bool		prepend = (rk1 == WJB_BEGIN_OBJECT);

		pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);

		if (prepend)
		{
			pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
			while ((r1 = JsonbIteratorNext(it_object, &v1, true)) != WJB_DONE)
				pushJsonbValue(state, r1, r1 != WJB_END_OBJECT ? &v1 : NULL);

			while ((r2 = JsonbIteratorNext(it_array, &v2, true)) != WJB_DONE)
				res = pushJsonbValue(state, r2, r2 != WJB_END_ARRAY ? &v2 : NULL);
		}
		else
		{
			while ((r1 = JsonbIteratorNext(it_array, &v1, true)) != WJB_END_ARRAY)
				pushJsonbValue(state, r1, &v1);

			pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
			while ((r2 = JsonbIteratorNext(it_object, &v2, true)) != WJB_DONE)
				pushJsonbValue(state, r2, r2 != WJB_END_OBJECT ? &v2 : NULL);

			res = pushJsonbValue(state, WJB_END_ARRAY, NULL);
		}
	}
	else
	{
		/*
		 * This must be scalar || object or object || scalar, as that's all
		 * that's left. Both of these make no sense, so error out.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid concatenation of jsonb objects")));
	}

	return res;
}

/*
 * Do most of the heavy work for jsonb_set/jsonb_insert
 *
 * If JB_PATH_DELETE bit is set in op_type, the element is to be removed.
 *
 * If any bit mentioned in JB_PATH_CREATE_OR_INSERT is set in op_type,
 * we create the new value if the key or array index does not exist.
 *
 * Bits JB_PATH_INSERT_BEFORE and JB_PATH_INSERT_AFTER in op_type
 * behave as JB_PATH_CREATE if new value is inserted in JsonbObject.
 *
 * All path elements before the last must already exist
 * whatever bits in op_type are set, or nothing is done.
 */
static JsonbValue *
setPath(JsonbIterator **it, Datum *path_elems,
		bool *path_nulls, int path_len,
		JsonbParseState **st, int level, JsonbValue *newval, int op_type)
{
	JsonbValue	v;
	JsonbIteratorToken r;
	JsonbValue *res;

	check_stack_depth();

	if (path_nulls[level])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("path element at position %d is null",
						level + 1)));

	r = JsonbIteratorNext(it, &v, false);

	switch (r)
	{
		case WJB_BEGIN_ARRAY:
			(void) pushJsonbValue(st, r, NULL);
			setPathArray(it, path_elems, path_nulls, path_len, st, level,
						 newval, v.val.array.nElems, op_type);
			r = JsonbIteratorNext(it, &v, false);
			Assert(r == WJB_END_ARRAY);
			res = pushJsonbValue(st, r, NULL);
			break;
		case WJB_BEGIN_OBJECT:
			(void) pushJsonbValue(st, r, NULL);
			setPathObject(it, path_elems, path_nulls, path_len, st, level,
						  newval, op_type);
			res = pushJsonbValue(st, WJB_END_OBJECT, NULL);
			break;
		case WJB_ELEM:
		case WJB_VALUE:
			res = pushJsonbValue(st, r, &v);
			break;
		default:
			elog(ERROR, "unrecognized iterator result: %d", (int) r);
			res = NULL;			/* keep compiler quiet */
			break;
	}

	return res;
}

/*
 * Copy object fields, but stop on the desired key if it is specified.
 *
 * True is returned if the key was found, otherwise false.
 */
static bool
copyJsonbObject(JsonbParseState **st, JsonbIterator **it, const JsonbValue *key)
{
	JsonbIteratorToken r;
	JsonbValue	keybuf;

	while ((r = JsonbIteratorNext(it, &keybuf, true)) == WJB_KEY)
	{
		JsonbValue	val;

		if (key &&
			key->val.string.len == keybuf.val.string.len &&
			memcmp(key->val.string.val, keybuf.val.string.val,
				   key->val.string.len) == 0)
			return true;	/* stop, key is found */

		(void) pushJsonbValue(st, r, &keybuf);

		/* Copy value */
		r = JsonbIteratorNext(it, &val, false);
		(void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &val : NULL);

		if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
		{
			int			walking_level = 1;

			while (walking_level != 0)
			{
				r = JsonbIteratorNext(it, &val, false);

				if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
					++walking_level;
				if (r == WJB_END_ARRAY || r == WJB_END_OBJECT)
					--walking_level;

				(void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &val : NULL);
			}
		}
	}

	Assert(r == WJB_END_OBJECT);

	return false;	/* key was not found */
}

/*
 * Object walker for setPath
 */
static void
setPathObject(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
			  int path_len, JsonbParseState **st, int level,
			  JsonbValue *newval, int op_type)
{
	JsonbValue *key,
				keybuf,
				val;

	if (level >= path_len || path_nulls[level])
		key = NULL;
	else
	{
		key = &keybuf;
		key->type = jbvString;
		key->val.string.val = VARDATA_ANY(path_elems[level]);
		key->val.string.len = VARSIZE_ANY_EXHDR(path_elems[level]);
	}

	/* Start copying object fields and stop on the desired key. */
	if (copyJsonbObject(st, it, key))
	{
		/* The desired key was found. */
		if (level == path_len - 1)
		{
			/*
			 * called from jsonb_insert(), it forbids redefining an
			 * existing value
			 */
			if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("cannot replace existing key"),
						 errhint("Try using the function jsonb_set "
								 "to replace key value.")));

			(void) JsonbIteratorNext(it, &val, true);	/* skip value */

			if (!(op_type & JB_PATH_DELETE))
			{
				(void) pushJsonbValue(st, WJB_KEY, key);
				(void) pushJsonbValue(st, WJB_VALUE, newval);
			}
		}
		else
		{
			(void) pushJsonbValue(st, WJB_KEY, key);
			setPath(it, path_elems, path_nulls, path_len, st, level + 1,
					newval, op_type);
		}

		/* Copy the remaining fields. */
		(void) copyJsonbObject(st, it, NULL);
	}
	else if (key && (op_type & JB_PATH_CREATE_OR_INSERT) &&
			 level == path_len - 1)
	{
		/* All fields were copied, but the desired key was not found. */
		(void) pushJsonbValue(st, WJB_KEY, key);
		(void) pushJsonbValue(st, WJB_VALUE, newval);
	}
}

/*
 * Array walker for setPath
 */
static void
setPathArray(JsonbIterator **it, Datum *path_elems, bool *path_nulls,
			 int path_len, JsonbParseState **st, int level,
			 JsonbValue *newval, uint32 nelems, int op_type)
{
	JsonbValue	v;
	int			idx,
				i;
	bool		done = false;

	/* pick correct index */
	if (level < path_len && !path_nulls[level])
	{
		long		lindex;

		if (!jsonb_get_array_index_from_text(path_elems[level], &lindex))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("path element at position %d is not an integer: \"%s\"",
							level + 1, TextDatumGetCString(path_elems[level]))));

		idx = lindex;
	}
	else
		idx = nelems;

	if (idx < 0)
	{
		if (-idx > nelems)
			idx = INT_MIN;
		else
			idx = nelems + idx;
	}

	if (idx > 0 && idx > nelems)
		idx = nelems;

	/*
	 * if we're creating, and idx == INT_MIN, we prepend the new value to the
	 * array also if the array is empty - in which case we don't really care
	 * what the idx value is
	 */

	if ((idx == INT_MIN || nelems == 0) && (level == path_len - 1) &&
		(op_type & JB_PATH_CREATE_OR_INSERT))
	{
		Assert(newval != NULL);
		(void) pushJsonbValue(st, WJB_ELEM, newval);
		done = true;
	}

	/* iterate over the array elements */
	for (i = 0; i < nelems; i++)
	{
		JsonbIteratorToken r;

		if (i == idx && level < path_len)
		{
			if (level == path_len - 1)
			{
				r = JsonbIteratorNext(it, &v, true);	/* skip */

				if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_CREATE))
					(void) pushJsonbValue(st, WJB_ELEM, newval);

				/*
				 * We should keep current value only in case of
				 * JB_PATH_INSERT_BEFORE or JB_PATH_INSERT_AFTER because
				 * otherwise it should be deleted or replaced
				 */
				if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_INSERT_BEFORE))
					(void) pushJsonbValue(st, r, &v);

				if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_REPLACE))
					(void) pushJsonbValue(st, WJB_ELEM, newval);

				done = true;
			}
			else
				(void) setPath(it, path_elems, path_nulls, path_len,
							   st, level + 1, newval, op_type);
		}
		else
		{
			r = JsonbIteratorNext(it, &v, false);

			(void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);

			if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
			{
				int			walking_level = 1;

				while (walking_level != 0)
				{
					r = JsonbIteratorNext(it, &v, false);

					if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
						++walking_level;
					if (r == WJB_END_ARRAY || r == WJB_END_OBJECT)
						--walking_level;

					(void) pushJsonbValue(st, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
				}
			}

			if ((op_type & JB_PATH_CREATE_OR_INSERT) && !done &&
				level == path_len - 1 && i == nelems - 1)
			{
				(void) pushJsonbValue(st, WJB_ELEM, newval);
			}
		}
	}
}

/*
 * Perform an actual data extraction or modification for the jsonb
 * subscripting. As a result the extracted Datum or the modified containers
 * value will be returned.
 */
Datum
jsonb_subscript_fetch(Datum containerSource, SubscriptingRefState *sbstate)
{
	JsonbValue *jbv = sbstate->privatedata;

	Assert(!sbstate->isassignment);

	return JsonbPGetDatum(JsonbValueToJsonb(jbv));
}

/*
 * Perform an actual data extraction or modification for the jsonb
 * subscripting. As a result the extracted Datum or the modified containers
 * value will be returned.
 */
Datum
jsonb_subscript_assign(Datum containerSource, SubscriptingRefState *sbstate)
{
	JsonbAssignState *astate = sbstate->privatedata;
	JsonbSubscriptState *subscript = &astate->subscripts[sbstate->numupper - 1];
	JsonbValue *res = NULL;
	JsonbValue *newval;
	JsonbValue jbv;
	JsonbIteratorToken tok;

	/* If the original jsonb is NULL, we will create a new container. */
	if (sbstate->resnull)
		sbstate->resnull = false;

	/* Transform the new value to jsonb */
	newval = to_jsonb_worker(sbstate->replacevalue, sbstate->refelemtype,
							 sbstate->replacenull);

	if (newval->type == jbvArray && newval->val.array.rawScalar)
		*newval = newval->val.array.elems[0];

	/* Push the new value */
	tok = subscript->is_array ? WJB_ELEM : WJB_VALUE;
	res = pushJsonbValue(&astate->ps, tok, newval);

	/* Finish unclosed arrays/objects */
	for (; subscript >= astate->subscripts; subscript--)
	{
		/*
		 * If the element does exists, then all preceding subscripts must exist
		 * and the iterator may contain remaining elements.  So we need to
		 * switch to copying from the iterator now.
		 */
		if (subscript[1].exists)
			break;

		if (subscript->is_array && subscript->array_index < 0)
		{
			/* Fill the gap between prepended element and 0th element */
			if (subscript->array_index < -1)
				push_null_elements(&astate->ps, -1 - subscript->array_index);

			if (subscript->exists)
				break;	/* original elements are copied from the iterator */
		}

		tok = subscript->is_array ? WJB_END_ARRAY : WJB_END_OBJECT;
		res = pushJsonbValue(&astate->ps, tok, NULL);
	}

	/* Copy remaining elements from the iterator */
	while ((tok = JsonbIteratorNext(&astate->iter, &jbv, false)) != WJB_DONE)
		res = pushJsonbValue(&astate->ps, tok, tok < WJB_BEGIN_ARRAY ? &jbv : NULL);

	return JsonbPGetDatum(JsonbValueToJsonb(res));
}

/*
 * Perform preparation for the jsonb subscripting. Since there are not any
 * particular restrictions for this kind of subscripting, we will verify that
 * it is not a slice operation. This function produces an expression that
 * represents the result of extracting a single container element or the new
 * container value with the source data inserted into the right part of the
 * container. If you have read until this point, and will submit a meaningful
 * review of this patch series, I'll owe you a beer at the next PGConfEU.
 */

/*
 * Handle jsonb-type subscripting logic.
 */
Datum
jsonb_subscript_handler(PG_FUNCTION_ARGS)
{
	SubscriptRoutines *sbsroutines = (SubscriptRoutines *)
		palloc0(sizeof(SubscriptRoutines));

	sbsroutines->prepare = jsonb_subscript_prepare;
	sbsroutines->validate = jsonb_subscript_validate;
	sbsroutines->fetch = jsonb_subscript_fetch;
	sbsroutines->assign = jsonb_subscript_assign;
	sbsroutines->init = jsonb_subscript_init;
	sbsroutines->step = jsonb_subscript_step;
	sbsroutines->selectexpr = jsonb_subscript_selectexpr;

	PG_RETURN_POINTER(sbsroutines);
}

SubscriptingRef *
jsonb_subscript_prepare(bool isAssignment, SubscriptingRef *sbsref)
{
	if (isAssignment)
	{
		sbsref->refelemtype = exprType((Node *) sbsref->refassgnexpr);
		sbsref->refassgntype = exprType((Node *) sbsref->refassgnexpr);
	}
	else
		sbsref->refelemtype = JSONBOID;

	return sbsref;
}

SubscriptingRef *
jsonb_subscript_validate(bool isAssignment, SubscriptingRef *sbsref,
						 ParseState *pstate)
{
	List	   *upperIndexpr = NIL;
	ListCell   *l;

	if (sbsref->reflowerindexpr != NIL)
	{
		Node	   *slice = NULL;

		/* Try to find first non-NULL lower subscript */
		foreach(l, sbsref->reflowerindexpr)
		{
			if (lfirst(l) != NULL)
			{
				slice = (Node *) lfirst(l);
				break;
			}
		}

		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("jsonb subscript does not support slices"),
				 parser_errposition(pstate, exprLocation(slice))));
	}

	foreach(l, sbsref->refupperindexpr)
	{
		Node	   *subexpr = (Node *) lfirst(l);
		Node	   *textexpr;
		Node	   *intexpr;
		Oid			subexprType;
		char		typcategory;
		bool		typispreferred;

		if (subexpr == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("jsonb subscript does not support slices"),
					 parser_errposition(pstate, exprLocation(
						((Node *) linitial(sbsref->refupperindexpr))))));

		subexprType = exprType(subexpr);

		textexpr = coerce_to_target_type(pstate,
										 subexpr, subexprType,
										 TEXTOID, -1,
										 COERCION_ASSIGNMENT,
										 COERCE_IMPLICIT_CAST,
										 -1);

		if (textexpr == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("jsonb subscript must have %s type", "text"),
					 parser_errposition(pstate, exprLocation(subexpr))));

		/* Try to coerce numeric types to int4 for array subscripting. */
		get_type_category_preferred(subexprType, &typcategory, &typispreferred);

		if (typcategory == TYPCATEGORY_NUMERIC)
		{
			intexpr = coerce_to_target_type(pstate,
											subexpr, subexprType,
											INT4OID, -1,
											COERCION_ASSIGNMENT,
											COERCE_IMPLICIT_CAST,
											-1);

			if (intexpr &&
				(subexprType == INT4OID ||
				 subexprType == INT2OID))
				textexpr = NULL;
		}
		else
			intexpr = NULL;

		/*
		 * If int4 expression variant exists, create a list with both text and
		 * int4 variants.
		 */
		subexpr = textexpr && intexpr ? (Node *) list_make2(textexpr, intexpr) :
			textexpr ? textexpr : intexpr;

		upperIndexpr = lappend(upperIndexpr, subexpr);
	}

	sbsref->refupperindexpr = upperIndexpr;

	return sbsref;
}

/*
 * Parse information about what elements of a jsonb document we want to iterate
 * in functions iterate_json(b)_values. This information is presented in jsonb
 * format, so that it can be easily extended in the future.
 */
uint32
parse_jsonb_index_flags(Jsonb *jb)
{
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken type;
	uint32		flags = 0;

	it = JsonbIteratorInit(&jb->root);

	type = JsonbIteratorNext(&it, &v, false);

	/*
	 * We iterate over array (scalar internally is represented as array, so,
	 * we will accept it too) to check all its elements.  Flag names are
	 * chosen the same as jsonb_typeof uses.
	 */
	if (type != WJB_BEGIN_ARRAY)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("wrong flag type, only arrays and scalars are allowed")));

	while ((type = JsonbIteratorNext(&it, &v, false)) == WJB_ELEM)
	{
		if (v.type != jbvString)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("flag array element is not a string"),
					 errhint("Possible values are: \"string\", \"numeric\", \"boolean\", \"key\", and \"all\".")));

		if (v.val.string.len == 3 &&
			pg_strncasecmp(v.val.string.val, "all", 3) == 0)
			flags |= jtiAll;
		else if (v.val.string.len == 3 &&
				 pg_strncasecmp(v.val.string.val, "key", 3) == 0)
			flags |= jtiKey;
		else if (v.val.string.len == 6 &&
				 pg_strncasecmp(v.val.string.val, "string", 5) == 0)
			flags |= jtiString;
		else if (v.val.string.len == 7 &&
				 pg_strncasecmp(v.val.string.val, "numeric", 7) == 0)
			flags |= jtiNumeric;
		else if (v.val.string.len == 7 &&
				 pg_strncasecmp(v.val.string.val, "boolean", 7) == 0)
			flags |= jtiBool;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("wrong flag in flag array: \"%s\"",
							pnstrdup(v.val.string.val, v.val.string.len)),
					 errhint("Possible values are: \"string\", \"numeric\", \"boolean\", \"key\", and \"all\".")));
	}

	/* expect end of array now */
	if (type != WJB_END_ARRAY)
		elog(ERROR, "unexpected end of flag array");

	/* get final WJB_DONE and free iterator */
	type = JsonbIteratorNext(&it, &v, false);
	if (type != WJB_DONE)
		elog(ERROR, "unexpected end of flag array");

	return flags;
}

/*
 * Iterate over jsonb values or elements, specified by flags, and pass them
 * together with an iteration state to a specified JsonIterateStringValuesAction.
 */
void
iterate_jsonb_values(Jsonb *jb, uint32 flags, void *state,
					 JsonIterateStringValuesAction action)
{
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken type;

	it = JsonbIteratorInit(&jb->root);

	/*
	 * Just recursively iterating over jsonb and call callback on all
	 * corresponding elements
	 */
	while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		if (type == WJB_KEY)
		{
			if (flags & jtiKey)
				action(state, v.val.string.val, v.val.string.len);

			continue;
		}
		else if (!(type == WJB_VALUE || type == WJB_ELEM))
		{
			/* do not call callback for composite JsonbValue */
			continue;
		}

		/* JsonbValue is a value of object or element of array */
		switch (v.type)
		{
			case jbvString:
				if (flags & jtiString)
					action(state, v.val.string.val, v.val.string.len);
				break;
			case jbvNumeric:
				if (flags & jtiNumeric)
				{
					char	   *val;

					val = DatumGetCString(DirectFunctionCall1(numeric_out,
															  NumericGetDatum(v.val.numeric)));

					action(state, val, strlen(val));
					pfree(val);
				}
				break;
			case jbvBool:
				if (flags & jtiBool)
				{
					if (v.val.boolean)
						action(state, "true", 4);
					else
						action(state, "false", 5);
				}
				break;
			default:
				/* do not call callback for composite JsonbValue */
				break;
		}
	}
}

/*
 * Iterate over json values and elements, specified by flags, and pass them
 * together with an iteration state to a specified JsonIterateStringValuesAction.
 */
void
iterate_json_values(text *json, uint32 flags, void *action_state,
					JsonIterateStringValuesAction action)
{
	JsonLexContext *lex = makeJsonLexContext(json, true);
	JsonSemAction *sem = palloc0(sizeof(JsonSemAction));
	IterateJsonStringValuesState *state = palloc0(sizeof(IterateJsonStringValuesState));

	state->lex = lex;
	state->action = action;
	state->action_state = action_state;
	state->flags = flags;

	sem->semstate = (void *) state;
	sem->scalar = iterate_values_scalar;
	sem->object_field_start = iterate_values_object_field_start;

	pg_parse_json(lex, sem);
}

/*
 * An auxiliary function for iterate_json_values to invoke a specified
 * JsonIterateStringValuesAction for specified values.
 */
static void
iterate_values_scalar(void *state, char *token, JsonTokenType tokentype)
{
	IterateJsonStringValuesState *_state = (IterateJsonStringValuesState *) state;

	switch (tokentype)
	{
		case JSON_TOKEN_STRING:
			if (_state->flags & jtiString)
				_state->action(_state->action_state, token, strlen(token));
			break;
		case JSON_TOKEN_NUMBER:
			if (_state->flags & jtiNumeric)
				_state->action(_state->action_state, token, strlen(token));
			break;
		case JSON_TOKEN_TRUE:
		case JSON_TOKEN_FALSE:
			if (_state->flags & jtiBool)
				_state->action(_state->action_state, token, strlen(token));
			break;
		default:
			/* do not call callback for any other token */
			break;
	}
}

static void
iterate_values_object_field_start(void *state, char *fname, bool isnull)
{
	IterateJsonStringValuesState *_state = (IterateJsonStringValuesState *) state;

	if (_state->flags & jtiKey)
	{
		char	   *val = pstrdup(fname);

		_state->action(_state->action_state, val, strlen(val));
	}
}

/*
 * Iterate over a jsonb, and apply a specified JsonTransformStringValuesAction
 * to every string value or element. Any necessary context for a
 * JsonTransformStringValuesAction can be passed in the action_state variable.
 * Function returns a copy of an original jsonb object with transformed values.
 */
Jsonb *
transform_jsonb_string_values(Jsonb *jsonb, void *action_state,
							  JsonTransformStringValuesAction transform_action)
{
	JsonbIterator *it;
	JsonbValue	v,
			   *res = NULL;
	JsonbIteratorToken type;
	JsonbParseState *st = NULL;
	text	   *out;
	bool		is_scalar = false;

	it = JsonbIteratorInit(&jsonb->root);
	is_scalar = it->isScalar;

	while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		if ((type == WJB_VALUE || type == WJB_ELEM) && v.type == jbvString)
		{
			out = transform_action(action_state, v.val.string.val, v.val.string.len);
			v.val.string.val = VARDATA_ANY(out);
			v.val.string.len = VARSIZE_ANY_EXHDR(out);
			res = pushJsonbValue(&st, type, type < WJB_BEGIN_ARRAY ? &v : NULL);
		}
		else
		{
			res = pushJsonbValue(&st, type, (type == WJB_KEY ||
											 type == WJB_VALUE ||
											 type == WJB_ELEM) ? &v : NULL);
		}
	}

	if (res->type == jbvArray)
		res->val.array.rawScalar = is_scalar;

	return JsonbValueToJsonb(res);
}

/*
 * Iterate over a json, and apply a specified JsonTransformStringValuesAction
 * to every string value or element. Any necessary context for a
 * JsonTransformStringValuesAction can be passed in the action_state variable.
 * Function returns a StringInfo, which is a copy of an original json with
 * transformed values.
 */
text *
transform_json_string_values(text *json, void *action_state,
							 JsonTransformStringValuesAction transform_action)
{
	JsonLexContext *lex = makeJsonLexContext(json, true);
	JsonSemAction *sem = palloc0(sizeof(JsonSemAction));
	TransformJsonStringValuesState *state = palloc0(sizeof(TransformJsonStringValuesState));

	state->lex = lex;
	state->strval = makeStringInfo();
	state->action = transform_action;
	state->action_state = action_state;

	sem->semstate = (void *) state;
	sem->scalar = transform_string_values_scalar;
	sem->object_start = transform_string_values_object_start;
	sem->object_end = transform_string_values_object_end;
	sem->array_start = transform_string_values_array_start;
	sem->array_end = transform_string_values_array_end;
	sem->scalar = transform_string_values_scalar;
	sem->array_element_start = transform_string_values_array_element_start;
	sem->object_field_start = transform_string_values_object_field_start;

	pg_parse_json(lex, sem);

	return cstring_to_text_with_len(state->strval->data, state->strval->len);
}

/*
 * Set of auxiliary functions for transform_json_string_values to invoke a
 * specified JsonTransformStringValuesAction for all values and left everything
 * else untouched.
 */
static void
transform_string_values_object_start(void *state)
{
	TransformJsonStringValuesState *_state = (TransformJsonStringValuesState *) state;

	appendStringInfoCharMacro(_state->strval, '{');
}

static void
transform_string_values_object_end(void *state)
{
	TransformJsonStringValuesState *_state = (TransformJsonStringValuesState *) state;

	appendStringInfoCharMacro(_state->strval, '}');
}

static void
transform_string_values_array_start(void *state)
{
	TransformJsonStringValuesState *_state = (TransformJsonStringValuesState *) state;

	appendStringInfoCharMacro(_state->strval, '[');
}

static void
transform_string_values_array_end(void *state)
{
	TransformJsonStringValuesState *_state = (TransformJsonStringValuesState *) state;

	appendStringInfoCharMacro(_state->strval, ']');
}

static void
transform_string_values_object_field_start(void *state, char *fname, bool isnull)
{
	TransformJsonStringValuesState *_state = (TransformJsonStringValuesState *) state;

	if (_state->strval->data[_state->strval->len - 1] != '{')
		appendStringInfoCharMacro(_state->strval, ',');

	/*
	 * Unfortunately we don't have the quoted and escaped string any more, so
	 * we have to re-escape it.
	 */
	escape_json(_state->strval, fname);
	appendStringInfoCharMacro(_state->strval, ':');
}

static void
transform_string_values_array_element_start(void *state, bool isnull)
{
	TransformJsonStringValuesState *_state = (TransformJsonStringValuesState *) state;

	if (_state->strval->data[_state->strval->len - 1] != '[')
		appendStringInfoCharMacro(_state->strval, ',');
}

static void
transform_string_values_scalar(void *state, char *token, JsonTokenType tokentype)
{
	TransformJsonStringValuesState *_state = (TransformJsonStringValuesState *) state;

	if (tokentype == JSON_TOKEN_STRING)
	{
		text	   *out = _state->action(_state->action_state, token, strlen(token));

		escape_json(_state->strval, text_to_cstring(out));
	}
	else
		appendStringInfoString(_state->strval, token);
}
