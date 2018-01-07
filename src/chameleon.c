/*-------------------------------------------------------------------------
 *
 * pgchameleon.c
 *		pgchameleon logical decoding output plugin derived from 
 *		postgresql's test_decoding.c  
 *
 * Copyright (c) 2012-2018, PostgreSQL Global Development Group
 * Copyright (c) 2018 Federico Campoli
 * IDENTIFICATION
 *		  src/chameleon.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"

#include "nodes/parsenodes.h"

#include "replication/output_plugin.h"
#include "replication/logical.h"
#include "replication/message.h"
#include "replication/origin.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

PG_MODULE_MAGIC;

/* These must be available to pg_dlsym() */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

typedef struct
{
	MemoryContext context;
	bool		include_xids;
	bool		include_timestamp;
	bool		skip_empty_xacts;
	bool		xact_wrote_changes;
	bool		only_local;
} chameleonData;

static void pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
				  bool is_init);
static void pg_decode_shutdown(LogicalDecodingContext *ctx);
static void pg_decode_begin_txn(LogicalDecodingContext *ctx,
					ReorderBufferTXN *txn);
static void pg_output_begin(LogicalDecodingContext *ctx,
				chameleonData *data,
				ReorderBufferTXN *txn,
				bool last_write);
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pg_decode_change(LogicalDecodingContext *ctx,
				 ReorderBufferTXN *txn, Relation rel,
				 ReorderBufferChange *change);
static bool pg_decode_filter(LogicalDecodingContext *ctx,
				 RepOriginId origin_id);
static void pg_decode_message(LogicalDecodingContext *ctx,
				  ReorderBufferTXN *txn, XLogRecPtr message_lsn,
				  bool transactional, const char *prefix,
				  Size sz, const char *message);

void
_PG_init(void)
{
	/* other plugins can perform things here */
}

/* specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = pg_decode_startup;
	cb->begin_cb = pg_decode_begin_txn;
	cb->change_cb = pg_decode_change;
	cb->commit_cb = pg_decode_commit_txn;
	cb->filter_by_origin_cb = pg_decode_filter;
	cb->shutdown_cb = pg_decode_shutdown;
	cb->message_cb = pg_decode_message;
}


/* initialize this plugin */
static void
pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
				  bool is_init)
{
	ListCell   *option;
	chameleonData *data;

	data = palloc0(sizeof(chameleonData));
	data->context = AllocSetContextCreate(ctx->context,
										  "text conversion context",
										  ALLOCSET_DEFAULT_SIZES);
	data->include_xids = true;
	data->include_timestamp = false;
	data->skip_empty_xacts = false;
	data->only_local = false;

	ctx->output_plugin_private = data;

	opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;

	foreach(option, ctx->output_plugin_options)
	{
		DefElem    *elem = lfirst(option);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		if (strcmp(elem->defname, "include-xids") == 0)
		{
			/* if option does not provide a value, it means its value is true */
			if (elem->arg == NULL)
				data->include_xids = true;
			else if (!parse_bool(strVal(elem->arg), &data->include_xids))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-timestamp") == 0)
		{
			if (elem->arg == NULL)
				data->include_timestamp = true;
			else if (!parse_bool(strVal(elem->arg), &data->include_timestamp))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "force-binary") == 0)
		{
			bool		force_binary;

			if (elem->arg == NULL)
				continue;
			else if (!parse_bool(strVal(elem->arg), &force_binary))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));

			if (force_binary)
				opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;
		}
		else if (strcmp(elem->defname, "skip-empty-xacts") == 0)
		{

			if (elem->arg == NULL)
				data->skip_empty_xacts = true;
			else if (!parse_bool(strVal(elem->arg), &data->skip_empty_xacts))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "only-local") == 0)
		{

			if (elem->arg == NULL)
				data->only_local = true;
			else if (!parse_bool(strVal(elem->arg), &data->only_local))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" = \"%s\" is unknown",
							elem->defname,
							elem->arg ? strVal(elem->arg) : "(null)")));
		}
	}
}

/* cleanup this plugin's resources */
static void
pg_decode_shutdown(LogicalDecodingContext *ctx)
{
	chameleonData *data = ctx->output_plugin_private;

	/* cleanup our own resources via memory context reset */
	MemoryContextDelete(data->context);
}

/* BEGIN callback */
static void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	chameleonData *data = ctx->output_plugin_private;

	data->xact_wrote_changes = false;
	if (data->skip_empty_xacts)
		return;

	pg_output_begin(ctx, data, txn, true);
}

static void
pg_output_begin(LogicalDecodingContext *ctx, chameleonData *data, ReorderBufferTXN *txn, bool last_write)
{
	OutputPluginPrepareWrite(ctx, last_write);
	appendStringInfo(ctx->out, "{'action':'BEGIN', 'xid': '%u', 'timestamp':'%s' }", txn->xid,timestamptz_to_str(txn->commit_time));
	OutputPluginWrite(ctx, last_write);

	
}

/* COMMIT callback */
static void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	chameleonData *data = ctx->output_plugin_private;

	if (data->skip_empty_xacts && !data->xact_wrote_changes)
		return;

	OutputPluginPrepareWrite(ctx, true);
	appendStringInfo(ctx->out, "{'action':'COMMIT', 'xid': '%u', 'timestamp':'%s' }", txn->xid,timestamptz_to_str(txn->commit_time));

	OutputPluginWrite(ctx, true);
}

static bool
pg_decode_filter(LogicalDecodingContext *ctx,
				 RepOriginId origin_id)
{
	chameleonData *data = ctx->output_plugin_private;

	if (data->only_local && origin_id != InvalidRepOriginId)
		return true;
	return false;
}
 
/*
 * Print literal `outputstr' already represented as string of type `typid'
 * into stringbuf `s'.
 * All types aren quoted for conversion in python dictionary. 
 *  Escaping is done as if standard_conforming_strings were disabled 
 *  as the python dictionary works with \'  .
 */
static void
print_literal(StringInfo s, Oid typid, char *outputstr)
{
	const char *valptr;
	
	appendStringInfoChar(s, 'r');
	appendStringInfoChar(s, '\'');	
	switch (typid)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			/* NB: We don't care about Inf, NaN et al. */
			appendStringInfoString(s, outputstr);
			break;

		case BITOID:
		case VARBITOID:
			appendStringInfo(s, "B'%s'", outputstr);
			break;

		case BYTEAOID:
			appendStringInfo(s, "%s", outputstr);
			break;
		case BOOLOID:
			if (strcmp(outputstr, "t") == 0)
				appendStringInfoString(s, "true");
			else
				appendStringInfoString(s, "false");
			break;

		default:
			
			for (valptr = outputstr; *valptr; valptr++)
			{
				char		ch = *valptr;
				/*escape for single quotes  '  */
				if (SQL_STR_DOUBLE(ch, false))
					appendStringInfoChar(s, '\\');
				appendStringInfoChar(s, ch);
			}
			
			break;
	}
	appendStringInfoChar(s, '\'');
}


/* print the tuple 'tuple' into the string s */
static void
tuple_to_dictionary(StringInfo s, TupleDesc tupdesc, HeapTuple tuple, bool skip_nulls)
{
	int			natt;
	/* print all columns individually */
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
				Form_pg_attribute attr; /* the attribute itself */
		Oid			typid;		/* type of current attribute */
		Oid			typoutput;	/* output function */
		bool		typisvarlena;
		Datum		origval;	/* possibly toasted Datum */
		bool		isnull;		/* column is null? */

		attr = TupleDescAttr(tupdesc, natt);

		/*
		 * don't print dropped columns, we can't be sure everything is
		 * available for them
		 */
		if (attr->attisdropped)
			continue;

		/*
		 * Don't print system columns, oid will already have been printed if
		 * present.
		 */
		if (attr->attnum < 0)
			continue;

		typid = attr->atttypid;

		/* get Datum from tuple */
		origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);

		if (isnull && skip_nulls)
			continue;

		/* query output function */
		getTypeOutputInfo(typid,
						  &typoutput, &typisvarlena);

		
		/* print attribute name */
		appendStringInfo(s, "'%s':",NameStr(attr->attname));
		
		/* print data */
		if (isnull)
			appendStringInfoString(s, "'null'");
		else if (!typisvarlena)
			print_literal(s, typid,
						  OidOutputFunctionCall(typoutput, origval));
		else
		{
			Datum		val;	/* we detoast the Datum always*/

			val = PointerGetDatum(PG_DETOAST_DATUM(origval));
			print_literal(s, typid, OidOutputFunctionCall(typoutput, val));
		}
		
		/* add comma */
		
		appendStringInfoChar(s, ',');
		
	}

}


/*
 * callback for individual changed tuples
 */
static void
pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 Relation relation, ReorderBufferChange *change)
{
	chameleonData *data;
	Form_pg_class class_form;
	TupleDesc	tupdesc;
	MemoryContext old;

	data = ctx->output_plugin_private;

	/* output BEGIN if we haven't yet */
	if (data->skip_empty_xacts && !data->xact_wrote_changes)
	{
		pg_output_begin(ctx, data, txn, false);
	}
	data->xact_wrote_changes = true;

	class_form = RelationGetForm(relation);
	tupdesc = RelationGetDescr(relation);

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	OutputPluginPrepareWrite(ctx, true);
	appendStringInfo(ctx->out, "{");
	if (data->include_xids)
		appendStringInfo(ctx->out, "'txid':'%u',", txn->xid);
	if (data->include_timestamp)
		appendStringInfo(ctx->out, "'timestamp':'%s',",
						 timestamptz_to_str(txn->commit_time));

	appendStringInfoString(ctx->out, "'table':");
	appendStringInfoString(ctx->out, "'");
	appendStringInfoString(ctx->out,
						   quote_qualified_identifier(
													  get_namespace_name(
																		 get_rel_namespace(RelationGetRelid(relation))),
													  NameStr(class_form->relname)));
	appendStringInfoString(ctx->out, "',");

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			appendStringInfoString(ctx->out, " 'action':'insert', 'values':{");
			if (change->data.tp.newtuple == NULL)
				appendStringInfoString(ctx->out, " (no-tuple-data)");
			else
				tuple_to_dictionary(ctx->out, tupdesc,
									&change->data.tp.newtuple->tuple,
									false);
			appendStringInfoString(ctx->out, " }");
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			appendStringInfoString(ctx->out, " 'action':'update', ");
			if (change->data.tp.oldtuple != NULL)
			{
				appendStringInfoString(ctx->out, " 'before_values': {");
				tuple_to_dictionary(ctx->out, tupdesc,
									&change->data.tp.oldtuple->tuple,
									false);
				appendStringInfoString(ctx->out, " }");
			}

			if (change->data.tp.newtuple == NULL)
				appendStringInfoString(ctx->out, " after_values: {}");
			else
				appendStringInfoString(ctx->out, " after_values:{");
				tuple_to_dictionary(ctx->out, tupdesc,
									&change->data.tp.newtuple->tuple,
									false);
				appendStringInfoString(ctx->out, " }");
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			/* if there was  PK, we emit the change */
			if (change->data.tp.oldtuple != NULL)
				appendStringInfoString(ctx->out, " 'action':'delete', 'values': {");
				tuple_to_dictionary(ctx->out, tupdesc,
									&change->data.tp.oldtuple->tuple,
									false);
				appendStringInfoString(ctx->out, " }");
			break;
		default:
			Assert(false);
	}
	appendStringInfo(ctx->out, "}");
	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);

	OutputPluginWrite(ctx, true);
}

static void
pg_decode_message(LogicalDecodingContext *ctx,
				  ReorderBufferTXN *txn, XLogRecPtr lsn, bool transactional,
				  const char *prefix, Size sz, const char *message)
{
	OutputPluginPrepareWrite(ctx, true);
	appendStringInfo(ctx->out, "message: transactional: %d prefix: %s, sz: %zu content:",
					 transactional, prefix, sz);
	appendBinaryStringInfo(ctx->out, message, sz);
	OutputPluginWrite(ctx, true);
}
