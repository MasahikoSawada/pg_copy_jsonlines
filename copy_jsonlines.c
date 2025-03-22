/*--------------------------------------------------------------------------
 *
 * copy_jsonlines.c
 *		JSON Lines text format support for COPY command.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/copy_jsonlines.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/copyapi.h"
#include "commands/copyto_internal.h"
#include "commands/copyfrom_internal.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(jsonlines_handler);

static void JsonLinesCopyFromInFunc(CopyFromState cstate, Oid atttypid, FmgrInfo *finfo,
									Oid *typioparam);
static void JsonLinesCopyFromStart(CopyFromState cstate, TupleDesc tupDesc);
static bool JsonLinesCopyFromOneRow(CopyFromState cstate, ExprContext *econtext, Datum *values,
									bool *nulls);
static void JsonLinesCopyFromEnd(CopyFromState cstate);
static void JsonLinesCopyToOutFunc(CopyToState cstate, Oid atttypid, FmgrInfo *finfo);
static void JsonLinesCopyToStart(CopyToState cstate, TupleDesc tupDesc);
static void JsonLinesCopyToOneRow(CopyToState cstate, TupleTableSlot *slot);
static void JsonLinesCopyToEnd(CopyToState cstate);

/*
 * Read one line from the source.
 *
 * This function uses raw_buf and line_buf, but not input_buf. raw_buf is used to
 * load the raw data from the source, and transferred data into line_buf until
 * it finds a new line character, which is the data separator of JSON Lines format.
 *
 * XXX: support only '\n' new line.
 */
static bool
JsonLineReadLine(CopyFromState cstate)
{
	int			nbytes;
	bool		result = false;

	resetStringInfo(&cstate->line_buf);

	for (;;)
	{
		char		*ptr;

		/* Load more data if needed */
		if (RAW_BUF_BYTES(cstate) <= 0)
		{
			int		inbytes;

			inbytes = CopyFromStateGetData(cstate, cstate->raw_buf, 1, RAW_BUF_SIZE);
			cstate->raw_buf[inbytes] = '\0';
			cstate->raw_buf_len = inbytes;
			cstate->raw_buf_index = 0;

			cstate->bytes_processed += inbytes;

			if (RAW_BUF_BYTES(cstate) <= 0)
			{
				result = true;
				break;
			}
		}

		ptr = strchr(cstate->raw_buf + cstate->raw_buf_index, '\n');

		if (ptr == NULL)
		{
			appendBinaryStringInfo(&cstate->line_buf,
								   cstate->raw_buf + cstate->raw_buf_index,
								   cstate->raw_buf_len - cstate->raw_buf_index);
			cstate->raw_buf_index = cstate->raw_buf_len;
			continue;
		}

		nbytes = (ptr - cstate->raw_buf) - cstate->raw_buf_index;
		appendBinaryStringInfo(&cstate->line_buf,
							   cstate->raw_buf + cstate->raw_buf_index,
							   nbytes);
		cstate->raw_buf_index += nbytes;

		/* consume '\n' */
		cstate->raw_buf_index++;
		break;
	}

	return result;
}

/*
 * Assign the input function data to the given *flinfo.
 */
static void
JsonLinesCopyFromInFunc(CopyFromState cstate, Oid atttypid, FmgrInfo *finfo, Oid *typioparam)
{
	Oid			func_oid;

	getTypeInputInfo(atttypid, &func_oid, typioparam);
	fmgr_info(func_oid, finfo);
}

static void
JsonLinesCopyFromStart(CopyFromState cstate, TupleDesc tupDesc)
{
	cstate->input_buf = cstate->raw_buf;
	cstate->input_reached_eof = false;
	initStringInfo(&cstate->line_buf);
}

/*
 * Write a C-string representation of the given JsonbValue to 'str'.
 */
static void
GetJsonbValueAsCString(JsonbValue *v, StringInfo str)
{
	switch (v->type)
	{
		case jbvNull:
			/* must be handled by the caller */
			break;

		case jbvBool:
			appendStringInfoString(str, v->val.boolean ? "true" : "false");
			break;

		case jbvString:
			appendBinaryStringInfo(str, v->val.string.val, v->val.string.len);
			break;
		case jbvNumeric:
			{
				Datum		cstr;

				cstr = DirectFunctionCall1(numeric_out,
										   PointerGetDatum(v->val.numeric));

				appendStringInfoString(str, DatumGetCString(cstr));
				break;
			}

		case jbvBinary:
			(void) JsonbToCString(str, v->val.binary.data, v->val.binary.len);
			break;

		default:
			elog(ERROR, "unrecognized jsonb type: %d", (int) v->type);
	}

	return;
}

static bool
JsonLinesCopyFromOneRow(CopyFromState cstate, ExprContext *econtext, Datum *values,	bool *nulls)
{
	TupleDesc tupdesc = RelationGetDescr(cstate->rel);
	Jsonb	*jb;
	Datum	jsonb_data;
	ListCell	*lc;
	StringInfoData buf;
	bool	ret;

	if (JsonLineReadLine(cstate))
		return false;

	/* Convert the raw input line to a jsonb value */
	ret = DirectInputFunctionCallSafe(jsonb_in, cstate->line_buf.data,
									  JSONBOID, -1,
									  (Node *) cstate->escontext,
									  &jsonb_data);

	if (!ret)
		elog(ERROR, "invalid data for jsonb value");

	jb = DatumGetJsonbP(jsonb_data);

	initStringInfo(&buf);
	foreach(lc, cstate->attnumlist)
	{
		int	attnum = lfirst_int(lc);
		JsonbValue	*v;
		JsonbValue vbuf;
		Form_pg_attribute att = TupleDescAttr(tupdesc, attnum - 1);
		char	*attname = NameStr(att->attname);

		/* The jsonb value for the key with the column name */
		v = getKeyJsonValueFromContainer(&jb->root,
										 attname, strlen(attname), &vbuf);

		/*
		 * Fill with NULL if either not found or the value represent NULL.
		 */
		if (v == NULL || v->type == jbvNull)
		{
			nulls[attnum - 1] = true;
			continue;
		}

		nulls[attnum - 1] = false;

		/* Convert the jsonb value to cstring */
		GetJsonbValueAsCString(v, &buf);

		/* Convert the cstring data into the column */
		ret = InputFunctionCallSafe(&(cstate->in_functions[attnum - 1]),
									buf.data,
									cstate->typioparams[attnum - 1],
									att->atttypmod,
									(Node *) cstate->escontext,
									&values[attnum - 1]);

		if (!ret)
			elog(ERROR, "could not convert jsonb value \"%s\" to data for column \"%s\"",
				 buf.data, attname);

		resetStringInfo(&buf);
	}

	return true;
}

static void
JsonLinesCopyFromEnd(CopyFromState cstate)
{
	/* Nothing to do */
}

static void
JsonLinesCopyToOutFunc(CopyToState cstate, Oid atttypid, FmgrInfo *finfo)
{
	/* Nothing to do */
}

static void
JsonLinesCopyToStart(CopyToState cstate, TupleDesc tupDesc)
{
	/* Nothing to do */
}
static void
JsonLinesCopyToOneRow(CopyToState cstate, TupleTableSlot *slot)
{
	Datum	json_text;
	char	*str;

	/*
	 * Convert the whole row to json value using row_to_json() function.
	 */
	json_text = DirectFunctionCall1(row_to_json, ExecFetchSlotHeapTupleDatum(slot));

	str = text_to_cstring(DatumGetTextP(json_text));
	appendBinaryStringInfo(cstate->fe_msgbuf, str, strlen(str));
	appendStringInfoCharMacro(cstate->fe_msgbuf, '\n');

	/* End of row */
	CopyToStateFlush(cstate);
}
static void
JsonLinesCopyToEnd(CopyToState cstate)
{
	/* Nothing to do */
}

static const CopyToRoutine JsonLinesCopyToRoutine = {
	.type = T_CopyToRoutine,
	.CopyToOutFunc = JsonLinesCopyToOutFunc,
	.CopyToStart = JsonLinesCopyToStart,
	.CopyToOneRow = JsonLinesCopyToOneRow,
	.CopyToEnd = JsonLinesCopyToEnd,
};

static const CopyFromRoutine JsonLinesCopyFromRoutine = {
	.type = T_CopyFromRoutine,
	.CopyFromInFunc = JsonLinesCopyFromInFunc,
	.CopyFromStart = JsonLinesCopyFromStart,
	.CopyFromOneRow = JsonLinesCopyFromOneRow,
	.CopyFromEnd = JsonLinesCopyFromEnd,
};

Datum
jsonlines_handler(PG_FUNCTION_ARGS)
{
	bool	is_from = PG_GETARG_BOOL(0);

	if (is_from)
		PG_RETURN_POINTER(&JsonLinesCopyFromRoutine);
	else
		PG_RETURN_POINTER(&JsonLinesCopyToRoutine);
}
