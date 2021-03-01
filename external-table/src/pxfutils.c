#include "pxfutils.h"

#if PG_VERSION_NUM >= 90400
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#endif
#include "catalog/pg_namespace.h"
#include "libpq/md5.h"
#include "utils/formatting.h"
#include "utils/syscache.h"

/*
 * Full name of the HEADER KEY expected by the PXF service
 * Converts input string to upper case and prepends "X-GP-OPTIONS-" string
 * This will be used for all user defined parameters to be isolate from internal parameters
 */
char *
normalize_key_name(const char *key)
{
	if (!key || strlen(key) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("internal error in pxfutils.c:normalize_key_name. Parameter key is null or empty.")));
	}

#if PG_VERSION_NUM >= 90400
	return psprintf("X-GP-OPTIONS-%s", asc_toupper(pstrdup(key), strlen(key)));
#else
	return psprintf("X-GP-OPTIONS-%s", str_toupper(pstrdup(key), strlen(key)));
#endif
}

/*
 * TypeOidGetTypename
 * Get the name of the type, given the OID
 */
char *
TypeOidGetTypename(Oid typid)
{

	Assert(OidIsValid(typid));

	HeapTuple	typtup = SearchSysCache(TYPEOID,
							ObjectIdGetDatum(typid),
							0, 0, 0);
	if (!HeapTupleIsValid(typtup))
		elog(ERROR, "cache lookup failed for type %u", typid);

	Form_pg_type typform = (Form_pg_type) GETSTRUCT(typtup);
	char	   *typname = psprintf("%s", NameStr(typform->typname));
	ReleaseSysCache(typtup);

	return typname;
}

/* Concatenate multiple literal strings using stringinfo */
char *
concat(int num_args,...)
{
	va_list		ap;
	StringInfoData str;

	initStringInfo(&str);

	va_start(ap, num_args);

	for (int i = 0; i < num_args; i++)
	{
		appendStringInfoString(&str, va_arg(ap, char *));
	}
	va_end(ap);

	return str.data;
}

/* Get authority (host:port) for the PXF server URL */
char *
get_authority(void)
{
	return psprintf("%s:%d", get_pxf_host(), get_pxf_port());
}

/* Returns the PXF Host defined in the PXF_HOST
 * environment variable or the default when undefined
 */
const char *
get_pxf_host(void)
{
	const char *hStr = getenv(ENV_PXF_HOST);
	if (hStr)
		elog(DEBUG3, "read environment variable %s=%s", ENV_PXF_HOST, hStr);
	else
		elog(DEBUG3, "environment variable %s was not supplied", ENV_PXF_HOST);
	return hStr ? hStr : PXF_DEFAULT_HOST;
}

/* Returns the PXF Port defined in the PXF_PORT
 * environment variable or the default when undefined
 */
const int
get_pxf_port(void)
{
	char *endptr = NULL;
	char *pStr = getenv(ENV_PXF_PORT);
	int port = PXF_DEFAULT_PORT;

	if (pStr) {
		port = (int) strtol(pStr, &endptr, 10);

		if (pStr == endptr)
			elog(ERROR, "unable to parse PXF port number %s=%s", ENV_PXF_PORT, pStr);
		else
			elog(DEBUG3, "read environment variable %s=%s", ENV_PXF_PORT, pStr);
	}
	else
	{
		elog(DEBUG3, "environment variable %s was not supplied", ENV_PXF_PORT);
	}

	return port;
}

/* Returns the 128-bit trace id to be propagated
 * to the PXF Service
 */
char *
GetTraceId(char* xid, char* filter, char* relnamespace, const char* relname, char* user)
{
	char	   *traceId,
			   *md5Hash;

	traceId = psprintf("%s:%s:%s:%s:%s", xid, filter, relnamespace, relname, user);
	elog(DEBUG3, "GetTraceId: generated traceId %s", traceId);

	md5Hash = palloc0(33);

	if (!pg_md5_hash(traceId, strlen(traceId), md5Hash))
	{
		elog(DEBUG3, "GetTraceId: Unable to calculate pg_md5_hash for traceId '%s'", traceId);
		return NULL;
	}

	elog(DEBUG3, "GetTraceId: generated md5 hash for traceId %s", md5Hash);

	return md5Hash;
}

/* Returns the 64-bit span id to be propagated
 * to the PXF Service
 */
char *
GetSpanId(char* traceId, char* segmentId)
{
	char	   *spanId,
			   *md5Hash,
			   *res;

	spanId = psprintf("%s:%s", traceId, segmentId);
	elog(DEBUG3, "GetSpanId: generated spanId %s", spanId);

	md5Hash = palloc0(33);
	res = palloc0(17);

	if (!pg_md5_hash(spanId, strlen(spanId), md5Hash))
	{
		elog(DEBUG3, "GetSpanId: Unable to calculate pg_md5_hash for spanId '%s'", spanId);
		return NULL;
	}

	strncpy(res, md5Hash, 16);
	elog(DEBUG3, "GetSpanId: generated md5 hash for spanId %s", res);
	pfree(md5Hash);

	return res;
}

/* Returns the namespace (schema) name for a given namespace oid */
char *
GetNamespaceName(Oid nsp_oid)
{
	HeapTuple	tuple;
	Datum		nspnameDatum;
	bool		isNull;

	tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsp_oid));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
						errmsg("schema with OID %u does not exist", nsp_oid)));

	nspnameDatum = SysCacheGetAttr(NAMESPACEOID, tuple, Anum_pg_namespace_nspname,
								   &isNull);

	ReleaseSysCache(tuple);

	return DatumGetCString(nspnameDatum);
}
