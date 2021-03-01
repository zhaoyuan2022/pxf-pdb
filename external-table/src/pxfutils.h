#ifndef _PXFUTILS_H_
#define _PXFUTILS_H_

#include "postgres.h"

/* convert input string to upper case and prepend "X-GP-OPTIONS-" prefix */
char	   *normalize_key_name(const char *key);

/* get the name of the type, given the OID */
char	   *TypeOidGetTypename(Oid typid);

/* Concatenate multiple literal strings using stringinfo */
char	   *concat(int num_args,...);

/* Get authority (host:port) for the PXF server URL */
char	   *get_authority(void);

/* Returns the PXF Host defined in the PXF_HOST
 * environment variable or the default when undefined
 */
const char *get_pxf_host(void);

/* Returns the PXF Port defined in the PXF_PORT
 * environment variable or the default when undefined
 */
const int  get_pxf_port(void);

/* Returns the 128-bit trace id to be propagated
 * to the PXF Service
 */
char	   *GetTraceId(char* xid, char* filter, char* relnamespace, const char* relname, char* user);

/* Returns the 64-bit span id to be propagated
 * to the PXF Service
 */
char	   *GetSpanId(char* traceId, char* segmentId);

/* Returns the namespace (schema) name for a given namespace oid */
char	   *GetNamespaceName(Oid nsp_oid);

#define PXF_PROFILE       "PROFILE"
#define FRAGMENTER        "FRAGMENTER"
#define ACCESSOR          "ACCESSOR"
#define RESOLVER          "RESOLVER"
#define ANALYZER          "ANALYZER"
#define ENV_PXF_HOST      "PXF_HOST"
#define ENV_PXF_PORT      "PXF_PORT"
#define PXF_DEFAULT_HOST  "localhost"
#define PXF_DEFAULT_PORT  5888

#endif							/* _PXFUTILS_H_ */
