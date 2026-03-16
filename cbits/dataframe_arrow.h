#pragma once
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Execute a JSON-encoded query plan and return Arrow C Data Interface pointers.
 *
 * plan_json  – null-terminated UTF-8 JSON (see DataFrame.IR for schema)
 * schema_out – receives ArrowSchema* cast to uint64_t
 * array_out  – receives ArrowArray*  cast to uint64_t
 *
 * Returns 0 on success, -1 on error (message written to stderr).
 *
 * Plan ops: ReadCsv, ReadTsv, FromArrow, Select, GroupBy, Sort, Limit
 *
 * Example (GroupBy):
 *   {"op":"GroupBy","keys":["Sex"],
 *    "aggregations":[{"name":"n","agg":"count","col":"Sex"}],
 *    "input":{"op":"ReadCsv","path":"data/titanic.csv"}}
 */
int dfExecutePlan(const char* plan_json,
                  uint64_t*   schema_out,
                  uint64_t*   array_out);

#ifdef __cplusplus
}
#endif
