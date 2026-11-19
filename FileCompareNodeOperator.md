# DuckDBFileCompareNodeOperator — DuckDB Implementation Specification

Nov 16, 2026 · @Ameya Kalamkar

## 1. Overview

`DuckDBFileCompareNodeOperator` compares two local files (left vs right) inside one `execute()` call, driven entirely by a JSON config passed in as `Map<String, Object>`. It uses the DuckDB connection prepared by the existing setup, loads both files, runs the comparison as generated SQL, writes result files, returns a `NodeResult` with a verdict, and closes the connection.

**In scope**

- Input formats: CSV and Parquet only (single file or glob of files per side).
- Local file system only, for inputs and outputs.
- Two modes: `keyed` (join on key columns, column-level diffs) and `keyless` (whole-row multiset compare).
- Per-column comparison rules: exact, numeric tolerance, date, string normalisation, custom SQL.
- Schema check, key check, thresholds, profiling, diff sampling, optional HTML report.
- `${VARIABLE}` placeholders resolved by the existing variable-resolver util before the operator binds the config.

**Out of scope**

- S3 or any remote storage, JSON and fixed-width inputs.
- A separate `scope` block (filters go in `preTransform`).
- A separate `aggregate` mode (aggregation goes in `preTransform`; group-by columns become `keys`).
- An `engine` block: DuckDB resource settings are not part of the JSON config (the DuckDB connection and its settings are prepared before the job runs; Appendix A lists recommended values, not requirements).

## 2. Operator contract

The operator is a stateless Spring bean implementing the existing `NodeOperator` interface; every resource it uses is created and released inside one `execute()` call.

The operator follows the project's existing `NodeOperator`, `NodeContext`, `NodeResult`, `NodeStatus` and engine specs; the signature and types below are a sketch, and where they differ the existing specs win. The DuckDB connection, its settings (memory, threads, temp directory, storage) and the DuckDB version (2.0, JDBC 2.0.0.1) come from the existing setup that runs before the job. The job only issues SQL on that connection; Appendix A lists recommended settings as guidance, not requirements.

```java
@Component("duckdbFileCompare")
public class DuckDBFileCompareNodeOperator implements NodeOperator {

    @Override
    public NodeResult execute(NodeContext ctx, Map<String, Object> config) {
        // phases 0-9, section 4
    }
}
```

**Inputs**

- `NodeContext ctx`: node name, run id, logger context, variables available to the resolver.
- `Map<String, Object> config`: the raw JSON config, parsed by Jackson into nested `LinkedHashMap` / `ArrayList` / scalar values.

**Variable resolution happens first, on the raw map.** The existing variable-resolver util walks the map recursively and replaces `${VARIABLE}` in every `String` value (including strings inside lists and nested maps). Only after that is the map converted to typed config. The operator never sees a placeholder; an unresolved placeholder left in the map is a config error.

**Binding:** one Jackson call turns the resolved map into typed records.

```java
ObjectMapper mapper = new ObjectMapper()
    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
    .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

CompareConfig cfg = mapper.convertValue(resolvedMap, CompareConfig.class);
```

**Output:** a `NodeResult` carrying

- `status`: SUCCESS, WARN or FAILED, mapped to the project's existing `NodeStatus` values.
- `metrics`: the counts from `summary.json` (rows per side, per bucket, per column).
- `outputs`: map of output name to absolute file path, so downstream DAG nodes can pick them up.
- `error`: phase name plus message when the node fails.

**Concurrency:** the bean is a singleton but holds no fields that change per call, so the DAG executor can run several compare nodes in parallel; each call gets its own DuckDB connection.

## 3. Configuration reference

Only `left`, `right` and `output.dir` are always required; `keys` is required in `keyed` mode. Everything else has a default.

```json
{
  "name": "trades_recon_${BUSINESS_DATE}",
  "mode": "keyed",
  "dryRun": false,
  "typeMode": "native",

  "left": {
    "alias": "SRC_SYSTEM",
    "path": "${IN_DIR}/trades_${BUSINESS_DATE}_*.csv",
    "format": "csv",
    "multiFile": { "unionByName": true, "filenameColumn": "src_file" },
    "options": {
      "delim": "|",
      "header": true,
      "quote": "\"",
      "dateformat": "%d/%m/%Y",
      "compression": "gzip",
      "nullstr": ["", "NULL"],
      "allVarchar": false
    }
  },
  "right": {
    "alias": "TARGET_SYSTEM",
    "path": "${IN_DIR}/trades.parquet",
    "format": "parquet"
  },

  "preTransform": {
    "left":  "SELECT *, upper(trim(ccy)) AS ccy FROM src WHERE trade_date = DATE '${BUSINESS_DATE}'",
    "right": "SELECT * FROM src WHERE status <> 'CANCELLED' AND tradeDate = DATE '${BUSINESS_DATE}'"
  },

  "keys": ["trade_id", "leg_no"],
  "columnMapping": { "trade_dt": "tradeDate" },
  "columns": { "include": [], "exclude": ["load_ts", "batch_id"] },

  "defaultRule": { "type": "exact" },
  "rules": [
    { "column": "notional", "type": "numeric", "absTolerance": 0.01 },
    { "column": "price", "type": "numeric", "relTolerance": 0.0001 },
    { "column": "tradeDate", "type": "date", "formatLeft": "%d/%m/%Y", "formatRight": "%Y-%m-%d" },
    { "column": "cpty", "type": "string", "ignoreCase": true, "trim": true },
    { "column": "rate", "type": "custom",
      "expr": "abs(L.rate - R.rate) < 0.5 OR (L.rate IS NULL AND R.rate = 0)" }
  ],

  "nullEqualsNull": true,
  "fastPath": "hash",

  "schemaCheck": { "onMissingColumn": "fail", "onExtraColumn": "warn", "onTypeMismatch": "warn" },
  "keyCheck": { "onDuplicate": "fail", "onNullKey": "warn" },

  "thresholds": {
    "maxMismatchPct": 0.5,
    "maxMismatchCount": 1000,
    "maxLeftOnly": 0,
    "maxRightOnly": 0,
    "abortCandidatePct": 20,
    "onBreach": "fail"
  },

  "profiling": { "enabled": true, "metrics": ["rowCount", "nullCount", "distinctCount", "min", "max"] },
  "sampling": { "diffSampleRows": 50 },

  "output": {
    "dir": "${OUT_DIR}/${BUSINESS_DATE}",
    "summary": "summary.json",
    "diffs": { "path": "diffs.parquet", "format": "parquet", "maxRows": 100000 },
    "leftOnly": { "path": "left_only.parquet", "format": "parquet" },
    "rightOnly": { "path": "right_only.parquet", "format": "parquet" },
    "duplicates": { "path": "dup_keys.csv", "format": "csv" },
    "report": { "enabled": true, "path": "report.html" },
    "includeMatched": false,
    "overwrite": true
  }
}
```

### Field reference

| Field | Type | Default | Meaning and validation |
| --- | --- | --- | --- |
| `name` | string | node name | Label used in logs and `summary.json` |
| `mode` | `keyed` \| `keyless` | `keyed` | `keyed` needs `keys`; `keyless` ignores `keys`, `rules`, `keyCheck` and warns if they are set |
| `dryRun` | boolean | `false` | Build and log all SQL, read only file schemas, write nothing |
| typeMode | native \| text | native | text = read and compare every column as VARCHAR, skip the type check; best for two CSVs with identical formatting (5.3.2) |
| `left` / `right` `.alias` | string | `LEFT` / `RIGHT` | Display name in summary and report |
| `left` / `right` `.path` | string | required | Local file, glob or directory (a directory expands to \*.csv or \*.parquet by format); at least one file must match |
| `left` / `right` `.format` | `csv` \| `parquet` | required | Anything else is a config error |
| `.multiFile.unionByName` | boolean | `false` | Align columns by name across globbed files |
| `.multiFile.filenameColumn` | string | none | Adds a column holding the source file name; auto-excluded from compare |
| `.options` (csv) | map | DuckDB auto-detect | Passed to `read_csv`: `delim`, `header`, `quote`, `escape`, `dateformat`, `timestampformat`, `compression`, `nullstr`, `allVarchar`, `skip`, `columns`, `types` |
| `.options` (parquet) | map | none | Not used in v1; rejected if present |
| `preTransform.left` / `.right` | SQL string, or `{ "sqlFile": "path" }` | `SELECT * FROM src` | Normalisation SQL supplied from outside, one per side, that brings both files to the same shape (5.3.1). One `SELECT` reading from `src`; CTEs allowed |
| `keys` | string list | required in keyed | Column names after mapping; must exist on both sides |
| `columnMapping` | map | empty | Left column name → right column name; applied to the left side only |
| `columns.include` | string list | empty = all | Compare only these (keys are always kept) |
| `columns.exclude` | string list | empty | Never compare these; wins over `include` |
| `defaultRule` | rule | `{type: exact}` | Rule for every compared column without its own rule (no `column` field) |
| `rules[]` | rule list | empty | One rule per column; duplicate `column` is a config error (section 6) |
| `nullEqualsNull` | boolean | `true` | Whether NULL on both sides counts as a match (not applied to `custom`) |
| `fastPath` | `hash` \| `none` | `hash` | Row-hash pre-check in keyed mode |
| `schemaCheck.*` | `fail` \| `warn` \| `ignore` | `fail`, `warn`, `warn` | Missing column / extra column / type mismatch |
| `keyCheck.onDuplicate` | `fail` \| `warn` | `fail` | `warn` routes duplicate-key rows to the duplicates bucket and compares the rest |
| `keyCheck.onNullKey` | `fail` \| `warn` | `warn` | Null-key rows are never joined; they are counted and written with duplicates |
| `thresholds.*` | number | unset = no limit | Limits checked after compare (abortCandidatePct is checked after pass 1); `maxMismatchPct` is a percent (0.5 = 0.5%) |
| `thresholds.onBreach` | `fail` \| `warn` | `fail` | Status when any limit is breached |
| `profiling.enabled` / `.metrics` | boolean / list | `false` / all five | Per-column stats per side in `summary.json` |
| `sampling.diffSampleRows` | int | `50` | Diff rows copied into `summary.json` |
| `output.dir` | string | required | Created if missing; all output paths are relative to it |
| `output.summary` | string | `summary.json` | Always written, also on failure |
| `output.diffs` / `leftOnly` / `rightOnly` / `duplicates` | `{path, format, maxRows?}` | written when non-empty, parquet | `format`: `parquet` \| `csv`; `maxRows` caps the file; parallelWrite: true writes a folder of files in parallel |
| `output.report` | `{enabled, path}` | disabled | Static HTML report |
| `output.includeMatched` | boolean | `false` | Also writes `matched.parquet` |
| `output.overwrite` | boolean | `true` | `false` fails in phase 0 if any target file already exists |

### Config validation

Validation runs in two stages and reports **every** error found in a stage at once, not just the first; any error makes the node FAILED and is listed in `summary.json` under `configErrors` with its JSON path (for example `rules[3].absTolerance`).

**Stage 1: at bind time, before any DuckDB statement**

| Area | Rule |
| --- | --- |
| JSON | Unknown fields rejected (`FAIL_ON_UNKNOWN_PROPERTIES`); duplicate keys rejected (`STRICT_DUPLICATE_DETECTION`); no unresolved `${...}` left after variable resolution |
| Required | `left`, `right`, `output.dir` always; `keys` in keyed mode |
| Enums | `mode`, `typeMode`, `format`, `fastPath`, rule `type`, policy values (`fail` / `warn` / `ignore`), `onBreach`, `profiling.metrics` only from their allowed lists |
| Sources | `format` is `csv` or `parquet`; `options` only for csv; `path` is a local file, glob or directory and matches at least one file |
| `preTransform` | Inline SQL or `sqlFile` (file exists, readable); exactly one `SELECT` reading `src`, checked with DuckDB's parser (`json_serialize_sql`) |
| `keys` | Not empty in keyed mode; no duplicate names; none listed in `columns.exclude` |
| `columnMapping` | No two left columns mapped to the same name; no mapping onto a name that already exists on the left |
| `columns` | `include` and `exclude` entries are non-blank names; overlap allowed (exclude wins) |
| Rules | One rule per column; fields must belong to the rule's type (for example `absTolerance` on a `string` rule is an error); tolerances ≥ 0; `date` formats non-blank; `custom.expr` is a single expression that references only `L.` and `R.`, checked by parsing `SELECT <expr>` with DuckDB; `defaultRule` has no `column` |
| Thresholds | All values ≥ 0; `maxMismatchPct` and `abortCandidatePct` between 0 and 100 |
| Sampling | `diffSampleRows` between 0 and 1,000 |
| Outputs | Paths relative to `output.dir`, no `..`; no two outputs with the same path; `maxRows` > 0; `overwrite: false` fails if any target already exists |
| Keyless | `keys`, `rules`, `keyCheck` set in keyless mode → warning in summary, not an error |

**Stage 2: at schema time, on the staging views (`DESCRIBE`, before the full read)**

| Area | Rule |
| --- | --- |
| Keys | Every key column exists on both sides; key types are compatible on both sides (a `VARCHAR` key against a `BIGINT` key is an error that suggests a cast in `preTransform`) |
| Columns | Every `columns.include` name exists after `preTransform`; in keyed mode at least one compare column remains |
| Rules | Every rule column is a compare column (not missing, excluded or a key) |
| Values | `numeric` rule columns cast to DECIMAL and `date` rule formats parse, both probed on a 1,000-row sample (`USING SAMPLE 1000 ROWS`) |
| Schema policies | Missing / extra / type-mismatch findings applied with their `schemaCheck` policies |
| Keyless size | `keylessMaxRows` checked (Parquet from metadata here; CSV right after materialising) |

## 4. Execution lifecycle

`execute()` runs ten phases in a fixed order; nothing reads the full files until the schema check has passed, and only rows whose hash differs reach the column rules.

&#91;embedded content: execute() lifecycle · 10 phases + cleanup\]

Phases 0 to 4 work on config and file metadata only, so config, path and schema errors surface in seconds. Phases 5 to 9 are where time goes at 20M rows; section 5 gives the SQL for each, and section 5.13 the tuning.

## 5. DuckDB implementation

Every step is SQL generated by `ComparePlanBuilder` and run on the connection provided by the existing setup, inside the job's work schema; each side is read from disk exactly once and materialised as a table. Identifiers are always double-quoted and string literals have `'` doubled, so column names with spaces or mixed case are safe.

### 5.1 Connection and work schema

```java
// connection comes from the existing setup: already configured, not opened or tuned here
String schema = "cmp_" + ctx.runId();
st.execute("CREATE SCHEMA \"" + schema + "\"");
st.execute("USE \"" + schema + "\"");
try {
    // phases ...
} finally {
    st.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
}
```

- The job never opens, configures or tunes DuckDB: no `SET` or `PRAGMA` for memory, threads, temp directory or storage. The existing setup does that before the job runs (Appendix A).
- Every object the job creates (views, tables) lives in the work schema `cmp_<runId>`, dropped in `finally`; nothing else on the connection is touched. Object names in this section are unqualified because of `USE`.
- Correctness never depends on row order, so the job works whether `preserve_insertion_order` is on or off.
- Target engine: DuckDB 2.0 (JDBC 2.0.0.1). The SQL uses the 2.0 lambda form `lambda x: ...`; asynchronous I/O for CSV and Parquet reads is automatic in 2.0 and needs nothing from the job.

### 5.2 Ingest

One raw view per side. CSV options map one-to-one to `read_csv` parameters (`allVarchar` → `all_varchar`); `filename=true` is added only when `filenameColumn` is set.

```sql
CREATE VIEW src_left AS
SELECT * RENAME (filename AS "src_file")
FROM read_csv('/in/trades_2026-11-16_*.csv',
              delim='|', header=true, quote='"', dateformat='%d/%m/%Y',
              compression='gzip', nullstr=['', 'NULL'], all_varchar=false,
              union_by_name=true, filename=true);

CREATE VIEW src_right AS
SELECT * FROM read_parquet('/in/trades.parquet');
```

A glob that matches no file fails here with a clear "no files found for left.path" error.

**Directories and multiple files.** `path` may be a file, a glob (`/in/trades_2026-11-16/*.csv`, `**` for subfolders) or a directory. A directory is expanded to `<dir>/*.csv` or `<dir>/*.parquet` according to `format`, so marker files such as `_SUCCESS` or `.crc` are never read. All matching files are read as one dataset, in parallel; each CSV header is skipped. For files with an identical layout leave `multiFile.unionByName` off: it is only needed when column order or sets differ, and it costs an extra schema read per file. `summary.json` reports the file count per side.

### 5.3 Pre-transform, mapping and column selection

The user's `preTransform` SQL is wrapped, not string-edited: `src` is supplied as a CTE, and the user SQL runs as a subquery, so it may contain its own `WITH`. Mapping and column selection use DuckDB star modifiers in the same statement. The result is a **view**: no data is read until the schema check has passed (5.5).

```sql
CREATE VIEW L_v AS
WITH src AS (SELECT * FROM src_left)
SELECT * EXCLUDE ("load_ts", "batch_id", "src_file")
         RENAME ("trade_dt" AS "tradeDate")
FROM ( SELECT *, upper(trim(ccy)) AS ccy FROM src
       WHERE trade_date = DATE '2026-11-16' ) t;

CREATE VIEW R_v AS
WITH src AS (SELECT * FROM src_right)
SELECT * EXCLUDE ("load_ts", "batch_id")
FROM ( SELECT * FROM src WHERE status <> 'CANCELLED'
       AND tradeDate = DATE '2026-11-16' ) t;
```

- `RENAME` (from `columnMapping`) is applied to L only.
- With `columns.include` set, the star is replaced by an explicit list: keys + included columns.
- Excluded names that do not exist on a side are dropped from that side's `EXCLUDE` list (DuckDB errors on unknown names) and logged.
- The filename column is excluded from the compare but kept in a side table if needed for diagnostics.

#### 5.3.1 Externally supplied normalisation SQL

`preTransform` is the single place where the two files are brought to the same level: the caller supplies one SQL per side, and the operator only wraps and runs it. After it, both sides must have the same key columns and the same compare column names, with types that the rules can compare.

```json
"preTransform": {
  "left":  { "sqlFile": "${SQL_DIR}/trades_src_normalise.sql" },
  "right": "SELECT trade_ref AS trade_id, leg AS leg_no, CAST(amt AS DECIMAL(18,2)) AS notional FROM src"
}
```

**Contract for the supplied SQL**

- Exactly one statement, a `SELECT` (CTEs allowed), reading from `src`. DDL, `COPY`, `ATTACH`, `INSTALL`, `SET` and multiple statements are rejected at bind time.
- It may rename, cast, trim, derive, filter, union-split, deduplicate and `GROUP BY`. Typical uses: align column names and types, convert date formats, map codes, aggregate to a common grain.
- It must not reference the other side; each side is normalised on its own.
- Output column names become the names used by `keys`, `rules` and `columns`.

**How it is loaded and checked**

- An inline string or a `sqlFile` path (local, read as UTF-8). `${VARIABLE}` placeholders inside the file are resolved by the same variable-resolver util as the config.
- One trailing `;` is stripped. The statement check uses DuckDB's own parser (`json_serialize_sql`), not a regex.
- Errors in the SQL surface in the schema phase (`DESCRIBE L_v`), before any full read, with the side and the SQL file named.
- `summary.json` records the SQL file path and a SHA-256 of the final SQL text, so a result can be traced to the exact normalisation used.

**What this simplifies**

- `columnMapping`, `columns.include` and `columns.exclude` stay as conveniences, but everything they do can be written in the supplied SQL; when both are used, the SQL runs first.
- Most `string` and `date` rules can move into the SQL as `trim`, `upper` or `strptime`, which leaves the rules for tolerances. Moving them keeps the row hash effective: values normalised in SQL hash equal, so fewer rows become candidates for pass 2.

**Performance:** the SQL runs inside the single materialise read (5.5), so it adds no extra pass over the files. A `GROUP BY` or `DISTINCT` on 20M rows still costs a hash aggregation, which spills to the temp directory when needed.

#### 5.3.2 typeMode: native or text

`typeMode` decides whether values are converted to typed values (dates, decimals, integers) on read; the schema type check itself costs seconds and is not what this saves.

|  | `native` (default) | `text` |
| --- | --- | --- |
| CSV read | Types inferred or given; values converted | `all_varchar = true`; no conversion |
| Parquet read | Types as stored | Every column cast `::VARCHAR` in the staging view (costly at scale; logged as a warning) |
| Schema type check | Applied with its policy | Skipped; only missing / extra columns are checked |
| Row hash | Typed values; mixed-type columns hashed as `VARCHAR` on both sides (5.5) | All values as text |
| `numeric` / `date` rules | Work on typed values | Cast inside the rule, so only candidate rows are converted |
| Best for | CSV vs Parquet, or sides that format values differently | Two CSVs from systems that format values identically |

In `text` mode, `1000.5` and `1000.50` hash differently, so formatting differences turn into pass 2 candidates; a numeric rule still matches them, at pass 2 cost. If formatting differs widely, use `native`, or normalise formats in `preTransform`.

### 5.4 Schema check

```sql
DESCRIBE L_v;   -- column_name, column_type, ...
DESCRIBE R_v;
```

Java builds three lists from the two results: missing (in L not R), extra (in R not L) and type mismatches. Name matching is case-insensitive, like DuckDB identifiers. Each list is applied with its policy; `fail` stops the run after writing `summary.json`. `DESCRIBE` on these views runs only the CSV sniffer or reads Parquet metadata, so a schema failure surfaces in seconds, before any full read. The **compare columns** are the columns on both sides, minus keys and excluded columns, sorted by name. A missing key column is always a config error.

### 5.5 Materialise with a row hash

After the schema check passes, each side is read from disk once into a table, with a 64-bit hash over the compare columns in a fixed, sorted order.

```sql
CREATE TABLE L AS
SELECT *, hash("amount", "book", "ccy", "cpty", ..., "price") AS __h   -- all compare columns, sorted
FROM L_v;

CREATE TABLE R AS
SELECT *, hash("amount", "book", "ccy", "cpty", ..., "price") AS __h
FROM R_v;
```

- `hash()` is DuckDB's built-in 64-bit hash, far cheaper than `md5(to_json(...))` across 500 columns. Argument position is part of the hash, and NULL hashes differently from `'NULL'` and `''`.
- A false match needs a collision between the two rows of the same key; at 64 bits that is about 1 in 1.8 × 10^19 per pair, which is accepted. `fastPath: none` turns it off.
- The shortcut is switched off automatically when `nullEqualsNull = false` (two NULLs hash equal but must mismatch). A `custom` rule that rejects equal values is not supported with the fast path; this is documented.
- DuckDB tables are columnar, so later steps that name only keys and `__h` never read the 500 value columns.

**Mixed types:** a compare column whose type differs between the sides (for example `VARCHAR` left, `BIGINT` right) is hashed as `"col"::VARCHAR` on both sides. Without this, equal values would hash differently and every row would become a pass 2 candidate.

```sql
-- price is DOUBLE on the left, VARCHAR on the right
CREATE TABLE L AS SELECT *, hash("amount", "ccy", "price"::VARCHAR, ...) AS __h FROM L_v;
CREATE TABLE R AS SELECT *, hash("amount", "ccy", "price"::VARCHAR, ...) AS __h FROM R_v;
```

**Key types:** joins on `BIGINT` / `INTEGER` keys are faster than on `VARCHAR`. Where a key is numeric in the source, cast it in `preTransform`.

### 5.6 Key check (keyed mode)

```sql
CREATE TABLE dup_keys AS
SELECT 'LEFT' AS side, "trade_id", "leg_no", count(*) AS cnt
FROM L GROUP BY ALL HAVING count(*) > 1
UNION ALL
SELECT 'RIGHT', "trade_id", "leg_no", count(*)
FROM R GROUP BY ALL HAVING count(*) > 1;

SELECT count(*) FROM L WHERE "trade_id" IS NULL OR "leg_no" IS NULL;  -- same for R
```

No filtered copy of L or R is made: null keys and (with `onDuplicate = warn`) duplicate keys are removed by anti-joins on the narrow inputs of pass 1.

#### 5.6.1 Identical-data shortcut

When both sides hold exactly the same rows, pass 1, pass 2 and the diffs are skipped. Two narrow scans decide it:

```sql
SELECT count(*) AS n, sum(hash("trade_id", "leg_no", __h)::HUGEINT) AS chk FROM L;
SELECT count(*) AS n, sum(hash("trade_id", "leg_no", __h)::HUGEINT) AS chk FROM R;
```

- Applies in keyed mode when the fast path is on, there are no duplicate keys and no null keys on either side.
- Equal `n` and equal `chk` → every row is MATCH; the summary records `shortcut: identical` and the left-only, right-only and diff outputs are not written.
- The checksum is order-independent, so files written in a different row order still qualify. A false "identical" needs a 64-bit collision across the whole sum, accepted like the row hash.
- When the data differs, the cost is two scans of three narrow columns, typically seconds at 20M rows.

### 5.7 Pass 1: classify on keys + hash

The full outer join carries only the keys and the 8-byte hash per row, never the value columns.

```sql
CREATE TABLE cls AS
WITH lk AS (
  SELECT "trade_id", "leg_no", __h FROM L
  ANTI JOIN (SELECT "trade_id", "leg_no" FROM dup_keys WHERE side = 'LEFT') d
       USING ("trade_id", "leg_no")
  WHERE "trade_id" IS NOT NULL AND "leg_no" IS NOT NULL
), rk AS (
  SELECT "trade_id", "leg_no", __h FROM R
  ANTI JOIN (SELECT "trade_id", "leg_no" FROM dup_keys WHERE side = 'RIGHT') d
       USING ("trade_id", "leg_no")
  WHERE "trade_id" IS NOT NULL AND "leg_no" IS NOT NULL
)
SELECT "trade_id", "leg_no",
  CASE WHEN lk.__h IS NULL   THEN 'RIGHT_ONLY'
       WHEN rk.__h IS NULL   THEN 'LEFT_ONLY'
       WHEN lk.__h = rk.__h  THEN 'MATCH'
       ELSE 'CANDIDATE' END AS __status
FROM lk FULL OUTER JOIN rk USING ("trade_id", "leg_no");
```

### 5.8 Pass 2: rule compare on candidates only

Only CANDIDATE keys are joined back to the wide tables. Each compare column becomes one struct in a list, and the list is filtered to the columns that differ, so the 500 rule predicates run in a single scan.

```sql
CREATE TABLE cand AS
SELECT c."trade_id", c."leg_no",
  list_filter([
    CASE WHEN NOT <eq_notional>
         THEN {'col': 'notional', 'l': L."notional"::VARCHAR, 'r': R."notional"::VARCHAR} END,
    CASE WHEN NOT <eq_price>
         THEN {'col': 'price',    'l': L."price"::VARCHAR,    'r': R."price"::VARCHAR} END,
    ...   -- one CASE per compare column
  ], lambda x: x IS NOT NULL) AS __diffs
FROM (SELECT "trade_id", "leg_no" FROM cls WHERE __status = 'CANDIDATE') c
JOIN L ON L."trade_id" = c."trade_id" AND L."leg_no" = c."leg_no"
JOIN R ON R."trade_id" = c."trade_id" AND R."leg_no" = c."leg_no";

CREATE VIEW cmp AS
SELECT "trade_id", "leg_no", __status FROM cls WHERE __status <> 'CANDIDATE'
UNION ALL
SELECT "trade_id", "leg_no",
       CASE WHEN len(__diffs) = 0 THEN 'MATCH' ELSE 'MISMATCH' END
FROM cand;
```

The `CASE` builds a diff entry only for columns that differ, so the 500 `VARCHAR` casts run only where there is a difference, not for every column of every candidate.

- A candidate whose differences are all within tolerance ends with an empty list and counts as MATCH.
- `lambda x: x.d` is the `DuckDB 2.0 lambda syntax; the arrow form is disabled by default in 2.0`.
- Pass 2 cost grows with the number of candidates, not with file size. If most rows differ (a wrong mapping or date filter), pass 2 approaches a full wide join; `thresholds.abortCandidatePct` (section 7) stops before it.

Bucket and per-column counts:

```sql
-- all bucket counts in one pass over cmp
SELECT count_if(__status = 'MATCH')      AS matched,
       count_if(__status = 'MISMATCH')   AS mismatched,
       count_if(__status = 'LEFT_ONLY')  AS left_only,
       count_if(__status = 'RIGHT_ONLY') AS right_only
FROM cmp;

-- per-column counts in one pass over diffs
SELECT column_name, count(*) FROM diffs GROUP BY ALL;
```

### 5.9 Column-level diffs

One `unnest` over the candidate table, instead of one query per column:

```sql
CREATE TABLE diffs AS
SELECT "trade_id", "leg_no",
       d.col AS column_name, d.l AS left_val, d.r AS right_val
FROM (SELECT "trade_id", "leg_no", unnest(__diffs) AS d FROM cand);
```

The table holds every diff, so `summary.json` reports true totals; `output.diffs.maxRows` is applied only when the file is written (5.12).

### 5.10 Keyless mode

Keyless mode is for limited data only, such as reference extracts or regression outputs, not the 20M-row files. A guard enforces this: an operator property `keylessMaxRows` (default 1,000,000 rows per side). For Parquet, the row count is read from file metadata before any data is read; for CSV, it is checked right after materialising. Above the limit, the node fails with a message to use keyed mode.

Both sides are projected onto the same ordered column list (compare columns sorted by name); a column whose types differ is cast to `VARCHAR` on both sides. Rules are not applied; normalisation belongs in `preTransform`.

```sql
CREATE TABLE left_only AS
SELECT "amount", "book", "ccy" FROM L
EXCEPT ALL
SELECT "amount", "book", "ccy" FROM R;

CREATE TABLE right_only AS
SELECT "amount", "book", "ccy" FROM R
EXCEPT ALL
SELECT "amount", "book", "ccy" FROM L;
```

Matched rows = rows(L) − rows(left\_only). There are no MISMATCH or duplicate buckets in this mode.

### 5.11 Profiling

```sql
SUMMARIZE L;   -- and SUMMARIZE R
```

`SUMMARIZE` returns per column: min, max, approx\_unique, null\_percentage and count. Java keeps the requested metrics and converts null percentage to a null count. `distinctCount` is approximate; this is stated in the summary.

### 5.12 Writing outputs

```sql
COPY (SELECT * FROM diffs LIMIT 100000)          -- output.diffs.maxRows
  TO '/out/2026-11-16/diffs.parquet' (FORMAT parquet, COMPRESSION zstd);

COPY (SELECT * FROM dup_keys)
  TO '/out/2026-11-16/dup_keys.csv' (FORMAT csv, HEADER true);

COPY (SELECT * EXCLUDE (__h) FROM L
      SEMI JOIN (SELECT "trade_id", "leg_no" FROM cls WHERE __status = 'LEFT_ONLY') o
      USING ("trade_id", "leg_no"))
  TO '/out/2026-11-16/left_only.parquet' (FORMAT parquet);
```

- Left-only and right-only files carry the full source rows, not just keys.
- `matched.parquet` is written from `cmp` only when `includeMatched = true`.
- A file is skipped when its table is empty, and the summary records it as `null` rather than a path.
- `summary.json` and `report.html` are written by Java (Jackson and a simple template), not by DuckDB.

**Parallel writes (optional).** Any output file may set `parallelWrite: true`, which adds `PER_THREAD_OUTPUT true` to its `COPY`. The output becomes a folder of files written in parallel instead of one file. Off by default; useful only when a left-only, right-only or matched output is very large.

### 5.13 Performance at 500 columns × 20M rows

The design reads each file once, runs the full join on keys + hash only, and touches the 500 value columns only for rows whose hash differs. The rest of the cost is set by the points below.

| Area | Do this | Why |
| --- | --- | --- |
| CSV typing | Give `types` (or `columns`) for wide CSVs, or `allVarchar: true` plus casts in `preTransform` | The sniffer samples early rows; a late value that breaks an inferred type fails after minutes of reading |
| CSV reading | Prefer several uncompressed or Parquet files over one large gzip file | gzip decompresses on one thread; a glob of files is read in parallel |
| Upstream format | Parquet where the producer allows it | Parsing 500 CSV columns is usually the largest single cost |
| Profiling | Off by default; when on, `SUMMARIZE (FROM L USING SAMPLE 1%)` | Full `SUMMARIZE` over 500 × 20M is a full extra scan with distinct estimation |
| Early abort | `thresholds.abortCandidatePct` checked between pass 1 and pass 2 | A broken mapping makes nearly every row a candidate; fail fast instead of a full wide join |
| Outputs | `maxRows` on diffs, `includeMatched: false`, no `ORDER BY` | Sorting or writing 20M matched rows costs more than the compare |
| Concurrency | Cap large compare nodes at one or two per pod in the DAG executor | Two concurrent compares of this size compete for the same memory and disk |
| Keyless mode | Use only for limited data; `keylessMaxRows` guard fails larger inputs | `EXCEPT ALL` over every column hashes and spills every row; keyed mode is the path for large files |

**Parallelism the job controls.** Each query already uses every thread the setup gives DuckDB, so the job never runs phases side by side. What it can influence is the input: a directory of several files, or one uncompressed CSV, is read in parallel, while a single gzip file decompresses on one thread. Parquet is scanned in parallel by row group, so a file with only a few very large row groups limits parallelism; ask producers for moderate row-group sizes. Thread, memory and disk settings are part of the DuckDB setup (Appendix A).

**To decide by benchmark (Phase 2), not by assumption:**

- **Materialise or re-read for Parquet.** Copying both sides into tables writes about 2 × 20M × 500 values. For CSV it pays off (one parse). For Parquet on both sides, reading the files twice (pass 1 for the hash, pass 2 for candidates only) may be cheaper. If it is, add `materialise: auto`.
- **`typeMode` text vs native** on the real CSV pair.
- **Key-bucket fallback, only if needed.** If pass 1 spills heavily on the target pod, split the keys into N buckets by `hash(keys) % N` and run pass 1 and pass 2 per bucket to cap memory. Not built unless the benchmark shows it is needed.

**Benchmark before committing numbers.** Generate a synthetic pair with DuckDB itself (`range(20000000)` plus 500 generated columns, about 1% of rows altered on the right), run it on the target pod size, and use the per-phase timings from the log (section 9) to find the slowest phase.

## 6. Rule renderers

Each rule type is a `RuleRenderer` bean that turns one rule into a SQL predicate `<eq_col>` over `L."col"` and `R."col"`; the predicate is true when the two values count as equal. Every predicate except `custom` is wrapped for NULLs and for unexpected NULL results.

```java
public interface RuleRenderer {
    String type();                                   // "numeric", "date", ...
    String render(Rule rule, ColumnPair cols);       // raw equality predicate
    void validate(Rule rule, ColumnPair cols);       // types, required fields
}
```

**NULL wrapper** (applied by `ComparePlanBuilder`, not by each renderer):

```sql
-- nullEqualsNull = true
coalesce( (L.c IS NULL AND R.c IS NULL)
          OR (L.c IS NOT NULL AND R.c IS NOT NULL AND <raw>), false )

-- nullEqualsNull = false
coalesce( L.c IS NOT NULL AND R.c IS NOT NULL AND <raw>, false )
```

The outer `coalesce(..., false)` makes a predicate that evaluates to NULL (for example, a date that fails to parse) count as a mismatch rather than silently passing.

### Raw predicates by type

| Type | Fields | Raw predicate |
| --- | --- | --- |
| `exact` | none | Same type on both sides: `L.c = R.c`. Types differ: `L.c::VARCHAR = R.c::VARCHAR` |
| `numeric` | `absTolerance`, `relTolerance` (either, both, or neither) | `abs(L.c::DECIMAL(38,10) - R.c::DECIMAL(38,10)) <= abs` OR `<= rel * greatest(abs(L.c), abs(R.c))`; neither = exact numeric equality |
| `date` | `formatLeft`, `formatRight` (optional) | `try_strptime(L.c::VARCHAR, fmtL)::DATE = try_strptime(R.c::VARCHAR, fmtR)::DATE`; no format = `try_cast(c AS DATE)` |
| `string` | `trim`, `ignoreCase` | `lower(trim(L.c::VARCHAR)) = lower(trim(R.c::VARCHAR))`, each function only when its flag is set |
| `custom` | `expr` | `expr` as written, using aliases `L` and `R`; no NULL wrapper |

### Rules for rules

- `defaultRule` applies to every compare column that has no rule; when absent it is `{type: exact}`. It has no `column` field.
- Two rules for the same column is a config error.
- A rule whose column is not a compare column (missing, excluded or a key) is a config error, so typos fail in phase 0 rather than being ignored.
- `numeric` on a column that cannot cast to DECIMAL fails in the schema phase via a `try_cast` probe on a sample (`USING SAMPLE 1000 ROWS`), not after the full read.
- There is no `regex` rule type. Extracting part of a value (for example an ISIN from `ISIN:US0378331005`) belongs in `preTransform` with `regexp_extract`, so the cleaned value also feeds the row hash.
- `custom` expressions are raw SQL from a trusted config. They may reference any column of L or R, not only their own.
- New rule types are added as one new `RuleRenderer` bean; nothing else changes.

## 7. Outputs and verdict

Every run writes `summary.json`, even a failed one; the other files are written only when they have rows, and the verdict comes from schema policy, key policy and thresholds, in that order.

### Files

| Output | Content | Written when |
| --- | --- | --- |
| `summary.json` | Counts, schema and key findings, per-column mismatch counts, threshold results, diff sample, profiling, timings, verdict | Always |
| `diffs` | key columns, `column_name`, `left_val`, `right_val` (values as text) | Keyed mode, at least one mismatch |
| `leftOnly` / `rightOnly` | Full rows from that side, all columns, `__h` removed | At least one such row |
| `duplicates` | `side`, key columns, `cnt`; null-key rows counted in the summary | Duplicates or null keys found |
| `matched` | Key columns of matched rows | `includeMatched = true` |
| `report` | Static HTML: summary tables plus the diff sample | `report.enabled = true` |

### summary.json

```json
{
  "name": "trades_recon_2026-11-16",
  "mode": "keyed",
  "status": "FAILED",
  "failedPhase": null,
  "left":  { "alias": "SRC_SYSTEM",    "files": 3, "rows": 20000000 },
  "right": { "alias": "TARGET_SYSTEM", "files": 1, "rows": 19999990 },
  "schema": {
    "compareColumns": 498,
    "missingInRight": [], "extraInRight": ["audit_user"],
    "typeMismatches": [{ "column": "price", "left": "DOUBLE", "right": "DECIMAL(18,6)" }]
  },
  "keys": { "duplicateLeft": 0, "duplicateRight": 0, "nullLeft": 0, "nullRight": 0 },
  "counts": {
    "matched": 19950000, "mismatched": 45000,
    "leftOnly": 5000, "rightOnly": 4990, "candidates": 61000
  },
  "mismatchByColumn": { "notional": 30000, "cpty": 15000 },
  "diffCount": 52000,
  "thresholds": [
    { "name": "maxMismatchPct", "limit": 0.5,  "actual": 0.2251, "breached": false },
    { "name": "maxLeftOnly",    "limit": 0,    "actual": 5000,   "breached": true }
  ],
  "diffSample": [
    { "trade_id": "T100", "leg_no": 1, "column": "notional", "left": "1000.00", "right": "1000.50" }
  ],
  "profiling": null,
  "timingsMs": { "ingest": 0, "schema": 0, "materialise": 0, "pass1": 0, "pass2": 0, "write": 0 },
  "outputs": {
    "diffs": "/out/2026-11-16/diffs.parquet",
    "leftOnly": "/out/2026-11-16/left_only.parquet",
    "rightOnly": "/out/2026-11-16/right_only.parquet",
    "duplicates": null
  }
}
```

The numbers above are illustrative. `mismatched` counts rows; `diffCount` counts cells, so one row can hold several diffs.

### Thresholds

- `maxMismatchPct` = mismatched rows ÷ rows whose key exists on both sides × 100.
- `maxMismatchCount`, `maxLeftOnly`, `maxRightOnly` compare against the raw counts.
- `abortCandidatePct` = candidates ÷ keys on both sides × 100, checked after pass 1; on breach, pass 2 is skipped and the summary says so.
- An unset threshold is not checked. All breached thresholds are listed, not only the first.

### Verdict

| Condition | Status |
| --- | --- |
| Config invalid, file missing, SQL error | FAILED, `failedPhase` set |
| Schema or key finding with policy `fail` | FAILED |
| Any threshold breached, `onBreach = fail` | FAILED |
| Any threshold breached, `onBreach = warn`, or any `warn` finding | WARN |
| Otherwise | SUCCESS |

Differences alone never fail the node: without thresholds, a compare that finds diffs is SUCCESS with the diffs reported. The `NodeResult` carries the status, the `counts` map as metrics and the `outputs` map.

## 8. Components and class design

The operator is a thin script over seven collaborators; SQL is built in one place and executed in another, so the plan can be unit-tested without running DuckDB.

| Class | Kind | Responsibility |
| --- | --- | --- |
| `DuckDBFileCompareNodeOperator` | Spring bean | `execute()`: runs the phases in order, times them, catches errors, always writes the summary |
| `CompareConfig` and nested records | Records | At most two top-level config types: `CompareConfig` (record) and the sealed `Rule` interface with `@JsonTypeInfo` on `type` (subtypes nested in it). Every other JSON block (`Source`, `CsvOptions`, `MultiFile`, `PreTransform`, `Columns`, `SchemaCheck`, `KeyCheck`, `Thresholds`, `Profiling`, `Sampling`, `Output`, `OutputFile`) is a nested record inside `CompareConfig` |
| `CompareConfigBinder` | Component | Resolved map → `CompareConfig` via `convertValue`, then static validation (required fields, enums, duplicate rules, `overwrite` pre-flight) |
| `SourceSqlFactory` | Component | `read_csv` / `read_parquet` view SQL from a `SourceSpec`; option-name mapping and literal escaping |
| `RuleRenderer` beans + `RuleRendererRegistry` | Beans + `Map<String, RuleRenderer>` | One predicate per rule type (section 6) |
| `ComparePlanBuilder` | Plain class | All SQL strings for a run from config + column lists: views, materialise, key check, pass 1, pass 2, diffs, keyless, COPY |
| `CompareExecutor` | Plain class | Runs plan statements on the connection, logs SQL at DEBUG, reads back counts |
| `SummaryBuilder` / `ReportWriter` | Components | `summary.json` via Jackson; static HTML report from a template |
| `VerdictEvaluator` | Plain class | Policies + thresholds → status (section 7) |

```java
public NodeResult execute(NodeContext ctx, Map<String, Object> raw) {
    RunState run = RunState.start(ctx);
    try {
        CompareConfig cfg = binder.bind(variableResolver.resolve(raw, ctx));
        try (WorkSchema db = WorkSchema.open(ctx)) {        // existing connection; creates and drops cmp_<runId>
            ComparePlan plan = planBuilder.sources(cfg);
            db.run(plan.ingestViews());
            SchemaResult schema = schemaChecker.check(db, cfg);    // DESCRIBE on views
            run.record(schema);
            if (cfg.dryRun()) return run.dryRun(planBuilder.full(cfg, schema));
            if (schema.mustFail()) return run.fail("schema");
            plan = planBuilder.full(cfg, schema);
            executor.execute(plan, db, run);                       // materialise, keys, pass 1, pass 2, diffs
            writer.write(plan, db, cfg.output(), run);
        }
        return run.finish(verdict.evaluate(cfg, run));
    } catch (CompareException e) {
        return run.fail(e.phase(), e);
    } finally {
        summaryBuilder.writeQuietly(run);                          // always
    }
}
```

`RunState` holds counts, findings, timings and output paths for one call; it is a local variable, so the bean stays stateless.

## 9. Error handling, logging and security

The node never throws out of `execute()`: every failure becomes a FAILED `NodeResult` with the phase named, and the summary is written first.

### Error handling

| Failure | Phase | Result |
| --- | --- | --- |
| Unresolved `${...}`, unknown field, bad enum, duplicate rule | Bind | FAILED before any connection is opened |
| Output file exists with `overwrite = false` | Bind | FAILED, no input read |
| Glob matches no file, unreadable file | Ingest | FAILED, file path in message |
| Missing key column; schema finding with `fail` | Schema | FAILED, findings in summary |
| CSV value breaks inferred type, rule cast error, DuckDB out of memory or disk | Materialise / compare | FAILED, DuckDB message + the failing statement's first 500 characters |
| Output directory not writable | Write | FAILED; earlier counts still in summary if the summary itself can be written |

- All SQL errors are wrapped in `CompareException(phase, sqlSnippet, cause)`.
- Cleanup always runs: work schema cmp\_\<runId> dropped; the connection itself belongs to the existing setup. Partial output files from a failed write are deleted so a downstream node never reads half a file.
- No retries inside the node; retry policy belongs to the DAG executor.

### Logging

- INFO: one line per phase with duration and row counts (`phase=pass1 ms=41230 candidates=61000`), plus the final verdict line.
- DEBUG: every generated SQL statement before it runs.
- `dryRun`: all SQL at INFO, then return without reading data.
- Every line carries `runId` and node name from `NodeContext` (MDC), so it lands in the execution log with other nodes.

### Security

- `preTransform` and `custom` rule SQL run as written. Configs must come from a trusted, reviewed source, never from end-user input.
- Paths and string values are inserted as SQL literals with `'` doubled; identifiers are always double-quoted with `"` doubled.
- Where the DuckDB version in use supports it, restrict file access to the input and output directories (`allowed_directories` with `enable_external_access = false`), so a `preTransform` cannot read other files.
- `summary.json` and the report hold sample data values; they inherit the classification of the input files.

## 10. Testing and delivery

The MVP is keyed mode with the two-pass compare, because that is the path the 500-column × 20M-row files need; keyless, profiling and the report follow.

### Testing

- **Plan tests:** `ComparePlanBuilder` output for a fixed config and column list, compared with checked-in SQL snapshots. No DuckDB needed.
- **Rule tests:** one small table per rule type covering equal, within tolerance, outside tolerance, NULL on one side, NULL on both, and unparseable values.
- **Golden-file tests:** small CSV/Parquet pairs under `src/test/resources/cases/<case>/` with `config.json` and an expected `summary.json` (timings ignored). Cases: identical files, left-only, right-only, duplicates warn/fail, type mismatch, mapping, include/exclude, glob with `unionByName`, keyless.
- **Failure tests:** each row of the error table in section 9 produces FAILED with the right phase and leaves no partial outputs.
- **Scale test:** the synthetic 500-column × 20M-row pair (5.13), run on the target pod size; record per-phase timings and peak disk as the baseline.

### Delivery phases

| Phase | Scope |
| --- | --- |
| 1 (MVP) | CSV + Parquet ingest, `preTransform`, mapping, include/exclude, schema check, key check, hash fast path, two-pass keyed compare, `exact` / `numeric` / `string` / `custom` rules, typeMode, diffs + left/right-only + summary, thresholds, work-schema isolation, directory paths, identical-data shortcut, parallelWrite |
| 2 | `date` rules, `abortCandidatePct`, `dryRun`, duplicates file, scale benchmark and tuning, key-bucket fallback only if the benchmark needs it |
| 3 | Keyless mode for limited data, with the keylessMaxRows guard, profiling with sampling, HTML report, `includeMatched` |

## Appendix A. Recommended DuckDB setup (outside this job)

These are recommendations for the existing setup that prepares DuckDB before the job runs; they are not requirements of the compare job, and the job never sets them.

| Setting | Recommended | Why |
| --- | --- | --- |
| Version | DuckDB 2.0 (JDBC 2.0.0.1) | The spec's SQL uses the 2.0 lambda form; asynchronous I/O for CSV and Parquet is built in |
| `threads` | The pod's CPU limit, set explicitly | DuckDB may see the host's cores instead of the container limit |
| `memory_limit` | Below the pod memory limit, leaving room for the JVM | Avoids OOM kills; DuckDB spills to disk above the limit |
| `temp_directory` | Fast local disk (NVMe), not network storage | Join and aggregation spill at 20M rows lands here |
| `preserve_insertion_order` | `false` | Lets large `CREATE TABLE` and `COPY` run with less memory; the job does not depend on row order |
| Storage | File-backed database, or in-memory with `temp_directory` spill; choose by benchmark | Both sides as tables (about 2 × 20M × 500 values) must fit on disk, not in RAM |
| Concurrency | When several large compare jobs share a pod, split `threads` and `memory_limit` between them | Each job's queries already use every thread they are given |
