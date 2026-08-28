# Requirements: DuckDB SQL-on-File Job Execution Engine

## 1. Background / Problem Statement

Today, a set of SQL jobs run against Sybase tables. As a plugin-style replacement, we want to
run the equivalent SQL logic using **DuckDB**, where the source data is already available as
**file extracts** of those tables (e.g. CSV) rather than live Sybase connections.

This document covers the full engine design in two layers:
- **Single-job execution** — a basic Java application that runs an ordered sequence of SQL
  statements against file-backed DuckDB tables and produces file output.
- **Multi-job batch execution** — a single invocation ("execution") that runs multiple,
  independent, isolated jobs concurrently against one shared physical DuckDB instance.

A single job is the foundational unit either way; batch execution is the same job model run
concurrently, with added isolation and lifecycle mechanics layered on top.

## 2. Goal

Build a Java job engine that:
- Reads one or more input files as DuckDB in-memory tables
- Executes an ordered sequence of SQL statements against those tables
- Writes one or more output files from the results
- Runs each job as a **do-and-die** unit of work (not a long-running service)
- Supports being invoked with **one or many job names at once**, running independent jobs
  **concurrently**, each fully isolated from the others, all backed by a single physical
  DuckDB in-memory instance
- Supports a declarative **pre-/post-processor lifecycle** around each job's SQL execution
  (schema/variable setup, output validation, notification), resolved from a registry of
  reusable Spring-managed components rather than one-off custom code per job

## 3. Scope

### In Scope
- DuckDB **in-memory** mode only (no persisted `.duckdb` database file)
- Input via DuckDB `READ_CSV` (and compatible readers), loaded by SQL statements that are
  themselves part of the job (source-loading SQL lives in the job's `sqls` list, not a separate
  config mechanism — keeps source-loading SQL reusable/self-contained across jobs)
- An ordered list of SQL statements executed sequentially within each job
- Job-level variables (`constants` + `envVariables`), resolved and loaded as **DuckDB session
  variables** before a job's `sqls` run
- Output via DuckDB `COPY ... TO` statements, as part of the job's own SQL sequence
- SQL execution via Spring JDBC's `ScriptUtils` (multi-statement execution)
- Connection management via HikariCP
- An execution (`runId` + a set of unique job names) that runs jobs **concurrently**, capped at
  a configurable limit (default 4 at a time), via duplicated DuckDB connections sharing one
  physical instance; jobs beyond the cap are **queued** (FIFO)
- Per-job isolation via a uniquely named schema (`<jobName>_<runId>`), created and dropped
  around each job's SQL execution
- A per-job **`JobContext`**, held in a `ThreadLocal`, carrying job-scoped state across that
  job's pre-processor, SQL execution, and post-processor steps
- A registered **pre-/post-processor lifecycle**, resolved via a single `Processor` interface
  implemented by Spring beans, injected as `Map<String, Processor>`, referenced by key from the
  job JSON
- Built-in generic pre-processor (schema + session-variable setup) and post-processor(s)
  (output validation, email notification)
- Upfront validation of the execution request (job-name uniqueness, `runId` presence), failing
  the whole execution immediately if invalid
- Job-level failure isolation once jobs are running (one job's failure doesn't stop others)
- A single, plain-text, execution-level log file (keyed by `runId`) that all jobs in the
  execution append their summary to
- Basic engine-level DuckDB resource configuration (temp directory, heap/memory limit),
  configured once per execution

### Out of Scope
- Multi-job orchestration / DAG sequencing across jobs (jobs in a batch are independent by
  design; covered by a separate, already-planned workflow engine effort for genuinely dependent
  workflows)
- Long-running / server-mode operation
- Non-file (live DB) sources or Sybase connectivity
- Retry, scheduling, alerting
- Persisted/on-disk DuckDB database
- Cross-job data sharing
- Job scheduling/triggering, or generation of `runId` itself (assumed supplied by an upstream
  caller and guaranteed unique)
- Arbitrary user-supplied (non-registered) processor code
- Queue prioritization beyond basic FIFO

## 4. Job Definition Format (JSON)

A job is defined by a JSON document with the following fields:

| Field | Type | Description |
|---|---|---|
| `jobName` | string | Unique identifier/name for the job |
| `runtime` | string/object | Runtime metadata for the job (exact shape TBD — e.g. timeout, execution date/context) |
| `constants` | map (key-value) | Fixed values known at job-definition time, loaded as DuckDB session variables |
| `envVariables` | array | Names of environment variables to resolve at runtime and load as DuckDB session variables |
| `sqls` | array (ordered) | The list of SQL statements to execute, in order — including source-loading, transformation, and `COPY`-based output statements |
| `preProcessors` | array | Ordered list of registered processor **bean keys** to run before `sqls` |
| `postProcessors` | array | Ordered list of registered processor **bean keys** to run after `sqls` |

`constants` and `envVariables` are merged into a single set of DuckDB session variables
(`SET variable_name = value;`), established before any statement in `sqls` runs:
- If the same variable name appears in both, **`envVariables` takes precedence** over
  `constants`.
- Independently, when resolving an environment variable's actual value at runtime, a
  same-named **system property takes precedence** over the environment variable.

## 5. Processor Model

- A single interface, one method: **`void process(JobContext context)`**. The same contract
  is used for both pre- and post-processors — stage (before/after `sqls`) is determined purely
  by which list (`preProcessors` or `postProcessors`) a bean key appears in.
- All beans implementing this interface are collected by Spring into a
  **`Map<String, Processor>`** (keyed by bean name/qualifier). A job's JSON references
  processors purely by their map key — no custom compiled code per job.

### 5.1 Built-in Pre-Processor: Schema & Session-Variable Setup
- Runs once, before a job's `sqls` execute. Generates and executes one generic in-memory SQL
  step that:
  1. Creates the job's isolation schema: `CREATE SCHEMA <jobName>_<runId>;`
  2. Sets the job's resolved session variables via `SET var = value;` statements

### 5.2 Built-in Post-Processor(s): Output Validation & Notification
- **Output validation**: after `sqls` (including cleanup) finish, checks that expected output
  was produced (e.g. output file existence/non-empty) and sets the job's final status.
- **Email notification**: sends an email reflecting the job's outcome, reading its
  configuration from the job definition already present on `JobContext` — no separate email
  config lookup.

## 6. Execution Model

- An **execution** = one engine invocation, identified by a mandatory `runId` (supplied
  externally; uniqueness guaranteed upstream, not generated by this engine) plus a **set of
  unique job names** to run.
- **Upfront validation**: before any job starts, the execution validates `runId` presence and
  job-name uniqueness. Failure here (`"Invalid input"` plus further detail on what was invalid)
  stops the **entire execution immediately** — a distinct, earlier failure mode from per-job
  failure once jobs are running.
- Jobs run **concurrently**, via DuckDB connection duplication: each job gets its own logical
  `Connection`, duplicated from and sharing the same single physical in-memory DuckDB instance
  for the execution. Concurrency is capped at a configurable limit (**default 4 at a time**);
  jobs beyond the cap are **queued FIFO** and started as slots free up.
- **`JobContext`** (created per job, held in a `ThreadLocal` since each job runs on its own
  thread) contains:
  - Job info (the job's JSON definition — `jobName`, `runtime`, processor lists, etc.)
  - Connection info (that job's duplicated DuckDB `Connection`)
  - Schema name (`<jobName>_<runId>`)
  - Resolved variable info (`constants`/`envVariables`, resolved values)
  - Status info: one of `PENDING` / `RUNNING` / `SUCCEEDED` / `FAILED` — surfaced only in the
    final execution-level log summary, not queryable during execution
  - **Not included:** SQL query results — `JobContext` is not a data-carrying/results object
- Isolation between concurrently running jobs, sharing one physical DuckDB instance, is achieved
  by:
  - **Schema isolation** — each job's tables/views live in its own `<jobName>_<runId>` schema;
    there is no shared/global table namespace, so no job can see another job's objects.
  - **Connection-scoped session variables** — because each job runs on its own duplicated
    connection, a variable set in one job's scope is naturally invisible to any other job, even
    with an identical name, requiring no explicit reset between jobs.

## 7. Functional Requirements

### 7.1 Job Definition & Variables
- FR-1: A job is fully described by the JSON structure in Section 4.
- FR-2: `constants` and `envVariables` are resolved to DuckDB session variables on the job's
  connection, set before the first statement in `sqls` executes, with `envVariables` taking
  precedence over `constants` on name collision, and system properties taking precedence over
  environment variables of the same name.
- FR-3: SQL statements reference variables via DuckDB's `getvariable('name')` (or equivalent),
  since session variables are not directly interpolated into SQL text/structure.
- FR-4: SQL statements execute strictly **in order**; a statement can depend on
  tables/results/variables set by a prior statement in the same job.

### 7.2 Input & Output Handling
- FR-5: Source loading (e.g. `READ_CSV` into a temp table/view) is expressed as ordinary SQL
  statements within the job's `sqls` list, so source-loading SQL snippets can be authored once
  and reused across jobs.
- FR-6: The engine should support at least CSV as an input format (extensibility to other
  DuckDB-readable formats such as Parquet is a nice-to-have, not a hard requirement here).
- FR-7: Output file(s) are produced using DuckDB `COPY ... TO` statements as part of the
  ordered `sqls` sequence — output is just the last statement(s) in the script, not a separate
  mechanism. Output format/location may use session variables (via `getvariable()`) for path
  templating.

### 7.3 Execution & Connections
- FR-8: The engine runs entirely in DuckDB **in-memory mode** — no on-disk DuckDB database is
  created or persisted.
- FR-9: Each job is a **do-and-die** unit of work: it runs its full ordered `sqls` sequence
  once, produces output(s), and terminates (success or failure) — no polling, daemonization,
  or scheduling loop.
- FR-10: SQL execution uses Spring JDBC's `ScriptUtils` to run each job's multi-statement
  script against its DuckDB JDBC connection.
- FR-11: An execution accepts a `runId` and a list of (unique) job names.
- FR-12: The execution request is validated upfront (`runId` present, job names unique); on
  failure, the whole execution stops immediately without running any job.
- FR-13: Jobs run concurrently, capped at a configurable limit (default: 4 at a time), via
  duplicated DuckDB connections sharing one physical instance.
- FR-14: Jobs beyond the concurrency cap are queued FIFO and started as running jobs complete.
- FR-15: Jobs are independent once running: one job's success or failure must not affect the
  execution, timing, or outcome of another job in the same execution.

### 7.4 Job Context
- FR-16: A `JobContext` (Section 6) is created per job and made available via `ThreadLocal` to
  that job's pre-processor(s), SQL execution, and post-processor(s), scoped strictly to its
  owning job/thread with no cross-job visibility.

### 7.5 Processor Lifecycle
- FR-17: Before a job's `sqls` execute, all processors listed in its `preProcessors` run, in
  order, resolved from the Spring-managed `Map<String, Processor>`, each invoked as
  `process(jobContext)`.
- FR-18: The generic schema/session-variable pre-processor creates the job's schema and sets
  all resolved session variables in one setup step.
- FR-19: After a job's `sqls` (including cleanup) complete, all processors listed in its
  `postProcessors` run, in order, via the same `process(jobContext)` contract.
- FR-20: The generic output-validation post-processor checks expected output was produced and
  determines job success/failure, updating status info on `JobContext`.
- FR-21: The generic notification post-processor sends an email reflecting the job's outcome,
  sourcing its configuration from the job definition on `JobContext`.

### 7.6 Cleanup & Failure Handling
- FR-22: Schema cleanup (`DROP SCHEMA <jobName>_<runId> CASCADE;`) executes as the final step
  of a job's SQL handling, **guaranteed to run** whether the preceding statements succeeded or
  failed, so no schema is ever left behind.
- FR-23: If any SQL statement in a job's sequence fails, the job fails fast (stops executing
  remaining statements) and is marked failed; the execution continues running/queuing the
  remaining jobs.
- FR-24: Failure is logged with enough context to identify which statement in the sequence
  failed.
- FR-25: At the end of the execution, a **per-job summary** (success/failure per job name) is
  written, in **plain text**, to a single execution-level log file, keyed by `runId`, shared by
  all jobs in that execution.

## 8. Non-Functional Requirements & Design Considerations

- NFR-1: **Simplicity first** — avoid unnecessary abstraction or premature generalization.
- NFR-2: Startup/teardown overhead should be minimal, since each job is a short-lived,
  do-and-die unit of work.
- NFR-3: Default concurrency cap is **4 jobs at a time** per execution; configurable.
- NFR-4: Basic DuckDB resource setup (temp directory, heap/memory limit) is configured once at
  execution start (via engine config), applied to the shared physical DuckDB instance — not
  configured per job.

### 8.1 DuckDB Session Variables — Considerations
- Session variables (`SET var = value`) are **connection-scoped**: they only exist for the
  life of the connection they were set on, and must be set *after* a connection is obtained
  (there's no way to "preload" them before a connection exists).
- Variables are read in SQL via `getvariable('name')` as an **expression** — they can't be used
  to interpolate structural SQL elements (table/column identifiers, DDL). This is fine for
  `WHERE` clauses, computed columns, or `COPY` target paths, but is a constraint worth keeping
  in mind when authoring job SQL.

### 8.2 Connection Model — Confirmed Approach & Residual Risk
- Concurrency is achieved via **DuckDB connection duplication**: each job's connection is
  duplicated from, and shares, one physical in-memory DuckDB instance, rather than one physical
  database per job (and rather than acquiring a fresh pooled connection per statement, which
  would risk statements in the same job losing visibility of each other's temp tables/
  variables).
- This rests on an assumption about the DuckDB JDBC driver's duplication semantics that should
  be validated early: that catalog objects (schemas/tables) **are** visible across duplicated
  connections (required, since all jobs share one instance and the isolation mechanism relies
  on schemas being creatable/droppable per job), while session-level state such as `SET`
  variables is **not** shared across duplicated connections (required for cross-job variable
  isolation). This is called out explicitly so it isn't discovered late during implementation.

### 8.3 Schema Naming
- Format: `<jobName>_<runId>`. Since `runId` is unique per execution (guaranteed upstream) and
  job names are unique within an execution (enforced upfront), this combination is
  collision-safe without needing a timestamp or random suffix.

### 8.4 Processor Registry
- Processors are plain Spring beans implementing the single `Processor` interface; the registry
  is a `Map<String, Processor>` auto-populated by Spring. Job JSON references processors purely
  by their map key.

### 8.5 Cleanup Guarantees
- Schema drop must be implemented as a guaranteed step (try/finally or equivalent) around each
  job's SQL execution so a mid-job failure still results in cleanup.

### 8.6 Logging
- One plain-text log file per execution, keyed by `runId`; all jobs in that execution append
  their result/summary to this same file. File naming/location/rotation will be defined later
  via `log4j2.xml` configuration.

## 9. Technology Stack

| Concern | Choice |
|---|---|
| Language | Java |
| Build tool | Gradle |
| SQL Engine | DuckDB (in-memory mode) |
| Job definition | JSON |
| Multi-statement SQL execution | Spring JDBC `ScriptUtils` |
| Connection pooling / management | HikariCP; duplicated DuckDB connections per job sharing one physical instance |
| Concurrency | Capped at 4 concurrent jobs per execution (configurable); excess jobs queued FIFO |
| Isolation mechanism | Per-job schema `<jobName>_<runId>`; connection-scoped session variables |
| Per-job state | `JobContext`, held in `ThreadLocal` (job info, connection, schema, variables, status — no SQL results) |
| Processor abstraction | Single `Processor` interface, `void process(JobContext)`; Spring beans injected as `Map<String, Processor>` |
| Built-in pre-processor | Schema creation + session-variable setup |
| Built-in post-processor(s) | Output validation; email notification (config sourced from `JobContext`) |
| Variable precedence | `envVariables` over `constants`; system property over environment variable |
| Failure scope | Whole-execution for upfront validation; per-job once running |
| Logging | Single plain-text, execution-level log file, keyed by `runId`; format defined via log4j2.xml |
| DuckDB resource config | Temp directory + heap/memory limit, set once per execution via config |

## 10. Open Questions

- OQ-1: Exact shape of the `runtime` field in the job JSON (timeout? execution context/date?).
- OQ-2: Validate DuckDB JDBC connection-duplication semantics against the assumption in
  Section 8.2 (shared catalog, isolated session state) before relying on it in implementation.
- OQ-3: Exact response/error shape for upfront validation failures (`"Invalid input"` plus
  detail) — field structure to be finalized at implementation time.

## 11. Success Criteria

- A job (JSON definition with `constants`/`envVariables` + ordered `sqls` including source
  loads and `COPY`-based outputs) can be executed end-to-end, whether invoked alone or as part
  of a multi-job execution.
- The same DuckDB SQL logic used today against Sybase tables can be pointed at file extracts
  and produce equivalent output, with only source/table wiring changes (not SQL logic changes).
- A single invocation, given a `runId` and a set of unique job names, validates the request
  upfront, then runs jobs concurrently (up to the configured cap, queuing the rest FIFO), each
  isolated by its own `<jobName>_<runId>` schema and connection-scoped session variables, all
  backed by one physical DuckDB instance.
- Each job's schema is guaranteed to be dropped after the job completes, whether it succeeds or
  fails; no session variables, data, or execution state are visible across jobs.
- A failure in one running job does not stop other jobs in the execution; a failure in upfront
  request validation stops the whole execution immediately.
- The execution's per-job outcomes are recorded in one plain-text, execution-level log file,
  keyed by `runId`.
- Pre-processor (schema + variable setup) and post-processor (output validation + email) run
  automatically for every job via the registered Spring-bean processor lifecycle, without
  job-specific custom code.
