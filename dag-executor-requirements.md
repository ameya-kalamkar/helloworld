# DAG Executor — Requirements Document

## 1. Purpose

A standalone Java component that parses an Airflow-style DAG definition (using `>>` and `[...]` list syntax) into a directed acyclic graph, validates it, and executes it with correct dependency ordering and parallelism across independent branches.

This document defines the DSL grammar, the in-memory graph model, validation rules, and execution semantics. Each node's actual work is delegated to a **TaskOperator**, provided by jobflow — the executor's job is graph construction, validation, and scheduling; it invokes the TaskOperator bound to each node rather than knowing anything about what that node does (SQL job, Flink job, etc.).

---

## 2. DSL Grammar

### 2.1 Tokens

| Token | Meaning |
|---|---|
| `IDENTIFIER` | Task ID — alphanumeric + underscore, must match an already-declared or first-use task |
| `>>` | "then" — left task(s) are upstream of right task(s) |
| `[`, `]` | List delimiters for fan-out / fan-in groups |
| `\|` | List item separator (accept optional surrounding whitespace: `[A\|B]` and `[A \| B]` both valid) |
| `;` | **Mandatory** statement separator — every statement must be terminated with `;`. Newlines are treated as pure whitespace (ignored by the tokenizer), not as statement boundaries — this removes significant-whitespace handling from the parser entirely; a DAG definition may still be formatted across multiple physical lines for readability, but statement boundaries are determined solely by `;` |

**`<<` is intentionally out of scope.** It's semantically redundant with `>>` (`C << A` ≡ `A >> C`, `C << [A,B]` ≡ `[A,B] >> C`, already covered by fan-in below) and only earns its keep in tools that need to declare a downstream task before its upstream is known — this DSL builds the full node registry before wiring edges (§3.1), so that constraint doesn't apply. Supporting it would double parser direction-handling and introduce an ambiguous-mixing problem (`A >> B << C`) for no new expressiveness. Decision: `>>`-only for v1; revisit only if a real ergonomic need surfaces.

### 2.2 Supported patterns

| # | Pattern | Meaning |
|---|---|---|
| 1 | `A >> B;` | Simple edge: A upstream of B |
| 2 | `A >> B >> C >> D;` | Multi-hop chain (n-hop, not just 2) |
| 3 | `A >> [B\|C];` | Fan-out: A upstream of B and C |
| 4 | `[A\|B] >> C;` | Fan-in: A and B upstream of C — *(reverse of #3)* |
| 5 | `A >> [B\|C] >> D;` | Fan-out then fan-in (diamond) |
| 6 | `[A\|B] >> [C\|D];` | List-to-list — see §2.3 for semantics decision |
| 7 | `A >> B; A >> C;` (separate statements) | Same source node referenced across statements; must resolve to one shared node with two children |
| 8 | `A >> [B];` | Single-element list; must degenerate to `A >> B` (same node identity as case 1) |
| 9 | `A;` (bare, no operator) | Isolated task declaration — no edges, still must register as a graph node |

### 2.3 List >> List semantics: pairwise (decided)

`[A|B] >> [C|D]` uses **pairwise/zip** semantics: A→C, B→D only — element-by-element by position, not all-to-all. This matches Airflow's `chain()` behavior rather than the raw bitshift cross-product.

**Requirement:** both lists must be equal length. `[A|B] >> [C|D|E]` is a parse-time/validation error (see §4), not a silent partial mapping.

### 2.4 Explicitly unsupported (reject at parse time, do not silently misparse)

- Nested lists: `A >> [B|[C|D]]`
- Self-loop: `A >> A`
- Empty list: `A >> []`
- Missing statement terminator: any statement not terminated with `;` is a parse error (not silently inferred from newline/EOF)

---

## 3. Graph Model

### 3.1 Node identity

- Task ID is the sole identity key. A **registry pass** must run before edge-building: scan all statements, collect every distinct task ID, create exactly one `Node` object per ID.
- All statement parsing (edges) must reference nodes from this registry — never create a new `Node` inline during edge parsing. This is what makes pattern #7 (same node split across statements) and #10 (list degeneration) work correctly.

### 3.2 Data structures

```
Node
  - id: String
  - upstream: Set<Node>
  - downstream: Set<Node>
  - taskOperator: TaskOperator   // provided by jobflow; executor invokes this to do the node's actual work
  - status: NodeStatus           // PENDING, RUNNING, SUCCESS, FAILED (§5.4)

Edge
  - from: Node
  - to: Node

DAG
  - nodes: Map<String, Node>     // registry, keyed by id
  - edges: List<Edge>            // full edge list, post-expansion of list syntax
```

### 3.3 Duplicate edges

`A >> B` declared twice (identical direction, same pair) is a **no-op**, not an error — the second declaration is silently deduped against the existing edge set (`Set<Edge>` semantics via `equals`/`hashCode` on `(from, to)`).

---

## 4. Validation Rules (must run before execution starts)

| Rule | Check | Failure behavior |
|---|---|---|
| No self-loops | `edge.from != edge.to` | Reject at parse time |
| No cycles | Full-graph cycle detection (DFS with recursion-stack tracking, or Kahn's algorithm as a byproduct of topological sort) | Reject before execution; report the cycle path in the error |
| No dangling references | Every task ID used in an edge must exist in the node registry | Reject at parse time |
| Equal-length lists on list>>list | `[A|B] >> [C|D]` requires both lists to be the same length (pairwise semantics, §2.3) | Reject at parse time |
| No empty DAG | At least one node must be declared | Reject |
| Isolated nodes allowed | A node with no upstream/downstream is valid (single-task DAG or standalone task) | Not an error |

Validation is a distinct pass from parsing — parsing builds the graph, validation certifies it's executable. Do not conflate the two; a syntactically valid statement can still produce an invalid graph once combined with other statements.

---

## 5. Execution Semantics

### 5.1 Ordering

- Topological sort determines valid execution order.
- Nodes with no unresolved upstream dependencies are eligible to run concurrently.
- A node becomes eligible the moment **all** of its upstream nodes have completed successfully.

**Scheduling is a ready-queue / Kahn's-algorithm loop, not branch-by-branch traversal:**

1. At DAG-run start, compute in-degree (count of unresolved upstream dependencies) for every node.
2. Any node with in-degree 0 is immediately eligible and submitted to the thread pool — this includes multiple independent nodes/branches at once, not one branch at a time.
3. When a node completes successfully, decrement the in-degree of each of its downstream nodes. Any downstream node that reaches in-degree 0 is added to the ready queue at that moment and submitted.
4. Repeat until the ready queue is empty and no tasks are in flight.

Execution order is derived purely from the dependency graph — **not** from declaration order in the DSL, and **not** "complete branch 1 fully, then branch 2." Independent branches interleave and run concurrently as soon as each is individually eligible, bounded only by the thread pool concurrency cap (§5.2). E.g. given `A >> B; A >> C; X >> Y;` — `X` (no dependencies) can start immediately in parallel with `A`; `B` and `C` each become eligible independently the moment `A` completes, and run concurrently with each other.

A `concurrency=1` configuration naturally degrades this to fully sequential, deterministic execution (useful for debugging) without needing a separate code path.

### 5.2 Parallelism

- Independent branches (no shared ancestry) execute in parallel via a thread pool.
- Concurrency cap: bounded by thread pool size (decided) — configurable pool size, FIFO queuing once the cap is hit, consistent with the pattern already used in the batch DuckDB job engine.
- Fan-out siblings (e.g. B and C under `A >> [B,C]`) run in parallel once A completes.
- Fan-in nodes (e.g. D under `[B,C] >> D`) wait for **all** upstream siblings to complete, not just one.

### 5.3 Failure handling: fail-fast (decided)

Any node failure halts all further scheduling — no new nodes are submitted once a failure occurs. Already-running nodes are allowed to finish (not forcibly interrupted); once all in-flight nodes complete, the DAG run is marked `FAILED` and no further nodes execute, regardless of whether they were otherwise eligible.

### 5.4 Status tracking

Per-node status: `PENDING → RUNNING → SUCCESS | FAILED`. Under fail-fast, nodes that never became eligible before the halt remain `PENDING` (they were never submitted — not marked `SKIPPED`, since fail-fast has no downstream-skip concept). DAG-run-level status: `SUCCESS` only if all nodes reach `SUCCESS`; `FAILED` if any node reaches `FAILED`.

---

## 6. Design Decisions (resolved)

| # | Question | Decision |
|---|---|---|
| 1 | List >> List semantics | Pairwise/zip, equal-length lists required (§2.3, §4) |
| 2 | Failure handling | Fail-fast — halt scheduling on any node failure, let in-flight nodes finish (§5.3) |
| 3 | Task execution abstraction | Each node executes via a **TaskOperator**, provided by jobflow — executor is agnostic to what the operator actually does (§1, §3.2) |
| 4 | Concurrency control | Bounded by thread pool size, configurable pool cap, FIFO queuing beyond cap (§5.2) |
| 5 | File-to-DAG mapping | One file = exactly one DAG |

---

## 7. Suggested Implementation Order

1. Tokenizer/parser → produces raw statement list (edges + bare node declarations)
2. Node registry pass → dedupe by ID
3. Graph builder → adjacency lists (upstream/downstream sets per node), applying list-expansion semantics from §2.3
4. Validator → self-loop, cycle, dangling-reference, empty-DAG checks
5. Topological sort / scheduler → produces execution levels or a live dependency-resolution loop
6. Executor → thread-pool-backed runner respecting concurrency cap, with chosen failure-handling mode
7. Status/reporting layer → per-node and per-run status aggregation
