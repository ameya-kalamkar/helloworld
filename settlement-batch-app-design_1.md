# Settlement Batch Application — Design Document

**Language:** Java only — no external DSL, no scripting layer, no rule engine.
**Execution engine:** Apache Flink today; designed to be swappable for Spark later (not currently planned, kept as a design constraint).

---

## 1. Goal

Business logic must be **visible and traceable to BRD requirements** for business users, without exposing technical/infra detail (reading/writing, connectors, tuning). Developers and application architects retain **full, precise control** of the platform/technical layer. The two concerns are split into separate modules with an enforced dependency boundary, not just a naming convention.

---

## 2. Naming hierarchy

```
Operation → Job → Stage → Rule / Calculator
```

| Level | Meaning | Key property |
|---|---|---|
| **Operation** | The full business capability, mapped 1:1 to a BRD document (e.g. "End-of-Day Settlement Operation"). | Business-facing name, no technical connotation. |
| **Job** | The atomic, independently executable unit — one Flink (or future Spark) application submission. Deliberately reuses Flink's own "Job" term (1:1 semantic match) and aligns with the jobflow DAG executor's own vocabulary. | Always contains **1 or more** Stages, never zero. |
| **Stage** | A grain-changing boundary within a Job (e.g. transaction record → account-day → settlement batch). Matches Flink's and Spark's own shuffle-boundary semantics, which helps the engine-portability goal. | Fixed input/output grain; own Calculator registry. |
| **Rule / Calculator** | Attribute-level business logic within a Stage. | One or more Calculators can target the same attribute across different stages via a repeatable annotation (see §5) — but exactly one Calculator per `(stage, target attribute)` pair. |

**Rejected alternatives and why:** *Process* (collides with Flink's `ProcessFunction`, BPM tooling connotation), *Task* (collides with Flink `TaskManager`/`Task` and with the jobflow DAG executor's own Task concept), *Workflow/Flow* (collides with orchestration tooling and Flink's `DataStream` vocabulary), *Program* (classic OS-level overload). *Capability* was the strongest alternative to *Operation* but *Operation* was kept as it matches back-office/settlement domain language.

---

## 3. Module layout

```
settlement-batch-app/
├── settings.gradle.kts
├── build.gradle.kts
│
├── contracts/                  // engine-agnostic, zero Flink/Spark imports
│   └── OperationDefinition.java, JobDefinition.java, StageDefinition.java,
│       StageRuntime.java, Schema.java, AttributeSpec.java, IOBinding.java
│
├── business/                   // BRD-traceable, business-reviewable, Flink-free
│   ├── model/          SourceRecord.java (nested), TargetRecord.java (flat, collections ok)
│   ├── calculator/     AttributeCalculator.java, CalculationContext.java,
│   │                    RecordCalculator.java, CalculatorRegistry.java
│   ├── rules/           enrich/, aggregate/, finalize/   (one calculator class per BRD item)
│   ├── annotation/      Calculates.java (repeatable)
│   └── lineage/         instrumented Source proxy + CalculationContext capture (see §6/§7)
│   └── (test) plain JUnit, no MiniCluster — one test class per BRD-id
│
├── catalog/                    // build-time doc/lineage generator, reads business/ only
│   └── CatalogGenerator.java, LineageGraphBuilder.java
│
├── sidecar/                    // per-Stage reference-data holder construction
│   └── ReferenceDataService.java (embedded DuckDB, loaded once per JVM), sidecar POJOs built per stage
│
├── platform-flink/             // ONLY module allowed to import Flink
│   ├── FlinkStageRuntime.java  // implements contracts.StageRuntime
│   ├── operators/RuleExecutionOperator.java   // thin wrapper over business/RecordCalculator
│   ├── connectors/sources/ (FLIP-27 FileSource), sinks/ (HeaderTrailerWriter, BulkWriter)
│   └── config/ (off-heap sizing, state backend, parallelism)
│
├── platform-spark/             // placeholder only, until Spark adoption is real
│
├── jobs/                       // one entrypoint per Job, wires StageDefinitions to FlinkStageRuntime
├── operations/                 // composes Jobs into an Operation, engine-agnostic
├── boundary-tests/             // ArchUnit — enforces the dependency graph below
└── docs/
    ├── brd-mapping/             // source BRD documents
    └── generated/                // catalog + lineage output, produced at build time — never hand-edited
```

**Enforced dependency direction:**
`contracts` ← nothing. `business` ← `contracts` only. `catalog`, `sidecar` ← `business`/`contracts`, never `platform-*`. `platform-flink` ← `contracts` + `business` (only module with a Flink dependency). `jobs` ← everything, composes one Job's Stages. `operations` ← `jobs` + `contracts` only (stays engine-agnostic despite orchestrating Flink-backed Jobs underneath).

**Kept fully separate:** the jobflow DAG executor (a distinct project). If used at all, it invokes `operations/`/`jobs/` entrypoints from outside the JVM — never a build dependency, never sharing scheduling code with Flink's own execution strategy.

---

## 4. Platform layer — what developers/architects control precisely

- Execution & scheduling: parallelism, slot allocation, operator chaining — left to Flink's own planner, no custom scheduler on top.
- Checkpointing / state backend config (currently local BATCH mode, `HashMapStateBackend`, checkpointing disabled).
- Which physical operations (`keyBy`, aggregate, window, join) implement a Stage's grain change.
- Source/sink connector implementations, formats, serialization.
- `FlinkStageRuntime` — the engine seam; implements `contracts.StageRuntime`, turns a `StageDefinition` into a running Flink job. A future `SparkStageRuntime` would sit here too, same contract, `business/` untouched.
- `RuleExecutionOperator` — a thin Flink operator (e.g. a `MapFunction`) that, per element, calls `business/RecordCalculator.process(record, sidecar)`. It carries no calculation logic itself and has no knowledge of what any calculator computes — all of that lives in `business/`, which stays independently testable with plain JUnit (no MiniCluster).

**Explicitly not here:** anything with `@Calculates`, any `AttributeCalculator`, any BRD-id, any target-attribute name — enforced by ArchUnit as a build failure if crossed.

---

## 5. Business/transformation layer — what's open to business visibility

### Core model
- **SourceRecord** — nested (classes with collections/other class members).
- **TargetRecord** — flat (collection-typed attributes allowed).
- **Exactly one Calculator per `(stage, target attribute)`** — single-writer property, makes attribute lineage unambiguous.

### AttributeCalculator — computes a value only, never assigns it

```java
public interface AttributeCalculator<S, SC, V> {
    V calculate(S source, SC sidecar, CalculationContext ctx);
}
```

- `S` — the Source type this calculator reads.
- `SC` — the Sidecar type for the stage it runs in (see "Sidecar" below); a calculator that doesn't need sidecar data can type this as `Object`.
- `V` — the value it produces. Assignment onto the target object is the framework's job (`RecordCalculator`), not the calculator's.
- A calculator can be **reusable across stages**: written against a broad/common `S` (and `Object` for `SC` if unneeded), it isn't tied to one stage's concrete target type — which stage(s)/target attribute(s) it's wired to is entirely determined by its `@Calculates` annotations (below), not by its own type signature.

### CalculationContext — intra-stage dependency resolution

```java
public interface CalculationContext {
    <V> V getDerived(String attributeName);
}
```

`getDerived` is a **memoized, recursive runtime resolver**, scoped to one record being processed:
- First call for an attribute name: check whether the target object already has that attribute set (covers the partially-filled-target case, see `RecordCalculator` below) — if so, return it, no calculator invoked.
- Otherwise, look up the registered calculator for `(stage, attributeName)`, invoke it, memoize the result on the record's context, return it.
- A later call for the same name (from this calculator or another) returns the memoized/target value — no recompute.
- A call for an attribute currently mid-resolution is a **cycle** — thrown at that point, naming the chain. Cycle detection is **runtime-only, per record** — there is deliberately no build-time/dry-run graph check, since the dependency graph can be data-dependent (conditional `getDerived` calls).

This mechanism is also how **target-attribute lineage is captured**: every `getDerived(X)` call made from inside calculator Y's `calculate()` is an observed edge `Y → X`. Nothing is developer-declared here — declared `consumes*` lists go stale over time, so lineage is derived from what the code actually does, not from what someone once wrote down. The same idea extends to **source-attribute lineage**: `S` is wrapped in a lightweight recording proxy before being handed to `calculate()`, and whichever getters were touched during that call become the observed `consumesSourceAttributes` for that invocation — aggregated across the per-BRD-id JUnit tests that already exist under `business/(test)`.

### `@Calculates` — repeatable annotation (replaces `@BusinessRule`)

```java
@Retention(RUNTIME) @Target(TYPE)
@Repeatable(Calculates.List.class)
public @interface Calculates {
    String id();                  // "BRD-114"
    String title();
    String ruleRef() default (""); // Jira key/link
    Class<?> sourceRecord();
    Class<?> targetRecord();
    Class<?> sidecarRecord();     // lets the registry validate/match sidecar type too
    String targetAttribute();     // Option B (§6): String, cross-checked at test time
    StageId stage();              // singular — repeatability gives the multiplicity

    @Retention(RUNTIME) @Target(TYPE)
    @interface List { Calculates[] value(); }
}
```

A calculator reused across stages carries multiple `@Calculates` repetitions that typically differ only in `stage()` / `targetAttribute()` / `targetRecord()`, with `sourceRecord()` (and usually `id`/`title`/`ruleRef`) unchanged across repetitions:

```java
@Calculates(id="BRD-201", title="Round notional", sourceRecord=TradeSource.class,
            targetRecord=EnrichTargetRecord.class, sidecarRecord=Object.class,
            targetAttribute="NOTIONAL_ROUNDED", stage=StageId.ENRICH)
@Calculates(id="BRD-201", title="Round notional", sourceRecord=TradeSource.class,
            targetRecord=FinalizeTargetRecord.class, sidecarRecord=Object.class,
            targetAttribute="SETTLEMENT_AMT_ROUNDED", stage=StageId.FINALIZE)
public class RoundToTwoDecimalsCalculator implements AttributeCalculator<TradeSource, Object, BigDecimal> {
    public BigDecimal calculate(TradeSource source, Object sidecar, CalculationContext ctx) {
        BigDecimal raw = ctx.getDerived("RAW_AMOUNT");
        return raw.setScale(2, RoundingMode.HALF_UP);
    }
}
```

### RecordCalculator — one instance per Stage, drives target population

```java
class RecordCalculator<S, SC, T> {
    RecordCalculator(StageId stage, Class<S> sourceType, Class<T> targetType, Class<SC> sidecarType,
                      String scanPackage,
                      List<Class<? extends AttributeCalculator<?,?,?>>> extraCalculators) {
        this.registry = CalculatorRegistry.resolve(
            stage, sourceType, targetType, sidecarType, scanPackage, extraCalculators);
    }

    T process(S source, SC sidecar) {
        return process(source, newTargetInstance(), sidecar);
    }

    T process(S source, T target, SC sidecar) {
        var ctx = new DefaultCalculationContext(source, target, sidecar, registry, stage);
        for (String attributeName : registry.attributeNames()) {
            if (!alreadySet(target, attributeName)) {
                assign(target, attributeName, ctx.getDerived(attributeName));
            }
        }
        return target;
    }
}
```

- **Exactly one `RecordCalculator` instance is created per Stage** and reused across every record that Stage processes — it holds no per-record mutable state (a fresh `CalculationContext` is created per `process()` call), so it's safe to share.
- `process(source, sidecar)` builds a fresh target from scratch; `process(source, target, sidecar)` continues populating an **already partially-filled** target, skipping any attribute already set — this covers cases where a target flows into a stage pre-populated from earlier work.

### CalculatorRegistry — resolved per RecordCalculator, not one global scan

- Scans the given `scanPackage` (+ subpackages) for classes carrying `@Calculates`/`@Calculates.List`.
- Adds any `extraCalculators` passed explicitly via the `RecordCalculator` constructor — for reusable calculators living outside that package scope.
- Filters the combined candidate set to entries whose `stage()` matches this `RecordCalculator`'s stage **and** whose `sourceRecord()`/`targetRecord()`/`sidecarRecord()` are compatible with its `S`/`T`/`SC` — a three-way signature match read directly off the annotation's `Class<?>` elements, sidestepping Java generic type erasure entirely (no reflection over generic supertypes needed).
- **Validates**, within that resolved set: exactly one calculator per target attribute; no duplicates; no gaps against the full attribute set for that stage.
- No longer performs a build-time topological sort (superseded — ordering now falls out of `CalculationContext.getDerived`'s runtime resolution, see above).

### Sidecar reference data

- Built **once per Stage, at stage start** — not per record, not globally shared as one object.
- A **plain class, not an interface** — whatever shape a given stage's calculators need, decided at that stage's construction. `RecordCalculator`/`CalculationContext` treat it as an opaque pass-through: they hand it to calculators untouched and never inspect its internals.
- `RecordCalculator`'s generic signature is still typed to it (`SC`), and `@Calculates.sidecarRecord()` lets the registry validate a calculator's declared sidecar type against it.
- Backed by the embedded-DuckDB reference-data pattern (per-JVM Spring container, loaded once, HikariCP pool) at the service level; the per-stage sidecar object is what's actually built from that service and handed down.
- Kept distinct from main pipeline input: sidecar data never appears in a Stage's input/output schema.

---

## 6. Compile-time & test-time lineage safety

Raw String references can silently drift — that's the reason for enum-bound stage identifiers, and it's a live tradeoff for the String-based elements below.

- **StageId** — small, stable set, hand-declared once in `contracts`: `enum StageId { ENRICH, AGGREGATE, FINALIZE }`.
- **Target attributes — scoped per Stage, not one global enum**, generated by an annotation processor reading each Stage's `TargetRecord` subtype, regenerated every build:
  ```java
  // generated from EnrichTargetRecord's fields
  public enum EnrichTargetAttribute { TRADE_DATE, COUNTERPARTY, NOTIONAL }
  ```
  Rename/remove a field → anything referencing the old constant **fails to compile** internally (used for the generated setter-dispatch table, see below) — the strongest available guarantee, no window where it can silently lie.
- **`@Calculates.targetAttribute()` and every `CalculationContext.getDerived(String)` call are String-based (Option B from the original design)** — this was extended further than originally scoped: not just the annotation, but every intra-stage dependency read is now a String, not an enum. `CalculatorRegistry.validate()` cross-references each declared/requested String against the real per-stage enum at test time. **Still open / not yet decided:** whether to have the same annotation processor also emit a small per-stage constants class (e.g. `EnrichAttributes.NOTIONAL = "NOTIONAL"`) purely so calculator code gets IDE autocomplete/rename-safety on these String literals, without changing the runtime type. Left for a future session.
- Java annotation elements may be: primitive, `String`, `Class`, another annotation, an enum, or arrays of any of these — values must resolve to compile-time constants, and no `null` defaults are allowed (use an explicit `UNSPECIFIED` sentinel, or make the element mandatory).

---

## 7. Lineage enforcement across the build lifecycle

| Stage | Mechanism | Catches |
|---|---|---|
| **Compile time** | Enum-bound `stage` references + generated per-stage target-attribute enum backing the setter-dispatch table (§6) | Renamed/removed fields — build fails immediately |
| **Test time** | `CalculatorRegistry.validate()` (1:1 coverage, no gaps/dupes against the generated enum); automatic lineage capture via the instrumented Source proxy + `CalculationContext` observation described in §5 — there is no declared list to diff against anymore, the captured graph *is* the lineage | Missing/duplicate calculators for an attribute; runtime cycles (per record, first time that data path executes) |
| **CI build** | `CatalogGenerator`/`LineageGraphBuilder` regenerate the catalog on every build (never hand-edited) from the captured lineage; cross-check every `@Calculates(id=...)` against a canonical BRD-id manifest | Stale/typo'd BRD-ids; requirements with zero implementing rule (reverse coverage) |

All three gates fail the build, not just warn — lineage integrity is treated with the same seriousness as a failing unit test.

---

## 8. Catalog — generated output, not authored source

- The "full picture" artifact is **generated JSON**, produced by `CatalogGenerator`/`LineageGraphBuilder` every build into `docs/generated/` — never hand-written, so it inherits every guarantee from §6/§7 for free.
- `consumesSource`/`consumesSidecars`/`consumesTargetAttributes` in the shape below are now populated from the **captured** lineage (§5/§7), not from any developer-declared field:
  ```json
  {
    "operation": "OP-SETTLEMENT",
    "jobs": [{ "jobId": "JOB-ENRICH-AGGREGATE",
      "stages": [{ "stageId": "ENRICH",
        "rules": [{ "brdId": "BRD-114", "targetAttribute": "SETTLEMENT_FLAG",
          "consumesSource": ["tradeDate","settleDate"],
          "consumesSidecars": ["holidayCalendar"] }] }] }]
  }
  ```
- Worth considering the **OpenLineage** spec instead of a bespoke schema, if the goal is a real lineage/observability tool rather than an internal doc — existing tooling (Marquez, DataHub, etc.) can visualize/query it without building a viewer. **Still undecided.**
- **JSON is never the authoring source.** Hand-authoring the Operation→Job→Stage→Rule definitions in JSON would reintroduce the exact drift problem §6/§7 exist to eliminate. Code stays the source of truth; JSON is the compiled-down, always-fresh artifact.

---

## 9. Open items / decisions to revisit next session

Calculator design (`AttributeCalculator`, `CalculationContext`, `@Calculates`, `RecordCalculator`, `CalculatorRegistry`, Sidecar) is now **finalized** per §5. Remaining:

- Sketch `CalculatorRegistry.resolve(...)` in full — package scanning, extra-class merging, three-way signature filtering, validation.
- Sketch the instrumented lineage-capture mechanism: the Source recording proxy and how `CalculationContext` records observed `getDerived` edges, and how both get aggregated across the per-BRD-id JUnit tests into the lineage graph.
- Sketch the annotation processor that generates, per stage: the `TargetAttribute` enum, and the reflection-free setter-dispatch table `RecordCalculator.assign(...)` uses.
- Decide whether to also generate a per-stage String-constants class for `getDerived`/`targetAttribute()` call sites (§6) — nice-to-have, not blocking.
- `ArchUnit` `LayerDependencyTest` — write the actual rule set enforcing the module graph in §3.
- `build.gradle.kts` per-module dependency declarations.
- Decide whether to adopt OpenLineage format for the generated catalog, or keep it bespoke.
