# Writing HySDS Workflow Specs

A **workflow spec** is a JSON document that chains HySDS jobs into a pipeline. You register
it with mozart once, then start runs of it with inputs. HySDS submits each job when its
predecessor finishes, passes outputs forward, fans work out over lists, retries flaky steps,
and records the run's state in one status document.

This guide is for mission developers who write specs and make their PGEs workflow-ready.
It covers **schema version 1.0** (the `feature/workflow-orchestration-v1` branch).

> **v1 in one paragraph.** A run is a straight line of jobs, with optional
> scatter/gather fan-out over a list. A node can't branch to two successors, and there are
> no joins, conditionals or loops. A job that fails for good halts the run. Every error in a
> spec is reported together when you register it, so a spec that registers is structurally
> sound.

---

## 1. Quick start

**1. Write a spec.** Two jobs: A's `message` output feeds B.

```json
{
  "id": "hello-chain", "version": "1.0.0", "schema_version": "1.0",
  "defaults": { "queue": "my_worker" },
  "inputs":   { "greeting": { "type": "string", "default": "hi" } },
  "entry": "A",
  "nodes": {
    "A":    { "kind": "job",  "run": "job-say:v1.0", "in": { "text": "$.inputs.greeting" },
              "out": ["message"], "next": ["to_B"] },
    "to_B": { "kind": "pass", "bind": { "B.text": "$.A.message" }, "next": ["B"] },
    "B":    { "kind": "job",  "run": "job-say:v1.0", "in": { "text": "$.to_B.text" },
              "out": ["message"], "next": [] }
  },
  "outputs": { "final": "$.B.message" }
}
```

**2. Register it.** The response is `201 {"spec_id": "hello-chain:1.0.0"}`, or a `400` that
lists every validation error.

```bash
M=https://<mozart-host>/mozart/api/v0.2/workflow     # v0.1 has the same endpoints
curl -X POST -H 'content-type: application/json' -d @hello-chain.json $M/specs
```

**3. Start a run.** The response is `202 {"run_id": "run-…"}`.

```bash
curl -X POST -H 'content-type: application/json' \
     -d '{"spec_id": "hello-chain:1.0.0", "inputs": {"greeting": "hello"}, "user": "jdoe"}' \
     $M/runs
```

**4. Watch it.** The run is done when `.result.status` is `completed`, `failed` or `cancelled`.

```bash
curl -s $M/runs/<run_id> | jq '.result | {status, error, outputs, nodes}'
```

For `job-say` to work here, it must accept a `text` param and write a `message` output.
[§2](#2-making-a-job-workflow-ready) explains how.

---

## 2. Making a job workflow-ready

Any registered HySDS job type can be a workflow step. The workflow engine talks to it
through two things: **params in** and **one outputs file out**.

### 2.1 Params in

A job node's `in` object becomes the job's submit params, exactly as if you had called
`POST /job/submit` yourself. Declare each key in the job-spec's `params` list, with the
`destination` you want (`context`, `positional` or `localize`):

```json
"params": [
  { "name": "mode",     "destination": "context" },
  { "name": "item_url", "destination": "localize" }
]
```

A `localize` param that holds an `s3://` URL is downloaded into the job directory before the
PGE starts, which is how a step consumes an upstream file. The full example is
`local-stack/wf-stac/` (`job-spec.json`, `hysds-io.json` and `run_wf_stac.py`).

### 2.2 Outputs out: `_workflow_outputs.json`

To expose values to later nodes, the PGE writes **one file named `_workflow_outputs.json`
at the job directory root** (its working directory):

```json
{
  "version": "1.0",
  "outputs": [
    { "name": "qa_score",  "value_d": 0.93 },
    { "name": "qa_status", "value_s": "PASS" },
    { "name": "reprocess", "value_b": false },
    { "name": "granules",  "value_s": ["s3://bucket/a.h5", "s3://bucket/b.h5"] }
  ]
}
```

- Each entry has a `name` and **exactly one** typed slot. `value_s` holds a string,
  `value_d` a number and `value_b` a boolean. Any slot can hold a list of that type, and the
  list's order is kept.
- Every name in the node's `out` list should appear here. A missing output isn't an error
  until something downstream references it. The run then halts with
  `node 'X' has no output 'y'`.
- The file is optional. Without it, the job has no outputs.
- A file that is malformed, uses the wrong slot type, repeats a name or is too large **fails
  the job**. The default size limit is 512 KB, set by `WORKFLOW_OUTPUTS_MAX_BYTES`.
  Outputs ride on a queue message, so keep them to scalars and references. Never put a
  payload here: publish it as a dataset and output its location.

### 2.3 Referencing published files: `class: File | Directory`

A PGE usually doesn't know where its products will be published. Mark an output as a file
or directory reference and give the **local path, relative to the job directory**:

```json
{ "name": "catalog", "class": "Directory", "value_s": "output/MY_DATASET_ID/catalog.json" },
{ "name": "items",   "class": "File",      "value_s": ["output/MY_DATASET_ID/a/a.json",
                                                       "output/MY_DATASET_ID/b/b.json"] }
```

After the job's datasets are published, HySDS rewrites each path to its published URL,
such as `s3://bucket/products/…/MY_DATASET_ID/a/a.json`. Downstream nodes only ever see the
URL. The rules:

- `class` requires the `value_s` slot.
- Every path must lie **inside a dataset the job publishes**: a directory that has a
  `<id>.dataset.json`. Otherwise the job fails. A job with post-processing disabled
  (`disable_post_builtins`) publishes nothing, so it can't use `class`.
- Symlinked dataset directories work. Paths are matched as written, not resolved.

A `class: File` list fed to a scatter gives one child per published file. That's the
standard way to fan out over a job's products.

### 2.4 Telling the retry policy why you failed (optional)

Every failed attempt gets an **error class**, which is what `retry_on` matches against
([§5.2](#52-retries)):

| class | when |
|---|---|
| `exit_code:<n>` | the PGE exited non-zero |
| `timeout` | the job hit its soft or hard time limit |
| `signal` | the worker caught a signal |
| `offline` | the worker went away (job-offline) |
| `worker` | any other worker-side error |
| `pge:<token>` | the PGE wrote `<token>` to `_error_class.txt` in the job directory |

To classify your own failures, write one token (`[A-Za-z0-9_:.-]`, at most 64 characters)
to `_error_class.txt` before exiting non-zero. For example, `throttled` becomes
`pge:throttled`. That class replaces `exit_code:<n>` or `worker`, but never `timeout`.

---

## 3. Spec reference

### 3.1 Top level

| field | required | notes |
|---|---|---|
| `id`, `version` | yes | Non-empty strings. The spec is stored as **`<id>:<version>`** and can't be changed. To change it, register a new version. Re-posting an existing id:version returns `409`. |
| `schema_version` | yes | Must be `"1.0"`. |
| `entry` | yes | The first node. It must be a `job` with no predecessors. |
| `nodes` | yes | `{node_id: node}`. Ids match `^[A-Za-z0-9_-]+$`. `inputs`, `scatter`, `finalize`, `halt`, `_finalize` and `_halt` are reserved. |
| `inputs` | no | `{name: {"type": T, "default"?: v}}`. T is one of `string`, `int`, `float`, `bool`, `string[]`, `int[]` or `float[]`. An input with no default is required at run time. A `float` input accepts ints. |
| `defaults` | no | `queue`, `max_retries`, `retry_on` and `timeout_hours`. Each job node can override the first three. |
| `outputs` | no | `{name: "$.<node>.<output>"}`. They're resolved into the run's `outputs` when it completes. |
| `label`, `description` | no | Free text. These are also allowed on every node. |

**Unknown fields are rejected**, both at the top level and on nodes. That catches typos like
`"nxt"`.

### 3.2 Node kinds

| kind | required fields | optional | `next` must be |
|---|---|---|---|
| `job` | `run` (a job_spec id, e.g. `job-foo:v1.0`), `in`, `out` (list of output names) | `queue`, `max_retries`, `retry_on` | `[]`, or one `pass` or `scatter` |
| `pass` | `bind`: `{"<next_node>.<param>": "$.ref"}`, non-empty | `connector` | exactly one `job`, the one named in every bind key |
| `scatter` | `child` (a `job` node id), `over` (usually a `$`-ref to a list) | `completion.threshold` in (0, 1], default `1.0`; `connector` | exactly one `gather` |
| `gather` | `collect`: `"$.<child>.<output>"` or a list of them | `connector` | `[]`, or one `pass` |

`next` may be omitted, which means `[]`. A node with `next: []` ends the run. The run
completes when that node completes.

**Allowed shapes**, as a grammar:

```
job → (pass → job)* → [scatter → gather → (pass → job)*] → …end
```

- **A job can't link straight to another job.** Put a `pass` between them, even if it just
  forwards a value.
- **A scatter child is owned by its scatter.** It must have `next: []`, must not appear in any
  other node's `next`, and can't be the entry.
- **One successor per node and one predecessor per node.** Specs with branches, joins or
  cycles are rejected.

### 3.3 References (`$`-refs)

Any string value that starts with `$.` is a reference. Every other value is a literal.

| ref | resolves to | where it's allowed |
|---|---|---|
| `$.inputs.<name>` | the run input, with defaults applied | anywhere |
| `$.<node>.<output>` | that node's output | the node must be a **strict ancestor** of the node using the ref, and must declare the output |
| `$.scatter.item` | this child's item from the scatter list | only in a scatter child's `in` |

Which outputs a node declares depends on its kind:

- a `job` declares its `out` list
- a `pass` declares the `<param>` part of each bind key, so `"B.text"` declares `text`
- a `gather` declares the `<output>` part of each `collect` ref. Its value is a **list**.

**Rules to remember:**

- Refs can sit at any depth in `in`, inside nested objects and lists:
  `{"options": {"files": ["$.A.f1", "$.A.f2"]}}`.
- A ref replaces the **whole string**. There's no interpolation: `"prefix-$.A.x"` is a
  literal.
- Values keep their JSON type. A ref to a `value_d` list yields a list of numbers, not a
  string.
- You can reference **any** ancestor, not just the one right before. A job after a pass can
  use `$.A.x` straight from an earlier job.
- A scatter child can reference ancestors of its scatter, such as `$.inputs.*` and the
  upstream job's outputs.
- `spec.outputs` can reference any node except a scatter child, because per-child outputs
  aren't stored. Reference the gather instead.
- The gather's bookkeeping values `n_completed` and `n_failed` are **not** referenceable in
  v1. They're visible in the run status doc.

### 3.4 What `pass` is for

With the default connector, a `pass` just copies values under new names. B could equally
reference `$.A.message` directly. The pass exists for two reasons: it's the required hop
between two jobs, and it's the extension point for a mission **pass connector** that
computes the next job's params. A connector can rename, reshape or look up the previous
job's published products. See [§7](#7-custom-connectors).

---

## 4. Fan-out: scatter and gather

```
list ─▶ fan_out (scatter over $.list.granules) ─▶ process × N ─▶ collect (gather) ─▶ to_mosaic ─▶ mosaic
```

```json
{
  "id": "granule-mosaic", "version": "1.0.0", "schema_version": "1.0",
  "label": "List granules, process each, mosaic the results",
  "defaults": { "queue": "factotum-job_worker-small", "timeout_hours": 12,
                "max_retries": 1, "retry_on": ["offline", "timeout"] },
  "inputs": {
    "collection": { "type": "string" },
    "bbox":       { "type": "float[]" }
  },
  "entry": "list",
  "nodes": {
    "list":    { "kind": "job", "run": "job-list_granules:v2.1",
                 "in": { "collection": "$.inputs.collection", "bbox": "$.inputs.bbox" },
                 "out": ["granules", "count"], "next": ["fan_out"] },
    "fan_out": { "kind": "scatter", "child": "process", "over": "$.list.granules",
                 "completion": { "threshold": 0.9 }, "next": ["collect"] },
    "process": { "kind": "job", "run": "job-process_granule:v2.1",
                 "queue": "factotum-job_worker-large",
                 "max_retries": 3, "retry_on": ["exit_code:75", "pge:throttled", "offline"],
                 "in": { "granule_url": "$.scatter.item", "collection": "$.inputs.collection",
                         "options": { "compress": true, "bands": ["B04", "B08"] } },
                 "out": ["product", "cloud_pct"], "next": [] },
    "collect": { "kind": "gather", "collect": ["$.process.product", "$.process.cloud_pct"],
                 "next": ["to_mosaic"] },
    "to_mosaic": { "kind": "pass", "bind": { "mosaic.products": "$.collect.product" },
                   "next": ["mosaic"] },
    "mosaic":  { "kind": "job", "run": "job-make_mosaic:v2.1",
                 "in": { "products": "$.to_mosaic.products", "bbox": "$.inputs.bbox" },
                 "out": ["mosaic"], "next": [] }
  },
  "outputs": { "mosaic": "$.mosaic.mosaic", "products": "$.collect.product" }
}
```

**How it runs:**

1. When `list` completes, the scatter resolves `over`. With the default connector, the
   value **must be a list**, or the run halts. Each item can be any JSON value. A `string[]`
   run input works too: `"over": "$.inputs.files"`.
2. One `process` job is submitted per item, and the item is bound to `$.scatter.item`.
   Children run in parallel, as far as the queue's workers allow.
3. The gather **waits until every child is terminal**, after its retries are used up. Then
   it checks the threshold:
   - If `completed / N ≥ threshold`, the run continues. Each collected output is a list of
     the **completed** children's values, **in item order**. Failed children are left out,
     so the list can be shorter than N.
   - Otherwise the run halts with `scatter process: k/N completed < threshold t`.
4. An **empty list** (N = 0) skips straight to the gather, which outputs empty lists.

**Sizing.** A default scatter's list comes from an upstream output, so the 512 KB outputs
limit caps its length. For larger or computed item sets, such as a STAC search, a manifest
or a GRQ query, use a mission scatter connector ([§7](#7-custom-connectors)).

---

## 5. Failure handling

### 5.1 Implicit halt

If a job fails and won't be retried, the run's `status` becomes `failed`. The failing node
gets `state: failed`, the run's `error` says why, and no further nodes are submitted. A
failed scatter child is the exception: its failure counts against the threshold instead
([§4](#4-fan-out-scatter-and-gather)).

### 5.2 Retries

| field | default | meaning |
|---|---|---|
| `max_retries` | `0` | The number of re-runs allowed after the first attempt. |
| `retry_on` | `[]` | The error classes ([§2.4](#24-telling-the-retry-policy-why-you-failed-optional)) worth retrying. |

Set them on a job node, or in `defaults` for every job node. A node's value replaces the
default; the two aren't merged. A failure is retried when **attempts so far < `max_retries`
and its class matches `retry_on`**.

`retry_on` patterns: `exit_code:<n>`, `exit_code:*`, `timeout`, `signal`, `offline`,
`worker`, `pge:<token>`, `pge:*` and `*` (anything). Invalid patterns are rejected at
registration.

Scatter children are retried individually. A child counts as failed only when its last
attempt fails.

> **Operator note:** workflow jobs are retried **only by their workflow**. A retry started
> from a user rule or the UI is skipped (and logged) for any job that carries a
> workflow tag.

Good defaults: `["offline"]` for transient infrastructure failures, plus the specific
`pge:` classes your PGE emits for transient failures. Avoid `*` on expensive steps,
because it retries real bugs too.

### 5.3 Run timeout and stalls

- `defaults.timeout_hours` caps the **whole run**. The deployment default is 48h, set by
  `WORKFLOW_RUN_TIMEOUT_HOURS`. When the cap is hit, the reaper fails the run with
  `run timed out after <h>h` and revokes its jobs. **A `timeout_hours` on a job node is
  accepted but ignored in v1.** For a per-job limit, use the job-spec's
  `soft_time_limit`/`time_limit`.
- If a run makes no progress for 10 minutes (`WORKFLOW_STALL_SECONDS`), the reaper replays
  the last events of its finished steps. This recovers lost messages. You don't need to do
  anything.

### 5.4 Cancelling

```bash
curl -X POST -H 'content-type: application/json' -d '{"user": "jdoe"}' $M/runs/<run_id>/cancel
```

| response | meaning |
|---|---|
| `202 {"revoked": n}` | The run and every unfinished node are now `cancelled`. Queued jobs are revoked and running jobs are killed. |
| `200` | The run was already cancelled. This call only re-sweeps leftover jobs, so it's safe to repeat. |
| `409` | The run had already completed or failed. |

---

## 6. Runs: API and status

| method & path | purpose |
|---|---|
| `POST /workflow/specs` | Register a spec. Returns `201`, `400` with `errors[]`, or `409` if it exists. |
| `GET /workflow/specs`, `GET /workflow/specs/<id:version>` | List specs or fetch one. |
| `POST /workflow/runs` `{spec_id, inputs, user?}` | Start a run. Returns `202 {run_id}`, `400` for bad inputs, or `404` for an unknown spec. |
| `GET /workflow/runs/<run_id>` | Get the status doc (below). |
| `GET /workflow/runs/<run_id>/steps` | Job counts per step and status, e.g. `{"process": {"job-completed": 41, "job-started": 9}}`. Use it for live progress, since the doc only shows node state. |
| `POST /workflow/runs/<run_id>/cancel` | Cancel a run ([§5.4](#54-cancelling)). |

**Status doc** (`.result`):

```json
{ "run_id": "run-…", "spec_id": "granule-mosaic:1.0.0", "status": "running",
  "username": "jdoe", "inputs": { "collection": "…", "bbox": [ … ] },
  "submitted_at": "…Z", "updated_at": "…Z", "ended_at": null, "error": null, "outputs": null,
  "nodes": {
    "list":    { "state": "completed", "job_id": "…", "outputs": { "granules": [ … ], "count": 50 } },
    "fan_out": { "state": "completed", "outputs": { "n": 50 } },
    "process": { "state": "running", "expected": 50, "threshold": 0.9 },
    "collect": { "state": "pending" }, "to_mosaic": { "state": "pending" }, "mosaic": { "state": "pending" } } }
```

- Run `status` is one of `running`, `completed`, `failed` or `cancelled`. `ended_at` is set
  when the run reaches one of the last three.
- Node `state` is one of `pending`, `queued`, `running`, `completed`, `failed` or
  `cancelled`. `queued` means the node's params are resolved and its submit is imminent.
  Jobs go from `queued` straight to `completed` or `failed`, so use `/steps` to see
  progress.
- After the gather finishes, the scatter child's entry adds `completed` and `failed` counts.

**Run inputs** are checked against the spec when the run starts. Unknown names, missing
required inputs and type mismatches all return `400` with the full list.

**Queues.** The entry job's queue comes from its own `queue` or from `defaults.queue`, and
one of the two is **required**. Later jobs also fall back to the job-spec's first
`recommended-queues` entry. Setting `defaults.queue` is the simplest choice.

---

## 7. Custom connectors

`pass`, `scatter` and `gather` nodes run as small HySDS jobs called **connectors**. A
mission can supply its own policy, such as a scatter that enumerates a STAC search, a
gather that reduces instead of collecting, or a pass that looks up the previous job's
products. To use one, name it on the node:

```json
"fan_out": { "kind": "scatter", "connector": "job-stac-scatter:v1.0", "child": "process",
             "over": "$.inputs.stac_query", "next": ["collect"] }
```

- With a custom scatter, `over` can be any value. Its meaning belongs to the connector.
- The connector must be on the deployment's **`WORKFLOW_CONNECTORS` allowlist** in mozart
  `settings.cfg`, and its job-spec must be registered. Otherwise registration fails and
  names the allowed ids.
- Without `connector`, the node uses the deployment default. The default is written into
  the stored spec, so a later change to the deployment default never alters a registered
  spec.

How to write one, including the hook SDK and the contract test kit: `workflow-connectors/README.md`,
with a worked plugin in the `workflow-connectors-example` repo.

---

## 8. Common registration errors

| error (abbreviated) | fix |
|---|---|
| `a job node's next must be a pass or scatter node, but 'B' is 'job'` | Insert a `pass` between the two jobs. |
| `'next' has 2 targets; v1 allows at most 1` | v1 has no parallel branches. Split the work into separate specs, or scatter over a list. |
| `fan-in is not allowed; has predecessors [...]` | Two nodes point at one node. v1 has no joins. |
| `$.X.y refers to 'X', which is not a strict ancestor of 'Z'` | You can only reference upstream nodes. Check the `next` chain, and note that a scatter child is not an ancestor of anything. |
| `refers to output 'y' not declared by node 'X'` | Add `y` to X's `out`. For a gather, only the `collect` outputs exist, not `n_completed`. |
| `refers to scatter child 'P', whose outputs are not stored` | In `spec.outputs` or downstream, reference the gather instead (`$.G.y`). |
| `'$.scatter.item' is only allowed in a scatter child's 'in'` | Move the ref into the child's `in`. |
| `bind key 'C.x' targets 'C' but next is 'B'` | A pass binds only its own successor's params. |
| `unknown field 'xyz'` | Check for a typo. Unknown fields are rejected, except `label` and `description`. |
| `kind 'switch' is not supported in v1` | `gate`, `switch` and `halt` are reserved for later versions. |
| `connector 'job-x' is not allowed for kind 'scatter'` | Ask an operator to add it to `WORKFLOW_CONNECTORS`. |
| `409 workflow spec … already exists` | Specs can't be changed. Bump `version`. |

To check a spec offline, before you post it:

```python
from hysds_commons.workflow_spec import validate_spec
print(validate_spec(json.load(open("my-spec.json"))))   # [] means valid
```

This runs the same checks as registration, except the connector allowlist and
job-spec-existence checks.

---

## 9. Not in v1

Conditionals (`gate`, `switch`), parallel branches, fan-in joins, loops, pause and resume,
per-run overrides, spec permissions, a CWL adapter, and per-job `timeout_hours`.

## See also

- Runtime contract (authoritative behavior): `hysds-workflow-docs` repo,
  `PLAN-workflow-orchestration-v1-contract.md`
- Output binding design: ADR-003, *Workflow Output Binding via Job-Scoped `_workflow_outputs.json`*
- Fixture specs: `hysds_commons.workflow_examples` and the `local-stack/wf-e2e/spec-*.json`
  files (linear, scatter/gather, retry, File/Directory outputs)
