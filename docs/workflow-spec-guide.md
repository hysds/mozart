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

**1. Write a spec.** Two steps of a radiometer processing chain. `L1B` turns an L1A granule
into L1B brightness temperatures (`L1B_TB`), and `L1C` grids them onto the EASE-Grid 2.0 36 km
grid (`L1C_TB`). L1B's `l1b_file` output feeds L1C.

```json
{
  "id": "radiometer-l1c", "version": "1.0.0", "schema_version": "1.0",
  "defaults": { "queue": "sps-job_worker-large" },
  "inputs": {
    "l1a_file": { "type": "string" },
    "crid":     { "type": "string", "default": "R19240" }
  },
  "entry": "L1B",
  "nodes": {
    "L1B":    { "kind": "job", "run": "job-l1b_tb:R19", "tag": "L1B_TB",
                "in": { "l1a_file": "$.inputs.l1a_file", "crid": "$.inputs.crid" },
                "out": ["l1b_file"], "next": ["to_L1C"] },
    "to_L1C": { "kind": "pass", "bind": { "L1C.l1b_file": "$.L1B.l1b_file" }, "next": ["L1C"] },
    "L1C":    { "kind": "job", "run": "job-l1c_tb:R19", "tag": "L1C_TB",
                "in": { "l1b_file": "$.to_L1C.l1b_file", "grid": "EASE2_M36km", "crid": "$.inputs.crid" },
                "out": ["l1c_file"], "next": [] }
  },
  "outputs": { "l1c_tb": "$.L1C.l1c_file" }
}
```

**2. Register it.** The response is `201 {"spec_id": "radiometer-l1c:1.0.0"}`, or a `400` that
lists every validation error.

```bash
M=https://<mozart-host>/mozart/api/v0.2/workflow     # v0.1 has the same endpoints
curl -X POST -H 'content-type: application/json' -d @radiometer-l1c.json $M/specs
```

**3. Start a run.** The response is `202 {"run_id": "run-…"}`.

```bash
curl -X POST -H 'content-type: application/json' \
     -d '{"spec_id": "radiometer-l1c:1.0.0", "user": "jdoe",
          "inputs": {"l1a_file": "s3://<bucket>/L1A_Radiometer/SMAP_L1A_RADIOMETER_45678_D_20261112T013517_R19240_001.h5"}}' \
     $M/runs
```

**4. Watch it.** The run is done when `.result.status` is `completed`, `failed` or `cancelled`.

```bash
curl -s $M/runs/<run_id> | jq '.result | {status, error, outputs, nodes}'
```

For `job-l1b_tb` to work here, it must accept `l1a_file` and `crid` params and write an
`l1b_file` output.
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
  { "name": "l1a_file", "destination": "localize" },
  { "name": "crid",     "destination": "context" }
]
```

A `localize` param that holds an `s3://` URL is downloaded into the job directory before the
PGE starts. That's how L1B gets its L1A granule. The full example is
`local-stack/wf-stac/` (`job-spec.json`, `hysds-io.json` and `run_wf_stac.py`).

### 2.2 Outputs out: `_workflow_outputs.json`

To expose values to later nodes, the PGE writes **one file named `_workflow_outputs.json`
at the job directory root** (its working directory):

```json
{
  "version": "1.0",
  "outputs": [
    { "name": "rfi_flagged_pct", "value_d": 3.7 },
    { "name": "orbit_direction", "value_s": "Descending" },
    { "name": "nrt",             "value_b": false },
    { "name": "bad_scans",       "value_d": [1182, 1183, 2410] }
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
{ "name": "l1b_file", "class": "File",
  "value_s": "output/SMAP_L1B_TB_45678_D_20261112T013517_R19240_001/SMAP_L1B_TB_45678_D_20261112T013517_R19240_001.h5" },
{ "name": "qa_dir",   "class": "Directory",
  "value_s": "output/SMAP_L1B_TB_45678_D_20261112T013517_R19240_001/qa" }
```

After the job's datasets are published, HySDS rewrites each path to its published URL,
such as `s3://<bucket>/products/L1B_TB/…/SMAP_L1B_TB_45678_D_…_001.h5`. Downstream nodes only ever see the
URL. The rules:

- `class` requires the `value_s` slot.
- Every path must lie **inside a dataset the job publishes**: a directory that has a
  `<id>.dataset.json`. Otherwise the job fails. A job with post-processing disabled
  (`disable_post_builtins`) publishes nothing, so it can't use `class`.
- Symlinked dataset directories work. Paths are matched as written, not resolved.

A `class: File` list fed to a scatter gives one child per published file. That's the
standard way to fan out over a job's products.

### 2.4 Knowing you're running in a workflow

A job can tell it was started by a workflow from **`_job.json`** in its work directory. A
workflow job has a top-level `workflow` block:

```json
"workflow": { "run_id": "run-…", "spec_id": "radiometer-l3smp-daily:1.0.0", "step_id": "L2SMP",
              "child_index": 9, "child_total": 30 }
```

`child_index` and `child_total` are set only for scatter children, and are `null` otherwise.
If the block is absent, the job is running standalone.

```python
wf = json.load(open("_job.json")).get("workflow")
if wf:
    print("step", wf["step_id"], "of run", wf["run_id"])
```

The block appears **only** in `_job.json`. `_context.json` doesn't carry it, and no
environment variables are set. Most PGEs don't need to check: they get their inputs as
params and write `_workflow_outputs.json` either way.

### 2.5 Telling the retry policy why you failed (optional)

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
| `defaults` | no | `queue`, `max_retries` and `retry_on`, which each job node can override, plus `timeout_hours`, the run timeout ([§5.3](#53-run-timeout-and-stalls)). |
| `outputs` | no | `{name: "$.<node>.<output>"}`. They're resolved into the run's `outputs` when it completes. |
| `label`, `description` | no | Free text. These are also allowed on every node. |

**Unknown fields are rejected**, both at the top level and on nodes. That catches typos like
`"nxt"`.

### 3.2 Node kinds

| kind | required fields | optional | `next` must be |
|---|---|---|---|
| `job` | `run` (a job_spec id, e.g. `job-l1b_tb:R19`), `in`, `out` (list of output names) | `tag`, `queue`, `max_retries`, `retry_on` | `[]`, or one `pass` or `scatter` |
| `pass` | none | `bind`: `{"<next_node>.<param>": "$.ref"}`, or `{}` / omitted for a pass that only orders steps; `connector` | exactly one `job`, the one named in every bind key |
| `scatter` | `child` (a `job` node id), `over` (usually a `$`-ref to a list) | `completion.threshold` in (0, 1], default `1.0`; `connector` | exactly one `gather` |
| `gather` | `collect`: `"$.<child>.<output>"` or a list of them | `connector` | `[]`, or one `pass` |

`next` may be omitted, which means `[]`. A node with `next: []` ends the run. The run
completes when that node completes.

**Allowed shapes**, as a grammar:

```
job → (pass → job)* → [scatter → gather → (pass → job)*] → …end
```

- **A job can't link straight to another job.** Put a `pass` between them, even if it just
  forwards a value or, with no `bind`, only sets the order (see
  [§3.7](#37-sequence-only-chains-no-outputs)).
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
- a `pass` declares the `<param>` part of each bind key, so `"L1C.l1b_file"` declares `l1b_file`.
  A pass with an empty or omitted `bind` declares nothing.
- a `gather` declares the `<output>` part of each `collect` ref, whose value is a **list**.
  It also always declares `n_completed` and `n_failed`, the integer child counts.

**Rules to remember:**

- Refs can sit at any depth in `in`, inside nested objects and lists:
  `{"ancillary": {"dem": "$.inputs.dem", "landcover": ["$.inputs.igbp", "$.inputs.ndvi"]}}`.
- A ref replaces the **whole string**. There's no interpolation: `"CRID-$.inputs.crid"`
  is a literal.
- Values keep their JSON type. A ref to a `value_d` list yields a list of numbers, not a
  string.
- You can reference **any** ancestor, not just the one right before. In the chain in
  [§3.6](#36-a-complete-chain-l1a-to-l2_sm_p), `L2SMP` could read `$.L1B.l1b_file` directly.
- A scatter child can reference ancestors of its scatter, such as `$.inputs.*` and the
  upstream job's outputs.
- `spec.outputs` can reference any node except a scatter child, because per-child outputs
  aren't stored. Reference the gather instead.
- `$.G.n_completed` and `$.G.n_failed` resolve to plain integers, so a later job can be told
  how many children succeeded or failed.

### 3.4 What `pass` is for

With the default connector, a `pass` just copies values under new names. L1C could equally
reference `$.L1B.l1b_file` directly. The pass exists for two reasons: it's the required hop
between two jobs, and it's the extension point for a mission **pass connector** that
computes the next job's params. A connector can rename, reshape or look up the previous
job's published products. See [§7](#7-custom-connectors).

A pass may also have an empty `bind` (`{}`) or no `bind` at all. It then passes nothing on
and only orders the steps: the next job starts after the previous one completes. It still
runs as a step, tagged with its node id. See [§3.7](#37-sequence-only-chains-no-outputs).

### 3.5 Job tags

Each job a job node submits is tagged with the node's `tag`, or with the node id if `tag` is
unset. Think of it as the step name: Figaro's tag facet shows `L1B_TB`, `L1C_TB`, `L2_SM_P`
and so on. `tag` must match `^[A-Za-z0-9_.:-]{1,128}$`, and only job nodes take it.

- Connector jobs are tagged with their node id (`to_L1B`, `per_half_orbit`, `collect_L2`).
  The internal end-of-run jobs are tagged `_finalize` and `_halt`.
- All children of a scatter carry the child node's tag. Tell them apart by
  `workflow.child_index`.
- A retried job keeps its tag.
- The run-level `tags` from `POST /workflow/runs` stay on the run doc and are not copied onto
  jobs.
- To find every job of a run, filter job status on `workflow.run_id` (add `workflow.step_id`
  for one step). Don't rely on tags for that.

### 3.6 A complete chain: L1A to L2_SM_P

A per-half-orbit radiometer chain, L0B → L1A → L1B_TB → L1C_TB → L2_SM_P. The L0B granule is
a run input. `L1B` retries a transient exit code or a lost worker twice, and the whole run is
capped at 6 hours.

```
L1A ─▶ to_L1B ─▶ L1B ─▶ to_L1C ─▶ L1C ─▶ to_L2 ─▶ L2SMP
```

```json
{
  "id": "radiometer-l2smp", "version": "1.0.0", "schema_version": "1.0",
  "description": "Per half-orbit radiometer chain: L0B -> L1A -> L1B_TB -> L1C_TB -> L2_SM_P",
  "defaults": { "queue": "sps-job_worker-large", "timeout_hours": 6 },
  "inputs": {
    "l0b_granule": { "type": "string" },
    "half_orbit":  { "type": "string" },
    "crid":        { "type": "string", "default": "R19240" },
    "ancillary":   { "type": "string" }
  },
  "entry": "L1A",
  "nodes": {
    "L1A":      { "kind": "job", "run": "job-l1a_radiometer:R19", "tag": "L1A_Radiometer",
                  "in": { "l0b_granule": "$.inputs.l0b_granule", "half_orbit": "$.inputs.half_orbit",
                          "crid": "$.inputs.crid" },
                  "out": ["l1a_file"], "next": ["to_L1B"] },
    "to_L1B":   { "kind": "pass", "bind": { "L1B.l1a_file": "$.L1A.l1a_file" }, "next": ["L1B"] },
    "L1B":      { "kind": "job", "run": "job-l1b_tb:R19", "tag": "L1B_TB",
                  "max_retries": 2, "retry_on": ["exit_code:75", "offline"],
                  "in": { "l1a_file": "$.to_L1B.l1a_file", "ancillary": "$.inputs.ancillary",
                          "crid": "$.inputs.crid" },
                  "out": ["l1b_file", "rfi_flagged_pct"], "next": ["to_L1C"] },
    "to_L1C":   { "kind": "pass", "bind": { "L1C.l1b_file": "$.L1B.l1b_file" }, "next": ["L1C"] },
    "L1C":      { "kind": "job", "run": "job-l1c_tb:R19", "tag": "L1C_TB",
                  "in": { "l1b_file": "$.to_L1C.l1b_file", "grid": "EASE2_M36km", "crid": "$.inputs.crid" },
                  "out": ["l1c_file"], "next": ["to_L2"] },
    "to_L2":    { "kind": "pass", "bind": { "L2SMP.l1c_file": "$.L1C.l1c_file" }, "next": ["L2SMP"] },
    "L2SMP":    { "kind": "job", "run": "job-l2_sm_p:R19", "tag": "L2_SM_P",
                  "in": { "l1c_file": "$.to_L2.l1c_file", "ancillary": "$.inputs.ancillary",
                          "crid": "$.inputs.crid" },
                  "out": ["l2_file", "retrieval_qual_pct"], "next": [] }
  },
  "outputs": { "l2_sm_p": "$.L2SMP.l2_file", "l1c_tb": "$.L1C.l1c_file",
               "rfi_flagged_pct": "$.L1B.rfi_flagged_pct" }
}
```

### 3.7 Sequence-only chains (no outputs)

Some PGE chains already hand products to each other through storage they agree on, and you
only need them to run in order. To do that, give every job `"out": []` and use passes
without a `bind`. No job writes `_workflow_outputs.json`.

```
L1B ─▶ seq_1 ─▶ L1C ─▶ seq_2 ─▶ L2SMP        (no values flow along the arrows)
```

```json
{
  "id": "radiometer-l2smp-shared", "version": "1.0.0", "schema_version": "1.0",
  "label": "Half-orbit L1B_TB -> L1C_TB -> L2_SM_P, products handed over on shared storage",
  "defaults": { "queue": "sps-job_worker-large", "timeout_hours": 6 },
  "inputs": {
    "half_orbit": { "type": "string" },
    "crid":       { "type": "string", "default": "R19240" },
    "shared_dir": { "type": "string", "default": "/data/wf-shared" }
  },
  "entry": "L1B",
  "nodes": {
    "L1B":   { "kind": "job", "run": "job-l1b_tb_shared:R19", "tag": "L1B_TB",
               "in": { "half_orbit": "$.inputs.half_orbit", "crid": "$.inputs.crid",
                       "shared_dir": "$.inputs.shared_dir" },
               "out": [], "next": ["seq_1"] },
    "seq_1": { "kind": "pass", "bind": {}, "next": ["L1C"] },
    "L1C":   { "kind": "job", "run": "job-l1c_tb_shared:R19", "tag": "L1C_TB",
               "in": { "half_orbit": "$.inputs.half_orbit", "crid": "$.inputs.crid",
                       "shared_dir": "$.inputs.shared_dir", "grid": "EASE2_M36km", "after": "L1B" },
               "out": [], "next": ["seq_2"] },
    "seq_2": { "kind": "pass", "next": ["L2SMP"] },
    "L2SMP": { "kind": "job", "run": "job-l2_sm_p_shared:R19", "tag": "L2_SM_P",
               "in": { "half_orbit": "$.inputs.half_orbit", "crid": "$.inputs.crid",
                       "shared_dir": "$.inputs.shared_dir", "after": "L1C" },
               "out": [], "next": [] }
  },
  "outputs": {}
}
```

How the jobs find each other's products:

- Each job reads `workflow.run_id` from its `_job.json` ([§2.4](#24-knowing-youre-running-in-a-workflow))
  and works under `<shared_dir>/<run_id>/`. That directory is unique per run, so concurrent
  runs don't collide.
- `L1B` writes its L1B_TB granule there under the agreed half-orbit name. `L1C` reads it and
  writes the L1C_TB granule, and `L2SMP` reads that.
- `after` is a plain literal (`"L1B"`, `"L1C"`). The job uses it to check that the step
  before it finished, for example by looking for `<shared_dir>/<run_id>/L1B.done`. If the file
  is missing, exit non-zero so the run halts ([§5.1](#51-implicit-halt)).

Things to know:

- `shared_dir` must be mounted on every worker that serves the queue. The orchestrator
  doesn't move these files and doesn't publish them for you.
- `seq_1` and `seq_2` declare no outputs, so a ref like `$.seq_1.x` is rejected at
  registration. With no job outputs, `spec.outputs` is `{}`.
- A retry reruns the job in the same run, with the same `run_id`, so it sees what the failed
  attempt left behind. Write to a temporary name and rename it when done.
- `bind: {}` and an omitted `bind` mean the same thing. A `bind` that isn't an object, such as
  `[]`, is rejected.

A version of this spec that is tested end to end is `wf-e2e/spec-noout-1.2.json` in
local-stack (`e2e-noout:1.2.0`). It runs A → pass_1 (`"bind": {}`) → B → pass_2 (no `bind`)
→ C.

---

## 4. Fan-out: scatter and gather

A daily soil-moisture composite. `find_L1C` lists the day's L1C_TB half-orbit granules. The
scatter runs `L2SMP` once per granule, the gather collects the L2_SM_P files, and `L3SMP`
composites them into the daily L3_SM_P product. The pass also hands L3 the number of
half-orbits that made it in.

```
find_L1C ─▶ per_half_orbit (scatter) ─▶ L2SMP × N ─▶ collect_L2 (gather) ─▶ to_L3 ─▶ L3SMP
```

```json
{
  "id": "radiometer-l3smp-daily", "version": "1.0.0", "schema_version": "1.0",
  "description": "Daily composite: L2_SM_P for every half-orbit L1C_TB of a day, then L3_SM_P",
  "defaults": { "queue": "sps-job_worker-large", "timeout_hours": 12 },
  "inputs": {
    "date":      { "type": "string" },
    "crid":      { "type": "string", "default": "R19240" },
    "ancillary": { "type": "string" }
  },
  "entry": "find_L1C",
  "nodes": {
    "find_L1C": { "kind": "job", "run": "job-list_l1c_tb:R19", "tag": "find_L1C_TB",
                  "queue": "sps-job_worker-small",
                  "in": { "date": "$.inputs.date", "crid": "$.inputs.crid" },
                  "out": ["l1c_files", "count"], "next": ["per_half_orbit"] },
    "per_half_orbit": { "kind": "scatter", "child": "L2SMP", "over": "$.find_L1C.l1c_files",
                  "completion": { "threshold": 0.9 }, "next": ["collect_L2"] },
    "L2SMP":    { "kind": "job", "run": "job-l2_sm_p:R19", "tag": "L2_SM_P",
                  "max_retries": 2, "retry_on": ["exit_code:75", "offline"],
                  "in": { "l1c_file": "$.scatter.item", "ancillary": "$.inputs.ancillary",
                          "crid": "$.inputs.crid" },
                  "out": ["l2_file"], "next": [] },
    "collect_L2": { "kind": "gather", "collect": "$.L2SMP.l2_file", "next": ["to_L3"] },
    "to_L3":    { "kind": "pass", "bind": { "L3SMP.l2_files": "$.collect_L2.l2_file",
                                             "L3SMP.n_half_orbits": "$.collect_L2.n_completed" },
                  "next": ["L3SMP"] },
    "L3SMP":    { "kind": "job", "run": "job-l3_sm_p:R19", "tag": "L3_SM_P",
                  "in": { "l2_files": "$.to_L3.l2_files", "n_half_orbits": "$.to_L3.n_half_orbits",
                          "date": "$.inputs.date", "crid": "$.inputs.crid" },
                  "out": ["l3_file"], "next": [] }
  },
  "outputs": { "l3_sm_p": "$.L3SMP.l3_file", "l2_sm_p": "$.collect_L2.l2_file",
               "n_half_orbits": "$.collect_L2.n_completed" }
}
```

**How it runs:**

1. When `find_L1C` completes, the scatter resolves `over`. With the default connector, the
   value **must be a list**, or the run halts. Each item can be any JSON value. A `string[]`
   run input works too: `"over": "$.inputs.files"`.
2. One `L2SMP` job is submitted per granule, and the item is bound to `$.scatter.item`.
   Children run in parallel, as far as the queue's workers allow.
3. The gather **waits until every child is terminal**, after its retries are used up. Then
   it checks the threshold:
   - If `completed / N ≥ threshold`, the run continues. Each collected output is a list of
     the **completed** children's values, **in item order**. Failed children are left out,
     so the list can be shorter than N. The gather also outputs the counts `n_completed`
     and `n_failed`.
   - Otherwise the run halts. With 26 of 30 half-orbits done against a 0.9 threshold, the
     error is `scatter L2SMP: 26/30 completed < threshold 0.9`.
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
| `retry_on` | `[]` | The error classes ([§2.5](#25-telling-the-retry-policy-why-you-failed-optional)) worth retrying. |

Set them on a job node, or in `defaults` for every job node. A node's value replaces the
default; the two aren't merged. A failure is retried when **attempts so far < `max_retries`
and its class matches `retry_on`**.

`retry_on` patterns: `exit_code:<n>`, `exit_code:*`, `timeout`, `signal`, `offline`,
`worker`, `pge:<token>`, `pge:*` and `*` (anything). Invalid patterns are rejected at
registration.

A retry re-runs the same job. It keeps its `job_id` and `payload_id`, its job status doc is
overwritten, and `job.retry_count` goes up by one. Earlier attempts' errors aren't kept on the
job doc. Scatter children are retried individually, and a child counts as failed only when
its last attempt fails.

> **Operator note:** workflow jobs are retried **only by their workflow**. A retry started
> from a user rule or the UI is skipped (and logged) for any job that has a `workflow`
> block ([§2.4](#24-knowing-youre-running-in-a-workflow)).

Good defaults: `["offline"]` for transient infrastructure failures, plus the specific
`pge:` classes your PGE emits for transient failures. Avoid `*` on expensive steps,
because it retries real bugs too.

### 5.3 Run timeout and stalls

- `defaults.timeout_hours` caps the **whole run**. The deployment default is 48h, set by
  `WORKFLOW_RUN_TIMEOUT_HOURS`. When the cap is hit, the reaper fails the run with
  `run timed out after <h>h` and revokes its jobs. There's no per-node timeout: `timeout_hours`
  on a job node is rejected at registration. To limit a single job, use the job-spec's
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
| `POST /workflow/runs` `{spec_id, inputs, user?, tags?}` | Start a run. `tags` is an optional list of non-empty strings for finding runs later. Returns `202 {run_id}`, `400` for bad inputs or tags, or `404` for an unknown spec. |
| `GET /workflow/runs/<run_id>` | Get the status doc (below). |
| `GET /workflow/runs/<run_id>/steps` | Job counts per step and status, e.g. `{"L2SMP": {"job-completed": 26, "job-started": 4}}`. Use it for live progress, since the doc only shows node state. |
| `POST /workflow/runs/<run_id>/cancel` | Cancel a run ([§5.4](#54-cancelling)). |

**Status doc** (`.result`). This one is for a run that failed below its scatter threshold:

```json
{ "run_id": "run-…", "spec_id": "radiometer-l3smp-daily:1.0.0",
  "spec_name": "radiometer-l3smp-daily", "spec_version": "1.0.0", "has_scatter": true,
  "status": "failed", "username": "jdoe", "tags": ["R19-reprocessing"],
  "inputs": { "date": "2026-11-12", "crid": "R19240", "ancillary": "…" },
  "submitted_at": "…Z", "updated_at": "…Z", "ended_at": "…Z", "duration_s": 5412.8,
  "current_nodes": [], "failed_node": "L2SMP", "error_class": "threshold",
  "error": "step L2SMP failed: scatter L2SMP: 26/30 completed < threshold 0.9", "outputs": null,
  "nodes": {
    "find_L1C":       { "state": "completed", "job_id": "…", "started_at": "…Z", "ended_at": "…Z",
                        "outputs": { "l1c_files": [ … ], "count": 30 } },
    "per_half_orbit": { "state": "completed", "outputs": { "n": 30 } },
    "L2SMP":          { "state": "failed", "expected": 30, "threshold": 0.9,
                        "attempts": [ { "job_id": "…", "payload_id": "…", "status": "job-failed",
                                        "error": "…", "error_class": "exit_code:75", "retry_count": 0,
                                        "ended_at": "…Z", "child_index": 9 } ] },
    "collect_L2": { "state": "pending" }, "to_L3": { "state": "pending" }, "L3SMP": { "state": "pending" } } }
```

**Run fields**

| field | meaning |
|---|---|
| `status` | `running`, `completed`, `failed` or `cancelled`. The last three are terminal. |
| `spec_id`, `spec_name`, `spec_version` | The spec, as `name:version` and split into its two parts. |
| `has_scatter` | `true` if the spec has a scatter node. |
| `username`, `tags` | Who started the run, and the tags from the start request (`[]` if none). |
| `inputs` | The resolved run inputs, with defaults applied. |
| `submitted_at`, `updated_at` | When the run started and when the doc last changed. |
| `ended_at`, `duration_s` | When the run became terminal (set once, never overwritten), and seconds from `submitted_at` to `ended_at`. |
| `current_nodes` | The nodes queued or running right now. `[]` once the run is terminal. |
| `error`, `failed_node`, `error_class` | Why the run didn't complete. See below. When a run fails, nodes after the failure stay `pending`; a cancel or timeout sets them to `cancelled`. |
| `outputs` | The resolved `spec.outputs`, set when the run completes. |

**`error_class` on a run that didn't complete**

| value | meaning | `failed_node` |
|---|---|---|
| the job's own class, e.g. `exit_code:1`, `worker`, `pge:throttled` | A job failed and wasn't retried ([§2.5](#25-telling-the-retry-policy-why-you-failed-optional)). | the failing job node |
| `threshold` | Too few scatter children completed. | the scatter child node |
| `timeout` | The run hit `defaults.timeout_hours`. | the first node that was running |
| `cancelled` | Someone cancelled the run. | none |
| `input_resolution` | A node's `in` or `bind` refs couldn't be resolved. | the node |
| `output_resolution` | `spec.outputs` couldn't be resolved at the end of the run. | the last node |
| `submit` | Mozart couldn't submit the entry job. | the entry node |
| `no_queue` | No queue could be found for a job. | the node |
| `unknown` | The failure carried no class. | the failing node |

**Node fields** (`nodes.<id>`). These are for display, and aren't searchable.

| field | meaning |
|---|---|
| `state` | `pending`, `queued`, `running`, `completed`, `failed` or `cancelled`. `queued` means the node's params are resolved and its submit is imminent. Jobs go from `queued` straight to `completed` or `failed`, so use `/steps` to see progress. |
| `started_at` | When the node was queued or started. For job nodes after the entry, it's the time the node was queued. |
| `ended_at` | When the node reached a terminal state. A node cancelled before it started has none. |
| `job_id`, `payload_id`, `outputs` | The job that ran the node, and its outputs. Scatter children don't store per-child outputs; the gather holds them. |
| `expected`, `completed`, `failed`, `threshold` | On a scatter child node: the number of children, how many completed and failed (set only when the gather runs), and the threshold. |
| `attempts` | One entry per failed attempt that was retried by `retry_on`, newest 100 kept: `job_id`, `payload_id`, `status`, `error`, `error_class`, `retry_count`, `ended_at`, and `child_index` for scatter children. Use it to see why earlier attempts failed, since the job doc only keeps the last one. |

**Run inputs** are checked against the spec when the run starts. Unknown names, missing
required inputs and type mismatches all return `400` with the full list.

**Queues.** Every job, the entry included, runs on its node's `queue`, else
`defaults.queue`, else the job-spec's first `recommended-queues` entry. If none of these
exists, starting a run returns `400` (for the entry job). For a later job, the run fails
with `error_class: no_queue`. Setting `defaults.queue` is the simplest choice.

---

## 7. Custom connectors

`pass`, `scatter` and `gather` nodes run as small HySDS jobs called **connectors**. A
mission can supply its own policy, such as a scatter that enumerates a STAC search, a
gather that reduces instead of collecting, or a pass that looks up the previous job's
products. To use one, name it on the node:

```json
"per_half_orbit": { "kind": "scatter", "connector": "job-stac-scatter:v1.0", "child": "L2SMP",
                    "over": "$.inputs.stac_query", "next": ["collect_L2"] }
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
| `refers to output 'y' not declared by node 'X'` | Add `y` to X's `out`. A gather declares only its `collect` outputs plus `n_completed` and `n_failed`. |
| `node X: timeout_hours is not supported per node in v1; use defaults.timeout_hours …` | Move it to `defaults.timeout_hours` (the run timeout), or set the job-spec's `time_limit`. |
| `refers to scatter child 'P', whose outputs are not stored` | In `spec.outputs` or downstream, reference the gather instead (`$.G.y`). |
| `'$.scatter.item' is only allowed in a scatter child's 'in'` | Move the ref into the child's `in`. |
| `bind key 'C.x' targets 'C' but next is 'B'` | A pass binds only its own successor's params. |
| `'bind' must be an object (or omitted/{} for a pure sequencing pass)` | Use an object, `{}`, or leave `bind` out. |
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
per-run overrides, spec permissions, and a CWL adapter.

## See also

- Runtime contract (authoritative behavior): `hysds-workflow-docs` repo,
  `PLAN-workflow-orchestration-v1-contract.md`
- Output binding design: ADR-003, *Workflow Output Binding via Job-Scoped `_workflow_outputs.json`*
- Fixture specs: `hysds_commons.workflow_examples` and the `local-stack/wf-e2e/spec-*.json`
  files (linear, scatter/gather, retry, File/Directory outputs, sequence-only)
