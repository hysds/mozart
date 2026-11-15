# Writing HySDS Workflow Specs

A **workflow spec** is a JSON document that chains HySDS jobs into a pipeline. You register
it with mozart once, then start runs of it with inputs. HySDS submits each job when its
predecessor finishes, passes outputs forward, fans work out over lists, retries flaky steps,
and records the run's state in one status document.

This guide is for mission developers who write specs and make their PGEs workflow-ready.
It covers **schema version 1.0** (the `feature/workflow-orchestration-v1` branch).

> **v1 in one paragraph.** A run is a line of jobs, with optional scatter/gather fan-out
> over a list. Since v1.2 a `switch` or `gate` can route the run down one of several arms,
> the arms can rejoin, and a `halt` can end the run early. There are no parallel branches,
> other joins or loops. A job that fails for good halts the run. Every error in a spec is
> reported together when you register it, so a spec that registers is structurally sound.

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
([§6.2](#62-retries)):

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
| `defaults` | no | `queue`, `max_retries` and `retry_on`, which each job node can override, plus `timeout_hours`, the run timeout ([§6.3](#63-run-timeout-and-stalls)). |
| `outputs` | no | `{name: "$.<node>.<output>"}`. They're resolved into the run's `outputs` when it completes. |
| `label`, `description` | no | Free text. These are also allowed on every node. |

**Unknown fields are rejected**, both at the top level and on nodes. That catches typos like
`"nxt"`.

### 3.2 Node kinds

| kind | required fields | optional | `next` must be |
|---|---|---|---|
| `job` | `run` (a job_spec id, e.g. `job-l1b_tb:R19`), `in`, `out` (list of output names) | `tag`, `queue`, `max_retries`, `retry_on` | `[]`, or one `pass`, `scatter`, `switch`, `gate` or `halt` |
| `pass` | none | `bind`: `{"<next_node>.<param>": "$.ref"}`, or `{}` / omitted for a pass that only orders steps; `connector` | exactly one `job`, the one named in every bind key |
| `scatter` | `child` (a `job` node id), `over` (usually a `$`-ref to a list) | `completion.threshold` in (0, 1], default `1.0`; `connector` | exactly one `gather` |
| `gather` | `collect`: `"$.<child>.<output>"` or a list of them | `connector` | `[]`, or one `pass`, `switch`, `gate` or `halt` |
| `switch` | `default`, plus either `cases` or `on` + `choices` ([§5.2](#52-switch)) | `connector` | no `next`; each arm names one `job`, `pass`, `scatter`, `switch`, `gate` or `halt` |
| `gate` | `when`, `on_true`, `on_false` ([§5.4](#54-gate)) | `connector` | no `next`; arms as for `switch` |
| `halt` | `status` (`failed` or `completed`), `reason` ([§5.5](#55-halt)) | none | no `next`; it ends the run |

`next` may be omitted, which means `[]`. A node with `next: []` ends the run. The run
completes when that node completes.

**Allowed shapes.** Each kind may point only to certain kinds:

| from | may point to |
|---|---|
| `job` | `pass`, `scatter`, `switch`, `gate`, `halt` |
| `pass` | `job` |
| `scatter` | `gather` |
| `gather` | `pass`, `switch`, `gate`, `halt` |
| `switch` / `gate` arm | `job`, `pass`, `scatter`, `switch`, `gate`, `halt` |

Without routing, that gives the familiar line:
`job → (pass → job)* → [scatter → gather → (pass → job)*] → …end`.

- **A job can't link straight to another job.** Put a `pass` between them, even if it just
  forwards a value or, with no `bind`, only sets the order (see
  [§3.7](#37-sequence-only-chains-no-outputs)).
- **A scatter child is owned by its scatter.** It must have `next: []`, must not appear in any
  other node's `next`, and can't be the entry.
- **One successor per node, and one target per switch arm.** Parallel branches and cycles
  are rejected.
- **One predecessor per node**, except an **exclusive rejoin**: a node reached from different
  arms of one switch ([§5.6](#56-rejoining-arms-and-what-a-node-can-reference)). Other joins
  are rejected.
- **The entry is a job.** A scatter child is a job too.

### 3.3 References (`$`-refs)

Any string value that starts with `$.` is a reference. Every other value is a literal.

| ref | resolves to | where it's allowed |
|---|---|---|
| `$.inputs.<name>` | the run input, with defaults applied | anywhere |
| `$.<node>.<output>` | that node's output | the node must lie on **every path** from `entry` to the node using the ref (with no switches, that's any upstream node), and must declare the output |
| `$.scatter.item` | this child's item from the scatter list | only in a scatter child's `in` |

Which outputs a node declares depends on its kind:

- a `job` declares its `out` list
- a `pass` declares the `<param>` part of each bind key, so `"L1C.l1b_file"` declares `l1b_file`.
  A pass with an empty or omitted `bind` declares nothing.
- a `switch` or `gate` declares `arm` and `target` ([§5.2](#52-switch))
- a `halt` declares nothing
- a `gather` declares the `<output>` part of each `collect` ref, whose value is a **list**.
  It also always declares `n_completed` and `n_failed`, the integer child counts.

**Rules to remember:**

- Refs can sit at any depth in `in`, inside nested objects and lists:
  `{"ancillary": {"dem": "$.inputs.dem", "landcover": ["$.inputs.igbp", "$.inputs.ndvi"]}}`.
- A ref replaces the **whole string**. There's no interpolation: `"CRID-$.inputs.crid"`
  is a literal.
- Where arms of a switch rejoin, `{"first_of": ["$.a.x", "$.b.x"]}` takes the value from the
  first listed node that completed
  ([§5.6](#56-rejoining-arms-and-what-a-node-can-reference)).
- Values keep their JSON type. A ref to a `value_d` list yields a list of numbers, not a
  string.
- You can reference **any** upstream node on every path, not just the one right before. In the chain in
  [§3.6](#36-a-complete-chain-l1a-to-l2_sm_p), `L2SMP` could read `$.L1B.l1b_file` directly.
- A scatter child can reference upstream nodes of its scatter, such as `$.inputs.*` and the
  upstream job's outputs.
- `spec.outputs` can reference any node except a scatter child, because per-child outputs
  aren't stored. Reference the gather instead. A ref into a node a switch skipped resolves
  to `null`.
- `$.G.n_completed` and `$.G.n_failed` resolve to plain integers, so a later job can be told
  how many children succeeded or failed.

### 3.4 What `pass` is for

With the default connector, a `pass` just copies values under new names. L1C could equally
reference `$.L1B.l1b_file` directly. The pass exists for two reasons: it's the required hop
between two jobs, and it's the extension point for a mission **pass connector** that
computes the next job's params. A connector can rename, reshape or look up the previous
job's published products. See [§8](#8-custom-connectors).

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
  is missing, exit non-zero so the run halts ([§6.1](#61-implicit-halt)).

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
or a GRQ query, use a mission scatter connector ([§8](#8-custom-connectors)).

---

## 5. Routing: switch, gate and halt

Three node kinds let a run choose where to go next:

- **`switch`** picks exactly one of several **arms** from a value: an upstream job's output or
  a run input.
- **`gate`** is a two-arm switch: true or false.
- **`halt`** ends the run on purpose, as `failed` or `completed`, with a reason.

Every possible target is named in the spec, so the whole graph is checked at registration.
A job never names the next node itself; at most it emits a **label**, and the spec maps
labels to targets ([§5.3](#53-routing-on-a-computed-label)). Nodes on the arms that weren't
taken become **`skipped`**. The spec's `schema_version` stays `"1.0"`.

### 5.1 Example: QA-routed L2_SM_P

`L2SMP` writes a `qa_score` output. The switch `qa_route` sends the run one of three ways:

```
                         ┌─ reject     (qa_score < 0.3) ──▶ reject  [halt: failed]
                         │
 L2SMP ──▶ qa_route ─────┼─ reprocess  (0.3 ≤ qa_score < qa_min) ──▶ reprocess_L2SMP ──▶ to_deliver ──┐
         [switch]        │                                                                [pass]      │
                         └─ default    (anything else) ───────────────────────────────────────────────┴──▶ deliver
```

```json
{
  "id": "radiometer-l2smp-qa", "version": "1.0.0", "schema_version": "1.0",
  "label": "L2_SM_P with QA routing: reject, reprocess, or deliver",
  "defaults": { "queue": "sps-job_worker-large", "timeout_hours": 8 },
  "inputs": {
    "l1c_file":   { "type": "string" },
    "crid":       { "type": "string", "default": "R19240" },
    "ancillary":  { "type": "string" },
    "qa_min":     { "type": "float",  "default": 0.8 }
  },
  "entry": "L2SMP",
  "nodes": {
    "L2SMP":    { "kind": "job", "run": "job-l2_sm_p:R19", "tag": "L2_SM_P",
                  "in": { "l1c_file": "$.inputs.l1c_file", "ancillary": "$.inputs.ancillary",
                          "crid": "$.inputs.crid" },
                  "out": ["l2_file", "qa_score"], "next": ["qa_route"] },
    "qa_route": { "kind": "switch",
                  "cases": [
                    { "name": "reject",
                      "when": { "left": "$.L2SMP.qa_score", "op": "lt", "right": 0.3 },
                      "next": ["reject"] },
                    { "name": "reprocess",
                      "when": { "all": [
                        { "left": "$.L2SMP.qa_score", "op": "ge", "right": 0.3 },
                        { "left": "$.L2SMP.qa_score", "op": "lt", "right": "$.inputs.qa_min" } ] },
                      "next": ["reprocess_L2SMP"] } ],
                  "default": ["deliver"] },
    "reject":   { "kind": "halt", "status": "failed",
                  "reason": "L2_SM_P retrieval quality below 0.3" },
    "reprocess_L2SMP": { "kind": "job", "run": "job-l2_sm_p:R19", "tag": "L2_SM_P_reprocess",
                         "in": { "l1c_file": "$.inputs.l1c_file", "ancillary": "$.inputs.ancillary",
                                 "crid": "$.inputs.crid", "rfi_filter": "aggressive" },
                         "out": ["l2_file"], "next": ["to_deliver"] },
    "to_deliver": { "kind": "pass", "next": ["deliver"] },
    "deliver":  { "kind": "job", "run": "job-l2_deliver:R19", "tag": "L2_SM_P_deliver",
                  "in": { "l2_file": { "first_of": ["$.reprocess_L2SMP.l2_file", "$.L2SMP.l2_file"] },
                          "crid": "$.inputs.crid", "route": "$.qa_route.arm" },
                  "out": ["delivered_granule"], "next": [] }
  },
  "outputs": { "route": "$.qa_route.arm",
               "l2_sm_p": { "first_of": ["$.reprocess_L2SMP.l2_file", "$.L2SMP.l2_file"] },
               "reprocessed": "$.reprocess_L2SMP.l2_file",
               "delivered": "$.deliver.delivered_granule" }
}
```

| `qa_score` | arm (`$.qa_route.arm`) | runs | skipped | run ends |
|---|---|---|---|---|
| `0.1` | `reject` | `reject` | `reprocess_L2SMP`, `to_deliver`, `deliver` | `failed`, `error_class: halted`, `error: "halted at reject: L2_SM_P retrieval quality below 0.3"` |
| `0.5` | `reprocess` | `reprocess_L2SMP`, `to_deliver`, `deliver` | `reject` | `completed` |
| `0.9` | `default` | `deliver` | `reject`, `reprocess_L2SMP`, `to_deliver` | `completed`; `outputs.reprocessed` is `null` |
| missing or `null` | none | nothing | nothing | `failed`, `error_class: input_resolution` |
| `"high"` (a string) | none | nothing | nothing | `failed`, `error_class: routing` |

`deliver` is reached from two arms. That's an **exclusive rejoin**, and it's allowed because
only one arm can fire ([§5.6](#56-rejoining-arms-and-what-a-node-can-reference)). `deliver`
gets its granule through **`first_of`**: the reprocessed `l2_file` if `reprocess_L2SMP` ran,
otherwise the original from `L2SMP`. The run output `l2_sm_p` uses the same `first_of`, so it
always names the granule that was delivered.

### 5.2 `switch`

A switch has **either** `cases` **or** `on` + `choices`, plus a `default`, which is required.

**`cases`**: a list of `{name?, when, next}`, tried top-down. The first `when` that is true
wins. If none is true, the run takes `default`.

- `name` is the arm's label. It matches `^[A-Za-z0-9_.:-]{1,128}$`, is unique in the switch, and
  defaults to `case_<i>` (`case_0`, `case_1`, …). `default` is reserved.
- `when` is a tree. A leaf is `{"left": …, "op": …, "right": …}`; `left` and `right` are each a
  `$`-ref or a JSON literal. Combine leaves with `{"all": [ … ]}`, `{"any": [ … ]}` (both
  non-empty) and `{"not": { … }}`. Nesting is capped at 8 levels. There's no expression string
  to parse.

| `op` | true when | operands |
|---|---|---|
| `eq`, `ne` | equal / not equal | any JSON values |
| `lt`, `le`, `gt`, `ge` | `<`, `≤`, `>`, `≥` | two numbers or two strings. A bool isn't a number. Anything else fails the run with `error_class: routing`. |
| `in` | `left` is an element of `right` | `right` is a list literal, or a ref that resolves to a list |

`all` and `any` stop at the first clause that decides them, so a ref in a clause that's never
evaluated can't fail the run.

**`on` + `choices`**: `on` is one `$`-ref. Its value is turned into JSON text and looked up
as a key in `choices`: strings as they are, `true` → `"true"`, `1` → `"1"`, `1.0` → `"1.0"`. A
value with no matching key takes `default`, so a typo in a label never strands the run.
`choices` keys follow the same pattern as case names.

**Arms.** `cases[i].next`, each `choices` value and `default` are each a list with **exactly
one** node id. Two arms may name the same target. An arm can point to a `job`, `pass`,
`scatter`, `switch`, `gate` or `halt`. When an arm points straight at a job, that job's `in`
is resolved as it would be after a pass.

**Outputs.** A switch declares two outputs: `arm`, the label taken (`"reprocess"`,
`"default"`, …), and `target`, the node it chose. Later nodes can read them as
`$.qa_route.arm` and `$.qa_route.target`.

**Other fields.** A switch takes `label`, `description` and `connector`. It has no `tag`:
its connector job is tagged with the node id, like any connector ([§3.5](#35-job-tags)).

### 5.3 Routing on a computed label

When the decision needs real work, such as opening a file, running a classifier or calling a
service, put it in an ordinary **decision job** that writes a label output. Then switch on that
label. Here an RFI classifier picks the L2_SM_P path for each L1C_TB granule:

```
                                                            ┌─ clean      ──▶ L2SMP
 L1C ──▶ to_classify ──▶ classify_rfi ──▶ rfi_route ────────┼─ rfi        ──▶ L2SMP_rfi
        [pass]          [decision job]   [switch on]        ├─ quarantine ──▶ quarantine  [halt: completed]
                                                            └─ default    ──▶ unrouted    [halt: failed]
```

```json
{
  "id": "radiometer-l1c-rfi-route", "version": "1.0.0", "schema_version": "1.0",
  "label": "L1C_TB, then an RFI classifier picks the L2_SM_P path",
  "defaults": { "queue": "sps-job_worker-large" },
  "inputs": {
    "l1b_file":  { "type": "string" },
    "crid":      { "type": "string", "default": "R19240" },
    "ancillary": { "type": "string" }
  },
  "entry": "L1C",
  "nodes": {
    "L1C":          { "kind": "job", "run": "job-l1c_tb:R19", "tag": "L1C_TB",
                      "in": { "l1b_file": "$.inputs.l1b_file", "grid": "EASE2_M36km", "crid": "$.inputs.crid" },
                      "out": ["l1c_file"], "next": ["to_classify"] },
    "to_classify":  { "kind": "pass", "bind": { "classify_rfi.l1c_file": "$.L1C.l1c_file" },
                      "next": ["classify_rfi"] },
    "classify_rfi": { "kind": "job", "run": "job-rfi_classifier:v2.0", "tag": "RFI_classify",
                      "in": { "l1c_file": "$.to_classify.l1c_file" },
                      "out": ["outcome"], "next": ["rfi_route"] },
    "rfi_route":    { "kind": "switch", "on": "$.classify_rfi.outcome",
                      "choices": { "clean": ["L2SMP"], "rfi": ["L2SMP_rfi"], "quarantine": ["quarantine"] },
                      "default": ["unrouted"] },
    "L2SMP":        { "kind": "job", "run": "job-l2_sm_p:R19", "tag": "L2_SM_P",
                      "in": { "l1c_file": "$.L1C.l1c_file", "ancillary": "$.inputs.ancillary",
                              "crid": "$.inputs.crid" },
                      "out": ["l2_file"], "next": [] },
    "L2SMP_rfi":    { "kind": "job", "run": "job-l2_sm_p:R19", "tag": "L2_SM_P",
                      "in": { "l1c_file": "$.L1C.l1c_file", "ancillary": "$.inputs.ancillary",
                              "crid": "$.inputs.crid", "rfi_filter": "aggressive" },
                      "out": ["l2_file"], "next": [] },
    "quarantine":   { "kind": "halt", "status": "completed",
                      "reason": "RFI contamination; half-orbit quarantined, no L2_SM_P" },
    "unrouted":     { "kind": "halt", "status": "failed",
                      "reason": "classify_rfi returned an unknown outcome" }
  },
  "outputs": { "l2_sm_p": "$.L2SMP.l2_file", "l2_sm_p_rfi": "$.L2SMP_rfi.l2_file",
               "rfi_outcome": "$.classify_rfi.outcome" }
}
```

`classify_rfi` writes `{"name": "outcome", "value_s": "rfi"}` to `_workflow_outputs.json`
([§2.2](#22-outputs-out-_workflow_outputsjson)). An outcome that isn't `clean`, `rfi` or
`quarantine` takes `default` and fails the run with a clear reason. `quarantine` ends the run
as `completed`, with `halt_reason` set and `outputs.l2_sm_p` and `outputs.l2_sm_p_rfi` both
`null`. The arms point straight at jobs, with no pass. `L2SMP` and `L2SMP_rfi` read
`$.L1C.l1c_file`, which lies on every path to them.

### 5.4 `gate`

A gate is a switch with two arms, for a yes/no question. It takes a `when` tree, `on_true`
and `on_false`, and its arm labels are `true` and `false`:

```json
"qa_gate": { "kind": "gate",
             "when": { "left": "$.L2SMP.qa_score", "op": "ge", "right": "$.inputs.qa_min" },
             "on_true": ["deliver"], "on_false": ["reject"] }
```

Everything else about a switch applies: one target per arm, the `arm` and `target` outputs,
and skipping.

### 5.5 `halt`

```json
"reject": { "kind": "halt", "status": "failed", "reason": "L2_SM_P retrieval quality below 0.3" }
```

| field | |
|---|---|
| `status` | `failed` or `completed`. Required. |
| `reason` | A string of 1 to 512 characters. Required. |
| `label`, `description` | Optional. |

A halt has no `next` and runs no job of its own. When a run reaches one:

- **`failed`**: the run fails with `failed_node` set to the halt node, `error_class: halted`
  and `error: "halted at <id>: <reason>"`. The halt node's state is `failed`.
- **`completed`**: the run completes. Its `outputs` are resolved as usual, and `halt_reason`
  holds the reason. The halt node's state is `completed`.

`halt` and `_halt` can't be used as node ids.

### 5.6 Rejoining arms and what a node can reference

**Exclusive rejoin.** A node may have more than one predecessor only when each incoming edge
comes from a **different arm of the same switch** (or gate), and that switch lies on every
path to the node. `deliver` in [§5.1](#51-example-qa-routed-l2_sm_p) qualifies: one edge is
the `default` arm, and the other comes through `reprocess_L2SMP` → `to_deliver` on the
`reprocess` arm. Two edges from the same arm, or from nodes that aren't on switch arms at all,
are still a fan-in and are rejected. Because only one arm fires, a rejoined node runs once.

**References need a node on every path.** A node may reference `$.M.x` only if `M` lies on
**every** path from `entry` to it. In graph terms, `M` must dominate it. Without switches,
that is simply every upstream node, as before. With switches it rules out nodes that might
have been skipped:

- `deliver` **can** read `$.L2SMP.*`, `$.qa_route.arm` and `$.inputs.*`, which run on every
  path.
- `deliver` **can't** read `$.reprocess_L2SMP.l2_file`. On the `default` path that node is
  skipped, so the value wouldn't exist. Registration fails with
  `$.reprocess_L2SMP.l2_file refers to 'reprocess_L2SMP', which is not on every path to 'deliver' (it may be skipped)`.
- A bind on `to_deliver` doesn't help: `to_deliver` is on the same arm, so `$.to_deliver.*`
  is rejected the same way.
- A switch can't read its own arms' nodes either.

**`first_of`: take the value from whichever arm ran.** When the node after a rejoin needs a
product that depends on the arm, list the candidates in order of preference:

```json
"l2_file": { "first_of": ["$.reprocess_L2SMP.l2_file", "$.L2SMP.l2_file"] }
```

At run time the first entry whose node **completed** wins. Skipped, pending and running nodes
fall through to the next entry. It's modeled on CWL's `pickValue: first_non_null`.

- **Form.** An object with the single key `first_of` and a list of **2 or more** refs, each
  `$.<node>.<output>` or `$.inputs.<name>`. No literals, no `$.scatter.item` and no nesting.
- **Where.** In a job's `in` (at any depth), as a pass `bind` value, and in spec `outputs`.
  Switch and gate conditions, scatter `over` and gather `collect` reject it.
- **Order matters.** A node on every path to the owner always completes, and a `$.inputs`
  entry always resolves, so either one wins over anything listed after it. Registration
  rejects entries that could never be used. Put the arm-specific nodes first and the shared
  upstream node, or a run input as a fallback, **last**.
- **Coverage.** Every path to the owner must pass through a listed node, unless an
  `$.inputs` entry is listed. Otherwise some run would have nothing to pick. Each listed node
  must be upstream of the owner on at least one path, and must declare the output.
- **A completed node must deliver.** If the chosen node completed but its output is missing
  or `null`, the run fails with `error_class: input_resolution`; it doesn't fall through.
  `false` and `0` are real values. If no entry resolves, the run also fails with
  `input_resolution`.
- **In spec outputs** there's no coverage rule (but `$.inputs` entries still go last). If
  nothing resolves, the output is `null`.

**Alternative: don't rejoin; one pass per arm.** End each arm with its own pass and its own
copy of the next job. Each pass binds from its own arm, which lies on every path to that copy.
This suits arms whose next steps really differ:

```json
"qa_route":     { "kind": "switch", "cases": [ … as above … ], "default": ["to_deliver"] },
"to_deliver":   { "kind": "pass", "bind": { "deliver.l2_file": "$.L2SMP.l2_file" }, "next": ["deliver"] },
"deliver":      { "kind": "job", "run": "job-l2_deliver:R19", "tag": "L2_SM_P_deliver",
                  "in": { "l2_file": "$.to_deliver.l2_file" }, "out": ["delivered_granule"], "next": [] },
"reprocess_L2SMP": { …, "next": ["to_deliver_r"] },
"to_deliver_r": { "kind": "pass", "bind": { "deliver_r.l2_file": "$.reprocess_L2SMP.l2_file" },
                  "next": ["deliver_r"] },
"deliver_r":    { "kind": "job", "run": "job-l2_deliver:R19", "tag": "L2_SM_P_deliver",
                  "in": { "l2_file": "$.to_deliver_r.l2_file" }, "out": ["delivered_granule"], "next": [] }
```

**Spec outputs** may reference any node except a scatter child, including nodes that might
be skipped. A ref into a node that was skipped resolves to **`null`**, as `reprocessed` does on
the `default` path in §5.1. Use `first_of` when you want a fallback instead of `null`.

### 5.7 At run time

**Skipped nodes.** When a switch picks an arm, every node reachable only from the other arms
is set to `state: skipped` with `skipped_by: "<switch id>"`. That includes scatter children
under those arms. Skipped is terminal, a skipped node is never in `current_nodes`, and it has
no `started_at` or `ended_at`. A node that is reachable from the taken arm, such as a rejoin
target, stays `pending` until it runs.

**The switch node** completes with `outputs: {"arm": "<label>", "target": "<node id>"}`. It runs
as a connector job on the `workflow-connector-switch` queue, tagged with the node id.

**When the switch can't decide**, the run fails and nothing is routed:

| cause | `error_class` | example `error` |
|---|---|---|
| A ref the switch needs is missing or `null` | `input_resolution` | `step qa_route failed: qa_route: cannot resolve $.L2SMP.qa_score: value is null` |
| An operand has the wrong type for `op` | `routing` | `… 'lt' needs two numbers or two strings, got str and float (a bool is not a number)` |

`failed_node` is the switch, which is `failed` with no outputs. The node before it is still
recorded `completed` with its outputs, so you can see the value that broke the switch. Nothing
is skipped: the arms stay `pending`. A resolved value that simply matches nothing isn't an
error. It takes `default`.

---

## 6. Failure handling

### 6.1 Implicit halt

If a job fails and won't be retried, the run's `status` becomes `failed`. The failing node
gets `state: failed`, the run's `error` says why, and no further nodes are submitted. A
failed scatter child is the exception: its failure counts against the threshold instead
([§4](#4-fan-out-scatter-and-gather)).

### 6.2 Retries

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

### 6.3 Run timeout and stalls

- `defaults.timeout_hours` caps the **whole run**. The deployment default is 48h, set by
  `WORKFLOW_RUN_TIMEOUT_HOURS`. When the cap is hit, the reaper fails the run with
  `run timed out after <h>h` and revokes its jobs. There's no per-node timeout: `timeout_hours`
  on a job node is rejected at registration. To limit a single job, use the job-spec's
  `soft_time_limit`/`time_limit`.
- If a run makes no progress for 10 minutes (`WORKFLOW_STALL_SECONDS`), the reaper replays
  the last events of its finished steps. This recovers lost messages. You don't need to do
  anything.

### 6.4 Cancelling

```bash
curl -X POST -H 'content-type: application/json' -d '{"user": "jdoe"}' $M/runs/<run_id>/cancel
```

| response | meaning |
|---|---|
| `202 {"revoked": n}` | The run and every unfinished node are now `cancelled`. Queued jobs are revoked and running jobs are killed. |
| `200` | The run was already cancelled. This call only re-sweeps leftover jobs, so it's safe to repeat. |
| `409` | The run had already completed or failed. |

---

## 7. Runs: API and status

| method & path | purpose |
|---|---|
| `POST /workflow/specs` | Register a spec. Returns `201`, `400` with `errors[]`, or `409` if it exists. |
| `GET /workflow/specs`, `GET /workflow/specs/<id:version>` | List specs or fetch one. |
| `POST /workflow/runs` `{spec_id, inputs, user?, tags?}` | Start a run. `tags` is an optional list of non-empty strings for finding runs later. Returns `202 {run_id}`, `400` for bad inputs or tags, or `404` for an unknown spec. |
| `GET /workflow/runs/<run_id>` | Get the status doc (below). |
| `GET /workflow/runs/<run_id>/steps` | Job counts per step and status, e.g. `{"L2SMP": {"job-completed": 26, "job-started": 4}}`. Use it for live progress, since the doc only shows node state. |
| `POST /workflow/runs/<run_id>/cancel` | Cancel a run ([§6.4](#64-cancelling)). |

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
| `halt_reason` | Set when a `halt` node with `status: completed` ended the run: its `reason` ([§5.5](#55-halt)). |
| `spec_id`, `spec_name`, `spec_version` | The spec, as `name:version` and split into its two parts. |
| `has_scatter` | `true` if the spec has a scatter node. |
| `username`, `tags` | Who started the run, and the tags from the start request (`[]` if none). |
| `inputs` | The resolved run inputs, with defaults applied. |
| `submitted_at`, `updated_at` | When the run started and when the doc last changed. |
| `ended_at`, `duration_s` | When the run became terminal (set once, never overwritten), and seconds from `submitted_at` to `ended_at`. |
| `current_nodes` | The nodes queued or running right now. `[]` once the run is terminal. |
| `error`, `failed_node`, `error_class` | Why the run didn't complete. See below. When a run fails, nodes after the failure stay `pending`; a cancel or timeout sets them to `cancelled`. Nodes a switch skipped stay `skipped`. |
| `outputs` | The resolved `spec.outputs`, set when the run completes. |

**`error_class` on a run that didn't complete**

| value | meaning | `failed_node` |
|---|---|---|
| the job's own class, e.g. `exit_code:1`, `worker`, `pge:throttled` | A job failed and wasn't retried ([§2.5](#25-telling-the-retry-policy-why-you-failed-optional)). | the failing job node |
| `threshold` | Too few scatter children completed. | the scatter child node |
| `timeout` | The run hit `defaults.timeout_hours`. | the first node that was running |
| `cancelled` | Someone cancelled the run. | none |
| `input_resolution` | A node's `in` or `bind` refs couldn't be resolved, a switch's ref was missing or `null`, or a `first_of` found no completed node or picked one with a missing or `null` output. | the node |
| `routing` | A switch or gate couldn't compare its operands, e.g. `lt` on a string and a number ([§5.7](#57-at-run-time)). | the switch |
| `halted` | The run reached a `halt` with `status: failed`. `error` is `halted at <id>: <reason>`. | the halt node |
| `output_resolution` | `spec.outputs` couldn't be resolved at the end of the run. | the last node |
| `submit` | Mozart couldn't submit the entry job. | the entry node |
| `no_queue` | No queue could be found for a job. | the node |
| `unknown` | The failure carried no class. | the failing node |

**Node fields** (`nodes.<id>`). These are for display, and aren't searchable.

| field | meaning |
|---|---|
| `state` | `pending`, `queued`, `running`, `completed`, `failed`, `cancelled` or `skipped` (on an arm a switch didn't take). `queued` means the node's params are resolved and its submit is imminent. Jobs go from `queued` straight to `completed` or `failed`, so use `/steps` to see progress. |
| `started_at` | When the node was queued or started. For job nodes after the entry, it's the time the node was queued. |
| `ended_at` | When the node reached a terminal state. A node cancelled before it started has none. |
| `job_id`, `payload_id`, `outputs` | The job that ran the node, and its outputs. Scatter children don't store per-child outputs; the gather holds them. A switch or gate stores `{"arm", "target"}`. |
| `skipped_by` | On a `skipped` node: the switch or gate that skipped it. |
| `expected`, `completed`, `failed`, `threshold` | On a scatter child node: the number of children, how many completed and failed (set only when the gather runs), and the threshold. |
| `attempts` | One entry per failed attempt that was retried by `retry_on`, newest 100 kept: `job_id`, `payload_id`, `status`, `error`, `error_class`, `retry_count`, `ended_at`, and `child_index` for scatter children. Use it to see why earlier attempts failed, since the job doc only keeps the last one. |

**Run inputs** are checked against the spec when the run starts. Unknown names, missing
required inputs and type mismatches all return `400` with the full list.

**Queues.** Every job, the entry included, runs on its node's `queue`, else
`defaults.queue`, else the job-spec's first `recommended-queues` entry. If none of these
exists, starting a run returns `400` (for the entry job). For a later job, the run fails
with `error_class: no_queue`. Setting `defaults.queue` is the simplest choice.

---

## 8. Custom connectors

`pass`, `scatter`, `gather`, `switch` and `gate` nodes run as small HySDS jobs called **connectors**. A
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

## 9. Common registration errors

| error (abbreviated) | fix |
|---|---|
| `a job node's next must be a pass or scatter or switch or gate or halt node, but 'B' is 'job'` | Insert a `pass` between the two jobs. |
| `'next' has 2 targets; v1 allows at most 1` | v1 has no parallel branches. To choose one of several paths, use a `switch`; to run many of the same job, scatter over a list. |
| `fan-in is only allowed where the incoming edges come from different arms of one switch; has predecessors [...]` | Two nodes point at one node, and they aren't on different arms of one switch ([§5.6](#56-rejoining-arms-and-what-a-node-can-reference)). |
| `$.X.y refers to 'X', which is not on every path to 'Z' (it may be skipped)` | You can only reference nodes on every path to `Z`. Check the `next` chain. After a rejoin, use one of the patterns in [§5.6](#56-rejoining-arms-and-what-a-node-can-reference). A scatter child is never on the path to anything. |
| `refers to output 'y' not declared by node 'X'` | Add `y` to X's `out`. A gather declares only its `collect` outputs plus `n_completed` and `n_failed`. |
| `node X: timeout_hours is not supported per node in v1; use defaults.timeout_hours …` | Move it to `defaults.timeout_hours` (the run timeout), or set the job-spec's `time_limit`. |
| `refers to scatter child 'P', whose outputs are not stored` | In `spec.outputs` or downstream, reference the gather instead (`$.G.y`). |
| `'$.scatter.item' is only allowed in a scatter child's 'in'` | Move the ref into the child's `in`. |
| `bind key 'C.x' targets 'C' but next is 'B'` | A pass binds only its own successor's params. |
| `'bind' must be an object (or omitted/{} for a pure sequencing pass)` | Use an object, `{}`, or leave `bind` out. |
| `unknown field 'xyz'` | Check for a typo. Unknown fields are rejected, except `label` and `description`. |
| `kind 'xyz' is not supported` | Use `job`, `pass`, `scatter`, `gather`, `switch`, `gate` or `halt`. |
| `a switch needs exactly one of 'cases' or 'on' + 'choices'` | Use one form, not both. |
| `'default' is required` | Every switch needs a `default` arm. |
| `'default' must be a list with exactly one node id, got [...]` | Each arm has exactly one target. The same applies to `cases[i].next`, each `choices` value, `on_true` and `on_false`. |
| `arm 'X' target 'Y' does not exist` | Fix the node id in the arm. |
| `arm 'X' target must be a job or pass or scatter or switch or gate or halt node, but 'G' is 'gather'` | A gather can only follow its scatter. |
| `first_of entry $.reprocess_L2SMP.l2_file can never be used; $.L2SMP.l2_file before it always resolves (it is on every path to 'deliver')` | Move the shared node (or a `$.inputs` entry: `… (is a workflow input)`) to the end of the list. |
| `first_of does not cover every path to 'deliver'; add a ref on the path through ['qa_route']` | Some path reaches the node without passing a listed node. List a node on that path, or end with a `$.inputs` fallback. |
| `first_of entry $.X.y refers to 'X', which is not an ancestor of 'N'` | List only nodes upstream of the owner. |
| `first_of must be a list of at least 2 refs, got [...]` | With one ref, use the plain ref. |
| `first_of entry 'x.h5' must be a '$.' ref (literals and nesting are not allowed)` | Use refs only. For a fallback, list a run input last. |
| `first_of must be an object with the single key 'first_of', has [...]` | Remove the extra keys. |
| `first_of is only allowed in a job's 'in', a pass 'bind' value and spec outputs (found at '…')` | Move it into a job's `in`, a pass `bind` or spec `outputs`. |
| `the arm label 'default' is reserved` | Rename the case or choice. |
| `cases[0].when.op must be one of ['eq', 'ne', 'lt', 'le', 'gt', 'ge', 'in'], got '<'` | Use the op names, not symbols. |
| `cases[0].when: 'in' needs a list literal or a $-ref on the right, got 0.3` | Give `in` a list. |
| `cases[0].when.not.not.… nests deeper than 8 levels` | Flatten the `when` tree. |
| `'status' must be one of ['failed', 'completed'], got 'ok'` | A halt ends the run as `failed` or `completed`. |
| `'reason' is required: a string of 1..512 characters` | Give the halt a reason. |
| `unknown field 'next' for kind 'halt'` (or `'tag'` for kind `switch`) | A halt has no successors, and a switch or gate has no `tag`. |
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

## 10. Not in v1

Parallel branches, joins other than an exclusive rejoin, loops, pause and resume,
per-run overrides, spec permissions, and a CWL adapter.

## See also

- Runtime contract (authoritative behavior): `hysds-workflow-docs` repo,
  `PLAN-workflow-orchestration-v1-contract.md`
- Output binding design: ADR-003, *Workflow Output Binding via Job-Scoped `_workflow_outputs.json`*
- Fixture specs: `hysds_commons.workflow_examples` and the `local-stack/wf-e2e/spec-*.json`
  files (linear, scatter/gather, retry, File/Directory outputs, sequence-only)
