# Writing HySDS Workflows in CWL

A **workflow** chains registered HySDS job types into a pipeline. You write it in
**CWL v1.2**, register it with mozart once, and then start runs of it with inputs. HySDS runs
each step as an ordinary HySDS job as soon as the steps it reads from have finished. It runs
independent steps in parallel, fans steps out over lists, retries flaky steps, and keeps the
run's state in one run document.

This guide is for mission developers who write workflows and make their PGEs workflow-ready.
It covers **workflows v2** (branch `feature/workflow-orchestration-v2`): workflows written in
CWL and compiled at registration to a JSON spec with `schema_version` `"2.0"`.

> **v2 in one paragraph.** A workflow is a CWL `Workflow` whose steps are abstract
> `Operation`s. A `hysds:JobType` hint binds each one to a registered job type. Jobs never
> run CWL. The engine follows CWL's dataflow model: a step is ready when every step it reads
> from has **settled** (completed or been skipped), and then its `when` decides whether it
> runs. Branches run in parallel and join wherever a step reads from several of them.
> Routing is a boolean `when`, often fed by the built-in decision job. A `hysds:Halt` step
> ends the run on purpose, and `hysds:OnError` decides what a failure does to the rest of the
> run.

**The design rule.** If HySDS accepts a workflow, the CWL dataflow alone has the same
meaning. The `hysds:` hints may make a run stricter (halting, error handling) or tune it
(queues, retries, scatter tolerance), but they never change what the dataflow computes. A
workflow that registers also passes `cwltool --validate`. cwltool can't *run* it, because its
steps are abstract Operations ("unrunnable abstract Operation").

The v1 JSON spec (`next`, `pass`, `switch`, `gate`, `first_of`) and its endpoints are gone.
There is no converter, so rewrite v1 specs in CWL.

---

## 1. Quick start

**1. Get an Operation for each job type** from mozart. It lists every hysds-io param, marks
the optional and defaulted ones, and lists the job's declared outputs:

```bash
curl -s https://<mozart-host>/mozart/api/v0.2/workflow/cwl/operation/job-radiometer-l1c:v3.2
```

```yaml
class: Operation
hints:
  hysds:JobType:
    job_type: job-radiometer-l1c:v3.2
inputs:
  l1b_file:
    type: string
outputs:
  l1c_file:
    type: File
```

Paste it under a step's `run:`. Remove the inputs you don't bind; optional ones can stay.

**2. Write the workflow.** Here is a two-step chain:

```yaml
cwlVersion: v1.2
class: Workflow
id: radiometer-l1b-l1c
label: L1B to L1C for one granule
$namespaces:
  hysds: https://github.com/hysds/hysds_commons/cwl/v1#
  s: https://schema.org/
s:version: 1.0.0

inputs:
  l1a_file: File
outputs:
  l1c_file: {type: File, outputSource: l1c/l1c_file}

steps:
  l1b:
    run:
      class: Operation
      inputs:  {l1a_file: File}
      outputs: {l1b_file: File}
      hints: {hysds:JobType: {job_type: "job-radiometer-l1b:v3.2"}}
    in:  {l1a_file: l1a_file}
    out: [l1b_file]
  l1c:
    run:
      class: Operation
      inputs:  {l1b_file: File}
      outputs: {l1c_file: File}
      hints: {hysds:JobType: {job_type: "job-radiometer-l1c:v3.2"}}
    in:  {l1b_file: l1b/l1b_file}
    out: [l1c_file]
```

**3. Validate, then register.** Validation is a dry run that stores nothing. Fix every
error it reports, then register the same file:

```bash
API=https://<mozart-host>/mozart/api/v0.2/workflow
curl -s -X POST -H 'content-type: text/yaml' --data-binary @l1b-l1c.cwl $API/cwl/validate | jq '.ok, .errors'
curl -s -X POST -H 'content-type: text/yaml' --data-binary @l1b-l1c.cwl "$API/cwl?user=jdoe"
# {"success": true, "spec_id": "radiometer-l1b-l1c:1.0.0", "sha256": "…", "warnings": []}
```

**4. Start a run and watch it:**

```bash
curl -s -X POST -H 'content-type: application/json' $API/runs \
  -d '{"spec_id": "radiometer-l1b-l1c:1.0.0", "user": "jdoe",
       "inputs": {"l1a_file": "s3://<bucket>/products/L1A_Radiometer/…/SMAP_L1A_RADIOMETER_45678_D_….h5"}}'
# {"success": true, "run_id": "run-3f2a…"}            (202 Accepted)
curl -s $API/runs/run-3f2a… | jq '.result.status, .result.steps[].state'
```

[§7](#7-worked-example-radiometer-orbit-to-l2smp) has the complete worked example: an orbit
chain with a scatter, parallel branches, a QA decision and a halt.

---

## 2. Making a job workflow-ready

Any registered HySDS job type can be a workflow step. The engine talks to a job in two ways:
**params in** and **one outputs file out**. This contract hasn't changed since v1.

### 2.1 Params in

A step's resolved inputs become the job's params, exactly as if you had called
`POST /job/submit` yourself. Each Operation input must be a param in the job type's hysds-io.
The job-spec gives each param its `destination` (`context`, `positional` or `localize`).
Registration rejects an Operation input that isn't a param (`unknown_param`).

- A `localize` param that holds an `s3://` URL is downloaded into the job directory before
  the PGE starts.
- `File` and `Directory` values reach the job as **URL strings**.
- An input that resolves to `null` is left out of the job, so the param's hysds-io default
  or its `optional` flag applies.

### 2.2 Outputs out: `_workflow_outputs.json`

To expose values to later steps, the PGE writes **one file named `_workflow_outputs.json`
at the root of the job directory**:

```json
{
  "version": "1.0",
  "outputs": [
    { "name": "qa_score",  "value_d": 0.87 },
    { "name": "orbit_dir", "value_s": "Descending" },
    { "name": "deliver",   "value_b": true },
    { "name": "bad_scans", "value_d": [1182, 1183, 2410] }
  ]
}
```

- Each entry has a `name` and **exactly one** typed slot: `value_s` (string), `value_d`
  (number) or `value_b` (boolean). Any slot can hold a list of its type, and the order is
  kept.
- Write every name in the step's `out` list. A missing output only becomes an error when
  something reads it. The reading step then fails with `input_resolution`:
  `cannot resolve $.l2smp.product: step 'l2smp' has no output 'product' (has: [...])`.
- A boolean that feeds a `when` must be written with `value_b`.
- These problems **fail the job** with error class **`outputs`**: a malformed file, a wrong
  slot type, a repeated name, a file over the size limit (512 KB by default, set by
  `WORKFLOW_OUTPUTS_MAX_BYTES`), or a `class` path that can't be resolved to a published URL
  ([§2.3](#23-referencing-published-files-class-file--directory)). Rerunning the job would
  write the same outputs, so `outputs` is **not retried** by `worker` or `pge:*` entries. It
  also takes precedence over a token in `_error_class.txt`. Only `retry_on: ["*"]` matches
  it. Outputs travel on a queue
  message, so keep them to scalars and references. Publish payloads as datasets and output
  their location.

### 2.3 Referencing published files: `class: File | Directory`

A PGE usually doesn't know where its products will be published. Mark an output as a file or
a directory and give its **local path, relative to the job directory**:

```json
{ "name": "l1c_file", "class": "File",
  "value_s": "output/SMAP_L1C_TB_45678_D_20261112T013517_R19240_001/SMAP_L1C_TB_45678_D_20261112T013517_R19240_001.h5" }
```

After the job's datasets are published, HySDS rewrites each path to its published URL, such
as `s3://<bucket>/products/L1C_TB/…/SMAP_L1C_TB_45678_D_…_001.h5`. Later steps only ever
see the URL.

- `class` requires the `value_s` slot.
- Every path must lie **inside a dataset the job publishes**, meaning a directory that has a
  `<id>.dataset.json`. Otherwise the job fails. A job with `disable_post_builtins` publishes
  nothing, so it can't use `class`.

### 2.4 Knowing you're running in a workflow

A workflow job has a top-level `workflow` block in **`_job.json`**:

```json
"workflow": { "run_id": "run-3f2a…", "spec_id": "radiometer-orbit-l2smp", "step_id": "l1a",
              "child_index": 9, "child_total": 30 }
```

`child_index` and `child_total` are set only for scatter children, and are `null`
otherwise. `_context.json` doesn't carry the block, and no environment variables are set.

### 2.5 Telling the retry policy why you failed (optional)

Every failed attempt gets an **error class**, which is what `hysds:Retry.retry_on` matches
against ([§4.4](#44-hysdsretry)):

| class | when |
|---|---|
| `exit_code:<n>` | the PGE exited non-zero |
| `timeout` | the job hit its soft time limit |
| `signal` | the worker caught a signal |
| `offline` | the worker went away (job-offline) |
| `worker` | any other worker-side error |
| `pge:<token>` | the PGE wrote `<token>` to `_error_class.txt` in the job directory |
| `outputs` | `_workflow_outputs.json` was invalid or too large, or a `class` path couldn't be resolved ([§2.2](#22-outputs-out-_workflow_outputsjson)) |

To classify your own failures, write one token (`[A-Za-z0-9_:.-]`, at most 64 characters) to
`_error_class.txt` before exiting non-zero. For example, `throttled` becomes `pge:throttled`.
A `pge:` class replaces `exit_code:<n>` or `worker`, but never `timeout` or `outputs`.

---

## 3. Authoring in CWL

HySDS accepts a defined subset of CWL v1.2, called the **HySDS profile**. Registration
rejects anything outside it with a specific error code ([§9](#9-registration-errors)).

### 3.1 The document

```yaml
cwlVersion: v1.2                 # required, exactly v1.2
class: Workflow
id: radiometer-orbit-l2smp       # required, [A-Za-z0-9_.-]+; the spec id
label: Radiometer orbit to L2SMP # optional
doc: L0B granules for one orbit …   # optional; becomes the spec description
$namespaces:
  hysds: https://github.com/hysds/hysds_commons/cwl/v1#   # required (any prefix)
  s: https://schema.org/
s:version: 1.0.0                 # required, any non-empty string
requirements:
  ScatterFeatureRequirement: {}         # needed if any step scatters
  MultipleInputFeatureRequirement: {}   # needed if any source is a list
hints:
  hysds:OnError: {mode: kill}
```

- **Identity.** A registered workflow is `<id>:<s:version>`, for example
  `radiometer-orbit-l2smp:1.0.0`, and it is **immutable**. Registering the same id and
  version with different bytes returns 409. Bump `s:version` for every change.
- **Namespace.** Hints are matched by their expanded URI, not by the prefix. Any prefix
  bound to `https://github.com/hysds/hysds_commons/cwl/v1#` works.
- **Self-contained.** A registered workflow never points outside itself. Each step's `run`
  is either an inline Operation or, in a packed document (`cwltool --pack`, a `$graph` with
  exactly one `Workflow`), a `run: "#id"` reference to an Operation in the same graph, which
  is inlined at registration. A `run:` naming an external file or URL is rejected
  (`unresolved_run`). So the stored CWL, which is also what the UI's **Download CWL** gives
  you, is a portable export that cwltool can validate anywhere.
- **The recommended way to write a step** is to fetch its Operation from
  `GET /workflow/cwl/operation/<job_type>` ([§1](#1-quick-start)) and inline it. Workflow
  builders do the same.
- **`$schemas`** is stripped with a warning. Registration never fetches anything.
- Other `s:` metadata, such as authors and license, is accepted and ignored.

**Standard requirement classes** are handled the same way whether they appear under
`requirements` or `hints`:

| class | handling |
|---|---|
| `ScatterFeatureRequirement`, `MultipleInputFeatureRequirement` | accepted. When the feature is used, the class must be under `requirements`, as cwltool demands. |
| `SchemaDefRequirement` | accepted, for record types only. The decision job's `Case` type uses it ([§3.7](#37-the-decision-job)). |
| `ToolTimeLimit` | accepted. A literal `timelimit` in seconds becomes the job's `time_limit` ([§4.6](#46-tooltimelimit)). |
| `ResourceRequirement` | ignored. CWL reads values in MiB, so a value of 64 or less looks like GB and draws a `resource_units` warning. |
| `DockerRequirement`, `NetworkAccess`, `EnvVarRequirement`, `SoftwareRequirement`, `InitialWorkDirRequirement`, `LoadListingRequirement`, `InplaceUpdateRequirement`, `WorkReuse` | ignored, with an `ignored_requirement` warning. The job type defines its own runtime. |
| `SubworkflowFeatureRequirement`, `StepInputExpressionRequirement`, `InlineJavascriptRequirement` | **rejected** |
| anything else | rejected (`unsupported_requirement`) |

### 3.2 Inputs and outputs

**Types.** The supported types are:

- `string`, `int`, `long`, `float`, `double`, `boolean`, `File`, `Directory` and `Any`;
- arrays of those (`T[]`);
- optional forms (`T?` or `[null, T]`);
- records declared in `SchemaDefRequirement`.

Nested arrays, unions other than `[null, T]`, and inline record or enum types are rejected.

**Workflow inputs** may have a `default`. Write a `File` or `Directory` default as a CWL
object. Registration compiles it to its `location` URL:

```yaml
inputs:
  orbit_id: string
  ancillary_version: {type: string, default: v7}
  mask: {type: File, default: {class: File, location: "s3://<bucket>/static/landmask_v3.h5"}}
```

HySDS passes files as URL strings, so some CWL file features are rejected
(`file_field_unsupported`): the File fields `secondaryFiles`, `checksum`, `format` and
`contents`, and the input options `loadContents` and `loadListing`.

**Workflow outputs** use `outputSource`. It is either one source, or a list with a
`pickValue` (plus `linkMerge: merge_flattened` if needed). An output whose value can be null
must be optional (`?`) or use a `pickValue` ([§3.6](#36-null-safety)).

```yaml
outputs:
  product:
    type: File?
    outputSource: [reprocess_l2smp/product, l2smp/product]
    pickValue: first_non_null
  browse: {type: File, outputSource: make_browse/browse_png}
```

### 3.3 Steps: an Operation bound to a job type

Every step's `run` is an inline `class: Operation` with **exactly one** binding hint:
`hysds:JobType` for a job step, or `hysds:Halt` for a halt step ([§3.8](#38-halting-on-purpose)).

```yaml
  l2smp:
    run:
      class: Operation
      inputs:  {l1c_file: File, ancillary_version: string}   # job params only
      outputs: {product: File}                               # names the job writes
      hints: {hysds:JobType: {job_type: "job-radiometer-l2smp:v2.0"}}
    in:  {l1c_file: l1c/l1c_file, ancillary_version: ancillary_version}
    out: [product]
```

- **Step ids** match `[A-Za-z0-9_-]+`. The ids `inputs`, `scatter` and `finalize` are
  reserved.
- **Operation `inputs`** must be params of the job type. Each param that the job type
  requires and that has no default must be declared and bound (`missing_param`).
- **Operation `outputs`** name what the job writes to `_workflow_outputs.json`. If the job
  type declares its outputs, each name is checked against them (`unknown_output`).
- **`in`** entries bind Operation inputs. CWL v1.2 also allows an `in` entry that is *not* an
  Operation input, such as a `when` flag. That entry is **not passed to the job**.
- **`out`** lists the outputs that later steps may read. A halt step has `out: []`.
- **Not allowed in `run`:** `CommandLineTool` and `ExpressionTool` (`tool_unsupported`), and
  `Workflow` (`subworkflow_unsupported`). Steps run registered job types, not CWL tools.

### 3.4 Sources, `pickValue` and `linkMerge`

An `in` entry is either a shorthand source (`l1c/l1c_file`, or the name of a workflow input)
or an object:

| field | meaning |
|---|---|
| `source` | a workflow input name or `step/output`, or a list of them |
| `pickValue` | `first_non_null`, `the_only_non_null` or `all_non_null` |
| `linkMerge` | `merge_flattened` only (`merge_nested` is rejected) |
| `default` | used when the resolved value is null |

**Not allowed:** `valueFrom` and any other expression. Compute such values in a job instead.

Every step that a source names becomes a **dependency**: the step waits until those steps
have settled. Workflow inputs are never dependencies, so a step that reads only workflow
inputs starts as soon as the run starts. Steps that don't depend on each other run in
parallel.

**Resolution order.** As in CWL, each input is resolved in this order:

1. `linkMerge`;
2. `pickValue`;
3. the step's `in` `default`, if the value is still null;
4. the Operation input's `default`;
5. the job's hysds-io default, applied at submit.

| `pickValue` | result | error (`input_resolution`) |
|---|---|---|
| `first_non_null` | the first non-null value in list order | every value is null |
| `the_only_non_null` | the one non-null value | no non-null value, or more than one |
| `all_non_null` | a list of the non-null values (possibly `[]`, never null) | — |

A step `default` does **not** rescue `first_non_null` or `the_only_non_null` when every
source is null. That is an error, as in CWL.

**Dropping failed scatter items.** After a scatter that tolerates failures
([§6.2](#62-tolerating-failed-items-hysdsgather)), use this canonical form:

```yaml
      l1a_files:
        source: [l1a/l1a_file]
        linkMerge: merge_flattened
        pickValue: all_non_null
```

The single-source form `{source: l1a/l1a_file, pickValue: all_non_null}` is also accepted. It
is normalized to the canonical form, with a `normalized_pick` warning.

### 3.5 Conditional steps: `when`

A step runs only if its `when` is true. HySDS supports exactly one form, a boolean input of
the step:

```yaml
  reprocess_l2smp:
    when: $(inputs.go)
    in:
      go: qa_check/reprocess        # the flag: not an Operation input, so not passed to the job
      l1c_file: l1c/l1c_file
      ancillary_version: reprocess_ancillary_version
```

The flag's source must be one of:

- a **non-optional `boolean` output** of a step that has no `when` of its own and doesn't
  scatter, or
- a workflow input of type `boolean` that is non-optional or has a default.

The flag takes a single source, with no list and no `pickValue`. Any other `when` is
rejected, such as `$(inputs.score > 0.5)` or JavaScript. To route on a comparison, use the
decision job ([§3.7](#37-the-decision-job)).

**Skips don't cascade**, as in CWL. All of a skipped step's outputs are `null`. A step that
reads from it still runs, with null for that source, unless its own `when` is false. Put a
`when` on every step that should not run, and handle the nulls ([§3.6](#36-null-safety)).

A step can't have both `scatter` and `when` (`scatter_with_when`), because CWL evaluates
`when` once per item. Put the condition on an upstream step.

### 3.6 Null safety

Registration proves that no null can reach an input that doesn't accept one. A source **can
be null** if its step has a `when`, if its type is optional, or if it comes from a scatter
that tolerates failures. Such a source may feed a non-optional input only if one of these
holds:

- the input uses `pickValue: first_non_null` or `the_only_non_null`, so an all-null result
  becomes a defined run-time error;
- the `in` entry has a `default`;
- the Operation input's type is optional (`T?`);
- the consuming step's `when` flag has **the same source** as the producing step's flag, so
  both steps run or neither does.

A step's own `when` is not enough by itself. Otherwise registration fails with `null_unsafe`:

```
steps.deliver.in.product.source  null_unsafe
'reprocess_l2smp/product' can be null (its step is conditional or its type is optional) but input 'product'
is not optional; use pickValue: first_non_null or the_only_non_null, give the input a default, or make its
type optional
```

Workflow outputs follow the same rule (`null_unsafe_output`): the output's type must be
optional, or the output must use a `pickValue`. cwltool also warns that a source "may
produce null". Once this check has proved an edge safe, that warning is dropped.

### 3.7 The decision job

When a decision is a comparison, such as "QA score below 0.3", use the built-in job type
**`job-workflow-decide:v1`**. It is an ordinary job on queue `workflow-connector-decide`, and
it writes **one boolean output per case, plus `otherwise`**. Exactly one of them is true:
**the first case that matches wins**, and `otherwise` is true only if no case matches. Feed
those booleans to `when` flags.

```yaml
requirements:
  MultipleInputFeatureRequirement: {}
  SchemaDefRequirement:
    types:
      - name: Case
        type: record
        fields:
          - {name: name, type: string}
          - {name: when, type: Any}
…
steps:
  qa_route:
    run:
      class: Operation
      inputs:
        value: float
        cases: {type: {type: array, items: Case}}
      outputs: {reject: boolean, reprocess: boolean, otherwise: boolean}
      hints: {hysds:JobType: {job_type: "job-workflow-decide:v1"}}
    in:
      value: l2smp_qa/qa_score
      cases:
        default:
          - {name: reject,    when: {op: lt, right: 0.3}}
          - {name: reprocess, when: {op: lt, right: 0.6}}
    out: [reject, reprocess, otherwise]

  reprocess_l2smp:
    when: $(inputs.go)
    in: {go: qa_route/reprocess, …}
```

**Params.**

- `value` is the value the cases test. It is optional.
- `cases` is a list of `{name, when}`. Give it as a literal `default` so that registration
  can check it.
- `otherwise` renames the no-match output. It defaults to `otherwise`.

**Conditions (`when`).** A condition is one of:

- a boolean literal;
- a comparison, `{op, right}` or `{left, op, right}`. An omitted `left` means `value`.
  `left` and `right` are JSON literals, and `op` is one of `eq ne lt le gt ge in`;
- `{all: […]}` or `{any: […]}`, which must be non-empty and short-circuit, or `{not: …}`.

Conditions can nest up to 8 levels.

| `op` | true when | operands |
|---|---|---|
| `eq`, `ne` | equal / not equal | any JSON values; `true` never equals `1` |
| `lt`, `le`, `gt`, `ge` | `<`, `≤`, `>`, `≥` | two numbers or two strings (a bool isn't a number) |
| `in` | `left` is an element of `right` | `right` is a list literal |

**Names.**

- Case names match `^[A-Za-z_][A-Za-z0-9_-]*$` and must be unique.
- `value` and `cases` are reserved, and a case can't use the `otherwise` name.
- Every name in `out` must be a case name or the otherwise name, and every Operation output
  must be a non-optional `boolean` (`decide_out`).

**Run-time failures.** These fail the decide job, and therefore the step:

- a comparison on a null or missing `value`;
- an ordering op on operands of mismatched types.

If `value` comes from a step that can be skipped, give the `value` input a `default`.

The decide step's outputs are never null and it has no `when`, so its booleans are valid
`when` flags.

### 3.8 Halting on purpose

A **halt step** ends the run with a status and a reason. It is never submitted as a job.

```yaml
  reject:
    when: $(inputs.go)
    run:
      class: Operation
      inputs: {}
      outputs: {}
      hints: {hysds:Halt: {status: failed, reason: "L2SMP failed QA"}}
    in:  {go: qa_check/reject}
    out: []
```

- `status` is `failed` or `completed`. `reason` is optional, at most 512 characters.
- The Operation declares no inputs and no outputs, `out` is empty, and no step can read from
  a halt step. The step's `in` holds only its `when` flag.
- A halt step without a `when` fires as soon as its dependencies settle.
- **If any halt has `status: completed`, every workflow output must be optional**
  (`output_not_optional`). A completed halt can end the run before the steps that feed an
  output have run.

What a halt does to the rest of the run depends on `hysds:OnError` ([§8.3](#83-halts)).

---

## 4. `hysds:` hints

| hint | where | fields | effect |
|---|---|---|---|
| `hysds:JobType` | Operation | `job_type` | binds the step to a job type (required for job steps) |
| `hysds:Halt` | Operation | `status`, `reason` | makes the step a halt ([§3.8](#38-halting-on-purpose)) |
| `hysds:OnError` | workflow | `mode`: `kill`, `drain` or `continue` | what a failure or halt does to the run. Default `kill` ([§8.2](#82-hysdsonerror)) |
| `hysds:Queue` | workflow, step, Operation | `queue` | the job queue. Default: the job-spec's first recommended queue |
| `hysds:Retry` | workflow, step, Operation | `max_retries`, `retry_on` | retries of failed attempts ([§4.4](#44-hysdsretry)) |
| `hysds:Gather` | scattered step | `tolerated_failure_percentage` | the share of items allowed to fail: at least 0 and less than 100. Default 0 ([§6.2](#62-tolerating-failed-items-hysdsgather)) |
| `hysds:Tag` | step | `tag` | the job tag. Default: the step id |
| `ToolTimeLimit` (standard) | workflow, step, Operation | `timelimit` | the job's `time_limit` ([§4.6](#46-tooltimelimit)) |

`hysds:` classes are **hints only**. Listing one under `requirements` fails with
`hysds_requirement`, and an unknown `hysds:` class fails with `unknown_hysds_hint`. Hint
values must be literals; expressions are rejected. The former `hysds:ScatterConnector` hint
is rejected with `scatter_connector_removed`: list items in an ordinary step instead
([§6.3](#63-listing-items-a-lister-step)).

### 4.1 Precedence

CWL rules apply. The **most specific level wins**: the Operation first, then the step, then
the workflow. A hint object is **replaced whole**, never merged field by field. A field that
no hint sets comes from the job-spec.

```yaml
hints:                               # workflow
  hysds:Retry: {max_retries: 1, retry_on: [offline, worker]}
steps:
  l1a:
    hints:                           # step: replaces the whole workflow Retry for l1a
      hysds:Retry: {max_retries: 2, retry_on: ["exit_code:137", offline]}
```

`l1a` retries up to twice on `exit_code:137` or `offline`, but *not* on `worker`, because
the workflow's `retry_on` isn't merged in.

### 4.2 Why these are hints

An engine that ignored `Gather`, `Retry`, `Queue` or `OnError` would be stricter but still
correct, so these can stay hints, as CWL intends. `JobType` and `Halt` don't pass that test.
In HySDS they are **binding hints and mandatory**. cwltool accepts them as unknown hints,
which is why cwltool validates a HySDS workflow but refuses to run it.

### 4.3 `hysds:Queue`

`{queue: radiometer-large}`. A step with no queue hint uses the first entry in its
job-spec's `recommended-queues`. If neither exists, starting a run fails with a 400 error,
`no queue for step(s): …`.

### 4.4 `hysds:Retry`

`{max_retries: 2, retry_on: ["exit_code:137", offline]}`. The hint needs at least one of the
two fields. A failed attempt is retried when both are true:

- fewer than `max_retries` retries have been made;
- the attempt's error class ([§2.5](#25-telling-the-retry-policy-why-you-failed-optional))
  matches a `retry_on` entry.

`retry_on` entries can be:

- the exact classes `timeout`, `signal`, `offline` and `worker`;
- `exit_code:<n>` or `exit_code:*`;
- `pge:<token>` or `pge:*`;
- `*`, which matches everything, including `outputs`.

`outputs` itself isn't an accepted `retry_on` entry (`bad_hint`). An outputs failure is
deterministic, so it is retried only under `*`.

A retry is a new job attempt with the same inputs. Every attempt is kept in the step's
`attempts[]` in the run document. Scatter children are retried individually. A run that is
`halting` never retries.

### 4.5 `hysds:Tag`

`{tag: l2smp-reprocess}` sets the job's tag, which appears in Figaro and job search. Scatter
children share their step's tag. Every workflow job can also be found by `workflow.run_id`
and `workflow.step_id` in `job_status`.

### 4.6 `ToolTimeLimit`

`{timelimit: 3600}` becomes the job's `time_limit` in seconds, rounded up. It must be a
literal number; an expression fails with `expression_unsupported`. `timelimit: 0` (CWL's
"no limit") isn't supported. It compiles with a `timelimit_zero` warning, and the job type's
own `time_limit` applies.

---

## 5. Validate, register and run

The endpoints live under both `/mozart/api/v0.1/workflow` and `/mozart/api/v0.2/workflow`,
which are identical. They need mozart installed with the `hysds_commons[cwl]` extra, which
provides cwltool. Without the extra, the CWL endpoints return **501** with an install hint.

| method and path | what it does | responses |
|---|---|---|
| `POST /workflow/cwl/validate` | compiles only; stores nothing | 200 `{ok, errors[], warnings[], compiled?, graph?, sha256}` |
| `POST /workflow/cwl` | compiles and registers | 201 registered; 200 already registered (same bytes); 400 invalid; 409 same `id:version` with different bytes |
| `GET /workflow/cwl` | lists registered workflows | |
| `GET /workflow/cwl/<id>:<version>` | returns the stored CWL, normalized CWL, compiled spec and sha256 | 404 if unknown |
| `GET /workflow/cwl/operation/<job_type>` | returns the Operation for a job type, as YAML (`?format=json` for JSON) | 404 if the job type has no job-spec or hysds-io |
| `POST /workflow/runs` | starts a run | 202 `{run_id}`; 400 bad inputs; 404 unknown spec |
| `GET /workflow/runs` | lists runs. Filters: `status`, `spec_id`, `spec_version`, `username`, `tag`. Paging: `size` (max 1000), `from` | |
| `GET /workflow/runs/<run_id>` | returns the run document | `source` is `redis` while the run is active, `opensearch` afterwards |
| `GET /workflow/runs/<run_id>/steps` | returns per-step job status counts from `job_status` (may lag) | |
| `POST /workflow/runs/<run_id>/cancel` | cancels the run | 202 requested; 200 already cancelled; 409 already completed or failed; 404 unknown |

### 5.1 Sending CWL

The body of `validate` and `register` is the CWL text. Send it as YAML or JSON with
`content-type: text/yaml`, or as a JSON object `{"cwl": "<text>", "user": "jdoe"}`. You can
also give the registering user as `?user=`.

**Validate first.** A single response reports every error as a structured
`{path, code, message}`. The `path` is a CWL path such as `steps.deliver.in.product.source`:

```json
{ "ok": false,
  "errors": [
    { "path": "steps.l1b.run.hints.hysds:JobType.job_type", "code": "unknown_job_type",
      "message": "job type 'job-radiometer-l1b:v3.3' is not registered" },
    { "path": "steps.deliver.when", "code": "when_flag_invalid",
      "message": "the when flag 'go' comes from step 'reprocess_l2smp', which can itself be skipped" } ],
  "warnings": [] }
```

On success, `compiled` is the 2.0 spec and `graph` is `{nodes, edges}`, for drawing the
workflow. Each node has `id`, `kind` (`job` or `halt`), `job_type`, `when` and `scatter`.

**What registration checks**, in order:

1. It parses the document, strips `$schemas` and inlines `#id` Operations.
2. It applies the profile rules (§3, §4).
3. It runs cwltool's loader and static checker, with no network access and no execution.
   cwltool errors appear as `cwl_validation`. Its warnings appear as `cwl_warning`, except
   messages about the `hysds:` hints.
4. It checks the registry:
   - each job type exists;
   - Operation inputs are hysds-io params, and required params are bound;
   - Operation outputs are among the job type's declared outputs.
5. It compiles to schema 2.0 and validates the result.

Job-spec defaults are **not** copied into the CWL or the spec. They apply at submit time.

### 5.2 Starting a run

```json
POST /workflow/runs
{ "spec_id": "radiometer-orbit-l2smp:1.0.0",
  "inputs":  { "orbit_id": "45678" },
  "tags":    ["reproc-2026q4"],
  "user":    "jdoe" }
```

- **Inputs are checked** against the workflow's declared types. Defaults fill missing
  inputs, and an optional input with no default is `null`. An unknown input, a missing
  required input or a type mismatch gets a 400 response that lists every problem.
- **`File` and `Directory` inputs** may be URL strings or CWL job-order objects
  (`{"class": "File", "location": "s3://…"}`), also inside arrays, so the input templates
  that cwltool generates work unchanged. An object is flattened to its `location` (or
  `path`), and the job receives the URL string. An object with neither field answers 400:
  `input 'l0b_file': File/Directory object needs a 'location' (or 'path')`.
- **Job definitions are frozen at run start.** mozart reads each job type's job-spec,
  hysds-io, container and recommended queue once and stores them with the run. Every submit in the run uses that frozen copy, so
  re-registering a job type doesn't affect runs that have already started. If a job type's
  job-spec, hysds-io or container is missing, the response is 400.
- **The response is 202 with the `run_id`.** The engine runs the workflow from there; mozart
  submits no jobs itself.

### 5.3 The run document

`GET /workflow/runs/<run_id>` returns the run document. While the run is active, the document
comes from Redis, which holds the live state. After the run ends, it comes from the
`workflow_status` index. That index is fed asynchronously and may lag by a few seconds.

```json
{ "schema_version": "2.0", "run_id": "run-3f2a…", "spec_id": "radiometer-orbit-l2smp",
  "spec_version": "1.0.0", "spec_sha256": "…", "status": "failed", "on_error": "kill",
  "inputs": {"orbit_id": "45678", "ancillary_version": "v7", "reprocess_ancillary_version": "v7-fallback"},
  "outputs": null, "error_class": "halted", "reason": "L2SMP failed QA", "failed_step": "reject",
  "cancelled_by": null, "created_at": "…", "started_at": "…", "ended_at": "…",
  "username": "jdoe", "tags": ["reproc-2026q4"],
  "steps": {
    "l1a": { "kind": "job", "state": "completed", "job_type": "job-radiometer-l1a:v3.2",
             "scatter": {"input": "l0b_file", "threshold": 0.95, "expected": 30,
                         "n_completed": 29, "n_failed": 1, "children": ["…", "…"]},
             "attempts": [] },
    "l2smp": { "kind": "job", "state": "completed", "job_id": "…",
               "attempts": [ {"attempt": 1, "job_id": "…", "status": "failed", "error": "…"},
                             {"attempt": 2, "job_id": "…", "status": "completed"} ] },
    "reprocess_l2smp": { "state": "skipped", "skipped_by": {"flag": "$.qa_check.reprocess", "value": false} },
    "reject":  { "kind": "halt", "state": "completed",
                 "halt": {"status": "failed", "reason": "L2SMP failed QA", "fired": true} },
    "deliver": { "state": "cancelled", "cancelled_by": {"kind": "halt", "step": "reject", "reason": "L2SMP failed QA"} } } }
```

| run `status` | meaning |
|---|---|
| `running` | steps are being released and run |
| `halting` | used only when the run must wait for jobs already running: under `drain` after a failure or halt, or under `continue` after a halt |
| `completed` | every step settled with no failure, or a `completed` halt fired. `outputs` holds the workflow outputs |
| `failed` | a step failed or a `failed` halt fired. `error_class`, `reason` and `failed_step` say which |
| `cancelled` | a user (or the run timeout) cancelled the run. `cancelled_by` names the user |

| step `state` | meaning |
|---|---|
| `waiting` | its dependencies haven't all settled |
| `queued`, `running` | the job, or the scatter's children, has been sent out |
| `completed` | done. A halt step that fired is `completed` with `halt.fired: true` |
| `skipped` | its `when` was false. `skipped_by` gives the flag and its value |
| `failed` | failed for good, after its retries. `error` and `error_class` say why |
| `cancelled` | it won't run. `cancelled_by.kind` is `failure`, `halt`, `user` or `upstream_failed` |

Step inputs, step outputs and full error text are **not** copied into the run document. Read
them from the job documents in `job_status`, by `job_id` or by `workflow.run_id` and
`workflow.step_id`:

- `job.params` holds the resolved inputs;
- `job_info.workflow_outputs` holds the outputs;
- the full traceback is there as well.

For a scatter step, `scatter.children` lists the child job ids when there are 500 or fewer.

### 5.4 Cancelling

`POST /workflow/runs/<run_id>/cancel`, with an optional body `{"user": "jdoe"}`, asks the
engine to cancel the run:

- queued jobs are revoked and running containers are killed;
- every unsettled step becomes `cancelled` (`kind: user`);
- the run ends `cancelled`.

A cancel always behaves like `kill`, whatever `hysds:OnError` says. Outputs and datasets that
were already published stay.

---

## 6. Scatter and connectors

### 6.1 Scattering a step

A scattered step runs its job **once per item** of one input list. It is **one step** in the
run, with N children. Each child is an ordinary job with its own job page and logs, and its
`_job.json` carries `child_index` and `child_total`.

```yaml
requirements:
  ScatterFeatureRequirement: {}
…
  l1a:
    run:
      class: Operation
      inputs:  {l0b_file: File, ancillary_version: string}
      outputs: {l1a_file: File}
      hints: {hysds:JobType: {job_type: "job-radiometer-l1a:v3.2"}}
    scatter: l0b_file                    # one child per element of find_l0b/l0b_files
    in:
      l0b_file: find_l0b/l0b_files
      ancillary_version: ancillary_version    # the same value for every child
    out: [l1a_file]
```

- A step scatters over exactly **one** input. `scatterMethod` is either omitted or
  `dotproduct`. Nested and cross-product scatters are rejected.
- The scatter input must be a **non-null array**. If its source can be null, registration
  fails (`scatter_input_null`). At run time, a null or non-list value fails the step with
  `input_resolution`.
- **An empty list** completes the step at once, with empty arrays, as in CWL.
- Each output of the step is an **array aligned to the input**: `l1a/l1a_file[i]` is
  child *i*'s `l1a_file`.
- A scattered step can't have a `when` ([§3.5](#35-conditional-steps-when)).

### 6.2 Tolerating failed items: `hysds:Gather`

By default (`tolerated_failure_percentage: 0`, the CWL behaviour), the step fails if even one
child fails after its retries. With a tolerance, the step completes as long as enough
children complete:

```yaml
    hints:
      hysds:Gather: {tolerated_failure_percentage: 5}
```

- The step settles once **every child has settled**. If `completed / N ≥ 1 − p/100`, the
  step completes, with **`null` at the index of every failed child**. Otherwise it fails with
  error class `scatter_threshold`, for example
  `scatter l1a: 27 of 30 children completed (90%), below the threshold of 95%; first failure: child 4 (exit_code:1): …`.
- A failed child never ends the run by itself. Only the step's settlement does.
- Items can be null, so any step or output that reads a tolerant scatter must handle them.
  Either drop the nulls with `linkMerge: merge_flattened` and `pickValue: all_non_null`
  ([§3.4](#34-sources-pickvalue-and-linkmerge)), or declare an optional item type
  (`File?[]`). Otherwise registration fails with `null_unsafe_scatter`.

`hysds:Gather` on a step that doesn't scatter fails with `misplaced_hint`.

### 6.3 Listing items: a lister step

When the items come from a manifest, a catalog query or a GRQ search rather than from a
product chain, list them in an **ordinary job step**, a *lister*, and scatter the next step
over its output. There is no special engine phase: a lister is a job like any other, with
its own Operation and `hysds:JobType`, and the scatter is plain CWL dataflow, just as
`find_l0b → l1a` is in the worked example ([§7](#7-worked-example-radiometer-orbit-to-l2smp)).

```yaml
  list_l0b:                              # the lister: its own job type and params
    run:
      class: Operation
      inputs:  {collection: string, start: string, end: string}
      outputs: {items: "File[]"}
      hints: {hysds:JobType: {job_type: "job-radiometer-list-l0b:v1.0"}}
    in: {collection: collection, start: start_time, end: end_time}
    out: [items]

  l1a:                                   # scatters over the lister's output
    scatter: l0b_file
    run:
      class: Operation
      inputs:  {l0b_file: File, ancillary_version: string}
      outputs: {l1a_file: File}
      hints: {hysds:JobType: {job_type: "job-radiometer-l1a:v3.2"}}
    in: {l0b_file: list_l0b/items, ancillary_version: ancillary_version}
    out: [l1a_file]
```

- **The query parameters belong to the lister.** Only the lister's job type declares them,
  and the scattered job type declares only what each child needs.
- **The list is an ordinary output.** The lister writes it to `_workflow_outputs.json` as a
  list of strings, numbers or booleans, all of one kind (or `class: File` paths,
  [§2.3](#23-referencing-published-files-class-file--directory)). Any output name works.
  - An empty list completes the scattered step at once, with empty arrays.
  - Outputs travel on a queue message, so a list is bounded by `WORKFLOW_OUTPUTS_MAX_BYTES`
    (512 KB by default). A list larger than that fails the lister **once**, with error class
    `outputs`, and isn't retried ([§2.2](#22-outputs-out-_workflow_outputsjson)). For very
    large sets, list batches (such as manifest URLs) and let each child
    expand its batch.
- **Retries and queues** come from the lister step's own hints, as for any job.

The `workflow_connectors.lister` helper reduces a lister to a few lines. It calls your
function with the job's params and writes the list as the output `items`:

```python
# mission/list_l0b.py   (job command: python /home/ops/mission/list_l0b.py)
from workflow_connectors import lister

def list_items(params):                   # params: {job param name: value}
    return query_l0b(params["collection"], params["start"], params["end"])   # -> list of URLs

if __name__ == "__main__":
    lister.main(list_items)
```

Build the mission image from one that has `workflow_connectors` (or install it), and register
a job-spec and hysds-io for the lister like any job type. The lister needs no OpenSearch or
REST access. The reference plugin `workflow-connectors-example` shows the whole shape: the
lister `job-example-range-list:v2.0.0`, the child job `job-example-process-item:v2.0.0`, a
sum job, and the CWL fixtures `e2e/mission-list-sum.cwl` and
`e2e/mission-list-tolerant.cwl` (with a 40% tolerance).

### 6.4 Built-in connectors

v2 has one built-in connector job, **`job-workflow-decide:v1`**
([§3.7](#37-the-decision-job)). It runs from the `hysds/workflow-connectors:v2.0.0` image on
queue `workflow-connector-decide`. Everything else is done **inside the engine**, from Redis,
with no extra job: readiness, `when`, input resolution, gather, halts and finalize. The v1
`pass`, `switch`, `gate`, `halt`, `finalize`, `scatter` and `gather` connector jobs no longer
exist, and neither does the scatter connector: listing is a lister step
([§6.3](#63-listing-items-a-lister-step)).

---

## 7. Worked example: radiometer orbit to L2SMP

The full file is `hysds-docs/examples/radiometer-orbit-l2smp-v2.cwl`. It registers, and it
passes `cwltool --validate`.

```
                                                       ┌──▶ make_browse ───────────────────────────┐
 find_l0b ──▶ l1a (scatter, 5%) ──▶ l1b ──▶ l1c ───────┤                                           ├──▶ deliver  [when deliver]
                                                       └──▶ l2smp ──▶ qa_check ──┬──▶ reprocess_l2smp [when reprocess] ──┘
                                                                                 └──▶ reject  [halt: failed, when reject]
```

```yaml
hints:                                   # workflow level: least specific
  hysds:OnError: {mode: kill}
  hysds:Queue: {queue: radiometer-default}
  hysds:Retry: {max_retries: 1, retry_on: [offline, worker]}

inputs:
  orbit_id: string
  ancillary_version: {type: string, default: v7}
  reprocess_ancillary_version: {type: string, default: v7-fallback}

outputs:
  product:  {type: File?, outputSource: [reprocess_l2smp/product, l2smp/product], pickValue: first_non_null}
  browse:   {type: File, outputSource: make_browse/browse_png}
  receipt:  {type: string?, outputSource: deliver/receipt}

steps:
  # find_l0b, l1a (scatter, §6.1-6.2), l1b (all_non_null, §3.4), l1c, make_browse, l2smp: plain job steps

  qa_check:                              # a mission job that writes three booleans and a score
    run:
      class: Operation
      inputs:  {product: File}
      outputs: {deliver: boolean, reprocess: boolean, reject: boolean, qa_score: float}
      hints: {hysds:JobType: {job_type: "job-radiometer-l2smp-qa:v1.0"}}
    in:  {product: l2smp/product}
    out: [deliver, reprocess, reject, qa_score]

  reprocess_l2smp:
    when: $(inputs.go)
    run: {class: Operation, inputs: {l1c_file: File, ancillary_version: string}, outputs: {product: File},
          hints: {hysds:JobType: {job_type: "job-radiometer-l2smp:v2.0"}}}
    in: {go: qa_check/reprocess, l1c_file: l1c/l1c_file, ancillary_version: reprocess_ancillary_version}
    out: [product]

  reject:
    when: $(inputs.go)
    run: {class: Operation, inputs: {}, outputs: {}, hints: {hysds:Halt: {status: failed, reason: "L2SMP failed QA"}}}
    in:  {go: qa_check/reject}
    out: []

  deliver:                               # joins make_browse, l2smp and (maybe) reprocess_l2smp
    when: $(inputs.go)
    run: {class: Operation, inputs: {product: File, browse: File, orbit_id: string}, outputs: {receipt: string},
          hints: {hysds:JobType: {job_type: "job-radiometer-deliver:v1.1"}}}
    in:
      go: qa_check/deliver
      product: {source: [reprocess_l2smp/product, l2smp/product], pickValue: first_non_null}
      browse: make_browse/browse_png
      orbit_id: orbit_id
    out: [receipt]
```

**How a run goes:**

1. `find_l0b` starts at once, because it reads only `orbit_id`.
2. `l1a` scatters over the L0B files, allowing up to 5% of them to fail. `l1b` drops the
   failed items with `merge_flattened` and `all_non_null`.
3. After `l1c`, **`make_browse` and `l2smp` run in parallel**. Both read `l1c/l1c_file`, and
   neither reads the other.
4. `qa_check` writes its flags. That releases its three dependents, and each one evaluates
   its own `when`.
5. `deliver` depends on `qa_check`, `reprocess_l2smp`, `l2smp` and `make_browse`, so it
   waits until `reprocess_l2smp` has **completed or been skipped**. With `first_non_null`,
   it takes the reprocessed product if there is one, and `l2smp`'s product otherwise.

| `qa_check` flags | runs | skipped | how the run ends |
|---|---|---|---|
| `deliver` | `deliver` | `reprocess_l2smp`, `reject` | `completed`; `product` = `l2smp/product` |
| `reprocess`, `deliver` | `reprocess_l2smp`, `deliver` | `reject` | `completed`; `product` = the reprocessed one |
| `reject` | `reject` | `reprocess_l2smp`, if it was released before the halt fired | `failed`, `error_class: halted`, `reason: "L2SMP failed QA"`, `failed_step: reject`; `deliver` is `cancelled` |
| none | — | all three | `completed`; `receipt` is `null` |

When `reject` fires under `kill`, the run ends at once, and `deliver`, which is still
waiting, is cancelled. In plain CWL, `deliver` would instead be skipped by its own `when`,
because its `deliver` flag is false. Either way nothing is delivered, so the dataflow has
the same meaning.

This is the compiled 2.0 form of `deliver`, from `compiled.steps`:

```json
"deliver": {"job_type": "job-radiometer-deliver:v1.1", "queue": "radiometer-default",
            "max_retries": 1, "retry_on": ["offline", "worker"],
            "deps": ["qa_check", "reprocess_l2smp", "l2smp", "make_browse"],
            "when": "$.qa_check.deliver",
            "in": {"product": {"source": ["$.reprocess_l2smp.product", "$.l2smp.product"], "pick": "first_non_null"},
                   "browse": {"source": ["$.make_browse.browse_png"]},
                   "orbit_id": {"source": ["$.inputs.orbit_id"]}},
            "out": ["receipt"]}
```

The `when`-only input `go` is folded into `when` and isn't passed to the job.

---

## 8. Engine behaviour

### 8.1 Dataflow

- **Readiness.** A step's dependencies are the distinct steps named in its sources,
  including its `when` flag. At run start, every step with no dependencies is released, and
  those steps start in parallel. When a step **settles** (completes or is skipped), each
  dependent whose dependencies have now all settled is released **exactly once**. The join
  is an atomic Redis set per step, so duplicate events are harmless.
- **On release**, the step's `when` is evaluated:
  - if it is false, the step is `skipped`, and that settle event releases its dependents in
    turn;
  - if it is true, the inputs are resolved from the recorded outputs
    ([§3.4](#34-sources-pickvalue-and-linkmerge)), and the job is submitted with its queue,
    retry policy and time limit. A halt step fires instead of being submitted.
- **No skip cascade.** A dependent of a skipped step is released normally and sees `null`
  for that source.
- **Finalize.** When every step has settled, the workflow `outputs` are resolved. A skipped
  source reads as `null`, with `pickValue` applied. On an **optional** output (`T?`), a
  `first_non_null` or `the_only_non_null` whose sources are all null gives `null`. Each
  output is then checked against its declared type. The run fails with `output_resolution`
  in two cases:
  - a non-optional output is null, including one whose `pickValue` found no value;
  - a `the_only_non_null` finds more than one value.

  Step inputs keep CWL's error semantics: an all-null pick on a step input is always an
  `input_resolution` error.
- **No concurrency cap.** There is no per-run cap: every released step is submitted, and
  queues and workers throttle the load.

### 8.2 `hysds:OnError`

A **trigger** is a step failing for good (after its retries) or a halt step firing. The mode
decides what happens to the rest of the run:

| mode | queued jobs | running jobs | steps not started | how the run ends |
|---|---|---|---|---|
| **`kill`** (default) | revoked | killed | `cancelled` | at once: `failed`, or the halt's status |
| **`drain`** | revoked | finish; their outputs and datasets are kept | `cancelled` | `halting` until the running jobs settle, then `failed` or the halt's status |
| **`continue`** | unaffected | unaffected | only the steps that depend on the failure, directly or transitively, are `cancelled`, with `kind: upstream_failed` | `failed`, once everything else has settled |

- **The first failure is reported.** `error_class`, `reason` and `failed_step` record the
  first trigger. Under `drain` and `continue`, they appear while the run is still `halting`
  or `running`.
- **Under `continue`, a failed step never reads as null.** Its dependents are cancelled,
  even one that uses `first_non_null` and has another source that completed. Independent
  branches run to the end and their outputs and datasets are published, but the run is
  `failed`.
- **Nothing retries while the run is `halting`.** If a running job fails at that point, the
  failure is recorded and the run ends `failed`.
- **Scatter steps under `drain`.** A scattered step that is fanning out is not cancelled:
  - its queued children are revoked and count as failed (error class `cancelled`);
  - its running children finish;
  - the step then settles by its threshold rule.

  Under `kill`, the scatter step and all its live children are cancelled.
- **Compared with cwltool.** cwltool's `--on-error stop` matches HySDS `drain`, and
  `--on-error continue` matches `continue`. The HySDS default, `kill`, is stricter than
  cwltool's default: a failed orbit stops using workers at once.

### 8.3 Halts

A halt **always stops the run**. Under `continue`, a halt behaves like `drain`: queued jobs
are revoked, steps not started are cancelled (`kind: halt`), running jobs finish, and the run
is `halting` until they do. Only failures follow the `continue` policy.

| halt `status` | how the run ends | run fields |
|---|---|---|
| `failed` | `failed` | `error_class: halted`; `reason` is the halt reason; `failed_step` is the halt step |
| `completed` | `completed` | `reason` is the halt reason. `outputs` are resolved, and outputs from steps that never ran are `null` |

**A failure outranks a completed halt.** If a job that was left running by a drain fails,
the run ends `failed`. If several halts fire, a `failed` halt outranks a `completed` one.

Under a `completed` halt, steps that never ran read as skipped. An optional output fed only
from them, with or without a `pickValue`, is `null` ([§8.1](#81-dataflow)).

### 8.4 Error classes

`error_class` on a failed run, step or attempt is one of:

| class | cause |
|---|---|
| `exit_code:<n>`, `timeout`, `signal`, `offline`, `worker`, `pge:<token>` | the job failed ([§2.5](#25-telling-the-retry-policy-why-you-failed-optional)) |
| `outputs` | the job's `_workflow_outputs.json` was invalid or too large, or a `class` path couldn't be resolved to a published URL. The engine uses the same class if it can't read a completed job's recorded outputs. Retried only under `retry_on: ["*"]` |
| `revoked`, `deduped` | the job was revoked, or the job worker deduplicated it |
| `lost` | the job never reported back, as found by the reaper ([§8.5](#85-timeouts-stalls-and-recovery)). It can be retried |
| `input_resolution` | a source didn't resolve: a missing output, a `first_non_null` or `the_only_non_null` with no answer, a `when` flag that isn't a boolean, or a null scatter input |
| `output_resolution` | a non-optional workflow output was null (also when its `pickValue` found no value), or `the_only_non_null` found several values |
| `scatter_threshold` | too few scatter children completed |
| `halted` | a `failed` halt fired |
| `submit` | the job couldn't be built from the frozen definition, for example because a required param was missing |
| `no_queue` | the step had no queue |
| `cancelled` | a scatter child was cancelled while the run was `halting` |

### 8.5 Timeouts, stalls and recovery

A **reaper** runs every `WORKFLOW_REAPER_INTERVAL` seconds (default 300). It finds runs from
the Redis active-run registry, not from OpenSearch, and:

- **cancels runs older than `WORKFLOW_RUN_TIMEOUT_HOURS`** (default 48), with a reason
  saying the run timed out;
- releases steps whose dependencies all settled but which were never released;
- replays lost job events;
- finishes `halting` runs whose running jobs have all ended;
- handles jobs that never report back:
  - a job still queued after `WORKFLOW_REAPER_QUEUED_SECONDS` fails with error class `lost`;
  - a running job that vanished is treated as `offline`.

  Either can be retried.

Apart from the timeout, it acts only on runs that have made no progress for
`WORKFLOW_STALL_SECONDS`.

While a run is active, **Redis is the source of truth**. Running workflows keep going while
OpenSearch is down, and the `workflow_status` record catches up afterwards. There are two
exceptions:

- registering a workflow and starting a run both read OpenSearch;
- job submission still depends on OpenSearch through the orchestrator's dedup check. This is
  a known core issue, tracked separately.

---

## 9. Registration errors

Every error has a `path`, a `code` and a `message`. These are the common ones:

| code | example message | fix |
|---|---|---|
| `missing_namespace` | `$namespaces must declare the hysds namespace https://github.com/hysds/hysds_commons/cwl/v1# (any prefix)` | add the namespace under `$namespaces` |
| `missing_version` | `s:version is required (declare s: https://schema.org/ and set s:version)` | add `s:version` |
| `bad_id` | `the workflow id is required and must match ^[A-Za-z0-9_.-]+$` | set `id` |
| `unbound_run` | `step 'l1b' is not bound: add hysds:JobType {job_type: ...} (or hysds:Halt) to the Operation's hints` | add the binding hint to the Operation, not the step |
| `double_binding` | `the Operation has both hysds:JobType and hysds:Halt; use exactly one` | keep one binding hint |
| `tool_unsupported` | `CommandLineTool steps are not supported; bind the step to a registered job type with an Operation and a hysds:JobType hint` | register the tool as a job type |
| `subworkflow_unsupported` | `subworkflows are not supported` | inline the steps |
| `unresolved_run` | `run reference 'l1b.cwl' cannot be resolved; inline a class: Operation with a hysds:JobType hint` | inline the Operation |
| `unknown_job_type` | `job type 'job-radiometer-l1b:v3.3' is not registered` | register the job type, or fix the version |
| `unknown_param` | `Operation input 'l1a' is not a parameter of job type 'job-radiometer-l1b:v3.2' (parameters: l1a_files, orbit_id)` | match the hysds-io param names |
| `missing_param` | `job type 'job-radiometer-l1b:v3.2' requires parameter 'orbit_id'; declare it on the Operation and bind it in 'in'` | declare and bind the param |
| `missing_input` | `required Operation input 'orbit_id' is not bound (add it to 'in', give it a default, or make its type optional)` | bind the input |
| `unknown_source` | `step 'l2smp' does not declare output 'l2_file'` | match the step's `out` |
| `cycle` | `dependency cycle: a -> b -> a` | break the cycle |
| `value_from_unsupported` | `valueFrom is not supported (expressions are not allowed); compute the value in a job` | compute the value in a job |
| `js_unsupported` | `JS expressions are not supported; use when: $(inputs.flag)` | use a boolean flag or the decision job |
| `bad_when` | `when refers to inputs.go, which is not an 'in' entry of the step` | add the flag to `in` |
| `when_flag_invalid` | `the when flag 'go' must be a non-optional boolean output of 'qa_check'` | write a `boolean` output |
| `when_flag_invalid` | `the when flag 'go' comes from step 'reprocess_l2smp', which can itself be skipped` | take the flag from an unconditional step |
| `null_unsafe` | see [§3.6](#36-null-safety) | use a `pickValue`, a `default` or an optional type |
| `null_unsafe_output` | `output 'receipt' is fed from 'deliver/receipt', which can be null, so the output type must be optional (?) or use pickValue` | make the output type `T?` |
| `null_unsafe_scatter` | `'l1a/l1a_file' comes from a scatter that tolerates failures, so its items can be null; use source: [l1a/l1a_file], linkMerge: merge_flattened, pickValue: all_non_null or make the item type optional` | use the canonical form ([§3.4](#34-sources-pickvalue-and-linkmerge)) |
| `output_not_optional` | `output 'browse' must be optional (?) because a hysds:Halt has status: completed (B21)` | make every output optional |
| `scatter_with_when` | `scatter and when cannot be combined on one step (B20); put the condition upstream` | move the condition upstream |
| `scatter_input_null` | `a scatter input must be a non-null array; 'find_l0b/l0b_files' can be null (use pickValue or a default, or make the producer unconditional)` | make the scatter input non-null |
| `scatter_unsupported` | `nested and cross-product scatters are not supported: scatter exactly one input` | scatter over one input |
| `missing_requirement` | `scatter requires ScatterFeatureRequirement in requirements` | declare the feature |
| `merge_nested_unsupported` | `linkMerge: merge_nested is not supported; use merge_flattened` | use `merge_flattened` |
| `misplaced_hint` | `hysds:OnError is only allowed on the workflow, not on the step` | move the hint |
| `hysds_requirement` | `hysds: classes are hints, not requirements: move hysds:Retry to hints` | move the class to `hints` |
| `unknown_hysds_hint` | `unknown hysds hint 'Retries' (known: Gather, Halt, JobType, OnError, Queue, Retry, Tag)` | fix the hint name |
| `scatter_connector_removed` | `hysds:ScatterConnector is no longer supported; list the items in an ordinary job step (a mission lister whose output is a list) and scatter over its output ('in: {item: lister/items}')` | use a lister step ([§6.3](#63-listing-items-a-lister-step)) |
| `bad_hint` | `invalid retry_on entry 'exit:137'` | see [§4.4](#44-hysdsretry) |
| `decide_cases` | `case name 'value' is reserved` | see [§3.7](#37-the-decision-job) |
| `decide_out` | `decide output 'maybe' is neither a case name nor the otherwise name (otherwise, reject, reprocess)` | list only case names and the otherwise name in `out` |
| `file_field_unsupported` | `'secondaryFiles' is not supported (File/Directory values are passed to jobs as URL strings)` | remove the field |
| `cwl_validation` | cwltool's own message, with its line and column | fix the CWL |

These codes are warnings, and `ok` stays true: `schemas_ignored`, `ignored_requirement`,
`resource_units`, `foreign_class`, `normalized_pick`, `timelimit_zero` and `cwl_warning`.

---

## 10. Ops notes

**Install.**

- mozart needs `hysds_commons[cwl]`, which brings cwltool 3.1.20251031082601 and
  cwl-utils >=0.40,<0.48. Nothing else imports cwltool.
- The workers need:
  - the `workflow_rules` consumer and reaper processes;
  - the `workflow-connector-decide` queue, with the `job-workflow-decide:v1` job-spec and
    hysds-io registered from `workflow-connectors/configs`.

  Mission listers are ordinary job types on ordinary queues: there is no connector
  allowlist.

**mozart settings** (`settings.cfg`):

| setting | default | purpose |
|---|---|---|
| `WORKFLOW_SPECS_INDEX` | `workflow_specs` | registered workflows |
| `WORKFLOW_STATUS_INDEX` | `workflow_status` | the **read alias** over the daily `workflow_status-YYYY.MM.DD` indices |

**celery settings** (`celeryconfig.py`):

| setting | default |
|---|---|
| `WORKFLOW_QUEUE` | `workflow_queue` |
| `WORKFLOW_REAPER_INTERVAL` | 300 s |
| `WORKFLOW_STALL_SECONDS` | 600 |
| `WORKFLOW_RUN_TIMEOUT_HOURS` | 48 |
| `WORKFLOW_REAPER_QUEUED_SECONDS`, `WORKFLOW_REAPER_VANISHED_SECONDS`, `WORKFLOW_REAPER_EVENT_GRACE_SECONDS`, `WORKFLOW_REAPER_LOCK_TTL` | 600, 300, 120, 120 |
| `WORKFLOW_RUN_TTL_AFTER_END` | 86400 s: a run's Redis keys live for 1 day after the run ends |
| `WORKFLOW_EVENT_TTL` | 86400 s: the fallback TTL of the stored terminal job events that the reaper replays |
| `WORKFLOW_SCATTER_MAX_SNAPSHOTS` | 20 progress snapshots per scatter step (code default) |
| `WORKFLOW_SCATTER_DOC_CHILDREN_MAX` | 500 child job ids kept in the run document (code default) |

**Production checklist.**

- **Redis** must run with AOF (`appendonly yes`) and `maxmemory-policy noeviction`. Size it
  for the run keys plus the backlog on the `logstash` list during an OpenSearch outage.
- **Logstash** needs the `workflow_status` branch (`external_gte` versioning,
  `document_id => run_id`).
- **OpenSearch** needs the sdscli `workflow_status` template, with its aliases, and the
  `*_status*` ISM policy.

The full list is in `hysds-docs/PLAN-workflow-cwl-profile.md`: §6.11 has the checklist, and
§6.12 has Redis memory sizing formulas and worked examples.

---

## 11. Not supported in v2

- Subworkflows (`run: {class: Workflow}`), and `CommandLineTool` and `ExpressionTool` steps.
- JavaScript, and any expression other than `when: $(inputs.<flag>)`. This rules out
  `valueFrom` and expressions in `timelimit` or resource values.
- Nested or cross-product scatter, `scatter` combined with `when`, and
  `linkMerge: merge_nested`.
- `secondaryFiles`, `format`, `checksum`, `contents`, `loadContents` and `loadListing`.
- `run:` references to separate files: every registered workflow is self-contained.
- The v1 scatter connector (`hysds:ScatterConnector`): use a lister step.
- `ToolTimeLimit` of 0 (no limit).
- A per-run concurrency cap, and pausing and resuming runs.

## See also

- Design and decisions: `hysds-docs/PLAN-workflow-cwl-profile.md`. The profile is in §4–5,
  the engine in §6, spec 2.0 in §7, and decisions B1–B28 in §2.
- Worked example: `hysds-docs/examples/radiometer-orbit-l2smp-v2.cwl`.
- Live fixtures, one per behaviour: `local-stack/wf-e2e-v2/fixtures/`. They cover a linear
  chain, a parallel join, a skip, the decision job, a failure, a retry, scatter tolerance,
  halts and on-error modes, and cancel.
- Output binding design: ADR-003, *Workflow Output Binding via Job-Scoped
  `_workflow_outputs.json`*.
- Code:
  - `hysds_commons/cwl/`: the profile and compiler;
  - `hysds/workflow_engine.py`: the engine;
  - `mozart/workflow_api.py`: the endpoints;
  - `workflow-connectors`: the decide job and the `lister` helper;
  - `workflow-connectors-example`: a reference mission plugin with a lister, a scattered
    child job and CWL fixtures.
