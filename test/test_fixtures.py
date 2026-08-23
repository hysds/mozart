"""Self-tests for the shared workflow-orchestration fixtures in conftest.py.

Later work units build their tests on these fixtures, so their shape is a
contract. These tests are what stops that contract drifting silently.
"""

NODE_KINDS = {"job", "pass", "scatter", "gather", "gate", "switch", "halt"}

SUCCESSOR_FIELDS = {
    "job": ("next",),
    "pass": ("next",),
    "scatter": ("next",),
    "gather": ("next",),
    "gate": ("on_true", "on_false"),
    "switch": ("default",),
    "halt": ("next",),
}

# ADR-003 typed value slots; exactly one per output entry
VALUE_SLOTS = ("value_s", "value_d", "value_b")


def test_workflow_spec_topology_is_closed(workflow_spec):
    """Every successor pointer names a node that exists in the spec."""
    nodes = workflow_spec["nodes"]
    assert workflow_spec["entry"] in nodes

    for node_id, node in nodes.items():
        kind = node["kind"]
        assert kind in NODE_KINDS, f"{node_id}: unknown kind {kind}"

        targets = []
        for field in SUCCESSOR_FIELDS[kind]:
            targets.extend(node.get(field, []))
        if kind == "switch":
            for case in node.get("cases", []):
                targets.extend(case["next"])
        for target in targets:
            assert target in nodes, f"{node_id} points at unknown node {target}"


def test_workflow_spec_refs_resolve_to_known_nodes_or_inputs(workflow_spec):
    """``$.<node>.<output>`` / ``$.inputs.<name>`` refs name real things."""
    nodes = workflow_spec["nodes"]
    inputs = workflow_spec["inputs"]

    def check(ref):
        if not (isinstance(ref, str) and ref.startswith("$.")):
            return
        head = ref[2:].split(".")[0]
        if head == "inputs":
            assert ref[2:].split(".")[1] in inputs, f"unknown input in {ref}"
        elif head == "scatter":
            return  # $.scatter.item -- the per-child element
        else:
            assert head in nodes or head in _switch_binds(nodes), f"bad ref {ref}"

    for node in nodes.values():
        for field in ("in", "bind"):
            for ref in node.get(field, {}).values():
                check(ref)
        check(node.get("over"))
        check(node.get("collect"))
        for side in ("left", "right"):
            check(node.get("when", {}).get(side))

    for ref in workflow_spec["outputs"].values():
        check(ref)


def _switch_binds(nodes):
    """Local aliases a ``switch`` introduces via its ``bind`` block."""
    names = set()
    for node in nodes.values():
        if node["kind"] == "switch":
            names.update(node.get("bind", {}))
    return names


def test_workflow_status_shares_node_namespace_with_spec(
    workflow_spec, workflow_status
):
    """``spec.nodes[X]`` and ``status.nodes[X]`` share the node-id namespace."""
    assert set(workflow_status["nodes"]) == set(workflow_spec["nodes"])
    for node_id, state in workflow_status["nodes"].items():
        assert state["state"] in {
            "pending",
            "running",
            "completed",
            "failed",
            "skipped",
        }, f"{node_id} has unknown state {state['state']}"


def test_workflow_status_scatter_records_expected_n(workflow_status):
    """The N-init race guard: scatter records ``expected`` at fan-out."""
    scatter = workflow_status["nodes"]["scatter_1"]
    assert scatter["state"] == "running"
    assert scatter["expected"] == 3


def test_workflow_queue_message_outputs_follow_adr_003(workflow_queue_message):
    """Each binding names exactly one typed slot, per ADR-003."""
    msg = workflow_queue_message
    for field in ("run_id", "step_id", "job_id", "status"):
        assert msg[field] is not None

    for output in msg["outputs"]:
        assert output["name"]
        slots = [slot for slot in VALUE_SLOTS if slot in output]
        assert slots == [slots[0]] and len(slots) == 1, (
            f"{output['name']} declares {len(slots)} typed slots"
        )


def test_workflow_queue_message_array_output_preserves_order(workflow_status):
    """Array bindings must keep their order -- scatter enumerates by index."""
    items = workflow_status["nodes"]["query_data"]["outputs"][0]["value_s"]
    assert items == ["granule_a.h5", "granule_b.h5", "granule_c.h5"]


def test_fake_redis_supports_barrier_hash_ops(fake_redis):
    """The barrier is a per-(run_id, step_id) hash of child_job_id -> status."""
    key = "workflow_barrier:run-abc123:process_granules"

    assert fake_redis.hsetnx(key, "child-0", "job-completed") == 1
    # a redelivered event for the same child is a no-op (dedup by job_id)
    assert fake_redis.hsetnx(key, "child-0", "job-failed") == 0
    fake_redis.hsetnx(key, "child-1", "job-completed")
    fake_redis.hsetnx(key, "child-2", "job-failed")

    counts = fake_redis.hgetall(key)
    assert len(counts) == 3
    assert sum(1 for v in counts.values() if v == "job-completed") == 2


def test_fake_redis_supports_atomic_lua_eval(fake_redis):
    """Barrier accounting runs as one atomic Lua op; fakeredis must eval it."""
    script = """
    local added = redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2])
    if added == 0 then return -1 end
    local done = redis.call('HLEN', KEYS[1])
    if tonumber(done) >= tonumber(ARGV[3]) then return 1 end
    return 0
    """
    key = "workflow_barrier:run-abc123:scatter_1"
    assert fake_redis.eval(script, 1, key, "child-0", "job-completed", 2) == 0
    # redelivery elects no winner
    assert fake_redis.eval(script, 1, key, "child-0", "job-completed", 2) == -1
    # exactly one winner on threshold
    assert fake_redis.eval(script, 1, key, "child-1", "job-completed", 2) == 1
