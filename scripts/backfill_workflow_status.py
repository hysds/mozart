#!/usr/bin/env python
"""Idempotent backfill of existing workflow_status run docs (LOCAL / dev stacks only).

 * copy the legacy ``user`` field to ``username`` (the mapped keyword) and drop ``user``
 * for terminal runs (completed/failed/cancelled) lacking ``ended_at``, set it from ``updated_at``
 * derive ``spec_name`` / ``spec_version`` (from ``spec_id``), ``has_scatter`` (from the registered spec),
   ``duration_s`` (``ended_at - submitted_at``) and, for failed runs, ``failed_node`` (the first node
   whose state is ``failed``). ``error_class`` and node ``attempts``/times are not derivable and stay absent.

Re-running is a no-op. Refuses to run unless the OpenSearch host is localhost/127.0.0.1,
unless --force-remote is passed. Use --dry-run to count what would change.
"""
import argparse
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse


SCRIPT = """
boolean changed = false;
if (ctx._source.containsKey('user')) {
  if (ctx._source.username == null && ctx._source.user != null) { ctx._source.username = ctx._source.user; }
  ctx._source.remove('user');
  changed = true;
}
if (ctx._source.ended_at == null && ctx._source.updated_at != null
    && (ctx._source.status == 'completed' || ctx._source.status == 'failed' || ctx._source.status == 'cancelled')) {
  ctx._source.ended_at = ctx._source.updated_at;
  changed = true;
}
if (!changed) { ctx.op = 'noop'; }
"""

QUERY = {"bool": {"minimum_should_match": 1, "should": [
    {"exists": {"field": "user"}},
    {"bool": {"filter": [{"terms": {"status": ["completed", "failed", "cancelled"]}}],
              "must_not": [{"exists": {"field": "ended_at"}}]}},
]}}


TERMINAL = ("completed", "failed", "cancelled")

# runs missing a derivable field (terminal runs also need duration_s; failed runs failed_node)
DERIVE_QUERY = {"bool": {"minimum_should_match": 1, "should": [
    {"bool": {"must_not": [{"exists": {"field": "spec_name"}}]}},
    {"bool": {"must_not": [{"exists": {"field": "has_scatter"}}]}},
    {"bool": {"filter": [{"terms": {"status": list(TERMINAL)}}, {"exists": {"field": "ended_at"}}],
              "must_not": [{"exists": {"field": "duration_s"}}]}},
    {"bool": {"filter": [{"term": {"status": "failed"}}], "must_not": [{"exists": {"field": "failed_node"}}]}},
]}}


def _ts(v):
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def derive_fields(doc, spec):
    """``{field: value}`` to add to a run doc (only fields that are missing and derivable)."""
    out = {}
    spec_id = doc.get("spec_id")
    if isinstance(spec_id, str) and ":" in spec_id and "spec_name" not in doc:
        out["spec_name"], out["spec_version"] = spec_id.rsplit(":", 1)
    if spec is not None and "has_scatter" not in doc:
        out["has_scatter"] = any((n or {}).get("kind") == "scatter" for n in (spec.get("nodes") or {}).values())
    start, end = _ts(doc.get("submitted_at")), _ts(doc.get("ended_at"))
    if doc.get("status") in TERMINAL and start and end and doc.get("duration_s") is None:
        out["duration_s"] = round(max(0.0, (end - start).total_seconds()), 3)
    if doc.get("status") == "failed" and not doc.get("failed_node"):
        failed = [nid for nid, n in (doc.get("nodes") or {}).items() if (n or {}).get("state") == "failed"]
        if failed:
            out["failed_node"] = failed[0]
    return out


def derive_pass(es, index, specs_index, dry_run):
    """Fill derivable fields doc by doc with a partial update (idempotent: only missing fields)."""
    specs, n_docs, n_upd = {}, 0, 0
    r = es.search(index=index, body={"query": DERIVE_QUERY, "size": 500, "sort": ["_doc"]}, scroll="2m")
    while r["hits"]["hits"]:
        for h in r["hits"]["hits"]:
            n_docs += 1
            doc = h["_source"]
            sid = doc.get("spec_id")
            if sid not in specs:
                g = es.get(index=specs_index, id=sid, ignore=404) if sid else None
                specs[sid] = g["_source"] if isinstance(g, dict) and g.get("found") else None
            fields = derive_fields(doc, specs[sid])
            if fields:
                n_upd += 1
                if not dry_run:
                    es.update(index=index, id=h["_id"], body={"doc": fields}, ignore=409)
        r = es.scroll(scroll_id=r["_scroll_id"], scroll="2m")
    return n_docs, n_upd


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force-remote", action="store_true", help="allow a non-local OpenSearch host")
    args = ap.parse_args()

    # imported here so the pure helpers above can be unit-tested without a celery config
    from hysds.es_util import get_mozart_es
    from mozart import app

    index = app.config["WORKFLOW_STATUS_INDEX"]
    es = get_mozart_es().es
    from hysds.celery import app as hysds_app

    es_url = str(hysds_app.conf.get("JOBS_ES_URL") or "")
    names = [urlparse(u if "//" in u else "//" + u).hostname for u in es_url.replace(",", " ").split()]
    if not args.force_remote and any(n not in ("localhost", "127.0.0.1", "::1") for n in names):
        sys.exit(f"refusing to backfill non-local OpenSearch ({names}); pass --force-remote to override")

    specs_index = app.config["WORKFLOW_SPECS_INDEX"]
    if args.dry_run:
        print("would update %d docs in %s" % (es.count(index=index, body={"query": QUERY})["count"], index))
        n_docs, n_upd = derive_pass(es, index, specs_index, True)
        print("would derive fields on %d of %d candidate docs (ended_at not yet backfilled counts as no duration)"
              % (n_upd, n_docs))
        return
    r = es.update_by_query(index=index, body={"query": QUERY, "script": {"lang": "painless", "source": SCRIPT}},
                           refresh=True, conflicts="proceed")
    print("%s: updated=%s noops=%s version_conflicts=%s" % (index, r.get("updated"), r.get("noops"),
                                                          r.get("version_conflicts")))
    es.indices.refresh(index=index)
    n_docs, n_upd = derive_pass(es, index, specs_index, False)
    print("%s: derived fields on %d of %d candidate docs" % (index, n_upd, n_docs))


if __name__ == "__main__":
    main()
