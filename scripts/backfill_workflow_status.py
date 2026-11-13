#!/usr/bin/env python
"""Idempotent backfill of existing workflow_status run docs (LOCAL / dev stacks only).

 * copy the legacy ``user`` field to ``username`` (the mapped keyword) and drop ``user``
 * for terminal runs (completed/failed/cancelled) lacking ``ended_at``, set it from ``updated_at``

Re-running is a no-op. Refuses to run unless the OpenSearch host is localhost/127.0.0.1,
unless --force-remote is passed. Use --dry-run to count what would change.
"""
import argparse
import sys
from urllib.parse import urlparse

from hysds.es_util import get_mozart_es
from mozart import app

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


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force-remote", action="store_true", help="allow a non-local OpenSearch host")
    args = ap.parse_args()

    index = app.config["WORKFLOW_STATUS_INDEX"]
    es = get_mozart_es().es
    from hysds.celery import app as hysds_app

    es_url = str(hysds_app.conf.get("JOBS_ES_URL") or "")
    names = [urlparse(u if "//" in u else "//" + u).hostname for u in es_url.replace(",", " ").split()]
    if not args.force_remote and any(n not in ("localhost", "127.0.0.1", "::1") for n in names):
        sys.exit(f"refusing to backfill non-local OpenSearch ({names}); pass --force-remote to override")

    if args.dry_run:
        print("would update %d docs in %s" % (es.count(index=index, body={"query": QUERY})["count"], index))
        return
    r = es.update_by_query(index=index, body={"query": QUERY, "script": {"lang": "painless", "source": SCRIPT}},
                           refresh=True, conflicts="proceed")
    print("%s: updated=%s noops=%s version_conflicts=%s" % (index, r.get("updated"), r.get("noops"),
                                                          r.get("version_conflicts")))


if __name__ == "__main__":
    main()
