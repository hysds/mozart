#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()

import argparse
import os
from jinja2 import Template

from hysds.es_util import get_mozart_es


mozart_es = get_mozart_es()


def get_parser():
    parser = argparse.ArgumentParser(
        description="Tool to install Mozart ES templates to the cluster."
    )
    parser.add_argument(
        "--install_job_templates",
        required=False,
        action="store_true",
        default=False,
        help="Optionally specify this flag to install the job templates (job_status, worker_status, "
        "event_status, task_status).",
    )
    parser.add_argument(
        "--template_dir",
        required=False,
        type=str,
        help="Optionally specify this flag to tell the tool where to find the given templates.",
    )
    return parser


def write_template(index, tmpl_file):
    """Write template to ES."""

    with open(tmpl_file) as f:
        tmpl = Template(f.read()).render(index=index)

    # https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.IndicesClient.delete_template
    mozart_es.es.indices.delete_template(name=index, ignore=[400, 404])

    # https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.IndicesClient.put_template
    mozart_es.es.indices.put_template(name=index, body=tmpl)
    print("Successfully installed template %s" % index)


def write_index_template(index, tmpl_file):
    """Write template to ES."""

    with open(tmpl_file) as f:
        tmpl = Template(f.read()).render(index=index)

    # https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.IndicesClient.delete_template
    mozart_es.es.indices.delete_index_template(name=index, ignore=[400, 404])

    # https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.IndicesClient.put_template
    mozart_es.es.indices.put_index_template(name=index, body=tmpl)
    print("Successfully installed index template %s" % index)


if __name__ == "__main__":
    args = get_parser().parse_args()

    if args.install_job_templates is False:
        indices = ("containers", "job_specs", "hysds_ios")

        curr_file = os.path.dirname(__file__)
        tmpl_file = os.path.abspath(
            os.path.join(curr_file, "..", "configs", "es_template.json")
        )
        tmpl_file = os.path.normpath(tmpl_file)

        for index in indices:
            write_template(index, tmpl_file)
    else:
        templates = [
            "job_status.template",
            "worker_status.template",
            "task_status.template",
            "event_status.template",
        ]
        if args.template_dir is None:
            raise RuntimeError(
                "Must specify --template_dir when installing job templates."
            )

        for template in templates:
            # Copy templates to etc/ directory
            template_file = f"{args.template_dir}/{template}"
            template_doc_name = template.split(".template")[0]
            print(f"Creating ES index template for {template}")
            write_index_template(template_doc_name, template_file)
