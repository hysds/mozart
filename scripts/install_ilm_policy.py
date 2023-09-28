#!/usr/bin/env python
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import open
from future import standard_library
standard_library.install_aliases()

import argparse
import json

from elasticsearch.client import IlmClient
from hysds.es_util import get_mozart_es


mozart_es = get_mozart_es()


def get_parser():
    parser = argparse.ArgumentParser(description="Tool to install Mozart ES ILM Policy")
    parser.add_argument(
        "--policy_file",
        required=True,
        type=str,
        help="Specify the Mozart ILM policy file.",
    )
    parser.add_argument(
        "--ilm_policy_name",
        required=False,
        type=str,
        default="ilm_policy_mozart",
        help="Optionally specify the Mozart ILM policy name. Defaults to 'ilm_policy_mozart'",
    )
    return parser


if __name__ == "__main__":
    args = get_parser().parse_args()

    with open(args.policy_file) as f:
        ilm_policy = json.load(f)

    # https://elasticsearch-py.readthedocs.io/en/7.x/api.html#elasticsearch.client.IlmClient
    # ignore 400 cause by IndexAlreadyExistsException when creating an index
    es_ilm = IlmClient(mozart_es.es)
    es_ilm.put_lifecycle(policy=args.ilm_policy_name, body=ilm_policy)
    print(f"Successfully installed ILM policy to index-delete-policy:\n{json.dumps(ilm_policy, indent=2)}")
