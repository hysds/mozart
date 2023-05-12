#!/usr/bin/env python
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import open
from future import standard_library
standard_library.install_aliases()
import os
import sys
import json

from hysds.es_util import get_mozart_es
mozart_es = get_mozart_es()


def write_template(tmpl_file):
    """Write template to ES."""

    with open(tmpl_file) as f:
        tmpl = json.load(f)

    # https://elasticsearch-py.readthedocs.io/en/1.3.0/api.html#elasticsearch.Elasticsearch.put_template
    mozart_es.es.indices.put_template(name="index_defaults", body=tmpl, ignore=400)
    print(f"Successfully installed template to index_defaults:\n{json.dumps(tmpl, indent=2)}")


if __name__ == "__main__":

    # get template file
    tmpl_file = os.path.abspath(sys.argv[1])

    write_template(tmpl_file)
