#!/usr/bin/env python
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import open
from future import standard_library
standard_library.install_aliases()

import os
from jinja2 import Template

from hysds.es_util import get_mozart_es


mozart_es = get_mozart_es()


def write_template(index, tmpl_file):
    """Write template to ES."""

    with open(tmpl_file) as f:
        tmpl = Template(f.read()).render(index=index)

    # https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.IndicesClient.delete_template
    mozart_es.es.indices.delete_template(name=index, ignore=400)

    # https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.IndicesClient.put_template
    mozart_es.es.indices.put_template(name=index, body=tmpl)
    print("Successfully installed template %s" % index)


if __name__ == "__main__":
    indices = ("containers", "job_specs", "hysds_ios")

    curr_file = os.path.dirname(__file__)
    tmpl_file = os.path.abspath(os.path.join(curr_file, '..', 'config', 'es_template.json'))
    tmpl_file = os.path.normpath(tmpl_file)

    for index in indices:
        write_template(index, tmpl_file)
