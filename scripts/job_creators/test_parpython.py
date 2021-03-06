from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import int
from future import standard_library
standard_library.install_aliases()
import os
import sys
import ftplib
from urllib.parse import urlsplit
from pprint import pprint, pformat

from mozart import app


def createParPythonMapJob(info):
    """
    Create map job json for IGRA matchup.

    Example:

    job = {
            'type': 'test_map_parpython',
            'params': {
                'year': 2010,
                'month': 7
            },
            'localize_urls': [
            ]
          }
    """

    print("Info:")
    pprint(info, indent=2)

    # build parrams
    job = {
        'type': 'test_map_parpython',
        'name': 'test_map_parpython-%04d-%02d' % (int(info['year']), int(info['month'])),
        'params': info,
        'localize_urls': []
    }

    print("Job:")
    pprint(job, indent=2)
    return job


def createParPythonReduceJob(info):
    """
    Create reduce job json for IGRA matchup.

    Example:

    job = {
            'type': 'test_reduce_parpython',
            'params': {
                'results': <results>,
            },
            'localize_urls': [
            ]
          }
    """

    print("Info:")
    pprint(info, indent=2)

    # build parrams
    job = {
        'type': 'test_reduce_parpython',
        'name': 'test_reduce_parpython',
        'params': info,
        'localize_urls': []
    }

    print("Job:")
    pprint(job, indent=2)
    return job
