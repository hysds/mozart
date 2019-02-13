from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
import os
import sys
from pprint import pprint, pformat

from mozart import app


def createJob(info):
    """
    Create job json for puccini product ingest.

    Example:

    job = {
            'type': 'product_ingest',
            'name': 'ALPSRP003030330',
            'params': {
                'host': "ops@puccini-ariamh.jpl.nasa.gov",
                'data_dir': "/data/public",
                'signal_file': "/data/public/staging/products/ALPSRP003030330.done",
                'staged_prod_dir': "/data/public/staging/products/ALPSRP003030330",
                'staged_work_dir': "/data/public/staging/working/ALPSRP003030330",
                'staged_failed_dir': "/data/public/staging/failed_dir/ALPSRP003030330",
                'datasets_file': "/home/ops/puccini/ops/ldos/datasets.xml",
                'grq_update_url': "http://grq.jpl.nasa.gov:8878/services/update/geoRegionQuery",
                'mozart_host': "mozart-ariamh.jpl.nasa.gov",
                'product_processed_queue': "product_processed",
                'staged_work_url': "http://puccini-ariamh.jpl.nasa.gov/staging/working/working/ALPSRP003030330"
            },
            'localize_urls': []
          }
    """

    # build params
    params = {}
    params['host'] = info['host']
    params['data_dir'] = info['data_dir']
    params['signal_file'] = info['signal_file']
    params['staged_prod_dir'] = info['staged_prod_dir']
    params['staged_work_dir'] = info['staged_work_dir']
    params['staged_failed_dir'] = info['staged_failed_dir']
    params['datasets_file'] = info['datasets_file']
    params['grq_update_url'] = info['grq_update_url']
    params['mozart_host'] = info['mozart_host']
    params['product_processed_queue'] = info['product_processed_queue']
    params['staged_work_url'] = info['staged_work_url']
    job = {
        'type': 'product_ingest',
        'name': os.path.basename(info['staged_prod_dir']),
        'params': params,
        'localize_urls': []
    }

    pprint(job, indent=2)
    return job
