import os, sys, ftplib
from urllib.parse import urlsplit
from pprint import pprint, pformat

from mozart import app


def network_selector(info):
    """
    Create job json for network selector.
    
    Example:

    job = {
            'type': 'network_selector',
            'params': {
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
                'output_file': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336.interferogram.json'
            },
            'localize_urls': []
          }
    """

    print("Info:")
    pprint(info, indent=2)

    # build params
    job = {
            'type': 'network_selector',
            'name': 'network_selector-%s' % info['id'],
            'params': info,
            'localize_urls': []
          }

    print("Job:")
    pprint(job, indent=2)
    return job

def create_interferogram(info):
    """
    Create map job json for generating interferogram.
    
    Example:

    job = {
            'type': 'create_interferogram',
            'params': {
                'netsel_file': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336.interferogram.json_0'
            },
            'localize_urls': [
              {
                'url': 'http://puccini-ariamh.jpl.nasa.gov:8085/work/jobs/CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336.interferogram.json_0'
              }
            ]
          }
    """

    print("Info:")
    pprint(info, indent=2)

    # build params
    job = {
            'type': 'create_interferogram',
            'name': 'create_interferogram-%s' % info['netsel_file'],
            'params': info,
            'localize_urls': [{'url': info['netsel_url']}]
          }

    print("Job:")
    pprint(job, indent=2)
    return job
