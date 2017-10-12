import os, sys, ftplib
from urlparse import urlsplit
from pprint import pprint, pformat

from mozart import app


def createJob(info):
    """
    Create job json for NS/CI sciflo execution.
    
    Example:

    job = {
            'type': 'sciflo-create_interferograms',
            'name': 'sciflo-create_interferograms-CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
            'params': {
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
                'output_name': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336.interferogram.json'
            },
            'localize_urls': []
          }
    """

    # build params
    params = {}
    params['id'] = info['objectid']
    params['output_name'] = '%s.interferogram.json' % info['objectid']
    job = {
            'type': 'sciflo-create_interferograms',
            'name': 'sciflo-create_interferograms-%s' % info['objectid'],
            'params': params,
            'localize_urls': []
          }

    pprint(job, indent=2)
    return job

def createDatastagerJob(info):
    """
    Create job json for CSK datastager.
    
    Example:

    job = {
            'type': 'datastager',
            'name': 'datastager-EL20140109_654225_3302686.6.2',
            'params': {
                'id': 'EL20140109_654225_3302686.6.2',
                'url': 'https://puccini-ariamh.jpl.nasa.gov/staging/datastager/EL20140109_654225_3302686.6.2'
            },
            'localize_urls': []
          }
    """

    # build params
    job = {
            'type': 'datastager',
            'name': 'datastager-%s' % info['id'],
            'params': info,
            'localize_urls': [],
          }

    pprint(job, indent=2)
    return job

def createDatastagerJobHigh(info):
    """
    Create job json for CSK datastager.
    
    Example:

    job = {
            'type': 'datastager-high',
            'name': 'datastager-high-EL20140109_654225_3302686.6.2',
            'params': {
                'id': 'EL20140109_654225_3302686.6.2',
                'url': 'https://puccini-ariamh.jpl.nasa.gov/staging/datastager/EL20140109_654225_3302686.6.2'
            },
            'localize_urls': []
          }
    """

    # build params
    job = {
            'type': 'datastager-high',
            'name': 'datastager-high-%s' % info['id'],
            'params': info,
            'localize_urls': [],
          }

    pprint(job, indent=2)
    return job
