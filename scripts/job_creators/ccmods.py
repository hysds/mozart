import os
import sys
from urllib.parse import urlsplit
from pprint import pprint, pformat

from mozart import app


def createIgraMatchupJob(info):
    """
    Create job json for IGRA matchup.

    Example:

    job = {
            'type': 'get_igra_modis_all',
            'name': 'get_igra_modis_all-2010-07',
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
    params = {}
    params['year'] = info['year']
    params['month'] = info['month']
    job = {
        'type': 'get_igra_modis_all',
        'name': 'get_igra_modis_all-%04d-%02d' % (int(info['year']), int(info['month'])),
        'params': params,
        'localize_urls': []
    }

    print("Job:")
    pprint(job, indent=2)
    return job


def createAggregateAirsJob(info):
    """
    Create job json for AIRS data aggregation.

    Example:

    job = {
            'type': 'get_airs_all',
            'name': 'get_airs_all-2007-10',
            'params': {
                'matchup_file': 'igra_airs_modis_matchup_indices-2007-10.sav'
            },
            'localize_urls': [
              {
                'url': 'ftp://puccini/repository/products/igra_airs_matchups/2007/igra_airs_modis_matchup_indices-2007-10/igra_airs_modis_matchup_indices-2007-10.sav'
              },
            ]
          }
    """

    print("Info:")
    pprint(info, indent=2)

    # sav file
    sav_file = '%s.sav' % info['objectid']

    # get dav url
    dav_url = None
    for url in info['urls']:
        if url.startswith('http://puccini-ccmods.jpl.nasa.gov:5000'):
            dav_url = os.path.join(url, sav_file)
    if dav_url is None:
        raise RuntimeError("Failed to find FTP url: %s" % pformat(info))

    # build params
    params = {}
    params['matchup_file'] = sav_file
    job = {
        'type': 'get_airs_all',
        'name': 'get_airs_all-%s' % '-'.join(os.path.splitext(sav_file)[0].split('-')[1:]),
        'params': params,
        'localize_urls': [{'url': dav_url}]
    }

    print("Job:")
    pprint(job, indent=2)
    return job
