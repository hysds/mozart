from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
import json
from pprint import pprint, pformat

from mozart import app


def notify_by_email(info):
    """
    Create job json for email notification.

    Example:

    job = {
            'type': 'notify_by_email',
            'name': 'action-notify_by_email-CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
            'params': {
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336'
                'emails': 'test@test.com,test2@test.com',
                'url': 'http://path_to_repo',
                'rule_name': 'Email CSK ingest'
            },
            'localize_urls': []
          }
    """

    # build params
    params = {}
    params['id'] = info['objectid']
    params['rule_name'] = info['rule']['rule_name']
    kwargs = json.loads(info['rule']['kwargs'])
    params['emails'] = kwargs['email_addresses']
    rule_hit = info['rule_hit']
    urls = rule_hit['_source']['urls']
    if len(urls) > 0:
        params['url'] = urls[0]
    else:
        params['url'] = None
    job = {
        'type': 'notify_by_email',
        'name': 'action-notify_by_email-%s' % info['objectid'],
        'params': params,
        'localize_urls': []
    }

    pprint(job, indent=2)
    return job


def ftp_push(info):
    """
    Create job json for FTP push.

    Example:

    job = {
            'type': 'ftp_push',
            'name': 'action-ftp_push-CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
            'params': {
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336'
                'url': 'http://path_to_repo'
                'ftp_url': 'ftp://username:password@test.ftp.com/public/data_drop_off',
                'emails': 'test@test.com,test2@test.com',
                'rule_name': 'FTP Push CSK Calimap'
            },
            'localize_urls': []
          }
    """

    # build params
    params = {}
    params['id'] = info['objectid']
    params['rule_name'] = info['rule']['rule_name']
    kwargs = json.loads(info['rule']['kwargs'])
    params['ftp_url'] = kwargs['ftp_url']
    params['emails'] = kwargs['email_addresses']
    rule_hit = info['rule_hit']
    urls = rule_hit['_source']['urls']
    if len(urls) > 0:
        params['url'] = urls[0]
    else:
        params['url'] = None
    job = {
        'type': 'ftp_push',
        'name': 'action-ftp_push-%s' % info['objectid'],
        'params': params,
        'localize_urls': []
    }

    pprint(job, indent=2)
    return job
