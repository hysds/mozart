from pprint import pprint, pformat

from mozart import app


def createJob(info):
    """
    Create job json for user rules processing.
    
    Example:

    job = {
            'type': 'user_rules_processor',
            'name': 'user_rules_processor-CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336',
            'params': {
                'id': 'CSKS4_RAW_B_HI_11_HH_RD_20131004020329_20131004020336'
            },
            'localize_urls': []
          }
    """

    # build params
    params = {}
    params['id'] = info['objectid']
    job = {
            'type': 'user_rules_processor',
            'name': 'user_rules_processor-%s' % info['objectid'],
            'params': params,
            'localize_urls': []
          }

    pprint(job, indent=2)
    return job
