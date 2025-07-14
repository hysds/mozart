from future import standard_library

standard_library.install_aliases()
import os
from pprint import pprint


def get_agg_file(info):
    """
    Create map job json for generating agg file.

    Example:

    job = {
            'type': 'get_agg_file',
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

    # build params
    job = {
        "type": "get_agg_file",
        "name": "get_agg_file-%04d-%02d" % (int(info["year"]), int(info["month"])),
        "params": info,
        "localize_urls": [],
    }

    print("Job:")
    pprint(job, indent=2)
    return job


def generate_merged_file(info):
    print("Info:")
    pprint(info, indent=2)

    # build parrams
    job = {
        "type": "generate_merged_file",
        "name": "generate_merged_file",
        "params": info,
        "localize_urls": info["agg_urls"],
    }

    print("Job:")
    pprint(job, indent=2)
    return job


def generate_pdf_plots(info):
    print("Info:")
    pprint(info, indent=2)

    # build parrams
    job = {
        "type": "generate_pdf_plots",
        "name": "generate_pdf_plots",
        "params": info,
        "localize_urls": [info["merge_url"]],
    }

    print("Job:")
    pprint(job, indent=2)
    return job


def wvcc_generate_matchup(info):
    """
    Create map job json for running WVCC matchup.

    Example:

    job = {
            'type': 'wvcc_generate_matchup',
            'params': {
                'dap_url': 'http://msas-dap.jpl.nasa.gov/...'
            },
            'localize_urls': [
            ]
          }
    """

    print("Info:")
    pprint(info, indent=2)

    airs_id = os.path.basename(info["dap_url"])[5:19]

    # build params
    job = {
        "type": "wvcc_generate_matchup",
        "name": "wvcc_generate_matchup-%s" % airs_id,
        "params": info,
        "localize_urls": [],
    }

    print("Job:")
    pprint(job, indent=2)
    return job
