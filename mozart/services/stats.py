import os, json, time, re, requests, traceback, math
from flask import jsonify, Blueprint, request
from datetime import datetime, timedelta
from dateutil.parser import parse
from pprint import pprint, pformat


from mozart import app
from mozart.lib.fab_utils import mpstat, mem_free, sys_date, top
from mozart.lib.job_utils import get_execute_nodes


mod = Blueprint('services/stats', __name__)


CPU_RE = re.compile(r'^Average:\s+(?P<cpu>.*?)\s+(?P<user>.*?)\s+.*?\s+(?P<sys>.*?)')
MEM_RE = re.compile(r'^Mem:\s+(?P<total>\d+)\s+(?P<used>\d+)')
SWAP_RE = re.compile(r'^Swap:\s+(?P<total>\d+)\s+(?P<used>\d+)')


def parse_cpu_stats(output):
    """Parse cpu stats to NVD3 discrete bar chart data."""

    values = []
    for line in output.split('\n'):
        match = CPU_RE.search(line.strip())
        if not match: continue
        stats = match.groupdict()
        if stats['cpu'] == 'CPU': continue
        elif stats['cpu'] == 'all':
            values.extend([
                {
                    'label': 'all CPU (user)',
                    'value': stats['user']
                },
                {
                    'label': 'all CPU (sys)',
                    'value': stats['sys']
                }
            ])
        else:
            values.extend([
                {
                    'label': 'CPU %s (user)' % stats['cpu'],
                    'value': stats['user']
                },
                {
                    'label': 'CPU %s (sys)' % stats['cpu'],
                    'value': stats['sys']
                }
            ])
    return values
               

def parse_mem_stats(output):
    """Parse memory stats to NVD3 discrete bar chart data."""

    values = []
    for line in output.split('\n'):
        match = MEM_RE.search(line.strip())
        if match:
            stats = match.groupdict()
            mem_total = float(stats['total'])
            mem_used = float(stats['used'])
            if mem_total == 0.: mem_used_perc = 0.
            else:
                mem_used_perc = math.ceil(float(stats['used'])/float(stats['total'])*100.)
            values.append({
                    'label': 'memory',
                    'value': mem_used_perc
            })
            continue
        match = SWAP_RE.search(line.strip())
        if match:
            stats = match.groupdict()
            swap_total = float(stats['total'])
            swap_used = float(stats['used'])
            if swap_total == 0.: swap_used_perc = 0.
            else:
                swap_used_perc = math.ceil(float(stats['used'])/float(stats['total'])*100.)
            values.append({
                    'label': 'swap',
                    'value': swap_used_perc
            })
            continue
    return values
               

@mod.route('/execute_nodes', methods=['GET'])
def execute_nodes():
    """Return the names of all execute nodes."""

    nodes = get_execute_nodes()
    return jsonify({
        'success': True,
        'message': '',
        'nodes': nodes,
        'total': len(nodes)
    })


@mod.route('/mpstat', methods=['GET'])
def get_mpstat():
    """Return the CPU stats for a node."""

    node = request.args.get('node', None)
    if node is None:
        return jsonify({
            'success': False,
            'message': "No execute node specfied."
        }), 500
    if node == app.config['PUCCINI_HOST']:
        user = app.config['PUCCINI_USER']
    else: user = app.config['EXECUTE_NODE_USER']
    try: output = mpstat(user, node)
    except (Exception, SystemExit), e:
        app.logger.info("Failed to execute mpstat('%s', '%s'):\n%s\n%s" %
                        (user, node, str(e), traceback.format_exc()))
        return jsonify({
            'success': False,
            'message': str(e),
            'stats': None,
            'node': node
        })

    return jsonify({
        'success': True,
        'message': '',
        'stats': output,
        'node': node
    })


@mod.route('/mem_free', methods=['GET'])
def get_mem_free():
    """Return the memory stats for a node."""

    node = request.args.get('node', None)
    if node is None:
        return jsonify({
            'success': False,
            'message': "No execute node specfied."
        }), 500
    if node == app.config['PUCCINI_HOST']:
        user = app.config['PUCCINI_USER']
    else: user = app.config['EXECUTE_NODE_USER']
    try: output = mem_free(user, node)
    except (Exception, SystemExit), e:
        app.logger.info("Failed to execute mem_free('%s', '%s'):\n%s\n%s" %
                        (user, node, str(e), traceback.format_exc()))
        return jsonify({
            'success': False,
            'message': str(e),
            'stats': None,
            'node': node
        })

    return jsonify({
        'success': True,
        'message': '',
        'stats': output,
        'node': node
    })


@mod.route('/cpu_mem_stats', methods=['GET'])
def get_cpu_mem_stats():
    """Return the CPU and memory stats for a node."""

    node = request.args.get('node', None)
    if node is None:
        return jsonify({
            'success': False,
            'message': "No execute node specfied."
        }), 500
    if node == app.config['PUCCINI_HOST']:
        user = app.config['PUCCINI_USER']
    else: user = app.config['EXECUTE_NODE_USER']
    try:
        cpu_stats = mpstat(user, node)
        values = parse_cpu_stats(cpu_stats)
        mem_stats = mem_free(user, node)
        values.extend(parse_mem_stats(mem_stats))
        date = sys_date(user, node)
        top_procs = top(user, node)
    except (Exception, SystemExit), e:
        app.logger.info("Failed to execute mpstat/mem_free('%s', '%s'):\n%s\n%s" %
                        (user, node, str(e), traceback.format_exc()))
        return jsonify({
            'success': False,
            'message': str(e),
            'node': node,
            'date': None,
            'stats': {
                'cpu': None,
                'memory': None,
                'top': None
            },
            'data': []
        })

    return jsonify({
        'success': True,
        'message': '',
        'node': node,
        'date': date,
        'stats': {
            'cpu': cpu_stats,
            'memory': mem_stats,
            'top': top_procs
        },
        'data': [{
            'key': 'CPU and Memory Utilization',
            'values': values
        }]
    })
