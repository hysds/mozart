from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from fabric.api import env, run, sudo


from mozart import app


def sys_date(user, host):
    """Return date on  host."""

    env.host_string = '%s@%s' % (user, host)
    env.key_filename = app.config['KEY_FILENAME']
    env.abort_on_prompts = True
    return run("TZ='America/Los_Angeles' date")


def mpstat(user, host):
    """Return mpstat for host."""

    env.host_string = '%s@%s' % (user, host)
    env.key_filename = app.config['KEY_FILENAME']
    env.abort_on_prompts = True
    return sudo('mpstat -P ALL 1 1')


def mem_free(user, host):
    """Return free for host."""

    env.host_string = '%s@%s' % (user, host)
    env.key_filename = app.config['KEY_FILENAME']
    env.abort_on_prompts = True
    return sudo('free')


def top(user, host):
    """Return top for host."""

    env.host_string = '%s@%s' % (user, host)
    env.key_filename = app.config['KEY_FILENAME']
    env.abort_on_prompts = True
    return sudo('top -n 1 -b')
