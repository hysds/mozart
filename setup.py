from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from setuptools import setup, find_packages

setup(
    name='mozart',
    version='1.1.6',
    long_description='HySDS job orchestration/worker web interface',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=['Flask>=0.10.1', 'gunicorn>=19.1.1', 'gevent>=1.1.1',
                      'eventlet>=0.17.4', 'requests>=2.7.0', 'Flask-SQLAlchemy>=1.0',
                      'Flask-WTF>=0.10.0', 'Flask-DebugToolbar>=0.9.0',
                      'Flask-Login>=0.3.2', 'simpleldap>=0.8',
                      'simplekml>=1.2.3', 'tornado>=4.0.2', 'pika>=0.9.14',
                      'pymongo>=2.7.2', 'boto>=2.38.0', 'python-dateutil',
                      'elasticsearch>=1.0.0,<2.0.0', 'pytz', 'numpy',
                      'flask-restx>=0.2.0', 'future>=0.17.1']
)
