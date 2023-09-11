from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from setuptools import setup, find_packages


setup(
    name='mozart',
    version='2.2.4',
    long_description='HySDS job orchestration/worker web interface',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'Flask<2.3.0',  # TODO: remove kluge when Flask-DebugToolbar fixes import error
        'flask-restx>=0.5.1',
        'Flask-SQLAlchemy>=3.0.0',
        'Flask-WTF>=0.15.1',
        'Flask-DebugToolbar>=0.11.0',
        'Flask-Login>=0.5.0',
        'gunicorn>=20.1.0',
        'gevent>=1.1.1',
        'eventlet>=0.33.3',
        'requests>=2.7.0',
        'simpleldap>=0.8',
        'simplekml>=1.2.3',
        'tornado>=4.0.2',
        'pika>=0.9.14',
        'pymongo>=2.7.2',
        'boto>=2.38.0',
        'python-dateutil',
        'elasticsearch>=7.0.0,<7.14.0',
        'python-jenkins==1.7.0',
        'future>=0.17.1',
        'pytz',
        'numpy',
        "werkzeug>=2.2.0",  # TODO: remove this pin after fix has been made https://stackoverflow.com/a/73105878
        "cryptography>=39.0.0"
    ]
)
