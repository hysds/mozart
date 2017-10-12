mozart
======

To install/develop
--------------------------
python setup.py develop|install

To run in development mode
--------------------------
python run.py

To run in production mode
--------------------------
gunicorn -w2 -b 0.0.0.0:8888 -k gevent --daemon -p mozart.pid mozart:app
