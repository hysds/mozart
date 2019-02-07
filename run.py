from mozart import app
from OpenSSL import SSL

#context = SSL.Context(SSL.SSLv23_METHOD)
# context.use_privatekey_file('/home/ops/ssl/server.key')
# context.use_certificate_file('/home/ops/ssl/server.pem')
context = ('server.pem', 'server.key')


if __name__ == '__main__':
    app.run(host="0.0.0.0",
            port=app.config['PORT'], debug=True, ssl_context=context)
