from future import standard_library
standard_library.install_aliases()
from flask_wtf import Form
from wtforms import TextField, BooleanField, PasswordField
from wtforms.validators import Required


class LoginForm(Form):
    username = TextField('username', validators=[Required()])
    password = PasswordField('password', validators=[Required()])
    #remember_me = BooleanField('remember_me', default = False)
