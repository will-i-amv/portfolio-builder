import os

from dotenv import load_dotenv


load_dotenv()
db_passwd = os.environ.get('DB_PASSWD')
db_user = os.environ.get('DB_USER')
db_host = os.environ.get('DB_HOST')


class Settings:
    ROOT_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev'

    # API keys
    API_KEY_TIINGO = os.environ.get('API_KEY_TIINGO')
    API_KEY_EODHD = os.environ.get('API_KEY_EODHD')

    # Database Configurations
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class DevSettings(Settings):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = (
        os.environ.get('DEV_DATABASE_URL') or 
        'sqlite:///' + os.path.join(Settings.ROOT_DIR, 'data-dev.sqlite')
    )


class TestSettings(Settings):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = (
        os.environ.get('TEST_DATABASE_URL') or
        'sqlite://'
    )
    WTF_CSRF_ENABLED = False


class ProdSettings(Settings):
    DB_URL = f'mysql://{db_user}:{db_passwd}@{db_host}'
    SQLALCHEMY_DATABASE_URI = f'{DB_URL}/main'
    SQLALCHEMY_BINDS = {
        "Main": (f'{DB_URL}/main'),
    }

    # Flask-APScheduler Configurations
    SCHEDULER_API_ENABLED = True


settings = {
    'development': DevSettings,
    'testing': TestSettings,
    'production': ProdSettings,
}
