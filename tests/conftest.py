import logging

import pytest
from flask.testing import FlaskClient
from flask_login import login_user, logout_user

from portfolio_builder import create_app, db as _db, scheduler as _sched
from portfolio_builder.auth.models import User


@pytest.fixture(scope='module')
def app():
    """Create application for the tests."""
    _app = create_app(settings_name='testing')
    _app.logger.setLevel(logging.CRITICAL)
    ctx = _app.test_request_context()
    ctx.push()

    yield _app

    ctx.pop()
    _sched.shutdown(wait=False)


@pytest.fixture(scope='module')
def db(app):
    """Create database for the tests."""
    _db.app = app # type: ignore
    with app.app_context():
        _db.create_all()
        test_user = User(username='TestUser', password='TestPass') # type: ignore
        _db.session.add(test_user)
        _db.session.commit()
    
    yield _db

    # Explicitly close DB connection
    _db.session.close()
    _db.drop_all()

 
@pytest.fixture(scope='module')
def client(app):
    return app.test_client()


@pytest.fixture(scope='function')
def login_required(app, client, db):
    # As an alternative to this fixture
    # just use LOGIN_DISABLED = True in your test settings
    with app.app_context():
        test_user = db.session.query(User).first() 
        with app.test_request_context():
            yield login_user(test_user)
            logout_user()
            with client.session_transaction() as session:
                session.clear()
