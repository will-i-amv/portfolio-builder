import pytest


def test_logged_out(client):
    res = client.get('/')
    assert res.status_code == 302


@pytest.mark.usefixtures("authenticated_request")
def test_logged_in(client):
    res = client.get('/')
    assert res.status_code == 200


