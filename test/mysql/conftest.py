import os
import pytest

@pytest.fixture(scope='module', autouse=True)
def init():
    os.environ['LINGORM_CONFIG'] = os.path.abspath(
        os.path.dirname(__file__))+'/database.json'