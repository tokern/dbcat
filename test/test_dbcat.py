import pytest

from dbcat.dbcat import render
from dbcat.scanners.json import File


@pytest.mark.skip(reason="no way of currently testing this")
def test_render():
    scanner = File("test", "test/catalog.json")
    catalog = scanner.scan()
    render([catalog])
