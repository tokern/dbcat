import pytest

from dbcat.catalog.scanners import FileScanner
from dbcat.dbcat import render


@pytest.mark.skip(reason="no way of currently testing this")
def test_render():
    scanner = FileScanner("test", "test/catalog.json")
    catalog = scanner.scan()
    render([catalog])
