from dbcat.catalog.scanners import FileScanner


def test_file_scanner():
    scanner = FileScanner("test", "test/catalog.json")
    catalog = scanner.scan()
    assert catalog.name == "test"
    assert len(catalog.schemata) == 1

    default_schema = catalog.schemata[0]
    assert default_schema.name == "default"
    assert len(default_schema.tables) == 8
