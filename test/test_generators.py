from dbcat.generators import CatalogObject, filter_objects


def test_simple_schema_include(load_source):
    catalog, conf, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = filter_objects(
        include_regex_str=[schema_objects[0].name],
        exclude_regex_str=None,
        objects=schema_objects,
    )
    assert len(filtered) == 1


def test_simple_schema_exclude(load_source):
    catalog, conf, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = filter_objects(
        exclude_regex_str=[schema_objects[0].name],
        include_regex_str=None,
        objects=schema_objects,
    )
    assert len(filtered) == 0


def test_simple_schema_include_exclude(load_source):
    catalog, conf, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = filter_objects(
        include_regex_str=[schema_objects[0].name],
        exclude_regex_str=[schema_objects[0].name],
        objects=schema_objects,
    )
    assert len(filtered) == 0


def test_regex_schema_include(load_source):
    catalog, conf, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = filter_objects(
        include_regex_str=[".*"], exclude_regex_str=None, objects=schema_objects
    )
    assert len(filtered) == 1


def test_regex_schema_exclude(load_source):
    catalog, conf, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = filter_objects(
        exclude_regex_str=[".*"], include_regex_str=None, objects=schema_objects
    )
    assert len(filtered) == 0


def test_regex_failed_schema_include(load_source):
    catalog, conf, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = filter_objects(
        include_regex_str=["fail.*"], exclude_regex_str=None, objects=schema_objects
    )
    assert len(filtered) == 0


def test_regex_failed_schema_exclude(load_source):
    catalog, conf, source = load_source
    schema_objects = [
        CatalogObject(s.name, s.id)
        for s in catalog.search_schema(source_like=source.name, schema_like="%")
    ]

    filtered = filter_objects(
        exclude_regex_str=["fail.*"], include_regex_str=None, objects=schema_objects
    )
    assert len(filtered) == 1


def test_regex_success_table_include(load_source):
    catalog, conf, source = load_source
    table_objects = [
        CatalogObject(t.name, t.id)
        for t in catalog.search_tables(
            source_like=source.name, schema_like="%", table_like="%"
        )
    ]

    filtered = filter_objects(
        include_regex_str=["full.*", "partial.*"],
        exclude_regex_str=None,
        objects=table_objects,
    )
    assert len(filtered) == 2
    assert sorted([c.name for c in filtered]) == [
        "full_pii",
        "partial_pii",
    ]


def test_regex_success_table_exclude(load_source):
    catalog, conf, source = load_source
    table_objects = [
        CatalogObject(t.name, t.id)
        for t in catalog.search_tables(
            source_like=source.name, schema_like="%", table_like="%"
        )
    ]

    filtered = filter_objects(
        exclude_regex_str=["full.*", "partial.*"],
        include_regex_str=None,
        objects=table_objects,
    )
    assert len(filtered) == 1
    assert [c.name for c in filtered] == ["no_pii"]


def test_regex_success_table_include_exclude(load_source):
    catalog, conf, source = load_source
    table_objects = [
        CatalogObject(t.name, t.id)
        for t in catalog.search_tables(
            source_like=source.name, schema_like="%", table_like="%"
        )
    ]

    filtered = filter_objects(
        include_regex_str=["full.*", "partial.*"],
        exclude_regex_str=["full.*"],
        objects=table_objects,
    )
    assert len(filtered) == 1
    assert sorted([c.name for c in filtered]) == ["partial_pii"]
