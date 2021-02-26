from contextlib import closing

from dbcat.catalog.orm import ColumnLineage


def test_add_edge(save_catalog):
    database, catalog = save_catalog
    expected_edges = [
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "redirect_id"),
        ),
        (
            ("test", "default", "page", "page_id"),
            ("test", "default", "page_lookup_nonredirect", "page_id"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_nonredirect", "redirect_title"),
        ),
        (
            ("test", "default", "page", "page_title"),
            ("test", "default", "page_lookup_nonredirect", "true_title"),
        ),
        (
            ("test", "default", "page", "page_latest"),
            ("test", "default", "page_lookup_nonredirect", "page_version"),
        ),
    ]

    for edge in expected_edges:
        source = catalog.get_column(
            database_name=edge[0][0],
            schema_name=edge[0][1],
            table_name=edge[0][2],
            column_name=edge[0][3],
        )

        target = catalog.get_column(
            database_name=edge[1][0],
            schema_name=edge[1][1],
            table_name=edge[1][2],
            column_name=edge[1][3],
        )

        catalog.add_column_lineage(source, target, {})

    with closing(catalog.session) as session:
        all_edges = session.query(ColumnLineage).all()
        assert set([(e.source.fqdn, e.target.fqdn) for e in all_edges]) == set(
            expected_edges
        )
