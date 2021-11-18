import logging
import re
from collections import namedtuple
from typing import Generator, List, Optional, Tuple

from dbcat.catalog import Catalog, CatSchema, CatSource, CatTable

LOGGER = logging.getLogger(__name__)

CatalogObject = namedtuple("CatalogObject", ["name", "id"])


class NoMatchesError(Exception):
    """Raise Exception if schema/table/column generators do not find any matches"""

    message = "No columns were scanned. Ensure include/exclude patterns are correct OR no new columns have been added"


def filter_objects(
    include_regex_str: Optional[List[str]],
    exclude_regex_str: Optional[List[str]],
    objects: List[CatalogObject],
) -> List[CatalogObject]:
    if include_regex_str is not None and len(include_regex_str) > 0:
        include_regex = [re.compile(exp, re.IGNORECASE) for exp in include_regex_str]
        matched_set = set()
        for regex in include_regex:
            matched_set |= set(
                list(filter(lambda m: regex.search(m.name) is not None, objects,))
            )

        objects = list(matched_set)

    if exclude_regex_str is not None and len(exclude_regex_str) > 0:
        exclude_regex = [re.compile(exp, re.IGNORECASE) for exp in exclude_regex_str]
        for regex in exclude_regex:
            objects = list(filter(lambda m: regex.search(m.name) is None, objects))

    return objects


def table_generator(
    catalog: Catalog,
    source: CatSource,
    include_schema_regex_str: List[str] = None,
    exclude_schema_regex_str: List[str] = None,
    include_table_regex_str: List[str] = None,
    exclude_table_regex_str: List[str] = None,
) -> Generator[Tuple[CatSchema, CatTable], None, None]:

    schemata = filter_objects(
        include_schema_regex_str,
        exclude_schema_regex_str,
        [
            CatalogObject(s.name, s.id)
            for s in catalog.search_schema(source_like=source.name, schema_like="%")
        ],
    )

    for schema_object in schemata:
        schema = catalog.get_schema_by_id(schema_object.id)
        LOGGER.info("Generating schema %s", schema.name)
        table_objects = filter_objects(
            include_table_regex_str,
            exclude_table_regex_str,
            [
                CatalogObject(t.name, t.id)
                for t in catalog.search_tables(
                    source_like=source.name, schema_like=schema.name, table_like="%"
                )
            ],
        )

        for table_object in table_objects:
            table = catalog.get_table_by_id(table_object.id)
            LOGGER.info("Generating table %s", table.name)
            yield schema, table
