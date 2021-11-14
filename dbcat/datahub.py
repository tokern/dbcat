import logging
import re
from contextlib import closing
from pathlib import Path
from typing import Iterable, List, Optional, Pattern, Type

import typer
from datahub.configuration import ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.sql.sql_common import (
    SQLSourceReport,
    SqlWorkUnit,
    get_schema_metadata,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass

from dbcat.api import open_catalog
from dbcat.generators import table_generator

LOGGER = logging.getLogger(__name__)


class CatalogConfig(ConfigModel):
    path: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None

    source_names: Optional[List[str]] = None
    include_schema_regex: Optional[List[str]] = None
    exclude_schema_regex: Optional[List[str]] = None
    include_table_regex: Optional[List[str]] = None
    exclude_table_regex: Optional[List[str]] = None

    include_source_name: bool = False
    env: str = DEFAULT_ENV


class CatalogSource(Source):
    int_pattern: Pattern = re.compile(".*int.*", flags=re.IGNORECASE)
    text_pattern: Pattern = re.compile(".*[char|text].*", flags=re.IGNORECASE)
    byte_pattern: Pattern = re.compile(".*binary.*", flags=re.IGNORECASE)
    date_pattern: Pattern = re.compile("date", flags=re.IGNORECASE)
    time_pattern: Pattern = re.compile("time", flags=re.IGNORECASE)
    timestamp_pattern: Pattern = re.compile(".*timestamp.*", flags=re.IGNORECASE)

    @staticmethod
    def get_column_type(data_type: str) -> SchemaFieldDataType:
        type_class: Type = NullTypeClass
        if CatalogSource.int_pattern.match(data_type) is not None:
            type_class = NumberTypeClass
        elif CatalogSource.text_pattern.match(data_type) is not None:
            type_class = StringTypeClass
        elif CatalogSource.byte_pattern.match(data_type) is not None:
            type_class = BytesTypeClass
        elif CatalogSource.date_pattern.match(data_type) is not None:
            type_class = DateTypeClass
        elif CatalogSource.time_pattern.match(data_type) is not None:
            type_class = TimeTypeClass
        elif CatalogSource.timestamp_pattern.match(data_type) is not None:
            type_class = TimeTypeClass

        return SchemaFieldDataType(type=type_class())

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        config = CatalogConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def __init__(self, config: CatalogConfig, ctx: PipelineContext):
        super(CatalogSource, self).__init__(ctx)
        self.config = config
        self.report = SQLSourceReport()

    def get_workunits(self) -> Iterable[WorkUnit]:
        catalog = open_catalog(
            app_dir=Path(typer.get_app_dir("tokern")),
            path=self.config.path,
            user=self.config.user,
            password=self.config.password,
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
        )

        with closing(catalog) as catalog:
            with catalog.managed_session:
                if (
                    self.config.source_names is not None
                    and len(self.config.source_names) > 0
                ):
                    sources = [
                        catalog.get_source(source_name)
                        for source_name in self.config.source_names
                    ]
                else:
                    sources = catalog.get_sources()

                for source in sources:
                    for schema, table in table_generator(
                        catalog=catalog,
                        source=source,
                        include_schema_regex_str=self.config.include_schema_regex,
                        exclude_schema_regex_str=self.config.exclude_schema_regex,
                        include_table_regex_str=self.config.include_table_regex,
                        exclude_table_regex_str=self.config.exclude_table_regex,
                    ):
                        if self.config.include_source_name:
                            dataset_name = f"{source.name}.{schema.name}.{table.name}"
                        else:
                            dataset_name = f"{schema.name}.{table.name}"
                        self.report.report_entity_scanned(dataset_name)

                        dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{source.source_type},{dataset_name},{self.config.env})"
                        dataset_snapshot = DatasetSnapshot(urn=dataset_urn, aspects=[],)

                        schema_fields = []
                        for column in catalog.get_columns_for_table(table):
                            global_tags: Optional[GlobalTagsClass] = None
                            if column.pii_type is not None:
                                global_tags = GlobalTagsClass(
                                    tags=[
                                        TagAssociationClass("urn:li:tag:pii"),
                                        TagAssociationClass(
                                            f"urn.li.tag.{column.pii_type.name}"
                                        ),
                                    ]
                                )

                            schema_fields.append(
                                SchemaField(
                                    fieldPath=column.name,
                                    type=CatalogSource.get_column_type(
                                        column.data_type
                                    ),
                                    nativeDataType=column.data_type,
                                    description=None,
                                    nullable=True,
                                    recursive=False,
                                    globalTags=global_tags,
                                )
                            )

                        schema_metadata = get_schema_metadata(
                            sql_report=self.report,
                            dataset_name=dataset_name,
                            platform=source.source_type,
                            columns=[],
                            canonical_schema=schema_fields,
                        )
                        dataset_snapshot.aspects.append(schema_metadata)

                        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                        wu = SqlWorkUnit(id=dataset_name, mce=mce)
                        self.report.report_workunit(wu)
                        yield wu

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass
