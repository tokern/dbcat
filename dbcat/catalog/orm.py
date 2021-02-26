from typing import Any, Dict, Optional

from snowflake.sqlalchemy import URL
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm import relationship

Base: DeclarativeMeta = declarative_base()


class CatSource(Base):
    __tablename__ = "sources"

    id = Column(Integer, primary_key=True)
    type = Column(String)
    name = Column(String)
    dialect = Column(String)
    uri = Column(String)
    port = Column(String)
    username = Column(String)
    password = Column(String)
    database = Column(String)
    instance = Column(String)
    cluster = Column(String)
    project_id = Column(String)
    project_credentials = Column(String)
    page_size = Column(String)
    filter_key = Column(String)
    included_tables_regex = Column(String)
    key_path = Column(String)
    account = Column(String)
    role = Column(String)
    warehouse = Column(String)
    schemata = relationship("CatSchema", back_populates="source")

    def __init__(
        self,
        type: str,
        name: str,
        dialect: Optional[str] = None,
        uri: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        instance: Optional[str] = None,
        cluster: Optional[str] = None,
        project_id: Optional[str] = None,
        project_credentials: Optional[str] = None,
        page_size: Optional[str] = None,
        filter_key: Optional[str] = None,
        included_tables_regex: Optional[str] = None,
        key_path: Optional[str] = None,
        account: Optional[str] = None,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        **kwargs,
    ):
        self.uri = uri
        self.port = port
        if type is not None:
            type = type.lower()
        self.type = type
        self.dialect = dialect
        self.username = username
        self.password = password
        self.name = name
        self.database = database
        self.instance = instance
        self.cluster = cluster
        self.project_id = project_id
        self.project_credentials = project_credentials
        self.page_size = page_size
        self.filter_key = filter_key
        self.included_tables_regex = included_tables_regex
        self.key_path = key_path
        self.role = role
        self.account = account
        self.warehouse = warehouse

    @property
    def conn_string(self):
        if self.type == "bigquery":
            project_id = self.project_id
            conn_string = f"bigquery://{project_id}"
        elif self.type == "snowflake":
            conn_string = URL(
                account=self.account,
                user=self.username,
                password=self.password,
                database=self.database,
                warehouse=self.warehouse,
                role=self.role,
            )
        else:
            username_password_placeholder = (
                f"{self.username}:{self.password}" if self.password is not None else ""
            )

            if self.type in ["redshift"]:
                self.dialect = "postgres"
            elif self.type == "mysql":
                self.dialect = "mysql+pymysql"
            else:
                self.dialect = self.type
            uri_port_placeholder = (
                f"{self.uri}:{self.port}" if self.port is not None else f"{self.uri}"
            )

            database = self.database or ""

            conn_string = f"{self.dialect}://{username_password_placeholder}@{uri_port_placeholder}/{database}"

        return conn_string

    @property
    def fqdn(self):
        return self.name

    def __repr__(self):
        return "<Source: {}>".format(self.name)

    def __eq__(self, other):
        return self.fqdn == other.fqdn

    def __hash__(self):
        return hash(self.fqdn)


class CatSchema(Base):
    __tablename__ = "schemata"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    source_id = Column(Integer, ForeignKey("sources.id"))
    source = relationship("CatSource", back_populates="schemata", lazy="joined")
    tables = relationship("CatTable", back_populates="schema")

    def __init__(self, name: str, source: CatSource):
        self.name = name
        self.source = source

    @property
    def fqdn(self):
        return self.source.name, self.name

    def __repr__(self):
        return "<Database: {}, Schema: {}>".format(self.source.name, self.name)

    def __eq__(self, other):
        return self.fqdn == other.fqdn

    def __hash__(self):
        return hash(self.fqdn)


class CatTable(Base):
    __tablename__ = "tables"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    schema_id = Column(Integer, ForeignKey("schemata.id"))
    schema = relationship("CatSchema", back_populates="tables", lazy="joined")
    columns = relationship(
        "CatColumn", back_populates="table", order_by="CatColumn.sort_order"
    )

    def __init__(self, name: str, schema: CatSchema):
        self.name = name
        self.schema = schema

    @property
    def fqdn(self):
        return self.schema.source.name, self.schema.name, self.name

    def __repr__(self):
        return "<Source: {}, Schema: {}, Table: {}>".format(
            self.schema.source.name, self.schema.name, self.name
        )

    def __eq__(self, other):
        return self.fqdn == other.fqdn

    def __hash__(self):
        return hash(self.fqdn)


class CatColumn(Base):
    __tablename__ = "columns"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)
    sort_order = Column(Integer)
    table_id = Column(Integer, ForeignKey("tables.id"))
    table = relationship("CatTable", back_populates="columns", lazy="joined")

    def __init__(self, name: str, type: str, sort_order: int, table: CatTable):
        self.name = name
        self.type = type
        self.sort_order = sort_order
        self.table = table

    @property
    def fqdn(self):
        return (
            self.table.schema.source.name,
            self.table.schema.name,
            self.table.name,
            self.name,
        )

    def __repr__(self):
        return "<Source: {}, Schema: {}, Table: {}, Column: {}>".format(
            self.table.schema.source.name,
            self.table.schema.name,
            self.table.name,
            self.name,
        )

    def __eq__(self, other):
        return self.fqdn == other.fqdn

    def __lt__(self, other) -> bool:
        for s, o in zip(
            [
                self.table.schema.source.name,
                self.table.schema.name,
                self.table.name,
                self.sort_order,
            ],
            [
                other.table.schema.database.name,
                other.table.schema.name,
                other.table.name,
                other.sort_order,
            ],
        ):
            if s < o:
                return True
            elif s > o:
                return False

        return False

    def __hash__(self):
        return hash(self.fqdn)


class ColumnLineage(Base):
    __tablename__ = "column_lineage"

    id = Column(Integer, primary_key=True)
    payload = Column(JSONB)

    source_id = Column(Integer, ForeignKey("columns.id"))
    target_id = Column(Integer, ForeignKey("columns.id"))
    source = relationship("CatColumn", foreign_keys=source_id, lazy="joined")
    target = relationship("CatColumn", foreign_keys=target_id, lazy="joined")

    def __init__(self, source: CatColumn, target: CatColumn, payload: Dict[Any, Any]):
        self.source = source
        self.target = target
        self.payload = payload

    def __repr__(self):
        return "<Edge: {} -{}-> {}>".format(self.source, self.target, self.payload)
