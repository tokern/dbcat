from contextlib import closing
from typing import List, Optional, Tuple

from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm import Session, relationship, sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from dbcat.catalog.metadata import Database
from dbcat.log_mixin import LogMixin

Base: DeclarativeMeta = declarative_base()


class CatDatabase(Base):
    __tablename__ = "databases"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    schemata = relationship("CatSchema", back_populates="database")

    def __init__(self, name: str):
        self.name = name

    @property
    def fqdn(self):
        return self.name

    def __repr__(self):
        return "<Database: {}>".format(self.name)

    def __eq__(self, other):
        return self.fqdn == other.fqdn

    def __hash__(self):
        return hash(self.fqdn)


class CatSchema(Base):
    __tablename__ = "schemata"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    database_id = Column(Integer, ForeignKey("databases.id"))
    database = relationship("CatDatabase", back_populates="schemata", lazy="joined")
    tables = relationship("CatTable", back_populates="schema")

    def __init__(self, name: str, database: CatDatabase):
        self.name = name
        self.database = database

    @property
    def fqdn(self):
        return self.database.name, self.name

    def __repr__(self):
        return "<Database: {}, Schema: {}>".format(self.database.name, self.name)

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
        return self.schema.database.name, self.schema.name, self.name

    def __repr__(self):
        return "<Database: {}, Schema: {}, Table: {}>".format(
            self.schema.database.name, self.schema.name, self.name
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
            self.table.schema.database.name,
            self.table.schema.name,
            self.table.name,
            self.name,
        )

    def __repr__(self):
        return "<Database: {}, Schema: {}, Table: {}, Column: {}>".format(
            self.table.schema.database.name,
            self.table.schema.name,
            self.table.name,
            self.name,
        )

    def __eq__(self, other):
        return self.fqdn == other.fqdn

    def __hash__(self):
        return hash(self.fqdn)


class Catalog(LogMixin):
    def __init__(
        self,
        type: str,
        user: str,
        password: str,
        database: str,
        host: str,
        port: str = None,
    ):
        self.type: str = type
        self.user: str = user
        self.password: str = password
        self.host: str = host
        self.port: int = int(port) if port is not None else 5432
        self.database: str = database
        self._engine: object = None
        self._session_factory = None

    @property
    def engine(self) -> object:
        if self._engine is None:
            self._engine = create_engine(
                "{type}://{user}:{password}@{host}:{port}/{database}".format(
                    type=self.type,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    database=self.database,
                )
            )
        return self._engine

    @property
    def session(self) -> Session:
        if self._session_factory is None:
            Base.metadata.create_all(self.engine)
            self._session_factory = sessionmaker(bind=self.engine)

        # See https://github.com/python/mypy/issues/4805
        return self._session_factory()  # type: ignore

    def close(self):
        if self._engine is not None:
            self._engine.dispose()

    @staticmethod
    def _get_one_or_create(
        session, model, create_method="", create_method_kwargs=None, **kwargs
    ) -> Tuple[Base, bool]:
        try:
            return session.query(model).filter_by(**kwargs).one(), True
        except NoResultFound:
            kwargs.update(create_method_kwargs or {})
            try:
                with session.begin_nested():
                    created = getattr(model, create_method, model)(**kwargs)
                    session.add(created)
                return created, False
            except IntegrityError:
                return session.query(model).filter_by(**kwargs).one(), True

    def get_database(self, database_name: str) -> CatDatabase:
        with closing(self.session) as session:
            return (
                session.query(CatDatabase)
                .filter(CatDatabase.name == database_name)
                .one()
            )

    def get_schema(self, database_name: str, schema_name: str) -> CatSchema:
        with closing(self.session) as session:
            return (
                session.query(CatSchema)
                .join(CatSchema.database)
                .filter(CatDatabase.name == database_name)
                .filter(CatSchema.name == schema_name)
                .one()
            )

    def get_table(
        self, database_name: str, schema_name: str, table_name: str
    ) -> CatTable:
        with closing(self.session) as session:
            return (
                session.query(CatTable)
                .join(CatTable.schema)
                .join(CatSchema.database)
                .filter(CatDatabase.name == database_name)
                .filter(CatSchema.name == schema_name)
                .filter(CatTable.name == table_name)
                .one()
            )

    def get_columns_for_table(
        self, table: CatTable, column_names: List[str] = []
    ) -> List[CatColumn]:
        with closing(self.session) as session:
            stmt = (
                session.query(CatColumn)
                .join(CatColumn.table)
                .join(CatTable.schema)
                .join(CatSchema.database)
                .filter(CatDatabase.name == table.schema.database.name)
                .filter(CatSchema.name == table.schema.name)
                .filter(CatTable.name == table.name)
            )

            if len(column_names) > 0:
                stmt = stmt.filter(CatColumn.name.in_(column_names))

            return stmt.all()

    def get_column(
        self, database_name: str, schema_name: str, table_name: str, column_name: str
    ) -> CatColumn:
        with closing(self.session) as session:
            return (
                session.query(CatColumn)
                .join(CatColumn.table)
                .join(CatTable.schema)
                .join(CatSchema.database)
                .filter(CatDatabase.name == database_name)
                .filter(CatSchema.name == schema_name)
                .filter(CatTable.name == table_name)
                .filter(CatColumn.name == column_name)
                .one()
            )

    def search_database(self, database_like: str) -> List[CatDatabase]:
        with closing(self.session) as session:
            return (
                session.query(CatDatabase)
                .filter(CatDatabase.name.like(database_like))
                .all()
            )

    def search_schema(
        self, schema_like: str, database_like: Optional[str] = None
    ) -> List[CatSchema]:
        with closing(self.session) as session:
            stmt = session.query(CatSchema)
            if database_like is not None:
                stmt = stmt.join(CatSchema.database).filter(
                    CatDatabase.name.like(database_like)
                )
            stmt = stmt.filter(CatSchema.name.like(schema_like))
            self.logger.debug(str(stmt))
            return stmt.all()

    def search_table(
        self,
        table_like: str,
        schema_like: Optional[str] = None,
        database_like: Optional[str] = None,
    ) -> List[CatTable]:
        with closing(self.session) as session:
            stmt = session.query(CatTable)
            if database_like is not None or schema_like is not None:
                stmt = stmt.join(CatTable.schema)
            if database_like is not None:
                stmt = stmt.join(CatSchema.database).filter(
                    CatDatabase.name.like(database_like)
                )
            if schema_like is not None:
                stmt = stmt.filter(CatSchema.name.like(schema_like))

            stmt = stmt.filter(CatTable.name.like(table_like))
            self.logger.debug(str(stmt))
            return stmt.all()

    def search_column(
        self,
        column_like: str,
        table_like: Optional[str] = None,
        schema_like: Optional[str] = None,
        database_like: Optional[str] = None,
    ) -> List[CatColumn]:
        with closing(self.session) as session:
            stmt = session.query(CatColumn)
            if (
                database_like is not None
                or schema_like is not None
                or table_like is not None
            ):
                stmt = stmt.join(CatColumn.table)
            if database_like is not None or schema_like is not None:
                stmt = stmt.join(CatTable.schema)
            if database_like is not None:
                stmt = stmt.join(CatSchema.database).filter(
                    CatDatabase.name.like(database_like)
                )
            if schema_like is not None:
                stmt = stmt.filter(CatSchema.name.like(schema_like))
            if table_like is not None:
                stmt = stmt.filter(CatTable.name.like(table_like))

            stmt = stmt.filter(CatColumn.name.like(column_like))
            self.logger.debug(str(stmt))
            print(str(stmt))
            return stmt.all()

    def save_catalog(self, database: Database) -> None:
        with closing(self.session) as session:
            db, db_found = self._get_one_or_create(
                session, CatDatabase, name=database.name
            )

            for s in database.schemata:
                schema, sch_found = self._get_one_or_create(
                    session, CatSchema, name=s.name, database=db
                )
                for t in s.tables:
                    table, tbl_found = self._get_one_or_create(
                        session, CatTable, name=t.name, schema=schema
                    )
                    index = 0
                    for c in t.columns:
                        cc, col_found = self._get_one_or_create(
                            session=session,
                            model=CatColumn,
                            create_method_kwargs={"type": c.type, "sort_order": index},
                            name=c.name,
                            table=table,
                        )
                        if col_found and cc.type != c.type:
                            cc.type = c.type
                        index += 1
            session.commit()
