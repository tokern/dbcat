from contextlib import closing, nullcontext
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from dbcat.catalog.models import (
    Base,
    CatColumn,
    CatSchema,
    CatSource,
    CatTable,
    ColumnLineage,
)
from dbcat.log_mixin import LogMixin


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
        self._current_session = None

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

    def __enter__(self):
        if self._current_session is not None:
            raise IntegrityError
        self._current_session = self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._current_session is not None:
            self._current_session.close()
            self._current_session = None

    def _get_session_manager(self):
        if self._current_session is not None:
            context_manager = nullcontext(self._current_session)
        else:
            context_manager = closing(self.session)

        return context_manager

    @staticmethod
    def _get_one_or_create(
        session, model, create_method="", create_method_kwargs=None, **kwargs
    ) -> Tuple[Base, bool]:
        try:
            return session.query(model).filter_by(**kwargs).one(), False
        except NoResultFound:
            kwargs.update(create_method_kwargs or {})
            try:
                with session.begin_nested():
                    created = getattr(model, create_method, model)(**kwargs)
                    session.add(created)
                session.commit()
                return created, True
            except IntegrityError:
                return session.query(model).filter_by(**kwargs).one(), False

    def add_source(self, name: str, type: str, **kwargs) -> CatSource:
        obj, created = self._get_one_or_create(
            self._current_session, CatSource, name=name, type=type, **kwargs
        )
        return obj

    def add_schema(self, schema_name: str, source: CatSource) -> CatSchema:
        obj, created = self._get_one_or_create(
            self._current_session, CatSchema, name=schema_name, source=source
        )

        return obj

    def add_table(self, table_name: str, schema: CatSchema) -> CatTable:
        obj, created = self._get_one_or_create(
            self._current_session, CatTable, name=table_name, schema=schema
        )

        return obj

    def add_column(
        self, column_name: str, type: str, sort_order: int, table: CatTable
    ) -> CatColumn:
        obj, created = self._get_one_or_create(
            session=self._current_session,
            model=CatColumn,
            create_method_kwargs={"type": type, "sort_order": sort_order},
            name=column_name,
            table=table,
        )

        return obj

    def add_column_lineage(
        self, source: CatColumn, target: CatColumn, job_id: str, payload: Dict[Any, Any]
    ) -> ColumnLineage:
        column_edge, created = self._get_one_or_create(
            self._current_session,
            ColumnLineage,
            source_id=source.id,
            target_id=target.id,
            job_id=job_id,
            create_method_kwargs={"payload": payload},
        )

        return column_edge

    def get_source(self, source_name: str) -> CatSource:
        with closing(self.session) as session:
            return session.query(CatSource).filter(CatSource.name == source_name).one()

    def get_schema(self, source_name: str, schema_name: str) -> CatSchema:
        with closing(self.session) as session:
            return (
                session.query(CatSchema)
                .join(CatSchema.source)
                .filter(CatSource.name == source_name)
                .filter(CatSchema.name == schema_name)
                .one()
            )

    def get_table(
        self, source_name: str, schema_name: str, table_name: str
    ) -> CatTable:
        with closing(self.session) as session:
            return (
                session.query(CatTable)
                .join(CatTable.schema)
                .join(CatSchema.source)
                .filter(CatSource.name == source_name)
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
                .join(CatSchema.source)
                .filter(CatSource.name == table.schema.source.name)
                .filter(CatSchema.name == table.schema.name)
                .filter(CatTable.name == table.name)
            )

            if len(column_names) > 0:
                stmt = stmt.filter(CatColumn.name.in_(column_names))
            stmt = stmt.order_by(CatColumn.sort_order)
            return stmt.all()

    def get_column(
        self, database_name: str, schema_name: str, table_name: str, column_name: str
    ) -> CatColumn:
        with self._get_session_manager() as session:
            return (
                session.query(CatColumn)
                .join(CatColumn.table)
                .join(CatTable.schema)
                .join(CatSchema.source)
                .filter(CatSource.name == database_name)
                .filter(CatSchema.name == schema_name)
                .filter(CatTable.name == table_name)
                .filter(CatColumn.name == column_name)
                .one()
            )

    def get_column_lineages(self, job_ids=None) -> List[ColumnLineage]:
        with closing(self.session) as session:
            query = session.query(ColumnLineage)
            if job_ids is not None and len(job_ids) > 0:
                self.logger.debug(
                    "Search for lineages from [{}]".format(",".join(list(job_ids)))
                )
                query = query.filter(ColumnLineage.job_id.in_(list(job_ids)))
            else:
                self.logger.debug("No job ids provided. Return all edges")
            return query.all()

    def search_sources(self, source_like: str) -> List[CatSource]:
        with closing(self.session) as session:
            return (
                session.query(CatSource).filter(CatSource.name.like(source_like)).all()
            )

    def search_schema(
        self, schema_like: str, source_like: Optional[str] = None
    ) -> List[CatSchema]:
        with closing(self.session) as session:
            stmt = session.query(CatSchema)
            if source_like is not None:
                stmt = stmt.join(CatSchema.source).filter(
                    CatSource.name.like(source_like)
                )
            stmt = stmt.filter(CatSchema.name.like(schema_like))
            self.logger.debug(str(stmt))
            return stmt.all()

    def search_tables(
        self,
        table_like: str,
        schema_like: Optional[str] = None,
        source_like: Optional[str] = None,
    ) -> List[CatTable]:
        with closing(self.session) as session:
            stmt = session.query(CatTable)
            if source_like is not None or schema_like is not None:
                stmt = stmt.join(CatTable.schema)
            if source_like is not None:
                stmt = stmt.join(CatSchema.source).filter(
                    CatSource.name.like(source_like)
                )
            if schema_like is not None:
                stmt = stmt.filter(CatSchema.name.like(schema_like))

            stmt = stmt.filter(CatTable.name.like(table_like))
            self.logger.debug(str(stmt))
            return stmt.all()

    def search_table(
        self,
        table_like: str,
        schema_like: Optional[str] = None,
        source_like: Optional[str] = None,
    ) -> CatTable:
        tables = self.search_tables(
            table_like=table_like, schema_like=schema_like, source_like=source_like
        )
        if len(tables) == 0:
            raise RuntimeError("'{}' table not found".format(table_like))
        elif len(tables) > 1:
            raise RuntimeError("Ambiguous table name. Multiple matches found")

        return tables[0]

    def search_column(
        self,
        column_like: str,
        table_like: Optional[str] = None,
        schema_like: Optional[str] = None,
        source_like: Optional[str] = None,
    ) -> List[CatColumn]:
        with closing(self.session) as session:
            stmt = session.query(CatColumn)
            if (
                source_like is not None
                or schema_like is not None
                or table_like is not None
            ):
                stmt = stmt.join(CatColumn.table)
            if source_like is not None or schema_like is not None:
                stmt = stmt.join(CatTable.schema)
            if source_like is not None:
                stmt = stmt.join(CatSchema.source).filter(
                    CatSource.name.like(source_like)
                )
            if schema_like is not None:
                stmt = stmt.filter(CatSchema.name.like(schema_like))
            if table_like is not None:
                stmt = stmt.filter(CatTable.name.like(table_like))

            stmt = stmt.filter(CatColumn.name.like(column_like))
            self.logger.debug(str(stmt))
            print(str(stmt))
            return stmt.all()
