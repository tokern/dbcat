import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, desc, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Query, Session, scoped_session, sessionmaker

from dbcat.catalog.models import (
    Base,
    CatColumn,
    CatSchema,
    CatSource,
    CatTable,
    ColumnLineage,
    DefaultSchema,
    Job,
    JobExecution,
    JobExecutionStatus,
)
from dbcat.log_mixin import LogMixin


class Catalog(LogMixin):
    def __init__(
        self,
        user: str,
        password: str,
        database: str,
        host: str,
        port: str = None,
        **kwargs
    ):
        self.user: str = user
        self.password: str = password
        self.host: str = host
        self.port: int = int(port) if port is not None else 5432
        self.database: str = database
        self._engine: object = None
        self._scoped_session = None
        self._connection_args = kwargs

    @property
    def engine(self) -> object:
        if self._engine is None:
            self._engine = create_engine(
                "postgresql://{user}:{password}@{host}:{port}/{database}".format(
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    database=self.database,
                ),
                **self._connection_args
            )
        return self._engine

    @property
    def scoped_session(self) -> scoped_session:
        if self._scoped_session is None:
            self._scoped_session = scoped_session(sessionmaker(bind=self.engine))

        return self._scoped_session

    def close(self):
        if self._scoped_session is not None:
            self._scoped_session.remove()

        if self._engine is not None:
            self._engine.dispose()

    def _create(
        self, model, create_method="", create_method_kwargs=None, **kwargs
    ) -> Base:
        session = self.scoped_session

        try:
            if create_method_kwargs is None:
                create_method_kwargs = {}
            create_method_kwargs.update(kwargs or {})
            created = getattr(model, create_method, model)(**create_method_kwargs)
            session.add(created)
            session.commit()
            return created
        except IntegrityError:
            session.rollback()
            return session.query(model).filter_by(**kwargs).one()
        finally:
            session.rollback()

    def add_source(self, name: str, source_type: str, **kwargs) -> CatSource:
        return self._create(CatSource, name=name, source_type=source_type, **kwargs)

    def add_schema(self, schema_name: str, source: CatSource) -> CatSchema:
        return self._create(CatSchema, name=schema_name, source=source)

    def add_table(self, table_name: str, schema: CatSchema) -> CatTable:
        return self._create(CatTable, name=table_name, schema=schema)

    def add_column(
        self, column_name: str, data_type: str, sort_order: int, table: CatTable
    ) -> CatColumn:
        return self._create(
            model=CatColumn,
            create_method_kwargs={"data_type": data_type, "sort_order": sort_order},
            name=column_name,
            table=table,
        )

    def add_job(self, name: str, source: CatSource, context: Dict[Any, Any]) -> Job:
        return self._create(
            model=Job,
            name=name,
            source=source,
            create_method_kwargs={"context": context},
        )

    def add_job_execution(
        self,
        job: Job,
        started_at: datetime.datetime,
        ended_at: datetime.datetime,
        status: JobExecutionStatus,
    ) -> JobExecution:
        return self._create(
            model=JobExecution,
            job_id=job.id,
            started_at=started_at,
            ended_at=ended_at,
            status=status,
        )

    def add_column_lineage(
        self,
        source: CatColumn,
        target: CatColumn,
        job_execution_id: int,
        context: Dict[Any, Any],
    ) -> ColumnLineage:
        return self._create(
            ColumnLineage,
            source_id=source.id,
            target_id=target.id,
            job_execution_id=job_execution_id,
            create_method_kwargs={"context": context},
        )

    def get_source(self, source_name: str) -> CatSource:
        return (
            self.scoped_session.query(CatSource)
            .filter(CatSource.name == source_name)
            .one()
        )

    def get_schema(self, source_name: str, schema_name: str) -> CatSchema:
        return (
            self.scoped_session.query(CatSchema)
            .join(CatSchema.source)
            .filter(CatSource.name == source_name)
            .filter(CatSchema.name == schema_name)
            .one()
        )

    def get_table(
        self, source_name: str, schema_name: str, table_name: str
    ) -> CatTable:
        return (
            self.scoped_session.query(CatTable)
            .join(CatTable.schema)
            .join(CatSchema.source)
            .filter(CatSource.name == source_name)
            .filter(CatSchema.name == schema_name)
            .filter(CatTable.name == table_name)
            .one()
        )

    def get_columns_for_table(
        self, table: CatTable, column_names: List[str] = None
    ) -> List[CatColumn]:
        stmt = (
            self.scoped_session.query(CatColumn)
            .join(CatColumn.table)
            .join(CatTable.schema)
            .join(CatSchema.source)
            .filter(CatSource.name == table.schema.source.name)
            .filter(CatSchema.name == table.schema.name)
            .filter(CatTable.name == table.name)
        )

        if column_names is not None:
            stmt = stmt.filter(CatColumn.name.in_(column_names))
        stmt = stmt.order_by(CatColumn.sort_order)
        return stmt.all()

    def get_column(
        self, database_name: str, schema_name: str, table_name: str, column_name: str
    ) -> CatColumn:
        return (
            self.scoped_session.query(CatColumn)
            .join(CatColumn.table)
            .join(CatTable.schema)
            .join(CatSchema.source)
            .filter(CatSource.name == database_name)
            .filter(CatSchema.name == schema_name)
            .filter(CatTable.name == table_name)
            .filter(CatColumn.name == column_name)
            .one()
        )

    def get_job(self, name: str) -> Job:
        return self.scoped_session.query(Job).filter(Job.name == name).one()

    def get_job_executions(self, job: Job) -> List[JobExecution]:
        return (
            self.scoped_session.query(JobExecution)
            .filter(JobExecution.job_id == job.id)
            .order_by(JobExecution.started_at.asc())
            .all()
        )

    def get_job_execution(self, job_execution_id: int) -> JobExecution:
        return (
            self.scoped_session.query(JobExecution)
            .filter(JobExecution.id == job_execution_id)
            .one()
        )

    def get_source_by_id(self, source_id: int) -> CatSource:
        return (
            self.scoped_session.query(CatSource).filter(CatSource.id == source_id).one()
        )

    def get_schema_by_id(self, schema_id: int) -> CatSchema:
        return (
            self.scoped_session.query(CatSchema).filter(CatSchema.id == schema_id).one()
        )

    def get_table_by_id(self, table_id: int) -> CatTable:
        return self.scoped_session.query(CatTable).filter(CatTable.id == table_id).one()

    def get_column_by_id(self, column_id: int) -> CatColumn:
        return (
            self.scoped_session.query(CatColumn).filter(CatColumn.id == column_id).one()
        )

    def get_job_by_id(self, job_id: int) -> Job:
        return self.scoped_session.query(Job).filter(Job.id == job_id).one()

    @staticmethod
    def _get_latest_job_executions(session: Session, job_ids: List[int]) -> Query:
        row_number_column = (
            func.row_number()
            .over(
                partition_by=JobExecution.job_id, order_by=desc(JobExecution.started_at)
            )
            .label("row_number")
        )
        query = (
            session.query(JobExecution.id)
            .filter(JobExecution.job_id.in_(job_ids))
            .add_columns(row_number_column)
            .from_self()
            .filter(row_number_column == 1)
        )
        query = query.from_self(JobExecution.id)
        print(query)
        return query

    def get_latest_job_executions(self, job_ids: List[int]) -> List[JobExecution]:
        return (
            self.scoped_session.query(JobExecution)
            .filter(
                JobExecution.id.in_(
                    self._get_latest_job_executions(
                        self.scoped_session(), job_ids
                    ).subquery()
                )
            )
            .all()
        )

    def get_column_lineages(self, job_ids: List[int] = None) -> List[ColumnLineage]:
        query = self.scoped_session.query(ColumnLineage)
        if job_ids is not None and len(job_ids) > 0:
            self.logger.debug(
                "Search for lineages from [{}]".format(
                    ",".join(str(v) for v in job_ids)
                )
            )
            query = query.filter(
                ColumnLineage.job_execution_id.in_(
                    self._get_latest_job_executions(
                        self.scoped_session(), job_ids
                    ).subquery()
                )
            )
        else:
            self.logger.debug("No job ids provided. Return all edges")
        return query.all()

    def get_sources(self) -> List[CatSource]:
        return self.scoped_session.query(CatSource).all()

    def search_sources(self, source_like: str) -> List[CatSource]:
        return (
            self.scoped_session.query(CatSource)
            .filter(CatSource.name.like(source_like))
            .all()
        )

    def search_schema(
        self, schema_like: str, source_like: Optional[str] = None
    ) -> List[CatSchema]:
        stmt = self.scoped_session.query(CatSchema)
        if source_like is not None:
            stmt = stmt.join(CatSchema.source).filter(CatSource.name.like(source_like))
        stmt = stmt.filter(CatSchema.name.like(schema_like))
        self.logger.debug(str(stmt))
        return stmt.all()

    def search_tables(
        self,
        table_like: str,
        schema_like: Optional[str] = None,
        source_like: Optional[str] = None,
    ) -> List[CatTable]:
        stmt = self.scoped_session.query(CatTable)
        if source_like is not None or schema_like is not None:
            stmt = stmt.join(CatTable.schema)
        if source_like is not None:
            stmt = stmt.join(CatSchema.source).filter(CatSource.name.like(source_like))
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
        stmt = self.scoped_session.query(CatColumn)
        if source_like is not None or schema_like is not None or table_like is not None:
            stmt = stmt.join(CatColumn.table)
        if source_like is not None or schema_like is not None:
            stmt = stmt.join(CatTable.schema)
        if source_like is not None:
            stmt = stmt.join(CatSchema.source).filter(CatSource.name.like(source_like))
        if schema_like is not None:
            stmt = stmt.filter(CatSchema.name.like(schema_like))
        if table_like is not None:
            stmt = stmt.filter(CatTable.name.like(table_like))

        stmt = stmt.filter(CatColumn.name.like(column_like))
        self.logger.debug(str(stmt))
        return stmt.all()

    def update_source(
        self, source: CatSource, default_schema: CatSchema
    ) -> DefaultSchema:
        return self._create(
            DefaultSchema, source_id=source.id, schema_id=default_schema.id
        )
