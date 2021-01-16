from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm import Session, relationship, sessionmaker

Base: DeclarativeMeta = declarative_base()


class Connection:
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


class CatDatabase(Base):
    __tablename__ = "databases"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    schemata = relationship("CatSchema", back_populates="database")

    def __init__(self, name: str):
        self.name = name


class CatSchema(Base):
    __tablename__ = "schemata"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    database_id = Column(Integer, ForeignKey("databases.id"))
    database = relationship("CatDatabase", back_populates="schemata")
    tables = relationship("CatTable", back_populates="schema")

    def __init__(self, name: str, database: CatDatabase):
        self.name = name
        self.database = database


class CatTable(Base):
    __tablename__ = "tables"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    schema_id = Column(Integer, ForeignKey("schemata.id"))
    schema = relationship("CatSchema", back_populates="tables")
    columns = relationship(
        "CatColumn", back_populates="table", order_by="CatColumn.sort_order"
    )

    def __init__(self, name: str, schema: CatSchema):
        self.name = name
        self.schema = schema


class CatColumn(Base):
    __tablename__ = "columns"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)
    sort_order = Column(Integer)
    table_id = Column(Integer, ForeignKey("tables.id"))
    table = relationship("CatTable", back_populates="columns")

    def __init__(self, name: str, type: str, sort_order: int, table: CatTable):
        self.name = name
        self.type = type
        self.sort_order = sort_order
        self.table = table
