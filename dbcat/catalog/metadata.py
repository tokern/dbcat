import re
from abc import ABC
from typing import List, Optional

from dbcat.log_mixin import LogMixin


class Connection(object):
    def __init__(
        self,
        metadata_source: str,
        dialect: Optional[str] = None,
        uri: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        name: Optional[str] = None,
        database: Optional[str] = None,
        instance: Optional[str] = None,
        cluster: Optional[str] = None,
        is_location_parsing_enabled: bool = False,
        included_schemas: List = [],
        excluded_schemas: List = [],
        included_keys: Optional[List[str]] = None,
        excluded_keys: Optional[List[str]] = None,
        included_key_regex: Optional[str] = None,
        excluded_key_regex: Optional[str] = None,
        included_tables_regex: Optional[str] = None,
        build_script_path: Optional[str] = None,
        venv_path: Optional[str] = None,
        python_binary: Optional[str] = None,
        key_path: Optional[str] = None,
        project_id: Optional[str] = None,
        project_credentials: Optional[str] = None,
        page_size: Optional[str] = None,
        filter_key: Optional[str] = None,
        where_clause_suffix: Optional[str] = "",
        **kwargs,
    ):

        self.uri = uri
        self.port = port
        if metadata_source is not None:
            metadata_source = metadata_source.lower()
        self.metadata_source = metadata_source
        self.dialect = dialect
        self.username = username
        self.password = password
        self.name = name
        self.database = database
        self.instance = instance
        self.cluster = cluster
        self.is_location_parsing_enabled = is_location_parsing_enabled
        self.included_schemas = included_schemas
        self.excluded_schemas = excluded_schemas
        self.included_keys = included_keys
        self.excluded_keys = included_keys
        self.included_key_regex = included_key_regex
        self.excluded_key_regex = excluded_key_regex
        self.included_tables_regex = included_tables_regex
        self.build_script_path = build_script_path
        self.venv_path = venv_path
        self.python_binary = python_binary
        self.key_path = key_path
        self.project_id = project_id
        self.key_path = key_path
        self.project_credentials = project_credentials
        self.page_size = page_size
        self.filter_key = filter_key
        self.where_clause_suffix = where_clause_suffix
        self.conn_string = self.infer_conn_string()

    def infer_conn_string(self):
        if self.metadata_source == "bigquery":
            project_id = self.project_id
            conn_string = f"bigquery://{project_id}"
        elif self.metadata_source == "neo4j":
            conn_string = f"bolt://{self.uri}:{self.port}"
        else:
            username_password_placeholder = (
                f"{self.username}:{self.password}" if self.password is not None else ""
            )

            if self.metadata_source in ["redshift"]:
                self.dialect = "postgres"
            elif self.metadata_source == "hivemetastore":
                self.dialect = self.dialect
            elif self.metadata_source == "mysql":
                self.dialect = "mysql+pymysql"
            else:
                self.dialect = self.metadata_source
            uri_port_placeholder = (
                f"{self.uri}:{self.port}" if self.port is not None else f"{self.uri}"
            )

            database = self.database or ""

            conn_string = f"{self.dialect}://{username_password_placeholder}@{uri_port_placeholder}/{database}"

        return conn_string


class NamedObject(ABC, LogMixin):
    def __init__(self, name, parent):
        self._name = name
        self._parent = parent
        self._children = []
        self._include_regex = ()
        self._exclude_regex = ()
        self.logger.debug(
            "Name: %s", name,
        )

    @property
    def name(self):
        return self._name

    @property
    def parent(self):
        return self._parent

    def get_children(self):
        matches = self._children
        if len(self._include_regex) > 0:
            matched_set = set()
            for regex in self._include_regex:
                matched_set |= set(
                    list(
                        filter(
                            lambda m: regex.search(m.get_name()) is not None,
                            self._children,
                        )
                    )
                )

            matches = list(matched_set)

        for regex in self._exclude_regex:
            matches = list(
                filter(lambda m: regex.search(m.get_name()) is None, matches)
            )

        return matches

    def add_child(self, child):
        self._children.append(child)

    def set_include_regex(self, include):
        self._include_regex = [re.compile(exp, re.IGNORECASE) for exp in include]

    def set_exclude_regex(self, exclude):
        self._exclude_regex = [re.compile(exp, re.IGNORECASE) for exp in exclude]


class Catalog(NamedObject):
    def __init__(self, name, parent):
        super(Catalog, self).__init__(name, parent)

    @property
    def schemata(self):
        return self._children


class Schema(NamedObject):
    def __init__(self, name, parent):
        super(Schema, self).__init__(name, parent)

    @property
    def tables(self):
        return self._children

    def __repr__(self):
        return "<Schema: {}, Catalog: {}, # Tables: {}>".format(
            self.name, self.parent.name, len(self._children)
        )


class Table(NamedObject):
    def __init__(self, name, parent):
        super(Table, self).__init__(name, parent)

    @property
    def columns(self):
        return self._children

    def __repr__(self):
        return "<Table: {}, Schema: {}, # Columns: {}>".format(
            self.name, self.parent.name, len(self._children)
        )


class Column(NamedObject):
    def __init__(self, name, parent, type):
        super(Column, self).__init__(name, parent)
        self._type = type

    @property
    def type(self):
        return self._type
