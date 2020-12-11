from abc import ABC, abstractmethod

from dbcat.catalog.metadata import Column, Database, Schema, Table


class Scanner(ABC):
    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

    @abstractmethod
    def scan(self):
        pass

    @staticmethod
    def create(config):
        scanners = []
        for db in config["databases"]:
            if db["type"] == "file":
                scanners.append(FileScanner(db["name"], db["path"]))

        return scanners


class FileScanner(Scanner):
    class Database(Database):
        def __init__(self, name, schemata):
            super(FileScanner.Database, self).__init__(name, None)

            for schema in schemata:
                self._children.append(FileScanner.Schema(parent=self, **schema))

    class Schema(Schema):
        def __init__(self, name, parent, tables):
            super(FileScanner.Schema, self).__init__(name, parent)

            for table in tables:
                self._children.append(FileScanner.Table(parent=self, **table))

    class Table(Table):
        def __init__(self, name, parent, columns):
            super(FileScanner.Table, self).__init__(name, parent)

            for column in columns:
                self._children.append(FileScanner.Column(parent=self, **column))

    class Column(Column):
        def __init__(self, parent, name, type):
            super(FileScanner.Column, self).__init__(name, parent, type)

    def __init__(self, name, path):
        super(FileScanner, self).__init__(name)
        self._path = path

    @property
    def path(self):
        return self._path

    def scan(self):
        import json

        with open(self.path, "r") as file:
            content = json.load(file)

        return FileScanner.Database(**content)
