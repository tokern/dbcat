from dbcat.catalog.metadata import Column as SuperCol
from dbcat.catalog.metadata import Database as SuperCat
from dbcat.catalog.metadata import Schema as SuperSch
from dbcat.catalog.metadata import Table as SuperTab
from dbcat.scanners import Scanner


class File(Scanner):
    class Database(SuperCat):
        def __init__(self, name, schemata):
            super(File.Database, self).__init__(name, None)

            for schema in schemata:
                self._children.append(File.Schema(parent=self, **schema))

    class Schema(SuperSch):
        def __init__(self, name, parent, tables):
            super(File.Schema, self).__init__(name, parent)

            for table in tables:
                self._children.append(File.Table(parent=self, **table))

    class Table(SuperTab):
        def __init__(self, name, parent, columns):
            super(File.Table, self).__init__(name, parent)

            for column in columns:
                self._children.append(File.Column(parent=self, **column))

    class Column(SuperCol):
        def __init__(self, parent, name, type):
            super(File.Column, self).__init__(name, parent, type)

    def __init__(self, name, path):
        super(File, self).__init__(name)
        self._path = path

    @property
    def path(self):
        return self._path

    def scan(self) -> Database:
        import json

        with open(self.path, "r") as file:
            content = json.load(file)

        return File.Database(**content)
