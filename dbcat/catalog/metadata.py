import re
from abc import ABC

from dbcat.log_mixin import LogMixin


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


class Table(NamedObject):
    def __init__(self, name, parent):
        super(Table, self).__init__(name, parent)

    @property
    def columns(self):
        return self._children


class Column(NamedObject):
    def __init__(self, name, parent, type):
        super(Column, self).__init__(name, parent)
        self._type = type

    @property
    def type(self):
        return self._type
