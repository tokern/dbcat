from abc import ABC, abstractmethod

# from dbcat.scanners.json import File


class Scanner(ABC):
    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

    @abstractmethod
    def scan(self):
        pass


#    @staticmethod
#    def create(config):
#        scanners = []
#        for db in config["databases"]:
#            if db["type"] == "file":
#                scanners.append(File(db["name"], db["path"]))
#
#        return scanners
