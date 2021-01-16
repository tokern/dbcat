class Catalog:
    def __init__(
        self, type: str, user: str, password: str, host: str, port: str = None
    ):
        self.type = type
        self.user = user
        self.password = password
        self.host = host
        self.port = port
