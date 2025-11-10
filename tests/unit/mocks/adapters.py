class ConnectorHost:
    def __init__(self, **overrides):
        self.endpoint = overrides.get('endpoint', 'db.local')
        self.database = overrides.get('database', 'app')
        self.table = overrides.get('table', 'items')
        self.user = overrides.get('user', 'svc')
        self.password = overrides.get('password', 'secret')
        self.port = overrides.get('port', 5432)
        self.autocommit = overrides.get('autocommit', False)
        self.engine = overrides.get('engine', 'postgres')
