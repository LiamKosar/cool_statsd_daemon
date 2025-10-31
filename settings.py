import yaml

class Config(object):

    port: int
    postgresql_env_variable_name: str
    max_connections: int
    min_connections: int
    num_queue_workers: int
    datagram_max_size: int
    log_batch_size: int
    # seconds
    log_batch_timeout: int
    log_table_name: str


    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Config, cls).__new__(cls)
        return cls.instance
    
    def initialize(self):
        with open('config.yaml', 'r') as file:
            config = yaml.safe_load(file)
            daemon_settings = config['settings']
            self.port = daemon_settings['port']
            self.postgresql_env_variable_name = daemon_settings['postgresql_env_variable_name']
            self.max_connections = daemon_settings['max_connections']
            self.min_connections = daemon_settings['min_connections']
            self.num_queue_workers = daemon_settings['num_queue_workers']
            self.datagram_max_size = daemon_settings['datagram_max_size']
            self.log_batch_size = daemon_settings['log_batch_size']
            self.log_batch_timeout = daemon_settings['log_batch_timeout']
            self.log_table_name = daemon_settings['log_table_name']
            

