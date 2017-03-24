import sys


class Config:
    default_database_server = "test"
    database_config_file = sys.path[0] + "/config/dev/database_config.json"
