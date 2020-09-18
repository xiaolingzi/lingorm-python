from .drivers.database_config import DatabaseConfig
from .drivers import mysql, sqlite


class ORM:
    @staticmethod
    def db(key):
        database_info = DatabaseConfig.get_config_by_key(key)
        if database_info["driver"] == "mysql":
            return mysql.Query(database_info)
        elif database_info["driver"] == "sqlite" or database_info["driver"] == "sqlite3":
            return sqlite.Query(database_info)
        else:
            return mysql.Query(database_info)
