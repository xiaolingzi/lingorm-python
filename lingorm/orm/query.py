from ..drivers.database_config import DatabaseConfig
from ..drivers.mysql import *


class Query:
    __database_info = None

    def __init__(self, key=None):
        if key is not None:
            self.__database_info = DatabaseConfig.get_config_by_key(key)
            self.__database_info["key"] = key

    def create_table(self, cls):
        pass

    def create_query(self):
        if self.__database_info["driver"] == "pdo_mysql":
            return MysqlORMQuery(self.__database_info)
        else:
            return MysqlORMQuery(self.__database_info)

    def create_where(self):
        if self.__database_info["driver"] == "pdo_mysql":
            return MysqlWhereExpression()
        else:
            return MysqlWhereExpression()

    def create_order(self):
        if self.__database_info["driver"] == "pdo_mysql":
            return MysqlOrderExpression()
        else:
            return MysqlOrderExpression()

    def create_sql(self):
        if self.__database_info["driver"] == "pdo_mysql":
            return MysqlQuery(self.__database_info)
        else:
            return MysqlQuery(self.__database_info)
