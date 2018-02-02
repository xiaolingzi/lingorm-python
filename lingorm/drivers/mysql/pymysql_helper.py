import pymysql
import re
from ..database_config import DatabaseConfig


class PyMysqlHelper:
    __connection_pool = {}

    def __init__(self, database_info):
        key = database_info["key"]
        if "servers" in database_info.keys():
            self.__connection = None
            self.__read_database = DatabaseConfig.get_read_write_database(database_info, "r")
            self.__write_database = DatabaseConfig.get_read_write_database(database_info, "w")
        elif key in PyMysqlHelper.__connection_pool:
            self.__connection = PyMysqlHelper.__connection_pool[key]
        else:
            self.__connection = self.__get_connection(database_info)
            PyMysqlHelper.__connection_pool[key] = self.__connection

    def __get_connection(self, database_info):
        return pymysql.connect(
            host=database_info["host"]
            , user=database_info["user"]
            , passwd=database_info["password"]
            , db=database_info["database"]
            , charset=database_info["charset"]
            , cursorclass=pymysql.cursors.DictCursor
        )

    def execute(self, sql, param_dict=None):
        if self.__connection is None:
            sql = sql.strip()
            if sql[0:6].lower() == "select":
                self.__connection = self.__get_connection(self.__read_database)
            else:
                self.__connection = self.__get_connection(self.__write_database)
        tran_result = self.__sql_translate(sql, param_dict)
        cursor = self.__connection.cursor()
        result = 0
        try:
            result = cursor.execute(tran_result["sql"], tran_result["param"])
            self.__connection.commit()
        except:
            self.__connection.rollback()
            raise

        cursor.close()
        return result

    def fetch_one(self, sql, param_dict=None):
        if self.__connection is None:
            self.__connection = self.__get_connection(self.__read_database)
        tran_result = self.__sql_translate(sql, param_dict)
        cursor = self.__connection.cursor()
        cursor.execute(tran_result["sql"], tran_result["param"])
        result = cursor.fetchone()
        self.__connection.commit()
        cursor.close()
        return result

    def fetch_all(self, sql, param_dict=None):
        if self.__connection is None:
            self.__connection = self.__get_connection(self.__read_database)
        tran_result = self.__sql_translate(sql, param_dict)
        cursor = self.__connection.cursor()
        cursor.execute(tran_result["sql"], tran_result["param"])
        result = cursor.fetchall()
        self.__connection.commit()
        cursor.close()
        return result

    def insert(self, sql, param_dict=None):
        if self.__connection is None:
            self.__connection = self.__get_connection(self.__write_database)
        tran_result = self.__sql_translate(sql, param_dict)
        cursor = self.__connection.cursor()
        cursor.execute(tran_result["sql"], tran_result["param"])
        result = int(cursor.lastrowid)
        self.__connection.commit()
        cursor.close()
        return result

    def __sql_translate(self, sql, param_dict):
        if param_dict is None or len(param_dict) == 0:
            return {"sql": sql, "param": param_dict}

        find_list = re.findall(r':([^\s,):]+)', sql)

        sql_deal = sql
        param_list = []
        for key in find_list:
            param_list.append(param_dict[key])
            sql_deal = re.sub(":" + key, "%s", sql_deal, 1)

        return {"sql": sql_deal, "param": param_list}
