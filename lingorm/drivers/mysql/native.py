import pymysql
import re
from .config import Config
import time


class Native:
    __connection_pool = {}
    __transaction_key = None
    transaction_connectors = {}
    __database_info = None

    def __init__(self, database_info, transaction_key=None):
        self.__transaction_key = transaction_key
        self.__database_info = self.__get_database_info(database_info)

    def __get_database_info(self, database_info):
        if "host" not in database_info.keys() or database_info["host"] == "":
            database_info["host"] = "127.0.0.1"
        if "port" not in database_info.keys() or database_info["port"] == "":
            database_info["port"] = 3306
        database_info["port"] = int(database_info["port"] )
        if "charset" not in database_info.keys() or database_info["charset"] == "":
            database_info["charset"] = "UTF8"
        return database_info

    def __connect(self, mode):
        database_info = Config.get_read_write_database(
            self.__database_info, mode)
        return pymysql.connect(
            host=database_info["host"], port=database_info["port"], user=database_info["user"], passwd=database_info["password"], db=database_info[
                "database"], charset=database_info["charset"], cursorclass=pymysql.cursors.DictCursor
        )

    def __get_cursor(self, sql, param_dict, mode=None):
        if self.__transaction_key is not None and self.__transaction_key in Native.transaction_connectors.keys():
            self.__connection = Native.transaction_connectors[self.__transaction_key]
        else:
            if mode is None:
                if sql.upper().startswith("SELECT"):
                    mode = Config.MODE_READ
                else:
                    mode = Config.MODE_WRITE
            self.__connection = self.__connect(mode)
        tran_result = self.__sql_translate(sql, param_dict)
        cursor = self.__connection.cursor()
        affected_rows = cursor.execute(tran_result["sql"], tran_result["param"])
        return {"cursor":cursor, "affected_rows":affected_rows}

    def execute(self, sql, param_dict=None):
        result = 0
        try:
            cursor = self.__get_cursor(sql, param_dict)
            if self.__transaction_key is None or self.__transaction_key not in Native.transaction_connectors.keys():
                self.__connection.commit()
            cursor["cursor"].close()
            result = cursor["affected_rows"]
        except:
            self.__connection.rollback()
            raise
        return result

    def fetch_one(self, sql, param_dict=None):
        cursor = self.__get_cursor(sql, param_dict, Config.MODE_READ)["cursor"]
        result = cursor.fetchone()
        cursor.close()
        return result

    def fetch_all(self, sql, param_dict=None):
        cursor = self.__get_cursor(sql, param_dict, Config.MODE_READ)["cursor"]
        result = cursor.fetchall()
        cursor.close()
        return result

    def insert(self, sql, param_dict=None):
        cursor = self.__get_cursor(sql, param_dict, Config.MODE_WRITE)["cursor"]
        result = int(cursor.lastrowid)
        if self.__transaction_key is None or self.__transaction_key not in Native.transaction_connectors.keys():
            self.__connection.commit()
        cursor.close()
        return result

    def begin(self):
        t = time.time()
        key = str(int(round(t * 1000000)))
        connection = self.__connect(Config.MODE_WRITE)
        Native.transaction_connectors[key]=connection
        return key
    
    def commit(self):
        if self.__transaction_key is not None and self.__transaction_key in Native.transaction_connectors.keys():
            Native.transaction_connectors[self.__transaction_key].commit()
        else:
            raise Exception("Begin a transaction first before commit")
        del Native.transaction_connectors[self.__transaction_key]

    def rollback(self):
        if self.__transaction_key is not None and self.__transaction_key in Native.transaction_connectors.keys():
            Native.transaction_connectors[self.__transaction_key].rollback()
        else:
            raise Exception("Begin a transaction first before rollback")
        del Native.transaction_connectors[self.__transaction_key]

    def __sql_translate(self, sql, param_dict):
        if param_dict is None or len(param_dict) == 0:
            return {"sql": sql, "param": param_dict}

        find_list = re.findall(r':([a-zA-Z0-9_\\-]+)', sql)

        sql_deal = sql
        param_list = []
        for key in find_list:
            param_list.append(param_dict[key])
            sql_deal = re.sub(":" + key, "%s", sql_deal, 1)

        return {"sql": sql_deal, "param": param_list}
