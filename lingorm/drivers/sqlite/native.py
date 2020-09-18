import sqlite3
import re
from .config import Config
import time


class Native:
    __transaction_key = None
    transaction_connectors = {}
    __database_info = None

    def __init__(self, database_info, transaction_key=None):
        self.__transaction_key = transaction_key
        self.__database_info = self.__get_database_info(database_info)

    def __get_database_info(self, database_info):
        if "file" not in database_info.keys() or database_info["file"] == "":
            raise Exception("Database file path not found")
        if "timeout" in database_info.keys() and database_info["timeout"] != "":
            database_info["timeout"] = int(database_info["timeout"])
        else:
            database_info["timeout"] = 5
        return database_info

    def dict_factory(self,cursor,row):
        d={}
        for index,col in enumerate(cursor.description):
            d[col[0]]=row[index]
        return d

    def __connect(self):
        conn = sqlite3.connect(self.__database_info["file"], self.__database_info["timeout"])
        conn.row_factory = self.dict_factory
        return conn

    def __get_cursor(self, sql, param_dict):
        if self.__transaction_key is not None and self.__transaction_key in Native.transaction_connectors.keys():
            self.__connection = Native.transaction_connectors[self.__transaction_key]
        else:
            self.__connection = self.__connect()
        tran_result = self.__sql_translate(sql, param_dict)
        cursor = self.__connection.cursor()
        cursor.execute(tran_result["sql"], tran_result["param"])
        return {"cursor": cursor, "affected_rows": cursor.rowcount}

    def execute(self, sql, param_dict=None):
        result = 0
        try:
            cursor = self.__get_cursor(sql, param_dict)
            if self.__transaction_key is None or self.__transaction_key not in Native.transaction_connectors.keys():
                self.__connection.commit()
            cursor["cursor"].close()
            result = cursor["affected_rows"]
            print(1111)
            print(result)
        except:
            self.__connection.rollback()
            raise
        return result

    def fetch_one(self, sql, param_dict=None):
        cursor = self.__get_cursor(sql, param_dict)["cursor"]
        result = cursor.fetchone()
        cursor.close()
        return result

    def fetch_all(self, sql, param_dict=None):
        cursor = self.__get_cursor(sql, param_dict)["cursor"]
        result = cursor.fetchall()
        cursor.close()
        return result

    def insert(self, sql, param_dict=None):
        cursor = self.__get_cursor(sql, param_dict)["cursor"]
        result = int(cursor.lastrowid)
        if self.__transaction_key is None or self.__transaction_key not in Native.transaction_connectors.keys():
            self.__connection.commit()
        cursor.close()
        return result

    def begin(self):
        t = time.time()
        key = str(int(round(t * 1000000)))
        connection = self.__connect()
        Native.transaction_connectors[key] = connection
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
        return {"sql": sql, "param": param_dict}
        # if param_dict is None or len(param_dict) == 0:
        #     return {"sql": sql, "param": param_dict}

        # find_list = re.findall(r':([a-zA-Z0-9_\\-]+)', sql)

        # sql_deal = sql
        # param_list = []
        # for key in find_list:
        #     param_list.append(param_dict[key])
        #     sql_deal = re.sub(":" + key, "?", sql_deal, 1)

        # return {"sql": sql_deal, "param": param_list}
