import time
import datetime
from ...mapping.field_type import FieldType
from ..query_abstract import QueryAbstract
from ...mapping.entity_converter import EntityConverter
from .query_builder import QueryBuilder
from .native_query import NativeQuery
from .where import Where
from .order_by import OrderBy
from .table_query import TableQuery
from .pymysql_helper import PyMysqlHelper


class Query(QueryAbstract):
    __database_info = None
    __native = None
    __transaction_key = None

    def __init__(self, database_info):
        self.__database_info = database_info
        self.__native = NativeQuery(database_info)

    def table(self, table):
        return TableQuery(self.__database_info).table(table)

    def first(self, cls, where, order_by=None):
        sql = self.__get_sql(cls, where, order_by)
        sql += " LIMIT 1"

        return self.__native.first(sql, where.param_dict, cls)

    def find(self, cls, where, order_by=None, top=0):
        sql = self.__get_sql(cls, where, order_by)
        return self.__native.find(sql, where.param_dict, cls)

    def find_page(self, cls, page_index, page_size, where, order_by=None):
        sql = self.__get_sql(cls, where, order_by)
        return self.__native.find_page(sql, where.param_dict, page_index, page_size, cls)

    def find_top(self, cls, limit, where, order_by=None):
        sql = self.__get_sql(cls, where, order_by)
        limit = int(limit)
        if limit > 0:
            sql += " LIMIT "+str(limit)

        return self.__native.find(sql, where.param_dict, cls)

    def find_count(self, cls, where):
        sql = self.__get_sql(cls, where)
        return self.__native.find_count(sql, where.param_dict)

    def __get_sql(self, cls, where, order_by=None):
        if where is None:
            raise Exception("Missing the where condition!")

        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if cls.__alias_table_name__ is not None:
            table_name = table_name + " " + cls.__alias_table_name__

        sql = "SELECT * FROM " + table_name + " WHERE " + where.sql
        if order_by is not None and order_by.sql is not None:
            sql += " ORDER BY " + order_by.sql
        return sql

    def insert(self, entity):
        if entity is None:
            return
        field_str = ""
        value_str = ""
        param_dict = {}
        index = 0
        for (key, field) in entity.__field_dict__.items():
            if field.is_generated:
                continue
            temp_name = "f" + str(index)
            field_str += field.field_name + ","
            value_str += ":" + temp_name + ","
            param_dict[temp_name] = FieldType.get_field_value(
                entity, key, field.field_type)
            index += 1
        field_str = field_str.strip(',')
        value_str = value_str.strip(',')

        table_name = entity.__table__
        if entity.__database__ is not None:
            table_name = entity.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name

        sql = "INSERT INTO " + table_name + \
            "(" + field_str + ")VALUES(" + value_str + ")"
        result = PyMysqlHelper(self.__database_info, self.__transaction_key).insert(sql, param_dict)
        return result

    def batch_insert(self, entity_list, none_ignore=False):
        if entity_list is None:
            return

        field_dict = entity_list[0].__field_dict__

        field_str = ""
        insert_field_list = []
        for (key, field) in field_dict.items():
            if field.is_generated:
                continue
            if none_ignore and entity_list[0].__getattribute__(key) is None:
                continue
            field_str += field.field_name + ","
            insert_field_list.append(field.field_name)
        field_str = field_str.strip(',')

        value_str = ""
        param_dict = {}
        index = 0

        for entity in entity_list:
            value_str += "("
            for (key, field) in field_dict.items():
                if field.is_generated:
                    continue
                if none_ignore and field.field_name not in insert_field_list:
                    continue
                if entity.__getattribute__(key) is None:
                    value_str += "DEFAULT,"
                else:
                    temp_name = "f" + str(index)
                    value_str += ":" + temp_name + ","
                    param_dict[temp_name] = FieldType.get_field_value(
                        entity, key, field.field_type)
                    index += 1
            value_str = value_str.strip(',')
            value_str += "),"

        value_str = value_str.strip(',')

        table_name = entity_list[0].__table__
        if entity_list[0].__database__ is not None:
            table_name = entity_list[0].__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name

        sql = "INSERT INTO " + table_name + \
            "(" + field_str + ")VALUES" + value_str
        result = self.__native.execute(sql, param_dict)
        return result

    def update(self, entity, none_ignore=False):
        if entity is None:
            return
        set_str = ""
        where_str = None
        param_dict = {}
        index = 0

        for (key, field) in entity.__field_dict__.items():
            if field.is_primary:
                temp_name = "p" + str(index)
                if where_str is None:
                    where_str = field.field_name + "=:" + temp_name
                else:
                    where_str += " AND " + field.field_name + "=:" + field.temp_name
                param_dict[temp_name] = FieldType.get_field_value(
                    entity, key, field.field_type)

            if field.is_generated:
                continue
            if none_ignore and entity.__getattribute__(key) is None:
                continue

            temp_name = "f" + str(index)
            set_str += field.field_name + "=:" + temp_name + ","
            param_dict[temp_name] = FieldType.get_field_value(
                entity, key, field.field_type)
            index += 1

        set_str = set_str.strip(',')

        table_name = entity.__table__
        if entity.__database__ is not None:
            table_name = entity.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name

        sql = "UPDATE " + table_name + " SET " + set_str + " WHERE " + where_str
        result = self.__native.execute(sql, param_dict)
        return result

    def batch_update(self, entity_list, none_ignore=False):
        if entity_list is None:
            return

        field_dict = entity_list[0].__field_dict__

        primary_list = []
        for (key, field) in field_dict.items():
            if field.is_primary:
                primary_list.append({"key": key, "type": field.field_type})

        if len(primary_list) > 1 or len(primary_list) < 1:
            raise Exception(
                "This method only applies to the tables with one primary key field")
        primary_key = primary_list[0]["key"]
        primary_type = primary_list[0]["type"]

        field_set_dict = {}
        param_dict = {}
        id_str = ""
        index = 0
        for entity in entity_list:
            temp_id_name = "p" + str(index)
            if not id_str:
                id_str = ":" + temp_id_name
            else:
                id_str += ",:" + temp_id_name
            param_dict[temp_id_name] = FieldType.get_field_value(
                entity, primary_key, primary_type)

            for (key, field) in field_dict.items():
                if field.is_primary:
                    continue
                if field.is_generated:
                    continue
                if none_ignore and entity.__getattribute__(key) is None:
                    continue

                temp_name = "f" + str(index)
                param_dict[temp_name] = FieldType.get_field_value(
                    entity, key, field.field_type)

                if field.field_name in field_set_dict:
                    field_set_dict[field.field_name] += " WHEN :" + \
                        temp_id_name + " THEN :" + temp_name
                else:
                    field_set_dict[field.field_name] = " WHEN :" + \
                        temp_id_name + " THEN :" + temp_name
                index += 1

        set_str = ""
        for (key, value) in field_set_dict.items():
            set_str += key + " = CASE " + primary_key + value + " ELSE " + key + " END,"
        set_str = set_str.strip(',')

        table_name = entity_list[0].__table__
        if entity_list[0].__database__ is not None:
            table_name = entity_list[0].__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name

        sql = "UPDATE " + table_name + " SET " + set_str + \
            " WHERE " + primary_key + " IN(" + id_str + ")"

        result = self.__native.execute(sql, param_dict)
        return result

    def update_by(self, cls, set_list, where):
        if set_list is None or len(set_list) < 1:
            raise Exception("Nothing needs to be updated!")
        if where is None:
            raise Exception("Missing the where condition!")

        set_str = ""
        for item in set_list:
            temp_str = ""
            if type(item) == str:
                temp_str = item
            else:
                expression = Where().get_expression(item, where.param_dict)
                where.param_dict = expression["param_dict"]
                temp_str = expression["sql"]
            if not set_str:
                set_str = temp_str
            else:
                set_str += "," + temp_str
        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if cls.__alias_table_name__ is not None:
            table_name = table_name + " " + cls.__alias_table_name__

        sql = "UPDATE " + table_name + " SET " + set_str + " WHERE " + where.sql
        result = self.__native.execute(sql, where.param_dict)
        return result

    def delete(self, entity):
        if entity is None:
            return

        param_dict = {}
        where_str = None
        index = 0
        for (key, field) in entity.__field_dict__.items():
            if field.is_primary:
                temp_name = "p" + str(index)
                param_dict[temp_name] = FieldType.get_field_value(
                    entity, key, field.field_type)
                if where_str is None:
                    where_str = field.field_name + "=:" + temp_name
                else:
                    where_str += " AND " + field.field_name + "=:" + temp_name
                index += 1

        table_name = entity.__table__
        if entity.__database__ is not None:
            table_name = entity.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name

        sql = "DELETE FROM " + table_name + " WHERE " + where_str
        result = self.__native.execute(sql, param_dict)
        return result

    def delete_by(self, cls, where):
        if where is None:
            raise Exception("Missing the where condition!")

        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        alias_table_name = ""
        if cls.__alias_table_name__ is not None:
            alias_table_name = cls.__alias_table_name__
            table_name = table_name + " " + cls.__alias_table_name__

        sql = "DELETE " + alias_table_name + " FROM " + table_name + " WHERE " + where.sql
        result = self.__native.execute(sql, where.param_dict)
        return result

    def create_query_builder(self):
        return QueryBuilder(self.__database_info)

    def create_native(self):
        return NativeQuery(self.__database_info)

    def create_where(self):
        return Where()

    def create_order_by(self):
        return OrderBy()

    def begin(self):
        self.__transaction_key = PyMysqlHelper(
            self.__database_info, self.__transaction_key).begin()
        self.__native = NativeQuery(
            self.__database_info, self.__transaction_key)

    def commit(self):
        PyMysqlHelper(self.__database_info, self.__transaction_key).commit()

    def rollback(self):
        PyMysqlHelper(self.__database_info, self.__transaction_key).rollback()
