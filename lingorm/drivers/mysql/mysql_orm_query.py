from ...mapping.field_type import FieldType
from ...drivers.orm_query_abstract import ORMQueryAbstract
from ...drivers.mysql.pymysql_helper import PyMysqlHelper
from ...mapping.entity_converter import EntityConverter
from .mysql_define import MysqlDefine


class MysqlORMQuery(ORMQueryAbstract):
    __pdo_mysql = None
    __database_info = None

    def __init__(self, database_info):
        self.__pdo_mysql = PyMysqlHelper(database_info)
        self.__database_info = database_info

    def fetch_one(self, cls, where_condition, order_condition=None):
        if where_condition is None:
            raise Exception("Missing the where condition!")

        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if cls.__alias_table_name__ is not None:
            table_name = table_name + " " + cls.__alias_table_name__

        sql = "select * from " + table_name + " where " + where_condition.sql
        if order_condition is not None and order_condition.sql is not None:
            sql += " order by " + order_condition.sql
        sql += " limit 1"

        data_dict = self.__pdo_mysql.fetch_one(sql, where_condition.param_dict)
        return EntityConverter.dict_to_entity(cls, data_dict)

    def fetch_all(self, cls, where_condition, order_condition=None, top=0):
        if where_condition is None:
            raise Exception("Missing the where condition!")

        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if cls.__alias_table_name__ is not None:
            table_name = table_name + " " + cls.__alias_table_name__

        sql = "select * from " + table_name + " where " + where_condition.sql
        if order_condition is not None and order_condition.sql is not None:
            sql += " order by " + order_condition.sql
        top = int(top)
        if top > 0:
            sql += " limit " + str(top)

        data_list = self.__pdo_mysql.fetch_all(sql, where_condition.param_dict)
        result = []
        if data_list is not None:
            for row_dict in data_list:
                result.append(EntityConverter.dict_to_entity(cls, row_dict))
        return result

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
            param_dict[temp_name] = FieldType.get_field_value(entity, key, field.field_type)
            index += 1
        field_str = field_str.strip(',')
        value_str = value_str.strip(',')

        table_name = entity.__table__
        if entity.__database__ is not None:
            table_name = entity.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name

        sql = "insert into " + table_name + "(" + field_str + ")values(" + value_str + ")"
        result = self.__pdo_mysql.insert(sql, param_dict)
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
                    value_str += "default,"
                else:
                    temp_name = "f" + str(index)
                    value_str += ":" + temp_name + ","
                    param_dict[temp_name] = FieldType.get_field_value(entity, key, field.field_type)
                    index += 1
            value_str = value_str.strip(',')
            value_str += "),"

        value_str = value_str.strip(',')

        table_name = entity_list[0].__table__
        if entity_list[0].__database__ is not None:
            table_name = entity_list[0].__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name

        sql = "insert into " + table_name + "(" + field_str + ")values" + value_str
        result = self.__pdo_mysql.execute(sql, param_dict)
        return result

    def update(self, entity, none_ignore=False):
        if entity is None:
            return
        set_str = ""
        where_str = None
        param_dict = {}
        index = 0

        for (key, field) in entity.__field_dict__.items():
            if field.primary_key:
                temp_name = "p" + str(index)
                if where_str is None:
                    where_str = field.field_name + "=:" + temp_name
                else:
                    where_str += " and " + field.field_name + "=:" + field.temp_name
                param_dict[temp_name] = FieldType.get_field_value(entity, key, field.field_type)

            if field.is_generated:
                continue
            if none_ignore and entity.__getattribute__(key) is None:
                continue

            temp_name = "f" + str(index)
            set_str += field.field_name + "=:" + temp_name + ","
            param_dict[temp_name] = FieldType.get_field_value(entity, key, field.field_type)
            index += 1

        set_str = set_str.strip(',')

        table_name = entity.__table__
        if entity.__database__ is not None:
            table_name = entity.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name

        sql = "update " + table_name + " set " + set_str + " where " + where_str
        result = self.__pdo_mysql.execute(sql, param_dict)
        return result

    def batch_update(self, entity_list, none_ignore=False):
        if entity_list is None:
            return

        field_dict = entity_list[0].__field_dict__

        primary_key_list = []
        for (key, field) in field_dict.items():
            if field.primary_key:
                primary_key_list.append(field.field_name)

        if len(primary_key_list) > 1 or len(primary_key_list) < 1:
            raise Exception("This method applies only to tables that have only one primary key field")
        primary_key = primary_key_list[0]

        field_set_dict = {}
        param_dict = {}
        id_str = None
        index = 0
        for entity in entity_list:
            for (key, field) in field_dict.items():
                temp_id_name = "p" + str(index)
                if field.primary_key:
                    if id_str is None:
                        id_str = ":" + temp_id_name
                    else:
                        id_str += ",:" + temp_id_name
                    param_dict[temp_id_name] = FieldType.get_field_value(entity, key, field.field_type)

                if field.is_generated:
                    continue
                if none_ignore and entity.__getattribute__(key) is None:
                    continue

                temp_name = "f" + str(index)
                param_dict[temp_name] = FieldType.get_field_value(entity, key, field.field_type)

                if key in field_set_dict:
                    field_set_dict[field.field_name] += " when :" + temp_id_name + " then :" + temp_name
                else:
                    field_set_dict[field.field_name] = " when :" + temp_id_name + " then :" + temp_name
                index += 1

        set_str = ""
        for (key, value) in field_set_dict.items():
            set_str += key + " = case " + primary_key + value + " else " + key + " end,"
        set_str = set_str.strip(',')

        table_name = entity_list[0].__table__
        if entity_list[0].__database__ is not None:
            table_name = entity_list[0].__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name

        sql = "update " + table_name + " set " + set_str + " where " + primary_key + " in(" + id_str + ")"
        result = self.__pdo_mysql.execute(sql, param_dict)
        return result

    def update_by(self, cls, set_list, where_condition):
        if set_list is None or len(set_list) < 1:
            raise Exception("Nothing needs to be updated!")
        if where_condition is None:
            raise Exception("Missing the where condition!")

        set_str = None
        for item in set_list:
            temp_str = ""
            if type(item) == str:
                temp_str = item
            else:
                expression = MysqlDefine().get_expression(item, where_condition.param_dict)
                where_condition.param_dict = expression["param_dict"]
                temp_str = expression["sql"]
            if set_str is None:
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

        sql = "update " + table_name + " set " + set_str + " where " + where_condition.sql
        result = self.__pdo_mysql.execute(sql, where_condition.param_dict)
        return result

    def delete(self, entity):
        if entity is None:
            return

        param_dict = {}
        where_str = None
        index = 0
        for (key, field) in entity.__field_dict__.items():
            if field.primary_key:
                temp_name = "p" + str(index)
                param_dict[temp_name] = FieldType.get_field_value(entity, key, field.field_type)
                if where_str is None:
                    where_str = field.field_name + "=:" + temp_name
                else:
                    where_str += " and " + field.field_name + "=:" + temp_name
                index += 1

        table_name = entity.__table__
        if entity.__database__ is not None:
            table_name = entity.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name

        sql = "delete from " + table_name + " where " + where_str
        result = self.__pdo_mysql.execute(sql, param_dict)
        return result

    def delete_by(self, cls, where_condition):
        if where_condition is None:
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

        sql = "delete " + alias_table_name + " from " + table_name + " where " + where_condition.sql
        result = self.__pdo_mysql.execute(sql, where_condition.param_dict)
        return result
