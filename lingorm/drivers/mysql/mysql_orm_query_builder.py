from ...drivers.orm_query_builder_abstract import ORMQueryBuilderAbstract
from ...drivers.mysql.pymysql_helper import PyMysqlHelper
from .mysql_define import MysqlDefine
from ...mapping.field import Field
from ...mapping.entity_converter import EntityConverter


class MysqlORMQueryBuilder(ORMQueryBuilderAbstract):
    __py_mysql = None

    def __init__(self, database_info):
        self.__pdo_mysql = PyMysqlHelper(database_info)
        self.__database_info = database_info

    def select(self, *args):
        if args is None or len(args) < 1:
            self._select_sql = "*"
            return self

        for column in args:
            field_str = column
            if isinstance(column, Field):
                field_str = column.field_name
                if column.alias_table_name is not None:
                    field_str = column.alias_table_name + "." + field_str
                if column.is_distinct:
                    field_str = "distinct " + field_str
                if column.is_count:
                    field_str = "count(" + field_str + ")"
                if column.is_sum:
                    field_str = "sum(" + field_str + ")"
                if column.alias_field_name is not None:
                    field_str = field_str + " as " + column.alias_field_name
            elif isinstance(column, object) and hasattr(column, "__alias_table_name__"):
                field_str = column.__alias_table_name__ + ".*"

            if self._select_sql is None:
                self._select_sql = field_str
            else:
                self._select_sql += "," + field_str
        return self

    def from_table(self, cls):
        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if cls.__alias_table_name__ is not None:
            table_name = table_name + " " + cls.__alias_table_name__
        self._from_sql = "from " + table_name
        return self

    def left_join(self, cls, on_expression):
        if self._param_dict is None:
            self._param_dict = on_expression.param_dict
        elif on_expression.param_dict is not None:
            self._param_dict = dict(self._param_dict, **on_expression.param_dict)

        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if cls.__alias_table_name__ is not None:
            table_name = table_name + " " + cls.__alias_table_name__

        join_sql = " left join " + table_name + " on " + on_expression.sql
        if self._join_sql is None:
            self._join_sql = join_sql
        else:
            self._join_sql += " " + join_sql

        return self

    def right_join(self, cls, on_expression):
        if self._param_dict is None:
            self._param_dict = on_expression.param_dict
        elif on_expression.param_dict is not None:
            self._param_dict = dict(self._param_dict, **on_expression.param_dict)

        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if cls.__alias_table_name__ is not None:
            table_name = table_name + " " + cls.__alias_table_name__

        join_sql = " right join " + table_name + " on " + on_expression.sql
        if self._join_sql is None:
            self._join_sql = join_sql
        else:
            self._join_sql += " " + join_sql

        return self

    def inner_join(self, cls, on_expression):
        if self._param_dict is None:
            self._param_dict = on_expression.param_dict
        elif on_expression.param_dict is not None:
            self._param_dict = dict(self._param_dict, **on_expression.param_dict)

        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if cls.__alias_table_name__ is not None:
            table_name = table_name + " " + cls.__alias_table_name__

        join_sql = " inner join " + table_name + " on " + on_expression.sql
        if self._join_sql is None:
            self._join_sql = join_sql
        else:
            self._join_sql += " " + join_sql

        return self

    def where(self, where_expression):
        self._where_sql = "where " + where_expression.sql
        if self._param_dict is None:
            self._param_dict = where_expression.param_dict
        elif where_expression.param_dict is not None:
            self._param_dict = dict(self._param_dict, **where_expression.param_dict)
        return self

    def order_by(self, field, order_type):
        if order_type is None or order_type.lower() not in MysqlDefine.order_dict:
            raise Exception("Order type not found")
        order_type = order_type.lower()
        order_type = MysqlDefine.order_dict[order_type]
        field_name = field
        if isinstance(field, Field):
            field_name = field.field_name
            if field.alias_table_name is not None:
                field_name = field.alias_table_name + "." + field_name
        if self._order_sql is None:
            self._order_sql = "order by " + field_name + " " + order_type
        else:
            self._order_sql += "," + field_name + " " + order_type

        return self

    def group_by(self, **args):
        if args is None:
            raise Exception("Fields for grouping not found")
        for field in args:
            field_str = args
            if isinstance(field, Field):
                field_str = field.alias_table_name + "." + field.field_name
            if self._group_sql is None:
                self._group_sql = "group by " + field_str
            else:
                self._group_sql += "," + field_str
        return self

    def limit(self, top_count):
        top_count = int(top_count)
        if top_count > 0:
            self._limit_sql = "limit " + str(top_count)
        return self

    def get_result(self, cls=None):
        sql = self.__get_sql()
        return self.__get_data(sql, cls)

    def get_page_result(self, page_index, page_size, cls=None):
        result = {"page_index": page_index, "page_size": page_size}
        sql = self.__get_sql()
        sql_count = "select count(*) as num from (" + sql + ") tmp"
        count_result = self.__get_data(sql_count)
        total_count = count_result[0]["num"]
        total_pages = int((total_count + page_size - 1) / page_size)
        result["total_count"] = total_count
        result["total_pages"] = total_pages
        if page_index > total_pages:
            result["data"] = []
        else:
            sql = "select * from (" + sql + ") tmp limit " + str(((page_index - 1) * page_size)) + ', ' + str(page_size)
            result["data"] = self.__get_data(sql, cls)
        return result

    def __get_sql(self):
        if self._from_sql is None:
            raise Exception("The table selected from not found")
        sql = ""
        if self._select_sql is None:
            self._select_sql = "*"
        sql += "select " + self._select_sql + " " + self._from_sql
        if self._join_sql is not None:
            sql += " " + self._join_sql
        if self._where_sql is not None:
            sql += " " + self._where_sql
        if self._group_sql is not None:
            sql += " " + self._group_sql
        if self._order_sql is not None:
            sql += " " + self._order_sql
        if self._limit_sql is not None:
            sql += " " + self._limit_sql
        self._sql = sql
        return sql

    def __get_data(self, sql, cls=None):
        temp_result = self.__pdo_mysql.fetch_all(sql, self._param_dict)
        if cls is None or temp_result is None:
            return temp_result
        result = []
        for item in temp_result:
            temp_entity = EntityConverter.dict_to_entity(cls, item)
            result.append(temp_entity)
        return result
