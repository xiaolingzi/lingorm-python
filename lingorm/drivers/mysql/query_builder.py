from ..query_builder_abstract import QueryBuilderAbstract
from ...mapping.field import Field
from ...mapping.condition import Condition
from .native_query import NativeQuery
from .where import Where
from .order_by import OrderBy
from .column import Column
from .pymysql_helper import PyMysqlHelper


class QueryBuilder(QueryBuilderAbstract):
    __native = None

    def __init__(self, database_info, transaction_key=None):
        self.__database_info = database_info
        self.__native = NativeQuery(database_info, transaction_key)

    def select(self, *args):
        self._select_sql = Column().select(args)
        return self

    def from_table(self, cls):
        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if cls.__alias_table_name__ is not None:
            table_name = table_name + " " + cls.__alias_table_name__
        self._from_sql = table_name
        return self

    def left_join(self, cls, on_expression):
        on_sql = ""
        if type(on_expression) == str and on_expression != "":
            on_sql = on_expression
        elif isinstance(on_expression, Where):
            on_sql = on_expression.sql
            self.param_dict = {**self._param_dict, **on_expression.param_dict}
        elif isinstance(on_expression, Condition):
            expression_dict = Where().get_expression(on_expression, self._param_dict)
            on_sql = expression_dict["sql"]
            self._param_dict = expression_dict["param_dict"]

        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if cls.__alias_table_name__ is not None:
            table_name = table_name + " " + cls.__alias_table_name__

        join_sql = "LEFT JOIN " + table_name + " ON " + on_sql
        if not self._join_sql:
            self._join_sql = join_sql
        else:
            self._join_sql += " " + join_sql

        return self

    def right_join(self, cls, on_expression):
        on_sql = ""
        if type(on_expression) == str and on_expression != "":
            on_sql = on_expression
        elif isinstance(on_expression, Where):
            on_sql = on_expression.sql
            self.param_dict = {**self._param_dict, **on_expression.param_dict}
        elif isinstance(on_expression, Condition):
            expression_dict = Where().get_expression(on_expression, self._param_dict)
            on_sql = expression_dict["sql"]
            self._param_dict = expression_dict["param_dict"]

        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if cls.__alias_table_name__ is not None:
            table_name = table_name + " " + cls.__alias_table_name__

        join_sql = "RIGHT JOIN " + table_name + " ON " + on_sql
        if not self._join_sql:
            self._join_sql = join_sql
        else:
            self._join_sql += " " + join_sql

        return self

    def inner_join(self, cls, on_expression):
        on_sql = ""
        if type(on_expression) == str and on_expression != "":
            on_sql = on_expression
        elif isinstance(on_expression, Where):
            on_sql = on_expression.sql
            self.param_dict = {**self._param_dict, **on_expression.param_dict}
        elif isinstance(on_expression, Condition):
            expression_dict = Where().get_expression(on_expression, self._param_dict)
            on_sql = expression_dict["sql"]
            self._param_dict = expression_dict["param_dict"]

        table_name = cls.__table__
        if cls.__database__ is not None:
            table_name = cls.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if cls.__alias_table_name__ is not None:
            table_name = table_name + " " + cls.__alias_table_name__

        join_sql = "INNER JOIN " + table_name + " ON " + on_sql
        if not self._join_sql:
            self._join_sql = join_sql
        else:
            self._join_sql += " " + join_sql

        return self

    def where(self, *args):
        where = Where().add_and(*args)
        self._where_sql = where.sql
        if not self._param_dict:
            self._param_dict = where.param_dict
        elif where.param_dict is not None:
            self._param_dict = dict(self._param_dict, **where.param_dict)
        return self

    def order_by(self, *args):
        order_by = OrderBy().by(*args)
        if not self._order_sql:
            self._order_sql = order_by.sql
        else:
            self._order_sql += "," + order_by.sql
        return self

    def group_by(self, *args):
        if args is None:
            raise Exception("Fields for grouping not found")
        for field in args:
            field_str = args
            if isinstance(field, Field):
                field_str = field.alias_table_name + "." + field.field_name
            if not self._group_sql:
                self._group_sql = field_str
            else:
                self._group_sql += "," + field_str
        return self

    def limit(self, top_count):
        top_count = int(top_count)
        if top_count > 0:
            self._limit_sql = "LIMIT " + str(top_count)
        return self

    def first(self, cls=None):
        sql = self.__get_sql()
        return self.__native.first(sql, self._param_dict, cls)

    def find(self, cls=None):
        sql = self.__get_sql()
        return self.__native.find(sql, self._param_dict, cls)

    def find_page(self, page_index, page_size, cls=None):
        sql = self.__get_sql()
        return self.__native.find_page(sql, self._param_dict, page_index, page_size, cls)

    def find_count(self):
        sql = self.__get_sql()
        return self.__native.find_count(sql, self._param_dict)

    def __get_sql(self):
        if not self._from_sql:
            raise Exception("The table selected from not found")
        sql = ""
        if not self._select_sql:
            self._select_sql = "*"
        sql += "SELECT " + self._select_sql + " FROM " + self._from_sql
        if self._join_sql:
            sql += " " + self._join_sql
        if self._where_sql:
            sql += " WHERE " + self._where_sql
        if self._group_sql:
            sql += " GROUP BY " + self._group_sql
        if self._order_sql:
            sql += " ORDER BY " + self._order_sql
        if self._limit_sql:
            sql += " LIMIT " + self._limit_sql
        self._sql = sql
        return sql
