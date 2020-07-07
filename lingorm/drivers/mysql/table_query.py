from ..table_query_abstract import TableQueryAbstract
from ...mapping.field import Field
from .native_query import NativeQuery
from .column import Column
from .where import Where
from .order_by import OrderBy


class TableQuery(TableQueryAbstract):
    __native = None
    __table = None

    def __init__(self, database_info):
        self.__database_info = database_info
        self.__native = NativeQuery(database_info)

    def table(self, table):
        self.__table = table
        table_name = table.__table__
        if table.__database__ is not None:
            table_name = table.__database__ + "." + table_name
        else:
            table_name = self.__database_info["database"] + "." + table_name
        if table.__alias_table_name__ is not None:
            table_name = table_name + " " + table.__alias_table_name__
        self._from_sql = table_name
        return self

    def select(self, *args):
        self._select_sql = Column().select(args)
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
            self._limit_sql = str(top_count)
        return self

    def first(self):
        sql = self.__get_sql()
        return self.__native.first(sql, self._param_dict, self.__table)

    def find(self):
        sql = self.__get_sql()
        return self.__native.find(sql, self._param_dict, self.__table)

    def find_page(self, page_index, page_size):
        sql = self.__get_sql()
        return self.__native.find_page(sql, self._param_dict, page_index, page_size, self.__table)

    def find_count(self):
        sql = self.__get_sql()
        return self.__native.find_count(sql, self._param_dict)

    def __get_sql(self):
        if self._from_sql is None:
            raise Exception("The table selected from not found")
        sql = ""
        if not self._select_sql:
            self._select_sql = "*"
        sql += "SELECT " + self._select_sql + " FROM " + self._from_sql
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
