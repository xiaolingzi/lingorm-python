from ...drivers.mysql.pymysql_helper import PyMysqlHelper
from ...mapping.entity_converter import EntityConverter
from ...drivers.sql_query_abstract import SqlQueryAbstract


class MysqlQuery(SqlQueryAbstract):
    __pdo_mysql = None

    def __init__(self, database_info):
        self.__pdo_mysql = PyMysqlHelper(database_info)

    def execute(self, sql, param_dict):
        return self.__pdo_mysql.execute(sql, param_dict)

    def get_result(self, sql, param_dict, cls=None):
        return self.__get_data(sql, param_dict, cls)

    def get_page_result(self, sql, param_dict, page_index, page_size, cls=None):
        result = {"page_index": page_index, "page_size": page_size}
        sql_count = "select count(*) as num from (" + sql + ") tmp"
        count_result = self.__get_data(sql_count, param_dict)
        total_count = count_result[0]["num"]
        total_pages = int((total_count + page_size - 1) / page_size)
        result["total_count"] = total_count
        result["total_pages"] = total_pages
        if page_index > total_pages:
            result["data"] = []
        else:
            sql = "select * from (" + sql + ") tmp limit " + str(((page_index - 1) * page_size)) + ', ' + str(page_size)
            result["data"] = self.__get_data(sql, param_dict, cls)
        return result

    def __get_data(self, sql, param_dict, cls=None):
        temp_result = self.__pdo_mysql.fetch_all(sql, param_dict)
        if cls is None or temp_result is None:
            return temp_result
        result = []
        for item in temp_result:
            temp_entity = EntityConverter.dict_to_entity(cls, item)
            result.append(temp_entity)
        return result
