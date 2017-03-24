from ...drivers.where_expression_abstract import WhereExpressionAbstract
from .mysql_define import MysqlDefine


class MysqlWhereExpression(WhereExpressionAbstract):
    sql = ""
    param_dict = {}

    def set_and(self, *args):
        self.sql = self.__get_sql_expression(args, 1)
        return self.sql

    def set_or(self, *args):
        self.sql = self.__get_sql_expression(args, 2)
        return self.sql

    def __get_sql_expression(self, args, set_type):
        if args is None:
            return ""

        sql = None
        for item in args:
            temp_sql = ""
            if type(item) == str:
                temp_sql = "(" + item + ")"
            else:
                expression_dict = MysqlDefine().get_expression(item, self.param_dict)
                temp_sql = expression_dict["sql"]
                self.param_dict = expression_dict["param_dict"]
            if sql is None:
                sql = temp_sql
            else:
                if set_type == 1:
                    sql += " and " + temp_sql
                else:
                    sql += " or " + temp_sql
        return sql
