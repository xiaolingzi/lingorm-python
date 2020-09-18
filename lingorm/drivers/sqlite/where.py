import re
from ..where_abstract import WhereAbstract
from ...mapping.field import Field
from ...common.common_define import CommonDefine


class Where(WhereAbstract):
    sql = ""
    param_dict = {}
    operator_dict = {
        "eq": "=",
        "neq": "<>",
        "gt": ">",
        "ge": ">=",
        "lt": "<",
        "le": "<=",
        "in": "IN",
        "nin": "NOT IN",
        "like": "LIKE",
        "fis": "FIND_IN_SET"
    }

    def add_and(self, *args):
        args = args + (self,)
        self.sql = self.__get_sql_expression(args, 1)
        return self

    def add_or(self, *args):
        args = args + (self,)
        self.sql = self.__get_sql_expression(args, 2)
        return self

    def get_and(self, *args):
        result = self.__get_sql_expression(args, 1)
        return result

    def get_or(self, *args):
        result = self.__get_sql_expression(args, 2)
        return result

    def or_and(self, *args):
        sql = self.__get_sql_expression(args, 1)
        return self.add_or(sql)

    def and_or(self, *args):
        sql = self.__get_sql_expression(args, 2)
        return self.add_and(sql)

    def __get_sql_expression(self, args, set_type):
        if args is None:
            return ""

        sql = ""
        for item in args:
            temp_sql = ""
            if type(item) == str and item != "":
                temp_sql = item
            elif isinstance(item, Where):
                temp_sql = item.sql
                self.param_dict = {**self.param_dict, **item.param_dict}
            else:
                expression_dict = self.get_expression(item, self.param_dict)
                temp_sql = expression_dict["sql"]
                self.param_dict = expression_dict["param_dict"]

            temp_str = re.sub(r"\(.*\)", "", temp_sql)
            if (" AND " in temp_str and set_type == 2) or (" OR " in temp_str and set_type == 1):
                temp_sql = "(" + temp_sql + ")"

            if not temp_sql:
                continue

            if not sql:
                sql = temp_sql
            else:
                if set_type == 1:
                    sql += " AND " + temp_sql
                else:
                    sql += " OR " + temp_sql
        return sql

    def get_expression(self, condition, param_dict):
        sql = ""
        field_name = condition.field_name
        if condition.alias_table_name is not None:
            field_name = condition.alias_table_name + "." + field_name

        if isinstance(condition.value, Field):
            if condition.value.alias_table_name is not None:
                sql = field_name + self.operator_dict[
                    condition.operator] + condition.value.alias_table_name + "." + condition.value.field_name
            else:
                sql = field_name + \
                    self.operator_dict[condition.operator] + \
                    condition.value.field_name
        else:

            if condition.value is None:
                if condition.operator == CommonDefine.OPERATOR_EQ:
                    sql = field_name+" IS NULL"
                elif condition.operator == CommonDefine.OPERATOR_NEQ:
                    sql = field_name + " IS NOT NULL"
            else:
                param_name = "p" + str(CommonDefine.SQL_PARAMETER_INDEX)
                CommonDefine.SQL_PARAMETER_INDEX += 1
                if condition.operator == CommonDefine.OPERATOR_IN or condition.operator == CommonDefine.OPERATOR_NIN:
                    in_str = ""
                    temp_value = []
                    if isinstance(condition.value, str):
                        temp_value = condition.value.split(',')
                    if isinstance(temp_value, list) or isinstance(temp_value, tuple):
                        in_index = 0
                        for value in temp_value:
                            if value:
                                temp_name = param_name + "_" + in_index
                                in_str = ":"+temp_name + ","
                                param_dict[temp_name] = value
                                CommonDefine.SQL_PARAMETER_INDEX += 1

                    in_str = in_str.strip(',')
                    sql = field_name + " " + \
                        self.operator_dict[condition.operator] + \
                        "(:" + param_name + ")"
                elif (condition.operator == CommonDefine.OPERATOR_FIND_IN_SET):
                    param_dict[param_name] = condition.value
                    sql = self.operator_dict[condition.operator] + \
                        "(:" + param_name + "," + field_name + ")"
                else:
                    param_dict[param_name] = condition.value
                    sql = field_name + " " + \
                        self.operator_dict[condition.operator] + \
                        " :" + param_name

        return {"sql": sql, "param_dict": param_dict}
