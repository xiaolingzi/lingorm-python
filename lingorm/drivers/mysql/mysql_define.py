from ...mapping.field import Field
from ...common.common_define import CommonDefine


class MysqlDefine:
    operator_dict = {
        "eq": "=",
        "neq": "<>",
        "gt": ">",
        "ge": ">=",
        "lt": "<",
        "le": "<=",
        "in": "in",
        "nin": "not in",
        "like": "like"
    }
    order_dict = {
        "asc": "asc",
        "desc": "desc"
    }

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
                sql = field_name + self.operator_dict[condition.operator] + condition.value.field_name
        else:
            param_name = "p" + str(CommonDefine.SQL_PARAMETER_INDEX);
            CommonDefine.SQL_PARAMETER_INDEX += 1
            param_dict[param_name] = condition.value
            if condition.operator == CommonDefine.OPERATOR_IN or condition.operator == CommonDefine.OPERATOR_NIN:
                sql = field_name + " " + self.operator_dict[condition.operator] + "(:" + param_name + ")"
            else:
                sql = field_name + " " + self.operator_dict[condition.operator] + " :" + param_name

        return {"sql": sql, "param_dict": param_dict}
