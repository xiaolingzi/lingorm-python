from ..common.common_define import CommonDefine
from .condition import Condition


class Field:
    def __init__(self, **kwargs):
        self.field_type = kwargs.get("field_type")
        self.field_length = kwargs.get("length")
        self.field_name = kwargs.get("field_name")
        self.default_value = kwargs.get("default_value")
        self.primary_key = kwargs.get("primary_key", False)
        self.is_generated = kwargs.get("is_generated", False)
        self.table_name = kwargs.get("table_name")
        self.alias_table_name = kwargs.get("alias_table_name")
        self.alias_field_name = kwargs.get("alias_field_name")

        self.column_funcs = []
        self.order_by = 0
        self.is_distinct = False

    def count(self):
        self.column_funcs.append("COUNT")
        return self

    def sum(self):
        self.column_funcs.append("SUM")
        return self

    def min(self):
        self.column_funcs.append("MIN")
        return self

    def max(self):
        self.column_funcs.append("MAX")
        return self

    def f(self, func):
        self.column_funcs.append(func)
        return self

    def distinct(self):
        self.is_distinct = True
        return self

    def alias(self, alias_name):
        self.alias_field_name = alias_name
        return self

    def eq(self, val):
        return self.get_condition(val, CommonDefine.OPERATOR_EQ)

    def neq(self, val):
        return self.get_condition(val, CommonDefine.OPERATOR_NEQ)

    def ge(self, val):
        return self.get_condition(val, CommonDefine.OPERATOR_GE)

    def gt(self, val):
        return self.get_condition(val, CommonDefine.OPERATOR_GT)

    def le(self, val):
        return self.get_condition(val, CommonDefine.OPERATOR_LE)

    def lt(self, val):
        return self.get_condition(val, CommonDefine.OPERATOR_LT)

    def in_(self, val):
        return self.get_condition(val, CommonDefine.OPERATOR_IN)

    def nin(self, val):
        return self.get_condition(val, CommonDefine.OPERATOR_NIN)

    def like(self, val):
        return self.get_condition(val, CommonDefine.OPERATOR_LIKE)

    def fis(self, val):
        return self.get_condition(val, CommonDefine.OPERATOR_FIND_IN_SET)

    def asc(self):
        self.order_by=0
        return self

    def desc(self):
        self.order_by=1
        return self

    def get_condition(self, val, operator):
        result = Condition()
        result.alias_table_name = self.alias_table_name
        result.table_name = self.table_name
        result.field_name = self.field_name
        result.operator = operator
        result.value = val
        return result
