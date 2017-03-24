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
        self.is_count = kwargs.get("is_count", False)
        self.is_sum = kwargs.get("is_sum", False)
        self.is_distinct = kwargs.get("is_distinct", False)

    def count(self):
        self.is_count = True
        return self

    def sum(self):
        self.is_sum = True
        return self

    def distinct(self):
        self.is_distinct = True
        return self

    def alias(self, alias_name):
        self.alias_field_name = alias_name
        return self;

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

    def get_condition(self, val, operator):
        result = Condition()
        result.alias_table_name = self.alias_table_name
        result.table_name = self.table_name
        result.field_name = self.field_name
        result.operator = operator
        result.value = val
        return result
