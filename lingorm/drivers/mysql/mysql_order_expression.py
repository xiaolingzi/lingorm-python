from ...drivers.order_expression_abstract import OrderExpressionAbstract
from ...drivers.mysql.mysql_define import MysqlDefine
from ...mapping.field import Field


class MysqlOrderExpression(OrderExpressionAbstract):
    sql = None

    def order_by(self, field, order):
        if field is None or order is None:
            raise Exception("parameter is not valid")
        if not isinstance(field, Field):
            raise Exception("The field parameter is not valid")
        order = order
        if order not in MysqlDefine.order_dict:
            raise Exception("The order string is not valid")

        field_name = field.field_name
        if field.alias_table_name is not None:
            field_name = field.alias_table_name + "." + field_name

        order_str = MysqlDefine.order_dict[order]
        if self.sql is None:
            self.sql = field_name + " " + order_str
        else:
            self.sql += "," + field_name + " " + order_str
        return self
