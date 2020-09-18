from ..order_by_abstract import OrderByAbstract
from ...mapping.field import Field


class OrderBy(OrderByAbstract):
    sql = None
    __order_name=("ASC", "DESC")

    def by(self, *args):
        order_str = ""
        for val in args:
            if isinstance(val, Field):
                field_name = val.field_name
                if val.alias_table_name is not None:
                    field_name = val.alias_table_name + "." + field_name
                order_str += ","+field_name+ " " + self.__order_name[val.order_by]
            elif isinstance(val, str):
                order_str+= ","+val
                
        order_str = order_str.strip(',')
        if self.sql is None:
            self.sql =  order_str
        else:
            self.sql += "," + order_str
        return self
