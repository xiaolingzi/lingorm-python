from .field import Field
from ..common.common_define import CommonDefine


class ClsMetaclass(type):
    def __new__(mcs, name, bases, dct):
        if "__table__" in dct and "__alias_table_name__" not in dct:
            dct["__alias_table_name__"] = "t" + str(CommonDefine.TABLE_INDEX)
            CommonDefine.TABLE_INDEX += 1
            field_dict = {}
            for (key, value) in dct.items():
                if isinstance(value, Field):
                    value.table_name = dct.get("__table__", None)
                    value.alias_table_name = dct["__alias_table_name__"]
                    if not hasattr(value, "field_name") or value.field_name is None:
                        value.field_name = key
                    if not hasattr(value, "field_type") or value.field_type is None:
                        value.field_type = "string"
                    dct[key] = value
                    field_dict[key] = value
            dct["__field_dict__"] = field_dict
        return super(ClsMetaclass, mcs).__new__(mcs, name, bases, dct)

    def __init__(cls, name, bases, dct):
        super(ClsMetaclass, cls).__init__(name, bases, dct)


class ORMEntity(metaclass=ClsMetaclass):
    pass
