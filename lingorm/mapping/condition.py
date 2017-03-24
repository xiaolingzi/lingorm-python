class Condition:
    def __init__(self, **kwargs):
        self.alias_table_name = kwargs.get("alias_table_name")
        self.table_name = kwargs.get("table_name")
        self.field_name = kwargs.get("field_name")
        self.operator = kwargs.get("operator")
        self.value = kwargs.get("value")


