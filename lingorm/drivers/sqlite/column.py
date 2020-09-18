from ...mapping.field import Field


class Column:
    def select(self, args):
        select_sql = ""
        if args is None or len(args) < 1:
            select_sql = "*"
            return self

        for column in args:
            field_str = column
            if isinstance(column, Field):
                field_str = column.field_name
                if column.alias_table_name:
                    field_str = column.alias_table_name + "." + field_str
                if column.is_distinct:
                    field_str = "DISTINCT " + field_str
                if column.column_funcs:
                    for func in column.column_funcs:
                        field_str = func+"(" + field_str + ")"
                if column.alias_field_name:
                    field_str = field_str + " AS " + column.alias_field_name
            elif isinstance(column, object) and hasattr(column, "__alias_table_name__"):
                field_str = column.__alias_table_name__ + ".*"

            if not select_sql:
                select_sql = field_str
            else:
                select_sql += "," + field_str
        return select_sql
