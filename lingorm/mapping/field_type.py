import datetime, time


class FieldType:
    @staticmethod
    def get_type_value(value, field_type):
        field_type = field_type.lower()
        result = value
        if field_type == "string":
            result = str(value)
        elif field_type == "int" or field_type == "integer":
            result = int(value)
        elif field_type == "datetime" or field_type == "date" or field_type == "time":
            result = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        elif field_type == "float" or field_type == "double" or field_type == "decimal":
            result = float(value)
        else:
            result = value
        return result

    @staticmethod
    def get_field_value(entity, property_name, field_type):
        field_type = field_type.lower()
        val = entity.__getattribute__(property_name)
        if field_type == "datetime":
            if type(val) == datetime.datetime:
                return val.strftime("%Y-%m-%d %H:%M:%S")
            elif type(val) == time.struct_time:
                return time.strftime("%Y-%m-%d %H:%M:%S", val)
            elif type(val) == float or type(val) == int:
                return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(val))
            else:
                raise Exception("Invalid datetime value")
        return val
