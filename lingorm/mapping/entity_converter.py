from .field import Field


class EntityConverter:
    @staticmethod
    def dict_to_entity(cls, data_dict):
        result = cls()
        for (key, value) in data_dict.items():
            flag = True
            for (attr_key, attr_value) in cls.__dict__.items():
                if isinstance(attr_value, Field):
                    if attr_value.field_name is None:
                        attr_value.field_name = attr_key
                    if attr_value.field_name == key:
                        temp_value = data_dict[key]
                        result.__setattr__(attr_key, temp_value)
                        flag = False
                        break
            if flag:
                result.__setattr__(key, data_dict[key])
        return result
