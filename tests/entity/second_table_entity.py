from lingorm.mapping import *


class SencondTableEntity(ORMEntity):
    __table__ = "second_table"
    __database__ = "test"
    id = Field(field_name="id", field_type="int",
               is_primary=True, is_generated=True)
    second_name = Field(field_name="second_name", field_type="string", length="45")
    second_number = Field(field_name="second_number", field_type="int")
    second_time = Field(field_name="second_time", field_type="datetime")

    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.second_name = kwargs.get("second_name")
        self.second_number = kwargs.get("second_number")
        self.second_time = kwargs.get("second_time")
