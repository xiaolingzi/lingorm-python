from lingorm.mapping import *


class FirstTableEntity(ORMEntity):
    __table__ = "first_table"
    __database__ = "test"
    id = Field(field_name="id", field_type="int",
               is_primary=True, is_generated=True)
    first_name = Field(field_name="first_name", field_type="string", length="45")
    first_number = Field(field_name="first_number", field_type="int")
    first_time = Field(field_name="first_time", field_type="datetime")

    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.first_name = kwargs.get("first_name")
        self.first_number = kwargs.get("first_number")
        self.first_time = kwargs.get("first_time")
