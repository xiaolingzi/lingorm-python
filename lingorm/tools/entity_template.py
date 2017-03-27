# from lingorm.mapping import *
#
#
# class {{class_name}}Entity(ORMEntity):
#     __table__ = "{{table_name}}"
#     __database__ = "{{database_name}}"
#     <--column-->
#     {{property_name}} = Field({{column_property}})
#     <--column-->
#     def __init__(self, **kwargs):
#         <--column_init-->
#         self.{{property_name}} = kwargs.get("{{property_name}}")
#         <--column_init-->