import time
from lingorm import ORM
import pytest
from ..entity.first_table_entity import FirstTableEntity


def test_delete():
    db = ORM.db("test")
    entity = FirstTableEntity()
    entity.first_name = "test_delete"
    entity.first_number = 6
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0

    exist_entity = db.table(FirstTableEntity).where(
        FirstTableEntity.id.eq(id)).first()
    result = db.delete(exist_entity)
    assert result == 1


def test_delete_by():
    db = ORM.db("test")
    entity = FirstTableEntity()
    entity.first_name = "test_delete_by"
    entity.first_number = 6
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0
    
    where = db.create_where().add_and(
        FirstTableEntity.first_name.like("%delete_by"))
    affected_rows = db.delete_by(FirstTableEntity, where)
    assert affected_rows == 1
