import time
from lingorm import ORM
import pytest
from ..entity.first_table_entity import FirstTableEntity


def test_update():
    db = ORM.db("test")
    entity = db.table(FirstTableEntity).first()
    entity.first_name = "update name" + str(time.time())
    entity.first_number = 2
    entity.first_time = '2020-02-01 00:00:01'
    affected_rows = db.update(entity)
    assert affected_rows == 1

    result = db.table(FirstTableEntity).where(
        FirstTableEntity.id.eq(entity.id)).first()
    assert result is not None
    assert result.first_name == entity.first_name


def test_batch_update():
    db = ORM.db("test")
    entity_list = db.table(FirstTableEntity).where(
        FirstTableEntity.id.gt(0)).limit(5).find()
    name = "batch update"+str(time.time())
    for entity in entity_list:
        entity.first_name = name
        entity.first_number = 3
        entity.first_time = '2020-03-01 00:00:01'
    affected_rows = db.batch_update(entity_list)
    assert affected_rows == len(entity_list)

    list = db.table(FirstTableEntity).where(
        FirstTableEntity.first_name.eq(name)).find()
    assert len(list) == len(entity_list)


def test_update_by():
    db = ORM.db("test")
    where = db.create_where().add_and(
        FirstTableEntity.first_name.like("%batch%"))
    set_list = [FirstTableEntity.first_name.eq(
        "update by"), FirstTableEntity.first_number.eq(5)]
    affected_rows = db.update_by(FirstTableEntity, set_list, where)
    assert affected_rows > 0
