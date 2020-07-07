from lingorm import ORM
import pytest
from ..entity.first_table_entity import FirstTableEntity


def test_insert():
    db = ORM.db("test")
    entity = FirstTableEntity()
    entity.first_name = "00test"
    entity.first_number = 1
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0

    result = db.table(FirstTableEntity).where(FirstTableEntity.id.eq(id)).first()
    assert result is not None
    assert result.first_name == entity.first_name

    entity.id = id
    db.delete(entity)


def test_batch_insert():
    db = ORM.db("test")
    entity_list = []
    for i in range(0, 10):
        entity = FirstTableEntity()
        entity.first_name = "batch"+str(i)
        entity.first_number = i
        entity.first_time = '2020-01-01 00:00:01'
        entity_list.append(entity)
    affected_rows = db.batch_insert(entity_list)
    assert affected_rows == 10

    where = db.create_where().add_and(FirstTableEntity.first_name.like("batch%"))
    db.delete_by(FirstTableEntity, where)
