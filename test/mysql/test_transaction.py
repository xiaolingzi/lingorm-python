from lingorm import ORM
import pytest
from ..entity.first_table_entity import FirstTableEntity


def test_transaction_commit():
    db = ORM.db("test")

    db.begin()
    entity = FirstTableEntity()
    entity.first_name = "00test"
    entity.first_number = 1
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0
    db.commit()

    result = db.table(FirstTableEntity).where(FirstTableEntity.id.eq(id)).first()
    assert result is not None
    assert result.first_name == entity.first_name

    entity.id = id
    db.delete(entity)

def test_transaction_rollback():
    db = ORM.db("test")

    db.begin()
    entity = FirstTableEntity()
    entity.first_name = "00test"
    entity.first_number = 1
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0
    db.rollback()

    result = db.table(FirstTableEntity).where(FirstTableEntity.id.eq(id)).first()
    assert result is None