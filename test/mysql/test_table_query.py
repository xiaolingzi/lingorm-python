from lingorm import ORM
import pytest
from ..entity.first_table_entity import FirstTableEntity


def test_table_query():
    db = ORM.db("test")
    entity = FirstTableEntity()
    entity.first_name = "table_query_1"
    entity.first_number = 1
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0

    entity = FirstTableEntity()
    entity.first_name = "table_query_2"
    entity.first_number = 2
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0

    t = FirstTableEntity
    where = db.create_where()
    where.add_and(t.first_name.eq("table_query_1"), t.first_number.eq(1))
    where.or_and(t.first_name.eq("table_query_2"), t.first_number.eq(2))
    result = db.table(t).select(t.first_name).where(
        where).order_by(t.id.desc()).find()
    assert result is not None
    assert len(result) == 2
    assert result[0].first_name == "table_query_2"

    result = db.table(t).select(t.first_name).where(
        where).order_by(t.id.desc()).first()
    assert result is not None
    assert result.first_name == "table_query_2"

    result = db.table(t).select(t.first_name).where(
        where).order_by(t.id.desc()).find_page(1, 1)
    assert result is not None
    assert result["total_pages"] == 2
    assert result["data"][0].first_name == "table_query_2"

    result = db.table(t).select(t.first_name).where(
        where).order_by(t.id.desc()).find_count()
    assert result == 2

    affected_rows = db.delete_by(FirstTableEntity, where)
    assert affected_rows == 2


def test_table_group_by():
    db = ORM.db("test")
    entity = FirstTableEntity()
    entity.first_name = "table_group_by"
    entity.first_number = 1
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0

    entity = FirstTableEntity()
    entity.first_name = "table_group_by"
    entity.first_number = 2
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0

    t = FirstTableEntity
    where = db.create_where()
    where.add_and(t.first_name.eq("table_group_by"), t.first_number.ge(1))
    result = db.table(t).select(t.first_name, t.first_number.max().alias("first_number"), t.id.count().alias(
        "num")).where(where).group_by(t.first_name).order_by("num desc").find()
    assert result is not None
    assert len(result) == 1
    assert result[0].first_number == 2

    affected_rows = db.delete_by(FirstTableEntity, where)
    assert affected_rows == 2
