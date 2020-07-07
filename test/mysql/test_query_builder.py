from lingorm import ORM
import pytest
from ..entity.first_table_entity import FirstTableEntity
from ..entity.second_table_entity import SencondTableEntity


def test_query_builder():
    db = ORM.db("test")
    entity = FirstTableEntity()
    entity.first_name = "query_builder_1"
    entity.first_number = 1
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0

    entity = FirstTableEntity()
    entity.first_name = "query_builder_2"
    entity.first_number = 2
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0

    t = FirstTableEntity
    where = db.create_where()
    where.add_and(t.first_name.eq("query_builder_1"), t.first_number.eq(1))
    where.or_and(t.first_name.eq("query_builder_2"), t.first_number.eq(2))
    builder = db.create_query_builder()
    builder = builder.select(t.first_name).from_table(
        t).where(where).order_by(t.id.desc())

    result = builder.find(t)
    assert result is not None
    assert len(result) == 2
    assert result[0].first_name == "query_builder_2"

    result = builder.first(t)
    assert result is not None
    assert result.first_name == "query_builder_2"

    result = builder.find_page(1, 1, t)
    assert result is not None
    assert result["total_pages"] == 2
    assert result["data"][0].first_name == "query_builder_2"

    result = builder.find_count()
    assert result == 2

    affected_rows = db.delete_by(FirstTableEntity, where)
    assert affected_rows == 2


def test_join_builder():
    db = ORM.db("test")
    entity = FirstTableEntity()
    entity.first_name = "join_builder_1"
    entity.first_number = 1
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0

    entity = SencondTableEntity()
    entity.second_name = "second_join_builder_2"
    entity.second_number = 1
    entity.second_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0

    t = FirstTableEntity
    where = db.create_where()
    where.add_and(t.first_name.eq("join_builder_1"), t.first_number.eq(1))
    builder = db.create_query_builder()
    builder = builder.select(t, SencondTableEntity).from_table(t).left_join(
        SencondTableEntity, t.first_number.eq(SencondTableEntity.second_number)).where(where).order_by(t.id.desc())

    result = builder.find()
    assert result is not None
    assert len(result) > 0
    assert "second_name" in result[0]
    assert result[0]["second_name"] == "second_join_builder_2"

    on_where = db.create_where()
    on_where.add_and(t.first_number.eq(SencondTableEntity.second_number), SencondTableEntity.second_name.eq("second_join_builder_2"))
    builder = db.create_query_builder()
    builder = builder.select(t, SencondTableEntity).from_table(t).left_join(
        SencondTableEntity, on_where).where(where).order_by(t.id.desc())

    result = builder.find()
    assert result is not None
    assert len(result) > 0
    assert "second_name" in result[0]
    assert result[0]["second_name"] == "second_join_builder_2"
