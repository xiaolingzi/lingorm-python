from lingorm import ORM
import pytest
from ..entity.first_table_entity import FirstTableEntity


def test_table_query():
    db = ORM.db("test")
    entity = FirstTableEntity()
    entity.first_name = "query_1"
    entity.first_number = 1
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0

    entity = FirstTableEntity()
    entity.first_name = "query_2"
    entity.first_number = 2
    entity.first_time = '2020-01-01 00:00:01'
    id = db.insert(entity)
    assert id > 0

    t = FirstTableEntity
    where = db.create_where()
    where.add_and(t.first_name.eq("query_1"), t.first_number.eq(1))
    where.or_and(t.first_name.eq("query_2"), t.first_number.eq(2))
    order_by = db.create_order_by().by(t.id.desc())

    result = db.find(t, where, order_by)
    assert result is not None
    assert len(result) == 2
    assert result[0].first_name == "query_2"

    result = db.find_top(t, 1, where, order_by)
    assert result is not None
    assert len(result) == 1
    assert result[0].first_name == "query_2"

    result = db.first(t, where, order_by)
    assert result is not None
    assert result.first_name == "query_2"

    result = db.find_page(t, 1, 1, where, order_by)
    assert result is not None
    assert result["total_pages"] == 2
    assert result["data"][0].first_name == "query_2"

    result = db.find_count(t, where)
    assert result == 2

    affected_rows = db.delete_by(FirstTableEntity, where)
    assert affected_rows == 2
