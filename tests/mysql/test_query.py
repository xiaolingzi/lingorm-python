from lingorm import ORM
import pytest
from ..entity.first_table_entity import FirstTableEntity


class TestQuery:

    def setup_class(self):
        self.db = ORM.db("test")
        self.db.begin()
        entity_list = []
        for i in range(0, 10):
            entity = FirstTableEntity()
            entity.first_name = "query "+str(i)
            entity.first_number = i
            entity.first_time = '2020-01-01 00:00:01'
            entity_list.append(entity)
        self.db.batch_insert(entity_list)

    def teardown_class(self):
        self.db.rollback()

    def test_find(self):
        t = FirstTableEntity
        where = self.db.create_where()
        where.add_and(t.first_name.eq("query 1"), t.first_number.eq(1))
        where.or_and(t.first_name.eq("query 2"), t.first_number.eq(2))
        order_by = self.db.create_order_by().by(t.id.desc())

        result = self.db.find(t, where, order_by)
        assert result is not None
        assert len(result) == 2
        assert result[0].first_name == "query 2"

    def test_find_top(self):
        t = FirstTableEntity
        where = self.db.create_where()
        where.add_and(t.first_name.eq("query 1"), t.first_number.eq(1))
        where.or_and(t.first_name.eq("query 2"), t.first_number.eq(2))
        order_by = self.db.create_order_by().by(t.id.desc())

        result = self.db.find_top(t, 1, where, order_by)
        assert result is not None
        assert len(result) == 1
        assert result[0].first_name == "query 2"

    def test_find_page(self):
        t = FirstTableEntity
        where = self.db.create_where()
        where.add_and(t.first_name.like("query%"))
        order_by = self.db.create_order_by().by(t.id.desc())

        result = self.db.find_page(t, 1, 2, where, order_by)
        assert result is not None
        assert result["total_pages"] == 5
        assert result["data"][0].first_name == "query 9"

    def test_first(self):
        t = FirstTableEntity
        where = self.db.create_where()
        where.add_and(t.first_name.eq("query 1"), t.first_number.eq(1))
        where.or_and(t.first_name.eq("query 2"), t.first_number.eq(2))
        order_by = self.db.create_order_by().by(t.id.desc())

        result = self.db.first(t, where, order_by)
        assert result is not None
        assert result.first_name == "query 2"

    def test_find_count(self):
        t = FirstTableEntity
        where = self.db.create_where()
        where.add_and(t.first_name.like("query%"))

        result = self.db.find_count(t, where)
        assert result == 10

