from lingorm import ORM
import pytest
from .entity.first_table_entity import FirstTableEntity
from .entity.second_table_entity import SencondTableEntity


class TestQueryBuilder:

    def setup_class(self):
        self.db = ORM.db("test")
        self.db.begin()
        first_list = []
        for i in range(0, 10):
            entity = FirstTableEntity()
            entity.first_name = "query "+str(i)
            entity.first_number = i
            entity.first_time = '2020-01-01 00:00:01'
            first_list.append(entity)
        self.db.batch_insert(first_list)

        second_list = []
        for i in range(0, 10):
            entity = SencondTableEntity()
            entity.second_name = "query "+str(i)
            entity.second_number = i
            entity.second_time = '2020-01-01 00:00:01'
            second_list.append(entity)
        self.db.batch_insert(second_list)

    def teardown_class(self):
        self.db.rollback()

    def test_find(self):
        f = FirstTableEntity
        s = SencondTableEntity
        where = self.db.create_where()
        where.add_and(f.first_name.eq("query 1"), f.first_number.eq(1))
        where.or_and(f.first_name.eq("query 2"), f.first_number.eq(2))
        builder = self.db.query_builder()
        builder = builder.select(f.first_name, s.second_name).from_table(f).left_join(
            s, f.first_number.eq(s.second_number)).where(where).order_by(f.id.desc())

        result = builder.find(f)
        assert result is not None
        assert len(result) == 2
        assert result[0].second_name == "query 2"

    def test_find_page(self):
        f = FirstTableEntity
        s = SencondTableEntity
        where = self.db.create_where()
        where.add_and(f.first_name.like("query%"))
        builder = self.db.query_builder()
        builder = builder.select(f.first_number, f.first_name.i().max().alias("first_name"), s.second_name.i().f("MAX").alias("second_name")).from_table(f).right_join(
            s, f.first_number.eq(s.second_number))
        if not builder:
            return
        builder = builder.where(where).group_by(f.first_number).order_by(f.first_number.desc())

        result = builder.find_page(1,2,f)
        assert result is not None
        assert result["total_pages"] == 5
        assert result["data"][0].second_name == "query 9"

    def test_first(self):
        f = FirstTableEntity
        s = SencondTableEntity
        where = self.db.create_where()
        where.add_and(f.first_name.eq("query 1"), f.first_number.eq(1))
        where.or_and(f.first_name.eq("query 2"), f.first_number.eq(2))
        builder = self.db.query_builder()
        builder = builder.select(f.first_name, s.second_name).from_table(f).inner_join(
            s, f.first_number.eq(s.second_number)).where(where).order_by(f.id.desc())

        result = builder.first(f)
        assert result is not None
        assert result.second_name == "query 2"

    def test_find_count(self):
        f = FirstTableEntity
        s = SencondTableEntity
        where = self.db.create_where()
        where.add_and(f.first_name.eq("query 1"), f.first_number.eq(1))
        where.or_and(f.first_name.eq("query 2"), f.first_number.eq(2))
        builder = self.db.query_builder()
        builder = builder.select(f.first_name, s.second_name).from_table(f).left_join(
            s, f.first_number.eq(s.second_number)).where(where).order_by(f.id.desc())

        result = builder.find_count()
        assert result == 2
