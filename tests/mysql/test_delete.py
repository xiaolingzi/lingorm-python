import time
from lingorm import ORM
import pytest
from ..entity.first_table_entity import FirstTableEntity


class TestDelete:

    def setup_class(self):
        self.db = ORM.db("test")
        self.db.begin()

    def teardown_class(self):
        self.db.rollback()

    def test_delete(self):
        entity = FirstTableEntity()
        entity.first_name = "test_delete"
        entity.first_number = 6
        entity.first_time = '2020-01-01 00:00:01'
        id = self.db.insert(entity)
        assert id > 0
        exist_entity = self.db.table(FirstTableEntity).where(
            FirstTableEntity.id.eq(id)).first()
        affected_rows = self.db.delete(exist_entity)
        assert affected_rows == 1

    def test_delete_by(self):
        entity = FirstTableEntity()
        entity.first_name = "test_delete_by"
        entity.first_number = 6
        entity.first_time = '2020-01-01 00:00:01'
        id = self.db.insert(entity)
        assert id > 0

        where = self.db.create_where().add_and(
            FirstTableEntity.first_name.like("%delete_by"))
        affected_rows = self.db.delete_by(FirstTableEntity, where)
        assert affected_rows == 1
