import time
from lingorm import ORM
import pytest
from .entity.first_table_entity import FirstTableEntity


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

    def test_update(self):
        entity = self.db.table(FirstTableEntity).first()
        entity.first_name = "update name" + str(time.time())
        entity.first_number = 2
        entity.first_time = '2020-02-01 00:00:01'
        affected_rows = self.db.update(entity)
        assert affected_rows == 1

        result = self.db.table(FirstTableEntity).where(
            FirstTableEntity.id.eq(entity.id)).first()
        assert result is not None
        assert result.first_name == entity.first_name


    def test_batch_update(self):
        entity_list = self.db.table(FirstTableEntity).where(
            FirstTableEntity.id.gt(0)).limit(5).find()
        name = "batch update"+str(time.time())
        for entity in entity_list:
            entity.first_name = name
            entity.first_number = 3
            entity.first_time = '2020-03-01 00:00:01'
        affected_rows = self.db.batch_update(entity_list)
        assert affected_rows == len(entity_list)

        list = self.db.table(FirstTableEntity).where(
            FirstTableEntity.first_name.eq(name)).find()
        assert len(list) == len(entity_list)


    def test_update_by(self):
        where = self.db.create_where().add_and(
            FirstTableEntity.first_name.like("query%"))
        set_list = [FirstTableEntity.first_name.eq(
            "update by"), FirstTableEntity.first_number.eq(5)]
        affected_rows = self.db.update_by(FirstTableEntity, set_list, where)
        assert affected_rows > 0
