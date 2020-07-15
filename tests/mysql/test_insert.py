from lingorm import ORM
import pytest
import time
from ..entity.first_table_entity import FirstTableEntity

class TestInsert:

    def setup_class(self):
        self.db = ORM.db("test")
        self.db.begin()

    def teardown_class(self):
        self.db.rollback()

    def test_insert(self):
        entity = FirstTableEntity()
        entity.first_name = "my test"
        entity.first_number = 1
        entity.first_time = time.time()
        id = self.db.insert(entity)
        assert id > 0

        result = self.db.table(FirstTableEntity).where(FirstTableEntity.id.eq(id)).first()
        assert result is not None
        assert result.first_name == entity.first_name


    def test_batch_insert(self):
        entity_list = []
        for i in range(0, 10):
            entity = FirstTableEntity()
            entity.first_name = "batch"+str(i)
            entity.first_number = i
            entity.first_time = '2020-01-01 00:00:01'
            entity_list.append(entity)
        affected_rows = self.db.batch_insert(entity_list)
        assert affected_rows == 10
