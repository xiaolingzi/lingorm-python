from abc import ABCMeta, abstractmethod


class ORMQueryAbstract:
    @abstractmethod
    def create_builder(self):
        pass

    @abstractmethod
    def fetch_one(self, cls, where_condition, order_condition=None):
        pass

    @abstractmethod
    def fetch_all(self, cls, where_condition, order_condition=None, top=0):
        pass

    @abstractmethod
    def insert(self, entity):
        pass

    @abstractmethod
    def batch_insert(self, entity_list):
        pass

    @abstractmethod
    def update(self, entity):
        pass

    @abstractmethod
    def batch_update(self, entity_list):
        pass

    @abstractmethod
    def update_by(self, cls, set_dict, where_condition):
        pass

    @abstractmethod
    def delete(self, entity):
        pass

    @abstractmethod
    def delete_by(self, cls, where_condition):
        pass
