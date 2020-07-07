from abc import ABCMeta, abstractmethod


class QueryAbstract:
    @abstractmethod
    def first(self, cls, where, order_by=None):
        pass

    @abstractmethod
    def find(self, cls, where, order_by=None, top=0):
        pass

    @abstractmethod
    def find_page(self, cls, page_index, page_size, where, order_by=None):
        pass

    @abstractmethod
    def find_count(self, cls, where):
        pass

    @abstractmethod
    def find_top(self, cls, limit, where, order_by=None):
        pass

    @abstractmethod
    def insert(self, entity):
        pass

    @abstractmethod
    def batch_insert(self, entity_list, none_ignore=False):
        pass

    @abstractmethod
    def update(self, entity, none_ignore=False):
        pass

    @abstractmethod
    def batch_update(self, entity_list, none_ignore=False):
        pass

    @abstractmethod
    def update_by(self, cls, set_dict, where):
        pass

    @abstractmethod
    def delete(self, entity):
        pass

    @abstractmethod
    def delete_by(self, cls, where):
        pass

    @abstractmethod
    def create_query_builder(self):
        pass

    @abstractmethod
    def create_native(self):
        pass

    @abstractmethod
    def create_where(self):
        pass

    @abstractmethod
    def create_order_by(self):
        pass
