from abc import ABCMeta, abstractmethod


class QueryBuilderAbstract:
    _sql = ""
    _param_dict = {}
    _select_sql = ""
    _from_sql = ""
    _join_sql = ""
    _where_sql = ""
    _group_sql = ""
    _order_sql = ""
    _limit_sql = ""

    @abstractmethod
    def select(self, **kwargs):
        pass

    @abstractmethod
    def from_table(self, cls, on_expression):
        pass

    @abstractmethod
    def left_join(self, cls, on_expression):
        pass

    @abstractmethod
    def right_join(self, cls, on_expression):
        pass

    @abstractmethod
    def inner_join(self, cls, on_expression):
        pass

    @abstractmethod
    def where(self, *args):
        pass

    @abstractmethod
    def order_by(self, *args):
        pass

    @abstractmethod
    def group_by(self, **args):
        pass

    @abstractmethod
    def limit(self, top_count):
        pass

    @abstractmethod
    def first(self, cls=None):
        pass

    @abstractmethod
    def find(self, cls=None):
        pass

    @abstractmethod
    def find_page(self, page_index, page_size, cls=None):
        pass

    @abstractmethod
    def find_count(self):
        pass
