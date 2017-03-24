from abc import ABCMeta, abstractmethod


class ORMQueryBuilderAbstract:
    _sql = None
    _param_dict = {}
    _select_sql = None
    _from_sql = None
    _join_sql = None
    _where_sql = None
    _group_sql = None
    _order_sql = None
    _limit_sql = None

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
    def where(self, where_expression):
        pass

    @abstractmethod
    def order_by(self, field, order_type):
        pass

    @abstractmethod
    def group_by(self, **args):
        pass

    @abstractmethod
    def limit(self, top_count):
        pass

    @abstractmethod
    def get_result(self, cls=None):
        pass

    @abstractmethod
    def get_page_result(self, page_index, page_size, cls=None):
        pass
