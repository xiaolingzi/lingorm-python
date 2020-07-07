from abc import ABCMeta, abstractmethod


class TableQueryAbstract:
    _sql = ""
    _param_dict = {}
    _select_sql = ""
    _from_sql = ""
    _where_sql = ""
    _group_sql = ""
    _order_sql = ""
    _limit_sql = ""
    
    @abstractmethod
    def table(self, table):
        pass
    
    @abstractmethod
    def select(self, **kwargs):
        pass

    @abstractmethod
    def where(self, **kwargs):
        pass

    @abstractmethod
    def order_by(self, **kwargs):
        pass

    @abstractmethod
    def group_by(self, **args):
        pass

    @abstractmethod
    def limit(self, top_count):
        pass

    @abstractmethod
    def first(self):
        pass

    @abstractmethod
    def find(self):
        pass

    @abstractmethod
    def find_page(self, page_index, page_size):
        pass

    @abstractmethod
    def find_count(self):
        pass