from abc import ABCMeta, abstractmethod


class NativeQueryAbstract:
    @abstractmethod
    def execute(self, sql, param_dict):
        pass

    @abstractmethod
    def first(self, sql, param_dict, cls=None):
        pass

    @abstractmethod
    def find(self, sql, param_dict, cls=None):
        pass

    @abstractmethod
    def find_page(self, sql, param_dict, page_index, page_size, cls=None):
        pass

    @abstractmethod
    def find_count(self, sql, param_dict):
        pass