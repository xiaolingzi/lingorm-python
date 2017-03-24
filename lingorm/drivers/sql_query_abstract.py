from abc import ABCMeta, abstractmethod


class SqlQueryAbstract:
    @abstractmethod
    def execute(self, sql, param_dict):
        pass

    @abstractmethod
    def get_result(self, sql, param_dict, cls=None):
        pass

    @abstractmethod
    def get_page_result(self, sql, param_dict, page_index, page_size, cls=None):
        pass

