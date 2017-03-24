from abc import ABCMeta, abstractmethod


class WhereExpressionAbstract:
    @abstractmethod
    def set_and(self, *args):
        pass

    @abstractmethod
    def set_or(self, *args):
        pass
