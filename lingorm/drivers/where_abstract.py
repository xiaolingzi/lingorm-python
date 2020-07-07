from abc import ABCMeta, abstractmethod


class WhereAbstract:
    @abstractmethod
    def add_and(self, *args):
        pass

    @abstractmethod
    def add_or(self, *args):
        pass

    @abstractmethod
    def get_and(self, *args):
        pass

    @abstractmethod
    def get_or(self, *args):
        pass

    @abstractmethod
    def or_and(self, *args):
        pass

    @abstractmethod
    def and_or(self, *args):
        pass
