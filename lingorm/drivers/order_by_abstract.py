from abc import ABCMeta, abstractmethod


class OrderByAbstract:
    @abstractmethod
    def by(self, *args):
        pass

