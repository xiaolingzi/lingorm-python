from abc import ABCMeta, abstractmethod


class OrderExpressionAbstract:
    @abstractmethod
    def order_by(self, field, order):
        pass

