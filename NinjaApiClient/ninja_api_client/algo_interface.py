from abc import ABC, abstractmethod


class Algo(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def warmup(self):
        pass

    @abstractmethod
    def run(self):
        pass
