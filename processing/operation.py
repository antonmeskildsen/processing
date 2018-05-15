from threading import Thread


class Operation(Thread):
    """
    A subclass of Thread that represents a kind of data operation.
    Operations are meant to be linked together with the queues they
    produce and/or consume, making pipelining common data operations
    easy.

    This superclass is mostly just a placeholder since Thread already
    includes the necessary methods for this to work.
    """

    pass
