import numpy as np
from queue import Queue
from typing import Generator
from processing.writers import *


def queue_generator(queue: Queue) -> Generator:
    """
    Create a generator using a blocking queue. This is especially
    useful if you need to interface the processing library with
    some python code expecting Iterables. Another use is simply
    making looping over the queue values prettier.

    :param queue: The queue to create a generator for
    :return: The generator
    """
    while True:
        elem = queue.get()
        if elem is None:
            break
        yield elem


def batch_queue_generator(queue: Queue, shape, batch_size) -> Generator[np.ndarray, np.ndarray, None]:
    buffer = np.zeros((batch_size, *shape))

    i = 0
    while True:
        elem = queue.get()
        if elem is None:
            yield buffer[:i]
            break

        if i < batch_size:
            buffer[i] = elem
        else:
            yield buffer
            buffer = np.zeros((batch_size, *shape))

        i += 1



