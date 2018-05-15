import itertools
from queue import Queue
from typing import Tuple

import numpy as np

from processing.operation import Operation


class Transformer(Operation):
    """
    Transformers apply a transformation to the input data and outputs
    the result. Although Python provides no way of ensuring this, all
    transformers should be pure functions.
    """

    def __init__(self, input, output=None):
        """
        The standard constructor for transformers simply assigns the
        input and output.
        :param input: input queue
        :param output: output queue
        """
        super().__init__()
        self._input = input
        if output is None:
            self._output = Queue(1000)
        else:
            self._output = output

    @property
    def input(self):
        return self._input

    @property
    def output(self):
        return self._output


class Zip(Transformer):
    """
    Equivalent to performing a zip operation on a sequence of iterables.
    """
    def __init__(self, *inputs):
        super().__init__(inputs, Queue(1000))

    def run(self):
        while True:
            elems = [input.get() for input in self.input]
            done = 0
            for elem in elems:
                if elem is None:
                    done += 1

            if len(elems) == done:
                self.output.put(None)
                return
            elif done > 0:
                raise BlockingIOError('One input queue sent shutdown signal before the other')

            self.output.put(tuple(elems))


class Split(Transformer):

    def __init__(self, input, split: Tuple[int, ...]):
        output = [Queue(1000) for _ in range(len(split))]
        super().__init__(input, output)
        self._splitting = list(zip(self.output, split, [0] + list(itertools.accumulate(split))[:-1]))

    def run(self):
        i = 0
        while True:
            elem = self.input.get()
            if elem is None:
                for out in self.output:
                    out.put(None)
                return

            for q, split_size, base in self._splitting:
                if base <= i < base+split_size:
                    q.put(elem)

            i = (i+1) % 100


class ReshapeArray(Transformer):

    def __init__(self, input, new_shape):
        super().__init__(input, output=Queue(1000))
        self._new_shape = new_shape

    def run(self):
        while True:
            array = self.input.get(timeout=1)
            if array is None:
                self.output.put(None)
                return
            array = np.reshape(array, self._new_shape)
            self.output.put(array)


class Duplicate(Transformer):
    """
    Duplicates every item from its input queue. This is useful if the
    output of another operation is going to be used in multiple places.
    """

    def __init__(self, input, n=2):
        super().__init__(input, [Queue(10000) for _ in range(n)])

    def nth(self, n):
        return self.output[n]

    def run(self):
        while True:
            elem = self.input.get()
            if elem is None:
                for out in self.output:
                    out.put(None)
                return

            for out in self.output:
                out.put(elem)


class SplitPredicate(Transformer):
    """
    Directs the input elements towards one of two outputs depending on the result
    of evaluating a provided predicate on the input. The input elements themselves
    are tuples containing first the value to evaluate the predicate with and then the data
    itself.
    """

    def __init__(self, input, pred):
        self._true_out = Queue(1000)
        self._false_out = Queue(1000)
        super().__init__(input, (self._true_out, self._false_out))
        self._pred = pred

    @property
    def positive(self):
        return self._true_out

    @property
    def negative(self):
        return self._false_out

    def run(self):
        while True:
            elem = self.input.get(timeout=1)
            if elem is None:
                for out in self.output:
                    out.put(None)
                return

            measure, item = elem
            if self._pred(measure):
                self.positive.put(item)
            else:
                self.negative.put(item)