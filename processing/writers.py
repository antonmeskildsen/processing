import os
import json
import glob
from functools import reduce

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import List

import cv2 as cv
import numpy as np

from processing.operation import Operation


class Writer(Operation):

    def __init__(self, input, path):
        super().__init__()
        self._path = path
        self._input = input

    @property
    def input(self):
        return self._input


class Dumper(Writer):
    """
    Discards any input.
    """
    def run(self):
        while True:
            elem = self.input.get(timeout=1)
            if elem is None:
                return


class SequenceWriter(Writer):

    def __init__(self, input, path, prefix, extension, start_index=0):
        super().__init__(input, path)
        self._prefix = prefix
        self._start_index = start_index
        self._extension = extension

    def run(self):
        if not os.path.exists(self._path):
            os.mkdir(self._path)
        elif not os.path.isdir(self._path):
            raise RuntimeError('Path is not a directory. A Sequence Writer needs a directory to write to.')

        while True:
            elem = self.input.get()
            if elem is None:
                return
            fname = os.path.join(self._path, '{}{}.{}'.format(self._prefix, self._start_index, self._extension))
            self._write(fname, elem)
            self._start_index += 1

    @abstractmethod
    def _write(self, fname, elem):
        pass


class ImageSequenceWriter(SequenceWriter):

    def __init__(self, input, path, prefix, start_index=0, extension='png'):
        super().__init__(input, path, prefix, extension, start_index)

    def _write(self, fname, elem):
        cv.imwrite(fname, elem)


class ArraySequenceWriter(SequenceWriter):

    def __init__(self, input, path, prefix, start_index=0, extension='npy'):
        super().__init__(input, path, prefix, extension, start_index)

    def _write(self, fname, elem):
        np.save(fname, elem)


class TrackFileWriter(Writer):

    def __init__(self, input, path, resolution, track_names=None):
        super().__init__(input, path)
        self._track_names = track_names
        self._types = None
        self._resolution = resolution

    def run(self):
        if os.path.exists(self._path):
            raise RuntimeError('File already exists!')

        elements = []
        while True:
            single = self.input.get()
            if single is None:
                break

            element_data = [elem[1] for elem in single]

            # TODO: Not pretty!!
            if not self._types:
                self._types = [elem[0] for elem in single]

            if not self._track_names:
                self._track_names = [elem[2] for elem in single]

            elements.append(element_data)

        length = len(elements)

        data = np.array(elements).T
        print(data.shape)
        data = data.tolist()

        tracks = []
        for name, type, dat in zip(self._track_names, self._types, data):
            tracks.append({
                'name': name,
                'type': type,
                'data': dat
            })

        total = {
            'length': length,
            'tracks': tracks,
            'video_resolution': {
                'width': self._resolution[1],
                'height': self._resolution[0]
            }
        }

        f = open(self._path, 'w')
        f.write(json.dumps(total))
        f.close()


class ArraySequenceToJsonWriter(Writer):

    def __init__(self, input, base_path, name):
        super().__init__(input, os.path.join(base_path, name))
        self.base_path = base_path

    def run(self):
        if os.path.exists(self._path):
            raise RuntimeError('File already exists!')

        elements = []

        i = 0
        while True:
            elem = self.input.get()
            if elem is None:
                break

            path, label = elem

            elements.append(elem)

            if i % 10000 == 0:
                print(i)

            i += 1

        out = {
            'base_path': self.base_path,
            'len': len(elements),
            'elements': elements
        }

        f = open(self._path, 'w')
        f.write(json.dumps(out))
        f.close()