import os
import json
import glob
import cv2 as cv
import numpy as np
from queue import Queue
from processing.operation import Operation
from abc import abstractmethod


class Loader(Operation):
    """
    A loader is an entry point for data in a processing pipeline. By reading
    in data from some external source, e.g. a file, it produces a queue of
    elements for other Operations to use.
    """

    def __init__(self, path, output=None):
        """
        Most often a Loader reads from some path and writes to
        a single output queue. This constructor sets up this common
        use case.

        :param path: The path (any kind of object really) to the resources to load.
        :param output: The queue that the loader will fill with loaded elements.
        """
        super().__init__()
        self._path = path
        if output:
            self._output = output
        else:
            self._output = Queue(1000)

    @property
    def output(self) -> Queue:
        """
        The queue that the loader will fill with loaded elements. This is most
        often a queue.
        :return: The output queue
        """
        return self._output


class TrackFileLoader(Loader):

    def __init__(self, path, track_names=None, keep_names=False, skip_pattern=None, stop=None):
        super().__init__(path)
        self._skip_pattern = skip_pattern
        self._stop_steps = stop
        self._track_names = track_names
        self._keep_names = keep_names

    def run(self):
        with open(self._path) as track_file:
            track_file = json.load(track_file)

            if self._track_names:
                track_dict = {track['name']: (track['type'], track['data'])
                              for track in track_file['tracks']}
                track_data_list = [track_dict[name][1] for name in self._track_names]
                track_type_list = [track_dict[name][0] for name in self._track_names]
                track_name_list = self._track_names
            else:
                track_data_list = [track['data'] for track in track_file['tracks']]
                track_type_list = [track['type'] for track in track_file['tracks']]
                track_name_list = [track['name'] for track in track_file['tracks']]

            i = 0
            total = 0
            skip = False
            for data_row in list(zip(*track_data_list)):
                if self._keep_names:
                    out = list(zip(track_type_list, data_row, track_name_list))
                else:
                    out = list(zip(track_type_list, data_row))

                if self._skip_pattern is not None:
                    n_do, n_skip = self._skip_pattern
                    if skip and i > n_skip:
                        skip = False
                        i = 0
                    elif not skip and i > n_do:
                        skip = True
                        i = 0

                    if not skip:
                        self.output.put(out)
                    i += 1
                else:
                    self.output.put(out)

                if self._stop_steps is not None and self._stop_steps < total:
                    break

            self.output.put(None)


class VideoLoader(Loader):

    def __init__(self, path, output=None, skip_pattern=None, stop=None):
        self._skip_pattern = skip_pattern
        self._stop_steps = stop
        super().__init__(path, output)


    def run(self):
        video = cv.VideoCapture(self._path)

        i = 0
        total = 0
        skip = False
        while True:
            ret, frame = video.read()
            if not ret:
                self.output.put(None)
                return

            if self._skip_pattern is not None:
                n_do, n_skip = self._skip_pattern
                if skip and i > n_skip:
                    skip = False
                    i = 0
                elif not skip and i > n_do:
                    skip = True
                    i = 0

                if not skip:
                    self.output.put(frame)
                i += 1
            else:
                self.output.put(frame)

            if self._stop_steps is not None and self._stop_steps < total:
                self.output.put(None)
                return

            total += 1


class SequenceLoader(Loader):

    def __init__(self, path, pattern, output=None, loop=False):
        super().__init__(path, output)
        self._pattern = pattern
        self._loop = loop

    @abstractmethod
    def _load(self, fname):
        pass

    def run(self):
        if not os.path.exists(self._path):
            raise IOError("Can't find specified path")

        def inner():
            complete = os.path.join(self._path, self._pattern)
            # TODO: implement proper sorting
            for fname in sorted(glob.glob(complete), key=os.path.basename):
                self._load(fname)

        if self._loop:
            while True:
                inner()
        else:
            inner()
            self.output.put(None)


class FileNameLoader(SequenceLoader):
    """
    Passes files found by SequenceLoader directly to output.
    """

    def _load(self, fname):
        self.output.put(fname)


class ArraySequenceLoader(SequenceLoader):

    def _load(self, fname):
        arr = np.load(fname)
        self.output.put(arr)


class ImageSequenceLoader(SequenceLoader):

    def _load(self, fname):
        image = cv.imread(fname)
        self.output.put(image)
