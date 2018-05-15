import math
import random
from queue import Queue

import cv2 as cv
import numpy as np

from processing.transformers import Transformer


class ConvertColorSpace(Transformer):
    """
    Changes the color space of input images.
    """

    def __init__(self, input, opencv_converter_code):
        """
        Sets up the transformer with the desired convertion code.
        :param input: input queue
        :param opencv_converter_code: color convertion code for the OpenCV function cvtColor
        """
        super().__init__(input, Queue(1000))
        self._code = opencv_converter_code

    def run(self):
        while True:
            elem = self.input.get(timeout=1)
            if elem is None:
                self.output.put(None)
                return
            out = cv.cvtColor(elem, self._code)
            self.output.put(out)


class Resize(Transformer):

    def __init__(self, input, new_size):
        super().__init__(input, Queue(1000))
        self._new_size = new_size

    def run(self):
        while True:
            elem = self.input.get()
            if elem is None:
                self.output.put(None)
                return

            out = cv.resize(elem, self._new_size)
            self.output.put(out)


class PositionMapGenerator(Transformer):

    def __init__(self, input, size, scaling):
        super().__init__(input, Queue(1000))
        self._size = size
        self._scaling = scaling

    def run(self):
        while True:
            elem = self.input.get(timeout=1)
            if elem is None:
                self.output.put(None)
                return

            map = np.zeros(self._size, dtype=np.float64)

            for _, data in elem:
                y, x = (data['y']+data['height']/2)*self._scaling[0], (data['x']+data['width']/2)*self._scaling[1]
                map[int(y), int(x)] = 1

            self.output.put(map)


class RegionExtractor(Transformer):

    def __init__(self, image_input, regions_input, padding=0):
        super().__init__((image_input, regions_input), Queue(1000))
        self._padding = padding

    @property
    def image_input(self):
        return self.input[0]

    @property
    def regions_input(self):
        return self.input[1]

    def run(self):
        while True:
            image = self.image_input.get(timeout=1)
            regions = self.regions_input.get(timeout=1)
            if image is None and regions is None:
                self.output.put(None)
                return
            elif image is None or regions is None:
                raise BlockingIOError('One input queue sent shutdown signal before the other')

            out = []
            for type, data in regions:
                if type != 'rectangle_region':
                    raise ValueError('Unexpected track type! RegionExtractor can only handle regions of type: '
                                     'rectangle_region')

                y = int(data['y'])
                x = int(data['x'])
                height = int(data['height'])
                width = int(data['width'])

                window = image[y:y+height+1, x:x+width+1]
                out.append(((y, x), window))

            self.output.put(out)


class RandomNegativeWindowGenerator(Transformer):

    def __init__(self, image_input, centers_input, window_size, positive_radius, n):
        super().__init__((image_input, centers_input), Queue(1000))
        self._window_size = window_size
        self._positive_radius = positive_radius
        self._n = n

    @property
    def image_input(self):
        return self.input[0]

    @property
    def centers_input(self):
        return self.input[1]

    def run(self):
        while True:
            image = self.image_input.get()
            centers = self.centers_input.get()
            if image is None and centers is None:
                self.output.put(None)
                return
            elif image is None or centers is None:
                raise BlockingIOError('One input queue sent shutdown signal before the other')

            current = 0
            while current < self._n:
                # Find random window coordinates
                y = random.randint(0, image.shape[0]-self._window_size[0])
                x = random.randint(0, image.shape[1]-self._window_size[1])

                # Get center coordinates
                cy = y + self._window_size[0]//2
                cx = x + self._window_size[1]//2

                # Check for overlap with positive centers
                is_negative = True
                for tcy, tcx in centers:
                    if tcy == 0 and tcx == 0:
                        continue
                    if abs(tcy-cy) <= self._positive_radius and abs(tcx-cx) <= self._positive_radius:
                        is_negative = False

                if is_negative:
                    # Create window slice
                    out = image[y:y+self._window_size[0], x:x+self._window_size[1]]
                    self.output.put(out)
                    current += 1


class PositiveWindowGenerator(Transformer):

    def __init__(self, image_input, centers_input, window_size, radius, max_n=None):
        super().__init__((image_input, centers_input), Queue(1000))
        self._window_size = window_size
        self._radius = radius
        self._max_n = max_n

    @property
    def image_input(self):
        return self.input[0]

    @property
    def centers_input(self):
        return self.input[1]

    def run(self):
        i=0
        while True:
            image = self.image_input.get(timeout=5)
            centers = self.centers_input.get(timeout=5)

            if image is None and centers is None:
                self.output.put(None)
                return
            elif image is None or centers is None:
                print(i)
                raise BlockingIOError('One input queue sent shutdown signal before the other. Maybe the lengths of '
                                      'the inputs don\'t match?')
            i+=1
            for tcy, tcx in centers:
                if tcy == 0 and tcx == 0:
                    continue

                if tcy - self._radius - self._window_size[0]/2 < 0 \
                        or tcy + self._radius + self._window_size[0]/2 > image.shape[0] \
                        or tcx - self._radius + self._window_size[1]/2 < 0 \
                        or tcx + self._radius + self._window_size[1]/2 > image.shape[1]:
                    #print(tcy+self._radius+self._window_size[0]/2, tcx+self._radius+self._window_size[1]/2)
                    raise ValueError('Selected features and radius results in window moving outside image boundaries!')

                tcy = int(tcy)-self._window_size[0]//2
                tcx = int(tcx)-self._window_size[0]//2
                outlist = []
                for y in range(tcy-self._radius, tcy+self._radius+1):
                    for x in range(tcx-self._radius, tcx+self._radius+1):
                        out = image[y:y+self._window_size[0], x:x+self._window_size[1]]
                        outlist.append(out)

                if self._max_n is not None:
                    random.shuffle(outlist)
                    if self._max_n < len(outlist):
                        outlist = outlist[:self._max_n]

                for out in outlist:
                    self.output.put(out)


class WindowGenerator(Transformer):

    def __init__(self, video_input, track_input, window_size, scaling, stride=1, radius=None):
        super().__init__((video_input, track_input), Queue(1000))
        self._video_input = video_input
        self._track_input = track_input
        self._window_size = window_size
        self._stride = stride
        self._radius = radius
        self._scaling = scaling

    def _get_window_config(self, type, data):
        """
        Calculate region size of window as well as its center position.

        :param type: track type for input
        :param data: actual track data
        :return: region size, position
        """
        region_size = self._window_size
        pos = (0, 0)
        if type == 'rectangle_region':
            region_size = (int(data['height']*self._scaling[0]), int(data['width']*self._scaling[1]))
            pos = (data['y']*self._scaling[0]+region_size[0]//2, data['x']*self._scaling[0]+region_size[1]//2)
        elif type == 'inscribed_circle':
            pos = (data['cy']*self._scaling[0], data['cx']*self._scaling[0])
        elif type == 'point':
            pos = (data['y']*self._scaling[0], data['x']*self._scaling[0])

        return region_size, pos

    def run(self):
        while True:
            vid_elem = self._video_input.get(timeout=1)
            track_elem = self._track_input.get(timeout=1)

            if vid_elem is None and track_elem is None:
                self.output.put(None)
                return
            type, data = track_elem[0]  # only use first element of tracks (TODO: maybe error handling?)

            region_size, pos = self._get_window_config(type, data)
            # Check image boundaries
            if self._radius:
                min_y = max(0, pos[0] - self._radius - region_size[0]//2)
                min_x = max(0, pos[1] - self._radius - region_size[1]//2)
                max_y = min(vid_elem.shape[0] - region_size[0], pos[0] + self._radius - region_size[0]//2)
                max_x = min(vid_elem.shape[1] - region_size[1], pos[1] + self._radius - region_size[1]//2)
            else:
                min_y, min_x = 0, 0
                max_y, max_x = vid_elem.shape[0] - region_size[0], vid_elem.shape[1] - region_size[1]

            # Create windows
            for y in range(int(min_y), int(max_y), self._stride):
                for x in range(int(min_x), int(max_x), self._stride):
                    window = np.array(vid_elem[y:y+region_size[0], x:x+region_size[1], :])
                    cy, cx = y+region_size[0]//2, x+region_size[1]//2
                    dist = math.sqrt((pos[0]-cy)**2 + (pos[1]-cx)**2)

                    window = cv.resize(window, self._window_size)
                    self.output.put((dist, window), timeout=1)