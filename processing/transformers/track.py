from queue import Queue

from processing.transformers import Transformer


class DiscardEmpty(Transformer):
    """
    Discards rows of tracks with empty elements. Removing just empty elements would be better,
    but because track elements are put through the pipeline row by row, this is the best solution.
    """
    def __init__(self, input):
        super().__init__(input, Queue(1000))

    def _iszero(self, data):
        for val in data.values():
            if val != 0:
                return False
        return True

    def run(self):
        while True:
            elem = self.input.get()
            if elem is None:
                self.output.put(None)
                return

            if not self._iszero(elem[1]):
                self.output.put(elem)


class Translate(Transformer):
    def __init__(self, input, translation):
        super().__init__(input, Queue(1000))
        self._translation = translation

    @property
    def translation(self):
        return self._translation

    def _translate(self, type, data):
        if type == 'point':
            res = {
                'x': data['x']+self.translation[1],
                'y': data['y']+self.translation[0]
            }
        elif type == 'inscribed_circle':
            res = {
                'cx':       data['cx']+self.translation[1],
                'cy':       data['cy']+self.translation[0],
                'width':    data['width'],
                'height':   data['height'],
                'angle':    data['angle']
            }
        elif type == 'rectangle_region':
            res = {
                'x':        data['x']+self.translation[1],
                'y':        data['y']+self.translation[0],
                'width':    data['width'],
                'height':   data['height']
            }
        else:
            raise ValueError('Unknown track type: {}'.format(type))
        return type, res

    def run(self):
        while True:
            elem = self.input.get(timeout=1)
            if elem is None:
                self.output.put(None)
                return

            out = [self._translate(type, data) for type, data in elem]
            self.output.put(out)


class Scale(Transformer):

    def __init__(self, input, scale):
        super().__init__(input, Queue(1000))
        self._scale = scale

    @property
    def scale(self):
        return self._scale

    def _rescale(self, type, data):
        if type == 'point':
            res = {
                'x': data['x']*self.scale,
                'y': data['y']*self.scale
            }
        elif type == 'inscribed_circle':
            res = {
                'cx':       data['cx']*self.scale,
                'cy':       data['cy']*self.scale,
                'width':    data['width']*self.scale,
                'height':   data['height']*self.scale,
                'angle':    data['angle']
            }
        elif type == 'rectangle_region':
            res = {
                'x':        data['x'] * self.scale,
                'y':        data['y'] * self.scale,
                'width':    data['width'] * self.scale,
                'height':   data['height'] * self.scale
            }
        else:
            raise ValueError('Unknown track type: {}'.format(type))
        return type, res

    def run(self):
        while True:
            elem = self.input.get(timeout=5)
            if elem is None:
                self.output.put(None)
                return

            out = [self._rescale(type, data) for type, data in elem]
            self.output.put(out)


class CenterExtractor(Transformer):

    def __init__(self, input):
        super().__init__(input, Queue(1000))

    @staticmethod
    def _track_center(type, data):
        if type == 'point':
            return data['y'], data['x']
        elif type == 'inscribed_circle':
            return data['cy'], data['cx']
        elif type == 'rectangle_region':
            return data['y']+data['height']/2, data['x']+data['width']/2
        else:
            raise ValueError('Unknown track type: {}'.format(type))

    def run(self):
        while True:
            elem = self.input.get(timeout=5)
            if elem is None:
                self.output.put(None)
                return

            out = [self._track_center(type, data) for type, data in elem]
            self.output.put(out)


class RoundToInt(Transformer):
    def __init__(self, input):
        super().__init__(input, Queue(1000))

    @staticmethod
    def _rounded(type, data):
        if type == 'point':
            res = {
                'x': int(data['x']),
                'y': int(data['y'])
            }
        elif type == 'inscribed_circle':
            res = {
                'cx':       int(data['cx']),
                'cy':       int(data['cy']),
                'width':    int(data['width']),
                'height':   int(data['height']),
                'angle':    int(data['angle'])
            }
        elif type == 'rectangle_region':
            res = {
                'x':        int(data['x']),
                'y':        int(data['y']),
                'width':    int(data['width']),
                'height':   int(data['height'])
            }
        else:
            raise ValueError('Unknown track type: {}'.format(type))
        return type, res

    def run(self):
        while True:
            elem = self.input.get(timeout=5)
            if elem is None:
                self.output.put(None)
                return

            out = [self._rounded(type, data) for type, data in elem]
            self.output.put(out)