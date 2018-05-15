from processing.loaders import *
from processing.transformers import *
from processing.writers import *


def region_position_map_pipeline(track_path,
                                 track_names,
                                 output_path,
                                 img_size,
                                 positionmap_size,
                                 region_size,
                                 output_folder=''):
    loader = TrackFileLoader(track_path, track_names)

    scaling = positionmap_size[0]/img_size[0], positionmap_size[1]/img_size[1]
    transformer = PositionMapGenerator(loader.output, positionmap_size, scaling)

    splitter = Split(transformer.output, (60, 20, 20))
    operations = [loader, transformer, splitter]

    if not os.path.exists(output_path):
        os.mkdir(output_path)

    dirs = tuple([os.path.join(path, output_folder) for path in ('train', 'test', 'val')])
    for out, dir in zip(splitter.output, dirs):
        writer = ArraySequenceWriter(out, os.path.join(output_path, dir), '')
        operations.append(writer)

    run(operations)


def window_radius_pipeline(video_path,
                           output_path,
                           image_size,
                           scaling,
                           window_size=(32, 32),
                           stride=1,
                           radius_negative=5,
                           radius_positive=3,
                           track_name='eye_left',
                           negative_restrict=None):
    video_loader = VideoLoader(video_path)

    track_path = os.path.splitext(video_path)[0] + '.json'
    track_loader = TrackFileLoader(track_path, track_names=[track_name])

    resize = ImageResize(video_loader.output, tuple(image_size))

    window_trans = WindowGenerator(resize.output, track_loader.output, tuple(window_size), scaling, stride, radius_negative)
    splitter = Split(window_trans.output, (60, 20, 20))

    operations = [video_loader, track_loader, resize, window_trans, splitter]

    if not os.path.exists(output_path):
        os.mkdir(output_path)

    dirs = ('train', 'test', 'val')
    prefix = os.path.splitext(os.path.basename(video_path))[0]
    for out, dir in zip(splitter.output, dirs):
        radius_trans = SplitPredicate(out, lambda dist: dist < radius_positive)
        out_dir = os.path.join(output_path, dir)
        if not os.path.exists(out_dir):
            os.mkdir(out_dir)
        positive_dir = os.path.join(out_dir, 'positive')
        negative_dir = os.path.join(out_dir, 'negative')

        writer_positive = ImageSequenceWriter(radius_trans.positive, positive_dir, prefix)
        writer_negative = ImageSequenceWriter(radius_trans.negative, negative_dir, prefix)
        operations.append(radius_trans)
        operations.append(writer_positive)
        operations.append(writer_negative)

    run(operations)  # TODO: Make into decorator


def image_sequence_pipeline(video_path, output_path, size):
    loader = VideoLoader(video_path)
    transformer = ImageResize(loader.output, tuple(size))
    splitter = Split(transformer.output, (60, 20, 20))

    operations = [loader, transformer, splitter]

    dirs = ('train', 'test', 'val')
    if not os.path.exists(output_path):
        os.mkdir(output_path)

    prefix = os.path.splitext(os.path.basename(video_path))[0]
    for out, dir in zip(splitter.output, dirs):
        writer = ImageSequenceWriter(out, os.path.join(output_path, dir), prefix)
        operations.append(writer)

    run(operations)  # TODO: Make into decorator


def start(operations):
    for operation in operations:
        operation.start()

def join(operations):
    for operation in operations:
        operation.join()

def run(operations):
    start(operations)
    join(operations)