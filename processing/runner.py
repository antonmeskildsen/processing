import json
import glob
import os
import argparse
from itertools import zip_longest
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

# TODO: Really fix before you die
#from processing import pipeline
#from models.slidingeye.dataset import pipeline
import processing.pipelines.simple_pupil_generator as pipeline

parser = argparse.ArgumentParser()
parser.add_argument('spec', help='Path to spec file')
args = parser.parse_args()

f = open(args.spec)
spec = json.load(f)

pipe = getattr(pipeline, spec['pipeline'])

base_path = spec['path']

base_args = spec['arguments']

patterns = [glob.glob(os.path.join(base_path, path))
            for path in spec['patterns'].values()]

names = list(spec['patterns'].keys())

# Kinda shitty way to add the base path to arguments containing paths
for key, val in base_args.items():
    if 'path' in key:
        base_args[key] = os.path.join(base_path, val)


pool = ProcessPoolExecutor(8)

for args in zip_longest(*patterns):
    kwargs = dict(zip(names, args))
    #p = pipe(**kwargs, **base_args)
    #ops = tfunc(**kwargs, **base_args)
    #pipe(**kwargs, **base_args)
    pool.submit(pipe, **kwargs, **base_args)


pool.shutdown(True)

print('Done!')


