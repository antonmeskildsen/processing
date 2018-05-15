import glob
import os
import time
from itertools import zip_longest
from concurrent.futures import ProcessPoolExecutor


def create_multiprocessing_job(pipeline, base_path, patterns, arguments):
    pattern_list = [glob.glob(os.path.join(base_path, path))
                    for path in patterns.values()]
    names = list(patterns.keys())
    print(pattern_list)
    for key, val in arguments.items():
        if 'path' in key:
            arguments[key] = os.path.join(base_path, val)

    pool = ProcessPoolExecutor(8)

    for args in zip_longest(*pattern_list):
        kwargs = dict(zip(names, args))
        pipeline(**kwargs, **arguments)

        #pool.submit(pipeline, **kwargs, **arguments)
        time.sleep(0.2)

    return pool
