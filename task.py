from ziptask import ZipTask
from time import time
from task_test import test, WORK_DIR


def main(work_dir):
    """ZipTask demo. Works in Python 3.6. Other versions not tested."""
    task = ZipTask(work_dir)
    """Task step by step."""
    task.setup()
    start = time()
    task.create_archives()
    end = time()
    print('1. Took %.8f seconds' % (end - start))
    start = time()
    task.handle_archives()
    end = time()
    print('2. Took %.8f seconds' % (end - start))
    start = time()
    """Task in one step."""
    task.run()
    end = time()
    print('1,2. Took %.8f seconds' % (end - start))

if __name__ == '__main__':
    test(WORK_DIR)
    main(WORK_DIR)
