from ziptask import ZipTask, ZipTaskException
from collections import Counter
from string import ascii_letters
import os.path
import csv
import re

WORK_DIR = '/var/tmp/ziptask/task'


def test(work_dir):
    """Functional test of ZipTask."""
    nzip = 5
    nxml = 10
    max_level = 5
    max_obj = 5
    task = ZipTask(work_dir, nzip=nzip, nxml=nxml, max_level=max_level,
                   max_obj=max_obj)
    task.run()
    assert os.path.isdir(work_dir), 'Work dir created'
    levels = os.path.join(
        work_dir, 'levels.csv')
    assert os.path.isfile(levels), 'Levels file created'
    with open(levels, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, fieldnames=['id', 'level'])
        set_ids = set()
        set_levels = set()
        [(set_ids.add(row['id']), set_levels.add(row['level']))
         for row in reader]
        assert len(set_ids) == nzip * nxml, 'Ids is unique'
        assert 1 <= len(set_levels) <= max_level, 'Number levels is correct'
        assert int(min(set_levels)) >= 1, 'Min level value is correct'
        assert int(max(set_levels)) <= max_level, 'Max level value is correct'
    objects = os.path.join(
        work_dir, 'objects.csv')
    assert os.path.isfile(objects), 'Objects file created'
    with open(objects, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, fieldnames=['id', 'object'])
        list_ids = list()
        list_obj = list()
        [(list_ids.append(row['id']), list_obj.append(row['object']))
         for row in reader]
        set_ids = set(list_ids)
        count_ids = Counter(list_ids)
        assert len(set_ids) == nzip * nxml, 'Number ids is correct'
        [_assert(1 <= count_ids[id_] <= max_obj, 'Number objects is correct')
         for id_ in set_ids]
        pattern = re.compile(r'^[{}]+$'.format(ascii_letters))
        [_assert(pattern.match(obj), 'Object is correct')
         for obj in list_obj]
    task.cleanup()
    assert (not os.path.exists(work_dir)), 'Work dir removed'
    try:
        task.create_archives()
    except ZipTaskException:
        assert True, 'Throw ZipTaskException on create archive'
    task.setup()
    task.create_archives()
    task.cleanup()
    try:
        task.handle_archives()
    except ZipTaskException:
        assert True, 'Throw ZipTaskException on read archive'


def _assert(exp, msg):
    assert exp, msg

if __name__ == '__main__':
    test(WORK_DIR)
