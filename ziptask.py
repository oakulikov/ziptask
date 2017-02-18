from random import randint
from functools import lru_cache
from itertools import dropwhile
from uuid import uuid4
from os import mkdir
from shutil import rmtree
from zipfile import ZipFile
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
import xml.etree.ElementTree as ET
import os.path
import csv
from utils import random_word


class ZipTaskException(Exception):
    pass


class ZipTask:
    """Provide interface to do task, please see README.

    Args:
    - work_dir: working dir will be removed if exists before created
    - nzip: number of zip files
    - nxml: number of xml files
    - max_level: max level value
    - max_obj: max number of objects
    - max_workers: max concurrency

    Methods:
    - run: execute task in one step
    - (re)setup: setup task params
    - cleanup: remove work dir
    - create_archives: 1st part of task
    - handle_archives: 2nd part of task"""

    def __init__(
            self,
            work_dir,
            nzip=50,
            nxml=100,
            max_level=100,
            max_obj=10,
            max_workers=cpu_count() * 4):
        self.work_dir = work_dir
        self.nzip = nzip
        self.nxml = nxml
        self.max_level = max_level
        self.max_obj = max_obj
        self.max_workers = max_workers

    def run(self):
        self.setup()
        self.create_archives()
        self.handle_archives()

    def create_archives(self):
        try:
            set(
                self._pool.map(
                    self._creator,
                    range(
                        1,
                        self.nzip +
                        1)))
        except Exception as e:
            raise ZipTaskException('Fails when create archive by {}'.format(e))

    def handle_archives(self):
        try:
            xml_data = list(
                self._pool.map(
                    self._reader, range(
                        1, self.nzip + 1)))
        except Exception as e:
            raise ZipTaskException('Fails when read archive by {}'.format(e))
        try:
            set(
                self._pool.map(
                    self._handler, [
                        (1, xml_data), (2, xml_data)]))
        except Exception as e:
            raise ZipTaskException('Fails when write files by {}'.format(e))

    def cleanup(self):
        if os.path.isdir(self.work_dir):
            rmtree(self.work_dir)

    def setup(self):
        self.cleanup()
        mkdir(self.work_dir)
        self._pool = ProcessPoolExecutor(max_workers=self.max_workers)
        self._creator = Creator(
            self.work_dir,
            self.nxml,
            self.max_level,
            self.max_obj)
        self._reader = Reader(
            self.work_dir,
            self.nxml)
        self._handler = Handler(
            self.work_dir)


class Creator:

    XML_TEMPLATE = """
        <root>
            <var name='id' value='{ustring}'/>
            <var name='level' value='{aint}'/>
            <objects>{objects}</objects>
        </root>
    """
    OBJ_TEMPLATE = "<object name='{astring}'/>"

    def __init__(self,
                 work_dir,
                 nxml,
                 max_level,
                 max_obj):
        self.work_dir = work_dir
        self.nxml = nxml
        self.max_level = max_level
        self.max_obj = max_obj

    def __call__(self, n):
        zip_name = os.path.join(
            self.work_dir,
            str(n) + '.zip')
        tmp_dir = os.path.join(
            self.work_dir, str(n))
        mkdir(tmp_dir)
        with ZipFile(zip_name, 'w') as myzip:
            for i in range(1, self.nxml + 1):
                arc_name = str(i) + '.xml'
                xml_name = os.path.join(tmp_dir, arc_name)
                self._write_xml(xml_name)
                myzip.write(xml_name, arcname=arc_name)
        rmtree(tmp_dir)

    @lru_cache()
    def _templates(self, n):
        return [self.OBJ_TEMPLATE] * n

    def _make_objects(self, n):
        templates = self._templates(n)
        objects = map(lambda t: t.format(
            astring=random_word(randint(1, n * 2))), templates)
        return ''.join(objects)

    def _make_xml(self):
        ustring = uuid4()
        aint = randint(1, self.max_level)
        objects = self._make_objects(randint(1, self.max_obj))
        return self.XML_TEMPLATE.format(
            ustring=ustring, aint=aint, objects=objects)

    def _write_xml(self, xml_name):
        with open(xml_name, 'w', encoding='utf-8') as myxml:
            myxml.write(self._make_xml())


class Reader:

    def __init__(self,
                 work_dir,
                 nxml):
        self.work_dir = work_dir
        self.nxml = nxml

    def __call__(self, n):
        zipname = os.path.join(
            self.work_dir,
            str(n) + '.zip')
        xml_data = []
        with ZipFile(zipname, 'r') as myzip:
            for i in range(1, self.nxml + 1):
                with myzip.open(str(i) + '.xml') as myxml:
                    xml = myxml.read().decode('utf-8')
                    xml_data.append(self._parse_xml(xml))
        return xml_data

    def _parse_xml(self, xml):
        root = ET.fromstring(xml)
        id_ = ''
        level = ''
        objects = []
        for var in root.findall('var'):
            attrs = var.attrib
            if attrs['name'] == 'id':
                id_ = attrs['value']
            elif attrs['name'] == 'level':
                level = attrs['value']
        for obj in root.find('objects').findall('object'):
            objects.append(obj.attrib['name'])
        return (id_, level, objects)


class Handler:

    def __init__(self,
                 work_dir):
        self.work_dir = work_dir
        self.config = dict()
        self.config[1] = {
            'csv_name': 'levels.csv',
            'fieldnames': ['id', 'level']
        }
        self.config[2] = {
            'csv_name': 'objects.csv',
            'fieldnames': ['id', 'object']
        }

    def __call__(self, args):
        (n, xml_data) = args
        csv_name = os.path.join(
            self.work_dir, self.config[n]['csv_name'])
        fieldnames = self.config[n]['fieldnames']
        with open(csv_name, 'w', encoding='utf-8') as mycsv:
            writer = csv.DictWriter(mycsv, fieldnames=fieldnames)
            if n == 1:
                [writer.writerow({'id': id_, 'level': level})
                 for archive in xml_data
                 for (id_, level, _) in archive]
            elif n == 2:
                [writer.writerow({'id': id_, 'object': obj})
                 for archive in xml_data
                 for (id_, _, objects) in archive
                 for obj in objects]
