#!/usr/bin/env python

"""
setup.py file for seekgzip
"""

import sys
import os.path

import os; os.environ['CC'] = 'g++'; os.environ['CXX'] = 'g++';
os.environ['CPP'] = 'g++'; os.environ['LDSHARED'] = 'g++'

from distutils.core import setup, Extension

seekgzip_module = Extension(
    '_seekgzip',
    sources = [
        'seekgzip.c',
        'export.cpp',
        'export_python.cpp',
        ],
    libraries=['z'],
    extra_link_args=['-shared'],
    language='c++',
    )

setup(
    name = '@PACKAGE@',
    version = '@VERSION@',
    author = 'Naoaki Okazaki',
    description = """SeekGzip Python module""",
    ext_modules = [seekgzip_module],
    py_modules = ["seekgzip"],
    )


