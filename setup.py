# coding: utf-8

from setuptools import setup, Extension

import re
import os
import sys
import codecs
from glob import glob

here = os.path.abspath(os.path.dirname(__file__))

if sys.version_info < (2,7):
    sys.exit('Sorry, Python < 2.7 is not supported')


def read(*parts):
    # intentionally *not* adding an encoding option to open
    return codecs.open(os.path.join(here, *parts), 'r').read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


long_description = read('README.md')

setup(
    name='kenshin',
    version=find_version('kenshin', '__init__.py'),
    description='A scalable time series database.',
    long_description=long_description,
    author='Zhaolong Zhu',
    url='https://github.com/douban/Kenshin',
    download_url='https://github.com/douban/Kenshin.git',
    author_email='zhuzhaolong0@gmail.com',
    install_requires=[],
    tests_require=['nose'],
    packages=['kenshin', 'kenshin.tools', 'rurouni', 'rurouni.state', 'twisted.plugins'],
    scripts=glob('bin/*'),
    zip_safe=False,
    platforms='any',
    setup_requires=['Cython'],
    ext_modules=[
        Extension(
            name='%s.%s' % ('rurouni', name),
            sources=['%s/%s.pyx' % ('rurouni', name)],
            extra_compile_args=['-O3', '-funroll-loops', '-Wall'],
        ) for name in ['fnv1a']
    ],
)
