# coding: utf-8

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import re
import os
import codecs
from glob import glob

here = os.path.abspath(os.path.dirname(__file__))


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
    url='http://code.dapps.douban.com/Kenshin',
    download_url='http://code.dapps.douban.com/Kenshin.git',
    author_email='zhuzhaolong0@gmail.com',
    install_requires=[
        'numpy',
        'zope.interface==4.1.1',
        'Twisted==13.1'
    ],
    tests_require=['nose'],
    packages=['kenshin', 'kenshin.tools', 'rurouni', 'rurouni.state', 'twisted.plugins'],
    scripts=glob('bin/*'),
)
