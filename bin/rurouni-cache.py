#!/usr/bin/env python
# coding: utf-8

import sys
import os.path

BIN_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BIN_DIR)

from rurouni.utils import run_twistd_plugin
from rurouni.exceptions import RurouniException

try:
    run_twistd_plugin(__file__)
except RurouniException as e:
    raise SystemError(e)
