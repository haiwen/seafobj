#!/usr/bin/env python3

import os

from utils import setup_logging, shell
from os.path import abspath, join

TOPDIR = abspath(join(os.getcwd()))


def main():
    shell("py.test", env=dict(os.environ))



if __name__ == "__main__":
    setup_logging()
    main()
