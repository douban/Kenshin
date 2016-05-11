# coding: utf-8

import os
import errno


def get_metric(path):
    import re
    abspath = os.path.abspath(path)
    realpath = os.path.realpath(path)
    metric = None
    if abspath != realpath:
        try:
            metric = re.split('/link/[a-z0-9]+/', abspath)[1]
            metric = metric[:-3]  # remove .hs
            metric = metric.replace('/', '.')
        except IndexError:
            pass
    return metric


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def roundup(x, base):
    """
    Roundup to nearest multiple of `base`.

    >>> roundup(21, 10)
    30
    >>> roundup(20, 10)
    20
    >>> roundup(19, 10)
    20
    """
    t = x % base
    return (x - t + base) if t else x


if __name__ == '__main__':
    import doctest
    doctest.testmod()
