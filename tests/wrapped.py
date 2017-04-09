import numpy as np
import pandas as pd
import inspect
import re
from functools import wraps
from easyaccess.eautils.fun_utils import toeasyaccess


@toeasyaccess
def my_func(a, b):
    """
    Sum two colums, if max_values is defined the values are clipped
    to that value
    """
    return (0. + a) * b


@toeasyaccess
def my_sum(a, b, min_value=None, max_value=None):
    """
    Sum two colums, if max_values is defined the values are clipped
    to that value
    """
    c = abs(a) + abs(b)
    if min_value is None:
        min_value = np.min(c)
    if max_value is None:
        max_value = np.max(c)
    return np.clip(c, float(min_value), float(max_value))
