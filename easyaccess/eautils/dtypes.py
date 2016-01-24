#!/usr/bin/env python
"""
Module for dealing with data type conversion between
Oracle, python, numpy, FITS, pandas, ...

Some useful documentation:
Oracle: https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm
numpy: http://docs.scipy.org/doc/numpy/reference/arrays.dtypes.html
FITS: https://heasarc.gsfc.nasa.gov/docs/software/fitsio/c/c_user/node20.html

Also, can double check agains Erin Sheldon's DESDB module:
https://github.com/esheldon/desdb

Numpy datetimes:
http://docs.scipy.org/doc/numpy-1.10.0/reference/arrays.datetime.html
"""

import cx_Oracle
import numpy as np

# Oracle data types
or_n  = cx_Oracle.NUMBER
or_s  = cx_Oracle.STRING
or_f  = cx_Oracle.NATIVE_FLOAT
or_o  = cx_Oracle.OBJECT
or_dt = cx_Oracle.DATETIME
or_ts = cx_Oracle.TIMESTAMP

def oracle2numpy(desc):
    """Takes an Oracle data type and converts to a numpy dtype string.

    TODO: Vectorize?
    
    Parameters:
    ----------
    info: Oracle column descriptor

    Returns:
    dtype: Numpy dtype string
    """
    name = desc[0]
    otype = desc[1]
    size = desc[3]
    digits = desc[4]
    scale = desc[5]
    
    if otype == or_n:
        if scale == 0:
            # Nothing after the decimal; integers
            if digits == 0:
                return "i8"
            elif digits <= 4:
                return "i2"
            elif digits <= 9:
                return "i4"
            elif digits <= 18:
                return "i8"
        else:
            # Otherwise, floats
            if digits <= 6:
                return "f4"
            elif digits <= 15:
                return "f8"
            else:
                # I didn't know this existed...
                return "f16" 
    elif otype == or_f:
        # Native floats
        if size == 4:
            return "f4"
        elif size == 8:
            return "f8"
    elif otype == or_s:
        return "S" + str(size)
    else:
        # Ignore other Oracle types for now
        return ""
        #msg = "Unsupported Oracle type: %s" % otype
        #raise ValueError(msg)

def numpy2oracle(dtype):
    """Takes a numpy dtype object and converts to an Oracle data type
    string.

    TODO: Vectorize?
    
    Parameters:
    ----------
    dtype: Numpy dtype object

    Returns:
    otype: Oracle data type string
    """
    kind = dtype.kind
    size = dtype.itemsize

    if (kind == 'S'):
        # string type
        return 'VARCHAR2(%d)' % size
    elif (kind == 'i' or kind == 'u'):
        if (size == 1):
            # 1-byte (8 bit) integer
            return 'NUMBER(3,0)'
        elif (size == 2):
            # 2-byte (16 bit) integer
            return 'NUMBER(5,0)'
        elif (size == 4):
            # 4-byte (32 bit) integer
            return 'NUMBER(10,0)'
        else:
            # 8-byte (64 bit) integer
            # 'i8' is 19 digits; 'u8' is 20 digits
            return 'NUMBER'
    elif (kind == 'f'):
        if (size == 4):
            # 4-byte (32 bit) float
            return 'BINARY_FLOAT'
        elif (size == 8):
            # 8-byte (64 bit) double
            return 'BINARY_DOUBLE'
        else:
            msg = "Unsupported float type: %s" % kind
            raise ValueError(msg)
    elif (kind == 'M'):
        return 'DATETIME'
    elif (kind == 'O'):
        # Careful pandas creates objects for strings...
        return 'OBJECT'
    else:
        return ""
        #msg = "Unsupported numpy dtype: %s" % dtype
        #raise ValueError(msg)


if __name__ == "__main__":
    import argparse
    description = __doc__
    parser = argparse.ArgumentParser(description=description)
    args = parser.parse_args()


