#!/usr/bin/env python
"""
Module for dealing with data type conversion between
Oracle, python, numpy, FITS, pandas, ...

Some useful documentation:
Oracle: https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm
cx_Oracle: https://cx-oracle.readthedocs.org/en/latest/
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
or_n = cx_Oracle.NUMBER
or_s = cx_Oracle.STRING
or_f = cx_Oracle.NATIVE_FLOAT
or_dt = cx_Oracle.DATETIME
or_ts = cx_Oracle.TIMESTAMP
# This is actually OBJECTVAR (hence 'or_ov')
or_ov = cx_Oracle.OBJECT


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
        # When no scale/digits avaiable, return float
        if scale is None and digits is None:
            return "f8"
        if scale == 0 and digits != 0:
            # Nothing after the decimal; integers
            if digits <= 4:
                return "i2"
            elif digits <= 9:
                return "i4"
            # This is sloppy...
            else:
                return "i8"
        else:
            if digits is None:
                return "f8"
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
        # msg = "Unsupported Oracle type: %s" % otype
        # raise ValueError(msg)


def oracle2fitsio(desc):
    """Takes an Oracle data type and converts to a numpy dtype
    suitable for writing with fitsio.

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

    if (otype == or_dt) or (otype == or_ts):
        return "S50"
    else:
        return oracle2numpy(desc)


def numpy2oracle(dtype):
    """Takes a numpy dtype object and converts to an Oracle data type
    string.

    TODO: Vectorize?

    Parameters:
    ----------
    dtype: Numpy dtype object

    Returns:
    --------
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
            # This is sloppy...
            # 'i8' is 19 digits; 'u8' is 20 digits
            return 'NUMBER(20,0)'
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
        # Should test on CREATED_DATE from PROD.PROCTAG@DESOPER
        return 'DATETIME'
    elif (kind == 'O'):
        # Careful pandas creates objects for strings...
        return 'OBJECT'
    else:
        return ""
        # msg = "Unsupported numpy dtype: %s" % dtype
        # raise ValueError(msg)


def numpy2desdm(desc):
    """
    Impose DESDM typing conventions based on column name.

    This is an experimental function for imposing some of the DESDM
    'conventions' for defining column types. The 'conventions' come
    mostly from the existing Y1A1 and PROD tables.

    This function is NOT comprehensive.

    Parameters:
    ----------
    desc : numpy dtype descriptor (i.e., np.dtype.descr)

    Returns:
    --------
    otype: Oracle data type string
    """
    name = desc[0].upper()
    dtype = np.dtype(desc[1])

    # It would be better to do this with a lookup dictionary rather
    # than 'if/elif' clauses. However, it is hard to do 'startswith'
    # when searching dictionary keys. It would also be more flexible
    # to use regexs.

    # Integer values
    if name.startswith(('CCDNUM')) or name in ['ATTNUM']:
        return "NUMBER(2,0)"
    elif name.startswith(('FLAGS_', 'OBSERVED_', 'MODEST_CLASS')):
        return "NUMBER(3,0)"
    elif name.startswith(('NEPOCHS')):
        return "NUMBER(4,0)"
    elif name in ['REQNUM']:
        return "NUMBER(7,0)"
    elif name.startswith(('HPIX', 'EXPNUM')):
        return "NUMBER(10,0)"
    # Temporary adjustment to deal with large object numbers
    elif name in ['COADD_OBJECTS_ID', 'COADD_OBJECT_ID', 'OBJECT_NUMBER', 'OBJECT_ID']:
        return "NUMBER(11,0)"
    elif name in ['QUICK_OBJECT_ID']:
        return "NUMBER(15,0)"
    # Floating point values
    elif name.strip('WAVG_').startswith(("CLASS_STAR", "SPREAD_", "SPREADERR_")):
        return 'BINARY_FLOAT'
    elif name.strip('WAVG_').startswith(("MAG_", "MAGERR_", "CALIB_MAG_")):
        return 'BINARY_FLOAT'
    # ADW: Y3A2 tables currently implement as doubles for no apparent reason.
    #elif name.strip('MOF_').startswith(("CM_MAG_","PSF_MAG_")):
    #    return 'BINARY_FLOAT'
    #elif name.strip('SOF_').startswith(("CM_MAG_","PSF_MAG_")):
    #    return 'BINARY_FLOAT'
    elif name.startswith(('SLR_SHIFT', 'DESDM_ZP', 'DESDM_ZPERR')):
        # DEPRECATED: ADW 2018-06-07
        return "NUMBER(6,4)"
    elif name in ['RA', 'DEC', 'RADEG', 'DECDEG', 'L', 'B']:
        return "NUMBER(9,6)"
    elif name.startswith(('ALPHAWIN','DELTAWIN')):
        return "BINARY_DOUBLE"
    # String values
    elif name in ['BAND']:
        # Needs to fit 'VR' and 'block'
        return "VARCHAR2(5)"
    elif name in ['UNITNAME']:
        # Why is this so large? Usually "D%8d" = VARCHAR2(9)
        return "VARCHAR2(20)"
    elif name in ['TAG']:
        return "VARCHAR2(30)"
    elif name in ['FILENAME']:
        # This is VARCHAR2(60) in prod.se_object, but seems like overkill
        # This is VARCHAR2(100) in Y3A2_COADD_OBJECT_BAND, but seems like overkill
        return "VARCHAR2(50)"
    else:
        return numpy2oracle(dtype)


if __name__ == "__main__":
    import argparse
    description = __doc__
    parser = argparse.ArgumentParser(description=description)
    args = parser.parse_args()
