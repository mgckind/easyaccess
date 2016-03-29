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
or_n  = cx_Oracle.NUMBER
or_s  = cx_Oracle.STRING
or_f  = cx_Oracle.NATIVE_FLOAT
or_dt = cx_Oracle.DATETIME
or_ts = cx_Oracle.TIMESTAMP
# This is actually OBJECTVAR (hence 'or_ov')
or_ov  = cx_Oracle.OBJECT

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
            # This is sloppy... since coverting i8 to NUMBER(22,0)
            else:
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
            return 'NUMBER(22,0)'
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


def numpy2desdm(desc):
    """
    This is an experimental function for following some of the DESDM
    'conventions' for defining column types. The 'conventions' come
    mostly from the Y1A1_OBJECTS table.
    
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

    # Integer values
    if name.startswith(('CCDNUM')):
        return "NUMBER(2,0)"
    elif name.startswith(('FLAGS_','OBSERVED_','MODEST_CLASS')):
        return "NUMBER(3,0)"
    elif name.startswith(('NEPOCHS')):
        return "NUMBER(4,0)"
    elif name.startswith(('HPIX','EXPNUM')):
        return "NUMBER(10,0)"
    elif name.endswith(('OBJECTS_ID','OBJECT_ID')):
        return "NUMBER(11,0)"
    # Float values
    elif name.startswith(("CLASS_STAR","MAGERR_","WAVG_MAGERR_")):
        return "NUMBER(5,4)"
    elif name.startswith(("MAG_","WAVG_MAG_","WAVGCALIB_MAG_")):
        return "NUMBER(6,4)"
    elif name.startswith(('SLR_SHIFT','DESDM_ZP','DESDM_ZPERR')):
        return "NUMBER(6,4)"
    elif name.startswith(("SPREAD_","SPREADERR_","WAVG_SPREAD_")):
        return "NUMBER(6,5)"
    elif name in ['RA','DEC','RADEG','DECDEG','L','B']:
        return "NUMBER(9,6)"
    # String values
    elif name in ['BAND']:
        return "VARCHAR2(5)"
    else:
        return numpy2oracle(dtype)

if __name__ == "__main__":
    import argparse
    description = __doc__
    parser = argparse.ArgumentParser(description=description)
    args = parser.parse_args()


