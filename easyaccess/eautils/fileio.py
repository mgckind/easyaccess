#!/usr/bin/env python
"""
Module for file input/output with pandas, fitsio, ...

Some useful documentation:
fitsio: https://github.com/esheldon/fitsio
numpy: 
pandas: 

"""
import os
import datetime
import numpy as np
import pandas as pd
import fitsio

try:
    import easyaccess.eautils.dtypes as eatypes
    import easyaccess.version as version
except ImportError:
    import eautils.dtypes as eatypes
    import version

PANDAS_DEFS = ('comma separated text', 'space separated tex', 'HDF5 format')
PANDAS_EXTS = ('.csv', '.tab', '.h5')

FITS_DEFS = ('FITS format',)
FITS_EXTS = ('.fits',)

FILE_DEFS = PANDAS_DEFS + FITS_DEFS
FILE_EXTS = PANDAS_EXTS + FITS_EXTS


def cutquery(query, length):
    """
    Return query in a list of fixed sized character strings
    """
    return [query[0 + i:length + i] for i in range(0, len(query), length)]


def unrecognized_filetype(filename, types=None):
    """
    Return message about unrecognized file type.

    Parameters:
    -----------
    filename : File name (or extension)
    
    Returns:
    --------
    msg : Unrecognized file message
    """
    if types is None: types = FILE_EXTS
    # Try to split the filename
    base, ext = os.path.splitext(filename)
    # Also allow just the file extension
    if ext == '': ext = base

    msg = "Unrecognized file type: '%s'\n" % ext
    msg += "Supported filetypes:\n"
    msg += ' ' + ', '.join("'%s'" % t for t in types)
    return msg


def check_filetype(filename, types=None):
    """
    Check file extension against allowed types.

    Parameters:
    -----------
    filename : Name (or extension) of file
    
    Returns:
    --------
    True : (Or raises IOError)
    """
    if types is None: types = FILE_EXTS
    # Try to split the filename
    base, ext = os.path.splitext(filename)
    # Also allow just the file extension
    if ext == '': ext = base

    if ext not in types:
        msg = unrecognized_filetype(ext, types)
        raise IOError(msg)
    else:
        return True


def write_file(filename, data, desc, fileindex=1, mode='w', max_mb=1000, query=''):
    """
    Write a pandas DataFrame to a file. Append to existing file as
    long as smaller than specified size.  Create a new file (and
    increment fileindex) when file grows too large.

    'fileindex' is 1-indexed for backwards compatibility

    Parameters:
    -----------
    filename : Output base filename (incremented by 'fileindex')
    data :     The DataFrame to write to the file
    desc :     The Oracle data descriptor
    fileindex: The index of the file to write.
    mode :     The write-mode: 'w'=write new file, 'a'=append to existing file
    max_mb :   Maximum file size.
    
    Returns:
    fileindex: The (possibly incremented) fileindex.
    """
    base, ext = os.path.splitext(filename)
    check_filetype(filename, FILE_EXTS)

    fileout = filename

    if mode == 'w':
        header = True
    if mode == 'a':
        if (fileindex == 1):
            thisfile = filename
        else:
            thisfile = base + '_%06d' % fileindex + ext

            # check the size of the current file
        size = float(os.path.getsize(thisfile)) / (2. ** 20)

        if (size > max_mb):
            # it's time to increment the file
            if (fileindex == 1):
                # this is the first one ... it needs to be moved
                lastfile = base + '_%06d' % fileindex + ext
                os.rename(filename, lastfile)

            # and make a new filename, after incrementing
            fileindex += 1

            thisfile = base + '_%06d' % fileindex + ext
            fileout = thisfile
            mode = 'w'
            header = True
        else:
            fileout = thisfile
            header = False

    if ext in PANDAS_EXTS:
        write_pandas(fileout, data, fileindex, mode=mode, header=header, query=query)
    if ext in FITS_EXTS:
        write_fitsio(fileout, data, desc, fileindex, mode=mode, query=query)

    return fileindex


def write_pandas(filename, df, fileindex, mode='w', header=True, query=''):
    """
    Write a pandas DataFrame to a file. Accepted file extension are
    defined by 'PANDAS_EXTS'.

    Parameters:
    -----------
    filename:  Output filename: '.csv','.tab','.h5'
    df :       DataFrame object
    fileindex: Index of this file (modifies filename based on maxfilesize)
    mode :     Write mode: 'w'=write, 'a'=append
    header :   Write header information

    Returns:
    --------
    None
    """
    base, ext = os.path.splitext(filename)
    check_filetype(filename, PANDAS_EXTS)

    if ext == '.csv':
        df.to_csv(filename, index=False, float_format='%.8f', sep=',',
                  mode=mode, header=header)
    if ext == '.tab':
        df.to_csv(filename, index=False, float_format='%.8f', sep=' ',
                  mode=mode, header=header)
    if ext == '.h5':
        df.to_hdf(filename, 'data', mode=mode, index=False,
                  header=header)  # , complevel=9,complib='bzip2'


def write_fitsio(filename, df, desc, fileindex, mode='w', query=''):
    """
    Write a pandas DataFrame to a FITS binary table using fitsio.

    It is necessary to convert the pandas.DataFrame to a numpy.array
    before writing, which leads to some hit in performance.

    Parameters:
    -----------
    filename:  Base output FITS filename (over-write if already exists).
    df :       DataFrame object
    desc :     Oracle descriptor object
    fileindex: Index of this file (modifies filename based on maxfilesize)
    mode :     Write mode: 'w'=write, 'a'=append

    Returns:
    --------
    None
    """
    check_filetype(filename, FITS_EXTS)

    # Create the proper recarray dtypes
    dtypes = []
    for d in desc:
        name, otype = d[0:2]
        if otype == eatypes.or_ov:
            # Assume that Oracle OBJECTVARs are 'f8'
            # Could this be better addressed elsewhere?
            dtypes.append((name, 'f8', len(df[name].values[0])))
            print(d, dtypes[-1])
        else:
            dtypes.append((name, eatypes.oracle2fitsio(d)))

    # Create numpy array to write
    arr = np.zeros(len(df.index), dtype=dtypes)

    # fill array
    for d in desc:
        name, otype = d[0:2]
        if otype == eatypes.or_ov:
            arr[name] = np.array(df[name].values.tolist())
        else:
            arr[name][:] = df[name].values

    # write or append...
    if mode == 'w':
        # assume that this is smaller than the max size!
        if os.path.exists(filename): os.remove(filename)
        fits = fitsio.FITS(filename, mode='rw')
        created = datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S')
        fits.write(arr)
        fits[1].write_history('Created by easyaccess ' + version.__version__ + ' on ' + created)
        fits[1].write_comment('Query = ' + query)
        fits.close()
        # fitsio.write(filename, arr, clobber=True)
    elif mode == 'a':
        # just append
        fits = fitsio.FITS(filename, mode='rw')
        fits[1].append(arr)
        fits.close()
    else:
        msg = "Illegal write mode!"
        raise Exception(msg)


def read_file(filename):
    """
    Read an input file with pandas or fitsio. 

    Unfortunately, the conversion between pandas and numpy is too slow
    to put data into a consistent framework.  

    Accepted file extensions are defined by 'FILE_EXTS'.

    Parameters:
    ----------
    filename : Input filename
    
    Returns:
    --------
    data    : pandas.DataFrame or fitsio.FITS object
    """
    base, ext = os.path.splitext(filename)
    check_filetype(ext, FILE_EXTS)

    if ext in PANDAS_EXTS:
        data = read_pandas(filename)
    elif ext in FITS_EXTS:
        data = read_fitsio(filename)
    else:
        raise IOError()
    return data


def get_dtypes(df):
    """
    Get dtypes from pandas DataFrame or fitsio.FITS
    """
    if df.file_type == 'pandas':
        dtypes = [df[c].dtype if df[c].dtype.kind != 'O'
                  else np.dtype('S' + str(max(df[c].str.len())))
                  for i, c in enumerate(df)]

    if df.file_type == 'fits':
        dtype = df[1].get_rec_dtype(vstorage='fixed')[0]
        dtypes = [dtype[i] for i, d in enumerate(dtype.descr)]
    return dtypes


def read_pandas(filename):
    """
    Read an input file into a pandas DataFrame.  Accepted file
    extension defined by 'PANDAS_EXTS'.

    Parameters:
    ----------
    filename : Input filename
    
    Returns:
    --------
    df : pandas.DataFrame object
    """
    # ADW: Pandas does a pretty terrible job of automatic typing
    base, ext = os.path.splitext(filename)
    check_filetype(filename, PANDAS_EXTS)

    try:
        if ext in ('.csv', '.tab'):
            if ext == '.csv': sepa = ','
            if ext == '.tab': sepa = None
            df = pd.read_csv(filename, sep=sepa, iterator = True)
            iterator = True
        elif ext in ('.h5'):
            df = pd.read_hdf(filename, key='data')  # iterator for hdf in padnas 0.18
            iterator = False
    except:
        msg = 'Problem reading %s\n' % filename
        raise IOError(msg)

    # Monkey patch to grab columns and values
    # List comprehension is faster but less readable
    #dtypes = [df[c].dtype if df[c].dtype.kind != 'O'
    #          else np.dtype('S' + str(max(df[c].str.len())))
    #          for i, c in enumerate(df)]

    #df.ea_get_columns = df.columns.values.tolist
    #df.ea_get_values = df.values.tolist
    #df.ea_get_dtypes = lambda: dtypes
    df.file_type = 'pandas'

    return df, iterator


def read_fitsio(filename):
    """Read an input FITS file into a numpy recarray. Accepted file
    extensions defined by 'FITS_EXTS'.

    Parameters:
    ----------
    filename : Input filename
    
    Returns:
    --------
    fits : fitsio.FITS object
    """
    check_filetype(filename, FITS_EXTS)
    try:
        fits = fitsio.FITS(filename)
    except:
        msg = 'Problem reading %s\n' % filename
        raise IOError(msg)
    # Monkey patch to grab columns and values
    #dtype = fits[1].get_rec_dtype(vstorage='fixed')[0]
    #dtypes = [dtype[i] for i, d in enumerate(dtype.descr)]

    #fits.ea_get_columns = fits[1].get_colnames
    #fits.ea_get_values = fits[1].read().tolist
    #fits.ea_get_dtypes = lambda: dtypes
    fits.file_type = 'fits'

    # ## # Hack to just get a subset of columns
    ### x1,x2 = 25,37
    ###  
    ### fits.ea_get_columns = lambda: fits[1].get_colnames()[x1:x2]
    ### print fits.ea_get_columns()
    ### fits.ea_get_values = lambda: fits[1].read(columns=fits.ea_get_columns()).tolist()
    ### fits.ea_get_dtypes = lambda: dtypes[x1:x2]

    return fits, True


if __name__ == "__main__":
    import argparse

    description = __doc__
    parser = argparse.ArgumentParser(description=description)
    args = parser.parse_args()
