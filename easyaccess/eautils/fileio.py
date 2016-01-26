#!/usr/bin/env python
"""
Module for dealing with file input/output with
pandas, fitsio, ...

Some useful documentation:
fitsio: https://github.com/esheldon/fitsio
numpy: 
pandas: 

"""
import os
import numpy as np
import fitsio

try:
    import easyaccess.eautils.dtypes as eatypes
except ImportError:
    import eautils.dtypes as eatypes


def write_file(filename, data, desc, fileindex=1, mode='w',max_mb=1000):
    """
    Write data to a file. Append to existing file as long as smaller
    than specified size.  Create a new file (and increment fileindex)
    when file grows too large.

    Parameters:
    -----------
    filename : Output base filename (incremented by 'fileindex')
    data :     The data to write to the file
    desc :     The Oracle data descriptor
    fileindex: The index of the file to write.
    mode :     The write-mode: 'w'=write new file, 'a'=append to existing file
    max_mb :   Maximum file size.
    
    Returns:
    fileindex: The (possibly incremented) fileindex.
    """
    # 'fileindex' is 1-indexed for backwards compatibility
    fileout = filename
    mode_write = mode

    for i, col in enumerate(data):
        nt = eatypes.oracle2numpy(desc[i])
        if nt != "": data[col] = data[col].astype(nt)

    fileparts = os.path.splitext(filename)
    base,ext = fileparts

    if mode == 'w':
        header_out = True
    if mode == 'a':
        if (fileindex == 1):
            thisfile = filename
        else:
            thisfile = base+'_%06d'%fileindex+ext 

        # check the size of the current file
        size = float(os.path.getsize(thisfile)) / (2. ** 20)

        if (size > max_mb):
            # it's time to increment the file
            if (fileindex == 1):
                # this is the first one ... it needs to be moved
                lastfile = base+'_%06d'%fileindex+ext
                os.rename(filename, )

            # and make a new filename, after incrementing
            fileindex += 1

            thisfile = base+'_%06d'%fileindex+ext 
            fileout = thisfile
            mode = 'w'
            header_out = True
        else:
            fileout = thisfile
            header_out = False

    if ext in ('.csv','.tab','.h5'):
        write_pandas(fileout, data, fileindex, mode=mode, header=header_out)
    elif ext == '.fits': 
        write_fitsio(fileout, data, desc, fileindex, mode=mode)
    else:
        msg = "Unrecognized file type: '%s'"%mode
        raise IOError(msg)

    return fileindex

def write_pandas(filename, df, fileindex, mode='w', header=True):
    """
    Write a pandas DataFrame to a file. Accepted extensions are:
    '.csv', '.tab', '.h5'

    Parameters:
    -----------
    
    Returns:
    --------
    """
    base,ext = os.path.splitext(filename)

    if ext == '.csv': 
        df.to_csv(filename, index=False, float_format='%.8f', sep=',',
                  mode=mode, header=header)
    elif ext == '.tab':
        df.to_csv(filename, index=False, float_format='%.8f', sep=' ',
                  mode=mode, header=header)
    elif ext == '.h5':
        df.to_hdf(filename, 'data', mode=mode, index=False,
                  header=header)  # , complevel=9,complib='bzip2'
    else:
        msg = "Unrecognized file type: '%s'"%ext
        raise IOError(msg)


def write_fitsio(filename, df, desc, fileindex, mode='w', max_mb=1000):
    """
    Write a pandas DataFrame to a FITS binary table using fitsio.

    Parameters:
    -----------
    filename:  Base output FITS filename (over-write if already exists).
    df :       DataFrame object
    desc :     Oracle descriptor object
    fileindex: Index of this file (modifies filename based on maxfilesize)
    mode :     Write mode: 'w'=write, 'a'=append
    maxmb :    Maximum filesize in MB.

    Returns:
    --------
    returns : None
    """
    # Create the proper recarray dtypes
    dtypes = []
    for d in desc:
        name,otype = d[0:2]
        if otype == eatypes.or_ov:
            # Assume that Oracle OBJECTVARs are 'f8'
            dtypes.append((name,'f8',len(df[name].values[0])))
        else:
            dtypes.append((name,eatypes.oracle2fitsio(d)))

    # Create numpy array to write
    arr = np.zeros(len(df.index), dtype=dtypes)

    # fill array
    for d in desc:
        name,otype = d[0:2]
        if otype == eatypes.or_ov:
            arr[name] = np.array(df[name].values.tolist)
        else:
            arr[name][:] = df[name].values

    # write or append...
    if mode == 'w':
        # assume that this is smaller than the max size!
        if os.path.exists(filename): os.remove(filename)
        fitsio.write(filename, arr, clobber=True)
    elif mode == 'a':
        # just append
        fits = fitsio.FITS(filename, mode='rw')
        fits[1].append(arr)
        fits.close()
    else:
        msg = "Illegal write mode!"
        raise Exception(msg)

if __name__ == "__main__":
    import argparse
    description = __doc__
    parser = argparse.ArgumentParser(description=description)
    args = parser.parse_args()
