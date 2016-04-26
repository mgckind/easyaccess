#!/usr/bin/env python
"""
Script for testing table upload.
"""
__author__ = "Alex Drlica-Wagner"
import os
from os.path import splitext
from glob import glob
import subprocess as sub

import numpy as np
import pandas as pd
import fitsio
import easyaccess as ea

BASENAME = 'load_table_test'
BASENAME2 = BASENAME+'2'
BASENAME3 = BASENAME+'3'

def create_test_data():
    r = np.linspace(0,360,180)
    d = np.linspace(-90,90,90)
    ra,dec = np.meshgrid(r,d)
    dtype = [('ra',float),('dec',float)]
    return np.rec.fromarrays([ra.flat,dec.flat],dtype=dtype)

def create_test_fits(filename=None,data=None):
    if filename is None: filename = BASENAME+'.fits'
    if data is None: data = create_test_data()
    fitsio.write(filename,data)
    return filename

def create_test_csv(filename=None,data=None):
    if filename is None: filename = BASENAME+'.csv'
    if data is None: data = create_test_data()
    df = pd.DataFrame(data)
    df.to_csv(filename,index=False, float_format='%.8f', sep=',')
    return filename
    
def create_test_tab(filename=None,data=None):
    if filename is None: filename = BASENAME+'.tab'
    if data is None: data = create_test_data()
    df = pd.DataFrame(data)
    df.to_csv(filename,index=False, float_format='%.8f', sep='\t')
    return filename

if __name__ == "__main__":
    import argparse
    description = __doc__
    parser = argparse.ArgumentParser(description=description)
    args = parser.parse_args()

    data = create_test_data()
    nrows = len(data)

    # Create the data files
    fitsfile=create_test_fits(data=data)
    csvfile =create_test_csv(data=data)
    tabfile =create_test_tab(data=data)

    filenames = [fitsfile,csvfile,tabfile]

    # Try loading through the python interface
    # NOTE: This requires a desservice.ini file
    conn = ea.connect()

    query = 'select * from %s'%BASENAME

    # Complains when the table doesn't exist, we could add: 
    # if conn.check_table_exists(BASENAME): conn.drop_table(BASENAME)
    
    for filename in filenames:
        # First try loading through python interface
        print("*** TESTING PYTHON INTERFACE ***")
        conn.drop_table(BASENAME)
        conn.load_table(filename)
        df = conn.query_to_pandas(query)
        assert len(df) == nrows

        # Then try loading with explicit tablename
        print("*** TESTING PYTHON INTERFACE ***")
        conn.drop_table(BASENAME)
        conn.load_table(filename,BASENAME)
        df = conn.query_to_pandas(query)
        assert len(df) == nrows        

        # Then try loading through interactive interface
        print("*** TESTING INTERACTIVE INTERFACE ***")
        conn.drop_table(BASENAME)
        cmd = 'load_table %s'%filename
        conn.onecmd(cmd)
        df = conn.query_to_pandas(query)
        assert len(df) == nrows        
        
        # Then try from the command line
        print("*** TESTING COMMAND LINE INTERFACE ***")
        conn.drop_table(BASENAME)
        cmd = 'easyaccess --load_table %s'%filename
        print cmd
        sub.check_call(cmd,shell=True)
        df = conn.query_to_pandas(query)
        assert len(df) == nrows        

        # Now try downloading a re-uploading
        print("*** TESTING RE-UPLOAD ***")
        filename2 = BASENAME2 + splitext(filename)[-1]
        conn.query_and_save(query,filename2)
        conn.drop_table(BASENAME2)
        conn.load_table(filename2)
        query2 = 'select * from %s'%(BASENAME2)
        df2 = conn.query_to_pandas(query2)
        assert len(df2) == len(data)


    # Now try grabbing from existing table
    nrows = 1000
    query = 'select RA,DEC from Y1A1_COADD_OBJECTS@DESSCI where rownum <= %s;'%nrows
    for ext in ('.fits','.csv','.tab'):
        print ("*** DOWNLOADING EXISTING TABLE ***")
        basename = BASENAME+'3'
        filename = basename + ext
        conn.query_and_save(query,filename)

        # Test through python interface
        print("*** TESTING PYTHON INTERFACE ***")
        conn.drop_table(basename)
        conn.load_table(filename)
        df = conn.query_to_pandas('select * from %s'%basename)
        assert len(df) == nrows

        # Then try loading through interactive interface
        print("*** TESTING INTERACTIVE INTERFACE ***")
        conn.drop_table(basename)
        cmd = 'load_table %s'%filename
        conn.onecmd(cmd)
        df = conn.query_to_pandas('select * from %s'%basename)
        assert len(df) == nrows        
        
        # Then try from the command line
        print("*** TESTING COMMAND LINE INTERFACE ***")
        conn.drop_table(basename)
        cmd = 'easyaccess --load_table %s'%filename
        print cmd
        sub.check_call(cmd,shell=True)
        df = conn.query_to_pandas('select * from %s'%basename)
        assert len(df) == nrows        


    # Clean up
    for table in [BASENAME,BASENAME2,BASENAME3]:
        print("*** DROPPING TABLE %s ***"%table)
        conn.drop_table(table)
    for filename in glob('*.csv')+glob('*.fits')+glob('*.tab'):
        print("*** REMOVING FILE %s ***"%filename)
        os.remove(filename)
