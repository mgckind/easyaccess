import unittest
import easyaccess as ea
import numpy as np
import pandas as pd
import os
import fitsio

def create_test_data():
    r = np.linspace(0,360,100)
    d = np.linspace(-90,90,100)
    ra,dec = np.meshgrid(r,d)
    dtype = [('RA',float),('DEC',float)]
    return np.rec.fromarrays([ra.flat,dec.flat],dtype=dtype)

class TestInterpreter(unittest.TestCase):
    
    conf = ea.config_mod.get_config(ea.config_file)
    conf.set('display', 'loading_bar', 'no')
    db = conf.get('easyaccess', 'database')
    desconf = ea.config_mod.get_desconfig(ea.desfile, db)
    con=ea.easy_or(conf, desconf, db, interactive=False, quiet=True, refresh=False)
    con2=ea.connect(quiet=True)
    tablename = 'testtable'
    nrows = 10000
    prefetch = 4000
    chunk = 1000
    sqlfile = 'temp.sql'
    csvfile = 'temp.csv'
    fitsfile = 'temp.fits'
    h5file = 'temp.h5'

    def test_describe(self):
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        df.to_csv(self.csvfile,index=False, float_format='%.8f', sep=',')
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        command = 'describe_table %s;' % self.tablename.upper()
        self.con.onecmd(command)
        self.con.drop_table(self.tablename)
        os.remove(self.csvfile)
    
    def test_help(self):
        command = 'help'
        self.con.onecmd(command)
        command = '?'
        self.con.onecmd(command)

    def test_add_comment(self):
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        self.con.drop_table(self.tablename)
        df.to_csv(self.csvfile,index=False, float_format='%.8f', sep=',')
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        command = "add_comment table %s 'Test table'" % self.tablename.upper()
        self.con.onecmd(command)
        command = "add_comment column %s.RA 'Coordinate'" % self.tablename.upper()
        self.con.onecmd(command)
        command = 'describe_table %s;' % self.tablename.upper()
        self.con.onecmd(command)
        self.con.drop_table(self.tablename)
        os.remove(self.csvfile)

    def test_select(self):
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        self.con.drop_table(self.tablename)
        df.to_csv(self.csvfile,index=False, float_format='%.8f', sep=',')
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        command = "select RA,DEC from %s ;" % self.tablename.upper()
        self.con.onecmd(command)
        self.con.drop_table(self.tablename)
        os.remove(self.csvfile)
        
    def test_select_csv(self):
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        self.con.drop_table(self.tablename)
        df.to_csv(self.csvfile,index=False, float_format='%.8f', sep=',')
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        command = "select RA,DEC from %s ; > %s" % (self.tablename.upper(), self.csvfile)
        self.con.onecmd(command)
        self.assertTrue(os.path.exists(self.csvfile))
        os.remove(self.csvfile)
        self.con.drop_table(self.tablename)


    def test_select_fits(self):
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        self.con.drop_table(self.tablename)
        df.to_csv(self.csvfile,index=False, float_format='%.8f', sep=',')
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        os.remove(self.csvfile)
        command = "select RA,DEC from %s ; > %s" % (self.tablename.upper(), self.fitsfile)
        self.con.onecmd(command)
        self.assertTrue(os.path.exists(self.fitsfile))
        os.remove(self.fitsfile)
        self.con.drop_table(self.tablename)
    
    def test_select_hdf5(self):
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        self.con.drop_table(self.tablename)
        df.to_csv(self.csvfile,index=False, float_format='%.8f', sep=',')
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        os.remove(self.csvfile)
        command = "select RA,DEC from %s ; > %s" % (self.tablename.upper(), self.h5file)
        self.con.onecmd(command)
        self.assertTrue(os.path.exists(self.h5file))
        os.remove(self.h5file)
        self.con.drop_table(self.tablename)

    def test_select_by_chunks(self):
        global load_bar
        load_bar = False
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        df.to_csv(self.csvfile,index=False, float_format='%.8f', sep=',')
        self.assertTrue(os.path.exists(self.csvfile))
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        for i in range(34):
            command = "append_table %s --tablename %s" % (self.csvfile, self.tablename)
            self.con.onecmd(command)
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*35)
        command = "prefetch set 30000"
        self.con.onecmd(command)
        self.con.outfile_max_mb = 1
        command = "select RA,DEC from %s ; > %s" % (self.tablename.upper(), self.csvfile)
        self.con.onecmd(command)
        for i in range(6):
            self.assertTrue(os.path.exists(os.path.splitext(self.csvfile)[0]+'_00000'+str(i+1)+'.csv'))
            os.remove(os.path.splitext(self.csvfile)[0]+'_00000'+str(i+1)+'.csv')
        self.con.outfile_max_mb = 1000
        self.con.drop_table(self.tablename)
        if os.path.exists(self.csvfile): os.remove(self.csvfile)


## TODO: 
# multiple files (prefetch)
# load_table
# append_table
# exceproc
# execute
# import and inline query
# loadsql and @
# find_


if __name__ == '__main__':
    unittest.main()
