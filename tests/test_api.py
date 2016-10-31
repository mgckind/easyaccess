from __future__ import print_function
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

class TestApi(unittest.TestCase):
    
    con = ea.connect(quiet=True)
    tablename = 'testtable'
    nrows = 10000
    prefetch = 4000
    chunk = 1000
    memsize = 1
    sqlfile = 'temp.sql'
    csvfile = 'temp.csv'
    fitsfile = 'temp.fits'
    h5file = 'temp.h5'

    
    def test_ea_import(self):
        print('\n*** test_ea_import ***\n')
        test1 = self.con.ea_import('wrapped')
        if test1 is not None:
            self.assertTrue(test1)
        test2 = self.con.ea_import('wrapped',  help=True)
        if test2 is not None:
            self.assertTrue(test2)

    def test_pandas_to_db(self):
        print('\n*** test_pandas_to_db ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        try:
            self.con.drop_table(self.tablename)
        except:
            pass
        self.assertTrue(self.con.pandas_to_db(df, tablename=self.tablename))
        cursor = self.con.cursor()
        self.assertTrue(self.con.ping())
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        # appending
        self.assertTrue(self.con.pandas_to_db(df, tablename=self.tablename, append=True))
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*2)
        self.con.drop_table(self.tablename)
        self.assertTrue(self.con.pandas_to_db(df, tablename=self.tablename, append=True))
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        self.con.drop_table(self.tablename)
        cursor.close()

    def test_query_to_pandas(self):
        print('\n*** test_query_to_pandas ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        try:
            self.con.drop_table(self.tablename)
        except:
            pass
        self.assertTrue(self.con.pandas_to_db(df, tablename=self.tablename))
        query = 'select RA,DEC from {:}'.format(self.tablename.upper())
        df2 = self.con.query_to_pandas(query)
        self.assertEqual( len(df), len(df2))
        self.assertEqual( df.columns.values.tolist().sort(), df2.columns.values.tolist().sort())
        #iterator
        df3 = self.con.query_to_pandas(query, prefetch=4000, iterator=True)
        self.assertEqual( len(df3.next()), 4000)
        self.assertEqual( df3.next().columns.values.tolist().sort(), df.columns.values.tolist().sort())
        self.assertEqual( len(df3.next()), 2000)
        self.con.drop_table(self.tablename)

    def test_describe_table(self):
        print('\n*** test_describe_table ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        try:
            self.con.drop_table(self.tablename)
        except:
            pass
        self.assertTrue(self.con.pandas_to_db(df, tablename=self.tablename))
        self.assertEqual(len(self.con.describe_table(self.tablename)), 2)
        self.con.drop_table(self.tablename)

    def test_loadsql(self):
        print('\n*** test_loadsql ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        try:
            self.con.drop_table(self.tablename)
        except:
            pass
        self.assertTrue(self.con.pandas_to_db(df, tablename=self.tablename))
        query = """
        -- This is a comment
        select RA, DEC from %s -- this is another comment
        """ % self.tablename
        with open(self.sqlfile,'w') as F: F.write(query)
        df2 = self.con.query_to_pandas(self.con.loadsql(self.sqlfile))
        self.assertEqual( len(df), len(df2))
        self.assertEqual( df.columns.values.tolist().sort(), df2.columns.values.tolist().sort())
        query = """
        -- This is a comment
        select RA, DEC from %s ; -- this is another comment
        """ % self.tablename
        with open(self.sqlfile,'w') as F: F.write(query)
        df2 = self.con.query_to_pandas(self.con.loadsql(self.sqlfile))
        self.assertEqual( len(df), len(df2))
        self.assertEqual( df.columns.values.tolist().sort(), df2.columns.values.tolist().sort())
        self.con.drop_table(self.tablename)
        os.remove(self.sqlfile)


    def test_mytables(self):
        print('\n*** test_mytables ***\n')
        df = self.con.mytables()
        self.assertTrue('FGOTTENMETADATA' in df['TABLE_NAME'].values.tolist())


    def test_load_table_csv(self):
        print('\n*** test_load_table_csv ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        self.assertTrue(os.path.exists(self.csvfile))
        self.con.drop_table(os.path.splitext(self.csvfile)[0].upper())
        # name from filename
        self.assertTrue(self.con.load_table(self.csvfile))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % os.path.splitext(self.csvfile)[0].upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        ## appending
        self.assertTrue(self.con.append_table(self.csvfile))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % os.path.splitext(self.csvfile)[0].upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*2)
        self.con.drop_table(os.path.splitext(self.csvfile)[0].upper())
        # name from tablename
        self.con.drop_table(self.tablename)
        self.assertTrue(self.con.load_table(self.csvfile, name=self.tablename))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        ## appending
        self.assertTrue(self.con.append_table(self.csvfile, name=self.tablename))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*2)
        # chunksize
        self.con.drop_table(self.tablename)
        self.assertTrue(self.con.load_table(self.csvfile, name=self.tablename, chunksize=self.chunk))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        ## appending
        self.assertTrue(self.con.append_table(self.csvfile, name=self.tablename, chunksize=self.chunk))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*2)
        self.con.drop_table(self.tablename)
        os.remove(self.csvfile)


    def test_load_append_table_memory_csv(self):
        print('\n*** test_load_append_table_memory_csv ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        for i in range(9):
            df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',',mode='a',header=False)
        self.assertTrue(os.path.exists(self.csvfile))
        # memsize
        self.con.drop_table(self.tablename)
        self.assertTrue(self.con.load_table(self.csvfile, name=self.tablename, memsize=self.memsize))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*10)
        # appending
        self.assertTrue(self.con.append_table(self.csvfile, name=self.tablename, memsize=self.memsize))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*20)
        ## end
        os.remove(self.csvfile)
        self.con.drop_table(self.tablename)

    def test_load_append_table_memory_chunk_csv(self):
        print('\n*** test_load_append_table_memory_chunk_csv ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        for i in range(9):
            df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',',mode='a',header=False)
        self.assertTrue(os.path.exists(self.csvfile))
        # memsize
        self.con.drop_table(self.tablename)
        self.assertTrue(self.con.load_table(self.csvfile, name=self.tablename, memsize=self.memsize, chunksize=self.chunk*10))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*10)
        # appending
        self.assertTrue(self.con.append_table(self.csvfile, name=self.tablename, memsize=self.memsize, chunksize=self.chunk*200))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*20)
        ## end
        os.remove(self.csvfile)
        self.con.drop_table(self.tablename)

    def test_load_table_fits(self):
        print('\n*** test_load_table_fits ***\n')
        data = create_test_data()
        fitsio.write(self.fitsfile, data, clobber=True)
        self.assertTrue(os.path.exists(self.fitsfile))
        self.con.drop_table(os.path.splitext(self.fitsfile)[0].upper())
        # name from filename
        self.assertTrue(self.con.load_table(self.fitsfile))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % os.path.splitext(self.fitsfile)[0].upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        ## appending
        self.assertTrue(self.con.append_table(self.fitsfile))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % os.path.splitext(self.fitsfile)[0].upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*2)
        self.con.drop_table(os.path.splitext(self.fitsfile)[0].upper())
        # name from tablename
        self.con.drop_table(self.tablename)
        self.assertTrue(self.con.load_table(self.fitsfile, name=self.tablename))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        ## appending
        self.assertTrue(self.con.append_table(self.fitsfile, name=self.tablename))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*2)
        # chunksize
        self.con.drop_table(self.tablename)
        self.assertTrue(self.con.load_table(self.fitsfile, name=self.tablename, chunksize=self.chunk))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        ## appending
        self.assertTrue(self.con.append_table(self.fitsfile, name=self.tablename, chunksize=self.chunk))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*2)
        self.con.drop_table(self.tablename)
        os.remove(self.fitsfile)

    def test_load_append_table_memory_fits(self):
        print('\n*** test_load_append_table_memory_fits ***\n')
        data = create_test_data()
        for i in range(4):
            data = np.concatenate((data,data))
        fitsio.write(self.fitsfile, data, clobber=True)
        self.assertTrue(os.path.exists(self.fitsfile))
        # memsize
        self.con.drop_table(self.tablename)
        self.assertTrue(self.con.load_table(self.fitsfile, name=self.tablename, memsize=self.memsize))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*16)
        # appending
        self.assertTrue(self.con.append_table(self.fitsfile, name=self.tablename, memsize=self.memsize))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*2*16)
        ## end
        os.remove(self.fitsfile)
        self.con.drop_table(self.tablename)

    def test_load_append_table_memory_chunk_fits(self):
        print('\n*** test_load_append_table_memory_chunk_fits ***\n')
        data = create_test_data()
        for i in range(4):
            data = np.concatenate((data,data))
        fitsio.write(self.fitsfile, data, clobber=True)
        self.assertTrue(os.path.exists(self.fitsfile))
        # memsize
        self.con.drop_table(self.tablename)
        self.assertTrue(self.con.load_table(self.fitsfile, name=self.tablename, memsize=self.memsize, chunksize=self.chunk*10))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*16)
        # appending
        self.assertTrue(self.con.append_table(self.fitsfile, name=self.tablename, memsize=self.memsize, chunksize=self.chunk*200))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*2*16)
        ## end
        os.remove(self.fitsfile)
        self.con.drop_table(self.tablename)


    def test_load_table_hdf5(self):
        print('\n*** test_load_table_hdf5 ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        df.to_hdf(self.h5file, key='data')
        self.assertTrue(os.path.exists(self.h5file))
        self.con.drop_table(os.path.splitext(self.h5file)[0].upper())
        # name from filename
        self.assertTrue(self.con.load_table(self.h5file))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % os.path.splitext(self.h5file)[0].upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        ## appending
        self.assertTrue(self.con.append_table(self.h5file))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % os.path.splitext(self.h5file)[0].upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*2)
        self.con.drop_table(os.path.splitext(self.h5file)[0].upper())
        # name from tablename
        self.con.drop_table(self.tablename)
        self.assertTrue(self.con.load_table(self.h5file, name=self.tablename))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        ## appending
        self.assertTrue(self.con.append_table(self.h5file, name=self.tablename))
        cursor = self.con.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*2)
        self.con.drop_table(self.tablename)
        os.remove(self.h5file)


    def test_query_and_save(self):
        print('\n*** test_query_and_save ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        cursor = self.con.cursor()
        try:
            self.con.drop_table(self.tablename)
        except:
            pass
        self.assertTrue(self.con.pandas_to_db(df, tablename=self.tablename))
        query = 'select RA,DEC from %s' % self.tablename.upper()
        self.con.query_and_save(query, self.csvfile, print_time=False)
        self.assertTrue(os.path.exists(self.csvfile))
        self.con.query_and_save(query, self.fitsfile, print_time=False)
        self.assertTrue(os.path.exists(self.fitsfile))
        self.con.query_and_save(query, self.h5file, print_time=False)
        self.assertTrue(os.path.exists(self.h5file))
        os.remove(self.csvfile)
        os.remove(self.fitsfile)
        os.remove(self.h5file)
        for i in range(34):
            self.assertTrue(self.con.pandas_to_db(df, tablename=self.tablename, append=True))
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows*35)
        self.con.outfile_max_mb = 1
        self.con.query_and_save(query, self.csvfile, print_time=False)
        for i in range(4):
            self.assertTrue(os.path.exists(os.path.splitext(self.csvfile)[0]+'_00000'+str(i+1)+'.csv'))
            os.remove(os.path.splitext(self.csvfile)[0]+'_00000'+str(i+1)+'.csv')
        self.con.query_and_save(query, self.fitsfile, print_time=False)
        for i in range(4):
            self.assertTrue(os.path.exists(os.path.splitext(self.fitsfile)[0]+'_00000'+str(i+1)+'.fits'))
            os.remove(os.path.splitext(self.fitsfile)[0]+'_00000'+str(i+1)+'.fits')

        self.con.outfile_max_mb = 1000
        self.con.drop_table(self.tablename)

    def test_inline_functions(self):
        print('\n*** test_inline_functions ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual( len(df), self.nrows)
        cursor = self.con.cursor()
        try:
            self.con.drop_table(self.tablename)
        except:
            pass
        self.assertTrue(self.con.pandas_to_db(df, tablename=self.tablename))
        query = 'select /*p: Y.my_sum(ra,dec) as testcol*/ from %s' % self.tablename
        self.con.ea_import('wrapped as Y')
        df = self.con.query_to_pandas(query)
        self.assertEqual(len(df), self.nrows)
        self.assertTrue('TESTCOL' in df.columns.values.tolist())
        self.con.drop_table(self.tablename)


if __name__ == '__main__':
    unittest.main()
