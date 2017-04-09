from __future__ import print_function
import unittest
import easyaccess as ea
import numpy as np
import pandas as pd
import os
import fitsio


def create_test_data():
    r = np.linspace(0, 360, 100)
    d = np.linspace(-90, 90, 100)
    ra, dec = np.meshgrid(r, d)
    dtype = [('RA', float), ('DEC', float)]
    return np.rec.fromarrays([ra.flat, dec.flat], dtype=dtype)


class TestInterpreter(unittest.TestCase):

    conf = ea.config_mod.get_config(ea.config_file)
    conf.set('display', 'loading_bar', 'no')
    db = conf.get('easyaccess', 'database')
    desconf = ea.config_mod.get_desconfig(ea.desfile, db)
    con = ea.easy_or(conf, desconf, db, interactive=False, quiet=True, refresh=False)
    con2 = ea.connect(quiet=True)
    tablename = 'testtable'
    nrows = 10000
    prefetch = 4000
    chunk = 1000
    memsize = 1
    sqlfile = 'temp.sql'
    csvfile = 'temp.csv'
    fitsfile = 'temp.fits'
    h5file = 'temp.h5'

    def test_describe(self):
        print('\n*** test_describe ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual(len(df), self.nrows)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        command = 'describe_table %s;' % self.tablename.upper()
        self.con.onecmd(command)
        self.con.drop_table(self.tablename)
        os.remove(self.csvfile)

    def test_add_comment(self):
        print('\n*** test_add_comment ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual(len(df), self.nrows)
        self.con.drop_table(self.tablename)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
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
        print('\n*** test_select ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual(len(df), self.nrows)
        self.con.drop_table(self.tablename)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        command = "select RA,DEC from %s ;" % self.tablename.upper()
        self.con.onecmd(command)
        self.con.drop_table(self.tablename)
        os.remove(self.csvfile)

    def test_select_csv(self):
        print('\n*** test_select_csv ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual(len(df), self.nrows)
        self.con.drop_table(self.tablename)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        command = "select RA,DEC from %s ; > %s" % (self.tablename.upper(), self.csvfile)
        self.con.onecmd(command)
        self.assertTrue(os.path.exists(self.csvfile))
        os.remove(self.csvfile)
        self.con.drop_table(self.tablename)

    def test_select_fits(self):
        print('\n*** test_select_fits ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual(len(df), self.nrows)
        self.con.drop_table(self.tablename)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        os.remove(self.csvfile)
        command = "select RA,DEC from %s ; > %s" % (self.tablename.upper(), self.fitsfile)
        self.con.onecmd(command)
        self.assertTrue(os.path.exists(self.fitsfile))
        os.remove(self.fitsfile)
        self.con.drop_table(self.tablename)

    def test_select_hdf5(self):
        print('\n*** test_select_hdf5 ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual(len(df), self.nrows)
        self.con.drop_table(self.tablename)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        os.remove(self.csvfile)
        command = "select RA,DEC from %s ; > %s" % (self.tablename.upper(), self.h5file)
        self.con.onecmd(command)
        self.assertTrue(os.path.exists(self.h5file))
        os.remove(self.h5file)
        self.con.drop_table(self.tablename)

    def test_select_by_chunks(self):
        print('\n*** test_select_by_chunks ***\n')
        global load_bar
        load_bar = False
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual(len(df), self.nrows)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
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
        self.assertEqual(len(fetch), self.nrows * 35)
        command = "prefetch set 30000"
        self.con.onecmd(command)
        self.con.outfile_max_mb = 1
        command = "select RA,DEC from %s ; > %s" % (self.tablename.upper(), self.csvfile)
        self.con.onecmd(command)
        for i in range(6):
            self.assertTrue(os.path.exists(os.path.splitext(
                self.csvfile)[0] + '_00000' + str(i + 1) + '.csv'))
            os.remove(os.path.splitext(self.csvfile)[0] + '_00000' + str(i + 1) + '.csv')
        self.con.outfile_max_mb = 1000
        self.con.drop_table(self.tablename)
        if os.path.exists(self.csvfile):
            os.remove(self.csvfile)

    def test_load_append_table_csv(self):
        print('\n*** test_load_append_table_csv ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual(len(df), self.nrows)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        self.assertTrue(os.path.exists(self.csvfile))
        self.con.drop_table(os.path.splitext(self.csvfile)[0].upper())
        # name from filename
        command = "load_table %s " % self.csvfile
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % os.path.splitext(self.csvfile)[0].upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)

        # appending
        command = "append_table %s " % self.csvfile
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % os.path.splitext(self.csvfile)[0].upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 2)
        self.con.drop_table(os.path.splitext(self.csvfile)[0].upper())
        os.remove(self.csvfile)

    def test_load_append_table_name_csv(self):
        print('\n*** test_load_append_table_name_csv ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual(len(df), self.nrows)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        self.assertTrue(os.path.exists(self.csvfile))
        # name from tablename
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        # appending
        command = "append_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 2)
        self.con.drop_table(self.tablename)
        os.remove(self.csvfile)

    def test_load_append_table_chunk_csv(self):
        print('\n*** test_load_append_table_chunk_csv ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual(len(df), self.nrows)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        self.assertTrue(os.path.exists(self.csvfile))
        # chunksize
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s --chunksize %s" % (
            self.csvfile, self.tablename, self.chunk)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        # appending
        command = "append_table %s --tablename %s --chunksize %s" % (
            self.csvfile, self.tablename, self.chunk)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 2)

    def test_load_append_table_memory_csv(self):
        print('\n*** test_load_append_table_memory_csv ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual(len(df), self.nrows)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        for i in range(9):
            df.to_csv(self.csvfile, index=False, float_format='%.8f',
                      sep=',', mode='a', header=False)
        self.assertTrue(os.path.exists(self.csvfile))
        # memsize
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s --memsize %s" % (
            self.csvfile, self.tablename, self.memsize)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 10)
        # appending
        command = "append_table %s --tablename %s --memsize %s" % (
            self.csvfile, self.tablename, self.memsize)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 20)
        # end
        os.remove(self.csvfile)
        self.con.drop_table(self.tablename)

    def test_load_append_table_memory_chunk_csv(self):
        print('\n*** test_load_append_table_memory_chunk_csv ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        self.assertEqual(len(df), self.nrows)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        for i in range(9):
            df.to_csv(self.csvfile, index=False, float_format='%.8f',
                      sep=',', mode='a', header=False)
        self.assertTrue(os.path.exists(self.csvfile))
        # memsize
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s --memsize %s --chunksize %s" % (
            self.csvfile, self.tablename, self.memsize, self.chunk * 10)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 10)
        # appending
        command = "append_table %s --tablename %s --memsize %s --chunksize %s" % (
            self.csvfile, self.tablename, self.memsize, self.chunk * 200)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 20)
        # end
        os.remove(self.csvfile)
        self.con.drop_table(self.tablename)

    def test_load_append_table_fits(self):
        print('\n*** test_load_append_table_fits ***\n')
        data = create_test_data()
        fitsio.write(self.fitsfile, data, clobber=True)
        self.assertTrue(os.path.exists(self.fitsfile))
        self.con.drop_table(os.path.splitext(self.fitsfile)[0].upper())
        # name from filename
        command = "load_table %s " % self.fitsfile
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % os.path.splitext(self.fitsfile)[0].upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)

        # appending
        command = "append_table %s " % self.fitsfile
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % os.path.splitext(self.fitsfile)[0].upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 2)
        self.con.drop_table(os.path.splitext(self.fitsfile)[0].upper())
        os.remove(self.fitsfile)

    def test_load_append_table_name_fits(self):
        print('\n*** test_load_append_table_name_fits ***\n')
        data = create_test_data()
        fitsio.write(self.fitsfile, data, clobber=True)
        self.assertTrue(os.path.exists(self.fitsfile))
        # name from tablename
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s" % (self.fitsfile, self.tablename)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        # appending
        command = "append_table %s --tablename %s" % (self.fitsfile, self.tablename)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 2)
        os.remove(self.fitsfile)
        self.con.drop_table(self.tablename)

    def test_load_append_table_chunk_fits(self):
        print('\n*** test_load_append_table_chunk_fits ***\n')
        data = create_test_data()
        fitsio.write(self.fitsfile, data, clobber=True)
        self.assertTrue(os.path.exists(self.fitsfile))
        # chunksize
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s --chunksize %s" % (
            self.fitsfile, self.tablename, self.chunk)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows)
        # appending
        command = "append_table %s --tablename %s --chunksize %s" % (
            self.fitsfile, self.tablename, self.chunk)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 2)
        os.remove(self.fitsfile)
        self.con.drop_table(self.tablename)

    def test_load_append_table_memory_fits(self):
        print('\n*** test_load_append_table_memory_fits ***\n')
        data = create_test_data()
        for i in range(4):
            data = np.concatenate((data, data))
        fitsio.write(self.fitsfile, data, clobber=True)
        self.assertTrue(os.path.exists(self.fitsfile))
        # memsize
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s --memsize %s" % (
            self.fitsfile, self.tablename, self.memsize)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 16)
        # appending
        command = "append_table %s --tablename %s --memsize %s" % (
            self.fitsfile, self.tablename, self.memsize)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 2 * 16)
        # end
        os.remove(self.fitsfile)
        self.con.drop_table(self.tablename)

    def test_load_append_table_memory_chunk_fits(self):
        print('\n*** test_load_append_table_memory_chunk_fits ***\n')
        data = create_test_data()
        for i in range(4):
            data = np.concatenate((data, data))
        fitsio.write(self.fitsfile, data, clobber=True)
        self.assertTrue(os.path.exists(self.fitsfile))
        # memsize
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s --memsize %s --chunksize %s" % (
            self.fitsfile, self.tablename, self.memsize, self.chunk * 10)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 16)
        # appending
        command = "append_table %s --tablename %s --memsize %s --chunksize %s" % (
            self.fitsfile, self.tablename, self.memsize, self.chunk * 200)
        self.con.onecmd(command)
        cursor = self.con2.cursor()
        temp = cursor.execute('select RA,DEC from %s' % self.tablename.upper())
        fetch = temp.fetchall()
        self.assertEqual(len(fetch), self.nrows * 2 * 16)
        # end
        os.remove(self.fitsfile)
        self.con.drop_table(self.tablename)

    def test_loadsql(self):
        print('\n*** test_loadsql ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        self.assertTrue(os.path.exists(self.csvfile))
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        os.remove(self.csvfile)
        query = """
        -- This is a comment
        select RA, DEC from %s -- this is another comment
         ; > %s
        """ % (self.tablename, self.csvfile)
        with open(self.sqlfile, 'w') as F:
            F.write(query)

        command = "loadsql %s" % (self.sqlfile)
        self.con.onecmd(command)
        self.assertTrue(os.path.exists(self.csvfile))
        df = pd.read_csv(self.csvfile, sep=',')
        self.assertEqual(len(df), self.nrows)
        os.remove(self.csvfile)
        self.assertFalse(os.path.exists(self.csvfile))
        os.remove(self.sqlfile)

    @unittest.skip("Need to reevaluate")
    def test_inline(self):
        print('\n*** test_inline ***\n')
        data = create_test_data()
        df = pd.DataFrame(data)
        df.to_csv(self.csvfile, index=False, float_format='%.8f', sep=',')
        self.assertTrue(os.path.exists(self.csvfile))
        self.con.drop_table(self.tablename)
        command = "load_table %s --tablename %s" % (self.csvfile, self.tablename)
        self.con.onecmd(command)
        command = "import wrapped as Y"
        self.con.onecmd(command)
        command = "select /*p: Y.my_sum(ra,dec) as testcol */, dec from %s ; > %s" % (
            self.tablename, self.csvfile)
        self.con.onecmd(command)
        self.assertTrue(os.path.exists(self.csvfile))
        df = pd.read_csv(self.csvfile, sep=',')
        self.assertEqual(len(df), self.nrows)
        self.assertTrue('TESTCOL' in df.columns.values.tolist())
        os.remove(self.csvfile)
        self.assertFalse(os.path.exists(self.csvfile))
        self.con.drop_table(self.tablename)


if __name__ == '__main__':
    unittest.main()
