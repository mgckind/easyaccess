from easyaccess.easyaccess import easy_or
import easyaccess.config_ea as config_mod 
import easyaccess.eautils.fileio as eafile
import easyaccess.eautils.fun_utils as fun_utils
import pandas as pd
import os 
import stat
import getpass

try: 
    from builtins import input, str, range 
except ImportError: 
    from __builtin__ import input, str, range

desfile = os.getenv("DES_SERVICES")
if not desfile:
    desfile = os.path.join(os.getenv("HOME"), ".desservices.ini")
if os.path.exists(desfile):
    amode = stat.S_IMODE(os.stat(desfile).st_mode)
    if amode != 2 ** 8 + 2 ** 7:
        print('Changing permissions to des_service file to read/write by user')
        os.chmod(desfile, 2 ** 8 + 2 ** 7)  # rw by user owner only    


class IterData(object):
    """
    Iterator class for cx_oracle
    """

    def __init__(self, cursor, extra_func=None):
        self.rows_count = 0
        self.cursor = cursor
        self.extra_func = extra_func
        self.data = pd.DataFrame(self.cursor.fetchmany(), columns=[
                                 rec[0] for rec in self.cursor.description])
        if self.extra_func is not None and not self.data.empty:
            funs, args, names = self.extra_func
            for kf in range(len(funs)):
                self.data = fun_utils.updateDF(
                    self.data, funs, args, names, kf)

    def __iter__(self):
        return self

    def next(self):
        if not self.data.empty:
            data = self.data
            self.rows_count += len(data)
            self.data = pd.DataFrame(self.cursor.fetchmany(), columns=[
                                     rec[0] for rec in self.cursor.description])
            if self.extra_func is not None and not self.data.empty:
                funs, args, names = self.extra_func
                for kf in range(len(funs)):
                    self.data = fun_utils.updateDF(
                        self.data, funs, args, names, kf)
            return data
        else:
            self.cursor.close()
            raise StopIteration('No more data in the DB')
            

class connect(easy_or):
    def __init__(self, section='', user=None, passwd=None, quiet=False, refresh=False):
        """
        Creates a connection to the DB as easyaccess commands, section is
         obtained from config file, can be bypass here, e.g., section = desoper

        Parameters:
        -----------
        section :  DB connection : dessci, desoper, destest
        user    :  Manualy use username
        passwd  :  password for username (if not enter is prompted)
        quiet   :  Don't print much

        Returns:
        --------
        easy_or object
        """
        self.quiet = quiet
        conf = config_mod.get_config(config_file)
        self.conf = conf
        pd.set_option('display.max_rows', conf.getint('display', 'max_rows'))
        pd.set_option('display.width', conf.getint('display', 'width'))
        pd.set_option('display.max_columns',
                      conf.getint('display', 'max_columns'))
        pd.set_option('display.max_colwidth',
                      conf.getint('display', 'max_colwidth'))
        if section == '':
            db = conf.get('easyaccess', 'database')
        else:
            db = section
        if user is not None:
            print('Bypassing .desservices file with user : %s' % user)
            if passwd is None:
                passwd = getpass.getpass(prompt='Enter password : ')
            desconf = config_mod.get_desconfig(desfile, db,
                                               verbose=False, user=user, pw1=passwd)
            desconf.set('db-' + db, 'user', user)
            desconf.set('db-' + db, 'passwd', passwd)
        else:
            desconf = config_mod.get_desconfig(desfile, db)
        easy_or.__init__(self, conf, desconf, db, interactive=False, quiet=quiet, pymod=True)
        try:
            self.cur.execute('create table FGOTTENMETADATA (ID int)')
        except:
            pass
        self.loading_bar = False

    def cursor(self):
        cursor = self.con.cursor()
        cursor.arraysize = int(self.prefetch)
        return cursor

    def ping(self, quiet=None):
        if quiet is None:
            quiet = self.quiet
        try:
            self.con.ping()
            if not quiet:
                print('Still connected to DB')
            return True
        except:
            if not quiet:
                print('Connection with DB lost')
            return False

    def close(self):
        self.con.close()

    def ea_import(self, import_line='', help=False):
        """
        Executes a import of module with functions to be used for inline query functions,
        checks whether function is wrapped @toeasyaccess and add module to library.

        Parameters:
        -----------
        import_line  : the usual line after import.
        help         : Print current loaded functions wrapped for easyaccess


        Use:
        ----
        ea_import('module as name')
        ea_import('my_module')

        Returns:
        --------

        Add functions from module to internal library to be used inline queries
        """

        if help:
            self.do_help_function('all')
            return True
        if import_line != '':
            self.do_import(' ' + import_line)
            return True

    def query_to_pandas(self, query, prefetch='', iterator=False):
        """
        Executes a query and return the results in pandas DataFrame. If result is too big
        it is better to save results to a file

        Parameters:
        -----------
        query     : The SQL query to be executed
        prefetch  : Number of rows to retrieve at each trip to the DB
        iterator  : Return interator, get data with .next() method (to avoid get all data at once)

        Returns:
        --------
        If iterator is False (default) the function returns a pandas DataFrame
        with the result of the query. If the iterator is True, it will return an iterator
        to retrieve data one piece at a time.
        """
        cursor = self.con.cursor()
        cursor.arraysize = int(self.prefetch)
        if prefetch != '':
            cursor.arraysize = int(prefetch)
        query = query.replace(';', '')
        query, funs, args, names = fun_utils.parseQ(query, myglobals=globals())
        extra_func = [funs, args, names]
        if funs is None:
            extra_func = None
        temp = cursor.execute(query)
        if temp.description is not None:
            if iterator:
                data = IterData(temp, extra_func)
            else:
                data = pd.DataFrame(temp.fetchall(), columns=[rec[0] for rec in temp.description])
                if extra_func is not None:
                    for kf in range(len(funs)):
                        data = fun_utils.updateDF(data, funs, args, names, kf)
        else:
            data = ""
        if not iterator:
            cursor.close()
        return data

    def describe_table(self, tablename):
        """
        Describes a table from the DB
        """
        return self.do_describe_table(tablename, False, return_df=True)

    def loadsql(self, filename):
        """
        Reads sql statement from a file, returns query to be parsed in
        query_and_save, query_to_pandas, etc.
        """
        query = read_buf(filename)
        if query.find(';') > -1:
            query = query.split(';')[0]
        return query

    def mytables(self):
        """
        List tables in own schema

        Returns:
        --------
        A pandas dataframe with a list of owner's tables
        """
        return self.do_mytables('', return_df=True, extra='')

    def myquota(self):
        """
        Show quota in current database
        """
        self.do_myquota('')

    def load_table(self, table_file, name=None, chunksize=None, memsize=None):
        """
        Loads and create a table in the DB. If name is not passed, is taken from
        the filename. Formats supported are 'fits', 'csv' and 'tab' files

        Parameters:
        -----------
        table_file : Filename to be uploaded as table (.csv, .fits, .tab)
        name       : Name of the table to be created
        chunksize  : Number of rows to upload at a time to avoid memory issues
        memsize    : Size of chunk to be read. In Mb.
                     If both specified, the lower number of rows is selected

        Returns:
        --------
        True if success otherwise False

        """
        try:
            self.do_load_table(table_file, name=name, chunksize=chunksize, memsize=memsize)
            return True
        except:
            # exception
            return False

    def append_table(self, table_file, name=None, chunksize=None, memsize=None):
        """
        Appends data to a table in the DB. If name is not passed, is taken from
        the filename. Formats supported are 'fits', 'csv' and 'tab' files

        Parameters:
        -----------
        table_file : Filename to be uploaded as table (.csv, .fits, .tab)
        name       : Name of the table to be created
        chunksize  : Number of rows to upload at a time to avoid memory issues
        memsize    : Size of chunk to be read. In Mb.
                     If both specified, the lower number of rows is selected

        Returns:
        --------
        True if success otherwise False
        """
        try:
            self.do_append_table(table_file, name=name, chunksize=chunksize, memsize=memsize)
            return True
        except:
            return False

    def find_tables(self, pattern=''):
        """
        Lists tables and views matching an oracle pattern.

        Parameters:
        -----------
        pattern  : The patter to search tables for, e.g. Y1A1_GOLD

        Returns:
        --------
        A pandas DataFram with the owners and table names. To select from a table use
        owner.table_name, is owner is DES_ADMIN just use table_name
        """
        pattern = pattern.replace('%', '')
        pattern = ''.join(pattern.split())
        pattern = "%" + pattern + "%"
        return self.do_find_tables(pattern, extra='', return_df=True)

    def pandas_to_db(self, df, tablename=None, append=False):
        """ Writes a pandas DataFrame directly to the DB

        Parameters:
        -----------
        df        : The DataFrame to be loaded to the DB
        tablename : The name of the table to be created
        append    : Set True if appending to existing table, if table doesn't exists it is created


        Returns:
        --------
        True or False depending on the success
        """
        if tablename is None:
            print("Please indicate a tablename to be ingested in the DB")
            return False
        if self.check_table_exists(tablename) and not append:
            print(
                colored('\n Table already exists. Table can be removed with:', 'red', self.ct))
            print(colored(' DESDB ~> DROP TABLE %s;\n' %
                          tablename.upper(), 'red', self.ct))
            return False
        df.file_type = 'pandas'
        if len(df) == 0:
            print('DataFrame is empty')
            return False
        dtypes = eafile.get_dtypes(df)
        columns = df.columns.values.tolist()
        values = df.values.tolist()
        if not self.check_table_exists(tablename):
            if append:
                print('Table does not exist. Creating table\n')
            self.create_table(tablename, columns, dtypes)
        self.insert_data(tablename, columns, values, dtypes)
        return True