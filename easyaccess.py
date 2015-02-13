# TODO:
# history
# history of queries
# upload table
# completer scope? to complete from particular table?
# update do_help
# refreash metadata after 24 hours or so
# Check for meatdata table and create cache and table if it doesn't exist
# parse connection file
# parse arguments
# Add timeout for just printing on screen with suggestion to run into a file
# call from outside

import warnings

warnings.filterwarnings("ignore")
import cmd
import cx_Oracle
import sys
import os
import re
# import readline
import dircache
import subprocess as sp
import os.path as op
import glob as gb
import threading
import time
import getpass
import csv
from termcolor import colored
import pandas as pd
import datetime
import pyfits as pf




# readline.parse_and_bind('tab: complete')

# section = "db-dessci"
#host = 'leovip148.ncsa.uiuc.edu'
#port = '1521'
#name = 'dessci'
#kwargs = {'host': host, 'port': port, 'service_name': name}
#dsn = cx_Oracle.makedsn(**kwargs)
or_n = cx_Oracle.NUMBER
or_s = cx_Oracle.STRING
or_f = cx_Oracle.NATIVE_FLOAT
or_o = cx_Oracle.OBJECT

options_prefetch = ['show', 'set', 'default']
options_edit = ['show', 'set_editor']

options_out = ['csv', 'tab', 'fits', 'h5']

options_def = ['Coma separated value', 'space separated value', 'Fits format', 'HDF5 format']

type_dict = {'float64': 'D', 'int64': 'K', 'float32': 'E', 'int32': 'J', 'object': '200A', 'int8': 'I'}
#PANDAS SET UP
pd.set_option('display.max_rows', 1500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 50)


def _complete_path(line):
    line = line.split()
    if len(line) < 2:
        filename = ''
        path = './'
    else:
        path = line[1]
        if '/' in path:
            i = path.rfind('/')
            filename = path[i + 1:]
            path = path[:i]
        else:
            filename = path
            path = './'
    ls = dircache.listdir(path)
    ls = ls[:]
    dircache.annotate(path, ls)
    if filename == '':
        return ls
    else:
        return [f for f in ls if f.startswith(filename)]


def read_buf(fbuf):
    """
    Read SQL files. It removes the ; at the end of the file if present
    """
    try:
        with open(fbuf) as f:
            content = f.read()
    except:
        print '\n' + 'Fail to load the file "{:}"'.format(fbuf)
        return ""
    list = [item for item in content.split('\n')]
    newquery = ''
    for line in list:
        if line[0:2] == '--': continue
        newquery += ' ' + line.split('--')[0]
    newquery = newquery.split(';')[0]
    return newquery


def change_type(info):
    if info[1] == or_n:
        if info[5] == 0 and info[4] >= 10:
            return "int64"
        elif info[5] == 0 and info[4] >= 3:
            return "int32"
        elif info[5] == 0 and info[4] >= 1:
            return "int8"
        elif info[5] > 0 and info[5] <= 5:
            return "float32"
        else:
            return "float64"
    elif info[1] == or_f:
        if info[3] == 4:
            return "float32"
        else:
            return "float64"
    else:
        return ""


def write_to_fits(df, fitsfile, mode='w', listN=[], listT=[]):
    if mode == 'w':
        C = pf.ColDefs([])
        for col in df:
            type_df = df[col].dtype.name
            if col in listN:
                print col
                fmt = listT[listN.index(col)]
            else:
                fmt = type_dict[type_df]
            CC = pf.Column(name=col, format=fmt, array=df[col].values)
            C.add_col(CC)
        SS = pf.BinTableHDU.from_columns(C)
        SS.writeto(fitsfile, clobber=True)
    if mode == 'a':
        Htemp = pf.open(fitsfile)
        nrows1 = Htemp[1].data.shape[0]
        ntot = nrows1 + len(df)
        SS = pf.BinTableHDU.from_columns(Htemp[1].columns, nrows=ntot)
        for colname in Htemp[1].columns.names:
            SS.data[colname][nrows1:] = df[colname].values
        SS.writeto(fitsfile, clobber=True)


class easy_or(cmd.Cmd, object):
    """cx_oracle interpreter for DESDM"""
    intro = colored("\nThe DESDM Database shell.  Type help or ? to list commands.\n", "cyan")

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.table_restriction_clause = " "
        self.savePrompt = colored('_________', 'cyan') + '\nDESDB ~> '
        self.prompt = self.savePrompt
        self.pipe_process_handle = None
        self.buff = None
        self.prefetch = 10000
        self.undoc_header = None
        self.doc_header = 'EasyAccess Commands (type help <command>):'
        self.user = 'mcarras2'
        self.dbhost = 'leovip148.ncsa.uiuc.edu'
        self.dbname = 'dessci'
        self.port = '1521'
        self.password = 'Alnilam1'
        kwargs = {'host': self.dbhost, 'port': self.port, 'service_name': self.dbname}
        dsn = cx_Oracle.makedsn(**kwargs)
        print 'Connecting to DB...'
        self.con = cx_Oracle.connect(self.user, self.password, dsn=dsn)
        self.cur = self.con.cursor()
        self.cur.arraysize = self.prefetch
        self.editor = os.getenv('EDITOR', 'nano')
        print 'Loading metadata into cache...'
        self.cache_table_names = self.get_tables_names()
        self.cache_usernames = self.get_userlist()
        self.cache_column_names = self.get_columnlist()

    ### OVERRIDE CMD METHODS
    def print_topics(self, header, cmds, cmdlen, maxcol):
        if header is not None:
            if cmds:
                self.stdout.write("%s\n" % str(header))
                if self.ruler:
                    self.stdout.write("%s\n" % str(self.ruler * len(header)))
                self.columnize(cmds, maxcol - 1)
                self.stdout.write("\n")


    def preloop(self):
        """
        Initialization before prompting user for commands.
        Despite the claims in the Cmd documentation, Cmd.preloop() is not a stub.
        """
        cmd.Cmd.preloop(self)  # # sets up command completion
        self._hist = []  # # No history yet
        self._locals = {}  # # Initialize execution namespace for user
        self._globals = {}

    def precmd(self, line):
        """ This method is called after the line has been input but before
             it has been interpreted. If you want to modifdy the input line
             before execution (for example, variable substitution) do it here.
         """

        # handle line continuations -- line terminated with \
        # beware of null lines.
        line = ' '.join(line.split())
        self.buff = line
        while line and line[-1] == "\\":
            self.buff = self.buff[:-1]
            line = line[:-1]  # strip terminal \
            temp = raw_input('...')
            self.buff += '\n' + temp
            line += temp

        #self.prompt = self.savePrompt

        if not line: return ""  # empty line no need to go further
        if line[0] == "@":
            if len(line) > 1:
                fbuf = line[1:].split()[0]
                line = read_buf(fbuf) + ';'
                self.buff = line
            else:
                print '@ must be followed by a filename'
                return ""

        # support model_query Get
        #self.prompt = self.savePrompt

        self._hist += [line.strip()]
        return line

    def emptyline(self):
        pass

    def default(self, line):
        fend = line.find(';')
        if fend > -1:
            with open('easy.buf', 'w') as filebuf:
                filebuf.write(self.buff)
            query = line[:fend]
            if line[fend:].find('>') > -1:
                try:
                    fileout = line[fend:].split('>')[1].strip().split()[0]
                    fileformat = fileout.split('.')[-1]
                    if fileformat in options_out:
                        print '\nFetching data and saving it to %s ...' % fileout + '\n'
                        self.query_and_save(query, fileout, mode=fileformat)
                    else:
                        print colored('\nFile format not valid.\n', 'red')
                        print 'Supported formats:\n'
                        for jj, ff in enumerate(options_out): print '%5s  %s' % (ff, options_def[jj])
                except:
                    print colored('\nMust indicate output file\n', "red")
                    print 'Format:\n'
                    print 'select ... from ... where ... ; > example.csv \n'
            else:
                self.query_and_print(query)

        else:
            print
            print 'Invalid command or missing ; at the end of query.'
            print 'Type help or ? to list commands'
            print

    def completedefault(self, text, line, begidx, lastidx):
        if line.upper().find('SELECT') > -1:
            #return self._complete_colnames(text)
            if line.upper().find('FROM') == -1:
                return self._complete_colnames(text)
            elif line.upper().find('FROM') > -1 and line.upper().find('WHERE') == -1:
                return self._complete_tables(text)
            else:
                return self._complete_colnames(text)
        else:
            return self._complete_tables(text)



            ### QUERY METHODS

    def query_and_print(self, query, print_time=True, err_arg='No rows selected', suc_arg='Done!'):
        self.cur.arraysize = self.prefetch
        t1 = time.time()
        try:
            self.cur.execute(query)
            if self.cur.description != None:
                header = [columns[0] for columns in self.cur.description]
                htypes = [columns[1] for columns in self.cur.description]
                info = [rec[1:6] for rec in self.cur.description]
                data = pd.DataFrame(self.cur.fetchall())
                t2 = time.time()
                elapsed = '%.1f seconds' % (t2 - t1)
                print
                if print_time: print colored('%d rows in %.2f seconds' % (len(data), (t2 - t1)), "green")
                if print_time: print
                if len(data) == 0:
                    fline = '   '
                    for col in header: fline += '%s  ' % col
                    print fline
                    print colored(err_arg, "red")
                else:
                    data.columns = header
                    data.index += 1
                    print data
            else:
                print colored(suc_arg, "green")
                self.con.commit()
            print
        except:
            (type, value, traceback) = sys.exc_info()
            print
            print colored(type, "red")
            print colored(value, "red")
            print


    def query_and_save(self, query, fileout, mode='csv', print_time=True):
        self.cur.arraysize = self.prefetch
        t1 = time.time()
        try:
            self.cur.execute(query)
            if self.cur.description != None:
                header = [columns[0] for columns in self.cur.description]
                htypes = [columns[1] for columns in self.cur.description]
                info = [rec[0:6] for rec in self.cur.description]
                first = True
                mode_write = 'w'
                header_out = True
                com_it = 0
                while True:
                    data = pd.DataFrame(self.cur.fetchmany())
                    com_it += 1
                    if first:
                        list_names = []
                        list_type = []
                        for inf in info:
                            if inf[1] == or_s:
                                list_names.append(inf[0])
                                list_type.append(str(inf[3]) + 'A')
                    if not data.empty:
                        data.columns = header
                        for jj, col in enumerate(data):
                            nt = change_type(info[jj])
                            if nt != "": data[col] = data[col].astype(nt)
                        if mode == 'csv': data.to_csv(fileout, index=False, float_format='%.6f', sep=',',
                                                      mode=mode_write, header=header_out)
                        if mode == 'tab': data.to_csv(fileout, index=False, float_format='%.6f', sep=' ',
                                                      mode=mode_write, header=header_out)
                        if mode == 'h5':  data.to_hdf(fileout, 'data', mode=mode_write, index=False,
                                                      header=header_out)  #, complevel=9,complib='bzip2'
                        if mode == 'fits': write_to_fits(data, fileout, mode=mode_write, listN=list_names,
                                                         listT=list_type)
                        if first:
                            mode_write = 'a'
                            header_out = False
                            first = False
                    else:
                        break
                t2 = time.time()
                elapsed = '%.1f seconds' % (t2 - t1)
                print
                if print_time: print colored('\n Written %d rows to %s in %.2f seconds and %d trips' % (
                    self.cur.rowcount, fileout, (t2 - t1), com_it - 1), "green")
                if print_time: print
            else:
                pass
            print
        except:
            (type, value, traceback) = sys.exc_info()
            print
            print colored(type, "red")
            print colored(value, "red")
            print


    def query_results(self, query):
        self.cur.execute(query)
        data = self.cur.fetchall()
        return data

    def get_tables_names(self):
        query = """
        select distinct table_name from fgottenmetadata
        union select distinct t1.owner || '.' || t1.table_name from all_tab_cols t1,
        des_users t2 where upper(t1.owner)=upper(t2.username) and t1.owner not in ('DES_ADMIN')"""
        #where owner not in ('XDB','SYSTEM','SYS', 'DES_ADMIN', 'EXFSYS','')
        temp = self.cur.execute(query)
        tnames = pd.DataFrame(temp.fetchall())
        table_list = tnames.values.flatten().tolist()
        return table_list

    def get_tables_names_user(self, user):
        query = "select distinct table_name from all_tables where owner=\'%s\' order by table_name" % user.upper()
        temp = self.cur.execute(query)
        tnames = pd.DataFrame(temp.fetchall())
        if len(tnames) > 0:
            print '\nTables from %s' % user.upper()
            print tnames
            #Add tname to cache (no longer needed)
            #table_list=tnames.values.flatten().tolist()
            #for table in table_list:
            #    tn=user.upper()+'.'+table.upper()
            #    try : self.cache_table_names.index(tn)
            #    except: self.cache_table_names.append(tn)
            #self.cache_table_names.sort()
        else:
            print 'User %s has no tables' % user.upper()

    def get_userlist(self):
        query = 'select distinct username from des_users order by username'
        temp = self.cur.execute(query)
        tnames = pd.DataFrame(temp.fetchall())
        user_list = tnames.values.flatten().tolist()
        return user_list

    def _complete_tables(self, text):
        options_tables = self.cache_table_names
        if text:
            return [option for option in options_tables if option.startswith(text.upper())]
        else:
            return options_tables

    def _complete_colnames(self, text):
        options_colnames = self.cache_column_names
        if text:
            return [option for option in options_colnames if option.startswith(text.upper())]
        else:
            return options_colnames

    def get_columnlist(self):
        query = """SELECT distinct column_name from fgottenmetadata  order by column_name"""
        temp = self.cur.execute(query)
        cnames = pd.DataFrame(temp.fetchall())
        col_list = cnames.values.flatten().tolist()
        return col_list


    ## DO METHODS
    def do_prefetch(self, line):
        """
        Shows, sets or sets to default the number of prefetch rows from Oracle
        The default is 10000, increasing this number uses more memory but return
        data faster. Decreasing this number reduce memory but increases
        communication trips with database thus slowing the process.

        Usage:
           - prefetch show         : Shows current value
           - prefetch set <number> : Sets the prefetch to <number>
           - prefetch default      : Sets value to 10000
        """
        line = "".join(line.split())
        if line.find('show') > -1:
            print '\nPrefetch value = {:}\n'.format(self.prefetch)
        elif line.find('set') > -1:
            val = line.split('set')[-1]
            if val != '':
                self.prefetch = int(val)
                print '\nPrefetch value set to  {:}\n'.format(self.prefetch)
        elif line.find('default') > -1:
            self.prefetch = 10000
            print '\nPrefetch value set to default (10000) \n'
        else:
            print '\nPrefetch value = {:}\n'.format(self.prefetch)

    def complete_prefetch(self, text, line, start_index, end_index):
        if text:
            return [option for option in options_prefetch if option.startswith(text)]
        else:
            return options_prefetch


    def do_hist(self, line):
        """Print a list of commands that have been entered"""
        print self._hist

    def do_shell(self, line):
        """
        Execute shell commands, ex. shell pwd
        You can also use !<command> like !ls, or !pwd to access the shell
        """
        os.system(line)

    def do_edit(self, line):
        """
        Opens a buffer file to edit a sql statement and then it reads it
        and executes the statement. By default it will show the current
        statement in buffer (or empty)

        Usage:
            - edit   : opens the editor (default from $EDITOR or nano)
            - edit set_editor <editor> : sets editor to <editor>, ex: edit set_editor vi
        """

        line = "".join(line.split())
        if line.find('show') > -1:
            print '\nEditor  = {:}\n'.format(self.editor)
        elif line.find('set_editor') > -1:
            val = line.split('set_editor')[-1]
            if val != '':
                self.editor = val
        else:
            os.system(self.editor + ' easy.buf')
            if os.path.exists('easy.buf'):
                newquery = read_buf('easy.buf')
                print
                print newquery
                print
                if (raw_input('submit query? (Y/N): ') in ['Y', 'y', 'yes']): self.query_and_print(newquery)
                print

    def complete_edit(self, text, line, start_index, end_index):
        if text:
            return [option for option in options_edit if option.startswith(text)]
        else:
            return options_edit

    def do_loadsql(self, line):
        """
        Loads a sql file with a query and ask whether it should be run

        Usage: loadsql <filename>   (use autocompletion)
        """
        newq = read_buf(line)
        print
        print newq
        print
        if (raw_input('submit query? (Y/N): ') in ['Y', 'y', 'yes']): self.query_and_print(newq)

    def complete_loadsql(self, text, line, start_idx, end_idx):
        return _complete_path(line)

    def do_exit(self, line):
        """
        Exits the program
        """
        try:
            os.system('rm -f easy.buf')
        except:
            pass
        try:
            cur.close()
        except:
            pass
        self.con.commit()
        self.con.close()
        sys.exit(0)

    def do_clear(self, line):
        """
        Clear screen
        """
        # TODO: platform dependent
        tmp = sp.call('clear', shell=True)


    #DO METHODS FOR DB

    def do_set_password(self, arg):
        """
        Set a new password on this and all other DES instances (DESSCI, DESOPER)

        Usage: set_password
        """
        print
        pw1 = getpass.getpass(prompt='Enter new password:')
        if re.search('\W', pw1):
            print colored("\nPassword contains whitespace, not set\n", "red")
            return
        if not pw1:
            print colored("\nPassword cannot be blank\n", "red")
            return
        pw2 = getpass.getpass(prompt='Re-Enter new password:')
        print
        if pw1 != pw2:
            print colored("Passwords don't match, not set\n", "red")
            return

        query = """alter user %s identified by "%s"  """ % (self.user, pw1)
        confirm = 'Password changed in %s' % self.dbname.upper()
        self.query_and_print(query, print_time=False, suc_arg=confirm)

        dbases = ['DESSCI', 'DESOPER']
        for db in dbases:
            if db == self.dbname.upper(): continue
            kwargs = {'host': self.dbhost, 'port': self.port, 'service_name': db}
            dsn = cx_Oracle.makedsn(**kwargs)
            temp_con = cx_Oracle.connect(self.user, self.password, dsn=dsn)
            temp_cur = temp_con.cursor()
            try:
                temp_cur.execute(query)
                confirm = 'Password changed in %s\n' % db.upper()
                print colored(confirm, "green")
                temp_con.commit()
                temp_cur.close()
                temp_con.close()
            except:
                confirm = 'Password could not changed in %s\n' % db.upper()
                print colored(confirm, "red")
                print sys.exc_info()


    def do_refresh_metadata_cache(self, arg):
        """ Refreshes meta data cache for auto-completion of table names and column names """

        # Meta data access: With the two linked databases, accessing the
        # "truth" via fgetmetadata has become maddenly slow.
        # what it returns is a function of each users's permissions, and their
        # "mydb". so yet another level of caching is needed. Ta loads a table
        # called fgottenmetadata in the user's mydb. It refreshes on command
        # or on timeout (checked at startup).

        # drop table if it exists, then make a new one.
        # created is the time that the cache table was created,


        #try:
        #    created = query_results("""
        #    select created  from DBA_OBJECTS
        #    where object_name = 'FGOTTENMETADATA' and owner = '%s'
        #    """ % self.user.upper())
        #except:
        #    # no meta data system present for this user.
        #    make_table = False
        #    print "Warning Metadata not available, continuing : "

        #get last update
        query_time = "select created from dba_objects where object_name = \'FGOTTENMETADATA\' and owner =\'%s\'  " % (
            self.user.upper())
        try:
            qt = self.cur.execute(query_time)
            last = qt.fetchall()
            now = datetime.datetime.now()
            diff = abs(now - last[0][0]).seconds / 3600.
            print 'Updated %.2f hours ago' % diff
        except:
            pass
        try:
            query = "DROP TABLE FGOTTENMETADATA"
            print
            self.query_and_print(query, print_time=False, suc_arg='FGOTTENMETADATA table Dropped!')
        except:
            pass
        try:
            print '\nRe-creating metadata table ...'
            query_2 = """create table fgottenmetadata  as  select * from table (fgetmetadata)"""
            self.query_and_print(query_2, print_time=False, suc_arg='FGOTTENMETADATA table Created!')
            print 'Loading metadata into cache...'
            self.cache_table_names = self.get_tables_names()
            self.cache_usernames = self.get_userlist()
            self.cache_column_names = self.get_columnlist()
        except:
            print colored("There was an error when refreshing the cache", "red")


    def do_show_db(self, arg):
        """
        Shows database connection information
        """
        print
        print "user: %s, host:%s, db:%s" % (self.user, self.dbhost, self.dbname)
        print "Personal links:"
        query = """
           select owner, db_link, username, host, created from all_db_links where OWNER = '%s'
        """ % (self.user.upper())
        self.query_and_print(query, print_time=False)

    def do_whoami(self, arg):
        """
        Print information about the user's details.

        Usage: whoami
        """
        sql_getUserDetails = "select * from des_users where username = '" + self.user + "'"
        self.query_and_print(sql_getUserDetails, print_time=False)

    def do_myquota(self, arg):
        """
        Print information about quota status.

        Usage: myquota
        """
        sql_getquota = "select TABLESPACE_NAME,  \
        MBYTES_USED/1024 as GBYTES_USED, MBYTES_LEFT/1024 as GBYTES_LEFT from myquota"
        self.query_and_print(sql_getquota, print_time=False)

    def do_mytables(self, arg):
        """
        Lists  table you have made in your 'mydb'

        Usage: mytables
        """
        query = "SELECT table_name FROM user_tables"
        self.query_and_print(query, print_time=False)

    def do_find_user(self, line):
        """
        Finds users given 1 criteria (either first name or last name)

        Usage: 
            - find_user Doe     # Finds all users with Doe as their names
            - find_user John%   # Finds all users with John IN their names (John, Johnson, etc...)
            - find_user P%      # Finds all users with first or lastname starting with P

        """
        if line == "": return
        line = " ".join(line.split())
        keys = line.split()
        query = 'select * from des_users where '
        if len(keys) >= 1:
            query += 'upper(firstname) like upper(\'' + keys[0] + '\') or upper(lastname) like upper(\'' + keys[
                0] + '\')'
        self.query_and_print(query, print_time=True)

    def complete_find_user(self, text, line, start_index, end_index):
        options_users = self.cache_usernames
        if text:
            return [option for option in options_users if option.startswith(text.lower())]
        else:
            return options_users


    def do_user_tables(self, arg):
        """
        List tables from given user

        Usage: user_tables <username>
        """
        return self.get_tables_names_user(arg)

    def complete_user_tables(self, text, line, start_index, end_index):
        options_users = self.cache_usernames
        if text:
            return [option for option in options_users if option.startswith(text.lower())]
        else:
            return options_users

    def do_describe_table(self, arg):
        """
        Usage: describe_table <table_name>
        Describes the columns in <table-name> as
          column_name, oracle_Type, date_length, comments

        This tool is useful in noting the lack of documentation for the
        columns. If you don't know the full table name you can use tab
        completion on the table name. Tables of ususal interest to
        scientists are described
        """
        tablename = arg.upper()
        schema = self.user.upper()  #default --- Mine
        link = ""  #default no link
        if "." in tablename: (schema, tablename) = tablename.split(".")
        if "@" in tablename: (tablename, link) = tablename.split("@")
        table = tablename

        #
        # loop until we find a fundamental definition OR determine there is
        # no reachable fundamental definition, floow links and resolving
        # schema names. Rely on how the DES database is constructed we log
        # into our own schema, and rely on synonyms for a "simple" view of
        # common schema.
        #
        while (1):
            #check for fundamental definition  e.g. schema.table@link
            q = """
            select * from all_tab_columns%s
               where OWNER = '%s' and
               TABLE_NAME = '%s'
               """ % ("@" + link if link else "", schema, table)
            if len(self.query_results(q)) != 0:
                #found real definition go get meta-data
                break

            # check if we are indirect by  synonym of mine
            q = """select TABLE_OWNER, TABLE_NAME, DB_LINK from USER_SYNONYMS%s
                            where SYNONYM_NAME= '%s'
            """ % ("@" + link if link else "", table)
            ans = self.query_results(q)
            if len(ans) == 1:
                #resolved one step closer to fundamental definition
                (schema, table, link) = ans[0]
                continue

            #check if we are indirect by a public synonym
            q = """select TABLE_OWNER, TABLE_NAME, DB_LINK from ALL_SYNONYMS%s
                             where SYNONYM_NAME = '%s' AND OWNER = 'PUBLIC'
            """ % ("@" + link if link else "", table)
            ans = self.query_results(q)
            if len(ans) == 1:
                #resolved one step closer to fundamental definition
                (schema, table, link) = ans[0]
                continue

            #failed to find the reference count on the query below to give a null result
            break  # no such table accessible by user

        # schema, table and link are now valid.
        link = "@" + link if link else ""
        q = """
        select
          atc.column_name, atc.data_type,
          atc.data_length || ',' || atc.data_precision || ',' || atc.data_scale DATA_FORMAT, acc.comments
          From all_tab_cols%s atc , all_col_comments%s acc
           where atc.owner = '%s' and atc.table_name = '%s' and
           acc.owner = '%s' and acc.table_name='%s' and acc.column_name = atc.column_name
           order by atc.column_id
           """ % (link, link, schema, table, schema, table)
        self.query_and_print(q, print_time=False, err_arg='Table does not exist or it is not accessible by user')
        return

    def complete_describe_table(self, text, line, start_index, end_index):
        return self._complete_tables(text)

    def do_find_tables(self, arg):
        """
        Lists tables and views matching an oracle pattern  e.g %SVA%,
        
        Usage : find_tables PATTERN
        """
        query = "SELECT distinct table_name from fgottenmetadata  WHERE upper(table_name) LIKE '%s' " % (arg.upper())
        self.query_and_print(query)

    def complete_find_tables(self, text, line, start_index, end_index):
        return self._complete_tables(text)


    def do_find_tables_with_column(self, arg):
        """                                                                                
        Finds tables having a column name matching column-name-string                                            
        
        Usage: find_tables_with_column  <column-name-substring>                                                                 
        Example: find_tables_with_column %MAG%  # hunt for columns with MAG 
        """
        #query  = "SELECT TABLE_NAME, COLUMN_NAME FROM fgottenmetadata WHERE COLUMN_NAME LIKE '%%%s%%' " % (arg.upper())
        query = """
           SELECT 
               table_name, column_name 
           FROM 
                fgottenmetadata 
           WHERE 
             column_name LIKE '%s'  
           UNION
           SELECT LOWER(owner) || '.' || table_name, column_name 
            FROM 
                all_tab_cols
            WHERE 
                 column_name LIKE '%s'
             AND
                 owner NOT LIKE '%%SYS'
             AND 
                 owner not in ('XDB','SYSTEM')
           """ % (arg.upper(), arg.upper())

        self.query_and_print(query)
        return

    def complete_find_tables_with_column(self, text, line, begidx, lastidx):
        return self._complete_colnames(text)

    def do_show_index(self, arg):
        """
        Describes the indices  in <table-name> as
          column_name, oracel_Type, date_length, comments

         Usage: describe_index <table_name>
        """

        # Parse tablename for simple name or owner.tablename.
        # If owner present, then add owner where clause.
        arg = arg.upper().strip()
        if not arg:
            print "table name required"
            return
        tablename = arg
        query_template = """select
             a.table_name, a.column_name, b.index_type, b.index_name, b.ityp_name from
             all_ind_columns a, all_indexes b
             where
             a.table_name LIKE '%s' and a.table_name like b.table_name
             """
        query = query_template % (tablename)
        nresults = self.query_and_print(query)
        return

    def complete_show_index(self, text, line, begidx, lastidx):
        return self._complete_tables(text)


    def load_table(self, line):
        """
        Loads a table from a file (csv or fits) taking name from filename and columns from header

        Usage: load_table <filename>
        Ex: example.csv has the following content
             RA,DEC,MAG
             1.23,0.13,23
             0.13,0.01,22

        This command will create a table named EXAMPLE with 3 columns RA,DEC and MAG and values taken from file

        Note: - For csv or tab files, first line must have the column names (without # or any other comment) and same format
        as data (using ',' or space)
              - For fits file header must have columns names and data types
              - For filenames use <table_name>.csv or <table_name>.fits do not use extra points
        """
        if line == "":
            print 'Must include table file!\n'
            return
        else:
            line = "".join(line.split())
            if line.find('/') > -1:
                filename = line.split('/')[-1]
            alls = filename.split('.')
            if len(alls) > 2:
                print 'Do not use extra . in filename'
                return
            else:
                table = alls[0]
                format = alla[1]
                if format == 'csv':
                    try:
                        DF == pd.read_csv(line, sep=',')
                    except:
                        print colored('\nProblems reading %s\n' % line, "red")
                        return

                    qtable = 'create table %s ( ' % table
                    for col in DF:
                        if DF[col].dtype.name == 'object':
                            qtable += col + ' ' + 'VARCHAR(' + str(max(DF['TILENAME'].str.len())) + '),'
                        elif DF[col].dtype.name.find('int'):
                            qtable += col + ' INT,'
                        elif DF[col].dtype.name.find('float'):
                            qtable += col + ' BINARY_DOUBLE,'
                        else:
                            qtable += col + ' NUMBER,'
                    qtable = qtable[:-1] + ')'
                    try:
                        self.cur.execute(qtable)
                    except:
                        (type, value, traceback) = sys.exc_info()
                        print
                        print colored(type, "red")
                        print colored(value, "red")
                        print
                        del DF
                        return

                    cols=','.join(DF.columns.values.tolist())
                    vals=',:'.join(DF.columns.values.tolist())
                    vals=':'+vals
                    qinsert='insert into %s (%s) values (%s)'%  (table.upper(), cols, vals)
                    try:
                        t1=time.time()
                        self.cur.executemany(qinsert, DF.values.tolist())
                        t2=time.time()
                        print colored('\n  Table %s created successfully with %d rows and %d columns in %.2f seconds' % (table.upper(), len(DF), len(DF.columns), t2-t1), "green")
                        del DF
                    except:
                        (type, value, traceback) = sys.exc_info()
                        print
                        print colored(type, "red")
                        print colored(value, "red")
                        print
                        return
                    return


    def complete_load_table(self, text, line, start_idx, end_idx):
        return _complete_path(line)


    #UNDOCCUMENTED DO METHODS

    def do_EOF(self, line):
        # exit program on ^D
        self.do_exit(line)

    def do_quit(self, line):
        self.do_exit(line)


if __name__ == '__main__':
    easy_or().cmdloop()
