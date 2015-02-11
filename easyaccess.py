#TODO:
# Find user
# Find tables
# Find tables with columns
# write fiels (csv, fits, hdf5)
# print myquota
# history
# history of queries

import warnings
warnings.filterwarnings("ignore")
import cmd
import cx_Oracle
import sys
import os
import re
#import readline
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



class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

#readline.parse_and_bind('tab: complete')

#section = "db-dessci"
#host = 'leovip148.ncsa.uiuc.edu'
#port = '1521'
#name = 'dessci'
#kwargs = {'host': host, 'port': port, 'service_name': name}
#dsn = cx_Oracle.makedsn(**kwargs)
or_n = cx_Oracle.NUMBER
or_s = cx_Oracle.STRING



options_prefetch = ['show', 'set', 'default']
options_edit     = ['show', 'set_editor']

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
    newquery=newquery.split(';')[0]
    return newquery


class easy_or(cmd.Cmd, object):
    """cx_oracle interpreter for DESDM"""
    intro = colored("\nThe DESDM Database shell.  Type help or ? to list commands.\n","cyan")

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.table_restriction_clause = " "
        self.savePrompt = 'DESDM >> '
        self.prompt = self.savePrompt
        self.pipe_process_handle = None
        self.buff = None
        self.prefetch = 5000
        self.undoc_header = None
        self.doc_header = 'EasyAccess Commands (type help <command>):'
        self.user='mcarras2'
        self.dbhost='leovip148.ncsa.uiuc.edu'
        self.dbname='dessci'
        self.port='1521'
        self.password='Alnilam1'
        kwargs = {'host': self.dbhost, 'port': self.port, 'service_name': self.dbname}
        dsn = cx_Oracle.makedsn(**kwargs)
        print 'Connecting to DB...'
        self.con = cx_Oracle.connect(self.user, self.password, dsn=dsn)
        self.cur = self.con.cursor()
        self.cur.arraysize = self.prefetch
        self.editor = os.getenv('EDITOR','nano')
        print 'Loading metadata into cache...'
        self.cache_table_names=self.get_tables_names()

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

         self.prompt = self.savePrompt

         if not line: return ""  # empty line no need to go further
         if line[0] == "@":
             if len(line) > 1:
                 fbuf = line[1:].split()[0]
                 line = read_buf(fbuf)+';'
                 self.buff=line
             else:
                 print '@ must be followed by a filename'
                 return ""

         # support model_query Get
         self.prompt = self.savePrompt

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
            self.query_and_print(query)

        else:
            print
            print 'Invalid command or missing ; at the end of query.'
            print 'Type help or ? to list commands'
            print

    def completedefault(self, text, line, begidx, lastidx):
        options_tables = self.cache_table_names
        if text:
            return [option for option in options_tables if option.startswith(text.upper())]
        else:
            return options_tables


### QUERY METHODS
    def query_and_print(self, query, print_time=True, err_arg='No rows selected', suc_arg='Done!'):
        t1 = time.time()
        try:
            self.cur.execute(query)
            if self.cur.description != None:
                header = [columns[0] for columns in self.cur.description]
                htypes = [columns[1] for columns in self.cur.description]
                data=pd.DataFrame(self.cur.fetchall())
                t2=time.time()
                elapsed='%.1f seconds' % (t2-t1)
                print
                if print_time: print colored('%d rows in %.2f seconds' %(len(data), (t2-t1)), "green")
                if print_time: print
                if len(data) == 0:
                    fline='   '
                    for col in header : fline+= '%s  ' % col
                    print fline
                    print colored(err_arg,"red")
                else:
                    data.columns=header
                    data.index+=1
                    print data
            else:
                print colored(suc_arg,"green")
                self.con.commit()
            print
        except:
            (type, value, traceback) = sys.exc_info()
            print
            print colored(type,"red")
            print colored(value,"red")
            print


    def query_results(self,query):
        self.cur.execute(query)
        data=self.cur.fetchall()
        return data

    def get_tables_names(self):
        query='select distinct table_name from fgottenmetadata order by table_name'
        temp=self.cur.execute(query)
        tnames=pd.DataFrame(temp.fetchall())
        table_list=tnames.values.flatten().tolist()
        return table_list

    def get_tables_names_user(self,user):
        query="select distinct table_name from all_tables where owner=\'%s\' order by table_name" % user.upper()
        temp=self.cur.execute(query)
        tnames=pd.DataFrame(temp.fetchall())
        if len(tnames) > 0:
            print '\nTables from %s' %  user.upper()
            print tnames
            table_list=tnames.values.flatten().tolist()
            for table in table_list:
                tn=user.upper()+'.'+table.upper()
                try : self.cache_table_names.index(tn)
                except: self.cache_table_names.append(tn)
            self.cache_table_names.sort()
        else:
            print 'User %s has no tables' % user.upper()



## DO METHODS
    def do_prefetch(self, line):
        """
        Shows, sets or sets to default the number of prefetch rows from Oracle
        The default is 5000, increasing this number uses more memory but return
        data faster. Decreasing this number reduce memory but increases
        communication trips with database thus slowing the process.

        Usage:
           - prefetch show         : Shows current value
           - prefetch set <number> : Sets the prefetch to <number>
           - prefetch default      : Sets value to 5000
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
            self.prefetch = 5000
            print '\nPrefetch value set to default (5000) \n'
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
                if (raw_input('submit query? (Y/N): ') in ['Y','y','yes']): self.query_and_print(newquery)
                print

    def complete_edit(self, text, line, start_index, end_index):
        if text:
            return [option for option in options_edit if option.startswith(text)]
        else:
            return options_edit

    def do_load(self, line):
        """
        Loads a sql file with a query and ask whether it should be run
        """
        newq = read_buf(line)
        print
        print newq
        print
        if (raw_input('submit query? (Y/N): ') in ['Y','y','yes']): self.query_and_print(newq)

    def complete_load(self, text, line, start_idx, end_idx):
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
        pw1=getpass.getpass(prompt='Enter new password:')
        if re.search('\W',pw1) :
            print colored("\nPassword contains whitespace, not set\n", "red")
            return
        if not pw1:
            print colored("\nPassword cannot be blank\n","red")
            return
        pw2=getpass.getpass(prompt='Re-Enter new password:')
        print
        if pw1 != pw2:
            print colored("Passwords don't match, not set\n","red")
            return

        query = """alter user %s identified by "%s"  """ % (self.user, pw1)
        confirm='Password changed in %s' % self.dbname.upper()
        self.query_and_print(query, print_time=False, suc_arg=confirm)

        dbases=['DESSCI','DESOPER']
        for db in dbases:
            if db == self.dbname.upper(): continue
            kwargs = {'host': self.dbhost, 'port': self.port, 'service_name': db}
            dsn = cx_Oracle.makedsn(**kwargs)
            temp_con = cx_Oracle.connect(self.user, self.password, dsn=dsn)
            temp_cur = temp_con.cursor()
            try:
                temp_cur.execute(query)
                confirm='Password changed in %s\n' % db.upper()
                print colored(confirm,"green")
                temp_con.commit()
                temp_cur.close()
                temp_con.close()
            except:
                confirm='Password could not changed in %s\n' % db.upper()
                print colored(confirm,"red")
                print sys.exc_info()





    def do_show_db (self, arg):
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
        sql_getUserDetails = "select * from des_users where username = '"+self.user+"'"
        self.query_and_print(sql_getUserDetails, print_time=False)

    def do_mytables(self, arg):
        """
        Lists  table you have made in your 'mydb'

        Usage: mytables
        """
        query = "SELECT table_name FROM user_tables"
        self.query_and_print(query, print_time=False)

    def do_user_tables(self,arg):
        """
        List tables from given user

        Usage: user_tables <username>
        """
        return self.get_tables_names_user(arg)

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
        schema=self.user.upper() #default --- Mine
        link=""                  #default no link
        if "." in tablename : (schema, tablename) = tablename.split(".")
        if "@" in tablename : (tablename, link)   = tablename.split("@")
        table = tablename

        #
        # loop until we find a fundamental definition OR determine there is
        # no reachable fundamental definition, floow links and resolving
        # schema names. Rely on how the DES database is constructed we log
        # into our own schema, and rely on synonyms for a "simple" view of
        # common schema.
        #
        while (1) :
            #check for fundamental definition  e.g. schema.table@link
            q = """
            select * from all_tab_columns%s
               where OWNER = '%s' and
               TABLE_NAME = '%s'
               """ % ("@" + link if link else "", schema, table)
            if len(self.query_results(q)) != 0 :
                #found real definition go get meta-data
                break

            # check if we are indirect by  synonym of mine
            q = """select TABLE_OWNER, TABLE_NAME, DB_LINK from USER_SYNONYMS%s
                            where SYNONYM_NAME= '%s'
            """  % ("@" + link if link else "", table)
            ans = self.query_results(q)
            if len(ans) == 1:
                #resolved one step closer to fundamental definition
                (schema, table, link) = ans[0]
                continue

            #check if we are indirect by a public synonym
            q = """select TABLE_OWNER, TABLE_NAME, DB_LINK from ALL_SYNONYMS%s
                             where SYNONYM_NAME = '%s' AND OWNER = 'PUBLIC'
            """  % ("@" + link if link else "", table)
            ans = self.query_results(q)
            if len(ans) == 1:
                #resolved one step closer to fundamental definition
                (schema, table, link) = ans[0]
                continue

            #failed to find the reference count on the query below to give a null result
            break   # no such table accessible by user

        # schema, table and link are now valid.
        link = "@" + link if link else ""
        q = """
        select
          atc.owner, atc.column_name, atc.data_type,
          atc.data_length, acc.comments
          From all_tab_cols%s atc , all_col_comments%s acc
           where atc.owner = '%s' and atc.table_name = '%s' and
           acc.owner = '%s' and acc.table_name='%s' and acc.column_name = atc.column_name
           order by atc.column_id
           """ % (link, link, schema, table, schema, table)
        self.query_and_print(q, print_time=False, err_arg='Table does not exist or it is not accessible by user')
        return

    def complete_describe_table(self, text, line, start_index, end_index):
        options_tables = self.cache_table_names
        if text:
            return [option for option in options_tables if option.startswith(text.upper())]
        else:
            return options_tables


#UNDOCCUMENTED DO METHODS

    def do_EOF(self, line):
        # exit program on ^D
        self.do_exit(line)

    def do_quit(self, line):
        self.do_exit(line)


if __name__ == '__main__':
    easy_or().cmdloop()
