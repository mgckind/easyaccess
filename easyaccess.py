import warnings
warnings.filterwarnings("ignore")
import cmd
import cx_Oracle
import sys
import os
import re
import readline
import dircache
import subprocess as sp
import os.path as op
import glob as gb
import threading
import time
import csv
from termcolor import colored
import pandas as pd




readline.parse_and_bind('tab: complete')

section = "db-dessci"
host = 'leovip148.ncsa.uiuc.edu'
port = '1521'
name = 'dessci'
kwargs = {'host': host, 'port': port, 'service_name': name}
dsn = cx_Oracle.makedsn(**kwargs)
or_n = cx_Oracle.NUMBER
or_s = cx_Oracle.STRING

options_prefetch = ['show', 'set', 'default']


#pd.set_option('display.height', 1500)
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
    ls = ls[:]  # for overwrite in annotate.
    dircache.annotate(path, ls)
    if filename == '':
        return ls
    else:
        return [f for f in ls if f.startswith(filename)]


def read_buf(fbuf):
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
        self.savePrompt = colored("DESDM >> ", "cyan")
        self.prompt = self.savePrompt
        self.pipe_process_handle = None
        self.buff = None
        self.prefetch = 5000
        self.undoc_header = None
        self.doc_header = 'EasyAccess Commands (type help <command>):'
        self.con = cx_Oracle.connect('mcarras2', 'Alnilam1', dsn=dsn)
        self.user='mcarras2'
        self.cur = self.con.cursor()
        self.cur.arraysize = self.prefetch


    def print_topics(self, header, cmds, cmdlen, maxcol):
        if header is not None:
            if cmds:
                self.stdout.write("%s\n" % str(header))
                if self.ruler:
                    self.stdout.write("%s\n" % str(self.ruler * len(header)))
                self.columnize(cmds, maxcol - 1)
                self.stdout.write("\n")




    def query_and_print(self, query):
        t1 = time.time()
        self.cur.execute(query)
        if self.cur.description != None:
            header = [columns[0] for columns in self.cur.description]
            htypes = [columns[1] for columns in self.cur.description]
            data=pd.DataFrame(self.cur.fetchall())
            data.columns=header
            data.index+=1
            t2=time.time()
            elapsed='%.1f seconds' % (t2-t1)
            print
            print colored('%d rows in %.2f seconds' %(len(data), (t2-t1)), "green")
            print
            print data
        else:
            print colored('Done!',"green")
            self.con.commit()

    def query_results(self,query):
        self.cur.execute(query)
        data=self.cur.fetchall()
        return data




    def do_prefetch(self, line):
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
        "execute shell commands, ex. shell pwd"
        os.system(line)

    def do_edit(self, line):
        "Opens a buffer file to edit and the reads it"
        os.system('nano easy.buf')
        if os.path.exists('easy.buf'):
            newquery = read_buf('easy.buf')
            print
            print newquery
            print
            if (raw_input('submit query? (Y/N): ') in ['Y','y','yes']): self.query_and_print(newquery)
            print


    def do_load(self, line):
        "Loads a sql file with a query "
        newq = read_buf(line)
        print
        print newq
        print
        if (raw_input('submit query? (Y/N): ') in ['Y','y','yes']): self.query_and_print(newq)


    def complete_load(self, text, line, start_idx, end_idx):
        return _complete_path(line)


    def preloop(self):
        """Initialization before prompting user for commands.
        Despite the claims in the Cmd documentaion, Cmd.preloop() is not a stub.
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

    def do_EOF(self, line):
        # exit program on ^D
        sys.exit(0)

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

    def do_exit(self, line):
        "exit the program"
        try:
            os.system('rm -f easy.buf')
        except:
            pass
        try:
            cur.close()
        except:
            pass
        # con.commit()
        # con.close()
        sys.exit(0)

    def do_quit(self, line):
        try:
            os.system('rm -f easy.buf')
        except:
            pass
        try:
            cur.close()
        except:
            pass
        # con.commit()
        # con.close()
        sys.exit(0)

    def do_clear(self, line):
        """
        Clear screen
        """
        # TODO: platform dependent
        tmp = sp.call('clear', shell=True)

    def do_mytables(self, arg):
        """
        Usage: mytables
        lists  table you have made in your 'mydb'  ex: mytables
        """
        query = "SELECT table_name FROM user_tables"
        self.query_and_print(query)


    def do_describe_table(self, arg):
        """
        Usage: describe_table <table_name>
        Describes the columns in <table-name> as
          column_name, oracel_Type, date_length, comments

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
        self.query_and_print(q)
        return



if __name__ == '__main__':
    easy_or().cmdloop()
