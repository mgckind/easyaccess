#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

__author__ = 'Matias Carrasco Kind'
__version__ = '1.2.1a'

# For compatibility with old python
try:
    from builtins import input, str, range
except ImportError:
    from __builtin__ import input, str, range

import warnings

warnings.filterwarnings("ignore")
import cmd
import cx_Oracle
import sys
import os
import shutil
import stat
import re
import eautils.dircache as dircache
import threading
import time
import getpass
import itertools
import logging

try:
    from termcolor import colored
except:
    def colored(line, color): return line
        
import pandas as pd
import datetime
import fitsio
import numpy as np
import argparse
import config_ea as config_mod
from eautils import des_logo as dl
import webbrowser
import signal

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')

global load_bar
pid = os.getpid()

# FILES
ea_path = os.path.join(os.environ["HOME"], ".easyaccess/")
if not os.path.exists(ea_path): os.makedirs(ea_path)
history_file = os.path.join(os.environ["HOME"], ".easyaccess/history")
config_file = os.path.join(os.environ["HOME"], ".easyaccess/config.ini")


# check if old path is there
ea_path_old = os.path.join(os.environ["HOME"], ".easyacess/")
if os.path.exists(ea_path_old) and os.path.isdir(ea_path_old):
    if not os.path.exists(history_file):
        shutil.copy2(os.path.join(os.environ["HOME"], ".easyacess/history"), history_file)
    if not os.path.exists(config_file):
        shutil.copy2(os.path.join(os.environ["HOME"], ".easyacess/config.ini"), config_file)
    #shutil.move(ea_path_old, os.path.join(os.environ["HOME"], ".easyacess_old/"))

# create files if they don't exist
if not os.path.exists(history_file):
    os.system('echo $null >> ' + history_file)
if not os.path.exists(config_file):
    os.system('echo $null >> ' + config_file)

desfile = os.getenv("DES_SERVICES")
if not desfile: desfile = os.path.join(os.getenv("HOME"), ".desservices.ini")
if os.path.exists(desfile):
    amode = stat.S_IMODE(os.stat(desfile).st_mode)
    if amode != 2 ** 8 + 2 ** 7:
        print('Changing permissions to des_service file to read/write only by user')
        os.chmod(desfile, 2 ** 8 + 2 ** 7)


def print_exception():
    (type, value, traceback) = sys.exc_info()
    print()
    print(colored(type, "red"))
    print(colored(value, "red"))
    print()


def loading():
    char_s = u"\u2606"
    if sys.stdout.encoding != 'UTF-8':
        char_s = "o"
    print()
    cc = 0
    spinner = itertools.cycle(list(range(13)) + list(range(1, 14, 1))[::-1])
    line2 = "  Press Ctrl-C to abort "
    try:
        while True:
            line = list('    |              |')
            time.sleep(0.1)
            idx = int(next(spinner))
            line[5 + idx] = char_s
            sys.stdout.write("".join(line))
            sys.stdout.write(line2)
            sys.stdout.flush()
            sys.stdout.write('\b' * len(line) + '\b' * len(line2))
    except:
        pass


or_n = cx_Oracle.NUMBER
or_s = cx_Oracle.STRING
or_f = cx_Oracle.NATIVE_FLOAT
or_o = cx_Oracle.OBJECT
or_ov = cx_Oracle.OBJECT
or_dt = cx_Oracle.DATETIME

options_prefetch = ['show', 'set', 'default']
options_add_comment = ['table', 'column']
options_edit = ['show', 'set_editor']
options_out = ['csv', 'tab', 'fits', 'h5']
options_def = ['Coma separated value', 'space separated value', 'Fits format', 'HDF5 format']
options_config = ['all', 'database', 'editor', 'prefetch', 'histcache', 'timeout', 'outfile_max_mb', 'max_rows',
                  'max_columns',
                  'width', 'max_colwidth', 'color_terminal', 'loading_bar', 'filepath', 'nullvalue', 'autocommit']
options_config2 = ['show', 'set']
options_app = ['check', 'submit', 'explain']

type_dict = {'float64': 'D', 'int64': 'K', 'float32': 'E', 'int32': 'J', 'object': '200A', 'int8': 'I'}


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
    Read SQL files, sql statement should end with ; if parsing to a file to write
    """
    try:
        with open(fbuf) as f:
            content = f.read()
    except:
        print('\n' + 'Fail to load the file "{:}"'.format(fbuf))
        return ""
    list = [item for item in content.split('\n')]
    newquery = ''
    for line in list:
        if line[0:2] == '--': continue
        newquery += ' ' + line.split('--')[0]
    # newquery = newquery.split(';')[0]
    return newquery


def change_type(info):
    if info[1] == or_n:
        if info[5] == 0 and info[4] >= 10:
            return "int64"
        elif info[5] == 0 and info[4] >= 3:
            return "int32"
        elif info[5] == 0 and info[4] >= 1:
            return "int8"
        elif 0 < info[5] <= 5:
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


def dtype2oracle(dtype):
    kind = dtype.kind
    size = dtype.itemsize

    if (kind == 'S'):
        # string type
        return 'VARCHAR2(%d)' % size
    elif (kind == 'i'):
        if (size == 1):
            # 1-byte (16 bit) integer
            return 'NUMBER(2,0)'
        elif (size == 2):
            # 2-byte (16 bit) integer
            return 'NUMBER(6,0)'
        elif (size == 4):
            # 4-byte (32 bit) integer
            return 'NUMBER(11,0)'
        else:
            # 8-byte (64 bit) integer
            return 'NUMBER'
    elif (kind == 'f'):
        if (size == 4):
            # 4-byte (32 bit) float
            return 'BINARY_FLOAT'
        elif (size == 8):
            # 8-byte (64 bit) double
            return 'BINARY_DOUBLE'
        else:
            msg = "Unsupported float type: %s" % kind
            raise ValueError(msg)
    else:
        msg = "Unsupported type: %s" % kind
        raise ValueError(msg)


def write_to_fits(df, fitsfile, fileindex, mode='w', listN=[], listT=[], fits_max_mb=1000):
    # build the dtypes...
    dtypes = []
    for col in df:
        if col in listN:
            fmt = listT[listN.index(col)]
        else:
            fmt = df[col].dtype.name

        if fmt == 'FLOAT':  # fot objects -- some --
            dtypes.append((col, 'f8', len(df[col].values[0])))
        else:
            dtypes.append((col, fmt))


    # create the numpy array to write
    arr = np.zeros(len(df.index), dtype=dtypes)

    # fill array
    for col in df:
        if col in listN:
            fmt = listT[listN.index(col)]
            if fmt == 'FLOAT':
                temp_arr = np.array(df[col].values.tolist())
                arr[col] = temp_arr
            else:
                arr[col][:] = df[col].values
        else:
            arr[col][:] = df[col].values

    # write or append...
    if mode == 'w':
        # assume that this is smaller than the max size!
        if os.path.exists(fitsfile): os.remove(fitsfile)
        fitsio.write(fitsfile, arr, clobber=True)
        return fileindex
    elif mode == 'a':
        # what is the actual name of the current file?
        fileparts = fitsfile.split('.fits')

        if (fileindex == 1):
            thisfile = fitsfile
        else:
            thisfile = '%s_%06d.fits' % (fileparts[0], fileindex)

        # check the size of the current file
        size = float(os.path.getsize(thisfile)) / (2. ** 20)

        if (size > fits_max_mb):
            # it's time to increment
            if (fileindex == 1):
                # this is the first one ... it needs to be moved
                # we're doing a 1-index thing here, because...
                os.rename(fitsfile, '%s_%06d.fits' % (fileparts[0], fileindex))

            # and make a new filename, after incrementing
            fileindex += 1

            thisfile = '%s_%06d.fits' % (fileparts[0], fileindex)

            if os.path.exists(thisfile): os.remove(thisfile)
            fitsio.write(thisfile, arr, clobber=True)
            return fileindex
        else:
            # just append
            fits = fitsio.FITS(thisfile, mode='rw')
            fits[1].append(arr)
            fits.close()
            return fileindex

    else:
        msg = "Illegal write mode!"
        raise Exception(msg)


class easy_or(cmd.Cmd, object):
    """cx_oracle interpreter for DESDM"""

    def __init__(self, conf, desconf, db, interactive=True, quiet=False):
        cmd.Cmd.__init__(self)
        self.intro = colored(
            "\neasyaccess  %s. The DESDM Database shell. \n* Type help or ? to list commands. *\n" % __version__,
            "cyan")
        self.writeconfig = False
        self.config = conf
        self.quiet = quiet
        self.desconfig = desconf
        self.editor = os.getenv('EDITOR', self.config.get('easyaccess', 'editor'))
        self.timeout = self.config.getint('easyaccess', 'timeout')
        self.prefetch = self.config.getint('easyaccess', 'prefetch')
        self.loading_bar = self.config.getboolean('display', 'loading_bar')
        self.nullvalue = self.config.getint('easyaccess', 'nullvalue')
        self.outfile_max_mb = self.config.getint('easyaccess', 'outfile_max_mb')
        self.autocommit = self.config.getboolean('easyaccess', 'autocommit')
        self.dbname = db
        self.savePrompt = colored('_________', 'cyan') + '\nDESDB ~> '
        self.prompt = self.savePrompt
        self.buff = None
        self.interactive = interactive
        self.undoc_header = None
        self.metadata = True
        self.doc_header = colored(' *General Commands*', "cyan") + ' (type help <command>):'
        self.docdb_header = colored('\n *DB Commands*', "cyan") + '      (type help <command>):'
        # connect to db
        self.user = self.desconfig.get('db-' + self.dbname, 'user')
        self.dbhost = self.desconfig.get('db-' + self.dbname, 'server')
        self.port = self.desconfig.get('db-' + self.dbname, 'port')
        self.password = self.desconfig.get('db-' + self.dbname, 'passwd')
        kwargs = {'host': self.dbhost, 'port': self.port, 'service_name': self.dbname}
        self.dsn = cx_Oracle.makedsn(**kwargs)
        if not self.quiet: print('Connecting to DB ** %s ** ...' % self.dbname)
        connected = False
        for tries in range(3):
            try:
                self.con = cx_Oracle.connect(self.user, self.password, dsn=self.dsn)
                if self.autocommit: self.con.autocommit = True
                connected = True
                break
            except Exception as e:
                lasterr = str(e).strip()
                print(colored("Error when trying to connect to database: %s" % lasterr, "red"))
                print("\n   Retrying...\n")
                time.sleep(8)
        if not connected:
            print('\n ** Could not successfully connect to DB. Try again later. Aborting. ** \n')
            os._exit(0)
        self.cur = self.con.cursor()
        self.cur.arraysize = self.prefetch
        msg = self.last_pass_changed()
        if msg and not self.quiet: print(msg)


    def handler(self, signum, frame):
        print('Ctrl+Z pressed')
        print('Job = %d Stopped' % pid)
        print(colored(' * Type bg to send this job to the background *', 'cyan'))
        print(colored(' * Type fg to bring this job to the foreground *', 'cyan'))
        print()
        os.kill(pid, signal.SIGSTOP)
        try:
            if self.loading_bar:
                if self.pload.pid != None:
                    os.kill(self.pload.pid, signal.SIGKILL)
        except AttributeError:
            pass


    # ## OVERRIDE CMD METHODS

    def cmdloop(self, intro=None):
        """Repeatedly issue a prompt, accept input, parse an initial prefix
        off the received input, and dispatch to action methods, passing them
        the remainder of the line as argument.

        """
        signal.signal(signal.SIGTSTP, self.handler)
        self.preloop()
        if self.use_rawinput and self.completekey:
            try:
                import readline

                self.old_completer = readline.get_completer()
                readline.set_completer(self.complete)
                # readline.parse_and_bind(self.completekey+": complete")
                if 'libedit' in readline.__doc__:
                    # readline linked to BSD libedit
                    if self.completekey == 'tab':
                        key = '^I'
                    else:
                        key = self.completekey
                    readline.parse_and_bind("bind %s rl_complete" % (key,))
                else:
                    # readline linked to the real readline
                    readline.parse_and_bind(self.completekey + ": complete")
            except ImportError:
                pass
        try:
            if intro is not None:
                self.intro = intro
            if self.intro:
                if not self.quiet:
                    if self.metadata:
                        # self.do_clear(None) #not cleaning screen
                        print()
                    dl.print_deslogo(color_term)
                    self.stdout.write(str(self.intro) + "\n")
            stop = None
            while not stop:
                if self.cmdqueue:
                    line = self.cmdqueue.pop(0)
                else:
                    if self.use_rawinput:
                        try:
                            line = input(self.prompt)
                        except EOFError:
                            line = 'EOF'
                    else:
                        self.stdout.write(self.prompt)
                        self.stdout.flush()
                        line = self.stdin.readline()
                        if not len(line):
                            line = 'EOF'
                        else:
                            line = line.rstrip('\r\n')
                line = self.precmd(line)
                stop = self.onecmd(line)
                stop = self.postcmd(stop, line)
            self.postloop()
        finally:
            if self.use_rawinput and self.completekey:
                try:
                    import readline

                    readline.set_completer(self.old_completer)
                except ImportError:
                    pass

    def do_help(self, arg):
        'List available commands with "help" or detailed help with "help cmd".'
        if arg:
            # XXX check arg syntax
            try:
                func = getattr(self, 'help_' + arg)
            except AttributeError:
                try:
                    doc = getattr(self, 'do_' + arg).__doc__
                    if doc:
                        doc = str(doc)
                        if doc.find('DB:') > -1: doc = doc.replace('DB:', '')
                        self.stdout.write("%s\n" % str(doc))
                        return
                except AttributeError:
                    pass
                self.stdout.write("%s\n" % str(self.nohelp % (arg,)))
                return
            func()
        else:
            self.do_clear(None)
            dl.print_deslogo(color_term)
            names = self.get_names()
            cmds_doc = []
            cmds_undoc = []
            cmds_db = []
            help = {}
            for name in names:
                if name[:5] == 'help_':
                    help[name[5:]] = 1
            names.sort()
            # There can be duplicates if routines overridden
            prevname = ''
            for name in names:
                if name[:3] == 'do_':
                    if name == prevname:
                        continue
                    prevname = name
                    cmd = name[3:]
                    if cmd in help:
                        cmds_doc.append(cmd)
                        del help[cmd]
                    elif getattr(self, name).__doc__:
                        doc = getattr(self, name).__doc__
                        if doc.find('DB:') > -1:
                            cmds_db.append(cmd)
                        else:
                            cmds_doc.append(cmd)
                    else:
                        cmds_undoc.append(cmd)
            self.stdout.write("%s\n" % str(self.doc_leader))
            self.print_topics(self.doc_header, cmds_doc, 15, 80)
            self.print_topics(self.docdb_header, cmds_db, 15, 80)
            self.print_topics(self.misc_header, list(help.keys()), 15, 80)
            self.print_topics(self.undoc_header, cmds_undoc, 15, 80)

            print(colored(' *Default Input*', 'cyan'))
            print('===================================================')
            print("* To run SQL queries just add ; at the end of query")
            print("* To write to a file  : select ... from ... where ... ; > filename")
            print(colored("* Supported file formats (.csv, .tab., .fits, .h5) ", "green"))
            print("* To check SQL syntax : select ... from ... where ... ; < check")
            print("* To see the Oracle execution plan  : select ... from ... where ... ; < explain")
            print()
            print("* To access an online tutorial type: online_tutorial ")


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
        tcache = threading.Timer(120, self.con.cancel)
        tcache.start()
        try:
            if not self.quiet: print('Loading metadata into cache...')

            cmd.Cmd.preloop(self)  # # sets up command completion
            create_metadata = False
            check = 'select count(table_name) from user_tables where table_name = \'FGOTTENMETADATA\''
            self.cur.execute(check)
            if self.cur.fetchall()[0][0] == 0:
                create_metadata = True
            else:
                query_time = "select created from dba_objects where object_name = \'FGOTTENMETADATA\' and owner =\'%s\'  " % (
                    self.user.upper())
                qt = self.cur.execute(query_time)
                last = qt.fetchall()
                now = datetime.datetime.now()
                diff = abs(now - last[0][0]).seconds / (3600.)
                if diff >= 24: create_metadata = True
            if create_metadata:
                query_2 = """create table fgottenmetadata  as  select * from table (fgetmetadata)"""
                self.cur.execute(query_2)

            self.cache_table_names = self.get_tables_names()
            self.cache_usernames = self.get_userlist()
            self.cache_column_names = self.get_columnlist()
            self.metadata = True
            tcache.cancel()

        except:
            print(colored(
                "\n Couldn't load metadata into cache (try later), no autocompletion for tables, columns or users this time\n",
                "red"))
            tcache.cancel()
            self.cache_table_names = []
            self.cache_usernames = []
            self.cache_column_names = []
            self.metadata = False





        # history
        ht = open(history_file, 'r')
        Allq = ht.readlines()
        ht.close()
        self._hist = []
        for lines in Allq: self._hist.append(lines.strip())
        self._locals = {}  # # Initialize execution namespace for user
        self._globals = {}


    def precmd(self, line):
        """ This method is called after the line has been input but before
             it has been interpreted. If you want to modify the input line
             before execution (for example, variable substitution) do it here.
         """
        try:
            self.con.ping()
        except:
            self.con = cx_Oracle.connect(self.user, self.password, dsn=self.dsn)
            if self.autocommit: self.con.autocommit = True
            self.cur = self.con.cursor()
            self.cur.arraysize = self.prefetch

        # handle line continuations -- line terminated with \
        # beware of null lines.
        line = ' '.join(line.split())
        self.buff = line
        while line and line[-1] == "\\":
            self.buff = self.buff[:-1]
            line = line[:-1]  # strip terminal \
            temp = input('... ')
            self.buff += '\n' + temp
            line += temp

        # self.prompt = self.savePrompt

        if not line: return ""  # empty line no need to go further
        if line[0] == "@":
            if len(line) > 1:
                fbuf = line[1:]
                if fbuf.find('>') > -1:
                    try:
                        fbuf = "".join(fbuf.split())
                        line = read_buf(fbuf.split('>')[0])
                        if line == "": return ""
                        if line.find('>') > -1:
                            line = line.split('>')[0]
                        outputfile = fbuf.split('>')[1]
                        line = line + ' > ' + outputfile
                    except:
                        outputfile = ''

                else:
                    line = read_buf(fbuf.split()[0])
                    if line == "": return ""

                self.buff = line
                print()
                print(line)
            else:
                print('@ must be followed by a filename')
                return ""
        if line[0] == '.':
            if len(line) == 1:
                self.do_clear(None)
                return ""


        # support model_query Get
        # self.prompt = self.savePrompt

        self._hist += [line.strip()]
        return line

    def emptyline(self):
        pass

    # This function overrides the cmd function to remove some characters from the delimiter
    def complete(self, text, state):
        """Return the next possible completion for 'text'.

        If a command has not been entered, then complete against command list.
        Otherwise try to call complete_<command> to get list of completions.
        """
        if state == 0:
            import readline

            readline.set_completer_delims(' \t\n`~!@#$%^&*()=[{]}\\|;:\'",<>/?')  # #overrides default delimiters
            origline = readline.get_line_buffer()
            line = origline.lstrip()
            stripped = len(origline) - len(line)
            begidx = readline.get_begidx() - stripped
            endidx = readline.get_endidx() - stripped
            if begidx > 0:
                cmd, args, foo = self.parseline(line)
                if cmd == '':
                    compfunc = self.completedefault
                else:
                    try:
                        compfunc = getattr(self, 'complete_' + cmd)
                    except AttributeError:
                        compfunc = self.completedefault
            else:
                compfunc = self.completenames
            self.completion_matches = compfunc(text, line, begidx, endidx)
        try:
            return self.completion_matches[state]
        except IndexError:
            return None


    def default(self, line):
        fend = line.find(';')
        if fend > -1:
            # with open('easy.buf', 'w') as filebuf:
            # filebuf.write(self.buff)
            query = line[:fend]
            if line[fend:].find('<') > -1:
                app = line[fend:].split('<')[1].strip().split()[0]
                if app.find('check') > -1:
                    print('\nChecking statement...')
                    try:
                        self.cur.parse(query)
                        print(colored('Ok!\n', 'green'))
                        return
                    except:
                        (type, value, traceback) = sys.exc_info()
                        print()
                        print(colored(type, "red"))
                        print(colored(value, "red"))
                        print()
                        return
                elif app.find('submit') > -1:
                    print(colored('\nTo be done: Submit jobs to the DB cluster', 'cyan'))
                    return
                elif app.find('explain') > -1:
                    exquery = 'explain plan for ' + query
                    try:
                        self.cur.execute(exquery)
                        planquery = 'SELECT PLAN_TABLE_OUTPUT FROM TABLE(DBMS_XPLAN.DISPLAY)'
                        self.query_and_print(planquery)
                        return
                    except:
                        print('Something went wrong')
                        return
                else:
                    return
            if line[fend:].find('>') > -1:
                try:
                    fileout = line[fend:].split('>')[1].strip().split()[0]
                    fileformat = fileout.split('.')[-1]
                    if fileformat in options_out:
                        print('\nFetching data and saving it to %s ...' % fileout + '\n')
                        self.query_and_save(query, fileout)
                    else:
                        print(colored('\nFile format not valid.\n', 'red'))
                        print('Supported formats:\n')
                        for jj, ff in enumerate(options_out): print('%5s  %s' % (ff, options_def[jj]))
                except KeyboardInterrupt or EOFError:
                    print(colored('\n\nAborted \n', "red"))
                except:
                    print(colored('\nMust indicate output file\n', "red"))
                    print('Format:\n')
                    print('select ... from ... where ... ; > example.csv \n')
            else:
                try:
                    self.query_and_print(query)
                except:
                    try:
                        self.con.cancel()
                    except:
                        pass
                    print(colored('\n\nAborted \n', "red"))


        else:
            print()
            print('Invalid command or missing ; at the end of query.')
            print('Type help or ? to list commands')
            print()

    def completedefault(self, text, line, begidx, lastidx):
        qstop = line.find(';')
        if qstop > -1:
            if line[qstop:].find('>') > -1:
                line = line[qstop + 1:]
                return _complete_path(line)
            if line[qstop:].find('<') > -1:
                if text:
                    return [option for option in options_app if option.startswith(text)]
                else:
                    return options_app

        if line[0] == '@':
            line = '@ ' + line[1:]
            return _complete_path(line)
        if line.upper().find('SELECT') > -1:
            # return self._complete_colnames(text)
            if line.upper().find('FROM') == -1:
                return self._complete_colnames(text)
            elif line.upper().find('FROM') > -1 and line.upper().find('WHERE') == -1:
                return self._complete_tables(text)
            else:
                return self._complete_colnames(text)
        else:
            return self._complete_tables(text)


    # ## QUERY METHODS

    def last_pass_changed(self):
        """
        Return creation time and last time password was modified
        """
        query = """
        select sysdate-ctime creation, sysdate-ptime passwd from sys.user$ where name = '%s' """ % self.user.upper()
        try:
            self.cur.execute(query)
            TT = self.cur.fetchall()
            ctime = int(TT[0][0])
            ptime = int(TT[0][1])
        except:
            return None
        if ptime > 200:
            msg = colored("*Important* ", "red") + 'Last time password change was ' + colored("%d" % ptime,
                                                                                              "red") + " days ago"
            if ptime == ctime: msg += colored(" (Never in your case!)", "red")
            msg += '\n Please change it using the ' + colored("set_password",
                                                                   "cyan") + ' command to get rid of this message\n'
            return msg


    def query_and_print(self, query, print_time=True, err_arg='No rows selected', suc_arg='Done!', extra="",
                        clear=False):
        #to be safe
        query = query.replace(';','')
        self.cur.arraysize = self.prefetch
        tt = threading.Timer(self.timeout, self.con.cancel)
        tt.start()
        t1 = time.time()
        if self.loading_bar: self.pload = Process(target=loading)
        if self.loading_bar: self.pload.start()
        try:
            self.cur.execute(query)
            if self.cur.description != None:
                header = [columns[0] for columns in self.cur.description]
                htypes = [columns[1] for columns in self.cur.description]
                info = [rec[1:6] for rec in self.cur.description]
                # data = pd.DataFrame(self.cur.fetchall())
                data = pd.DataFrame(self.cur.fetchmany())
                while True:
                    if data.empty: break
                    rowline = '   Rows : %d, Avg time (rows/sec): %.1f ' % (
                        self.cur.rowcount, self.cur.rowcount * 1. / (time.time() - t1))
                    if self.loading_bar: sys.stdout.write(colored(rowline, 'yellow'))
                    if self.loading_bar: sys.stdout.flush()
                    if self.loading_bar: sys.stdout.write('\b' * len(rowline))
                    if self.loading_bar: sys.stdout.flush()
                    temp = pd.DataFrame(self.cur.fetchmany())
                    if not temp.empty:
                        data = data.append(temp, ignore_index=True)
                    else:
                        break
                t2 = time.time()
                tt.cancel()
                if self.loading_bar:
                    # self.pload.terminate()
                    if self.pload.pid != None: os.kill(self.pload.pid, signal.SIGKILL)
                if clear: self.do_clear(None)
                print()
                if print_time: print(colored('\n%d rows in %.2f seconds' % (len(data), (t2 - t1)), "green"))
                if print_time: print()
                if len(data) == 0:
                    fline = '   '
                    for col in header: fline += '%s  ' % col
                    if extra != "":
                        print(colored(extra + '\n', "cyan"))
                    print(fline)
                    print(colored(err_arg, "red"))
                else:
                    data.columns = header
                    data.index += 1
                    if extra != "":
                        print(colored(extra + '\n', "cyan"))
                    # ADW: Oracle distinguishes between NaN and Null while pandas 
                    # does not making this replacement confusing...
                    # ##try:
                    # ##    data.fillna('Null', inplace=True)
                    # ##except:
                    ###    pass
                    print(data)
            else:
                t2 = time.time()
                tt.cancel()
                if self.loading_bar:
                    # self.pload.terminate()
                    if self.pload.pid != None: os.kill(self.pload.pid, signal.SIGKILL)
                if clear: self.do_clear(None)
                print(colored(suc_arg, "green"))
                if self.autocommit: self.con.commit()
            print()
        except:
            (type, value, traceback) = sys.exc_info()
            self.con.cancel()
            t2 = time.time()
            if self.loading_bar:
                # self.pload.terminate()
                if self.pload.pid != None: os.kill(self.pload.pid, signal.SIGKILL)
            print()
            print(colored(type, "red"))
            print(colored(value, "red"))
            print()
            if t2 - t1 > self.timeout:
                print('\nQuery is taking too long for printing on screen')
                print('Try to output the results to a file')
                print('Using > FILENAME after query, ex: select from ... ; > test.csv')
                print('To see a list of compatible format\n')


    def query_and_save(self, query, fileout, print_time=True):
        """
        Executes a query and save the results to a file, Supported formats are
        .csv, .tab, .fits and .h5
        """
        query = query.replace(';','')
        fileformat = fileout.split('.')[-1]
        mode = fileformat
        if fileformat in options_out:
            pass
        else:
            print(colored('\nFile format not valid.\n', 'red'))
            print('Supported formats:\n')
            for jj, ff in enumerate(options_out): print('%5s  %s' % (ff, options_def[jj]))
            return
        if mode != fileout.split('.')[-1]:
            print(colored(' fileout extension and mode do not match! \n', "red"))
            return
        fileout_original = fileout
        self.cur.arraysize = self.prefetch
        t1 = time.time()
        if self.loading_bar: self.pload = Process(target=loading)
        if self.loading_bar: self.pload.start()
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
                    rowline = '   Rows : %d, Avg time (rows/sec): %.1f ' % (
                        self.cur.rowcount, self.cur.rowcount * 1. / (time.time() - t1))
                    if self.loading_bar: sys.stdout.write(colored(rowline, 'yellow'))
                    if self.loading_bar: sys.stdout.flush()
                    if self.loading_bar: sys.stdout.write('\b' * len(rowline))
                    if self.loading_bar: sys.stdout.flush()
                    com_it += 1
                    if first:
                        fileindex = 1  # 1-indexed for backwards compatibility
                        list_names = []
                        list_type = []
                        for inf in info:
                            if inf[1] == or_s:
                                list_names.append(inf[0])
                                # list_type.append(str(inf[3]) + 'A') #pyfits uses A, fitsio S
                                list_type.append('S' + str(inf[3]))
                            if inf[1] == or_ov:
                                list_names.append(inf[0])
                                list_type.append('FLOAT')
                            if inf[1] == or_dt:
                                list_names.append(inf[0])
                                list_type.append('S50')

                    if not data.empty:
                        data.columns = header
                        data.fillna(self.nullvalue, inplace=True)
                        for jj, col in enumerate(data):
                            nt = change_type(info[jj])
                            if nt != "": data[col] = data[col].astype(nt)

                        if mode_write == 'a' and mode in ('csv', 'tab', 'h5'):
                            fileparts = fileout_original.split('.' + mode)
                            if (fileindex == 1):
                                thisfile = fileout_original
                            else:
                                thisfile = '%s_%06d.%s' % (fileparts[0], fileindex, mode)

                            # check the size of the current file
                            size = float(os.path.getsize(thisfile)) / (2. ** 20)

                            if (size > self.outfile_max_mb):
                                # it's time to increment
                                if (fileindex == 1):
                                    # this is the first one ... it needs to be moved
                                    # we're doing a 1-index thing here, because...
                                    os.rename(fileout_original, '%s_%06d.%s' % ( fileparts[0], fileindex, mode))

                                # and make a new filename, after incrementing
                                fileindex += 1

                                thisfile = '%s_%06d.%s' % (fileparts[0], fileindex, mode)
                                fileout = thisfile
                                mode_write = 'w'
                                header_out = True
                                first = True

                            else:
                                fileout = thisfile
                                header_out = False

                        if mode == 'csv': data.to_csv(fileout, index=False, float_format='%.8f', sep=',',
                                                      mode=mode_write, header=header_out)
                        if mode == 'tab': data.to_csv(fileout, index=False, float_format='%.8f', sep=' ',
                                                      mode=mode_write, header=header_out)
                        if mode == 'h5':  data.to_hdf(fileout, 'data', mode=mode_write, index=False,
                                                      header=header_out)  # , complevel=9,complib='bzip2'
                        if mode == 'fits': fileindex = write_to_fits(data, fileout, fileindex, mode=mode_write,
                                                                     listN=list_names,
                                                                     listT=list_type, fits_max_mb=self.outfile_max_mb)
                        if first:
                            mode_write = 'a'
                            header_out = False
                            first = False
                    else:
                        break
                t2 = time.time()
                if self.loading_bar:
                    # self.pload.terminate()
                    if self.pload.pid != None: os.kill(self.pload.pid, signal.SIGKILL)
                elapsed = '%.1f seconds' % (t2 - t1)
                print()
                if print_time: print(colored('\n Written %d rows to %s in %.2f seconds and %d trips' % (
                    self.cur.rowcount, fileout, (t2 - t1), com_it - 1), "green"))
                if print_time: print()
            else:
                pass
            print()
        except:
            (type, value, traceback) = sys.exc_info()
            if self.loading_bar:
                # self.pload.terminate()
                if self.pload.pid != None: os.kill(self.pload.pid, signal.SIGKILL)
            print()
            print(colored(type, "red"))
            print(colored(value, "red"))
            print()


    def query_results(self, query):
        self.cur.execute(query)
        data = self.cur.fetchall()
        return data

    def get_tables_names(self):

        if self.dbname in ('dessci', 'desoper'):
            query = """
            select distinct table_name from fgottenmetadata
            union select distinct t1.owner || '.' || t1.table_name from all_tab_cols t1,
            des_users t2 where upper(t1.owner)=upper(t2.username) and t1.owner not in ('DES_ADMIN')"""
        if self.dbname in ('destest'):
            query = """
            select distinct table_name from fgottenmetadata
            union select distinct t1.owner || '.' || t1.table_name from all_tab_cols t1,
            dba_users t2 where upper(t1.owner)=upper(t2.username) and t1.owner not in ('XDB','SYSTEM','SYS', 'DES_ADMIN', 'EXFSYS' ,'MDSYS','WMSYS','ORDSYS')"""
        temp = self.cur.execute(query)
        tnames = pd.DataFrame(temp.fetchall())
        table_list = tnames.values.flatten().tolist()
        return table_list

    def get_tables_names_user(self, user):
        if user == "": return self.do_help('tables_names_user')
        user = user.replace(";", "")
        query = "select distinct table_name from all_tables where owner=\'%s\' order by table_name" % user.upper()
        temp = self.cur.execute(query)
        tnames = pd.DataFrame(temp.fetchall())
        self.do_clear(None)
        if len(tnames) > 0:
            print(colored('\nPublic tables from %s' % user.upper(), "cyan"))
            print(tnames)
            # Add tname to cache (no longer needed)
            # table_list=tnames.values.flatten().tolist()
            # for table in table_list:
            # tn=user.upper()+'.'+table.upper()
            # try : self.cache_table_names.index(tn)
            # except: self.cache_table_names.append(tn)
            #self.cache_table_names.sort()
        else:
            if self.dbname in ('dessci', 'desoper'):
                query = """select count(username) as cc  from des_users where upper(username) = upper('%s')""" % user
            if self.dbname in ('destest'):
                query = """select count(username) as cc from dba_users where upper(username) = upper('%s')""" % user
            temp = self.cur.execute(query)
            tnames = temp.fetchall()
            if tnames[0][0] == 0:
                print(colored('User %s does not exist in DB' % user.upper(), 'red'))
            else:
                print(colored('User %s has no tables' % user.upper(), 'cyan'))

    def get_userlist(self):
        if self.dbname in ('dessci', 'desoper'):
            query = 'select distinct username from des_users order by username'
        if self.dbname in ('destest'):
            query = 'select distinct username from dba_users order by username'
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

    def get_columnlist_table(self, tablename):
        query = """SELECT distinct column_name from fgottenmetadata where table_name = %s order by column_name""" % (
            tablename)
        temp = self.cur.execute(query)
        cnames = pd.DataFrame(temp.fetchall())
        col_list = cnames.values.flatten().tolist()
        return col_list


    # # DO METHODS
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
            print('\nPrefetch value = {:}\n'.format(self.prefetch))
        elif line.find('set') > -1:
            val = line.split('set')[-1]
            if val != '':
                self.prefetch = int(val)
                self.config.set('easyaccess', 'prefetch', str(val))
                self.writeconfig = True
                print('\nPrefetch value set to  {:}\n'.format(self.prefetch))
        elif line.find('default') > -1:
            self.prefetch = 10000
            self.config.set('easyaccess', 'prefetch', '10000')
            o
            self.writeconfig = True
            print('\nPrefetch value set to default (10000) \n')
        else:
            print('\nPrefetch value = {:}\n'.format(self.prefetch))

    def complete_prefetch(self, text, line, start_index, end_index):
        if text:
            return [option for option in options_prefetch if option.startswith(text)]
        else:
            return options_prefetch


    def do_history(self, arg):
        """
        Print the history buffer to the screen, oldest to most recent.
        IF argument n is present print the most recent N items.

        Usage: history [n]
        """
        if readline_present:
            nall = readline.get_current_history_length()
            firstprint = 0
            if arg.strip(): firstprint = max(nall - int(arg), 0)
            for index in range(firstprint, nall):
                print(index, readline.get_history_item(index))


    def do_shell(self, line):
        """
        Execute shell commands, ex. shell pwd
        You can also use !<command> like !ls, or !pwd to access the shell

        Uses autocompletion after first command
        """
        os.system(line)


    def complete_shell(self, text, line, start_idx, end_idx):
        if line:
            line = ' '.join(line.split()[1:])
            return _complete_path(line)


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
            print('\nEditor  = {:}\n'.format(self.editor))
        elif line.find('set_editor') > -1:
            val = line.split('set_editor')[-1]
            if val != '':
                self.editor = val
                self.config.set('easyaccess', 'editor', val)
                self.writeconfig = True
        else:
            os.system(self.editor + ' easy.buf')
            if os.path.exists('easy.buf'):
                newquery = read_buf('easy.buf')
                if newquery == "": return
                print()
                print(newquery)
                print()
                if (input('submit query? (Y/N): ') in ['Y', 'y', 'yes']):
                    self.default(newquery)


    def complete_edit(self, text, line, start_index, end_index):
        if text:
            return [option for option in options_edit if option.startswith(text)]
        else:
            return options_edit

    def do_loadsql(self, line):
        """
        DB:Loads a sql file with a query and ask whether it should be run
        There is a shortcut using @, ex : @test.sql  (or @test.sql > myfile.csv to override output file)

        Usage: loadsql <filename with sql statement>   (use autocompletion)

        Optional: loadsql <filename with sql statement> > <output_file> to write to a file, not to the screen
        """

        if line.find('>') > -1:
            try:
                line = "".join(line.split())
                newq = read_buf(line.split('>')[0])
                if newq.find('>') > -1:
                    newq = newq.split('>')[0]
                outputfile = line.split('>')[1]
                newq = newq + ' > ' + outputfile
            except:
                outputfile = ''


        else:
            newq = read_buf(line)

        if newq == "": return
        if self.interactive:
            print()
            print(newq)
            print()
            if (input('submit query? (Y/N): ') in ['Y', 'y', 'yes']):
                self.default(newq)
        else:
            self.default(newq)


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
            self.cur.close()
        except:
            pass
        try:
            if self.autocommit: self.con.commit()
            self.con.close()
        except:
            pass
        if readline_present:
            readline.write_history_file(history_file)
        if self.writeconfig:
            self.config.set('display', 'loading_bar', 'yes' if load_bar else 'no')
            config_mod.write_config(config_file, self.config)
        os._exit(0)

    def do_clear(self, line):
        """
        Clear screen. There is a shortcut by typing . on the interpreter
        """
        # TODO: platform dependent
        # tmp = sp.call('clear', shell=True)
        sys.stdout.flush()
        #tmp = os.system(['clear', 'cls'][os.name == 'nt'])
        pass

    def do_config(self, line):
        """
        Change parameters from config file (config.ini). Smart autocompletion enabled

        Usage:
            - config <parameter> show : Shows current value for parameter in config file
                e.j.
            - config <parameter> set <value> : Sets parameter to given value
            - config all show: Shows all parameters and their values
            - config filepath: Prints the path to the config file

        Parameters:
            database          : Default DB to connect to
            editor            : Editor for editing sql queries, see --> help edit
            prefetch          : Number of rows prefetched by Oracle, see --> help prefetch
            histcache         : Length of the history of commands
            timeout           : Timeout for a query to be printed on the screen. Doesn't apply to output files
            nullvalue         : value to replace Null entries when writing a file (default = -9999)
            outfile_max_mb    : Max size of each fits file in MB
            max_rows          : Max number of rows to display on the screen. Doesn't apply to output files
            width             : Width of the output format on the screen
            max_columns       : Max number of columns to display on the screen. Doesn't apply to output files
            max_colwidth      : Max number of characters per column at display. Doesn't apply to output files
            color_terminal    : yes/no toggles the color for terminal std output. Need to restart easyaccess
            loading_bar       : yes/no toggles the loading bar. Useful for background jobs
            autocommit        : yes/no toggles the autocommit for DB changes (default is yes)
        """
        global load_bar
        if line == '': return self.do_help('config')
        oneline = "".join(line.split())
        if oneline.find('show') > -1:
            key = oneline.split('show')[0]
            for section in (self.config.sections()):
                if key == 'all':
                    print()
                    for key0, val in self.config.items(section):
                        strr = 'Current value for %s' % key0
                        strr = strr.ljust(32)
                        print('%s = %s ' % (strr, val))
                elif key == 'filepath':
                    print('\n config file path = %s\n' % config_file)
                    return
                else:
                    if self.config.has_option(section, key):
                        print('\nCurrent value for %s = %s ' % (key, self.config.get(section, key)))
                        break
            print()
        elif oneline.find('filepath') > -1:
            print('\n config file path = %s\n' % config_file)
        elif oneline.find('set') > -1:
            if oneline.find('all') > -1:
                return self.do_help('config')
            key = oneline.split('set')[0]
            val = oneline.split('set')[1]
            if val == '': return self.do_help('config')
            int_keys = ['prefetch', 'histcache', 'timeout', 'max_rows', 'width', 'max_columns', 'outfile_max_mb',
                        'nullvalue', 'loading_bar', 'autocommit', 'max_col_width']
            # if key in int_keys: val=int(val)
            for section in (self.config.sections()):
                if self.config.has_option(section, key):
                    self.config.set(section, key, str(val))
                    if key == 'loading_bar': load_bar = True if val == 'yes' else False
                    self.writeconfig = True
                    break
            self.config.set('display', 'loading_bar', 'yes' if load_bar else 'no')
            config_mod.write_config(config_file, self.config)
            if key == 'max_columns': pd.set_option('display.max_columns', self.config.getint('display', 'max_columns'))
            if key == 'max_rows': pd.set_option('display.max_rows', self.config.getint('display', 'max_rows'))
            if key == 'width': pd.set_option('display.width', self.config.getint('display', 'width'))
            if key == 'max_colwidth': pd.set_option('display.max_colwidth',
                                                    self.config.getint('display', 'max_colwidth'))
            if key == 'editor': self.editor = self.config.get('easyaccess', 'editor')
            if key == 'timeout': self.timeout = self.config.getint('easyaccess', 'timeout')
            if key == 'prefetch': self.prefetch = self.config.get('easyaccess', 'prefetch')
            if key == 'loading_bar': self.loading_bar = self.config.getboolean('display', 'loading_bar')
            if key == 'nullvalue': self.nullvalue = self.config.getint('easyaccess', 'nullvalue')
            if key == 'outfile_max_mb': self.outfile_max_mb = self.config.getint('easyaccess', 'outfile_max_mb')
            if key == 'autocommit': self.autocommit = self.config.getboolean('easyaccess', 'autocommit')

            return
        else:
            return self.do_help('config')

    def complete_config(self, text, line, start_index, end_index):
        line2 = ' '.join(line.split())
        args = line2.split()
        if text:
            if len(args) > 2:
                return [option for option in options_config2 if option.startswith(text)]
            else:
                return [option for option in options_config if option.startswith(text)]
        else:
            if len(args) > 1:
                return options_config2
            else:
                return options_config


    # DO METHODS FOR DB

    def do_set_password(self, arg):
        """
        DB:Set a new password on this and all other DES instances (DESSCI, DESOPER)

        Usage: set_password
        """
        print()
        pw1 = getpass.getpass(prompt='Enter new password:')
        if re.search('\W', pw1):
            print(colored("\nPassword contains whitespace, not set\n", "red"))
            return
        if not pw1:
            print(colored("\nPassword cannot be blank\n", "red"))
            return
        pw2 = getpass.getpass(prompt='Re-Enter new password:')
        print()
        if pw1 != pw2:
            print(colored("Passwords don't match, not set\n", "red"))
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
                print(colored(confirm, "green"))
                temp_con.commit()
                temp_cur.close()
                temp_con.close()
                self.desconfig.set('db-dessci', 'passwd', pw1)
                self.desconfig.set('db-desoper', 'passwd', pw1)
                config_mod.write_desconfig(desfile, self.desconfig)
            except:
                confirm = 'Password could not changed in %s\n' % db.upper()
                print(colored(confirm, "red"))
                print(sys.exc_info())


    def do_refresh_metadata_cache(self, arg):
        """
        DB:Refreshes meta data cache for auto-completion of table
        names and column names .
        """

        # Meta data access: With the two linked databases, accessing the
        # "truth" via fgetmetadata has become very slow.
        # what it returns is a function of each users's permissions, and their
        # "mydb". so yet another level of caching is needed. Ta loads a table
        # called fgottenmetadata in the user's mydb. It refreshes on command
        # or on timeout (checked at startup).

        # get last update
        verb = True
        if arg == 'quiet': verb = False
        query_time = "select created from dba_objects where object_name = \'FGOTTENMETADATA\' and owner =\'%s\'  " % (
            self.user.upper())
        try:
            qt = self.cur.execute(query_time)
            last = qt.fetchall()
            now = datetime.datetime.now()
            diff = abs(now - last[0][0]).seconds / (3600.)
            if verb: print('Updated %.2f hours ago' % diff)
        except:
            pass
        try:
            query = "DROP TABLE FGOTTENMETADATA"
            self.cur.execute(query)
        except:
            pass
        try:
            if verb: print('\nRe-creating metadata table ...')
            query_2 = """create table fgottenmetadata  as  select * from table (fgetmetadata)"""
            message = 'FGOTTENMETADATA table Created!'
            if not verb:  message = ""
            self.query_and_print(query_2, print_time=False, suc_arg=message)
            if verb: print('Loading metadata into cache...')
            self.cache_table_names = self.get_tables_names()
            self.cache_usernames = self.get_userlist()
            self.cache_column_names = self.get_columnlist()
        except:
            if verb: print(colored("There was an error when refreshing the cache", "red"))


    def do_show_db(self, arg):
        """
        DB:Shows database connection information
        """
        lines = "user: %s\ndb  : %s\nhost: %s\n" % (self.user.upper(), self.dbname.upper(), self.dbhost.upper())
        lines = lines + "\nPersonal links:"
        query = """
        SELECT owner, db_link, username, host, created 
        FROM all_db_links where OWNER = '%s'
        """ % (self.user.upper())
        self.query_and_print(query, print_time=False, extra=lines, clear=True)

    def do_whoami(self, arg):
        """
        DB:Print information about the user's details.

        Usage: whoami
        """
        if self.dbname in ('dessci', 'desoper'):
            sql_getUserDetails = """
            select d.username, d.email, d.firstname as first, d.lastname as last, trunc(sysdate-t.ptime,0)||' days ago' last_passwd_change,
            trunc(sysdate-t.ctime,0)||' days ago' created
            from des_users d, sys.user$ t  where d.username = '""" + self.user + """' and t.name=upper(d.username)"""
        if self.dbname in ('destest'):
            print(colored('\nThis function is not implemented in destest\n', 'red'))
            sql_getUserDetails = "select * from dba_users where username = '" + self.user + "'"
        self.query_and_print(sql_getUserDetails, print_time=False, clear=True)

    def do_myquota(self, arg):
        """
        DB:Print information about quota status.

        Usage: myquota
        """
        query = """
        SELECT tablespace_name, mbytes_used/1024 as GBYTES_USED, 
        mbytes_left/1024 as GBYTES_LEFT from myquota
        """
        self.query_and_print(query, print_time=False, clear=True)

    def do_mytables(self, arg):
        """
        DB:Lists tables you have made in your user schema.

        Usage: mytables
        """
        # query = "SELECT table_name FROM user_tables"
        query = """
        SELECT t.table_name, s.bytes/1024/1024/1024 SIZE_GBYTES 
        FROM user_segments s, user_tables t 
        WHERE s.segment_name = t.table_name
        """

        self.query_and_print(query, print_time=False, extra="List of my tables", clear=True)

    def do_find_user(self, line):
        """
        DB:Finds users given 1 criteria (either first name or last name)

        Usage: 
            - find_user Doe     # Finds all users with Doe in their names
            - find_user John%   # Finds all users with John IN their names (John, Johnson, etc...)
            - find_user P%      # Finds all users with first, lastname or username starting with P

        """
        if line == '': return self.do_help('find_user')
        line = " ".join(line.split())
        keys = line.split()
        if self.dbname in ('dessci', 'desoper'):
            query = 'select * from des_users where '
        if self.dbname in ('destest'):
            query = 'select * from dba_users where '
        if len(keys) >= 1:
            query += 'upper(firstname) like upper(\'' + keys[0] + '\') or upper(lastname) like upper(\'' + keys[
                0] + '\') or upper(username) like upper (\'' + keys[0] + '\')'
        self.query_and_print(query, print_time=False, clear=True)

    def complete_find_user(self, text, line, start_index, end_index):
        options_users = self.cache_usernames
        if text:
            return [option for option in options_users if option.startswith(text.lower())]
        else:
            return options_users


    def do_user_tables(self, arg):
        """
        DB:List tables from given user

        Usage: user_tables <username>
        """
        if arg == "": return self.do_help('user_tables')
        return self.get_tables_names_user(arg)

    def complete_user_tables(self, text, line, start_index, end_index):
        options_users = self.cache_usernames
        if text:
            return [option for option in options_users if option.startswith(text.lower())]
        else:
            return options_users

    def get_tablename_tuple(self, tablename):
        """
        Return the tuple (schema,table,link) that can be used to
        locate the `real` table requested.
        """
        table = tablename
        schema = self.user.upper()  # default --- Mine
        link = ""  # default no link
        
        if "." in table: (schema, table) = table.split(".")
        if "@" in table: (table, link) = table.split("@")

        # Loop until we find a fundamental definition OR determine there is
        # no reachable fundamental definition, follow links and resolve
        # schema names. Rely on how the DES database is constructed we log
        # into our own schema, and rely on synonyms for a "simple" view of
        # common schema.

        while (1):
            # check for fundamental definition  e.g. schema.table@link
            q = """
            select count(*) from ALL_TAB_COLUMNS%s
            where OWNER = '%s' 
            and TABLE_NAME = '%s'
            """ % ("@" + link if link else "", schema, table)
            ans = self.query_results(q)
            if ans[0][0] > 0:
                # found real definition return the tuple
                return (schema,table,link)

            # check if we are indirect by  synonym of mine
            q = """
            select TABLE_OWNER, TABLE_NAME, DB_LINK from USER_SYNONYMS%s
            where SYNONYM_NAME = '%s'
            """ % ("@" + link if link else "", table)
            ans = self.query_results(q)
            if len(ans) == 1:
                # resolved one step closer to fundamental definition
                (schema, table, link) = ans[0]
                continue

            # check if we are indirect by a public synonym
            q = """
            select TABLE_OWNER, TABLE_NAME, DB_LINK from ALL_SYNONYMS%s
            where SYNONYM_NAME = '%s' AND OWNER = 'PUBLIC'
            """ % ("@" + link if link else "", table)
            ans = self.query_results(q)
            if len(ans) == 1:
                # resolved one step closer to fundamental definition
                (schema, table, link) = ans[0]
                continue

            # failed to find the reference to the table
            # no such table accessible by user
            break

        msg = "No table found for: %s"%tablename
        raise Exception(msg)


    def do_describe_table(self, arg, clear=True):
        """
        DB:This tool is useful for noting the lack of documentation
        for columns. If you don't know the full table name you can
        use tab completion on the table name. Tables of usual interest
        are described.

        Usage: describe_table <table_name>
        Describes the columns in <table-name> as
          column_name, oracle_Type, date_length, comments

        Optional: describe_table <table_name> with <pattern>
        Describes only the columns with certain pattern, example:

        describe_table Y1A1_COADD_OBJECTS with MAG%
        will describe only columns starting with MAG in that table
        """
        if arg == '':
            return self.do_help('describe_table')
        arg = arg.replace(';', '')
        arg = " ".join(arg.split())
        tablename = arg.split()[0]
        tablename = tablename.upper()
        pattern = None
        try:
            extra = arg.split()[1]
            if extra.upper() == 'WITH':
                pattern = arg.split()[2].upper()
        except:
            pass

        try: 
            schema,table,link = self.get_tablename_tuple(tablename)
            # schema, table and link are now valid.
            link = "@" + link if link else ""
            qcom = """ 
            select comments from all_tab_comments%s atc 
            where atc.table_name = '%s'""" % (link, table)
            comment_table = self.query_results(qcom)[0][0]
        except: 
            print(colored("Table not found.", "red"))
            return

        # String formatting parameters
        params = dict(schema=schema, table=table, link=link,
                      pattern=pattern, comment=comment_table)
                      
        if pattern:
            comm = """Description of %(table)s with pattern %(pattern)s commented as: '%(comment)s'""" % params
            q = """
            select atc.column_name, atc.data_type,
            atc.data_length || ',' || atc.data_precision || ',' || atc.data_scale DATA_FORMAT, acc.comments
            from all_tab_cols%(link)s atc , all_col_comments%(link)s acc
            where atc.owner = '%(schema)s' and atc.table_name = '%(table)s'
            and acc.owner = '%(schema)s' and acc.table_name = '%(table)s' 
            and acc.column_name = atc.column_name 
            and atc.column_name like '%(pattern)s'
            order by atc.column_id
            """ % params
        else:
            comm = """Description of %(table)s commented as: '%(comment)s'""" % params
            q = """
            select atc.column_name, atc.data_type,
            atc.data_length || ',' || atc.data_precision || ',' || atc.data_scale DATA_FORMAT, acc.comments
            from all_tab_cols%(link)s atc , all_col_comments%(link)s acc
            where atc.owner = '%(schema)s' and atc.table_name = '%(table)s'
            and acc.owner = '%(schema)s' and acc.table_name = '%(table)s' 
            and acc.column_name = atc.column_name
            order by atc.column_id
            """ % params

        self.query_and_print(q, print_time=False,
                             err_arg='Table does not exist or it is not accessible by user or pattern do not match',
                             extra=comm, clear=clear)
        return

    def complete_describe_table(self, text, line, start_index, end_index):
        return self._complete_tables(text)

    def do_find_tables(self, arg):
        """
        DB:Lists tables and views matching an oracle pattern  e.g %SVA%,
        
        Usage : find_tables PATTERN
        """
        if arg == '': return self.do_help('find_tables')
        arg = arg.replace(';', '')
        query = "SELECT distinct table_name from fgottenmetadata  WHERE upper(table_name) LIKE '%s' " % (arg.upper())
        self.query_and_print(query)

    def complete_find_tables(self, text, line, start_index, end_index):
        return self._complete_tables(text)


    def do_find_tables_with_column(self, arg):
        """           
        DB:Finds tables having a column name matching column-name-string.
        
        Usage: find_tables_with_column  <column-name-substring>                                                                 
        Example: find_tables_with_column %MAG%  # hunt for columns with MAG 
        """
        # query  = "SELECT TABLE_NAME, COLUMN_NAME FROM fgottenmetadata WHERE COLUMN_NAME LIKE '%%%s%%' " % (arg.upper())
        if arg == '': return self.do_help('find_tables_with_column')
        query = """
           SELECT table_name, column_name 
           FROM fgottenmetadata 
           WHERE column_name LIKE '%s'  
           UNION
           SELECT LOWER(owner) || '.' || table_name, column_name 
           FROM all_tab_cols
           WHERE column_name LIKE '%s'
           AND owner NOT LIKE '%%SYS'
           AND owner not in ('XDB','SYSTEM')
           """ % (arg.upper(), arg.upper())

        self.query_and_print(query)
        return

    def complete_find_tables_with_column(self, text, line, begidx, lastidx):
        return self._complete_colnames(text)

    def do_show_index(self, arg):
        """
        DB:Describes the indices  in <table-name> as
          column_name, oracle_Type, date_length, comments

         Usage: describe_index <table_name>
        """

        if arg == '':
            return self.do_help('show_index')
        arg = arg.replace(';', '')
        arg = " ".join(arg.split())
        tablename = arg.split()[0]
        tablename = tablename.upper()

        try: 
            schema,table,link = self.get_tablename_tuple(tablename)
            link = "@" + link if link else ""
        except: 
            print(colored("Table not found.", "red"))
            return

        # Parse tablename for simple name or owner.tablename.
        # If owner present, then add owner where clause.

        params = dict(schema=schema,table=table,link=link)
        query = """
        SELECT UNIQUE tab.table_name,icol.column_name,
        idx.index_type,idx.index_name
        FROM all_tables%(link)s tab
        JOIN all_indexes%(link)s idx on idx.table_name = tab.table_name
        JOIN all_ind_columns%(link)s icol on idx.index_name = icol.index_name
        WHERE tab.table_name='%(table)s' and tab.owner = '%(schema)s'
        ORDER BY icol.column_name,idx.index_name
        """ % params
        nresults = self.query_and_print(query)
        return

    def complete_show_index(self, text, line, begidx, lastidx):
        return self._complete_tables(text)

    def get_filename(self, line):
        line = line.replace(';', '')
        if line == "":
            print('\nMust include table filename!\n')
            return
        if line.find('.') == -1:
            print(colored('\nError in filename\n', "red"))
            return

        filename = "".join(line.split())
        basename = os.path.basename(filename)
        alls = basename.split('.')
        if len(alls) > 2:
            print("\nDo not use extra '.' in filename\n")
            return

        return filename

    def check_table_exists(self, table):
        # check table first
        self.cur.execute(
            'select count(table_name) from user_tables where table_name = \'%s\'' % table.upper())
        exists = self.cur.fetchall()[0][0]
        return exists

    def load_data(self, filename):
        base, format = os.path.basename(filename).split('.')
        if format in ('csv', 'tab'):
            if format == 'csv': sepa = ','
            if format == 'tab': sepa = None
            try:
                DF = pd.read_csv(filename, sep=sepa)
            except:
                msg = 'Problem reading %s\n' % filename
                raise Exception(msg)
            # Monkey patch to grab columns and values
            DF.ea_get_columns = DF.columns.values.tolist
            DF.ea_get_values = DF.values.tolist
        elif format in ('fits', 'fit'):
            try:
                DF = fitsio.FITS(filename)
            except:
                msg = 'Problems reading %s\n' % filename
                raise Exception(msg)
            # Monkey patch to grab columns and values
            DF.ea_get_columns = DF[1].get_colnames
            DF.ea_get_values = DF[1].read().tolist
        else:
            msg = "Format not recognized: %s \nUse 'csv' or 'fits' as extensions\n" % format
            raise Exception(msg)
        return DF

    def new_table_columns(self, DF, format='csv'):
        qtable = '( '
        if format in ('csv', 'tab'):
            for col in DF:
                if DF[col].dtype.name == 'object':
                    qtable += col + ' ' + 'VARCHAR2(' + str(max(DF[col].str.len())) + '),'
                elif DF[col].dtype.name.find('int') > -1:
                    qtable += col + ' INT,'
                elif DF[col].dtype.name.find('float') > -1:
                    qtable += col + ' BINARY_DOUBLE,'
                else:
                    qtable += col + ' NUMBER,'
        elif format in ('fits', 'fit'):
            # returns a list of column names
            col_n = DF[1].get_colnames()
            # and the data types
            dtypes = DF[1].get_rec_dtype(vstorage='fixed')[0]
            for i in range(len(col_n)):
                qtable += '%s %s,' % (col_n[i], dtype2oracle(dtypes[i]))
        # cut last , and close paren
        qtable = qtable[:-1] + ')'

        return qtable

    def drop_table(self, table):
        # Do we want to add a PURGE to this query?
        qdrop = "DROP TABLE %s" % table.upper()
        self.cur.execute(qdrop)
        if self.autocommit: self.con.commit()

    def create_table(self, table, DF, format='csv'):
        qtable = 'create table %s ' % table
        qtable += self.new_table_columns(DF, format)
        self.cur.execute(qtable)
        if self.autocommit: self.con.commit()

    def insert_data(self, columns, values, table):
        """ Insert data columns into DB table.
        """
        cols = ','.join(columns)
        vals = ',:'.join(columns)
        vals = ':' + vals
        qinsert = 'insert into %s (%s) values (%s)' % (table.upper(), cols, vals)

        t1 = time.time()
        self.cur.executemany(qinsert, values)
        t2 = time.time()
        if self.autocommit: self.con.commit()
        print(colored(
            '\n Inserted %d rows and %d columns into table %s in %.2f seconds' % (
                len(values), len(columns), table.upper(), t2 - t1), "green"))


    def do_load_table(self, line, name=''):
        """
        DB:Loads a table from a file (csv or fits) taking name from filename and columns from header

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
        filename = self.get_filename(line)
        if filename is None: return
        base, format = os.path.basename(filename).split('.')

        if name == '':
            table = base
        else:
            table = name

        # check table first
        if self.check_table_exists(table):
            print('\n Table already exists. Table can be removed with:' \
                  '\n  DROP TABLE %s;\n' % table.upper())
            return

        try:
            DF = self.load_data(filename)
        except:
            print_exception()
            return

        columns = DF.ea_get_columns()
        values = DF.ea_get_values()

        try:
            self.create_table(table, DF, format)
        except:
            print_exception()
            self.drop_table(table)
            del DF
            return
        del DF

        try:
            self.insert_data(columns, values, table)
        except:
            print_exception()
            self.drop_table(table)
            return

        print(colored('\n Table %s loaded successfully.' % table.upper(), "green"))
        print(colored(
            '\n You might want to refresh the metadata (refresh_metadata_cache)\n so your new table appears during autocompletion',
            "cyan"))
        print()
        print(colored('To make this table public run:', "blue"), '\n')
        print(colored('   grant select on %s to DES_READER; ' % table.upper(), "blue"), '\n')
        return


    def complete_load_table(self, text, line, start_idx, end_idx):
        return _complete_path(line)


    def do_append_table(self, line, name=''):
        """
        DB:Appends a table from a file (csv or fits) taking name from filename and columns from header.

        Usage: append_table <filename>
        Ex: example.csv has the following content
             RA,DEC,MAG
             1.23,0.13,23
             0.13,0.01,22

        This command will append the contents of example.csv to the table named EXAMPLE.

        Note: - For csv or tab files, first line must have the column names (without # or any other comment) and same format
        as data (using ',' or space)
              - For fits file header must have columns names and data types
              - For filenames use <table_name>.csv or <table_name>.fits do not use extra points
        """

        filename = self.get_filename(line)
        if filename is None: return
        base, format = os.path.basename(filename).split('.')

        if name == '':
            table = base
        else:
            table = name

        # check table first 
        if not self.check_table_exists(table):
            print('\n Table does not exist. Table can be created with:' \
                  '\n  CREATE TABLE %s (COL1 TYPE1(SIZE), ..., COLN TYPEN(SIZE));\n' % table.upper())
            return
        try:
            DF = self.load_data(filename)
        except:
            print_exception()
            return

        columns = DF.ea_get_columns()
        values = DF.ea_get_values()
        del DF

        try:
            self.insert_data(columns, values, table)
        except:
            print_exception()
            return

        print(colored('\n Table %s appended successfully.' % table.upper(), "green"))


    def complete_append_table(self, text, line, start_idx, end_idx):
        return _complete_path(line)


    def do_add_comment(self, line):
        """
        DB:Add comments to table and/or columns inside tables

        Usage: 
            - add_comment table <TABLE> 'Comments on table'
            - add_comment column <TABLE.COLUMN> 'Comments on columns'

        Ex:  add_comment table MY_TABLE 'This table contains info'
             add_comment columns MY_TABLE.ID 'Id for my objects'

        This command supports smart-autocompletion. No `;` is 
        necessary (and it will be inserted into comment if used).

        """

        line = " ".join(line.split())
        keys = line.split()
        oneline = "".join(keys)
        if oneline.find('table') > -1:
            if len(keys) == 1:
                print(colored('\nMissing table name\n', "red"))
                return
            table = keys[1]
            if len(keys) == 2:
                print(colored('\nMissing comment for table %s\n' % table, "red"))
                return
            comm = " ".join(keys[2:])
            if comm[0] == "'": comm = comm[1:-1]
            qcom = """comment on table %s is '%s'""" % (table, comm)
            message = "Comment added to table: %s" % table
            self.query_and_print(qcom, print_time=False, suc_arg=message)
        elif oneline.find('column') > -1:
            if len(keys) == 1:
                print(colored('\nMissing column name (TABLE.COLUMN)\n', "red"))
                return
            col = keys[1]
            if len(keys) == 2:
                print(colored('\nMissing comment for column %s\n' % col, "red"))
                return
            if len(keys) > 2 and col.find('.') == -1:
                print(colored('\nMissing column name for table %s\n', "red") % col)
                return
            comm = " ".join(keys[2:])
            if comm[0] == "'": comm = comm[1:-1]
            qcom = """comment on column  %s is '%s'""" % (col, comm)
            message = "Comment added to column: %s in table %s" % (col.split('.')[1], col.split('.')[0])
            self.query_and_print(qcom, print_time=False, suc_arg=message)
        else:
            print(colored('\nMissing arguments\n', "red"))
            self.do_help('add_comment')

    def complete_add_comment(self, text, line, begidx, lastidx):
        if line:
            oneline = "".join(line.strip())
            if oneline.find('table') > -1:
                return self._complete_tables(text)
            elif oneline.find('column') > -1:
                if oneline.find('.') > -1:
                    colname = text.split('.')[-1]
                    tablename = text.split('.')[0]
                    return [tablename + '.' + cn for cn in self._complete_colnames(colname) if cn.startswith(colname)]
                else:
                    return self._complete_tables(text)
            else:
                return [option for option in options_add_comment if option.startswith(text)]
        else:
            return options_add_comment


    # UNDOCCUMENTED DO METHODS


    def do_EOF(self, line):
        # exit program on ^D
        self.do_exit(line)

    def do_quit(self, line):
        self.do_exit(line)

    def do_select(self, line):
        self.default('select ' + line)

    def do_SELECT(self, line):
        self.default('SELECT ' + line)

    def do_clean_history(self, line):
        if readline_present: readline.clear_history()

    def do_online_tutorial(self, line):
        tut = webbrowser.open_new_tab('http://deslogin.cosmology.illinois.edu/~mcarras2/data/DESDM.html')


        # #################################################


def to_pandas(cur):
    """
    Returns a pandas DataFrame from a executed query 
    """
    if cur.description != None:
        data = pd.DataFrame(cur.fetchall(), columns=[rec[0] for rec in cur.description])
    else:
        data = ""
    return data


color_term = True


class connect(easy_or):
    def __init__(self, section='', quiet=False):
        """
        Creates a connection to the DB ans easyaccess commands, section is obtained frmo
        config file, can be bypass here, e.g., section = desoper
        """
        self.quiet = quiet
        conf = config_mod.get_config(config_file)
        self.conf = conf
        pd.set_option('display.max_rows', conf.getint('display', 'max_rows'))
        pd.set_option('display.width', conf.getint('display', 'width'))
        pd.set_option('display.max_columns', conf.getint('display', 'max_columns'))
        pd.set_option('display.max_colwidth', conf.getint('display', 'max_colwidth'))
        if section == '':
            db = conf.get('easyaccess', 'database')
        else:
            db = section
        desconf = config_mod.get_desconfig(desfile, db)
        easy_or.__init__(self, conf, desconf, db, interactive=False, quiet=quiet)
        self.loading_bar = False

    def cursor(self):
        cursor = self.con.cursor()
        cursor.arraysize = self.prefetch
        return cursor

    def ping(self):
        try:
            self.con.ping()
            if not self.quiet: print('Still connected to DB')
        except:
            if not self.quiet: print('Connection with DB lost')

    def close(self):
        self.con.close()

    def query_to_pandas(self, query, prefetch=''):
        """
        Executes a query and return the results in pandas DataFrame
        """
        cursor = self.con.cursor()
        cursor.arraysize = self.prefetch
        if prefetch != '': cursor.arraysize = prefetch
        query = query.replace(';' , '')
        temp = cursor.execute(query)
        if temp.description != None:
            data = pd.DataFrame(temp.fetchall(), columns=[rec[0] for rec in temp.description])
        else:
            data = ""
        cursor.close()
        return data


    def describe_table(self, tablename):
        """
        Describes a table from the DB
        """
        self.do_describe_table(tablename, False)


    def loadsql(self, filename):
        """
        Reads sql statement from a file, returns query to be parsed in query_and_save, query_to_pandas, etc.
        """
        query = read_buf(filename)
        if query.find(';') > -1:
            query = query.split(';')[0]
        return query

    def mytables(self):
        """
        List tables in own schema
        """
        self.do_mytables('')

    def myquota(self):
        """
        Show quota in current database
        """
        self.do_myquota('')

    def load_table(self, table_file, name=''):
        """
        Loads and create a table in the DB. If name is not passed, is taken from
        the filename. Formats supported are 'fits', 'csv' and 'tab' files
        """
        self.do_load_table(table_file, name=name)

    def append_table(self, table_file, name=''):
        """
        Appends data to a table in the DB. If name is not passed, is taken from
        the filename. Formats supported are 'fits', 'csv' and 'tab' files
        """
        self.do_append_table(table_file, name=name)


# #################################################

class MyParser(argparse.ArgumentParser):
    def error(self, message):
        print('\n*****************')
        sys.stderr.write('error: %s \n' % message)
        print('*****************\n')
        self.print_help()
        sys.exit(2)


def initial_message(quiet=False, clear=True):
    if not quiet:
        if clear: os.system(['clear', 'cls'][os.name == 'nt'])
        # No messages for now


if __name__ == '__main__':

    conf = config_mod.get_config(config_file)
    # PANDAS DISPLAY SET UP
    pd.set_option('display.max_rows', conf.getint('display', 'max_rows'))
    pd.set_option('display.width', conf.getint('display', 'width'))
    pd.set_option('display.max_columns', conf.getint('display', 'max_columns'))
    pd.set_option('display.max_colwidth', conf.getint('display', 'max_colwidth'))

    load_bar = conf.getboolean('display', 'loading_bar')
    if load_bar:
        from multiprocessing import Process

    color_term = True
    if not conf.getboolean('display', 'color_terminal'):
        # Careful, this is duplicated from imports at the top of the file
        def colored(line, color): return line
        color_term = False

    # ADW: What about all the readline imports in the code?
    try:
        import readline
        readline_present = True
    except:
        readline_present = False

    if readline_present:
        try:
            readline.read_history_file(history_file)
            readline.set_history_length(conf.getint('easyaccess', 'histcache'))
        except:
            print(colored('readline might have problems accessing history', 'red'))

    parser = MyParser(
        description='Easy access to the DES database. There is a configuration file located in %s for more customizable options' % config_file)
    parser.add_argument("-v", "--version", dest='version', action="store_true",
                        help="show program's version number and exit")
    parser.add_argument("-c", "--command", dest='command', 
                        help="Executes command and exit")
    parser.add_argument("-l", "--loadsql", dest='loadsql', 
                        help="Loads a sql command, execute it and exit")
    parser.add_argument("-lt", "--load_table", dest='loadtable', 
                        help="Loads data from a csv, tab, or fits formatted file \
                        into a DB table using the filename as the table name")
    parser.add_argument("-at", "--append_table", dest='appendtable', 
                        help="Appends data from a csv, tab, or fits formatted file \
                        into a DB table using the filename as the table name")
    parser.add_argument("-s", "--db",dest='db', #choices=[...]?
                        help="Override database name [dessci,desoper,destest]")
    parser.add_argument("-q", "--quiet", action="store_true", dest='quiet', 
                        help="Silence initialization, no loading bar")
    parser.add_argument("-u", "--user", dest='user', help="username")
    parser.add_argument("-p", "--password", dest='password', help="password")
    args = parser.parse_args()

    if args.version:
        print('version: %s' % __version__)
        os._exit(0)

    if args.quiet:
        conf.set('display', 'loading_bar', 'no')

    if args.db is not None:
        db = args.db
        if db[:3] == 'db-': db = db[3:]
    else:
        db = conf.get('easyaccess', 'database')

    desconf = config_mod.get_desconfig(desfile, db)

    if args.user is not None:
        print('Bypassing .desservices file with user : %s' % args.user)
        if args.password is None:
            print('Must include password')
            os._exit(0)
        else:
            desconf.set('db-' + db, 'user', args.user)
            desconf.set('db-' + db, 'passwd', args.password)

    if args.command is not None:
        initial_message(args.quiet, clear=False)
        cmdinterp = easy_or(conf, desconf, db, interactive=False, quiet=args.quiet)
        cmdinterp.onecmd(args.command)
        os._exit(0)
    elif args.loadsql is not None:
        initial_message(args.quiet, clear=False)
        cmdinterp = easy_or(conf, desconf, db, interactive=False, quiet=args.quiet)
        linein = "loadsql " + args.loadsql
        cmdinterp.onecmd(linein)
        os._exit(0)
    elif args.loadtable is not None:
        initial_message(args.quiet, clear=False)
        cmdinterp = easy_or(conf, desconf, db, interactive=False, quiet=args.quiet)
        linein = "load_table " + args.loadtable
        cmdinterp.onecmd(linein)
        os._exit(0)
    elif args.appendtable is not None:
        initial_message(args.quiet, clear=False)
        cmdinterp = easy_or(conf, desconf, db, interactive=False, quiet=args.quiet)
        linein = "append_table " + args.appendtable
        cmdinterp.onecmd(linein)
        os._exit(0)
    else:
        initial_message(args.quiet, clear=True)
        easy_or(conf, desconf, db, quiet=args.quiet).cmdloop()
