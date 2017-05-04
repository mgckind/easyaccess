#!/usr/bin/env python
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import cmd
import os
import sys
import cx_Oracle
import shutil
import stat
import re
import getpass
from multiprocessing import Process
from easyaccess.version import __version__
from easyaccess.version import last_pip_version
import easyaccess.config_ea as config_mod
from easyaccess.eautils import des_logo as dl
import easyaccess.eautils.dtypes as eatypes
import easyaccess.eautils.fileio as eafile
import easyaccess.eautils.fun_utils as fun_utils
import easyaccess.eaparser as eaparser
from easyaccess.eautils.import_utils import Import
from easyaccess.eautils.ea_utils import *
import threading
import time
import pandas as pd
import webbrowser
import signal
import warnings
warnings.filterwarnings("ignore")
# For compatibility with old python
try:
    from builtins import input, str, range
except ImportError:
    from __builtin__ import input, str, range

__author__ = 'Matias Carrasco Kind'


def without_color(line, color, mode=0):
    return line


try:
    from termcolor import colored as with_color

    def colored(line, color, mode=0):
        if mode == 1:
            return with_color(line, color)
        else:
            return line
except ImportError:
    colored = without_color
try:
    import readline
    readline_present = True
    try:
        import gnureadline as readline
    except ImportError:
        pass
except ImportError:
    readline_present = False

positive = ['yes', 'y', 'true', 't', 'on']
negative = ['no', 'n', 'false', 'f', 'off']
input_options = ', '.join([i[0]+'/'+i[1] for i in zip(positive, negative)])

sys.path.insert(0, os.getcwd())
# For python functions to work
fun_utils.init_func()
pid = os.getpid()

# FILES
ea_path = os.path.join(os.environ["HOME"], ".easyaccess/")
if not os.path.exists(ea_path):
    os.makedirs(ea_path)
history_file = os.path.join(os.environ["HOME"], ".easyaccess/history")
config_file = os.path.join(os.environ["HOME"], ".easyaccess/config.ini")


# check if old path is there
ea_path_old = os.path.join(os.environ["HOME"], ".easyacess/")
if os.path.exists(ea_path_old) and os.path.isdir(ea_path_old):
    if not os.path.exists(history_file):
        shutil.copy2(os.path.join(
            os.environ["HOME"], ".easyacess/history"), history_file)
    if not os.path.exists(config_file):
        shutil.copy2(os.path.join(
            os.environ["HOME"], ".easyacess/config.ini"), config_file)

# create history and config files if they don't exist
if not os.path.exists(history_file):
    os.system('echo $null >> ' + history_file)
if not os.path.exists(config_file):
    os.system('echo $null >> ' + config_file)

# DES SERVICES FILE
desfile = os.getenv("DES_SERVICES")
if not desfile:
    desfile = os.path.join(os.getenv("HOME"), ".desservices.ini")
if os.path.exists(desfile):
    amode = stat.S_IMODE(os.stat(desfile).st_mode)
    if amode != 2 ** 8 + 2 ** 7:
        print('Changing permissions to des_service file to read/write by user')
        os.chmod(desfile, 2 ** 8 + 2 ** 7)  # rw by user owner only


class easy_or(cmd.Cmd, Import, object):
    """Easy cx_Oracle interpreter for DESDM."""

    def set_messages(self):
        db_user = self.desconfig.get('db-' + self.dbname, 'user')
        intro_keys = {
            'db': colored(self.dbname, "green", self.ct),
            'user': colored(db_user, "green", self.ct),
            'ea_version': colored("easyaccess " + __version__, "cyan", self.ct)
            }
        self.intro = colored(

            """
{ea_version}. The DESDM Database shell.
Connected as {user} to {db}.
** Type 'help' or '?' to list commands. **
            """.format(**intro_keys), "cyan", self.ct)
        self.savePrompt = colored(
            '_________', 'magenta', self.ct) + '\nDESDB ~> '
        self.prompt = self.savePrompt
        self.doc_header = colored(
            ' *General Commands*', "cyan", self.ct) + ' (type help <command>):'
        self.docdb_header = colored(
            '\n *DB Commands*', "cyan", self.ct) + '      (type help <command>):'

    def __init__(self, conf, desconf, db, interactive=True,
                 quiet=False, refresh=True):
        cmd.Cmd.__init__(self)
        self.config = conf
        self.ct = int(self.config.getboolean('display', 'color_terminal'))
        self.writeconfig = False
        self.quiet = quiet
        self.refresh = refresh
        self.desconfig = desconf
        # ADW: It would be better to set these automatically...
        self.editor = os.getenv('EDITOR', self.config.get('easyaccess', 'editor'))
        self.timeout = self.config.getint('easyaccess', 'timeout')
        self.prefetch = self.config.getint('easyaccess', 'prefetch')
        self.loading_bar = self.config.getboolean('display', 'loading_bar')
        self.nullvalue = self.config.getint('easyaccess', 'nullvalue')
        self.outfile_max_mb = self.config.getint('easyaccess', 'outfile_max_mb')
        self.autocommit = self.config.getboolean('easyaccess', 'autocommit')
        self.desdm_coldefs = self.config.getboolean('easyaccess', 'desdm_coldefs')
        self.trim_whitespace = self.config.getboolean('easyaccess', 'trim_whitespace')
        self.dbname = db
        self.buff = None
        self.interactive = interactive
        self.undoc_header = None
        self.metadata = True
        # connect to db
        self.user = self.desconfig.get('db-' + self.dbname, 'user')
        self.dbhost = self.desconfig.get('db-' + self.dbname, 'server')
        self.service_name = self.desconfig.get('db-' + self.dbname, 'name')
        self.port = self.desconfig.get('db-' + self.dbname, 'port')
        self.password = self.desconfig.get('db-' + self.dbname, 'passwd')
        kwargs = {'host': self.dbhost, 'port': self.port, 'service_name': self.service_name}
        self.dsn = cx_Oracle.makedsn(**kwargs)
        if not self.quiet:
            print('Connecting to DB ** %s ** ...' % self.dbname)
        connected = False
        for tries in range(3):
            try:
                self.con = cx_Oracle.connect(
                    self.user, self.password, dsn=self.dsn)
                if self.autocommit:
                    self.con.autocommit = True
                connected = True
                break
            except Exception as e:
                lasterr = str(e).strip()
                print(colored("Error when trying to connect to database: %s" %
                              lasterr, "red", self.ct))
                print("\n   Retrying...\n")
                time.sleep(5)
        if not connected:
            print('\n ** Could not successfully connect to DB. Try again later. Aborting. ** \n')
            os._exit(0)
        self.cur = self.con.cursor()
        self.cur.arraysize = int(self.prefetch)
        msg = self.last_pass_changed()
        if msg and not self.quiet:
            print(msg)
        self.set_messages()

    def handler(self, signum, frame):
        """
        Executed with ^Z (Ctrl+Z) is pressed.
        """
        print('Ctrl+Z pressed')
        print('Job = %d Stopped' % pid)
        print(colored('* Type bg to send this job to the background *', 'cyan', self.ct))
        print(colored('* Type fg to take this job to the foreground *', 'cyan', self.ct))
        print()
        os.kill(pid, signal.SIGSTOP)
        try:
            if self.loading_bar:
                if self.pload.pid is not None:
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
                try:
                    import gnureadline as readline
                except:
                    pass
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
                    dl.print_deslogo(self.ct)
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
                    try:
                        import gnureadline as readline
                    except:
                        pass
                    readline.set_completer(self.old_completer)
                except ImportError:
                    pass

    def do_help(self, arg):
        """
        List available commands with "help" or detailed help with "help cmd".
        """
        if arg:
            # TODO check arg syntax
            try:
                func = getattr(self, 'help_' + arg)
            except AttributeError:
                try:
                    doc = getattr(self, 'do_' + arg).__doc__
                    if doc:
                        doc = str(doc)
                        if doc.find('DB:') > -1:
                            doc = doc.replace('DB:', '')
                        self.stdout.write("%s\n" % str(doc))
                        return
                except AttributeError:
                    pass
                self.stdout.write("%s\n" % str(self.nohelp % (arg,)))
                return
            func()
        else:
            self.do_clear(True)
            dl.print_deslogo(self.ct)
            self.stdout.write(str(self.intro) + "\n")
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
            self.print_topics(self.doc_header, cmds_doc, 80)
            self.print_topics(self.docdb_header, cmds_db, 80)
            self.print_topics(self.misc_header, list(help.keys()), 80)
            self.print_topics(self.undoc_header, cmds_undoc, 80)

            print(colored(' *Default Input*', 'cyan', self.ct))
            print(self.ruler * 80)
            print("* To run SQL queries just add ; at the end of query")
            print("* To write to a file  : select ... from ... "
                  "where ... ; > filename")
            print(colored(
                "* Supported file formats (.csv, .tab., .fits, .h5) ",
                "green", self.ct))
            print("* To check SQL syntax : select ... from ... "
                  "where ... ; < check")
            print(
                "* To see the Oracle execution plan  : select ... "
                "from ... where ... ; < explain")
            print()
            print("* To access an online tutorial type: online_tutorial ")

    def print_topics(self, header, cmds, maxcol):
        if header is not None:
            if cmds:
                self.stdout.write("%s\n" % str(header))
                if self.ruler:
                    self.stdout.write("%s\n" % str(self.ruler * maxcol))
                self.columnize(cmds, maxcol - 1)
                self.stdout.write("\n")

    def preloop(self):
        """
        Initialization before prompting user for commands.
        Despite the claims in the Cmd documentation, Cmd.preloop() is not a stub.
        """
        self.cache_table_names = []
        self.cache_usernames = []
        self.cache_column_names = []
        self.metadata = False
        cmd.Cmd.preloop(self)  # # sets up command completion
        if self.refresh:
            try:
                self.do_refresh_metadata_cache('')
                print()
            except:
                print(colored(
                    "\n Couldn't load metadata into cache (try later), no"
                    " autocompletion for tables, columns or users this time\n",
                    "red", self.ct))

        # history
        ht = open(history_file, 'r')
        Allq = ht.readlines()
        ht.close()
        self._hist = []
        for lines in Allq:
            self._hist.append(lines.strip())
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
            self.con = cx_Oracle.connect(
                self.user, self.password, dsn=self.dsn)
            if self.autocommit:
                self.con.autocommit = True
            self.cur = self.con.cursor()
            self.cur.arraysize = int(self.prefetch)

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

        if not line:
            return ""  # empty line no need to go further
        if line[0] == "@":
            if len(line) > 1:
                fbuf = line[1:]
                fbuf = fbuf.replace(';', '')
                if fbuf.find('>') > -1:
                    try:
                        fbuf = "".join(fbuf.split())
                        line = read_buf(fbuf.split('>')[0])
                        if line == "":
                            return ""
                        if line.find(';') > -1:
                            line = line.split(';')[0]
                        outputfile = fbuf.split('>')[1]
                        line = line + '; > ' + outputfile
                    except:
                        outputfile = ''

                else:
                    line = read_buf(fbuf.split()[0])
                    if line == "":
                        return ""

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

    # This function overrides the cmd function to remove some characters from
    # the delimiter
    def complete(self, text, state):
        """Return the next possible completion for 'text'.

        If a command has not been entered, then complete against command list.
        Otherwise try to call complete_<command> to get list of completions.
        """
        if state == 0:
            import readline
            try:
                import gnureadline as readline
            except:
                pass

            # #overrides default delimiters
            readline.set_completer_delims(
                ' \t\n`~!@#$%^&*()=[{]}\\|;:\'",<>/?')
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
        """
        Default function called for line execution.

        Parameters:
        -----------
        line : The input line.

        Returns:
        --------
        None
        """
        fend = line.find(';')
        if fend > -1:
            # with open('easy.buf', 'w') as filebuf:
            # filebuf.write(self.buff)
            query = line[:fend]
            # PARSE QUERY HERE
            try:
                query, funs, args, names = fun_utils.parseQ(query)
            except:
                print_exception(mode=self.ct)
                return
            extra_func = [funs, args, names]
            if funs is None:
                extra_func = None
            if line[fend:].find('<') > -1:
                app = line[fend:].split('<')[1].strip().split()[0].lower()
                if app.find('check') > -1:
                    print('\nChecking statement...')
                    try:
                        self.cur.parse(query.encode())
                        print(colored('Ok!\n', 'green', self.ct))
                        return
                    except:
                        print_exception(mode=self.ct)
                        return
                elif app.find('submit') > -1:
                    print(colored(
                        '\nTo be done: Submit jobs to the DB cluster',
                        'cyan', self.ct))
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
                    print('\nFetching data and saving it to %s ...' %
                          fileout + '\n')
                    eafile.check_filetype(fileout)
                    self.query_and_save(query, fileout, extra_func=extra_func)
                except KeyboardInterrupt or EOFError:
                    print(colored('\n\nAborted \n', "red", self.ct))
                except IndexError:
                    print(colored('\nMust indicate output file\n', "red", self.ct))
                    print('Format:\n')
                    print('DESDB ~> select ... from ... where ... ; > example.csv \n')
                except:
                    print_exception(mode=self.ct)
            else:
                try:
                    self.query_and_print(query, extra_func=extra_func)
                except:
                    try:
                        self.con.cancel()
                    except:
                        pass
                    print(colored('\n\nAborted \n', "red", self.ct))

        else:
            print()
            print("Invalid command or missing ';' at the end of query.")
            print("Type 'help' or '?' to list commands")
            print()

    def completedefault(self, text, line, begidx, lastidx):
        qstop = line.find(';')
        if qstop > -1:
            if line[qstop:].find('>') > -1:
                line = line[qstop + 1:]
                return complete_path(line)
            if line[qstop:].find('<') > -1:
                if text:
                    return [option for option in options_app if option.startswith(text.lower())]
                else:
                    return options_app

        if line[0] == '@':
            line = '@ ' + line[1:]
            return complete_path(line)
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
        select sysdate-ctime creation,
        sysdate-ptime passwd from sys.user$ where name = '%s' """ % self.user.upper()
        try:
            self.cur.execute(query)
            TT = self.cur.fetchall()
            ctime = int(TT[0][0])
            ptime = int(TT[0][1])
        except:
            return None
        if ptime > 200:
            msg = colored("*Important* ", "red") + 'Last time password change was '
            msg += colored("%d" % ptime, "red", self.ct) + " days ago"
            if ptime == ctime:
                msg += colored(" (Never in your case!)", "red", self.ct)
            msg += '\n Please change it using the '
            msg += colored("set_password", "cyan", self.ct)
            msg += ' command to get rid of this message\n'
            return msg

    def query_and_print(self, query, print_time=True,
                        err_arg='No rows selected', suc_arg='Done!', extra="",
                        clear=False, extra_func=None, return_df=False):
        # to be safe
        query = query.replace(';', '')
        if extra_func is not None:
            p_functions = extra_func[0]
            p_args = extra_func[1]
            p_names = extra_func[2]
        self.cur.arraysize = int(self.prefetch)
        tt = threading.Timer(self.timeout, self.con.cancel)
        tt.start()
        t1 = time.time()
        if self.loading_bar:
            self.pload = Process(target=loading)
        if self.loading_bar:
            self.pload.start()
        try:
            self.cur.execute(query)
            if self.cur.description is not None:
                header = [columns[0] for columns in self.cur.description]
                htypes = [columns[1] for columns in self.cur.description]
                info = [rec[1:6] for rec in self.cur.description]
                # data = pd.DataFrame(self.cur.fetchall())
                updated = False
                data = pd.DataFrame(self.cur.fetchmany())
                while True:
                    if data.empty:
                        break
                    if extra_func is not None and not updated:
                        data.columns = header
                        for kf in range(len(p_functions)):
                            data = fun_utils.updateDF(
                                data, p_functions, p_args, p_names, kf)
                        updated = True
                    rowline = ' Rows : %d, Rows/sec: %d ' % (
                        self.cur.rowcount, self.cur.rowcount * 1. / (time.time() - t1))
                    if self.loading_bar:
                        sys.stdout.write(colored(rowline, 'yellow', self.ct))
                    if self.loading_bar:
                        sys.stdout.flush()
                    if self.loading_bar:
                        sys.stdout.write('\b' * len(rowline))
                    if self.loading_bar:
                        sys.stdout.flush()
                    temp = pd.DataFrame(self.cur.fetchmany())
                    if not temp.empty:
                        if extra_func is not None:
                            temp.columns = header
                            for kf in range(len(p_functions)):
                                temp = fun_utils.updateDF(
                                    temp, p_functions, p_args, p_names, kf)
                        data = data.append(temp, ignore_index=True)
                    else:
                        break
                t2 = time.time()
                tt.cancel()
                if self.loading_bar:
                    # self.pload.terminate()
                    if self.pload.pid is not None:
                        os.kill(self.pload.pid, signal.SIGKILL)
                if clear:
                    self.do_clear(None)
                print()
                if print_time:
                    print(colored('\n%d rows in %.2f seconds' %
                                  (len(data), (t2 - t1)), "green", self.ct))
                if print_time:
                    print()
                if len(data) == 0:
                    fline = '   '
                    for col in header:
                        fline += '%s  ' % col
                    if extra != "":
                        print(colored(extra + '\n', "cyan", self.ct))
                    print(fline)
                    print(colored(err_arg, "red", self.ct))
                else:
                    if extra_func is None:
                        data.columns = header
                    data.index += 1
                    if extra != "":
                        print(colored(extra + '\n', "cyan", self.ct))
                    # ADW: Oracle distinguishes between NaN and Null while
                    # pandas does not making this replacement confusing...
                    # try:
                    #     data.fillna('Null', inplace=True)
                    # except:
                    # pass
                    if return_df:
                        return data
                    if 'COMMENTS' in data.columns:
                        try:
                            width = data['COMMENTS'].str.len().max()
                            if pd.isnull(width):
                                width = 4
                            format_f = lambda s: '{: <{width}}'.format(s, width=width)
                            temp_col = format_f('COMMENTS')
                            data.rename(columns={'COMMENTS': temp_col}, inplace=True)
                            print(data.to_string(formatters={temp_col: format_f}))
                        except:
                            pass
                    else:
                        print(data)
            else:
                t2 = time.time()
                tt.cancel()
                if self.loading_bar:
                    # self.pload.terminate()
                    if self.pload.pid is not None:
                        os.kill(self.pload.pid, signal.SIGKILL)
                if clear:
                    self.do_clear(None)
                print(colored(suc_arg, "green", self.ct))
                if self.autocommit:
                    self.con.commit()
            print()
        except:
            (type, value, traceback) = sys.exc_info()
            self.con.cancel()
            t2 = time.time()
            if self.loading_bar:
                # self.pload.terminate()
                if self.pload.pid is not None:
                    os.kill(self.pload.pid, signal.SIGKILL)
            print()
            print(colored(type, "red", self.ct))
            print(colored(value, "red", self.ct))
            print()
            if t2 - t1 > self.timeout:
                print(colored('Query is taking too long on the interpreter', "red", self.ct))
                mt = 'Try to output the results to a file\nor increase timeout (now is {} s) using:'
                msg = colored(mt.format(self.timeout), 'green', self.ct)
                print(msg)
                print("\'config timeout set XXXXX\'")

    def query_and_save(self, query, fileout, print_time=True, extra_func=None):
        """
        Execute a query and save the results to a file.
        Supported formats are: '.csv', '.tab', '.h5', and '.fits'
        """
        # to be safe
        query = query.replace(';', '')
        eafile.check_filetype(fileout)
        if extra_func is not None:
            p_functions = extra_func[0]
            p_args = extra_func[1]
            p_names = extra_func[2]
        self.cur.arraysize = int(self.prefetch)
        t1 = time.time()
        if self.loading_bar:
            self.pload = Process(target=loading)
        if self.loading_bar:
            self.pload.start()
        # if True:
        try:
            self.cur.execute(query)
            if self.cur.description is not None:
                header = [columns[0] for columns in self.cur.description]
                htypes = [columns[1] for columns in self.cur.description]
                info = [rec[0:6] for rec in self.cur.description]
                names_info = [jj[0] for jj in info]
                first = True
                mode_write = 'w'
                header_out = True
                com_it = 0
                while True:
                    data = pd.DataFrame(self.cur.fetchmany())
                    rowline = ' Rows : %d, Rows/sec: %d ' % (
                        self.cur.rowcount, self.cur.rowcount * 1. / (time.time() - t1))
                    if self.loading_bar:
                        sys.stdout.write(colored(rowline, 'yellow', self.ct))
                    if self.loading_bar:
                        sys.stdout.flush()
                    if self.loading_bar:
                        sys.stdout.write('\b' * len(rowline))
                    if self.loading_bar:
                        sys.stdout.flush()
                    com_it += 1
                    # 1-indexed for backwards compatibility
                    if first:
                        fileindex = 1
                    info2 = info
                    if not data.empty:
                        data.columns = header
                        data.fillna(self.nullvalue, inplace=True)
                        for i, col in enumerate(data):
                            nt = eatypes.oracle2numpy(info[i])
                            if nt != "":
                                data[col] = data[col].astype(nt)
                        if extra_func is not None:
                            for kf in range(len(p_functions)):
                                data = fun_utils.updateDF(
                                    data, p_functions, p_args, p_names, kf)
                            # UPDATE INFO before write_file
                                info2 = []
                                for cc in data.columns:
                                    if cc in names_info:
                                        info2.append(
                                            info[names_info.index(cc)])
                                    else:
                                        info2.append(
                                            tuple([cc, 'updated', 0, 0, 0, 0]))

                        fileindex = eafile.write_file(fileout, data, info2, fileindex,
                                                      mode_write,
                                                      max_mb=self.outfile_max_mb, query=query)

                        if first:
                            mode_write = 'a'
                            header_out = False
                            first = False
                    else:
                        break
                t2 = time.time()
                if self.loading_bar:
                    # self.pload.terminate()
                    if self.pload.pid is not None:
                        os.kill(self.pload.pid, signal.SIGKILL)
                elapsed = '%.1f seconds' % (t2 - t1)
                print()
                if print_time:
                    print(colored('\n Written %d rows to %s in %.2f seconds and %d trips' % (
                        self.cur.rowcount, fileout, (t2 - t1), com_it - 1), "green", self.ct))
                if print_time:
                    print()
            else:
                pass
            print()
        except:
            (type, value, traceback) = sys.exc_info()
            if self.loading_bar:
                # self.pload.terminate()
                if self.pload.pid is not None:
                    os.kill(self.pload.pid, signal.SIGKILL)
            print()
            print(colored(type, "red", self.ct))
            print(colored(value, "red", self.ct))
            print()

    def query_results(self, query):
        self.cur.execute(query)
        data = self.cur.fetchall()
        return data

    def get_tables_names(self):

        if self.dbname in ('dessci', 'desoper', 'destest', 'newsci'):
            query = """
            select table_name from DES_ADMIN.CACHE_TABLES
            union select table_name from user_tables
            """
        temp = self.cur.execute(query)
        tnames = pd.DataFrame(temp.fetchall())
        table_list = tnames.values.flatten().tolist()
        return table_list

    def get_tables_names_user(self, user):
        if user == "":
            return self.do_help('tables_names_user')
        user = user.replace(";", "")
        query = """
            select distinct table_name from all_tables
             where owner=\'%s\' order by table_name""" % user.upper()
        temp = self.cur.execute(query)
        tnames = pd.DataFrame(temp.fetchall())
        self.do_clear(None)
        if len(tnames) > 0:
            print(colored('\nPublic tables from %s' %
                          user.upper(), "cyan", self.ct))
            print(tnames)
        else:
            if self.dbname in ('dessci', 'desoper'):
                query = """
                    select count(username) as cc  from des_users
                     where upper(username) = upper('%s')""" % user
            if self.dbname in ('destest'):
                query = """
                    select count(username) as cc from dba_users
                     where upper(username) = upper('%s')""" % user
            temp = self.cur.execute(query)
            tnames = temp.fetchall()
            if tnames[0][0] == 0:
                print(colored('User %s does not exist in DB' %
                              user.upper(), 'red', self.ct))
            else:
                print(colored('User %s has no tables' %
                              user.upper(), 'cyan', self.ct))

    def get_userlist(self):
        if self.dbname in ('dessci', 'desoper'):
            query = 'select distinct username from des_users order by username'
        if self.dbname in ('destest', 'newsci'):
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
        query = """SELECT column_name from DES_ADMIN.CACHE_COLUMNS"""
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
            self.prefetch = 30000
            self.config.set('easyaccess', 'prefetch', '30000')
            self.writeconfig = True
            print('\nPrefetch value set to default (30000) \n')
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
            if arg.strip():
                firstprint = max(nall - int(arg), 0)
            for index in range(firstprint, nall):
                print(index, readline.get_history_item(index))
            # if arg.strip():
            #    self.do_clear(None)
            #    line = readline.get_history_item(int(arg))
            #    line = self.precmd(line)
            #    self.onecmd(line)

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
            return complete_path(line)

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
                if newquery == "":
                    return
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
        There is a shortcut using @, ex : @test.sql  (or @test.sql > myfile.csv
        to override output file)

        Usage: loadsql <filename with sql statement>   (use autocompletion)

        Optional: loadsql <filename with sql statement> > <output_file> to
        write to a file, not to the screen
        """
        line = line.replace(';', '')
        if line.find('>') > -1:
            try:
                line = "".join(line.split())
                newq = read_buf(line.split('>')[0])
                if newq.find(';') > -1:
                    newq = newq.split(';')[0]
                outputfile = line.split('>')[1]
                newq = newq + '; > ' + outputfile
            except:
                outputfile = ''

        else:
            newq = read_buf(line)

        if newq == "":
            return
        if self.interactive:
            print()
            print(newq)
            print()
            if (input('submit query? (Y/N): ') in ['Y', 'y', 'yes']):
                self.default(newq)
        else:
            self.default(newq)

    def complete_loadsql(self, text, line, start_idx, end_idx):
        return complete_path(line)

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
            if self.autocommit:
                self.con.commit()
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
        if line is None:
            return
        try:
            tmp = os.system('clear')
        except:
            try:
                tmp = os.system('cls')
            except:
                pass

    def do_config(self, line):
        """
        Change parameters from config file (config.ini). Smart autocompletion enabled

        Usage:
            - config <parameter> show : Shows current value of parameter
            - config <parameter> set <value> : Sets parameter to given value
            - config all show: Shows all parameters and their values
            - config filepath: Prints the path to the config file

        Parameters:
            database          : Default DB to connect to
            editor            : Editor for editing sql queries, see --> help edit
            prefetch          : Number of rows prefetched by Oracle, see --> help prefetch
            histcache         : Length of the history of commands
            timeout           : Timeout for a query to be printed on the screen.
                                Doesn't apply to output files
            nullvalue         : value to replace Null entries when writing a file (default = -9999)
            outfile_max_mb    : Max size of each fits file in MB
            autocommit        : yes/no toggles the autocommit for DB changes (default is yes)
            trim_whitespace   : Trim whitespace from strings when uploading data to the DB
                                (default yes)
            desdm_coldefs     : Use DESDM DB compatible data types when uploading data (default yes)

            max_rows          : Max number of rows to display on the screen.
                                Doesn't apply to output files
            width             : Width of the output format on the screen
            max_columns       : Max number of columns to display on the screen.
                                Doesn't apply to output files
            max_colwidth      : Max number of characters per column at display.
                                Doesn't apply to output files
            color_terminal    : yes/no toggles the color for terminal std output
            loading_bar       : yes/no toggles the loading bar. Useful for background jobs

        """
        global load_bar
        if line == '':
            return self.do_help('config')
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
                        print('\nCurrent value for %s = %s ' %
                              (key, self.config.get(section, key)))
                        break
            print()
        elif oneline.find('filepath') > -1:
            print('\n config file path = %s\n' % config_file)
        elif oneline.find('set') > -1:
            if oneline.find('all') > -1:
                return self.do_help('config')
            key = oneline.split('set')[0]
            val = oneline.split('set')[1]
            if val == '':
                return self.do_help('config')
            for section in (self.config.sections()):
                if self.config.has_option(section, key):
                    if key in ['loading_bar', 'color_terminal', 'autocommit', 'trim_whitespace',
                               'desdm_coldefs']:
                        val = val.lower()
                        temp = True if val in positive else False if val in negative else 'error'
                        if temp == 'error':
                            print(colored('\nInvalid value, options '
                                          'are: {}\n'.format(input_options), "red", self.ct))
                            return
                        else:
                            val = 'yes' if temp else 'no'
                            if key == 'loading_bar':
                                load_bar = temp
                            if key == 'color_terminal':
                                self.ct = temp
                                self.set_messages()
                    self.config.set(section, key, str(val))
                    self.writeconfig = True
                    break
            self.config.set('display', 'loading_bar', 'yes' if load_bar else 'no')
            config_mod.write_config(config_file, self.config)
            if key == 'max_columns':
                pd.set_option('display.max_columns', self.config.getint('display', 'max_columns'))
            if key == 'max_rows':
                pd.set_option('display.max_rows', self.config.getint('display', 'max_rows'))
            if key == 'width':
                pd.set_option('display.width', self.config.getint('display', 'width'))
            if key == 'max_colwidth':
                pd.set_option('display.max_colwidth', self.config.getint('display', 'max_colwidth'))
            if key == 'editor':
                self.editor = self.config.get('easyaccess', 'editor')
            if key == 'timeout':
                self.timeout = self.config.getint('easyaccess', 'timeout')
            if key == 'prefetch':
                self.prefetch = self.config.get('easyaccess', 'prefetch')
            if key == 'loading_bar':
                self.loading_bar = self.config.getboolean('display', 'loading_bar')
            if key == 'nullvalue':
                self.nullvalue = self.config.getint('easyaccess', 'nullvalue')
            if key == 'outfile_max_mb':
                self.outfile_max_mb = self.config.getint('easyaccess', 'outfile_max_mb')
            if key == 'autocommit':
                self.autocommit = self.config.getboolean('easyaccess', 'autocommit')
            if key == 'trim_whitespace':
                self.trim_whitespace = self.config.getboolean('easyaccess', 'trim_whitespace')
            if key == 'desdm_coldefs':
                self.desdm_coldefs = self.config.getboolean('easyaccess', 'desdm_coldefs')

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

    def do_execproc(self, line):
        """
        DB:Execute procedures in the DB, arguments can be floating numbers or strings

        Usage:
            execproc PROCEDURE('arg1', 'arg2', 10, 20, 'arg5', etc...)

        To see list of positional arguments and their data types use:
            execproc PROCEDURE() describe
        """
        if line == '':
            return self.do_help('execproc')
        line = line.replace(';', '')
        line = "".join(line.split())
        proc_name = line[0:line.index('(')]
        argument_list = line[line.index('(') + 1:line.index(')')].split(',')
        arguments = []
        for arg in argument_list:
            if arg.startswith(("\"", "\'")):
                arg = arg[1:-1]
            else:
                try:
                    arg = float(arg)
                except ValueError:
                    pass
            arguments.append(arg)
        if line[line.index(')'):].find('describe') > -1:
            comm = """\n Description of procedure '%s' """ % proc_name
            query = """
                select argument_name, data_type,position,in_out from all_arguments where
                 object_name = '%s' order by position""" % proc_name.upper()
            self.query_and_print(query, print_time=False, clear=True,
                                 extra=comm, err_arg="Procedure does not exist")
            return
        try:
            outproc = self.cur.callproc(proc_name, arguments)
            print(colored('Done!', "green", self.ct))
        except:
            print_exception(mode=self.ct)

    def do_set_password(self, arg):
        """
        DB:Set a new password on this and all other DES instances (DESSCI, DESOPER)

        Usage: set_password
        """
        print()
        pw1 = getpass.getpass(prompt='Enter new password:')
        if re.search('\W', pw1):
            print(colored("\nPassword contains whitespace, not set\n", "red", self.ct))
            return
        if not pw1:
            print(colored("\nPassword cannot be blank\n", "red", self.ct))
            return
        pw2 = getpass.getpass(prompt='Re-Enter new password:')
        print()
        if pw1 != pw2:
            print(colored("Passwords don't match, not set\n", "red", self.ct))
            return

        query = """alter user %s identified by "%s"  """ % (self.user, pw1)
        confirm = 'Password changed in %s' % self.dbname.upper()
        self.query_and_print(query, print_time=False, suc_arg=confirm)
        self.desconfig.set('db-'+self.dbname, 'passwd', pw1)
        config_mod.write_desconfig(desfile, self.desconfig)
        if self.dbname not in ('dessci', 'desoper'):
            return
        dbases = ['DESSCI', 'DESOPER']
        for db in dbases:
            kwargs = {'host': self.dbhost, 'port': self.port, 'service_name': self.service_name}
            dsn = cx_Oracle.makedsn(**kwargs)
            temp_con = cx_Oracle.connect(self.user, self.password, dsn=dsn)
            temp_cur = temp_con.cursor()
            try:
                temp_cur.execute(query)
                confirm = 'Password changed in %s\n' % db.upper()
                print(colored(confirm, "green", self.ct))
                temp_con.commit()
                temp_cur.close()
                temp_con.close()
                self.desconfig.set('db-dessci', 'passwd', pw1)
                self.desconfig.set('db-desoper', 'passwd', pw1)
                config_mod.write_desconfig(desfile, self.desconfig)
            except:
                confirm = 'Password could not be changed in %s\n' % db.upper()
                print(colored(confirm, "red", self.ct))
                print(sys.exc_info())

    def do_refresh_metadata_cache(self, arg):
        """
        DB:Refreshes meta data cache for auto-completion of table
        names and column names .
        """
        verb = True
        try:
            if verb:
                print('Loading metadata into cache...')
            self.cache_table_names = self.get_tables_names()
            self.cache_usernames = self.get_userlist()
            self.cache_column_names = self.get_columnlist()
        except:
            if verb:
                print(
                    colored("There was an error when refreshing the metadata", "red", self.ct))
        try:
            self.cur.execute('create table FGOTTENMETADATA (ID int)')
        except:
            pass

    def do_show_db(self, arg):
        """
        DB:Shows database connection information
        """
        lines = "user: %s\ndb  : %s\nhost: %s\n" % (
            self.user.upper(), self.dbname.upper(), self.dbhost.upper())
        lines = lines + "\nPersonal links:"
        query = """
        SELECT owner, db_link, username, host, created
        FROM all_db_links where OWNER = '%s'
        """ % (self.user.upper())
        self.query_and_print(query, print_time=False, extra=lines, clear=True)

    def do_change_db(self, line):
        """
        DB: Change to another database, namely dessci, desoper or destest

         Usage:
            change_db DB     # Changes to DB, it does not refresh metadata, e.g.: change_db desoper

        """
        if line == '':
            return self.do_help('change_db')
        line = " ".join(line.split())
        key_db = line.split()[0]
        if key_db in ('dessci', 'desoper', 'destest', 'newsci'):
            if key_db == self.dbname:
                print(colored("Already connected to : %s" % key_db, "green", self.ct))
                return
            self.dbname = key_db
            # connect to db
            try:
                self.user = self.desconfig.get('db-' + self.dbname, 'user')
                self.password = self.desconfig.get('db-' + self.dbname, 'passwd')
                self.dbhost = self.desconfig.get('db-' + self.dbname, 'server')
                self.service_name = self.desconfig.get('db-' + self.dbname, 'name')
            except:
                print(colored("DB {} does not exist in your desservices file".format(
                    key_db), "red", self.ct))
                return
            kwargs = {'host': self.dbhost, 'port': self.port, 'service_name': self.service_name}
            self.dsn = cx_Oracle.makedsn(**kwargs)
            if not self.quiet:
                print('Connecting to DB ** %s ** ...' % self.dbname)
            self.con.close()
            connected = False
            for tries in range(3):
                try:
                    self.con = cx_Oracle.connect(
                        self.user, self.password, dsn=self.dsn)
                    if self.autocommit:
                        self.con.autocommit = True
                    connected = True
                    break
                except Exception as e:
                    lasterr = str(e).strip()
                    print(colored(
                        "Error when trying to connect to database: %s" % lasterr, "red", self.ct))
                    print("\n   Retrying...\n")
                    time.sleep(5)
            if not connected:
                print(
                    '\n ** Could not successfully connect to DB. Try again later. Aborting. ** \n')
                os._exit(0)
            self.cur = self.con.cursor()
            self.cur.arraysize = int(self.prefetch)
            print()
            print("Run refresh_metadata_cache to reload the auto-completion metatada")
            self.set_messages()
            return
        else:
            print(colored("DB {} does not exist or you don't have access to it".format(
                key_db), "red", self.ct))
            return

    def complete_change_db(self, text, line, start_index, end_index):
        options_db = ['desoper', 'dessci', 'destest', 'newsci']
        if text:
            return [option for option in options_db if option.startswith(text.lower())]
        else:
            return options_db

    def do_whoami(self, arg):
        """
        DB:Print information about the user's details.

        Usage: whoami
        """
        # It might be useful to print user roles as well
        # select GRANTED_ROLE from USER_ROLE_PRIVS

        if self.dbname in ('dessci', 'desoper'):
            sql_getUserDetails = """
            select d.username, d.email, d.firstname as first, d.lastname as last,
             trunc(sysdate-t.ptime,0)||' days ago' last_passwd_change,
            trunc(sysdate-t.ctime,0)||' days ago' created
            from des_users d, sys.user$ t  where
             d.username = '""" + self.user + """' and t.name=upper(d.username)"""
        if self.dbname in ('destest'):
            print(
                colored('\nThis function is not implemented in destest\n', 'red', self.ct))
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

    def do_mytables(self, arg, return_df=False, extra="List of my tables"):
        """
        DB:Lists tables you have made in your user schema.

        Usage: mytables
        """
        # query = "SELECT table_name FROM user_tables"
        query = """
        SELECT t.table_name, s.bytes/1024/1024/1024 SIZE_GBYTES
        FROM user_segments s, user_tables t
        WHERE s.segment_name = t.table_name order by t.table_name
        """

        df = self.query_and_print(
            query, print_time=False, extra=extra, clear=True, return_df=return_df)
        if return_df:
            return df

    def do_find_user(self, line):
        """
        DB:Finds users given 1 criteria (either first name or last name)

        Usage:
            - find_user Doe     # Finds all users with Doe in their names
            - find_user John%   # Finds all users with John IN their names (John, Johnson, etc...)
            - find_user P%      # Finds all users with first, lastname or username starting with P

        """
        if line == '':
            return self.do_help('find_user')
        line = " ".join(line.split())
        keys = line.split()
        if self.dbname in ('dessci', 'desoper'):
            query = 'select * from des_users where '
        if self.dbname in ('destest'):
            query = 'select * from dba_users where '
        if len(keys) >= 1:
            query += 'upper(firstname) like upper(\'' + keys[0] + '\') or '
            query += 'upper(lastname) like upper(\'' + keys[0] + '\') or '
            query += 'upper(username) like upper (\'' + keys[0] + '\')'
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
        if arg == "":
            return self.do_help('user_tables')
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
        locate the fundamental definition of the table requested.
        """
        table = tablename
        schema = self.user.upper()  # default --- Mine
        link = ""  # default no link

        if "." in table:
            (schema, table) = table.split(".")
        if "@" in table:
            (table, link) = table.split("@")

        # Loop until we find a fundamental definition OR determine there is
        # no reachable fundamental definition, follow links and resolve
        # schema names. Rely on how the DES database is constructed we log
        # into our own schema, and rely on synonyms for a "simple" view of
        # common schema.

        while (1):
            # check for fundamental definition  e.g. schema.table@link
            q = """
            select count(*) from ALL_TAB_COLUMNS%s
            where OWNER = '%s' and TABLE_NAME = '%s'
            """ % ("@" + link if link else "", schema, table)
            ans = self.query_results(q)
            if ans[0][0] > 0:
                # found real definition return the tuple
                return (schema, table, link)

            # check if we are indirect by synonym of user
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

        msg = "No table found for: %s" % tablename
        raise Exception(msg)

    def do_describe_table(self, arg, clear=True, extra=None, return_df=False):
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
            schema, table, link = self.get_tablename_tuple(tablename)
            # schema, table and link are now valid.
            link = "@" + link if link else ""
            qcom = """
            select comments from all_tab_comments%s atc
            where atc.table_name = '%s'""" % (link, table)
            comment_table = self.query_results(qcom)[0][0]
        except:
            comment_table = ''
            print(colored("Table not found.", "red", self.ct))
            return

        try:
            qnum = """
            select to_char(num_rows) from all_tables where
            table_name='%s' and  owner = '%s' """ % (table, schema)
            numrows_table = self.query_results(qnum)[0][0]
            if numrows_table is None:
                numrows_table = 'Not available'
        except:
            numrows_table = "Not available"

        # String formatting parameters
        params = dict(schema=schema, table=table, link=link,
                      pattern=pattern, comment=comment_table)

        if pattern:
            comm = """Description of %(table)s with """
            comm += """pattern %(pattern)s commented as: '%(comment)s'""" % params
            comm += "\nEstimated number of rows:" + colored(" %s" % numrows_table, "green", self.ct)
            q = """
            select atc.column_name, atc.data_type,
            case atc.data_type
            when 'NUMBER' then '(' || atc.data_precision || ',' || atc.data_scale || ')'
            when 'VARCHAR2' then atc.CHAR_LENGTH || ' characters'
            else atc.data_length || ''  end as DATA_FORMAT,
            acc.comments
            from all_tab_cols%(link)s atc , all_col_comments%(link)s acc
            where atc.owner = '%(schema)s' and atc.table_name = '%(table)s'
            and acc.owner = '%(schema)s' and acc.table_name = '%(table)s'
            and acc.column_name = atc.column_name
            and atc.column_name like '%(pattern)s'
            order by atc.column_name
            """ % params
        else:
            comm = """Description of %(table)s commented as: '%(comment)s'""" % params
            comm += "\nEstimated number of rows:" + colored(" %s" % numrows_table, "green", self.ct)
            q = """
            select atc.column_name, atc.data_type,
            case atc.data_type
            when 'NUMBER' then '(' || atc.data_precision || ',' || atc.data_scale || ')'
            when 'VARCHAR2' then atc.CHAR_LENGTH || ' characters'
            else atc.data_length || ''  end as DATA_FORMAT,
            acc.comments
            from all_tab_cols%(link)s atc , all_col_comments%(link)s acc
            where atc.owner = '%(schema)s' and atc.table_name = '%(table)s'
            and acc.owner = '%(schema)s' and acc.table_name = '%(table)s'
            and acc.column_name = atc.column_name
            order by atc.column_name
            """ % params

        if extra is None:
            extra = comm
        err_msg = 'Table does not exist or it is not accessible by user or pattern do not match'
        df = self.query_and_print(q, print_time=False,
                                  err_arg=err_msg, extra=extra, clear=clear, return_df=return_df)
        if return_df:
            return df
        return

    def complete_describe_table(self, text, line, start_index, end_index):
        return self._complete_tables(text)

    def do_find_tables(self, arg, extra=None, return_df=False):
        """
        DB:Lists tables and views matching an oracle pattern  e.g %SVA%,

        Usage : find_tables PATTERN
        """
        if extra is None:
            extra = 'To select from a table use owner.table_name' \
                    ' except for DESADMIN where only table_name is enough'
        if arg == '':
            return self.do_help('find_tables')
        arg = arg.replace(';', '')
        query = "SELECT owner,table_name from all_tables  WHERE upper(table_name) LIKE '%s' " % (
            arg.upper())
        df = self.query_and_print(query, extra=extra, return_df=return_df)
        if return_df:
            return df

    def complete_find_tables(self, text, line, start_index, end_index):
        return self._complete_tables(text)

    def do_find_tables_with_column(self, arg):
        """
        DB:Finds tables having a column name matching column-name-string.

        Usage: find_tables_with_column  <column-name-substring>
        Example: find_tables_with_column %MAG%  # hunt for columns with MAG
        """
        if arg == '':
            return self.do_help('find_tables_with_column')
        query = """
           SELECT t.owner || '.' || t.table_name as table_name, t.column_name
           FROM all_tab_cols t, DES_ADMIN.CACHE_TABLES d
           WHERE t.column_name LIKE '%s'
           AND t.table_name = d.table_name
           """ % (arg.upper())

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
            schema, table, link = self.get_tablename_tuple(tablename)
            link = "@" + link if link else ""
        except:
            print(colored("Table not found.", "red", self.ct))
            return

        # Parse tablename for simple name or owner.tablename.
        # If owner present, then add owner where clause.

        params = dict(schema=schema, table=table, link=link)
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

    def check_table_exists(self, table):
        # check table first
        self.cur.execute(
            "select count(table_name) from user_tables where table_name = \'%s\'" % table.upper())
        exists = self.cur.fetchall()[0][0]
        return exists

    def load_data(self, filename):
        """Load data from a file into a pandas.DataFrame or
        fitsio.FITS object. We return the object itself, since it
        might be useful. We also monkey patch three functions to
        access the data as lists directly for upload to the DB.

        data.ea_get_columns = list of column names
        data.ea_get_values  = list of column values
        data.ea_get_dtypes  = list of column numpy dtypes

        Parameters:
        -----------
        filename : Input file name.

        Returns:
        --------
        data : A pandas.DataFrame or fitsio.FITS object
        """
        data = eafile.read_file(filename)
        return data

    def new_table_columns(self, columns, dtypes):
        """
        Create the SQL query to create a new table from a list of column names and numpy dtypes.

        Parameters:
        -----------
        columns : List of column names
        dtypes  : List of numpy dtypes

        Returns:
        --------
        query   : SQL query string
        """
        # Start the table columns
        qtable = '( '

        for column, dtype in zip(columns, dtypes):
            if self.desdm_coldefs:
                qtable += '%s %s,' % (column, eatypes.numpy2desdm([column, dtype]))
            else:
                qtable += '%s %s,' % (column, eatypes.numpy2oracle(dtype))

        # cut last ',' and close paren
        qtable = qtable[:-1] + ')'

        return qtable

    def drop_table(self, table, purge=False):
        # Added optional argument to purge the table.
        if not purge:
            qdrop = "DROP TABLE %s" % table.upper()
        else:
            qdrop = "DROP TABLE %s PURGE" % table.upper()

        try:
            self.cur.execute(qdrop)
        except cx_Oracle.DatabaseError:
            print(colored(
                "\n Couldn't drop '%s' (doesn't exist)." % (table.upper()), 'red', self.ct))
        if self.autocommit:
            self.con.commit()

    def create_table(self, table, columns, dtypes):
        """
        Create a DB table from a list of columns and numpy dtypes.

        Parameters:
        ----------
        table   : Name of the Oracle table to create
        columns : List of column names
        dtypes  : List of numpy dtypes

        Returns:
        --------
        None
        """
        qtable = 'create table %s ' % table
        qtable += self.new_table_columns(columns, dtypes)
        self.cur.execute(qtable)
        if self.autocommit:
            self.con.commit()

    def insert_data(self, table, columns, values, dtypes=None, niter=0):
        """Insert data into a DB table.

        Trim trailing whitespace from string columns. Because of the
        way `executemany` works, input needs to by python lists.

        Parameters:
        -----------
        table   : Name of the table to insert into
        columns : List of column names.
        values  : List of values
        dtypes  : List of numpy dtypes

        Returns:
        --------
        None

        """
        if dtypes is None:
            len(columns) * [None]

        # Remove trailing whitespace from string values
        cvals = []
        for column, dtype in zip(columns, dtypes):
            if dtype.kind == 'S' and self.trim_whitespace:
                cvals.append('TRIM(TRAILING FROM :%s)' % column)
            else:
                cvals.append(':%s' % column)

        cols = ','.join(columns)
        vals = ','.join(cvals)

        qinsert = 'insert into %s (%s) values (%s)' % (
            table.upper(), cols, vals)
        self.msg = ''
        try:
            t1 = time.time()
            self.cur.executemany(qinsert, values)
            t2 = time.time()
            if self.autocommit:
                self.con.commit()
        except cx_Oracle.DatabaseError as e:
            if self.desdm_coldefs:
                self.msg = str(e)
                self.msg += "\n If you are sure, you can disable DESDM column typing: \n"
                self.msg += " DESDB ~> config desdm_coldefs set no"
            raise cx_Oracle.DatabaseError(self.msg)

        print(colored(
            '\n [Iter: %d] Inserted %d rows and %d columns into table %s in %.2f seconds' % (
                niter + 1, len(values), len(columns), table.upper(), t2 - t1), "green", self.ct))

    def do_load_table(self, line, name=None, chunksize=None, memsize=None):
        """
        DB:Loads a table from a file (csv or fits) taking name from filename and columns from header

        Usage: load_table <filename> [--tablename NAME] [--chunksize CHUNK] [--memsize MEMCHUNK]
        Ex: example.csv has the following content
             RA,DEC,MAG
             1.23,0.13,23
             0.13,0.01,22

        This command will create a table named EXAMPLE with 3 columns RA,DEC and MAG and
        values taken from file

        Optional Arguments:

            --tablename NAME            given name for the table, default is taken from filename
            --chunksize CHUNK           Number of rows to be inserted at a time.
                                        Useful for large files that do not fit in memory
            --memsize MEMCHUNK          The size in Mb to be read in chunks. If both specified,
                                        the lower number of rows is selected
                                        (the lower memory limitations)

        Note: - For csv or tab files, first line must have the column names (without # or any
        other comment) and same format as data (using ',' or space)
              - For fits file header must have columns names and data types
              - For filenames use <table_name>.csv or <table_name>.fits do not use extra points
        """
        line = line.replace(';', '')
        load_parser = KeyParser(prog='', usage='', add_help=False)
        load_parser.add_argument(
            'filename', help='name for the file', action='store', default=None)
        load_parser.add_argument(
            '--tablename', help='name for the table', action='store', default=None)
        load_parser.add_argument('--chunksize',
                                 help='number of rows to read in blocks to avoid memory issues',
                                 action='store', type=int, default=None)
        load_parser.add_argument('--memsize', help='size of the chunks to be read in Mb ',
                                 action='store', type=int, default=None)
        load_parser.add_argument(
            '-h', '--help', help='print help', action='store_true')
        try:
            load_args = load_parser.parse_args(line.split())
        except SystemExit:
            self.do_help('load_table')
            return
        if load_args.help:
            self.do_help('load_table')
            return
        filename = eafile.get_filename(load_args.filename)
        table = load_args.tablename
        invalid_chars = ['-', '$', '~', '@', '*']
        for obj in [table, name]:
            if obj is None:
                if any((char in invalid_chars) for char in filename):
                    print()
                    print(colored(
                        'Invalid table name, change filename or use --tablename\n', 'red', self.ct))
                    return
            else:
                if any((char in invalid_chars) for char in obj):
                    print(colored('\nInvalid table name\n', 'red', self.ct))
                    return

        chunk = load_args.chunksize
        memchunk = load_args.memsize
        if chunksize is not None:
            chunk = chunksize
        if memsize is not None:
            memchunk = memsize
        if memchunk is not None:
            memchunk_rows = eafile.get_chunksize(filename, memory=memchunk)
            if chunk is not None:
                chunk = min(chunk, memchunk_rows)
            else:
                chunk = memchunk_rows
        if filename is None:
            return
        base, ext = os.path.splitext(os.path.basename(filename))

        if ext == '.h5' and chunk is not None:
            print(colored("\nHDF5 file upload with chunksize is not supported yet. Try without "
                          "--chunksize\n", "red", self.ct))
            return

        if table is None:
            table = base
            if name is not None:
                table = name

        # check table first
        if self.check_table_exists(table):
            print(
                colored('\n Table already exists. Table can be removed with:', 'red', self.ct))
            print(colored(' DESDB ~> DROP TABLE %s;\n' %
                          table.upper(), 'red', self.ct))
            return

        try:
            data, iterator = eafile.read_file(filename)
        except:
            print_exception(mode=self.ct)
            return

        # Get the data in a way that Oracle understands

        iteration = 0
        done = False
        total_rows = 0
        if data.file_type == 'pandas':
            while not done:
                try:
                    if iterator:
                        df = data.get_chunk(chunk)
                    else:
                        df = data
                    df.file_type = 'pandas'
                    if len(df) == 0:
                        break
                    if iteration == 0:
                        dtypes = eafile.get_dtypes(df)
                    columns = df.columns.values.tolist()
                    values = df.values.tolist()
                    total_rows += len(df)
                except:
                    break
                if iteration == 0:
                    try:
                        self.create_table(table, columns, dtypes)
                    except:
                        print_exception(mode=self.ct)
                        self.drop_table(table)
                        return
                try:
                    if not done:
                        self.insert_data(
                            table, columns, values, dtypes, iteration)
                        iteration += 1
                        if not iterator:
                            done = True
                except:
                    print_exception(mode=self.ct)
                    self.drop_table(table)
                    return

        if data.file_type == 'fits':
            if chunk is None:
                chunk = data[1].get_nrows()
            start = 0
            while not done:
                try:
                    df = data
                    if iteration == 0:
                        dtypes = eafile.get_dtypes(df)
                        columns = df[1].get_colnames()
                    values = df[1][start:start + chunk].tolist()
                    start += chunk
                    if len(values) == 0:
                        break
                    total_rows += len(values)
                except:
                    break
                if iteration == 0:
                    try:
                        self.create_table(table, columns, dtypes)
                    except:
                        print_exception(mode=self.ct)
                        self.drop_table(table)
                        return

                try:
                    if not done:
                        self.insert_data(
                            table, columns, values, dtypes, iteration)
                        iteration += 1
                except:
                    print_exception(mode=self.ct)
                    self.drop_table(table)
                    return

        print(colored(
            '\n ** Table %s loaded successfully '
            'with %d rows.\n' % (table.upper(), total_rows), "green", self.ct))
        print(colored(
            ' You may want to refresh the metadata so your new '
            'table appears during\n autocompletion', "cyan", self.ct))
        print(colored(' DESDB ~> refresh_metadata_cache;', "cyan", self.ct))

        print()
        print(colored(' To make this table public run:', "blue", self.ct))
        print(colored(' DESDB ~> grant select on %s to DES_READER;' %
                      table.upper(), "blue", self.ct), '\n')
        return

    def complete_load_table(self, text, line, start_idx, end_idx):
        return complete_path(line)

    def do_append_table(self, line, name=None, chunksize=None, memsize=None):
        """
        DB:Appends a table from a file (csv or fits) taking its name from filename
        and the columns from header.

        Usage: append_table <filename> [--tablename NAME] [--chunksize CHUNK] [--memsize MEMCHUNK]
        Ex: example.csv has the following content
             RA,DEC,MAG
             1.23,0.13,23
             0.13,0.01,22

        This command will append the contents of example.csv to the table named EXAMPLE.
        It is meant to use after load_table command

         Optional Arguments:

              --tablename NAME           given name for the table, default is taken from filename
              --chunksize CHUNK          Number of rows to be inserted at a time. Useful for large
                                         files that do not fit in memory
              --memsize MEMCHUNK         The size in Mb to be read in chunks. If both specified,
                                         the lower number of rows is selected
                                         (the lower memory limitations)

        Note: - For csv or tab files, first line must have the column names
        (without # or any other comment) and same format as data (using ',' or space)
              - For fits file header must have columns names and data types
              - For filenames use <table_name>.csv or <table_name>.fits do not use extra points
        """
        line = line.replace(';', '')
        append_parser = KeyParser(prog='', usage='', add_help=False)
        append_parser.add_argument(
            'filename', help='name for the file', action='store', default=None)
        append_parser.add_argument(
            '--tablename', help='name for the table to append to', action='store', default=None)
        append_parser.add_argument('--chunksize',
                                   help='number of rows to read in blocks to avoid memory '
                                        'issues', action='store', default=None, type=int)
        append_parser.add_argument('--memsize', help='size of the chunks to be read in Mb ',
                                   action='store', type=int, default=None)
        append_parser.add_argument(
            '-h', '--help', help='print help', action='store_true')
        try:
            append_args = append_parser.parse_args(line.split())
        except SystemExit:
            self.do_help('append_table')
            return
        if append_args.help:
            self.do_help('append_table')
            return
        filename = eafile.get_filename(append_args.filename)
        table = append_args.tablename
        invalid_chars = ['-', '$', '~', '@', '*']
        for obj in [table, name]:
            if obj is None:
                if any((char in invalid_chars) for char in filename):
                    print(colored('\nInvalid table name, change filename '
                                  'or use --tablename\n', 'red', self.ct))
                    return
            else:
                if any((char in invalid_chars) for char in obj):
                    print(colored('\nInvalid table name\n', 'red', self.ct))
                    return

        chunk = append_args.chunksize
        memchunk = append_args.memsize
        if chunksize is not None:
            chunk = chunksize
        if memsize is not None:
            memchunk = memsize
        if memchunk is not None:
            memchunk_rows = eafile.get_chunksize(filename, memory=memchunk)
            if chunk is not None:
                chunk = min(chunk, memchunk_rows)
            else:
                chunk = memchunk_rows

        if filename is None:
            return
        base, ext = os.path.splitext(os.path.basename(filename))

        if ext == '.h5' and chunk is not None:
            print(colored("\nHDF5 file upload with chunksize is not supported yet. Try without "
                          "--chunksize\n", "red", self.ct))
            return

        if table is None:
            table = base
            if name is not None:
                table = name

        # check table first
        if not self.check_table_exists(table):
            print('\n Table does not exist. Table can be created with:'
                  '\n DESDB ~> CREATE TABLE %s '
                  '(COL1 TYPE1(SIZE), ..., COLN TYPEN(SIZE));\n' % table.upper())
            return
        try:
            data, iterator = eafile.read_file(filename)
        except:
            print_exception(mode=self.ct)
            return

        iteration = 0
        done = False
        total_rows = 0
        if data.file_type == 'pandas':
            while not done:
                try:
                    if iterator:
                        df = data.get_chunk(chunk)
                    else:
                        df = data
                    df.file_type = 'pandas'
                    if len(df) == 0:
                        break
                    if iteration == 0:
                        dtypes = eafile.get_dtypes(df)
                    columns = df.columns.values.tolist()
                    values = df.values.tolist()
                    total_rows += len(df)
                except:
                    break
                try:
                    if not done:
                        self.insert_data(
                            table, columns, values, dtypes, iteration)
                        iteration += 1
                        if not iterator:
                            done = True
                except:
                    print_exception(mode=self.ct)
                    return

        if data.file_type == 'fits':
            if chunk is None:
                chunk = data[1].get_nrows()
            start = 0
            while not done:
                try:
                    df = data
                    if iteration == 0:
                        dtypes = eafile.get_dtypes(df)
                        columns = df[1].get_colnames()
                    values = df[1][start:start + chunk].tolist()
                    start += chunk
                    if len(values) == 0:
                        break
                    total_rows += len(values)
                except:
                    break
                try:
                    if not done:
                        self.insert_data(
                            table, columns, values, dtypes, iteration)
                        iteration += 1
                except:
                    print_exception(mode=self.ct)
                    return

        print(colored('\n ** Table %s appended '
                      'successfully with %d rows.' % (table.upper(), total_rows), "green", self.ct))

    def complete_append_table(self, text, line, start_idx, end_idx):
        return complete_path(line)

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
                print(colored('\nMissing table name\n', "red", self.ct))
                return
            table = keys[1]
            if len(keys) == 2:
                print(colored('\nMissing comment for table %s\n' %
                              table, "red", self.ct))
                return
            comm = " ".join(keys[2:])
            if comm[0] == "'":
                comm = comm[1:-1]
            qcom = """comment on table %s is '%s'""" % (table, comm)
            message = "Comment added to table: %s" % table
            self.query_and_print(qcom, print_time=False, suc_arg=message)
        elif oneline.find('column') > -1:
            if len(keys) == 1:
                print(colored('\nMissing column name (TABLE.COLUMN)\n', "red", self.ct))
                return
            col = keys[1]
            if len(keys) == 2:
                print(colored('\nMissing comment for column %s\n' %
                              col, "red", self.ct))
                return
            if len(keys) > 2 and col.find('.') == -1:
                print(colored('\nMissing column name for table %s\n',
                              "red", self.ct) % col)
                return
            comm = " ".join(keys[2:])
            if comm[0] == "'":
                comm = comm[1:-1]
            qcom = """comment on column  %s is '%s'""" % (col, comm)
            message = "Comment added to column: %s in table %s" % (
                col.split('.')[1], col.split('.')[0])
            self.query_and_print(qcom, print_time=False, suc_arg=message)
        else:
            print(colored('\nMissing arguments\n', "red", self.ct))
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
                    return [tablename + '.' + cn for cn in
                            self._complete_colnames(colname) if cn.startswith(colname)]
                else:
                    return self._complete_tables(text)
            else:
                return [option for option in options_add_comment if option.startswith(text)]
        else:
            return options_add_comment

    def do_version(self, line):
        """
        Print current  and latest pip version of easyacccess
        """
        last_version = last_pip_version()
        print()
        print(colored("Current version  : easyaccess {}".format(
            __version__), "green", self.ct))
        print(colored("Last pip version : easyaccess {}".format(
            last_version), "green", self.ct))
        print()
        return

    # UNDOCUMENTED DO METHODS

    def do_EOF(self, line):
        # Exit program on ^D (Ctrl+D)
        print()  # For some reason this is missing...
        self.do_exit(line)

    def do_quit(self, line):
        self.do_exit(line)

    def do_select(self, line):
        self.default('select ' + line)

    def do_SELECT(self, line):
        self.default('SELECT ' + line)

    def do_clear_history(self, line):
        if readline_present:
            readline.clear_history()

    def do_online_tutorial(self, line):
        tut = webbrowser.open_new_tab(
            'http://deslogin.cosmology.illinois.edu/~mcarras2/data/DESDM.html')
        del tut


# ############## PYTOHN API ###############################

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


def to_pandas(cur):
    """
    Returns a pandas DataFrame from a executed query
    """
    if cur.description is not None:
        data = pd.DataFrame(cur.fetchall(), columns=[
                            rec[0] for rec in cur.description])
    else:
        data = ""
    return data


class connect(easy_or):
    def __init__(self, section='', user=None, passwd=None, quiet=False, refresh=False):
        """
        Creates a connection to the DB as easyaccess commands, section is
         obtained from config file, can be bypass here, e.g., section = desoper

        Parameters:
        -----------
        section :  DB connection : dessci, desoper, destest, newsci
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
        desconf = config_mod.get_desconfig(desfile, db)
        if user is not None:
            print('Bypassing .desservices file with user : %s' % user)
            if passwd is None:
                passwd = getpass.getpass(prompt='Enter password : ')
            desconf.set('db-' + db, 'user', user)
            desconf.set('db-' + db, 'passwd', passwd)
        easy_or.__init__(self, conf, desconf, db, interactive=False, quiet=quiet)
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


# #################################################

def initial_message(quiet=False, clear=True):
    if not quiet:
        if clear:
            os.system(['clear', 'cls'][os.name == 'nt'])
        # No messages for now


def cli():
    """
    Main function to run the command line interpreter either interactively or just simple commands
    """
    global load_bar, colored
    conf = config_mod.get_config(config_file)

    if readline_present:
        try:
            readline.read_history_file(history_file)
            readline.set_history_length(conf.getint('easyaccess', 'histcache'))
        except:
            print(colored('readline might have problems accessing history', 'red'))

    args = eaparser.get_args(config_file)  # Reads command line arguments

    # PANDAS DISPLAY SET UP
    pd.set_option('display.max_rows', conf.getint('display', 'max_rows'))
    pd.set_option('display.width', conf.getint('display', 'width'))
    pd.set_option('display.max_columns', conf.getint('display', 'max_columns'))
    pd.set_option('display.max_colwidth', conf.getint('display', 'max_colwidth'))
    load_bar = conf.getboolean('display', 'loading_bar')
    if args.quiet:
        conf.set('display', 'loading_bar', 'no')

    if args.db is not None:
        db = args.db
        if db[:3] == 'db-':
            db = db[3:]
    else:
        db = conf.get('easyaccess', 'database')

    if args.user is not None:
        print('Bypassing .desservices file with user : %s' % args.user)
        if args.password is None:
            print('Must include password')
            os._exit(0)
        else:
            desconf = config_mod.get_desconfig(
                desfile, db, verbose=False, user=args.user, pw1=args.password)
            desconf.set('db-' + db, 'user', args.user)
            desconf.set('db-' + db, 'passwd', args.password)
    else:
        desconf = config_mod.get_desconfig(desfile, db)

        initial_message(args.quiet, clear=False)
    if args.command is not None:
        cmdinterp = easy_or(conf, desconf, db, interactive=False,
                            quiet=args.quiet, refresh=not args.norefresh)
        cmdinterp.onecmd(args.command)
    elif args.loadsql is not None:
        cmdinterp = easy_or(conf, desconf, db, interactive=False,
                            quiet=args.quiet, refresh=not args.norefresh)
        linein = "loadsql " + args.loadsql
        cmdinterp.onecmd(linein)
    elif args.loadtable is not None:
        cmdinterp = easy_or(conf, desconf, db, interactive=False,
                            quiet=args.quiet, refresh=not args.norefresh)
        linein = "load_table " + args.loadtable
        if args.tablename is not None:
            linein += ' --tablename ' + args.tablename
        if args.chunksize is not None:
            linein += ' --chunksize ' + str(args.chunksize)
        if args.memsize is not None:
            linein += ' --memsize ' + str(args.memsize)
        cmdinterp.onecmd(linein)
    elif args.appendtable is not None:
        cmdinterp = easy_or(conf, desconf, db, interactive=False,
                            quiet=args.quiet, refresh=not args.norefresh)
        linein = "append_table " + args.appendtable
        if args.tablename is not None:
            linein += ' --tablename ' + args.tablename
        if args.chunksize is not None:
            linein += ' --chunksize ' + str(args.chunksize)
        if args.memsize is not None:
            linein += ' --memsize ' + str(args.memsize)
        cmdinterp.onecmd(linein)
    else:
        initial_message(args.quiet, clear=True)
        easy_or(conf, desconf, db, quiet=args.quiet,
                refresh=not args.norefresh).cmdloop()
    os._exit(0)


if __name__ == '__main__':
    "Main function"
    cli()
