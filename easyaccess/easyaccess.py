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
import easyaccess.config_ea as config_mod
from easyaccess.eautils import des_logo as dl
import easyaccess.eautils.dtypes as eatypes
import easyaccess.eautils.fileio as eafile
import easyaccess.eautils.fun_utils as fun_utils
import easyaccess.eaparser as eaparser
from easyaccess.eautils.import_utils import Import
from easyaccess.eautils.cli_utils import CommandActions
from easyaccess.eautils.db_utils import DatabaseActions
from easyaccess.eautils.des_utils import DesActions
from easyaccess.eautils.ea_utils import *
import threading
import time
import pandas as pd
import signal
import warnings
warnings.filterwarnings("ignore")
try:
    from builtins import input, str, range
except ImportError:
    from __builtin__ import input, str, range

__author__ = 'Matias Carrasco Kind'

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
dbnames = ('dessci', 'desoper', 'destest', 'desdr')

# commands not available in public DB
NOT_PUBLIC = ['add_comment', 'append_table', 'change_db', 'execproc',
              'find_user', 'load_table', 'myquota', 'mytables', 'user_tables']

sys.path.insert(0, os.getcwd())
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


class easy_or(cmd.Cmd, CommandActions, DatabaseActions, DesActions, Import, object):
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
            '_________', 'magenta', self.ct) + '\n%s ~> ' % (self.dbname.upper())
        self.prompt = self.savePrompt
        self.doc_header = colored(
            ' *General Commands*', "cyan", self.ct) + ' (type help <command>):'
        self.docdb_header = colored(
            '\n *DB Commands*', "cyan", self.ct) + '      (type help <command>):'

    def __init__(self, conf, desconf, db, interactive=True,
                 quiet=False, refresh=True, pymod=False):
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
        self.compression = self.config.getboolean('easyaccess', 'compression')
        self.desdm_coldefs = self.config.getboolean('easyaccess', 'desdm_coldefs')
        self.trim_whitespace = self.config.getboolean('easyaccess', 'trim_whitespace')
        self.dbname = db
        self.buff = None
        self.interactive = interactive
        self.undoc_header = None
        self.metadata = True
        self.user = self.desconfig.get('db-' + self.dbname, 'user')
        self.dbhost = self.desconfig.get('db-' + self.dbname, 'server')
        self.service_name = self.desconfig.get('db-' + self.dbname, 'name')
        self.port = self.desconfig.get('db-' + self.dbname, 'port')
        self.password = self.desconfig.get('db-' + self.dbname, 'passwd')
        kwargs = {'host': self.dbhost, 'port': self.port, 'service_name': self.service_name}
        self.dsn = cx_Oracle.makedsn(**kwargs)
        ora_code = 0
        if not self.quiet:
            print('Connecting to DB ** %s ** ...' % self.dbname)
        connected = False
        for tries in range(1):
            try:
                self.con = cx_Oracle.connect(self.user, self.password, dsn=self.dsn)
                if self.autocommit:
                    self.con.autocommit = True
                connected = True
                break
            except Exception as e:
                trace = sys.exc_info()
                ora_code = trace[1].args[0].code
                if ora_code == 28001:
                    break
                lasterr = str(e).strip()
                print(colored("Error when trying to connect to database: %s" %
                              lasterr, "red", self.ct))
                print("\n   Retrying...\n")
                time.sleep(5)
        if ora_code == 28001:
            print(colored("ORA-28001: the password has expired "
                  "or cannot be the default one", "red", self.ct))
            print(colored("Need to create a new password\n", "red", self.ct))
            pw1 = getpass.getpass(prompt='Enter new password:')
            if re.search('\W', pw1):
                print(colored("\nPassword contains whitespace, not set\n", "red", self.ct))
                if pymod:
                    raise Exception('Not connected to the DB')
                else:
                    os._exit(0)
            if not pw1:
                print(colored("\nPassword cannot be blank\n", "red", self.ct))
                if pymod:
                    raise Exception('Not connected to the DB')
                else:
                    os._exit(0)
            pw2 = getpass.getpass(prompt='Re-Enter new password:')
            print()
            if pw1 != pw2:
                print(colored("Passwords don't match, not set\n", "red", self.ct))
                if pymod:
                    raise Exception('Not connected to the DB')
                else:
                    os._exit(0)
            try:
                self.con = cx_Oracle.connect(self.user, self.password,
                                             dsn=self.dsn, newpassword=pw1)
                if self.autocommit:
                    self.con.autocommit = True
                self.password = pw1
                connected = True
                self.desconfig.set('db-'+self.dbname, 'passwd', pw1)
                config_mod.write_desconfig(desfile, self.desconfig)
            except Exception as e:
                lasterr = str(e).strip()
                print(colored("Error when trying to connect to database: %s" %
                              lasterr, "red", self.ct))
        if not connected:
            print('\n ** Could not successfully connect to DB. Try again later. Aborting. ** \n')
            if pymod:
                raise Exception('Not connected to the DB')
            else:
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
                        if arg in NOT_PUBLIC and self.dbname == 'desdr':
                            doc = colored('\n\t* Command not availble in Public Release DB *\n',
                                          'red', self.ct) + doc
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
                    if cmd in NOT_PUBLIC and self.dbname == 'desdr':
                        continue
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

    # print topics
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
                            format_f = lambda s: '{: <{width}}'.format(s, width=int(width))
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
        except cx_Oracle.DatabaseError as exc:
            # Whenever DatabaseError is raised e.g. by a syntax error in a query
            # close the connection to the database instead of trying to cancel a transaction.
            # Otherwise, the connection will hang indefinitely during the next attempt to utilize it.
            # See also:
            # * https://community.oracle.com/thread/3612149
            # * https://community.oracle.com/thread/717738
            # * https://github.com/mgckind/easyaccess/issues/130
            print(colored('Your query raised DatabaseError:', "red", self.ct))
            print(colored(str(exc), "red", self.ct))
            self.con.close()
            if self.loading_bar:
                if self.pload.pid is not None:
                    os.kill(self.pload.pid, signal.SIGKILL)
        except Exception as exc:
            (type, value, traceback) = sys.exc_info()
            self.con.cancel()
            t2 = time.time()

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
                                                      max_mb=self.outfile_max_mb, query=query, comp=self.compression)

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

        if self.dbname in dbnames:
            query = """
            select table_name from DES_ADMIN.CACHE_TABLES
            union select table_name from user_tables
            """
        temp = self.cur.execute(query)
        tnames = pd.DataFrame(temp.fetchall())
        table_list = tnames.values.flatten().tolist()
        return table_list


    def get_userlist(self):
        if self.dbname in ('dessci', 'desoper'):
            query = 'select distinct username from dba_users order by username'
        if self.dbname in ('destest'):
            query = 'select distinct username from dba_users order by username'
        if self.dbname in ('desdr'):
            return []
        temp = self.cur.execute(query)
        tnames = pd.DataFrame(temp.fetchall())
        user_list = tnames.values.flatten().tolist()
        return user_list

    def get_columnlist(self):
        query = """SELECT column_name from DES_ADMIN.CACHE_COLUMNS"""
        temp = self.cur.execute(query)
        cnames = pd.DataFrame(temp.fetchall())
        col_list = cnames.values.flatten().tolist()
        return col_list


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
            compression       : yes/no toggles compressed output files (bzip2 for h5, gzip for rest).
                                default(no). It is slower but yields smaller files, fits doesn't support
                                append on compressed files, workaround is to increase prefetch
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
                               'desdm_coldefs', 'compression']:
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
            if key == 'compression':
                self.compression = self.config.getboolean('easyaccess', 'compression')
            if key == 'autocommit':
                self.autocommit = self.config.getboolean('easyaccess', 'autocommit')
            if key == 'trim_whitespace':
                self.trim_whitespace = self.config.getboolean('easyaccess', 'trim_whitespace')
            if key == 'desdm_coldefs':
                self.desdm_coldefs = self.config.getboolean('easyaccess', 'desdm_coldefs')

            return
        else:
            return self.do_help('config')

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
