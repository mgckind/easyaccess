from easyaccess.eautils.ea_utils import * 
from easyaccess.version import last_pip_version 
from easyaccess.version import __version__ 
import easyaccess.config_ea as config_mod 
import os 
import stat
import sys
import cmd
import getpass
import re 
import cx_Oracle
import webbrowser

try: 
    from builtins import input, str, range 
except ImportError: 
    from __builtin__ import input, str, range
    
try: 
    import readline
    readline_present = True
    try: 
        import gnureadline as readline
    except ImportError: 
        pass
except ImportError: 
    readline_present = False

class CommandActions(object):
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
            'http://matias-ck.com/easyaccess/')
        del tut   
        
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
        
    def complete_prefetch(self, text, line, start_index, end_index):
        if text:
            return [option for option in options_prefetch if option.startswith(text)]
        else:
            return options_prefetch  
   
    def complete_shell(self, text, line, start_idx, end_idx):
        if line:
            line = ' '.join(line.split()[1:])
            return complete_path(line)

        
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
            
