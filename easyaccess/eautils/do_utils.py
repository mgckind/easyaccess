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
    import readline
    readline_present = True
    try: 
        import gnureadline as readline
    except ImportError: 
        pass
except ImportError: 
    readline_present = False

class Do_Func(object):
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
        
        
        
    def do_user_tables(self, arg):
        """
        DB:List tables from given user

        Usage: user_tables <username>
        """
        if arg == "":
            return self.do_help('user_tables')
        return self.get_tables_names_user(arg) 
    
    
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
        
    def complete_edit(self, text, line, start_index, end_index):
        if text:
            return [option for option in options_edit if option.startswith(text)]
        else:
            return options_edit   
   

    def complete_loadsql(self, text, line, start_idx, end_idx):
        return complete_path(line)
    
    
    def complete_change_db(self, text, line, start_index, end_index):
        options_db = ['desoper', 'dessci', 'destest']
        if text:
            return [option for option in options_db if option.startswith(text.lower())]
        else:
            return options_db

        
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
            
            
    def complete_find_user(self, text, line, start_index, end_index):
        options_users = self.cache_usernames
        if text:
            return [option for option in options_users if option.startswith(text.lower())]
        else:
            return options_users 
        
        
    def complete_user_tables(self, text, line, start_index, end_index):
        options_users = self.cache_usernames
        if text:
            return [option for option in options_users if option.startswith(text.lower())]
        else:
            return options_users
        
    def complete_describe_table(self, text, line, start_index, end_index):
        return self._complete_tables(text)
    
    def complete_find_tables(self, text, line, start_index, end_index):
        return self._complete_tables(text)
    
    def complete_find_tables_with_column(self, text, line, begidx, lastidx):
        return self._complete_colnames(text)
    
    def complete_show_index(self, text, line, begidx, lastidx):
        return self._complete_tables(text)

    def complete_load_table(self, text, line, start_idx, end_idx):
        return complete_path(line)
    
    def complete_append_table(self, text, line, start_idx, end_idx):
        return complete_path(line)
    
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
        
        
