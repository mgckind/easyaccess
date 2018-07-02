from easyaccess.eautils.ea_utils import * 
import os 
import sys 
import getpass
import re 
import cx_Oracle

#class that contains most of the "do" functions used in easyaccess 
class Do_Func(object):
    #problem with importing do_history is that it references readline_present, a variable created in easyaccess.py 
    #This reference problem will need to be addressed 
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
        DB:Set a new password on this DES instance

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
        try:
            self.query_and_print(query, print_time=False, suc_arg=confirm)
            self.desconfig.set('db-'+self.dbname, 'passwd', pw1)
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
        DB: Change to another database, namely dessci, desoper, destest

         Usage:
            change_db DB     # Changes to DB, it does not refresh metadata, e.g.: change_db desoper

        """
        if line == '':
            return self.do_help('change_db')
        line = " ".join(line.split())
        key_db = line.split()[0]
        if key_db in ('dessci', 'desoper', 'destest'):
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
            for tries in range(1):
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

