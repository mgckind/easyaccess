from easyaccess.eautils.ea_utils import *
from easyaccess.version import last_pip_version 
from easyaccess.version import __version__ 
import easyaccess.config_ea as config_mod 
import pandas as pd
import os 
import stat
import sys
import cmd
import getpass
import re 
import cx_Oracle


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


try: 
    import readline
    readline_present = True
    try: 
        import gnureadline as readline
    except ImportError: 
        pass
except ImportError: 
    readline_present = False
    
class DesActions(object): 
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
            confirm = 'Password could not be changed in %s\n' % self.dbname.upper()
            print(colored(confirm, "red", self.ct))
            print(sys.exc_info()) 
            
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
           AND t.owner || '.' || t.table_name = d.table_name
           """ % (arg.upper())

        self.query_and_print(query)
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
        
        
    def get_tables_names_user(self, user):
        if user == "":
            return do_help('tables_names_user')
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

     
        
                