import cmd
#import cx_Oracle
import sys
import os
import re
import readline
readline.parse_and_bind('tab: complete')


section="db-dessci"
host='leovip148.ncsa.uiuc.edu'
port='1521'
name='dessci'
kwargs = {'host': host, 'port': port, 'service_name':name}
#dsn = cx_Oracle.makedsn(**kwargs)


class easy_or(cmd.Cmd,object):
    """cx_oracle interpreter for DESDM"""
    intro = 'The DESDM Database shell.  Type help or ? to list commands.\n'

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.table_restriction_clause  = " "
        self.savePrompt = ">> "       
        self.prompt = self.savePrompt
        self.pipe_process_handle = None 


        #global con
        #con=cx_Oracle.connect('mcarras2','Alnilam1',dsn=dsn)
        #cur=con.cursor()
        #self.cache_completion_names()


    def do_greet(self, line):
        " Says hello"
        print "hello"
   
    def do_hist(self, line):
        """Print a list of commands that have been entered"""
        print self._hist

    def do_shell(self, line):
        "execute shell commands"
        os.system(line)

    def do_edit(self,line):
        "Opens a buffer file to edit and the reads it"
        os.system('nano easy.buf')
        if os.path.exists('easy.buf'):
            with open('easy.buf') as f: content = f.read()
            List = [item for item in content.split('\n')]
            newquery = ' '.join(List)
            if (raw_input('submit query? (yes/no):')=='yes'):
                print newquery
            print

    def preloop(self):
        """Initialization before prompting user for commands.
        Despite the claims in the Cmd documentaion, Cmd.preloop() is not a stub.
        """
        cmd.Cmd.preloop(self)   ## sets up command completion
        self._hist    = []      ## No history yet
        self._locals  = {}      ## Initialize execution namespace for user
        self._globals = {}

    def precmd(self, line):
        """ This method is called after the line has been input but before
            it has been interpreted. If you want to modifdy the input line
            before execution (for example, variable substitution) do it here.
        """
        self._hist += [ line.strip() ]
        return line

    def do_EOF(self, line):
        "exit program on ^D"
        print
        sys.exit(0)

    def emptyline(self): pass


    def do_exit(self,line):
        "exit the program"
        try: os.system('rm -f easy.buf')
        except: pass
        try:
            cur.close()
        except:
            pass
        #con.commit()
        #con.close()
        sys.exit(0)


if __name__ == '__main__':
    easy_or().cmdloop()
