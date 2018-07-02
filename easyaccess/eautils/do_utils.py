from easyaccess.eautils.ea_utils import * 
import os 

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
                    