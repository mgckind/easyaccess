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