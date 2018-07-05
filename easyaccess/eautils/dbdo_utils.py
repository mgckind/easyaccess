from easyaccess.eautils.ea_utils import *



try: #try import readline, readline_present = True 
    import readline
    readline_present = True
    try: #try import gnureadline as readline 
        import gnureadline as readline
    except ImportError: #if import error, pass 
        pass
except ImportError: #except import error, readline_present = False 
    readline_present = False

class DB_Func(object): 
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

    