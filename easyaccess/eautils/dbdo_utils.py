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
    