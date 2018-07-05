from easyaccess.eautils.ea_utils import *


try: 
    import readline
    readline_present = True
    try: 
        import gnureadline as readline
    except ImportError: 
        pass
except ImportError: 
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
    