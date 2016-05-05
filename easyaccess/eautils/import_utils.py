import sys
import importlib
try:
    import fun_utils
except ImportError as e:
    try:
        import eautils.fun_utils as fun_utils
    except:
        import easyaccess.eautils.fun_utils as fun_utils

from inspect import getmembers, isfunction
try:
    from termcolor import colored
except:
    def colored(line, color): return line

def print_exception():
    (type, value, traceback) = sys.exc_info()
    print()
    print(colored(type, "red"))
    print(colored(value, "red"))
    print()

class Import(object):
    def do_import(self, line):
        """
        Use to import modules to call functions inline query, similar to the import module in python.

        Use only to import modules directly, like:
            DESDB ~> import module
            or
            DESDB ~> import module as name

        Functions inside module need to be wrapped for easyaccess, like

        from eautils.fun_utils import toeasyaccess

        @toeasyaccess
        def my_func(a,b):
            ...
            return column
        """

        line.replace(';','')
        line = ' '.join(line.split())
        line = line.split()
        if len(line) == 3 and line[1] == 'as':
            mod = line[0]
            modname = line[-1]
        elif len(line) == 1:
            mod = line[0]
            modname = line[0]
        else:
            print(colored('Use: import module OR import module as name',"red"))
            return
        command = modname + ' = importlib.import_module(\''+mod+'\')'
        if modname in globals().keys():
            try:
                exec('reload('+modname+')', globals())
            except NameError:
                exec('importlib.reload('+modname+')', globals())
        try:
            exec(command ,globals())
            func_list = [f for f in getmembers(globals()[modname]) if (isfunction(f[1]) and hasattr(f[1], 'in_easyaccess'))]
            if len(func_list) > 0:
                print(colored("The following functions are accessible by easyaccess", "green"))
                print(colored("i.e., they are wrapped with @toeasyaccess", "green"))
                print('')
                for f in func_list:
                    print('    '+modname+'.'+f[0]+'()')
                    fun_utils.ea_func_dictionary[modname+'.'+f[0]] =  f[1]
            else:
                print(colored("No function wrapped for easyaccess was found in "+modname, "red"))
                print(colored("See documentation to see how to wrap functions", "red"))
        except:
            print_exception()
            return

    def do_help_function(self, line):
        """
        Print help from a loaded external function wrapped by @toeasyaccess
        It uses autocompletion
         
        Use: DESDB ~> help_function function

        Use: DESDB ~> help_function all
             To list all loaded functions
        """
        line = line.replace(';','')
        line = line.replace('()','')
        if line.split() == []:
            return self.do_help('help_function')
        function = line.split()[0]
        if function.lower() == 'all':
            print("\nThese are the loaded functions for easyaccess:\n")
            for k in fun_utils.ea_func_dictionary.keys():
                print('    '+k)
            return
        if not function in fun_utils.ea_func_dictionary.keys():
            print(colored("\nFunction {0} is not loaded, please import module (check help import for more info)\n".format(function),"red"))
            return
        else:
            print("\nHelp for {0}:\n".format(function))
            func = fun_utils.ea_func_dictionary[function]
            print(function+func.__doc1__)
            print(func.__doc__)


    def complete_help_function(self, text, line, start_index, end_index):
        if text:
            return [function for function in fun_utils.ea_func_dictionary.keys() if function.startswith(text)]
        else:
            return fun_utils.ea_func_dictionary.keys()

