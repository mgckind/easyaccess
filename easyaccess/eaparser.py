from __future__ import print_function
import argparse
from . import config_ea as config_mod
from .version import __version__
import sys
import os


class MyParser(argparse.ArgumentParser):
    def error(self, message):
        print('\n*****************')
        sys.stderr.write('error: %s \n' % message)
        print('*****************\n')
        self.print_help()
        sys.exit(2)


def get_args(config_file):
    conf = config_mod.get_config(config_file)
    parser = MyParser(
        description='Easy access to the DES database. There is a configuration file '
                    'located in %s for more customizable options' % config_file)
    parser.add_argument("-v", "--version", action="store_true",
                        help="print version number and exit")
    parser.add_argument("-c", "--command", dest='command',
                        help="Executes command and exit")
    parser.add_argument("-l", "--loadsql", dest='loadsql',
                        help="Loads a sql command, execute it and exit")
    parser.add_argument("-lt", "--load_table", dest='loadtable',
                        help="Loads data from a csv, tab, or fits formatted file \
                        into a DB table using the filename as the table name or a custom \
                        name with --tablename MYTABLE")
    parser.add_argument("-at", "--append_table", dest='appendtable',
                        help="Appends data from a csv, tab, or fits formatted file \
                        into a DB table using the filename as the table name or a custom \
                        name with --tablename MYABLE")
    parser.add_argument("--tablename", dest='tablename',
                        help="Custom table name to be used with --load_table\
                        or --append_table")
    parser.add_argument("--chunksize", dest='chunksize', type=int, default=None,
                        help="Number of rows to be inserted at a time. Useful for large files "
                             "that do not fit in memory. Use with --load_table or --append_table")
    parser.add_argument("--memsize", dest='memsize', type=int, default=None,
                        help=" Size of chunk to be read at a time in Mb. Use with --load_table or "
                             "--append_table")
    parser.add_argument("-s", "--db", dest='db',
                        choices=['dessci', 'desoper', 'destest', 'desdr'],
                        help="Override database name [dessci,desoper,destest,desdr]")
    parser.add_argument("-q", "--quiet", action="store_true", dest='quiet',
                        help="Silence initialization, no loading bar")
    parser.add_argument("-u", "--user", dest='user')
    parser.add_argument("-p", "--password", dest='password')
    parser.add_argument("-nr", "--no_refresh", dest='norefresh', action="store_true",
                        help="Do not refresh metadata at starting up to speed initialization. "
                             "Metadata can always be refreshed from inside using the "
                             "refresh_metadata command")
    parser.add_argument("--config", help="--config show, will print content of "
                        "config file\n"
                        "--config reset will reset config to default "
                        "values\n"
                        "--config set param1=val1 param2=val2 will "
                        "modify parameters for the session only", nargs='+')
    args = parser.parse_args()

    if args.version:
        print("\nCurrent : easyaccess {:} \n".format(__version__))
        sys.exit()

    if args.config:
        if args.config[0] == 'show':
            print('\n Showing content of the config file (%s) :\n' % config_file)
            file_temp = open(config_file, 'r')
            for line in file_temp.readlines():
                print(line.strip())
            file_temp.close()
            sys.exit()
        elif args.config[0] == 'reset':
            print('\n ** Reset  config file (%s) to its default!! **:\n' % config_file)
            check = input(' Proceed? (y/[n]) : ')
            if check.lower() == 'y':
                os.remove(config_file)
                conf = config_mod.get_config(config_file)
                sys.exit()
        elif args.config[0] == 'set':
            if len(args.config) == 1:
                parser.print_help()
                sys.exit()
            entries = ','.join(args.config[1:])
            entries = entries.replace(',,', ',')
            entries = entries.split(',')
            for e in entries:
                if e == '':
                    continue
                updated = False
                try:
                    key, value = e.split('=')
                    for section in (conf.sections()):
                        if conf.has_option(section, key):
                            conf.set(section, key, str(value))
                            updated = True
                    if not updated:
                        raise
                except:
                    print("Check the key exists or that you included the '=' for the "
                          "parameter\nFor more info use --help.")
                    sys.exit()
        else:
            parser.print_help()
            sys.exit()
    return args
