from __future__ import print_function
import argparse
import itertools
import dircache
import sys
import os
import time
import signal
import fileio as eafile


def print_exception(pload=None, mode=1):
    (type, value, traceback) = sys.exc_info()
    if pload and (pload.pid is not None):
        os.kill(pload.pid, signal.SIGKILL)
    print()
    print(colored(type, "red", mode))
    print(colored(value, "red", mode))
    print()


options_prefetch = ['show', 'set', 'default']
options_add_comment = ['table', 'column']
options_edit = ['show', 'set_editor']
options_out = eafile.FILE_EXTS
options_def = eafile.FILE_DEFS
# ADW: It would be better to grab these from the config object
options_config = ['all', 'database', 'editor', 'prefetch', 'histcache', 'timeout',
                  'outfile_max_mb', 'max_rows', 'max_columns',
                  'width', 'max_colwidth', 'color_terminal', 'loading_bar', 'filepath', 'nullvalue',
                  'autocommit', 'trim_whitespace', 'desdm_coldefs']
options_config2 = ['show', 'set']
options_app = ['check', 'submit', 'explain']


def read_buf(fbuf):
    """
    Read SQL files, sql statement should end with ';' if parsing to a file to write.
    """
    try:
        with open(fbuf) as f:
            content = f.read()
    except:
        print('\n' + 'Fail to load the file "{:}"'.format(fbuf))
        return ""
    list = [item for item in content.split('\n')]
    newquery = ''
    for line in list:
        if line[0:2] == '--':
            continue
        newquery += ' ' + line.split('--')[0]
    # newquery = newquery.split(';')[0]
    return newquery


class KeyParser(argparse.ArgumentParser):
    def error(self, message):
        sys.exit(2)


def loading():
    char_s = u"\u2606"
    if sys.stdout.encoding != 'UTF-8':
        char_s = "o"
    print()
    cc = 0
    spinner = itertools.cycle(list(range(13)) + list(range(1, 14, 1))[::-1])
    line2 = "  Ctrl-C to abort; "
    try:
        while True:
            line = list('    |              |')
            time.sleep(0.1)
            idx = int(next(spinner))
            line[5 + idx] = char_s
            sys.stdout.write("".join(line))
            sys.stdout.write(line2)
            sys.stdout.flush()
            sys.stdout.write('\b' * len(line) + '\b' * len(line2))
    except:
        pass


def complete_path(line):
    line = line.split()
    if len(line) < 2:
        filename = ''
        path = './'
    else:
        path = line[1]
        if '/' in path:
            i = path.rfind('/')
            filename = path[i + 1:]
            path = path[:i]
        else:
            filename = path
            path = './'
    ls = dircache.listdir(path)
    ls = ls[:]
    dircache.annotate(path, ls)
    if filename == '':
        return ls
    else:
        return [f for f in ls if f.startswith(filename)]
