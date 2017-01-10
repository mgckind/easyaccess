from __future__ import print_function
import argparse
import itertools
import dircache
import sys
import time

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

def _complete_path(line):
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
