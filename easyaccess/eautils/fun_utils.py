from __future__ import print_function
import inspect
import re
from functools import wraps
import importlib
import numpy as np
import pandas as pd


try:
    from termcolor import colored
except:
    def colored(line, color):
        return line


def init_func():
    global ea_func_dictionary
    ea_func_dictionary = {}


def toeasyaccess(custom):
    @wraps(custom)
    def easy_function(*args, **kwargs):
        check = inspect.getargspec(custom)
        nargs = len(check.args)
        if check.defaults is not None:
            ndef = len(check.defaults)
        else:
            ndef = 0
        return custom(*args, **kwargs)

    check = inspect.getargspec(custom)
    try:
        n_def = len(check.defaults)
    except:
        n_def = 0
    nargs_p = len(check.args) - n_def
    head = []
    for j, ag in enumerate(check.args):
        if j < nargs_p:
            head.append(ag)
        else:
            head.append(ag + '=' + str(check.defaults[j-narg_p]))    
    temp = easy_function
    temp.__doc1__ = '('+', '.join(head)+')'
    temp.in_easyaccess = True
    temp.__doc__ = custom.__doc__

    return temp


def parseQ(query, myglobals=None):
    entries = re.findall('/\*p:(.*?)\*/', query)
    funs = None
    args = None
    names = None
    nf = 0
    if len(entries) > 0:
        funs = []
        args = []
        names = []
        for e in entries:
            try:
                name = e.split()[e.split().index('as') + 1]
            except:
                name = None
            temp = "".join(e.split())
            f = temp[:temp.find('(')]
            if name is None: name = f.lower()
            ar = temp[temp.find('(') + 1:temp.find(')')]
            funs.append(f)  # f.lower()
            all_args = ar.split(',')
            positional = []
            optional = []
            new = []
            for a in all_args:
                if a.find('=') > -1:
                    optional.append(a)
                    new.append(a)
                else:
                    positional.append(a)
            args.append([new, len(positional)])
            names.append(name)
            b = [j + ' as F' + str(nf) + 'arg' + str(i) for i, j in enumerate(positional)]
            query = query.replace('/*p:' + e + '*/', ",".join(b))
            nf += 1
        for f in funs:
            modname = f
            if f.find('.') > -1: modname, func_name = f.split('.')
            try:
                if myglobals is None:
                    _ = ea_func_dictionary[f]
            except:
                print(colored("\n\nYou might need to import %s" % modname, "red"))
                raise
    return query, funs, args, names


def updateDF(D, f, a, n, idx, myglobals=None):
    """
    Updates a data frame in place.
    """
    #ii = np.where(D.columns.values == 'F' + str(idx) + 'ARG0')[0][0]
    ii = D.columns.get_loc('F' + str(idx) + 'ARG0')
    func = f[idx]
    if func.find('.') > -1:
        modname, func_name = func.split('.')
        try:
            if myglobals is not None:
                HM = myglobals[modname]
            else:
                HM = ea_func_dictionary[func] 
        except:
            print(colored("\n\nYou might need to import %s" % modname, "red"))
            raise
        if myglobals is not None:
            H = getattr(HM, func_name)
        else:
            H = HM
    else:
        if myglobals is not None:
            H = globals()[func]
        else:
            H = ea_func_dictionary[func]
    args = []
    kwargs = {}
    for j in range(a[idx][1]):
        args.append(D['F' + str(idx) + 'ARG' + str(j)])
    for sa in a[idx][0]:
        key, value = sa.split('=')
        kwargs[key] = value
    temp = H(*args, **kwargs)
    D.insert(ii, n[idx].upper(), temp)
    for j in range(a[idx][1]):
        D.drop('F' + str(idx) + 'ARG' + str(j), 1, inplace=True)
    return D














