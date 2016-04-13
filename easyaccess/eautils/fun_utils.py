import numpy as np
import pandas as pd
import inspect
import re
import healpy as hp
from functools import wraps
import importlib
#import easyaccess as ea
#con = ea.connect()

def toeasyaccess(custom):
    @wraps(custom)
    def easy_function(*args, **kwargs):
        check = inspect.getargspec(custom)
        nargs = len(check.args)
        if check.defaults is not None:
            ndef = len(check.defaults)
        else:
            ndef = 0
        #print 'This function uses %d arguments' % (nargs-ndef)
        return custom(*args, **kwargs)
    temp = easy_function
    temp.in_easyaccess = True
    temp.__doc__ = 'EAF:'+custom.__doc__ 
            
    return temp
        
    
@toeasyaccess
def my_sum(a,b, min_value= None, max_value=None):
    """
    Sum two colums, if max_values is defined the values are clipped
    to that value
    """
    c = abs(a) + abs(b)
    if min_value is None: min_value = np.min(c)
    if max_value is None: max_value = np.max(c)
    return np.clip(c, float(min_value), float(max_value))


@toeasyaccess
def ang2hpix(ra,dec, nside=None, nest='False'):
    """
    Converts from ra and dec to Hpix index with a given nside and nested schema
    """
    phi = ra/180.*np.pi
    theta = (90.-dec)/180.*np.pi
    nside = int(nside)
    nest = nest.lower() in ("yes", "true", "t", "1")
    return hp.ang2pix(nside, theta, phi, nest)



def parseQ(query):
    entries=re.findall( '/\*p:(.*?)\*/', query)
    funs = None
    args = None
    names = None
    nf=0
    if len(entries) > 0:
        funs = []
        args = []
        names = []
        for e in entries:
            try:
                name=e.split()[e.split().index('as')+1]
            except:
                name=None
            temp = "".join(e.split())
            f = temp[:temp.find('(')]
            if name is None: name=f.lower()
            ar = temp[temp.find('(')+1:temp.find(')')]
            funs.append(f)  # f.lower()
            all_args = ar.split(',')
            positional=[]
            optional=[]
            new = []
            for a in all_args:
                if a.find('=') > -1 :
                    optional.append(a)
                    new.append(a)
                else:
                    positional.append(a)
            args.append([new,len(positional)])
            names.append(name)
            b=[j+' as F'+str(nf)+'arg'+str(i) for i,j in enumerate(positional)]
            query = query.replace('/*p:'+e+'*/', ",".join(b))
            nf+=1
    return query, funs, args, names




def updateDF(D, f, a, n, idx):
    ii = np.where(D.columns.values=='F'+str(idx)+'ARG0')[0][0]
    func = f[idx]
    if func.find('.') > -1 :
        mod_name, func_name = func.rsplit('.',1)
        mod = importlib.import_module(mod_name)
        H = getattr(mod, func_name)
        #modname,func_name = func.split('.')
        #HM = globals()[modname]
        #H = getattr(HM, func_name)
    else:
        H = globals()[func]
    args = []
    kwargs = {}
    for j in range(a[idx][1]):
        args.append(D['F'+str(idx)+'ARG'+str(j)])
    for sa in a[idx][0]:
        key,value = sa.split('=')
        kwargs[key] = value
    temp = H(*args,**kwargs)
    D.insert(ii,n[idx].upper(),temp)
    for j in range(a[idx][1]):
        D.drop('F'+str(idx)+'ARG'+str(j),1,inplace=True)
    
    
#Q = """
#SELECT /* just a comment */ t.hpix,t.ra,/*p:  My_Sum(t.ra,t.dec) as sum*/,
#t.dec, /*p:  ang2hpix(t.ra,t.dec,nside=16384, nest=True) as hpix2*/
#from y1a1_hpix t
#where rownum < 10"""


#Q2,f,a,n =parseQ(Q)
#D=con.query_to_pandas(Q2)
#print D
#for k in range(len(f)):
#    updateDF(D,f,a,n,k)
















