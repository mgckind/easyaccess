import sys
import os
#try:
#from setuptools import setup, find_packages
#except ImportError:
from distutils.core import setup
prjdir = os.path.dirname(__file__)

def read(filename):
    return open(os.path.join(prjdir, filename)).read()

extra_link_args = []
libraries = []
library_dirs = []
include_dirs = []
setup(
    name='easyaccess',
    version='1.2.0a',
    author='Matias Carrasco Kind',
    author_email='mcarras2@illinois.edu',
    scripts=['easyaccess'],
    py_modules=['easyaccess','config','eautils.des_logo','eautils.dircache'],
    #packages=find_packages(),
    packages=['eautils'],
    license='LICENSE.txt',
    description='Easy access to access DES DB. Command line interpreter client for DESDM',
    long_description=read('README.md'),
    url='https://github.com/mgckind/easyaccess',
    install_requires=['pandas >= 0.14','termcolor','fitsio >= 0.9.6','cx_Oracle'],
)
