import sys
import os
try:
    from setuptools import setup
except ImportError:
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
    version='1.0.2',
    author='Matias Carrasco Kind',
    author_email='mcarras2@illinois.edu',
    scripts=['easyaccess'],
    py_modules=['easyaccess','config','utils'],
    license='NCSA',
    description='Easy Access to access DES DB',
    long_description=read('README.md'),
    url='https://github.com/mgckind/easyaccess',
    install_requires=['numpy','pandas >= 0.14','termcolor','pyfits >= 3.3','cx_Oracle'],
)
