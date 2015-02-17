import sys
from numpy.distutils.core import setup, Extension
prjdir = os.path.dirname(__file__)

def read(filename):
    return open(os.path.join(prjdir, filename)).read()

extra_link_args = []
libraries = []
library_dirs = []
include_dirs = []
setup(
    name='easyaccess',
    version='1.0.0',
    author='Matias Carrasco Kind',
    author_email='mcarras2@illinois.edu',
    scripts=['easyA'],
    py_modules=['termcolor'],
    license='NCSA',
    description='Easy Access to access DES DB',
    long_description=read('README.md'),
    url='https://github.com/mgckind/easyaccess',
    install_requires=['pandas','termcolor','pyfits','cx_Oracle'],
)
