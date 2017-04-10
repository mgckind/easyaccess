import sys
import os
try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

prjdir = os.path.dirname(__file__)
__version__ = ''


def read(filename):
    return open(os.path.join(prjdir, filename)).read()


exec(open('easyaccess/version.py').read())


if sys.argv[-1] == 'publish':
    os.system("python setup.py sdist upload")
    os.system("python setup.py bdist_wheel --universal upload ")
    print("You probably want to also tag the version now:")
    print("  git tag -a %s -m 'version %s'" % (__version__, __version__))
    print("  git push --tags")
    sys.exit()

extra_link_args = []
libraries = []
library_dirs = []
include_dirs = []
try:
    pkgs = find_packages()
except NameError:
    pkgs = ['easyaccess', 'easyaccess.eautils', 'tests']
setup(
    name='easyaccess',
    version=__version__,
    author='Matias Carrasco Kind',
    author_email='mcarras2@illinois.edu',
    scripts=['bin/easyaccess'],
    packages=pkgs,
    license='LICENSE.txt',
    description='Easy access to the DES DB. Enhanced command line SQL interpreter client for DES',
    long_description=read('README.md'),
    url='https://github.com/mgckind/easyaccess',
    install_requires=['pandas >= 0.14', 'termcolor', 'fitsio >= 0.9.6', 'setuptools',
                      'cx_Oracle', 'numpy', 'future >= 0.15.0', 'requests'],
)
