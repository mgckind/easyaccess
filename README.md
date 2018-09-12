# easyaccess <a href="https://github.com/mgckind/easyaccess/releases/tag/1.4.4"> <img src="https://img.shields.io/badge/release-v1.4.4-blue.svg" alt="latest release" /></a> <a href="https://github.com/mgckind/easyaccess/blob/master/LICENSE.txt"><img src="https://img.shields.io/badge/license-NCSA%20License-blue.svg" alt="License" /> </a> <a href="https://pypi.python.org/pypi/easyaccess/1.4.4"><img src="https://img.shields.io/badge/pypi-v1.4.4-orange.svg" alt="pypi version"/></a><a href="https://anaconda.org/mgckind/easyaccess"> <img src="https://img.shields.io/badge/Anaconda Cloud-v1.4.4-blue.svg" /> </a>
![help_screen](data/help.gif)

Enhanced command line SQL interpreter client for astronomical databases.

Python Command Line Interpreter to access Oracle DES DB
using cx_Oracle

For a short tutorial check [here](http://matias-ck.com/easyaccess)

**Current version = 1.4.4**

## Requirements

- Oracle Client > 11g.2 (External library, no python)
  Check [here](https://opensource.ncsa.illinois.edu/confluence/display/DESDM/Instructions+for+installing+Oracle+client+and+easyaccess+without+EUPS) for instructions on how to install these libraries
- [cx_Oracle](https://bitbucket.org/anthony_tuininga/cx_oracle)

  Note that cx_Oracle needs libaio on some Linux systems (e.g., #98)

  Note that cx_Oracle needs libbz2 on some Linux systems
- [fitsio](https://github.com/esheldon/fitsio) >= 0.9.6
- [pandas](http://pandas.pydata.org/) >= 0.14
- [termcolor](https://pypi.python.org/pypi/termcolor)
- [PyTables](http://pytables.github.io/) (optional, for hdf5 output)
- [future](http://python-future.org/) (for python 2/3 compatibility)
- [requests](http://docs.python-requests.org/en/master/)
- [gnureadline](https://github.com/ludwigschwardt/python-gnureadline) (optional, for better console behavior in OS X)
- importlib (This is only needed if running python 2.6)


## Some *nice* features
- Nice output format (using pandas)
- Very flexible configuration
- Smart tab autocompletion for commands, table names, column names, and file paths
- Write output results to CSV, TAB, FITS, or HDF5 files
- Load tables from CSV, FITS or HDF5 files directly into DB (memory friendly by using number of rows or memory limit)
- Intrinsic DB commands to describe tables, schema, quota, and more
- Easyaccess can be imported as module from Python with a complete Python API
- Run commands directly from command line
- Load SQL queries from a file and/or from the editor
- Show the execution plan of a query if needed
- Python functions can be run in a inline query

## Conda installation
Now easyaccess can be installed using [conda](http://conda.pydata.org/docs/install/quick.html) out of the box!

    conda install easyaccess==1.4.3 -c mgckind

## Pip installation
easyaccess can also be installed using `pip` but it'd require the installation of the oracle instant client first

    pip install easyaccess==1.4.3

## Interactive interpreter

Assuming that ```easyaccess``` is in your path, you can enter the interactive interpreter by calling ```easyaccess``` without any command line arguments:

        easyaccess

### Running SQL commands
Once inside the interpreter run SQL queries by adding a ";" at the end::

        DESDB ~> select ... from ... where ... ;

To save the results into a table add ">" after the end of the query (after ";") and namefile at the end of line

        DESDB ~> select ... from ... where ... ; > test.fits

The file types supported so far are: .csv, .tab, .fits, and .h5. Any other extension is ignored.

### Load tables
To load a table it needs to be in a csv format with columns names in the first row
the name of the table is taken from filename or with optional argument --tablename

        DESDB ~> load_table <filename> --tablename <mytable> --chunksize <number of rows to read/upload> --memsize <memory in MB to read at a time>

The --chunsize and --memsize are optional arguments to facilitate uploading big files.

### Load SQL queries
To load SQL queries just run:

        DESDB ~> loadsql <filename.sql>
or

        DESDB ~> @filename.sql

The query format is the same as the interpreter, SQL statement must end with ";" and to write output files the query must be followed by " > <output file>"

### Configuration

The configuration file is located at ```$HOME/.easyaccess/config.ini``` but everything can be configured from inside easyaccess type:

        DESDB ~> help config

to see the meanings of all the options, and:

        DESDB ~> config all show

to see the current values, to modify one value, e.g., the prefetch value

        DESDB ~> config prefetch set 50000

and to see any particular option (e.g., timeout):

        DESDB ~> config timeout show

## Command line usage

Much of the functionality provided through the interpreter is also available directly from the command line. To see a list of command-line options, use the ```--help``` option

        easyaccess --help
