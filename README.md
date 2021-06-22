# easyaccess <a href="https://github.com/des-labs/easyaccess/releases/tag/1.4.7"> <img src="https://img.shields.io/badge/release-v1.4.7-blue.svg" alt="latest release" /></a> <a href="https://github.com/des-labs/easyaccess/blob/master/LICENSE.txt"><img src="https://img.shields.io/badge/license-NCSA%20License-blue.svg" alt="License" /> </a>  ![](https://img.shields.io/conda/v/mgckind/easyaccess.svg) [![DOI](http://joss.theoj.org/papers/10.21105/joss.01022/status.svg)](https://doi.org/10.21105/joss.01022)

Enhanced command line SQL interpreter client for astronomical surveys.
![help_screen](data/help.gif)

## Description
`easyaccess` is an enhanced command line interpreter and Python package created to facilitate access to astronomical catalogs stored in SQL Databases. It provides a custom interface with custom commands and was specifically designed to access data from the Dark Energy Survey Oracle database, including autocompletion of tables, columns, users and commands, simple ways to upload and download tables using csv, fits and HDF5 formats, iterators, search and description of tables among others. It can easily be extended to another surveys or SQL databases. The package was completely written in Python and support customized addition of commands and functionalities.

For a short tutorial check [here](http://matias-ck.com/easyaccess)

**Current version = 1.4.7**

#### DES DR1/DR2 users
For DES public data release, you can start `easyaccess` with:

    easyaccess -s desdr

To create an account click [here](https://des.ncsa.illinois.edu/easyweb/signup/).

## Requirements

- [Oracle Client](https://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html) > 11g.2 (External library, no python)
  Check [here](https://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html) for instructions on how to install these libraries
- [cx_Oracle](https://cx-oracle.readthedocs.io/en/latest/index.html)
  - Note that cx_Oracle needs libaio on some Linux systems
  - Note that cx_Oracle needs libbz2 on some Linux systems
- [fitsio](https://github.com/esheldon/fitsio) == 1.0.5
- [pandas](http://pandas.pydata.org/) >= 0.14
- [numpy](https://docs.scipy.org/doc/numpy-1.15.1/reference/index.html)
- [termcolor](https://pypi.python.org/pypi/termcolor)
- [PyTables](http://pytables.github.io/) (optional, for hdf5 output)
- [future](http://python-future.org/) (for python 2/3 compatibility)
- [requests](http://docs.python-requests.org/en/master/)
- [gnureadline](https://github.com/ludwigschwardt/python-gnureadline) (optional, for better console behavior in OS X)

## Installation

Installing `easyaccess` can be a little bit tricky given the external libraries required, in particular the Oracle libraries which are free to use. If you are primarily interested in *using* the `easyaccess` client, we recommend building and running the Docker image as described below. 

### Docker

Building and running `easyaccess` in Docker is easy. Run the `docker build` command followed by a `docker run` command as shown here:

```
$ docker build -t easyaccessclient:latest .
$ docker run -it --rm easyaccessclient:latest easyaccess -s desdr

Enter username : 
Enter password : 
Connecting to DB ** desdr ** ...
Loading metadata into cache...
     _______      
     \      \      
  // / .    .\    
 // /   .    _\   
// /  .     / // 
\\ \     . / //  
 \\ \_____/ //   
  \\_______//    DARK ENERGY SURVEY
   `-------`     DATA MANAGEMENT

easyaccess 1.4.8-dev. The DESDM Database shell.

_________
DESDR ~> SELECT RA, DEC, MAG_AUTO_G, TILENAME FROM DR2_MAIN sample(0.001) FETCH FIRST 5 ROWS ONLY ;


          RA        DEC  MAG_AUTO_G      TILENAME
1  13.142238 -43.729656   21.594194  DES0053-4331
2  13.023884 -58.278531   24.332121  DES0053-5831
3  13.103845 -62.469223   23.710896  DES0053-6248
4  13.057452 -18.648209   27.200878  DES0051-1832
5  13.095345 -38.464359   25.606516  DES0050-3832

```

### Source Installation

`easyaccess` is based heavily on the Oracle python client `cx_Oracle`, you can follow the installation instructions from [here](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html#quick-start-cx-oracle-installation). For `cx_Oracle` to work, you will need the Oracle Instant Client packages which can be obtained from [here](https://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html).

Make sure you have these libraries installed before proceeding to the installation of easyaccess, you can try by opening a Python interpreter and type:

    import cx_Oracle

If you have issues, please check the [Troubleshooting page](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html#troubleshooting) or our [FAQ page](FAQ.md).

You can clone this repository and install `easyaccess` with:

    python setup.py install

## FAQ
We have a running list of [FAQ](FAQ.md) which we will constantly update, please check [here](FAQ.md).

## Contributing
Please take a look st our [Code of Conduct](CODE_OF_CONDUCT.md) and or [contribution guide](CONTRIBUTING.md).


## Citation
If you use `easyaccess` in your work we would encourage to use this reference [https://arxiv.org/abs/1810.02721](https://arxiv.org/abs/1810.02721) or copy/paste this BibTeX:
```
@ARTICLE{2018arXiv181002721C,
       author = {{Carrasco Kind}, M. and {Drlica-Wagner}, A. and {Koziol}, A.~M.~G. and
        {Petravick}, D.},
        title = "{easyaccess: Enhanced SQL command line interpreter for astronomical surveys}",
      journal = {arXiv e-prints},
     keywords = {Astrophysics - Instrumentation and Methods for Astrophysics},
         year = 2018,
        month = Oct,
          eid = {arXiv:1810.02721},
        pages = {arXiv:1810.02721},
archivePrefix = {arXiv},
       eprint = {1810.02721},
 primaryClass = {astro-ph.IM},
       adsurl = {https://ui.adsabs.harvard.edu/\#abs/2018arXiv181002721C},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
```


## Usage

For a short tutorial and documentation see [here](http://matias-ck.com/easyaccess), note that not all the features are available for the public use, i.e., DR1 users.

### Some *great* features
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


#### Interactive interpreter

Assuming that ```easyaccess``` is in your path, you can enter the interactive interpreter by calling ```easyaccess``` without any command line arguments:

        easyaccess

#### Running SQL commands
Once inside the interpreter run SQL queries by adding a ";" at the end::

        DESDB ~> select ... from ... where ... ;

To save the results into a table add ">" after the end of the query (after ";") and namefile at the end of line

        DESDB ~> select ... from ... where ... ; > test.fits

The file types supported so far are: .csv, .tab, .fits, and .h5. Any other extension is ignored.

#### Load tables
To load a table it needs to be in a csv format with columns names in the first row
the name of the table is taken from filename or with optional argument --tablename

        DESDB ~> load_table <filename> --tablename <mytable> --chunksize <number of rows to read/upload> --memsize <memory in MB to read at a time>

The --chunsize and --memsize are optional arguments to facilitate uploading big files.

#### Load SQL queries
To load SQL queries just run:

        DESDB ~> loadsql <filename.sql>
or

        DESDB ~> @filename.sql

The query format is the same as the interpreter, SQL statement must end with ";" and to write output files the query must be followed by " > <output file>"

#### Configuration

The configuration file is located at ```$HOME/.easyaccess/config.ini``` but everything can be configured from inside easyaccess type:

        DESDB ~> help config

to see the meanings of all the options, and:

        DESDB ~> config all show

to see the current values, to modify one value, e.g., the prefetch value

        DESDB ~> config prefetch set 50000

and to see any particular option (e.g., timeout):

        DESDB ~> config timeout show

#### Command line usage

Much of the functionality provided through the interpreter is also available directly from the command line. To see a list of command-line options, use the ```--help``` option

        easyaccess --help

## Architecture

We have included a simplified UML diagram describing the architecture and dependencies of `easyaccess` which shows only the different methods for a given class and the name of the file hosting a given class. The main class, `easy_or()`, inherits all methods from all different subclasses, making this model flexible and extendable to other surveys or databases. These methods are then converted to command line commands and functions that can be called inside `easyaccess`. Given that there are some DES specific functions, we have moved DES methods into a separate class `DesActions()`.

![`easyaccess` architecture diagram](paper/classes_simple.png)
