
    a
    a
    a
# easyaccess
refactored version of trivialAccess for accessing the DB

Python Command Line Interpreter to access Oracle DES DB
using cx_Oracle 

For a short tutorial chek [here](http://deslogin.cosmology.illinois.edu/~mcarras2/data/DESDM.html)
(Using des credentials)

## Requirements

- Oracle Client > 11g.2 (External library, no python)
- cx_Oracle
- pyfits >= 3.3
- pandas >= 0.14
- termcolor
- PyTables (for hdf5 output)

## Some *nice* features
- Smart tab completion for commands, table names, column names and file paths accordingly
- write output results to csv, tab, fits files or HDF5 files
- load tables from csv or fits directly into DB
- intrinsic db commands to describe tables, own schema, quota and more
- It can be imported as module
- Can run command directly from command line
- Load sql query from file and/or from editor
- Many more

## Basic use

### Running SQL commands
Once inside the interpreter run SQL queries by adding a ; at the end::

        DESDB ~> select ... from ... where ... ;

To save the results into a table add ">" after the end of the query (after ";") and namefile at the end of line

        DESDB ~> select ... from ... where ... ; > test.fits

The files supported so far are (.csv, .tab, .fits, .h5) any other extension is ignored

### Load tables
To load a table it needs to be in a csv format with columns names in the first row
the name of the table is taken from filename


        DESDB ~> load_table <filename>

### Load SQL queries
To load sql queries just run:

        DESDB ~> loadsql <filename.sql>
or

        DESDB ~> @filename.sql

The format is the same as in command line, SQL statement must end with ;
and to write output files it must be followed by > <output file>

### TODO
    - There is a bug with some versions of readline
    - Other small changes when loading tables
    - Self-upgrade
