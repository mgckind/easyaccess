# easyaccess
refactored version of trivialAccess

Python Command Line Interpreter to access Oracle DES DB
using cx_Oracle 

## Requirements

- Oracle Client > 11g.2
- cx_Oracle
- pyfits
- pandas
- termcolor
- PyTables (for hdf5 output)

## Some *nice* features
- Smart tab completion for commands, table names, column names and file paths accordingly
- write output results to csv, tab, fits files or HDF5 files
- load tables from csv directly into DB
- intrinsic db commands to describe tables, own schema, quota and more
- It can be imported as module
- Can run command directly from command line
- Load sql query from file and/or from editor

## To do

- load tables from fits file and hdf5 files
- command for self upgrade

## Basic use

### Running SQL commands
Once inside the interpreter run SQL queries by adding a ; at the end::

        DESDB ~> select ... from ... where ... ;

To save the results into a table add ">" after the end of the query (after ";") and namefile at the end of line

        DESDB ~> select ... from ... where ... ; > test.fits


