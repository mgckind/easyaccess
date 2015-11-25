# Changes

## v1.2.1a
#### XXXX-XXX-XX
- Fixes a bug with python2/3 compatibility (see #36)

## v1.2.0
#### 2015-OCT-01
- Fixes several issues, like: #35, #34, #30, #26 #25 among others
- Does not clear screen after query
- Added a message when password has not been set in the last 200 days  (added extra columns to whoami as well)
- Fix problem with connection after entering a wrong query which used to hangs in some networks
- Bugfixes (show_index, change display setting without restarting, others)
- Added explain option to see the execution plan, i.e. DESDB ~> SELECT * FROM TABLE; < explain
- Added extra configuration option for column width (useful when printing large text columns)
- Added optional user/password from command line (using --user <user> and --password <password>)
- Added size info to mytables command
- Added append_tables option to append data to existing tables
- Minor changes in uploading tables
- **python 3 compatible**
- **change config.ini folder, from .easyacess/ to .easyaccess (typo)**
- move multiprocess import call depending on whether the loading bar is set

## v1.1.0
#### 2015-APR-28
- **Change pyfits to fitsio**
- Added autocommit as configuration option to commit changes or not by default (default = yes)
- Added timeout (1 min) to initial metadata cache loading 

## v1.0.8
#### 2015-APR-27 
- Fixed bug with single character for fits 
- Fix enconding issues (between ASCII and UTF-8)
- Loading bar termination issue fixed
- Added message after loading table for sharing tables
- Started migratrion from pyfits to fitsio
- Set max limit for output files in MB
- Add python API features for key commands and bugfix (All commnands can still be accessed from python API)

## v1.0.7
#### 2015-MAR-31 
- Null and NaN values fixed and customizable (config nullvalue set -9999) It will print 'Null' on the screen but nullvalue on the files
- Added a SQL syntax checker (<SQL query> ; < check)
- Reformatted help 

## v1.0.6
#### 2015-MAR-25 
- Background with Ctrl-Z, bg and fg 
- optional loading bar from config.ini file (default = yes)
- Connections trials added and error information when not connected
- print version at startup
- Number of rows received when running queries into a file
- Added command to change parameters from config.ini file from inside easyaccess (config)
- Fix issue with describe_table in latest DB schemas

## v1.0.5
#### 2015-MAR-20 
- version 1.0.4 was skipped to be in sync with pip
- Added optional color in config file (default = yes)
- Added quiet option for initialization (easyaccess -q)
- command to open an online tutorial (online_tutorial)

## v1.0.3
#### 2015-MAR-18 
- Fix an installation bug

## v1.0.2
#### 2015-MAR-18 
- Added DES Logo
- Loading bar
- Check whether des service file is read/write only by user, otherwise it change access mode
- Added comments from table to describe_table
- Added command to add comments to tables and columns (add_comment)
- Fix minor issues and improve formatting


## v1.0.1
#### 2015-FEB-20 
- Fix a bug at exit after error 
- Added a checker of DES_SERVICES files and prompt user/password when file doesn't exist
- Minor bugs

## v1.0.0
#### 2015-FEB-17
- release

