# Changes


## v1.4.4
#### 2018-XXX-XX
- Improved version to work with public release DB
- Update dtypes to match Y3 tables (PR #149)
- Fix bytes to unicode issue for py3 when writing h5 files
- Fix bug with Oracle types NUMBER with no scale/digits
- Add desdr database and remove oldoper
- Fix HDF5 output files, (see #140)
- Add compression option to configuration, gzip is applied to .csv,.tab,.fits (latter only for 1 DB trip), bzip2 for hdf5
  fits can be opened in append mode when they are compressed, workaround is to increase prefetch

## v1.4.3
#### 2017-AUG-15
- Reduce the number of tries for connection
- Add options to connect to new desoper and remove oldsci, passwords not longer linked (PR #136)
- Allow `describe_table` to work for materialized views (see #135)

## v1.4.2
#### 2017-MAY-23
- Fix a minor migration bug from version 1.4.1
- Add option to reset password after expiration (see #125)
- Add support for new databases (see #126)
- Update tutorial link
- Fix minor bugs and update commands (see #119 #120)
- Fix bugs regarding cx_Oracle (see #117)

## v1.4.0
#### 2017-APR-16
- Refactor and huge clean up, now bin/ folder has the startup script (see PR #113 and #111)
- Fix metadata and cache lookup for autocompletion (see #92, #94, #93)
- Add estimate number of rows on describe_table when available (see #75)
- Toggle color mode without exiting interpreter (see PR #110)
- 80 character loading bar (see PR #103)
- Added more information to help command (see PR #100 and #109)
- Fix config option and boolean values (see Pr #115)
- Justify comments to the left in describe_table i(#105)
- Added `change_db` option switch between dabatases without logging out (see PR #90 , #86)
- Fixes authentication and des service files
- Added api to access descut services directly (see PR #87)
- Fix readline issues related with some OS X (see #88 and #112)


## v1.3.1
#### 2016-MAY-24
- Adds optional purge to drop_table (see PR #74)
- Fix a bug setting prefetch parameters using config set #76
- Improve import functionality for inline queries (see PR #78)
- Add option to upload files in chunk of memory (in addition to the chunk by rows), using --memsize option.
  This calculates an approximate number of rows to upload on each trip, avoiding memory issues. (see PR #79)

## v1.3.0
#### 2016-MAY-09
- Fixes a bug with python2/3 compatibility (see Pull-request #36)
- Removes ; from query for python API (query_* functions) issue #37
- Added execproc to run sql/pl procedures, see help execproc for more information (see PR #48)
- Fix show_index and describe_table commands and queries (no more repeated rows) (see PR #42 and #43)
- Case insensitive options (PR #45)
- Reorganization of the structure of the code, this way is much simpler to develop (PR #49)
- Conda installation
- In-query python functions (beta) write your own function and call it from query#67
- Query and execution information in FITS header #50
- Fix bug when loading sql from file #55
- Dealing better with trailing white space #52
- Fix several minor issues: #37, #40
- configuration options at command line (reset and configuration per session) with --config
- Improved python API , added some extra functions (pandas_to_db) and imprived docs#68
- Load big files in chunks -- very useful to load big files in chunks without running into memory issues #66
- Fix find_tables (to include owners) and describe_table (data_type) commands
- Added unittest suites #65 #70
- Added --no_refresh option in command line to quick start up


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
