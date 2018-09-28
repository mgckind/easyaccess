---
title: 'easyaccess: Enhanced SQL command line interpreter for astronomical surveys'
tags:
  - Python
  - Astronomy
  - SQL
  - Surveys
authors:
  - name: Matias Carrasco Kind
    orcid: 0000-0002-4802-3194
    affiliation: 1
  - name: Alex Drlica-Wagner
    orcid: 0000-0001-8251-933X
    affiliation: 2
  - name: Audrey Koziol
    orcid: 0000-0001-8234-2116
    affiliation: 1
  - name: Don Petravick
    orcid: 0000-0002-3685-2497
    affiliation: 1
affiliations:
  - name: National Center for Supercomputing Applications, University of Illinois at Urbana-Champaign. 1205 W Clark St, Urbana, IL USA 61801
    index: 1
  - name: Fermi National Accelerator Laboratory, P. O. Box 500, Batavia,IL 60510, USA
    index: 2
date: 27 Sep 2018
bibliography: paper.bib
---


# Summary

`easyaccess` is an enhanced command line interpreter and Python package created to facilitate access to astronomical catalogs stored in SQL Databases. It provides a custom interface with custom commands and was specifically designed to access data from the [Dark Energy Survey](https://www.darkenergysurvey.org/) Oracle database, although gerenerally, it can easily be adapted to extend to another survey or SQL database. The package was completely written in [Python](https://www.python.org/) and support customized addition of commands and functionalities.
Visit [https://github.com/mgckind/easyaccess](https://github.com/mgckind/easyaccess) to view installation instructions, tutorials, and the Python source code for `easyaccess`.

# Dark Energy Survey

The Dark Energy Survey (DES) [@DES2005; @DES2016] is an international, collaborative effort of over 500 scientists from 26 institutions in seven countries. One of the main DES the goals is to map hundreds of millions of galaxies, detect thousands of supernovae, and find patterns in the large-scale structure within the cosmic web with the objective to reveal the nature of the mysterious dark matter and dark energy that is accelerating the expansion of our Universe. Survey operations of the Southern skies began on on August 31, 2013, and will conclude early 2019. For about 500 nights, DES has been taking thousands of images of the deep sky which are transferred and processed at the National Center for Supercomputing Applications ([NCSA](http://www.ncsa.illinois.edu/)) where immense catalogs of sources and metadata are created with hundreds of millions of entries (billions in the case of individual detections), describing all sources found within the images as well as other relevant information about the data.
A significant subset of this data is, for now, only accessible to the DES collaboration but a significant sub-set was recently [made  public](https://des.ncsa.illinois.edu/releases/dr1) [@DR1] and can be accessed through several mechanisms including `easyaccess` and [web interfaces](https://des.ncsa.illinois.edu/easyweb/) which have `easyaccess` running in the backend. This public release includes information for almost 400M astrophysical sources and complementary tables to allow scientific analysis.

## DES users

The first release of `easyaccess` was on February 17th, 2015 and since then, over 300 users have used it to access the DES databases within the DES collaboration as shown in Figure 1. We note that the number of DES accounts is almost 800, but this is considering all users that had an account including those before the first released version. Recently in August 2018, with version 1.4.4 we added support for the public release and since then we have increased the number of public users.

![Number of user since first version](easyaccess_users.png)

# `easyaccess`

`easyaccess` is a command line interpreter which is heavily based in the [`cmd`](https://docs.python.org/3/library/cmd.html) Python core module and `termcolor` [@termcolor], at its core as well other external and open sourced libraries including NumPy [@NumPy], `pandas` [@pandas], `fitsio` [@fitsio] and `h5py` [@h5py] to handle and transform array data coming to or from the DB, `cx_Oracle` [@cxoracle] to handle the Oracle communication and `requests` [@requests] for external URL requests. Figure 2 shows an example of
the welcome screen as seen as a DES user.

![Welcome screenshot](easyaccess_welcome.png)

## Features

 `easyaccess` has a variety of features including a history of past commands and smart tab  auto-completion for commands, functions, columns, users, tables, and paths as being typed. Tables can be written directly into comma-separated-values (CSV) files (or white-space separated), FITS [@FITS], and HDF5 [@hdf5] files and an iterator is provided to avoid memory constraints when retrieving large tables. Tables can also be displayed on the screen and most of the formatting is done using `pandas`. Similarly, users with DB space can easily upload tables from any of the file format described above and share with other users. The uploading mechanism is done chunk-wise, allowing large tables to be loaded while keeping memory usage low.

 In addition, there are a variety of customized functions to search and describe the tables, search for users and user tables, check for quota, check the Oracle execution plan, and soon the ability to run asynchronous jobs through a dedicated server. There are dozens of other minor features that allow for a seamless experience while exploring and discovering data within the hundreds of tables inside the DB.

One can also load SQL queries from a file into the database, or run SQL queries inside the `easyaccess` python module in another IDE. Most of the features are also exposed through a Python API and can be run inside a Jupyter [@jupyter] notebook or similar tool along with the scientific code analysis.

While also using `easyaccess`, users can submit and request cutouts around specific positions or objects which are generated from the images. This allows better integration with other data services for a richer scientific workflow.

## Architecture

As complement information, we have added a simplified UML diagram, in Figure 3, of the `easyaccess` architecture with dependencies. Figure 3 shows only the different methods for a given class and the name of the file hosting a given class. The main class `easy_or()` inherits all methods from all different subclasses, making this model flexible and extendable to other surveys or databases. These methods are then converted to command line commands and functions that can be called inside `easyaccess`. Given that there are some DES specific functions, we have moved DES methods into a separate class `DesActions()`.

![`easyaccess` architecture diagram](classes_simple.png)

## Installation

To download easyaccess you can follow the provided options or clone the source code from the GitHub repository at [https://github.com/mgckind/easyaccess](https://github.com/mgckind/easyaccess).  We also provide other means to install `easyaccess` using the standard channels as described below.

- From source

    `python setup.py install`
- [conda](https://conda.io/docs/)

    `conda install easyaccess -c mgckind`

- [Docker](https://hub.docker.com/r/mgckind/easyaccess/)

    `docker pull mgckind/easyaccess`

- [pip](https://pypi.org/project/easyaccess/1.4.4/)

    `pip install easyaccess`


# Acknowledgments

The DES Data Management System is supported by the National Science Foundation under Grant NSF AST 07-15036 and NSF AST 08-13543

# References
