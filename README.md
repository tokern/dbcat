[![CircleCI](https://circleci.com/gh/tokern/dbcat.svg?style=svg)](https://circleci.com/gh/tokern/dbcat)
[![codecov](https://codecov.io/gh/tokern/dbcat/branch/main/graph/badge.svg)](https://codecov.io/gh/tokern/dbcat)
[![PyPI](https://img.shields.io/pypi/v/dbcat.svg)](https://pypi.python.org/pypi/dbcat)
[![image](https://img.shields.io/pypi/l/dbcat.svg)](https://pypi.org/project/dbcat/)
[![image](https://img.shields.io/pypi/pyversions/dbcat.svg)](https://pypi.org/project/dbcat/)

# Data Catalog for Databases and Data Warehouses

## Overview

*dbcat* builds and maintains metadata from all your databases and data warehouses. 
*dbcat* is simple to use and maintain. Build a data catalog in minutes by providing
credentials using a command line application or API. Automate collection of metadata using
cron or other workflow automation tools.

*dbcat* stores the catalog in a Postgresql database. Use cloud hosting platforms to ease 
operations in maintaining the catalog in a Postgresql database. 

Access the catalog using raw sql or the python APIs provided by *dbcat* in your python
application.

## Quick Start

*dbcat* is distributed as a python application.

    python3 -m venv .env
    source .env/bin/activate
    pip install piicatcher

    # configure the application
    
    dbcat -c <config dir> pull

## Supported Technologies

The following databases are supported:

* MySQL/Mariadb
* PostgreSQL
* AWS Redshift
* BigQuery
* Snowflake
* AWS Glue

