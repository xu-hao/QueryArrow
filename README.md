# QueryArrow

QueryArrow is motivated by the following applications: bidirectional metadata integration from different metadata sources, metadata policies, metadata migration from different databases, metadata indexing.

QueryArrow provides a systematic solution to shared namespace and unshared namespace federation of metadata. In particular, QueryArrow allows querying multiple zones and multiple data sources including NoSQL databases, and updating data sources. For data sources that support two-phase commit, QueryArrow also supports distributed transactions. QueryArrow also enables poly-fill for features that the underlying database does not support.

A QueryArrow instance includes a QueryArrow Service and QueryArrow plugins. Each plugin provides interface with one database. Currently, the implemented databases include PostgreSQL, SQLite, Neo4j, ElasticSearch, and various in-memory data structures. The message protocol is defined in JSONRPC. The queries are issued from the client in the QueryArrow Language. QAL is a unified querying language for SQL and noSQL databases.

Slides: http://irods.org/wp-content/uploads/2016/06/queryarrow2_Hao-Xu_DICE_iRODS-UGM-2016.pdf

# How to build

## ubuntu 16.04

### Install GHC 8.0.1

from source:

https://www.haskell.org/ghc/download_ghc_8_0_1#sources

install debian package

https://packages.debian.org/sid/amd64/ghc/download

Find out where ghc is installed.

If built from source, it is `<prefix>/lib/ghc-8.0.1/`. Default `<prefix>` is `/usr/local`.

If installed debian package, it is `/usr/lib/ghc`.

### Build QueryArrow

    git clone http://github.com/xu-hao/QueryArrow

    cd QueryArrow

    git checkout development

    stack build

### Create QueryArrow package

    cd ..

make a new directory

    mkdir build

    cd build

    ../QueryArrow/find_dependencies.sh ../QueryArrow

    cpack --config CPackConfig.cmake

### Install QueryArrow package

    dpkg -i queryarrow-0.2-Linux-amd64.deb

### Build C++ plugin

build irods
