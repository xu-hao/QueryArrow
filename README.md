# QueryArrow

## Resources
Hao Xu, Ben Keller, Antoine de Torcy, Jason Coposky (2016) QueryArrow: Bidirectional Integration of Multiple Metadata Sources. 8th iRODS User Group Meeting, University of North Carolina at Chapel Hill. June 2016.

Slides: https://irods.org/uploads/2016/06/queryarrow2_Hao-Xu_DICE_iRODS-UGM-2016.pdf

Technical Report: https://irods.org/uploads/2015/01/xu-queryarrow-2016.pdf

Specification: https://github.com/xu-hao/CertifiedQueryArrow

## Introduction

QueryArrow is motivated by the following applications: bidirectional metadata integration from different metadata sources, metadata policies, metadata migration from different databases, metadata indexing.

QueryArrow provides a systematic solution to shared namespace and unshared namespace federation of metadata. In particular, QueryArrow allows querying multiple zones and multiple data sources including NoSQL databases, and updating data sources. For data sources that support two-phase commit, QueryArrow also supports distributed transactions. QueryArrow also enables poly-fill for features that the underlying database does not support.

A QueryArrow instance includes a QueryArrow Service and QueryArrow plugins (QAPs). Each plugin provides interface with one data store. Currently, the implemented QAPs include

|        Name       |           Description          |
|:-----------------:|:------------------------------:|
|      Sum      |           aggregation          |
|  Translation  |         policy support         |
|     Cache     |             caching            |
|      Remote/TCP   |            remoting            |
| FileSystem | interfacing with file system |
| ElasticSearch/ElasticSearch | interfacing with ElasticSearch |
|     Cypher/Neo4j     |     interfacing with Neo4j     |
|   SQL/HDBC/PostgreSQL  |    interfacing with Postgres   |
|    SQL/HDBC/SQLite3    |    interfacing with SQLite3    |
|  SQL/HDBC/CockroachDB   |  interfacing with CockroachDB  |
|  Include   |  include other JSON files  |
|  InMemory/StateMap  |      in-memory mutable map     |
| InMemory/Map |     in-memory immutable map    |
|       InMemory/BuiltIn      |   built-in predicates, `eq`, `ne`, `like_regex`, `not_like_regex`, `le`, `ge`, `lt`, `gt`, `concat`, `strlen`, `add`, `sub`, `mul`, `div`, `mod`, `exp`, `sleep`, `encode`     |


The queries are issued from the client in the QueryArrow Language. QAL is a unified querying language for SQL and noSQL databases.

## How to build

### ubuntu 16.04

#### Install GHC 8.0.2

from source:

https://www.haskell.org/ghc/download_ghc_8_0_2#sources

Find out where ghc is installed.

If built from source, it is `<prefix>/lib/ghc-8.0.2/`. Default `<prefix>` is `/usr/local`.

#### Install Stack

Follow the instructions on this page:

https://docs.haskellstack.org

If you install from Ubuntu repo, make sure you run

    stack upgrade

#### Build QueryArrow

    apt-get install postgresql-server-dev-all libsqlite3-dev -y

    git clone http://github.com/xu-hao/QueryArrow

    cd QueryArrow

    git checkout development

    stack build

#### Create QueryArrow package

    cd ..

make a new directory

    mkdir build

    cd build

    ../QueryArrow/find_dependencies.sh ../QueryArrow

    cpack --config CPackConfig.cmake

#### Install QueryArrow package

    dpkg -i queryarrow-0.2-Linux-amd64.deb

## QueryArrow Configuration

By default QueryArrow Configuration files are stored in the `/etc/QueryArrow/tdb-plugin-gen-abs.json` file.

An exmaple is

~~~json
{
    "db_plugin" : {
        "db_config" : {
            "max_cc" : 1024,
            "cache_db_plugin": {
                "db_config" : {
                    "rewriting_file_path" : "../QueryArrow-gen/rewriting-plugin-gen.rules",
                    "include_file_path": ["../QueryArrow-gen"],
                    "trans_db_plugin": {
                        "db_config" : {
                            "summands" : [{
                                "db_config" : {
                                    "db_port" : 5432,
                                    "db_name" : "ICAT",
                                    "db_password" : "testpassword",
                                    "db_host" : "localhost",
                                    "db_username" : "irods",
                                    "db_predicates" : "../QueryArrow-gen/gen/ICATGen",
                                    "db_namespace" : "ICAT",
                                    "db_sql_mapping" : "../QueryArrow-gen/gen/SQL/ICATGen"
                                },
                                "qap_name" : "ICAT",
                                "catalog_database_type" : "SQL/HDBC/PostgreSQL"
                            }, {
                                "db_config" : {
                                    "fs_port" : 0,
                                    "db_namespace" : "FileSystem",
                                    "fs_host" : "",
                                    "fs_root" : ["/tmp"],
                                    "fs_hostmap" : [["", 0, "/tmp"]]
                                },
                                "db_name" : "",
                                "catalog_database_type" : "FileSystem"
                            }, {
                                "db_config" : {
                                    "db_namespace" : "BuiltIn",
                                },
                                "qap_name" : "",
                                "catalog_database_type" : "InMemory/BuiltIn"
                            }]
                        },
                        "qap_name" : "sum",
                        "catalog_database_type" : "Sum",
                    }
                },
                "qap_name" : "trans",
                "catalog_database_type" : "Translation"
            }
        },
        "qap_name" : "cache",
        "catalog_database_type" : "Cache"
    },
    "servers" :[{
        "server_protocol" : "service/tcp",
        "server_config" : {
            "tcp_server_addr" : "*",
            "tcp_server_port" : 12345
        }
    }]
}
~~~

## QueryArrow CLI

QueryArrow provides a CLI command `QueryArrow`.

## QueryArrow Server

QueryArrow provides server protocols. The service protocol is used for clients to communicate with QueryArrow. The remote protocol is used by the Remote QAP. The file system protocol is used by the FileSystem QAP

|       |unix domain socket|tcp socket|http|
|:-----:|:----------------:|:--------:|:--:|
|service| implemented| implemented | implemented |
|remote| implemented | implemented| |
|file system|  | implemented | |

The command for starting the server is `QueryArrowServer`.

## Build C++ plugin

In addition to running QueryArrow as a standalone service, QueryArrow provides an iRODS database plugin.

(to be updated: how to build irods plugin)
