# QueryArrow

## Resources
Hao Xu, Ben Keller, Antoine de Torcy, Jason Coposky (2016) QueryArrow: Bidirectional Integration of Multiple Metadata Sources. 8th iRODS User Group Meeting, University of North Carolina at Chapel Hill. June 2016.

Slides: https://irods.org/uploads/2016/06/queryarrow2_Hao-Xu_DICE_iRODS-UGM-2016.pdf

Technical Report: https://irods.org/uploads/2015/01/xu-queryarrow-2016.pdf

Specification: https://github.com/xu-hao/CertifiedQueryArrow

## Introduction

QueryArrow is motivated by the following applications: bidirectional metadata integration from different metadata sources, metadata policies, metadata migration from different databases, metadata indexing.

QueryArrow provides a systematic solution to shared namespace and unshared namespace federation of metadata. In particular, QueryArrow allows querying multiple multiple data sources including NoSQL databases, and updating data sources. For data sources that support two-phase commit, QueryArrow also supports distributed transactions. QueryArrow also enables poly-fill for features that the underlying database does not support.

A QueryArrow instance includes a QueryArrow Service and QueryArrow plugins (QAPs). Each plugin provides interface with one data store.
The queries are issued from the client in the QueryArrow Language (QAL). QAL is a unified query and update language for SQL and NoSQL databases.

## How to build

### Ubuntu 16.04 and CentOS 7

#### Install GHC 8.0.2

From source:

https://www.haskell.org/ghc/download_ghc_8_0_2#sources

Find out where ghc is installed.

If built from source, it is `<prefix>/lib/ghc-8.0.2/`. Default `<prefix>` is `/usr/local`.

#### Install Stack

Follow the instructions on this page:

https://docs.haskellstack.org

If you install from system repo, make sure you run

    stack upgrade

#### Install Packages

On `Ubuntu`

    apt-get install postgresql-server-dev-all libsqlite3-dev -y

On `CentOS`

    yum install postgresql-devel sqlite3-devel -y

#### Build QueryArrow

    git clone http://github.com/xu-hao/QueryArrow

    cd QueryArrow

    stack build

#### Create QueryArrow Package

    cd ..

Make a new directory

    mkdir build

    cd build

    ../QueryArrow/find_dependencies.sh ../QueryArrow

    cpack --config CPackConfig.cmake

#### Install QueryArrow Package

On `Ubuntu`

    dpkg -i queryarrow-0.2-Linux-amd64.deb

On `CentOS`

    yum install queryarrow-0.2-Linux-amd64.rpm

## QueryArrow Configuration

By default QueryArrow Configuration files are stored in the `/etc/QueryArrow/tdb-plugin-gen-abs.yaml` file.

An example is

~~~yaml
db_plugin:
  qap_name: cache
  catalog_database_type: Cache
  db_config:
    max_cc: 1024
    cache_db_plugin:
      qap_name: trans
      catalog_database_type: Translation
      db_config:
        rewriting_file_path: "../QueryArrow-gen/rewriting-plugin-gen.rules"
        include_file_path:
        - "../QueryArrow-gen"
        trans_db_plugin:
          qap_name: sum
          catalog_database_type: Sum
          db_config:
            summands:
            - qap_name: ICAT
              catalog_database_type: SQL/HDBC/PostgreSQL
              db_config:
                db_port: 5432
                db_name: ICAT
                db_password: testpassword
                db_host: localhost
                db_username: irods
                db_predicates: "../QueryArrow-gen/gen/ICATGen"
                db_namespace: ICAT
                db_sql_mapping: "../QueryArrow-gen/gen/SQL/ICATGen"
            - qap_name: ''
              catalog_database_type: FileSystem
              db_config:
                fs_port: 0
                db_namespace: FileSystem
                fs_host: ''
                fs_root: "/tmp"
                fs_hostmap:
                - - ''
                  - 0
                  - "/tmp"
            - qap_name: ''
              catalog_database_type: InMemory/BuiltIn
              db_config:
                db_namespace: BuiltIn
servers:
- server_protocol: service/tcp
  server_config:
    tcp_server_addr: "*"
    tcp_server_port: 12345
~~~


## QueryArrow Plugin

Currently, the implemented QAPs include

|        Name       |           Description          | `db_config`       |
|:-----------------:|:------------------------------:|:------------:|
|      Sum      |           aggregation          |`summands`, `db_namespace`|
|  Translation  |         policy support         |`rewriting_file_path`, `include_file_path`,`trans_db_plugin`|
|     Cache     |             caching            |`max_cc`, `cache_db_plugin`|
|      Remote/TCP   |            remoting            |`db_host`, `db_port`|
| FileSystem | interfacing with file system | `fs_host`, `fs_port`, `fs_root`, `fs_hostmap`, `db_namespace`|
| ElasticSearch/ElasticSearch | interfacing with ElasticSearch | `db_name`, `db_namespace`, `db_predicates`, `db_sql_mapping`, `db_host`, `db_port`, `db_username`, `db_password`|
|     Cypher/Neo4j     |     interfacing with Neo4j     |`db_namespace`, `db_predicates`, `db_sql_mapping`, `db_host`, `db_port`, `db_username`, `db_password`|
|   SQL/HDBC/PostgreSQL  |    interfacing with Postgres   |`db_name`, `db_namespace`, `db_predicates`, `db_sql_mapping`, `db_host`, `db_port`, `db_username`, `db_password`|
|    SQL/HDBC/SQLite3    |    interfacing with SQLite3    |`db_file_path`, `db_namespace`, `db_predicates`, `db_sql_mapping`|
|  SQL/HDBC/CockroachDB   |  interfacing with CockroachDB  |`db_name`, `db_namespace`, `db_predicates`, `db_sql_mapping`, `db_host`, `db_port`, `db_username`, `db_password`|
|  Include   |  include other config files  |`include`|
|  InMemory/StateMap  |      in-memory mutable map     |`db_namespace`,`predicate_name`,`db_map`|
| InMemory/Map |     in-memory immutable map    |`db_namespace`,`predicate_name`,`db_map`|
|       InMemory/BuiltIn      |   built-in predicates: `like_regex`, `not_like_regex`, `eq`, `ne`, `le`, `ge`, `lt`, `gt`, `concat`, `substr`, `strlen`, `add`, `sub`, `mul`, `div`, `mod`, `exp`, `like`, `not_like`, `in`, `replace`, `regex_replace`, `sleep`, `encode`     |`db_namespace`|


## QueryArrow CLI

QueryArrow provides a CLI command `QueryArrow`.

## QueryArrow Server

QueryArrow provides server protocols. The service protocol is used for clients to communicate with QueryArrow. The remote protocol is used by the Remote QAP. The file system protocol is used by the FileSystem QAP.

|       |unix domain socket|tcp socket|http|
|:-----:|:----------------:|:--------:|:--:|
|service| implemented | implemented | implemented |
|remote| implemented | implemented| |
|file system|  | implemented | |

|  Service     |`server_config`|
|:-----:|:----------------:|
|service/tcp, remote/tcp | `tcp_server_addr`, `tcp_server_port` |
|service/http|`http_server_port`|
|service/unix domain socket, remote/unix domain socket| `uds_server_addr` |
|file system/tcp| `fs_server_port`, `fs_server_root`, `fs_server_addr`|

The command for starting the server is `QueryArrowServer`.

## iRODS Database Plugin

In addition to running QueryArrow as a standalone service, QueryArrow provides an iRODS database plugin. To build it, follow instructions for building iRODS database plugins.
