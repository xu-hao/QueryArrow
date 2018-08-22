# install `postgres`

```
docker run --name postgres -e POSTGRES_PASSWORD=qap -e POSTGRES_USER=data -e POSTGRES_DB=pdb -d postgres
```

```
docker run -it --rm --link postgres:postgres postgres psql -h postgres -Udata -dpdb
```

```
create table r_coll_main (
    coll_id integer primary key,
    coll_name varchar
);
```

# install `neo4j`

```
docker run --publish=7474:7474 --publish=7687:7687 --name neo4j -d neo4j
```

```
CALL dbms.security.createUser("data", "qap", false)
```

# run QueryArrow server
`fish`

```
docker run -v (pwd)/data:/queryarrow --link postgres:postgres --link neo4j:neo4j --rm --name queryarrow -p 12345 --entrypoint QueryArrowServer queryarrow:0.1.0 /queryarrow/data.yaml
```

# run QueryArrow http server
`fish`

```
docker run -v (pwd)/data:/queryarrow --link postgres:postgres --link neo4j:neo4j --rm --name queryarrow -p 12345 --entrypoint QueryArrowServer queryarrow:0.1.0 /queryarrow/datahttp.yaml
```

# run QueryArrow CLI

## insert data
`fish`
```
docker run -v (pwd)/data:/queryarrow --link postgres:postgres --link neo4j:neo4j --rm --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "insert COLL_OBJ(1) COLL_NAME(1,\"data\")" ""
docker run -v (pwd)/data:/queryarrow --link postgres:postgres --link neo4j:neo4j --rm --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "insert COLL_OBJ(2) COLL_NAME(2,\"data2\")" ""
docker run -v (pwd)/data:/queryarrow --link postgres:postgres --link neo4j:neo4j --rm --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "insert META_OBJ(1,\"metadata\")" ""
```
## query data
`fish`
```
docker run -v (pwd)/data:/queryarrow --link postgres:postgres --link neo4j:neo4j --rm --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "COLL_NAME(x,y)" "x y" --show-headers
docker run -v (pwd)/data:/queryarrow --link postgres:postgres --link neo4j:neo4j --rm --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "META_OBJ(x,y)" "x y" --show-headers
docker run -v (pwd)/data:/queryarrow --link postgres:postgres --link neo4j:neo4j --rm --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "COLL_NAME(x,y) META_OBJ(x,\"metadata\")" "x y" --show-headers
docker run -v (pwd)/data:/queryarrow --link postgres:postgres --link neo4j:neo4j --rm --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "COLL_NAME(x,\"data\") META_OBJ(x,y)" "x y" --show-headers
```
