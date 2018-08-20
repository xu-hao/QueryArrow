# install `postgres`

create user `data`

set password for `data` to `qap`

create database `pdb`

grant all privileges on `pdb` to user `data`

# install `neo4j`

create user `data` 


set password for `data` to `qap`

```
CALL dbms.security.createUser("data", "qap", false)
```

# run QueryArrow server
`fish`

```
docker run -v (pwd)/data:/queryarrow --net host --p 12345 --entrypoint QueryArrowServer queryarrow:0.1.0 /queryarrow/data.yaml
```

# run QueryArrow CLI

## insert data
`fish`
```
docker run -v (pwd)/data:/queryarrow --net host --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "insert COLL_OBJ(1) COLL_NAME(1,\"data\")" ""
docker run -v (pwd)/data:/queryarrow --net host --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "insert COLL_OBJ(2) COLL_NAME(2,\"data2\")" ""
docker run -v (pwd)/data:/queryarrow --net host --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "insert META_OBJ(1,\"metadata\")" ""
```
## query data
`fish`
```
docker run -v (pwd)/data:/queryarrow --net host --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "COLL_NAME(x,y)" "x y" --show-headers
docker run -v (pwd)/data:/queryarrow --net host --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "META_OBJ(x,y)" "x y" --show-headers
docker run -v (pwd)/data:/queryarrow --net host --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "COLL_NAME(x,y) META_OBJ(x,\"metadata\")" "x y" --show-headers
docker run -v (pwd)/data:/queryarrow --net host --entrypoint QueryArrow queryarrow:0.1.0 -c /queryarrow/data.yaml "COLL_NAME(x,\"data\") META_OBJ(x,y)" "x y" --show-headers
```
