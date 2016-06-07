all: build

ubuntu:
	sudo apt-get install alex happy zlib1g-dev libsqlite3-dev postgresql-server-dev-all

cabal:
	cabal install --dependencies-only --enable-shared

cabalprof:
	cabal install --dependencies-only --enable-profiling --enable-shared

gen: src/*
	mkdir -p gen
	mkdir -p gen/SQL
	wget -O gen/schema.sql https://raw.githubusercontent.com/irods/irods/master/plugins/database/src/icatSysTables.sql.pp
	echo "create table R_META_ACCESS ( meta_id INT64TYPE not null, object_id INT64TYPE not null, user_id INT64TYPE not null, access_type_id INT64TYPE not null, create_ts varchar(32), modify_ts varchar(32)) ;" >> gen/schema.sql
	# cp src/schema.sql gen/
	runhaskell -isrc -igen src/schema_parser_main.hs

build: gen src/*
	mkdir -p build
	ghc -isrc -igen -outputdir build -o build/Main -O2 -fPIC -threaded src/Main.hs
	ghc -isrc -igen  -outputdir build -o /dev/null -O2 -fPIC -dynamic -shared src/SQL/HDBC/PostgreSQL.hs
	ghc -isrc -igen  -outputdir build -o /dev/null -O2 -fPIC -dynamic -shared src/SQL/HDBC/Sqlite3.hs
	ghc -isrc -igen  -outputdir build -o /dev/null -O2 -fPIC -dynamic -shared src/Cypher/Neo4j.hs
	ghc -isrc -igen -itest -outputdir build -o build/ZmqSend test/ZmqSend.hs

prof: gen src/*
	mkdir -p prof
	ghc -isrc -igen -outputdir prof -o prof/Main -O0 -prof -fprof-auto -rtsopts src/Main.hs
	ghc -isrc -igen -outputdir prof -o prof/Test -O0 -prof -fprof-auto -rtsopts test/Test.hs

hdbctest: gen src/* test/*
	mkdir -p hdbctest
	ghc -isrc -itest -igen -outputdir hdbctest -o hdbctest/PostgreSQLTest -O2 test/PostgreSQLTest.hs

coverage: gen src/* test/*
	if [ -e coverage ]; then rm -rf coverage; fi
	mkdir -p coverage
	cd coverage; ghc -fhpc -i../src  -i../gen -outputdir . -o Test ../test/Test.hs
	cd coverage; ./Test
	cd coverage; hpc markup Test --fun-entry-count

test: gen src/* test/* external
	runhaskell -isrc -igen test/Test.hs

clean:
	rm -rf gen
	rm -rf build
	rm -rf coverage
