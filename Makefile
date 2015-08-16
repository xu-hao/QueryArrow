all: build

gen: src/*
	mkdir -p gen
	mkdir -p gen/SQL
	wget -O gen/schema.sql https://raw.githubusercontent.com/irods/irods/master/plugins/database/src/icatSysTables.sql.pp
	runhaskell -isrc -igen src/schema_parser_main.hs

build: gen src/* external
	mkdir -p build
	ghc -isrc -igen -outputdir build -o build/Main -fPIC src/Main.hs
	ghc -isrc -igen  -outputdir build -o /dev/null -fPIC -dynamic -shared src/SQL/HDBC/PostgreSQL.hs
	ghc -isrc -igen  -outputdir build -o /dev/null -fPIC -dynamic -shared src/SQL/HDBC/Sqlite3.hs
	ghc -isrc -igen  -outputdir build -o /dev/null -fPIC -dynamic -shared src/Cypher/Neo4j.hs

coverage: gen src/* test/* external
	if [ -e coverage ]; then rm -rf coverage; fi
	mkdir -p coverage
	cd coverage; ghc -fhpc -i../src  -i../gen -outputdir . -o Test ../test/Test.hs
	cd coverage; ./Test
	cd coverage; hpc markup Test --fun-entry-count

test: gen src/* test/* external
	runhaskell -isrc -igen test/Test.hs

ffi: gen src/* external
	mkdir -p ffi
	ghc -isrc  -igen -outputdir ffi -o ffi/libdb.so -fPIC -dynamic -shared src/FFI.hs
	ghc -isrc  -igen -outputdir ffi -o /dev/null -fPIC -dynamic -shared src/FO/Noop.hs
	ghc -isrc  -igen -outputdir ffi -o /dev/null -fPIC -dynamic -shared src/FO/E.hs
	ghc -isrc  -igen -outputdir ffi -o /dev/null -fPIC -dynamic -shared src/FO/CVC4.hs
	ghc -isrc  -igen -outputdir ffi -o /dev/null -fPIC -dynamic -shared src/SQL/HDBC/PostgreSQL.hs
	ghc -isrc  -igen -outputdir ffi -o /dev/null -fPIC -dynamic -shared src/SQL/HDBC/Sqlite3.hs
	ghc -isrc  -igen -outputdir ffi -o /dev/null -fPIC -dynamic -shared src/Cypher/Neo4j.hs
	ghc test/ffi.cpp -o ffi/main -I. -lstdc++ -ldb -optc-g -optl-Wl,-rpath,/usr/lib/ghc/hpc-0.6.0.0 -optl-Wl,-rpath,/usr/lib/ghc/hoopl-3.9.0.0 -optl-Wl,-rpath,/usr/lib/ghc/bin-package-db-0.0.0.0 -optl-Wl,-rpath,/usr/lib/ghc/binary-0.5.1.1 -optl-Wl,-rpath,ffi -Lffi -no-hs-main


external:
	mkdir -p external
	wget -O external/E.tgz http://wwwlehre.dhbw-stuttgart.de/~sschulz/WORK/E_DOWNLOAD/V_1.9/E.tgz
	cd external; tar zxvf E.tgz
	cd external/E; ./configure
	cd external/E; make
	mkdir -p external/CVC4
	wget -O external/CVC4/cvc4-1.4-x86_64-linux-opt http://cvc4.cs.nyu.edu/builds/x86_64-linux-opt/cvc4-1.4-x86_64-linux-opt
	chmod +x external/CVC4/cvc4-1.4-x86_64-linux-opt

clean:
	rm -rf gen
	rm -rf build
	rm -rf ffi
	rm -rf coverage
	rm -rf external
