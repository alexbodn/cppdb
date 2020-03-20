#!/bin/bash

run_test()
{
	if $1 "$2" &>report.txt
	then
		echo "Passed: $1 $2"
	else
		echo "Failed: $1 $2"
		cat report.txt >>fail.txt
	fi
	cat report.txt >>all.txt
}


rm -f all.txt fail.txt

# This is not supported by freetds driver
#	'odbc:DSN=MSSQL;UID=root;PWD=rootroot;@engine=mssql' \
#	'odbc:DSN=MSSQL;UID=root;PWD=rootroot;@engine=mssql;@utf=wide' \

for STR in \
	'postgresql:dbname=test' \
	'postgresql:dbname=test;@blob=bytea' \
	'sqlite3:db=/tmp/test.db' \
	'mysql:database=test' \
	'odbc:Driver=PostgreSQL ANSI;Database=test;@engine=postgresql' \
	'odbc:Driver=Sqlite3;Database=/tmp/test.db;@engine=sqlite3' \
	'odbc:Driver=MySQL;Database=test;@engine=mysql' \

do
	rm -f /tmp/test.db
	
	for SUFFIX in '' ';@use_prepared=off' ';@pool_size=5' ';@use_prepared=off;@pool_size=5'
	do
		run_test ./test_backend "$STR$SUFFIX"
		run_test ./test_basic "$STR$SUFFIX"
	done
done

run_test ./test_caching


