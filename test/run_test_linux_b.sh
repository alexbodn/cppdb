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

for STR in \
	'postgresql:dbname=test;@blob=bytea' \
	'postgresql:dbname=test' \
	'sqlite3:db=/tmp/test.db' \
	'mysql:database=test' \
	'odbc:@engine=postgresql;Database=test;Driver=Postgresql ANSI' \
	'odbc:@engine=sqlite3;Driver=Sqlite3;database=/tmp/test.db' \
	'odbc:@engine=mysql;Database=test;Driver=MySQL' \

do
	rm -f /tmp/test.db
	for SUFFIX in '' ';@use_prepared=off' ';@pool_size=5' ';@use_prepared=off;@pool_size=5'
	do
		run_test ./test_backend "$STR$SUFFIX"
		run_test ./test_basic "$STR$SUFFIX"
	done
done

run_test ./test_caching
