///////////////////////////////////////////////////////////////////////////////
//                                                                             
//  Copyright (C) 2020  Alex Bodnaru <alexbodn@gmail.com>
//                                                                             
//  Distributed under:
//
//                   the Boost Software License, Version 1.0.
//              (See accompanying file LICENSE_1_0.txt or copy at 
//                     http://www.boost.org/LICENSE_1_0.txt)
//
//  or (at your opinion) under:
//
//                               The MIT License
//                 (See accompanying file MIT.txt or a copy at
//              http://www.opensource.org/licenses/mit-license.php)
//
///////////////////////////////////////////////////////////////////////////////
#include <cppdb/backend.h>
#include <cppdb/schema.h>
#include <cppdb/driver_manager.h>
#include <sstream>
#include <iostream>

#include "test.h"


bool test_64bit_integer = true;
bool test_blob = true;
bool wide_api = false;
bool pq_oid = false;


/*
void test_template(cppdb::ref_ptr<cppdb::backend::connection> sql, std::string const &//cs
)
{
	cppdb::ref_ptr<cppdb::backend::statement> stmt;
	cppdb::ref_ptr<cppdb::backend::result> res;
}
*/

void test0(cppdb::ref_ptr<cppdb::backend::connection> /*sql*/, std::string const &/*cs*/)
{
	std::cout << "Test the string replace routine" << std::endl;
	std::string s;
	s = cppdb::str_replace("Number Of Beans", " ", "_");
	TEST(s == "Number_Of_Beans");
	s = cppdb::str_replace("ghghjghugtghty", "gh", "X", 2);
	TEST(s == "XXjghugtghty");
	s = cppdb::str_replace("ghghjghugtghty", "gh", "h12");
	TEST(s == "h12h12jh12ugth12ty");

	std::cout << "Test the string split and join routines" << std::endl;
	std::string input = "aa_bb_cc";
	std::string delim = "_";;
	std::vector<std::string> output = cppdb::str_split(input, delim.c_str()[0]);
	std::string result = cppdb::str_join(output, delim);
	TEST(result == input);
	std::string input2 = "aa bb cc \ndd";
	std::vector<std::string> output2 = cppdb::str_split(input2);
	std::string result2 = cppdb::str_join(output2, " ");
	TEST(result2 == "aa bb cc dd");

	std::cout << "Test the string replacement as python dict %" << std::endl;
	std::string hay3 = "%19(aaa)abc %% %%123(aaa)x bla %%%-(aaa)p foo %+(aaa) bar (aaa)s xyz %%%%%%%(aaa)z $";
	std::string result3 = "---bc %% %%123(aaa)x bla %%--- foo %+(aaa) bar (aaa)s xyz %%%%%%--- $";
	TEST(result3 == cppdb::str_replace_format(hay3, "aaa", "---", 0));
}

void test1(cppdb::ref_ptr<cppdb::backend::connection> sql, std::string const &/*cs*/)
{
	cppdb::ref_ptr<cppdb::backend::statement> stmt;
	cppdb::ref_ptr<cppdb::backend::result> res;
	try {
		stmt = sql->prepare("drop table test");
		stmt->exec(); 
	}catch(...) {}

	if(sql->engine()=="mssql" && wide_api) {
		stmt = sql->prepare("create table test ( x integer not null, y nvarchar(1000) )");
	}
	else
		stmt = sql->prepare("create table test ( x integer not null, y varchar(1000) )");
	
	std::cout << "Basic select " << std::endl;

	stmt->exec();
	stmt = sql->prepare("select * from test");
	res = stmt->query();
	TEST(!res->next());
	stmt  = sql->prepare("insert into test(x,y) values(10,'foo?')");
	stmt->exec();
	stmt = sql->prepare("select x,y from test");

	res = stmt->query();
	TEST(res->next());
	TEST(res->cols()==2);
	int iv;
	std::string sv;
	TEST(res->fetch(0,iv));
	TEST(iv==10);
	TEST(res->fetch(1,sv));
	TEST(sv=="foo?");
	TEST(!res->next());
	res.reset();
	stmt = sql->prepare("insert into test(x,y) values(20,NULL)");
	stmt->exec();
	stmt = sql->prepare("select y from test where x=?");
	stmt->bind(1,20);
	res = stmt->query();
	TEST(res->next());
	TEST(res->is_null(0));
	sv="xxx";
	TEST(!res->fetch(0,sv));
	TEST(sv=="xxx");
	TEST(!res->next());
	res.reset();
	stmt->reset();
	stmt->bind(1,10);
	res = stmt->query();
	TEST(res->next());
	sv="";
	TEST(!res->is_null(0));
	TEST(res->fetch(0,sv));
	TEST(sv=="foo?");
	stmt = sql->prepare("DELETE FROM test");
	stmt->exec();
	std::cout << "Unicode Test" << std::endl;
	if(sql->engine()!="mssql" || wide_api) {
		std::string test_string = "Pease שלום Мир ﺱﻼﻣ";
		stmt = sql->prepare("insert into test(x,y) values(?,?)");
		stmt->bind(1,15);
		stmt->bind(2,test_string);
		stmt->exec();
		stmt = sql->prepare("select x,y from test");
		res = stmt->query();
		TEST(res->next());
		sv="";
		res->fetch(1,sv);
		TEST(sv==test_string);
	}
	else {
		std::cout << "This does not support unicode, skipping" << std::endl;
	}
}

void test2(cppdb::ref_ptr<cppdb::backend::connection> /*sql*/, std::string const &/*cs*/)
{
	cppdb::schema::schema schm;
	
	cppdb::ref_ptr<cppdb::schema::entity> artist = schm.add_entity("Artist");
	artist->table("artist");
	artist->add_columns({
		{
			"artistid", cppdb::schema::params_type{
				{"data_type", "integer"},
				{"is_nullable", "0"},
				{"is_auto_increment", "1"}
			}
		},
		{
			"name", cppdb::schema::params_type{
				{"data_type", "text"}
			}
		}
	});
	artist->set_primary_key({"artistid"});
	artist->add_unique_constraint({"name"});
	artist->has_many("cds", "Cd", {"artistid"});

	cppdb::ref_ptr<cppdb::schema::entity> cd = schm.add_entity("Cd");
	cd->table("cd");
	cd->add_columns({
		{
			"cdid", cppdb::schema::params_type{
				{"data_type", "integer"},
				{"is_nullable", "0"},
				{"is_auto_increment", "1"}
			}
		},
		{
			"artistid", cppdb::schema::params_type{
				{"data_type", "integer"}
			},
		},
		{
			"title", cppdb::schema::params_type{
				{"data_type", "text"}
			},
		},
		{
			"year", cppdb::schema::params_type{
				{"data_type", "datetime"},
				{"is_nullable", "1"}
			}
		}
	});
	cd->set_primary_key({"cdid"});
	cd->add_unique_constraint({"title", "artistid"});
	cd->belongs_to("artist", "Artist", {"artistid"});
	cd->has_many("tracks", "Track", {"cdid"});

	cppdb::ref_ptr<cppdb::schema::entity> track = schm.add_entity("Track");
	track->table("track");
	track->add_columns({
		{
			"trackid", {
				{"data_type", "integer"},
				{"is_auto_increment", "1"}
			}
		},
		{
			"cdid", {
				{"data_type", "integer"}
			}
		},
		{
			"title", {
				{"data_type", "text"}
			},
		}
	});
	track->set_primary_key({"trackid"});
	track->add_unique_constraint({"title", "cdid"});
	track->belongs_to("cd", "Cd", {"cdid"});

	schm.finalize();
}

void run_test(void (*func)(cppdb::ref_ptr<cppdb::backend::connection>, std::string const &),cppdb::ref_ptr<cppdb::backend::connection> sql, std::string cs="")
{
	try {
		func(sql, cs);
	}
	catch(cppdb::cppdb_error const &e) {
		std::cerr << "Catched exception " << e.what() << std::endl;
		std::cerr << "Last tested line " << last_line  << std::endl;
		failed++;
		return;
	}
	passed++;
}

void test(std::string conn_str)
{
	cppdb::connection_info conn(conn_str);
	cppdb::driver_manager &manager = cppdb::driver_manager::instance();
	cppdb::ref_ptr<cppdb::backend::driver> driver = manager.find_driver(conn);
	return;
	cppdb::ref_ptr<cppdb::backend::connection> sql(cppdb::driver_manager::instance().connect(conn_str));
	
	cppdb::ref_ptr<cppdb::backend::statement> stmt;
	wide_api = (sql->driver()=="odbc" && conn_str.find("utf=wide")!=std::string::npos);
	
	std::cout << "Basic setup" << std::endl;

	if(sql->engine() == "postgresql") {
		if(sql->driver() == "odbc")
			pq_oid = true;
		else if(sql->driver()=="postgresql" && conn_str.find("@blob=bytea") == std::string::npos)
			pq_oid = true;
	}

//	run_test(test0,sql);
//	run_test(test1,sql);
//	run_test(test2,sql);
}



int main(int argc,char **argv)
{
	if(argc!=2) {
		std::cerr << "Usage: test_backend connection_string" << std::endl;
		return 1;
	}
	std::string cs = argv[1];
	std::cout << "test_schema:\"" << cs  << '"' << std::endl;
	try {
		test(cs);
	}
	CATCH_BLOCK();
	SUMMARY();
}
