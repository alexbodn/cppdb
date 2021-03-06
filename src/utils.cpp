///////////////////////////////////////////////////////////////////////////////
//                                                                             
//  Copyright (C) 2010-2011  Artyom Beilis (Tonkikh) <artyomtnk@yahoo.com>     
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
#define CPPDB_SOURCE
#include <cppdb/utils.h>
#include <cppdb/errors.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <sstream>
#include <regex>
#include <locale>

#include <iostream>

namespace cppdb {
	std::string format_date(std::tm const &v)
	{
		char buf[11]= {0};
		strftime(buf,sizeof(buf),"%Y-%m-%d",&v);
		return std::string(buf);
	}
	std::string format_time(std::tm const &v)
	{
		char buf[20]= {0};
		strftime(buf,sizeof(buf),"%H:%M:%S",&v);
		return std::string(buf);
	}
	std::string format_datetime(std::tm const &v)
	{
		char buf[64]= {0};
		strftime(buf,sizeof(buf),"%Y-%m-%d %H:%M:%S",&v);
		return std::string(buf);
	}

	std::tm parse_date(std::string const &v)
	{
		if(strlen(v.c_str())!=v.size())
			throw bad_value_cast();
		return parse_date(v.c_str());
	}
	std::tm parse_date(char const *v)
	{
		std::tm t=std::tm();
		int n;
		n = sscanf(v,"%d-%d-%d",
			&t.tm_year,&t.tm_mon,&t.tm_mday);
		if(n!=3) 
		{
			throw bad_value_cast();
		}
		t.tm_year-=1900;
		t.tm_mon-=1;
		t.tm_isdst = -1;
		if(mktime(&t)==-1)
			throw bad_value_cast();
		return t;
	}
	std::tm parse_time(std::string const &v)
	{
		if(strlen(v.c_str())!=v.size())
			throw bad_value_cast();
		return parse_time(v.c_str());
	}
	std::tm parse_time(char const *v)
	{
		std::tm t=std::tm();
		int n;
		double sec = 0;
		n = sscanf(v,"%d:%d:%lf",
			&t.tm_hour,&t.tm_min,&sec);
		if(n!=3) 
		{
			throw bad_value_cast();
		}
		t.tm_isdst = -1;
		t.tm_sec=static_cast<int>(sec);
		if(mktime(&t)==-1)
			throw bad_value_cast();
		return t;
	}
	std::tm parse_datetime(std::string const &v)
	{
		if(strlen(v.c_str())!=v.size())
			throw bad_value_cast();
		return parse_datetime(v.c_str());
	}
	std::tm parse_datetime(char const *v)
	{
		std::tm t=std::tm();
		int n;
		double sec = 0;
		n = sscanf(v,"%d-%d-%d %d:%d:%lf",
			&t.tm_year,&t.tm_mon,&t.tm_mday,
			&t.tm_hour,&t.tm_min,&sec);
		if(n!=3 && n!=6) 
		{
			return parse_time(v);
		}
		t.tm_year-=1900;
		t.tm_mon-=1;
		t.tm_isdst = -1;
		t.tm_sec=static_cast<int>(sec);
		if(mktime(&t)==-1)
			throw bad_value_cast();
		return t;
	}

	namespace {
		bool is_blank_char(char c)
		{
			return c==' ' || c=='\t' || c=='\r' || c=='\n' || c=='\f';
		}
	}
		std::string str_trim(std::string const &s)
		{
			if(s.empty())
				return s;
			size_t start=0,end=s.size()-1;
			while(start < s.size() && is_blank_char(s[start])) {
				start++;
			}
			while(end > start && is_blank_char(s[end])) {
				end--;
			}
			return s.substr(start,end-start+1);
		}

	void parse_connection_string(	std::string const &connection_string,
					std::string &driver,
					std::map<std::string,std::string> &params)
	{
		params.clear();
		size_t p = connection_string.find(':');
		if( p == std::string::npos )
			throw cppdb_error("cppdb::Invalid connection string - no driver given");
		driver = connection_string.substr(0,p);
		p++;
		while(p<connection_string.size()) {
			size_t n=connection_string.find('=',p);
			if(n==std::string::npos)
				throw cppdb_error("Invalid connection string - invalid property");
			std::string key = str_trim(connection_string.substr(p,n-p));
			p=n+1;
			std::string value;
			while(p<connection_string.size() && is_blank_char(connection_string[p]))
			{
				++p;
			}
			if(p>=connection_string.size()) {
				/// Nothing - empty property
			}
			else if(connection_string[p]=='\'') {
				p++;
				while(true) {
					if(p>=connection_string.size()) {
						throw cppdb_error("Invalid connection string unterminated string");
					}
					if(connection_string[p]=='\'') {
						if(p+1 < connection_string.size() && connection_string[p+1]=='\'') {
							value+='\'';
							p+=2;
						}
						else {
							p++;
							break;
						}
					}
					else {
						value+=connection_string[p];
						p++;
					}
				}
			}
			else {
				size_t n=connection_string.find(';',p);
				if(n==std::string::npos) {
					value=str_trim(connection_string.substr(p));
					p=connection_string.size();
				}
				else {
					value=str_trim(connection_string.substr(p,n-p));
					p=n;
				}
			}
			if(params.find(key)!=params.end()) {
				throw cppdb_error("cppdb::invalid connection string duplicate key");
			}
			params[key]=value;
			while(p<connection_string.size()) {
				char c=connection_string[p];
				if(is_blank_char(c))
					++p;
				else if(c==';') {
					++p;
					break;
				}
			}
		}
	} //

	std::string str_replace(
		std::string const &sHaystack, std::string const &sNeedle, std::string const &sReplace, 
		size_t nTimes)
	{
		size_t found = 0, pos = 0, c = 0;
		size_t len = sNeedle.size();
		size_t replen = sReplace.size();
		std::string input(sHaystack);
		
		do {
			found = input.find(sNeedle, pos);
			if (found == std::string::npos) {
				break;
			}
			input.replace(found, len, sReplace);
			pos = found + replen;
			++c;
		} while(!nTimes || c < nTimes);
		
		return input;
	}

	std::string str_replace_format(
		std::string const &sHaystack, std::string const &sNeedle, 
		std::string const &sReplace, 
		std::string (*formater)(std::string const &))
	{
		std::string sre = "(^|[^%])((%%){0,})[%]{1}([^(%]{0,})\\(" + sNeedle + "\\)([a-z])";
		std::regex re(sre);
		std::string replace = "$1$2" + str_replace(
			formater ? formater(sReplace) : sReplace, "$", "$$", 0);
		return std::regex_replace(sHaystack, re, replace);
	}

	std::string str_join(std::vector<std::string> const &strings, std::string const &delim)
	{
		std::ostringstream out;
		if(!strings.empty())
		{
			auto iter = strings.begin();
			while(true)
			{
				out << *iter;
				++iter;
				if(iter == strings.end())
				{
					break;
				}
				else
				{
					out << delim;
				}
			}
		}
		return out.str();
	}

	std::vector<std::string> str_split(const std::string &input, char delim)
	{
		std::stringstream ss(input);
		std::string item;
		std::vector<std::string> elems;
		if (delim) {
			while (std::getline(ss, item, delim)) {
				elems.push_back(item);
				// elems.push_back(std::move(item)); // if C++11
			}
		}
		else {
			while (ss >> item) {
				elems.push_back(item);
				// elems.push_back(std::move(item)); // if C++11
			}
		}
		return elems;
	}

	bool properties::has(std::string const &prop) const
	{
		return properties_.find(prop) != properties_.end();
	}
	std::string const &properties::get(std::string const &prop,std::string const &default_value) const
	{
		properties_type::const_iterator p=properties_.find(prop);
		if(p==properties_.end()) {
			return default_value;
		}
		else {
			return p->second;
		}
	}
	int properties::get(std::string const &prop,int default_value) const
	{
		properties_type::const_iterator p=properties_.find(prop);
		if(p==properties_.end())
			return default_value;
		std::istringstream ss;
		ss.imbue(std::locale::classic());
		ss.str(p->second);
		int val;
		ss >> val;
		if(!ss || !ss.eof()) {
			throw cppdb_error("cppdb::properties property " + prop + " expected to be integer value");
		}
		return val;
	}

	void properties::set(std::string const &key, std::string const &value)
	{
		properties_[key] = value;
	}

	std::string properties::dump() const
	{
		properties_type::const_iterator p;
		bool first = true;
		std::string str = "{\n";
		for (p=properties_.begin();p!=properties_.end();p++) {
			if (!first)
				str += ",\n";
			else
				first = false;
			str+="\t{\"";
			str+=str_replace(p->first, "\"", "\\\"");
			str+="\"}, {\"";
			str+=str_replace(p->second, "\"", "\\\"");
			str+="\"}";
		}
		return str + "\n}";
	}

	std::string properties::format(std::string const &fmt, std::string (*formater)(std::string const &)) const
	{
		properties_type::const_iterator p;
		std::string fmtp = fmt;
		for (p=properties_.begin();p!=properties_.end();p++) {
			fmtp = str_replace_format(fmtp, p->first, p->second, formater);
		}
		return str_replace(fmtp, "%%", "%");
	}

	std::string connection_info::conn_str(std::string const &delimiter, std::string (*formater)(std::string const &)) const
	{
		properties_type::const_iterator p;
		std::string str;
		for(p=properties_.begin();p!=properties_.end();p++) {
			if(p->first.empty() || p->first[0]=='@')
				continue;
			str+=p->first;
			str+="=";
			str+=(formater ? formater(p->second) : p->second);
			str+=delimiter;
		}
		return str;
	}
	

}
