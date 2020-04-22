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
#ifndef CPPDB_UTIL_H
#define CPPDB_UTIL_H

#include <cppdb/defs.h>
#include <string>
#include <ctime>
#include <map>
#include <vector>


namespace cppdb {

	///
	/// \brief parse a string as date & time value.
	/// 
	/// Used by backend implementations;
	///
	CPPDB_API std::tm parse_date(char const *value);
	CPPDB_API std::tm parse_time(char const *value);
	CPPDB_API std::tm parse_datetime(char const *value);
	///
	/// \brief format a string as date & time value.
	/// 
	/// Used by backend implementations;
	///
	CPPDB_API std::string format_date(std::tm const &v);
	CPPDB_API std::string format_time(std::tm const &v);
	CPPDB_API std::string format_datetime(std::tm const &v);
	///
	/// \brief parse a string as date & time value.
	/// 
	/// Used by backend implementations;
	///
	CPPDB_API std::tm parse_date(std::string const &v);
	CPPDB_API std::tm parse_time(std::string const &v);
	CPPDB_API std::tm parse_datetime(std::string const &v);

	///
	/// \brief Parse a connection string \a cs into driver name \a driver_name and list of properties \a props
	///
	/// The connection string format is following:
	///
	/// \verbatim  driver:[key=value;]*  \endverbatim 
	///
	/// Where value can be either a sequence of characters (white space is trimmed) or it may be a general
	/// sequence encloded in a single quitation marks were double quote is used for insering a single quote value.
	///
	/// Key values starting with \@ are reserved to be used as special cppdb  keys
	/// For example:
	///
	/// \verbatim   mysql:username= root;password = 'asdf''5764dg';database=test;@use_prepared=off' \endverbatim 
	///
	/// Where driver is "mysql", username is "root", password is "asdf'5764dg", database is "test" and
	/// special value "@use_prepared" is off - internal cppdb option.
	CPPDB_API void parse_connection_string(	std::string const &cs,
						std::string &driver_name,
						std::map<std::string,std::string> &props);
	///
	/// remove whitespace at both ends of a string
	///
	std::string str_trim(std::string const &s);
	///
	///replace nTimes occurences of sNeedle in sHaystack by sReplace
	///if nTimes == 0, replace all occurences thereof
	///
	CPPDB_API std::string str_replace(
						std::string const &sHaystack, std::string const &sNeedle, 
						std::string const &sReplace, size_t nTimes=0);
	///
	/// replace all occurences of a python style variable to dict format
	///
	std::string str_replace_format(
		std::string const &sHaystack, std::string const &sNeedle, 
		std::string const &sReplace, 
		std::string (*formater)(std::string const &)=NULL);
	///
	/// join the strings in the strings vector, delimiting by string delim
	///
	CPPDB_API std::string str_join(
						std::vector<std::string> const &strings, 
						std::string const &delim);
	///
	/// split the input string by the delimiter char delim, into a vector of strings
	/// if delim not given, whitespace will be assumed
	///
	CPPDB_API std::vector<std::string> str_split(
						const std::string &input, char delim=0);

	///
	/// \brief Class that represents parsed key value properties file
	///
	class CPPDB_API properties {
	public:
		///
		/// Type that represent key, values set
		///
		typedef std::map<std::string,std::string> properties_type;
		
		///
		/// Cheks if property \a prop, has been given in connection string.
		///
		bool has(std::string const &prop) const;
		///
		/// Get property \a prop, returning \a default_value if not defined.
		///
		std::string const &get(std::string const &prop,std::string const &default_value=std::string()) const;
		///
		/// Get numeric value for property \a prop, returning \a default_value if not defined. 
		/// If the value is not a number, throws cppdb_error.
		///
		int get(std::string const &prop,int default_value) const;
		///
		/// set a property value
		///
		void set(std::string const &key, std::string const &value)
		{
			properties_[key] = value;
		}
		///
		/// Dump in a kind of json format
		///
		std::string dump() const;
		///
		/// String format using the values of properties
		///
		std::string format(std::string const &fmt, std::string (*formater)(std::string const &)=NULL) const;
		
		properties() {}
		properties(properties_type const &props) : properties_(props) {}
	protected:
		///
		/// The std::map of key value properties.
		///
		properties_type properties_;
	};

	///
	/// \brief Class that represents parsed connection string
	///
	class CPPDB_API connection_info : public properties {
	public:
		///
		/// The original connection string
		///
		std::string connection_string;
		///
		/// The driver name
		///
		std::string driver;
		///
		/// Default constructor - empty info
		///	
		connection_info()
		{
		}
		///
		/// Create connection_info from the connection string parsing it.
		///
		explicit connection_info(std::string const &cs) :
			connection_string(cs)
		{
			parse_connection_string(cs,driver,properties_);
		}
		std::string conn_str(std::string const &delimiter, std::string (*formater)(std::string const &)=NULL) const;

	};

}
#endif
