///////////////////////////////////////////////////////////////////////////////
//                                                                             
//  Copyright (C) 2020 Alex Bodnaru <alexbodn@gmail.com>
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
#include <cppdb/dialect.h>
#include <cppdb/backend.h>

#include <map>
#include <list>
#include <algorithm>
#include <cctype>
#include <string>

namespace cppdb {
	namespace backend {

		void dialect::set_keywords(std::vector<std::pair<std::string, std::string>> const &kw)
		{
			for (const std::pair<std::string, std::string> & pr : kw) {
				set_keyword(pr.first, pr.second);
			}
		}
		void dialect::init()
		{
			set_keywords({
				{"engine", "generic"},
				{"datetime", "timestamp"},
				{"blob", ""}
			});
		}
		///
		/// render type with optional parameters in parantheses
		///
		std::string dialect::render_type(std::string const &name, int param, int param2) const
		{
			std::string str = name;
			std::transform(
				str.begin(), str.end(), str.begin(),
				[](unsigned char chr){ return std::tolower(chr); }
			);
			str = type_name(str);
			if (param >= 0) {
				str += ("(" + std::to_string(param));
				if (param2 >= 0) {
					str += (", " + std::to_string(param2));
				}
				str += ")";
			}
			return str;
		}
		std::string dialect::render_type(std::string const &name, std::vector<int> const &params) const
		{
			return render_type(
				name, 
				params.size() > 0 ? params[0] : -1, 
				params.size() > 1 ? params[1] : -1
			);
		}
		dialect::dialect()
		{
			init();
		}
		dialect::dialect(std::vector<std::pair<std::string, std::string>> const &kw)
		{
			init();
			set_keywords(kw);
		}
		void dialect::dispose(dialect *d)
		{
			delete d;
		}
		dialect::~dialect()
		{
		}

	} // backend

	
} // cppdb


