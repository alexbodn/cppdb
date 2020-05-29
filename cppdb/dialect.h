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
#ifndef CPPDB_DIALECT_H
#define CPPDB_DIALECT_H

#include <vector>
#include <cppdb/defs.h>
#include <cppdb/errors.h>
#include <cppdb/utils.h>
#include <cppdb/ref_ptr.h>

namespace cppdb {
	namespace backend {	

		/// \cond INTERNAL	
		class CPPDB_API dialect : public ref_counted {
		protected:
			properties keywords_;
			///
			/// set the keword(s) value for the name
			///
			void set_keyword(std::string const &name, std::string const &value)
			{
				keywords_.set(name, value);
			}
			void set_keywords(std::vector<std::pair<std::string, std::string>> const &kw);
			///
			/// get the value for the name
			///
			std::string const &get_keyword(std::string const &name, std::string const &default_value) const
			{
				return keywords_.get(name, default_value);
			}
			///
			/// translate the type name acording to this dialect
			///
			virtual std::string const &type_name(std::string const &name) const
			{
				return get_keyword(name, name);
			}
			virtual void init();
		public:
			dialect();
			dialect(std::vector<std::pair<std::string, std::string>> const &kw);
			~dialect();
			///
			/// render type with optional parameters in parantheses
			///
			virtual std::string render_type(std::string const &name, int param=-1, int param2=-1) const;
			virtual std::string render_type(std::string const &name, std::vector<int> const &params) const;
			///
			/// Return name of the type for bigint
			///
			virtual std::string type_bigint() const
			{
				return render_type("bigint");
			}
			///
			/// Return name of the type for real
			///
			virtual std::string type_real(int size=-1) const
			{
				return render_type("real", size);
			}
			///
			/// Return name of the type for decimal
			///
			virtual std::string type_decimal(int precision, int scale=-1) const
			{
				return render_type("decimal", precision, scale);
			}
			///
			/// Return name of the type for n?varchar
			///
			virtual std::string type_varchar(int length=-1) const
			{
				return render_type("varchar", length);
			}
			virtual std::string type_nvarchar(int length=-1) const
			{
				return render_type("nvarchar", length);
			}
			///
			/// Return name of the type for datetime
			///
			virtual std::string type_datetime() const
			{
				return render_type("datetime");
			}
			///
			/// Return name of the type for blob
			///
			virtual std::string type_blob() const
			{
				return render_type("blob");
			}
			///
			/// Return type for autoincrement pk
			///
			virtual std::string type_autoincrement_pk() const
			{
				return get_keyword("type_autoincrement_pk", "");
			}
			///
			/// SQL for last insert id
			///
			virtual std::string sequence_last() const
			{
				return get_keyword("sequence_last", "");
			}
			///
			/// create table suffix
			///
			virtual std::string create_table_suffix() const
			{
				return get_keyword("create_table_suffix", "");
			}
			///
			/// Escape a string for inclusion in SQL query. 
			///
			virtual std::string escape(std::string const &s) const
			{
				return str_replace(s, "'", "''");
			}
			///
			/// expose all the keywords
			///
			properties const & get_keywords() const
			{
				return keywords_;
			}
			static void dispose(dialect *d);
		};
		/// \endcond

	} // backend
} // cppdb

#endif
