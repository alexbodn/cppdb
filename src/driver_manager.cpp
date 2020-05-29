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
#include <cppdb/driver_manager.h>
#include <cppdb/shared_object.h>
#include <cppdb/backend.h>
#include <cppdb/utils.h>
#include <cppdb/mutex.h>

#include <vector>
#include <list>

extern "C" {
	#ifdef CPPDB_WITH_SQLITE3 
	cppdb::backend::connection *cppdb_sqlite3_get_connection(cppdb::connection_info const &cs);
	cppdb::backend::dialect *cppdb_sqlite3_get_dialect();
	#endif
	#ifdef CPPDB_WITH_PQ 
	cppdb::backend::connection *cppdb_postgresql_get_connection(cppdb::connection_info const &cs);
	cppdb::backend::dialect *cppdb_postgresql_get_dialect();
	#endif
	#ifdef CPPDB_WITH_ODBC
	cppdb::backend::connection *cppdb_odbc_get_connection(cppdb::connection_info const &cs);
	cppdb::backend::dialect *cppdb_odbc_get_dialect();
	#endif
	#ifdef CPPDB_WITH_MYSQL
	cppdb::backend::connection *cppdb_mysql_get_connection(cppdb::connection_info const &cs);
	cppdb::backend::dialect *cppdb_mysql_get_dialect();
	#endif
}


namespace cppdb {

	typedef backend::static_driver::connect_function_type connect_function_type;
	typedef backend::static_driver::get_dialect_function_type get_dialect_function_type;

	class so_driver : public backend::loadable_driver {
	public:
		so_driver(std::string const &name,std::vector<std::string> const &so_list) :
			connect_(0)
		{
			get_dialect_function_type get_dialect_ = NULL;
			std::string connect_symbol_name = "cppdb_" + name + "_get_connection";
			std::string get_dialect_symbol_name = "cppdb_" + name + "_get_dialect";
			for(unsigned i=0;i<so_list.size();i++) {
				so_ = shared_object::open(so_list[i]);
				if(so_) {
					so_->safe_resolve(connect_symbol_name,connect_);
					so_->safe_resolve(get_dialect_symbol_name,get_dialect_);
					break;
				}
			}
			if(!so_) {
				throw cppdb_error("cppdb::driver failed to load driver " + name + " - no module found");
			}
			if(!connect_) {
				throw cppdb_error("cppdb::driver failed to load connect");
			}
			if(!get_dialect_) {
				throw cppdb_error("cppdb::driver failed to load dialect");
			}
			dialect_ = get_dialect_();
		}
		virtual backend::connection *open(connection_info const &ci)
		{
			return connect_(ci);
		}
		~so_driver()
		{
			// after so_ will be unloaded, the dialect, allocated in so_, 
			// won't be freeable and it's deletion will segfault
			dialect_.reset();
		}
	private:
		connect_function_type connect_;
		ref_ptr<shared_object> so_;
	};
	
	backend::connection *driver_manager::connect(std::string const &str)
	{
		connection_info conn(str);
		return connect(conn);
	}

	backend::connection *driver_manager::connect(connection_info const &conn)
	{
		ref_ptr<backend::driver> drv_ptr = find_driver(conn);
		if (!drv_ptr) {
			throw cppdb_error("cppdb::driver_manager failed to find driver " + conn.driver);
		}
		return drv_ptr->connect(conn);
	}
	ref_ptr<backend::driver> driver_manager::find_driver(connection_info const &conn, std::string const &driver_name)
	{
		ref_ptr<backend::driver> drv_ptr;
		std::string const &dr_name = driver_name.empty() ? conn.driver : driver_name;
		drivers_type::iterator p;
		{ // get driver
			p=drivers_.find(dr_name);
			if(p!=drivers_.end()) {
				drv_ptr = p->second;
			}
			else {
				drv_ptr = load_driver(conn, dr_name);
				return install_driver(dr_name, drv_ptr);
			}
		}
		return drv_ptr;
	}
	void driver_manager::collect_unused()
	{
		std::list<ref_ptr<backend::driver> > garbage;
		{
			mutex::guard lock(lock_);
			drivers_type::iterator p=drivers_.begin(),tmp;
			while(p!=drivers_.end()) {
				if(!p->second->in_use()) {
					garbage.push_back(p->second);
					tmp=p;
					++p;
					drivers_.erase(tmp);
				}
				else {
					++p;
				}
			}
		}
		garbage.clear();
	}

	#if defined(WIN32) || defined(_WIN32) || defined(__WIN32) || defined(__CYGWIN__)
	
	#	define CPPDB_LIBRARY_SUFFIX_V1 "-" CPPDB_SOVERSION CPPDB_LIBRARY_SUFFIX
	#	define CPPDB_LIBRARY_SUFFIX_V2 CPPDB_LIBRARY_SUFFIX
	
	#elif defined(__APPLE__)
	
	#	define CPPDB_LIBRARY_SUFFIX_V1 "." CPPDB_SOVERSION CPPDB_LIBRARY_SUFFIX
	#	define CPPDB_LIBRARY_SUFFIX_V2 CPPDB_LIBRARY_SUFFIX

	#else

	#	define CPPDB_LIBRARY_SUFFIX_V1 CPPDB_LIBRARY_SUFFIX "." CPPDB_SOVERSION
	#	define CPPDB_LIBRARY_SUFFIX_V2 CPPDB_LIBRARY_SUFFIX

	#endif

	#if (defined(WIN32) || defined(_WIN32) || defined(__WIN32)) && !defined(__CYGWIN__)
	
	#	define PATH_SEPARATOR ';'

	#else

	#	define PATH_SEPARATOR ':'

	#endif


	ref_ptr<backend::driver> driver_manager::load_driver(connection_info const &conn, std::string const &driver_name)
	{
		std::vector<std::string> so_names;
		std::string module;
		std::vector<std::string> search_paths = search_paths_;
		std::string mpath=conn.get("@modules_path");
		if(!mpath.empty()) {
			size_t sep = mpath.find(PATH_SEPARATOR);
			search_paths.push_back(mpath.substr(0,sep));
			while(sep<mpath.size()) {
				size_t next = mpath.find(PATH_SEPARATOR,sep+1);
				search_paths.push_back(mpath.substr(sep+1,next - sep+1));
				sep = next;
			}
		}
		std::string const &dr_name = driver_name.empty() ? conn.driver : driver_name;
		if(!(module=conn.get("@module")).empty()) {
			so_names.push_back(module);
		}
		else {
			std::string so_name1 = CPPDB_LIBRARY_PREFIX "cppdb_" + dr_name + CPPDB_LIBRARY_SUFFIX_V1;
			std::string so_name2 = CPPDB_LIBRARY_PREFIX "cppdb_" + dr_name + CPPDB_LIBRARY_SUFFIX_V2;

			for(unsigned i=0;i<search_paths.size();i++) {
				so_names.push_back(search_paths[i]+"/" + so_name1);
				so_names.push_back(search_paths[i]+"/" + so_name2);
			}
			if(!no_default_directory_) {
				so_names.push_back(so_name1);
				so_names.push_back(so_name2);
			}
		}
		ref_ptr<backend::driver> drv=new so_driver(dr_name,so_names);
		return drv;
	}

	ref_ptr<backend::driver> driver_manager::install_driver(std::string const &name,ref_ptr<backend::driver> drv, bool force)
	{
		if (!force && drivers_.count(name)) {
			return drivers_[name];
		}
		if(!drv) {
			throw cppdb_error("cppdb::driver_manager::install_driver: Can't install empty driver");
		}
		// minimalize the need to lock
		mutex::guard lock(lock_);
		drivers_[name]=drv;
		return drv;
	}

	driver_manager::driver_manager() : 
		no_default_directory_(false)
	{
	}
// Borland erros on hidden destructors in classes without only static methods.
#ifndef __BORLANDC__
	driver_manager::~driver_manager()
	{
	}
#endif
	
	void driver_manager::add_search_path(std::string const &p)
	{
		mutex::guard l(lock_);
		search_paths_.push_back(p);
	}
	void driver_manager::clear_search_paths()
	{
		mutex::guard l(lock_);
		search_paths_.clear();
	}
	void driver_manager::use_default_search_path(bool v)
	{
		mutex::guard l(lock_);
		no_default_directory_ = !v;
	}
	driver_manager &driver_manager::instance()
	{
		static driver_manager instance;
		return instance;
	}
	namespace {
		struct initializer {
			initializer() { 
				driver_manager::instance(); 
				#ifdef CPPDB_WITH_SQLITE3 
				driver_manager::instance().install_driver(
					"sqlite3",new backend::static_driver(cppdb_sqlite3_get_connection, cppdb_sqlite3_get_dialect)
				);
				#endif
				#ifdef CPPDB_WITH_ODBC
				driver_manager::instance().install_driver(
					"odbc",new backend::static_driver(cppdb_odbc_get_connection, cppdb_odbc_get_dialect)
				);
				#endif
				#ifdef CPPDB_WITH_PQ 
				driver_manager::instance().install_driver(
					"postgresql",new backend::static_driver(cppdb_postgresql_get_connection, cppdb_postgresql_get_dialect)
				);
				#endif
				#ifdef CPPDB_WITH_MYSQL
				driver_manager::instance().install_driver(
					"mysql",new backend::static_driver(cppdb_mysql_get_connection, cppdb_mysql_get_dialect)
				);
				#endif
			}
			
		} init;
	}
} // cppdb
