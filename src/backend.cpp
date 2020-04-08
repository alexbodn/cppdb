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
#include <cppdb/backend.h>
#include <cppdb/utils.h>
#include <cppdb/pool.h>

#include <map>
#include <list>
#include <algorithm>
#include <cctype>
#include <string>

#include <iostream>

namespace cppdb {
	namespace backend {
		//result
		struct result::data {};
		result::result() {}
		result::~result() {}
		
		//statement
		struct statement::data {};

		statement::statement() : cache_(0), connection_(0)
		{
		}
		statement::~statement()
		{
		}
		void statement::cache(statements_cache *c)
		{
			cache_ = c;
		}

		void statement::dispose(statement *p)
		{
			if(!p)
				return;
			statements_cache *cache = p->cache_;
			p->cache_ = 0;
			if(cache) 
				cache->put(p);
			else
				delete p;
		}

		long long statement::sequence_last(std::string const &sequence)
		{
			std::string q = connection_->sequence_last();
			if(q.empty()) {
				q = connection_->get_dialect()->sequence_last();
			}
			if(q.empty()) {
				throw not_supported_by_backend(
					"cppdb::sequence_last is not supported");
			}
			ref_ptr<statement> st = connection_->prepare(q);
			if(q.find('?')!=std::string::npos) {
				st->bind(1,sequence);
			}
			ref_ptr<result> res = st->query();
			long long last_id;
			if(!res->next() || res->cols()!=1 || !res->fetch(0,last_id)) {
				throw cppdb_error("cppdb::sequence_last failed to fetch last value");
			}
			res.reset();
			st.reset();
			return last_id;
		}

		//statements cache//////////////

		struct statements_cache::data {

			data() : 
				size(0),
				max_size(0) 
			{
			}

			struct entry;
			typedef std::map<std::string,entry> statements_type;
			typedef std::list<statements_type::iterator> lru_type;
			struct entry {
				ref_ptr<statement> stat;
				lru_type::iterator lru_ptr;
			};
			
			statements_type statements;
			lru_type lru;
			size_t size;
			size_t max_size;


			void insert(ref_ptr<statement> st)
			{
				statements_type::iterator p;
				if((p=statements.find(st->sql_query()))!=statements.end()) {
					p->second.stat = st;
					lru.erase(p->second.lru_ptr);
					lru.push_front(p);
					p->second.lru_ptr = lru.begin();
				}
				else {
					if(size > 0 && size >= max_size) {
						statements.erase(lru.back());
						lru.pop_back();
						size--;
					}
					std::pair<statements_type::iterator,bool> ins = 
						statements.insert(std::make_pair(st->sql_query(),entry()));
					p = ins.first;
					p->second.stat = st;
					lru.push_front(p);
					p->second.lru_ptr = lru.begin();
					size ++;
				}
			}

			ref_ptr<statement> fetch(std::string const &query)
			{
				ref_ptr<statement> st;
				statements_type::iterator p = statements.find(query);
				if(p==statements.end())
					return st;
				st=p->second.stat;
				lru.erase(p->second.lru_ptr);
				statements.erase(p);
				size --;
				return st;
			}

			void clear()
			{
				lru.clear();
				statements.clear();
				size=0;
			}
		}; // data

		statements_cache::statements_cache() 
		{
		}
		void statements_cache::set_size(size_t n)
		{
			if(n!=0 && !active()) {
				d.reset(new data());
				d->max_size = n;
			}
		}
		void statements_cache::put(statement *p_in)
		{
			if(!active()) {
				delete p_in;
			}
			ref_ptr<statement> p(p_in);
			p->reset();
			d->insert(p);
		}
		ref_ptr<statement> statements_cache::fetch(std::string const &q)
		{
			if(!active())
				return 0;
			return d->fetch(q);
		}
		void statements_cache::clear()
		{
			d->clear();
		}
		statements_cache::~statements_cache()
		{
		}

		bool statements_cache::active()
		{
			return d.get()!=0;
		}

		//////////////
		//connection
		//////////////
		
		struct connection::data {
			typedef std::list<connection_specific_data *> conn_specific_type;
			conn_specific_type conn_specific;
			~data()
			{
				for(conn_specific_type::iterator p=conn_specific.begin();p!=conn_specific.end();++p)
					delete *p;
			}
		};
		ref_ptr<statement> connection::prepare(std::string const &q) 
		{
			if(default_is_prepared_)
				return get_prepared_statement(q);
			else
				return get_statement(q);
		}
		
		ref_ptr<statement> connection::get_statement(std::string const &q)
		{
			ref_ptr<statement> st = create_statement(q);
			st->set_connection(this);
			return st;
		}

		ref_ptr<statement> connection::get_prepared_statement(std::string const &q)
		{
			ref_ptr<statement> st;
			if(!cache_.active()) {
				st = prepare_statement(q);
				return st;
			}
			st = cache_.fetch(q);
			if(!st)
				st = prepare_statement(q);
			st->cache(&cache_);
			st->set_connection(this);
			return st;
		}

		ref_ptr<statement> connection::get_prepared_uncached_statement(std::string const &q)
		{
			ref_ptr<statement> st = prepare_statement(q);
			st->set_connection(this);
			return st;
		}

		connection::connection(connection_info const &info) :
			d(new connection::data),
			pool_(0),
			once_called_(0),
			recyclable_(1),
			dialect_(0)
		{
			int cache_size = info.get("@stmt_cache_size",64);
			if(cache_size > 0) {
				cache_.set_size(cache_size);
			}
			sequence_last_ = info.get("@sequence_last", "");
			std::string def_is_prep = info.get("@use_prepared","on");
			if(def_is_prep == "on")
				default_is_prepared_ = 1;
			else if(def_is_prep == "off") 
				default_is_prepared_ = 0;
			else
				throw cppdb_error("cppdb::backend::connection: @use_prepared should be either 'on' or 'off'");
		}
		connection::~connection()
		{
		}

		bool connection::once_called() const
		{
			return once_called_;
		}
		void connection::once_called(bool v)
		{
			once_called_ = v;
		}
		connection_specific_data *connection::connection_specific_get(std::type_info const &type) const
		{
			for(data::conn_specific_type::const_iterator p=d->conn_specific.begin();p!=d->conn_specific.end();++p) {
				if(typeid(**p) == type)
					return *p;
			}
			return 0;
		}
		connection_specific_data *connection::connection_specific_release(std::type_info const &type)
		{
			for(data::conn_specific_type::iterator p=d->conn_specific.begin();p!=d->conn_specific.end();++p) {
				if(typeid(**p) == type) {
					connection_specific_data *ptr = *p;
					d->conn_specific.erase(p);
					return ptr;
				}
			}
			return 0;
		}
		void connection::connection_specific_reset(std::type_info const &type,connection_specific_data *ptr)
		{
			std::auto_ptr<connection_specific_data> tmp(ptr);
			if(ptr && typeid(*ptr)!=type) {
				throw cppdb_error(
					std::string("cppdb::connection_specific::Inconsistent pointer type")
					+ typeid(*ptr).name() 
					+ " and std::type_info reference:"
					+ type.name()
				);
			}
			for(data::conn_specific_type::iterator p=d->conn_specific.begin();p!=d->conn_specific.end();++p) {
				if(typeid(**p) == type) {
					delete *p;
					if(ptr)
						*p = tmp.release();
					else
						d->conn_specific.erase(p);
					return;
				}
			}
			if(ptr) {
				d->conn_specific.push_back(0);
				d->conn_specific.back() = tmp.release();
			}
		}

		ref_ptr<pool> connection::get_pool()
		{
			return pool_;
		}
		void connection::set_pool(ref_ptr<pool> p)
		{
			pool_ = p;
		}
		void connection::set_driver(ref_ptr<loadable_driver> p)
		{
			driver_ = p;
		}
		void connection::clear_cache()
		{
			cache_.clear();
		}

		void connection::recyclable(bool opt)
		{
			recyclable_ = opt;
		}

		bool connection::recyclable()
		{
			return recyclable_;
		}
		
		void connection::dispose(connection *c)
		{
			if(!c)
				return;
			ref_ptr<pool> p = c->pool_;
			c->pool_ = 0;
			if(p && c->recyclable())
				p->put(c);
			else {
				c->clear_cache();
				// Make sure that driver would not be
				// destoryed destructor of connection exits
				ref_ptr<loadable_driver> driver = c->driver_;
				delete c;
				driver.reset();
			}
		}
		void dialect::set_keywords(std::vector<std::pair<std::string, std::string>> const &kw)
		{
			for (const std::pair<std::string, std::string> & pr : kw) {
				set_keyword(pr.first, pr.second);
			}
		}
		void dialect::init()
		{
			set_keywords({
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
		connection *driver::connect(connection_info const &cs)
		{
			return open(cs);
		}
		ref_ptr<cppdb::backend::dialect> driver::get_dialect()
		{
			throw cppdb_error("cppdb::backend::driver: this driver does not implement a dialog");
		}
		bool loadable_driver::in_use()
		{
			return use_count() > 1;
		}
		connection *loadable_driver::connect(connection_info const &cs)
		{
			connection *c = open(cs);
			c->set_driver(ref_ptr<loadable_driver>(this));
			return c;
		}

		static_driver::static_driver(connect_function_type c) : connect_(c)
		{
		}
		static_driver::~static_driver()
		{
		}
		bool static_driver::in_use()
		{
			return true;
		}
		backend::connection *static_driver::open(connection_info const &ci)
		{
			return connect_(ci);
		}

	} // backend

	struct connection_specific_data::data {};

	connection_specific_data::connection_specific_data()
	{
	}
	connection_specific_data::~connection_specific_data()
	{
	}
	
	
} // cppdb


