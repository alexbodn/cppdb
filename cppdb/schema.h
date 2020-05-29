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
#ifndef CPPDB_SCHEMA_H
#define CPPDB_SCHEMA_H 1

#include <cppdb/defs.h>
#include <cppdb/errors.h>
#include <cppdb/ref_ptr.h>

#include <cppdb/utils.h>

#include <map>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>

namespace cppdb {
namespace schema {

	typedef std::map<std::string, std::string> params_type;
	
	class schema;
	class entity;

	class CPPDB_API column : public ref_counted {
	protected:
		virtual void init(params_type const &params)
		{
			data_type_ = "";
			size_[0] = size_[1] = -1;
			is_nullable_ = 1;
			is_nullable_specified_ = 0;
			is_auto_increment_ = 0;
			is_numeric_ = is_foreign_key_ = 0;
			default_value_ = "";
			default_value_specified_ = 0; 
			sequence_ = "";
			retrieve_on_insert_ = 0;
			auto_nextval_ = 0;
			domain_ = {};
			
			params_type::const_iterator p;
			for (p = params.begin(); p != params.end(); p++) {
				if (p->first == "data_type") {
					data_type_ = p->second;
				}
				if (p->first == "size") {
					try {
						char comma;
						std::istringstream ss(p->second);
						ss >> size_[0];
						if (!ss.eof()) {
							ss >> comma;
							ss >> size_[1];
						}
					}
					catch (...) {
					}
				}
				if (p->first == "is_nullable") {
					is_nullable_ = std::stoi(p->second);
					is_nullable_specified_ = 1;
				}
				if (p->first == "is_auto_increment") {
					is_auto_increment_ = std::stoi(p->second);
				}
				if (p->first == "is_numeric") {
					is_numeric_ = std::stoi(p->second);
				}
				if (p->first == "is_foreign_key") {
					is_foreign_key_ = std::stoi(p->second);
				}
				if (p->first == "default_value") {
					default_value_ = std::stoi(p->second);
					default_value_specified_ = 1;
				}
				if (p->first == "sequence") {
					sequence_ = std::stoi(p->second);
				}
				if (p->first == "retrieve_on_insert") {
					retrieve_on_insert_ = std::stoi(p->second);
				}
				if (p->first == "auto_nextval") {
					auto_nextval_ = std::stoi(p->second);
				}
				if (p->first == "set_on_create") {
					set_on_create_ = std::stoi(p->second);
				}
				if (p->first == "set_on_update") {
					set_on_update_ = std::stoi(p->second);
				}
				//parse list in extra for domain
			}
			if (data_type_.empty()) {
				throw cppdb_error("cppdb::schema: no data type for column " + name_);
			}
		}
	public:
		column(std::string const &name, params_type const &params)
			: name_(name)
		{
			init(params);
		}
		~column();
		std::string const &name() const {return name_;}
		std::string data_type() const {return data_type_;}
		unsigned int const *size() const {return size_;}
		bool is_nullable() const {return is_nullable_ != 0;}
		bool is_nullable_specified() const {return is_nullable_specified_ != 0;}
		bool is_auto_increment() const {return is_auto_increment_ != 0;}
		std::string const &default_value() const {return default_value_;}
		bool default_value_specified() const {return default_value_specified_;}
		std::string const &sequence() const {return sequence_;}
		bool auto_nextval() const {return auto_nextval_ != 0;}
		bool set_on_create() const {return set_on_create_ != 0;}
		bool set_on_update() const {return set_on_update_ != 0;}
		std::vector<std::string> domain() const {return domain_;}
		properties attrs() const
		{
			properties res;
			std::string s1("1"), s0("0");
			res.set("data_type", data_type_);
			std::stringstream ss;
			if (size_[0] > 0) {
				ss << size_[0];
				if (size_[1] > 0) {
					ss << ',' << size_[1];
				}
			}
			res.set("size", ss.str());
			if (is_nullable_specified_) {
				res.set("is_nullable", is_nullable_ ? s1 : s0);
			}
			res.set("is_auto_increment", is_auto_increment_ ? s1 : s0);
			if (default_value_specified_) {
				res.set("default_value", default_value_);
			}
			res.set("sequence", sequence_);
			res.set("auto_nextval", auto_nextval_ ? s1 : s0);
			res.set("set_on_create", set_on_create_ ? s1 : s0);
			res.set("set_on_update", set_on_update_ ? s1 : s0);
			return res;
		}
	private:
		std::string name_;
		std::string data_type_;
		unsigned int size_[2];
		unsigned is_nullable_ : 1; 
		unsigned is_nullable_specified_ : 1; 
		unsigned is_auto_increment_ : 1; // about the pk
		unsigned is_numeric_ : 1; // meaningless
		unsigned is_foreign_key_ : 1; // meaningless
		std::string default_value_;
		unsigned default_value_specified_ : 1; 
		std::string sequence_;
		unsigned retrieve_on_insert_ : 1; // meaningless
		// if you do not use a trigger to get the nextval, you have to set the "sequence" value as well
		unsigned auto_nextval_ : 1; // for non pk fields
		unsigned set_on_create_ : 1;
		unsigned set_on_update_ : 1;
		unsigned reserved_ : 22;
		std::vector<std::string> domain_;
	};

	class CPPDB_API many2one_relation {
		virtual void init(params_type const &params)
		{
			join_type_ = "";
			cascade_delete_ = 0;
			cascade_update_ = 0;
			many_side_unique_ = 0;
			many_side_optional_ = 1;
			
			params_type::const_iterator p;
			for (p = params.begin(); p != params.end(); p++) {
				if (p->first == "join_type") {
					join_type_ = p->second;
				}
				if (p->first == "cascade_delete") {
					cascade_delete_ = std::stoi(p->second);
				}
				if (p->first == "cascade_update") {
					cascade_update_ = std::stoi(p->second);
				}
				if (p->first == "many_side_unique") {
					many_side_unique_ = std::stoi(p->second);
				}
				if (p->first == "many_side_optional") {
					many_side_optional_ = std::stoi(p->second);
				}
			}
		}
	public:
	
		many2one_relation(schema *s, std::string const &name, 
			std::string const &many_side, std::string const &one_side, 
			std::vector<std::pair<std::string, std::string>> field_pairs, 
			params_type const &params, 
			ref_ptr<entity> many_side_entity=0, ref_ptr<entity> one_side_entity=0)
			: schema_(s), name_(name), 
			many_side_(many_side), one_side_(one_side), field_pairs_(field_pairs), 
			many_side_entity_(many_side_entity), one_side_entity_(one_side_entity), 
			many_side_fields_{}, one_side_fields_{}
		{
			init(params);
			for (auto const &pr : field_pairs_) {
				many_side_fields_.push_back(pr.first);
				one_side_fields_.push_back(pr.second);
			}
		}
		std::string const &name() const
		{
			return name_;
		}
		entity &many_side() const
		{
			return *many_side_entity_;
		}
		entity &one_side() const
		{
			return *one_side_entity_;
		}
		std::vector<std::pair<std::string, std::string>> const &field_pairs() const
		{
			return field_pairs_;
		}
		std::vector<std::string> const &many_side_fields() const
		{
			return many_side_fields_;
		}
		std::vector<std::string> const &one_side_fields() const
		{
			return one_side_fields_;
		}
		bool link_sides();
		bool operator==(many2one_relation const &other) const
		{
			if (!name_.empty() && name_ == other.name_) {
				return true;
			}
			return 
				many_side_fields_ == other.many_side_fields_ && 
				one_side_fields_ == other.one_side_fields_;
		}
		many2one_relation complement() const
		{
			std::vector<std::pair<std::string, std::string>> field_pairs;
			for (auto const &pr : field_pairs_) {
				field_pairs.push_back(std::make_pair(pr.second, pr.first));
			}
			return many2one_relation(
				schema_, "", one_side_, many_side_, field_pairs, {}, 
				one_side_entity_, many_side_entity_);
		}
		std::string show() const
		{
			std::stringstream tmp;
			tmp << "relation ";
			if (!name_.empty()) {
				tmp << name_ << ' ';
			}
			tmp << many_side_ << '(' << str_join(many_side_fields_, ", ") << ")>";
			tmp << one_side_ << '(' << str_join(one_side_fields_, ", ") << ")";
			return tmp.str();
		}
	private:
		schema *schema_;
		std::string name_;
		std::string many_side_;
		std::string one_side_;
		std::vector<std::pair<std::string, std::string>> field_pairs_;
		unsigned cascade_delete_ : 1;
		unsigned cascade_update_ : 1;
		unsigned many_side_unique_ : 1; // if optional, nonclustered unique index, else constraint
		unsigned many_side_optional_ : 1; // the column(s) may allow nulls
		std::string join_type_; // if optional, may allow left/right outer joins
		                        // in orm
		unsigned reserved_ : 28;
		
		ref_ptr<entity> many_side_entity_;
		ref_ptr<entity> one_side_entity_;
		std::vector<std::string> many_side_fields_;
		std::vector<std::string> one_side_fields_;
	};

	// an object mapping to a table, for relationships
	class CPPDB_API entity : public ref_counted_tracing {
	public:
		entity(schema *s, std::string const &name)
			: schema_(s), name_(name) {}
		~entity();
		void add_column(column *col)
		{
			if (columns_.count(col->name())) {
				throw cppdb_error("cppdb::schema: column " + col->name() + " already in entity " + name_);
			}
			columns_[col->name()] = ref_ptr<column>(col);
			column_names_.push_back(col->name());
		}
		ref_ptr<column> &add_column(std::string const &name, params_type const &coldef)
		{
			add_column(new column(name, coldef));
			return columns_[name];
		}
		void add_columns(std::map<std::string, params_type> const &coldefs)
		{
			for (auto &row: coldefs) {
				add_column(row.first, row.second);
			}
		}
		void remove_column(std::string const &name)
		{
			try {
				columns_.erase(name);
			}
			catch (...) {}
		}
		
		void set_primary_key(std::vector<std::string> const &cols)
		{
			primary_columns_ = cols;
		}
		void add_unique_constraint(std::vector<std::string> const &cols)
		{
			add_unique_constraint("", cols);
		}
		void add_unique_constraint(std::string const &name, std::vector<std::string> const &cols)
		{
			for (auto const &pr : unique_constraints_) {
				if ((!name.empty() && pr.first == name) || pr.second == cols) {
					throw cppdb_error("cppdb::schema: unique constraint " + name + "(" + str_join(cols, ",") + ") duplicate in " + name_);
				}
			}
			unique_constraints_.push_back(std::make_pair(name, cols));
		}
		void add_unique_constraints(std::map<std::string, std::vector<std::string>> const &cols)
		{
			for (auto &row: cols) {
				add_unique_constraint(row.first, row.second);
			}
		}
		void add_unique_constraints(std::vector<std::vector<std::string>> const &cols)
		{
			for (auto &row: cols) {
				add_unique_constraint("", row);
			}
		}
		std::string name_unique_constraint(std::vector<std::string> const &colnames) const
		{
			return table_name_ + "_" + str_join(colnames, "_");
		}
		
		// belongs_to relation
		// will generate foreign keys, where the table of the entity
		// is the many side
		void belongs_to(std::string const &name, std::string const &one_side, std::vector<std::string> common_fields, params_type params={})
		{
			std::vector<std::pair<std::string, std::string>> one_many_fields = double_strings(common_fields);
			build_relation(name, name_, one_side, one_many_fields, params);
		}
		void belongs_to(std::string const &name, std::string const &one_side, std::vector<std::pair<std::string, std::string>> one_many_fields, params_type params={})
		{
			build_relation(name, name_, one_side, one_many_fields, params);
		}
		void belongs_to(std::vector<std::string> common_fields, std::string const &one_side, params_type params={})
		{
			std::vector<std::pair<std::string, std::string>> one_many_fields = double_strings(common_fields);
			build_relation("", name_, one_side, one_many_fields, params);
		}
		
		// has_many relation
		// will generate foreign keys on the remote entity
		// where the one side is the table of this entity
		void has_many(std::string const &name, std::string const &many_side, std::vector<std::string> common_fields, params_type params={})
		{
			std::vector<std::pair<std::string, std::string>> many_one_fields = double_strings(common_fields);
			build_relation(name, many_side, name_, many_one_fields, params);
		}
		void has_many(std::string const &name, std::string const &many_side, std::vector<std::pair<std::string, std::string>> many_one_fields, params_type params={})
		{
			build_relation(name, many_side, name_, many_one_fields, params);
		}
		// if many_side has unique fields named after fields in the one_side pk
		void has_many(std::string const &name, std::string const &many_side, params_type params={})
		{
			std::vector<std::pair<std::string, std::string>> many_one_fields = double_strings(primary_columns_);
			build_relation(name, many_side, name_, many_one_fields, params);
		}
		
		// has_one relation
		// similar to has_many, but the remote columns are unique
		// in their entity as unique constraints
		// invoke might_have, while ensuring !is_nullable on the many side
		void has_one(std::string const &name, std::string const &many_side, std::vector<std::string> common_fields, params_type params={})
		{
			params_type p(params);
			p["many_side_unique"] = "1";
			p["many_side_optional"] = "0";
			has_many(name, many_side, common_fields, p);
		}
		void has_one(std::string const &name, std::string const &many_side, std::vector<std::pair<std::string, std::string>> many_one_fields, params_type params={})
		{
			params_type p(params);
			p["many_side_unique"] = "1";
			p["many_side_optional"] = "0";
			has_many(name, many_side, many_one_fields, p);
		}
		// if many_side has unique fields named after fields in the one_side pk
		void has_one(std::string const &name, std::string const &many_side, params_type params={})
		{
			params_type p(params);
			p["many_side_unique"] = "1";
			p["many_side_optional"] = "0";
			has_many(name, many_side, p);
		}
		
		// might_have relation
		// it's an optional has_one relationship.
		// the many column(s) may be nullable.
		// the many uniqueness might need implementation as an unclustered index
		void might_have(std::string const &name, std::string const &many_side, std::vector<std::string> common_fields, params_type params={})
		{
			params_type p(params);
			p["many_side_unique"] = "1";
			p["many_side_optional"] = "1";
			has_many(name, many_side, common_fields, p);
		}
		void might_have(std::string const &name, std::string const &many_side, std::vector<std::pair<std::string, std::string>> many_one_fields, params_type params={})
		{
			params_type p(params);
			p["many_side_unique"] = "1";
			p["many_side_optional"] = "1";
			has_many(name, many_side, many_one_fields, p);
		}
		// if many_side has unique fields named after fields in the one_side pk
		void might_have(std::string const &name, std::string const &many_side, params_type params={})
		{
			params_type p(params);
			p["many_side_unique"] = "1";
			p["many_side_optional"] = "1";
			has_many(name, many_side, p);
		}
		
		// many_to_many relation WIP
		// needs an homologue many_to_many
		void many_to_many(std::string const &/*name*/, std::string const &/*link*/, std::string const &/*foreign_name*/)
		{
		}
		
		void table(std::string const &name)
		{
			table_name_ = name;
		}
		std::string const &name() const
		{
			return name_;
		}
		std::string const &table_name() const
		{
			return table_name_;
		}
		std::map<std::string, ref_ptr<column>> const &columns() const
		{
			return columns_;
		}
		ref_ptr<column> fetch_column(std::string const &name)
		{
			return columns_[name];
		}
		std::vector<std::string> const &column_names() const
		{
			return column_names_;
		}
		std::vector<std::string> const &primary_columns() const
		{
			return primary_columns_;
		}
		bool check_unique_columns(std::vector<std::string> const &columns) const
		{
			std::vector<std::string> cols = columns;
			std::sort(cols.begin(), cols.end());
			
			std::vector<std::string> unique = primary_columns_;
			std::sort(unique.begin(), unique.end());
			if (cols == unique) {
				return true;
			}
			
			for (auto const &pr : unique_constraints_) {
				unique = pr.second;
				std::sort(unique.begin(), unique.end());
				if (unique == cols) {
					return true;
				}
			}
			
			return false;
		}
		std::vector<many2one_relation> const &relations() const
		{
			return relations_;
		}
		std::vector<many2one_relation> const &many2one_relations() const
		{
			return many2one_relations_;
		}
		bool finalize()
		{
			if (!test_pk()) {
				return false;
			}
			link_relations();
			
			return true;
		}
		bool test_pk() const;

	protected:
		void build_relation(std::string const &name, 
			std::string const &many_side, std::string const &one_side, 
			std::vector<std::pair<std::string, std::string>> field_pairs, 
			params_type const &params={})
		{
			bool to_many = (name_ == many_side);
			//entity *many = to_many ? this : NULL;
			many2one_relation relation(
				schema_, name, many_side, one_side, field_pairs, params//, many
				);
			add_relation(relation, to_many);
		}
		void add_relation(many2one_relation const &relation, bool to_many=false)
		{
			if (has_relation(relation)) {
				return;
			}
			if (to_many) {
				many2one_relations_.push_back(relation);
			}
			else {
				relations_.push_back(relation);
			}
		}
		std::vector<std::pair<std::string, std::string>> double_strings(
			std::vector<std::string> const &source)
		{
			std::vector<std::pair<std::string, std::string>> result;
			for (auto const &str : source) {
				result.push_back(make_pair(str, str));
			}
			return result;
		}
		bool has_relation(many2one_relation const &relation) const
		{
			for (auto const &rel : relations_) {
				if (rel == relation) {
					return true;
				}
			}
			for (auto const &rel : many2one_relations_) {
				if (rel == relation) {
					return true;
				}
			}
			return false;
		}
		// later, fetch a resultset of the other type
		many2one_relation const *fetch_relation(std::string const &name) const
		{
			for (auto const &rel : relations_) {
				if (rel.name() == name) {
					return &rel;
				}
			}
			for (auto const &rel : many2one_relations_) {
				if (rel.name() == name) {
					return &rel;
				}
			}
			return NULL;
		}
		void link_relations()
		{
			for (auto &rel : relations_) {
				rel.link_sides();
			}
			for (auto &rel : many2one_relations_) {
				rel.link_sides();
			}
		}
		void complement_relations()
		{
			for (auto &rel : relations_) {
				entity &many_side = rel.many_side();
				many_side.add_relation(rel.complement(), true);
			}
		}
	private:
		schema *schema_;
		std::string name_;
		std::string table_name_;
		std::map<std::string, ref_ptr<column>> columns_;
		std::vector<std::string> column_names_;
		// our field(s) in a foreign key for self table
		// they should match the order of the primary key
		// the anonymous key will be generated, if inexistent
		std::vector<std::string> parent_columns_;
		std::vector<std::string> primary_columns_;
		std::vector<std::pair<std::string, std::vector<std::string>>> unique_constraints_;
		std::vector<many2one_relation> relations_;
		std::vector<many2one_relation> many2one_relations_;
		friend class schema;
	};

	class CPPDB_API schema {
	public:
		schema() {}
		ref_ptr<entity> &add_entity(std::string const &name)
		{
			if (entities_.count(name)) {
				throw cppdb_error("cppdb::schema: " + name + " already exists");
			}
			entities_[name] = ref_ptr<entity>(new entity(this, name));
			return entities_[name];
		}
		ref_ptr<entity> &get_entity(std::string const &name)
		{
			return entities_[name];
		}
		void finalize()
		{
			for (auto &ent : entities_) {
				ent.second->finalize();
			}
			for (auto &ent : entities_) {
				ent.second->complement_relations();
			}
		}
	private:
		std::map<std::string, ref_ptr<entity>> entities_;
	};

} // schema
} // cppdb

#endif // CPPDB_SCHEMA_H
