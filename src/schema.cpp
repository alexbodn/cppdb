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
#define CPPDB_SOURCE

#include <cppdb/defs.h>
#include <cppdb/errors.h>
#include <cppdb/ref_ptr.h>

#include <cppdb/schema.h>

#include <map>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>

#include <iostream>

namespace cppdb {
namespace schema {

		column::~column()
		{
			std::cout << "column destructor:" << name_ << std::endl;
		}

		bool many2one_relation::link_sides()
		{
			many_side_entity_ = schema_->get_entity(many_side_);
			one_side_entity_ = schema_->get_entity(one_side_);
			// test one side columns are unique, and not nullable
			if (!one_side_entity_->check_unique_columns(one_side_fields_)) {
				throw cppdb_error("cppdb::schema: one side columns are not unique " + show());
				return false;
			}
			for (auto const &col : one_side_fields_) {
				if (one_side_entity_->fetch_column(col)->is_nullable()) {
					throw cppdb_error("cppdb::schema: one side columns are nullable " + show());
					return false;
				}
			}
			// many side is unique if o2o, nullable only if optional
			if (many_side_unique_ && 
					!many_side_entity_->check_unique_columns(many_side_fields_)) {
				throw cppdb_error("cppdb::schema: many side columns should also be unique " + show());
				return false;
			}
			if (!many_side_optional_) {
				for (auto const &col : many_side_fields_) {
					if (many_side_entity_->fetch_column(col)->is_nullable()) {
						throw cppdb_error("cppdb::schema: many side columns should also not be nullable " + show());
						return false;
					}
				}
			}
			return true;
		}

		entity::~entity()
		{
			std::cout << "~entity()" << std::endl;
		}
		
		bool entity::test_pk() const
		{
			std::vector<std::string> ai;
			for (auto &col: columns_) {
				if (!col.second->is_auto_increment()) {
					continue;
				}
				ai.push_back(col.first);
			}
			for (auto &col: ai) {
				if (std::find(primary_columns_.begin(), primary_columns_.end(), col) == primary_columns_.end()) {
					throw cppdb_error("cppdb::schema: " + name_ + ": all the auto increment fields should be part of the primary key");
					return false;
				}
			}
			if (ai.size() > 1) {
				throw cppdb_error("cppdb::schema: " + name_ + ": a primary key may have only one auto increment field");
				return false;
			}
			if (ai.size() && ai != primary_columns_) {
				throw cppdb_error("cppdb::schema: " + name_ + ": the auto increment field should be the only field in the primary key");
				return false;
			}
			return true;
		}
#if 0
class CPPDB_API sqlrender {

	void init()
	{
		echo_ = false;
		
		prelude_ = "";
		postfix_ = "";
		table_sufix_ = "";
		query_prefix_ = "";
		dump_extension_ = ".sql";
		inline_domains_ = false;
		default_delimiter_ = ";"
		oncreate_inline_ = true;
		inline_timestamps_ = true;
		render_pk_ = true;
		inline_fk_ = true;
		deferred_fk_ = "";
		inline_domain_ = false;
		render_paramplace_ = "?";
		autoincrement_suffix_ = "";
		trigger_format_ = "%s";
		
		type_render_ =
		{
			{"integer", sqlrender::render_integer}, 
			{"tinyint", sqlrender::render_integer}, 
			{"smallint", sqlrender::render_integer}, 
			{"enum", sqlrender::render_string}, 
			{"float", sqlrender::render_number}, 
			{"real", sqlrender::render_number}, 
			{"decimal", sqlrender::render_number}, 
			{"numeric", sqlrender::render_number}, 
			{"char", sqlrender::render_string}, 
			{"nchar", sqlrender::render_string}, 
			{"varchar", sqlrender::render_string}, 
			{"nvarchar", sqlrender::render_string}, 
			{"text", sqlrender::render_string}, 
			{"ntext", sqlrender::render_string}, 
			{"mediumtext", sqlrender::render_string}, 
			{"date", sqlrender::render_date}, 
			{"time", sqlrender::render_time}, 
			{"datetime", sqlrender::render_datetime}, 
			{"timestamp", sqlrender::render_datetime}, 
			{"boolean", sqlrender::render_bool}, 
		};
	}
protected:
	bool echo_;
	std::string prelude_;
	std::string postfix_;
	std::string table_sufix_;
	std::string query_prefix_;
	std::string dump_extension_;
	bool inline_domains_;
	std::string default_delimiter_;
	bool oncreate_inline_;
	std::string inline_timestamps_;
	bool render_pk_;
	bool inline_fk_;
	std::string deferred_fk_;
	bool inline_domain_;
	std::string render_paramplace_;
	std::string autoincrement_suffix_;
	std::map<std::string, std::string> autoincrement_attrs_;
	properties getdate_by_type_;

	typedef std::string (*)(sqlrender const &d, std::string const &v) val_render;
	std::map<std::string, val_render> type_render_;

	properties trigger_field_action_before_;
	std::string trigger_field_action_after_;
	std::vector<std::string> on_create_trigger_template_;
	std::string on_create_trigger_;
	std::string oncreate_inline_;
	std::vector<std::string> on_update_trigger_template_;
	std::string on_update_trigger_;
	std::string onupdate_inline_;
	std::string trigger_format_;

	std::vector<std::string> triggers_;
	std::map<std::string, std::string> trigger_actions_;
	std::map<std::string, std::string> trigger_templates_;
	std::vector<std::string> check_constraints_;
	std::vector<std::string> fk_constraints_;

	virtual std::string do_render_string(std::string const &value) const
	{
		return "'" + str_replace(s, "'", "''") + "'";
	}
	virtual std::string do_render_datetime(std::string const &value) const
	{
		return render_string(value);
	}
	virtual std::string do_render_date(std::string const &value) const
	{
		return render_datetime(value);
	}
	virtual std::string do_render_time(std::string const &value) const
	{
		return render_datetime(value);
	}
	virtual std::string do_render_integer(std::string const &value) const
	{
		for (c = 0; c < value.size(); ++c) {
			if (!std::isdigit(value[c])) {
				throw cppdb_error("cppdb::schema: " + value + " is not an integer");
			}
		}
		return value;
	}
	virtual std::string do_render_number(std::string const &value) const
	{
		for (c = 0; c < value.size(); ++c) {
			if (!std::isdigit(value[c]) && value[c] != '.') {
				throw cppdb_error("cppdb::schema: " + value + " is not a number");
			}
		}
		return value;
	}
	virtual std::string do_render_bool(std::string const &value) const
	{
		return std::to_string(std::stoi(value) ? 1 : 0);
	}

	static std::string render_string(sqlrender const &s, std::string const &value)
	{
		return s.do_render_string(value);
	}
	static std::string render_datetime(sqlrender const &s, std::string const &value)
	{
		return s.do_render_datetime(value);
	}
	static std::string render_date(sqlrender const &s, std::string const &value)
	{
		return s.do_render_date(value);
	}
	static std::string render_time(sqlrender const &s, std::string const &value)
	{
		return s.do_render_time(value);
	}
	static std::string render_integer(sqlrender const &s, std::string const &value)
	{
		return s.do_render_integer(value);
	}
	static std::string render_number(sqlrender const &s, std::string const &value)
	{
		return s.do_render_number(value);
	}
	static std::string render_bool(sqlrender const &s, std::string const &value)
	{
		return s.do_render_bool(value);
	}
	std::string render_value(std::string const &data_type, std::string const &value)
	{
		val_render renderer = type_render_[data_type];
		return renderer(this, value);
	}

	virtual std::string render_concat(std::string const &left, std::string const &right) const
	{
		return std::string(left + " || " + right);
	}
	virtual std::string render_default(std::string const &value) const
	{
		return std::string("default (" + value + ")");
	}
	virtual std::string render_name(std::string const &name) const
	{
		return std::string("[" + name + "]");
	}
	virtual std::string render_unique_column(std::string const &name) const
	{
		return render_name(name);
	}

	/*
	attr_render = dict(
		data_type=(None, lambda c, x: type_render.get(x)),
		is_nullable=('required', lambda c, x: not int(x)),
		default_value=('default', None), 
		size=('size', None), 
		extra=("domain", lambda c, x: x['list']),
		set_on_create=lambda c, x: int(x),
		set_on_update=lambda c, x: int(x),
		is_auto_increment=lambda c, x: int(x), 
	)
	*/

	virtual std::string render_field(column const &col, entity const &ent, std::string const &prefix="\t") const
	{
		std::stringstream res;
		properties attrs = col.attrs();
		
		for (const std::pair<std::string, std::string> &pr : autoincrement_attrs_) {
			attrs[pr.first] = pr.second;
		}
		
		res << prefix;
		res << render_name(col.name()) << ' ';
		res << render_type(attrs["data_type"], col.size());
		
		if (col.domain()) {
			std::stringstream domain_vals;
			if (!inline_domains_) {
				domain_vals << "check (" << render_name(name) << " in ";
			}
			domain_vals << '(';
			bool first = true;
			for (const std::string &part : col.domain()) {
				if (!first) {
					domain_vals << ", ";
					first = false;
				}
				domain_vals << render_value(attrs["data_type"], part);
			}
			domain_vals << ')';
			if (!inline_domains_) {
				domain_vals << ')';
				check_constraints_.push_back(domain_vals.str());
			}
			else {
				res << domain_vals.str();
			}
		}
		
		std::string getdate = getdate_by_type_.get(attrs["data_type"], "");
		properties params = {
			{"field", name}, 
			{"getdate", getdate}, 
			{"getdate_tr", str_replace(getdate, "%", "%%")}, 
			{"table", ent.table_name()}, 
			{"default_delimiter", default_delimiter_}
		};
		
		bool nameinpk = ent.primary_columns().count(name);
		if (attrs.has("set_on_create") && nameinpk)
		{
			std::map<std::string, std::vector<std::string>> trigger_actions;
			std::map<std::string, std::string> trigger_templates;
			std::vector<std::string> triggers;
			
			if (trigger_field_action_before_) {
				std::string event = "before-insert";
				if (!trigger_actions.count(event)) {
					trigger_actions[event] = {};
				}
				trigger_actions[event].push_back(params.format(
					trigger_field_action_before_["insert"]));
			}
			if (trigger_field_action_after_) {
				std::string event = "after-insert";
				if (!trigger_actions.count(event)) {
					trigger_actions[event] = {};
				}
				trigger_actions[event].push_back(params.format(
					trigger_field_action_after_));
			}
			if (on_create_trigger_template_) {
				std::string const &stage = on_create_trigger_template_[0];
				std::string const &text = on_create_trigger_template_[1];
				trigger_templates[stage + "-insert"] = params.format(text);
			}
			if (on_create_trigger_) {
				triggers.push_back(params.format(on_create_trigger_));
			}
			if (!oncreate_inline_.empty() && self.inline_timestamps_) {
				attrs["default_rendered"] = "1";
				attrs["default_value"] = getdate_[attrs["data_type"]];
			}
			else {
				for (auto const &event_action : trigger_actions) {
				if (!trigger_actions_.count(event_action.first)) {
					trigger_actions_[event_action.first] = {};
				}
				trigger_actions_[event_action.first].push_back(event_action.second);
				for (auto const &event_template : trigger_templates) {
					trigger_templates_[event_template.first] = event_template.second;
				}
				triggers_.insert(
					triggers_.end(), triggers.begin(), triggers.end());
			}
		}
		
		if (attrs.has("default_value")) {
			if (!attrs.has("default_rendered")) {
				attrs["default_value"] = render_value(attrs["data_type"], attrs["default_value"]);
			}
			res << render_default(attrs["default_value"]);
		}
		
		if (col.is_nullable_specified()) {
			if (!col.is_nullable()) {
				res << " not";
			}
			res << " null";
		}
		
		if (attrs.has("set_on_update") && nameinpk)
		{
			std::map<std::string, std::vector<std::string>> trigger_actions;
			std::map<std::string, std::string> trigger_templates;
			std::vector<std::string> triggers;
			
			{
				bool first = true;
				std::stringstream tmp;
				for (std::string const &field : ent.column_names()) {
					if (field == name) {
						continue;
					}
					if (!first) {
						tmp << ", ";
					}
					else {
						first = false;
					}
					tmp << render_name(field);
				}
				params["other_fields"] = tmp.str();
			}
			{
				bool first = true;
				std::stringstream tmp;
				for (std::string const &field : ent.primary_columns()) {
					if (!first) {
						tmp << " and ";
					}
					else {
						first = false;
					}
					tmp << render_name("new") << '.' << render_name(field) << '=' 
						<< render_name(field);
				}
				params["where_pk"] = tmp.str();
			}
			if (trigger_field_action_before_) {
				std::string event = "before-update";
				if (!trigger_actions.count(event)) {
					trigger_actions[event] = {};
				}
				trigger_actions[event].push_back(params.format(
					trigger_field_action_before_["update"]));
			}
			if (trigger_field_action_after_) {
				std::string event = "after-insert";
				if (!trigger_actions.count(event)) {
					trigger_actions[event] = {};
				}
				trigger_actions[event].push_back(params.format(
					trigger_field_action_after_));
			}
			if (on_update_trigger_template_) {
				std::string const &stage = on_update_trigger_template_[0];
				std::string const &text = on_update_trigger_template_[1];
				trigger_templates[stage + "-insert"] = params.format(text);
			}
			if (on_update_trigger_) {
				triggers.push_back(params.format(on_update_trigger_));
			}
			if (!onupdate_inline_.empty() && self.inline_timestamps_) {
				res << params.format(onupdate_inline_);
			}
			else {
				for (auto const &event_action : trigger_actions) {
				if (trigger_actions_.count(event_action.first)) {
					trigger_actions_[event_action.first] = {};
				}
				trigger_actions_[event_action.first].push_back(event_action.second);
				for (auto const &event_template : trigger_templates) {
					trigger_templates_[event_template.first] = event_template.second;
				}
				triggers_.insert(
					triggers_.end(), triggers.begin(), triggers.end());
			}
		}
		if (autoincrement_suffix_ && col.is_auto_increment()) {
			res << ' ' << autoincrement_suffix_;
		}
		return attrs.format(res.str());
	}

	virtual std::vector<std::string> render_entity(entity const &ent, std::string const &prefix="\t", bool with_fk=true)
	{
		this_render_pk_ = render_pk_;
		triggers_ = {};
		trigger_actions_ = {};
		trigger_templates_ = {};
		check_constraints_ = {};

		int c = 0;

		std::vector<std::string> fields;
		for (auto const &col : entity.columns()) {
			fields.push_back(render_field(col.second, ent, prefix));
		}
		std::vector<std::string> constraints;
		c = 0;
		for (auto const &check : check_constraints_) {
			constraints.push_back(
				render_name("check" + std::to_string(c)) + " " + check);
			++c;
		}
		if (this_render_pk_) {
			stringstream tmp;
			bool first = true;
			tmp << render_name("pk_" + ent.table_name());
			tmp << " primary key (";
			for (auto const &col : ent.primary_columns()) {
				if (!first) {
					tmp << ", ";
				}
				else {
					first = false;
				}
				tmp << render_unique_column(col, ent);
			}
			tmp << ")";
			constraints.push_back(tmp.str());
		}
		c = 0;
		for (auto const &unique : ent.unique_constraints()) {
			stringstream tmp;
			bool first = true;
			std::string const &name = unique.first;
			std::vector<std::string> const &columns = unique.second;
			if (name.empty()) {
				tmp << render_name(ent.table_name() + "_unique" + std::to_string(c));
			}
			else {
				tmp << render_name(name);
			}
			tmp << " unique (";
			for (auto const &col : columns) {
				if (!first) {
					tmp << ", ";
				}
				else {
					first = false;
				}
				tmp << render_unique_column(col, ent);
			}
			tmp << ")";
			constraints.push_back(tmp.str());
			++c;
		}
		for (auto const & rel : ent.many2one_relations()) {
			bool first = true;
			std::string our_fields, other_fields;
			entity const &other_entity = ent.one_side();
			std::string const &other_table = other_entity.table_name();
			for (auto const &fields_pair : ent.field_pairs()) {
				if (!first) {
					our_fields += ", ";
					other_fields += ", ";
				}
				else {
					first = false;
				}
				our_fields += render_name(fields_pair.first);
				other_fields += render_name(fields_pair.second);
			}
			stringstream tmp;
			tmp << render_name("fk_" + ent.table_name() + "_" + rel.first);
			tmp << " foreign key (" << our_fields << ")";
			tmp << " references " << render_name(other_table);
			tmp << " (" << other_fields << ")" << deferred_fk_;
			std::string constraint = tmp.str();
			if (!with_fk) {
				continue;
			}
			if (!inline_fk_) {
				stringstream tmp;
				tmp << "ALTER TABLE " << render_name(ent.table_name());
				tmp << " ADD CONSTRAINT " << constraint;
				fk_constraints_.push_back(tmp.str());
			}
			else {
				constraints.push_back(constraint);
			}
		}

		std::vector<std::string> named_constraints;
		for (auto const &constraint : constraints) {
			named_constraints.push_back(prefix + "constraint " + constraint);
		}

		std::vector<std::string> create;
		{
			stringstream tmp;
			tmp << "create table " << render_name(ent.table_name()) << " ( " << std::endl;
			bool first = true;
			for (auto const &field : fields) {
				if (!first) {
					tmp << ", ";
				}
				else {
					first = false;
				}
				tmp << field;
			}
			for (auto const &constraint : named_constraints) {
				tmp << constraint;
			}
			tmp << ") " << table_sufix_;
			create.push_back(tmp.str());
		}

		c = 0;
		for (auto const &trg : trigger_templates_) {
			std::map<std::string, std::vector<std::string>>::iterator actions_it;
			actions_it = trigger_actions_.find(trg.first);
			if (actions_it != trigger_actions_.end()) {
				properties props({
					{"c", std::to_string(c)},
					{"content", str_join(actions_it.second, "\n")}
				});
				std::string trigger = props.format(trg.second);
				trigger = str_replace(trigger_format_, "%s", trigger);
				create.push_back(trigger);
				++c;
			}
		}

		int c1 = 0;
		for (auto const &trg : triggers_) {
			std::string trigger str_replace_format(
				trg, "c", std::to_string(c+c1));
			trigger = str_replace(trigger_format_, "%s", trigger);
			create.push_back(trigger);
			++c1;
		}

		return create;
	}


	virtual std::vector<std::string> render_schema(schema const &sch, std::string const &prefix='\t', std::vector<std::string> const &only_tables={}, bool with_fk=true)
	{
		std::vector<std::string> statements = {prelude_};
		fk_constraints_ = {};

		if (only_tables) {
			with_fk = false;
		}
		for (auto const &ent : schema.entities()) {
			if (!only_tables.empty() && !only_tables.count(name)) {
				continue;
			}
			std::vector<std::string> entity = render_entity(ent, prefix, with_fk);
			statements.insert(
				statements.end(), 
				std::make_move_iterator(entity.begin()),
				std::make_move_iterator(entity.end())
			);
		}
		statements.insert(
			statements.end(), 
			std::make_move_iterator(fk_constraints_.begin()),
			std::make_move_iterator(fk_constraints_.end())
		);
		statements.push_back(postfix_);

		return statements;
	}

	std::string script_combine(std::vector<std::string> const &statements)
	{
		std::string tmp;
		std::stringstream res;
		for (std::string const &statement : statements) {
			tmp = str_trim(statement);
			if (!tmp.empty() && tmp != default_delimiter_) {
				res << statement << default_delimiter_ << std::endl;
			}
		}
		return res.str();
	}

	std::string db_executelist(std::vector<std::string> const &statements)
	{
		std::string tmp;
		std::vector<std::string> res;
		for (std::string const &statement : statements) {
			tmp = str_trim(statement);
			if (!tmp.empty() && tmp != default_delimiter_ && tmp != query_prefix_) {
				res.push_back(query_prefix_);
				res.push_back(statement);
			}
		}
		return res;
	}

};

#endif //0

} // schema
} // cppdb

