#include <fstream>
#include "ParserSQL.h"

void ParserSQL::parse(const std::string& sql_request) {
  std::regex createTableRegex(R"(CREATE\s+TABLE\s+(\w+)\s*\((.*)\);)");
  std::regex insertIntoRegex(R"(INSERT\s+INTO\s+(\w+)\s*\((.*)\)\s*VALUES\s*\((.*)\);)");
  std::regex selectRegex(R"(SELECT\s+(.*)\s+FROM\s+(\w+);)");
  std::regex deleteRegex(R"(DELETE\s+FROM\s+(\w+);)");
  std::regex dropTableRegex(R"(DROP\s+TABLE\s+(\w+)\s*;)");
  std::regex updateRegex(R"(UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?\s*;)");
  std::regex leftJoinRegex(R"(SELECT\s+(.*)\s+FROM\s+(\w+)\s+LEFT\s+JOIN\s+(\w+)\s+(ON|WHERE)\s+(.*);)");
  std::regex rightJoinRegex(R"(SELECT\s+(.*)\s+FROM\s+(\w+)\s+RIGHT\s+JOIN\s+(\w+)\s+(ON|WHERE)\s+(.*);)");
  std::regex innerJoinRegex(R"(SELECT\s+(.*)\s+FROM\s+(\w+)\s+INNER\s+JOIN\s+(\w+)\s+(ON|WHERE)\s+(.*);)");

  std::smatch match;
  if (std::regex_match(sql_request, match, createTableRegex)) {
    std::string tableName = match[1];
    std::string columns = match[2];
    processCreateTable(tableName, columns);
  } else if (std::regex_match(sql_request, match, insertIntoRegex)) {
    std::string tableName = match[1];
    std::string columnNames = match[2];
    std::string values = match[3];
    processInsertInto(tableName, columnNames, values);
  } else if (std::regex_match(sql_request, match, selectRegex)) {
    std::string columns = match[1];
    std::string tableName = match[2];
    processSelect(columns, tableName);
  } else if (std::regex_match(sql_request, match, deleteRegex)) {
    std::string tableName = match[1];
    processDelete(tableName);
  } else if (std::regex_match(sql_request, match, dropTableRegex)) {
    std::string tableName = match[1];
    processDropTable(tableName);
  } else if (std::regex_match(sql_request, match, updateRegex)) {
    std::string tableName = match[1];
    std::string columnValuePairs = match[2];
    std::string whereClause = match[3];
    processUpdate(tableName, columnValuePairs, whereClause);
  } else if (std::regex_match(sql_request, match, leftJoinRegex)) {
    std::string columns = match[1];
    std::string tableName = match[2];
    std::string joinTable = match[3];
    std::string joinCondition = match[5];
    processJoin(columns, tableName, "LEFT", joinTable, joinCondition);
  } else if (std::regex_match(sql_request, match, rightJoinRegex)) {
    std::string columns = match[1];
    std::string tableName = match[2];
    std::string joinTable = match[3];
    std::string joinCondition = match[5];
    processJoin(columns, tableName, "RIGHT", joinTable, joinCondition);
  } else if (std::regex_match(sql_request, match, innerJoinRegex)) {
    std::string columns = match[1];
    std::string tableName = match[2];
    std::string joinTable = match[3];
    std::string joinCondition = match[5];
    processJoin(columns, tableName, "INNER", joinTable, joinCondition);
  } else {
    throw std::invalid_argument("Invalid SQL statement.");
  }
}
void ParserSQL::processCreateTable(const std::string& tableName, const std::string& columns) {
  std::vector<std::string> columns_names;
  std::vector<field_types> columns_types;
  std::string primary_column_name;
  std::unordered_set<std::string> auto_incr_columns_names;
  std::unordered_set<std::string> not_null_columns_names;
  std::vector<std::pair<std::string, std::string>> foreign_columns_and_tables_names;

  std::istringstream iss(columns);
  std::vector<std::string> columnDefinitions;
  std::vector<std::string> foreignKeys;
  std::string definition;
  while (std::getline(iss, definition, ',')) {
    if (!definition.find("FOREIGN KEY")) {
      foreignKeys.push_back(definition);
    } else {
      columnDefinitions.push_back(definition);
    }
    while (std::isspace(iss.peek())) {
      iss.ignore();
    }
  }

  for (const std::string& columnDef : columnDefinitions) {
    std::istringstream issColumnDef(columnDef);
    std::vector<std::string> columnComponents;
    std::string columnComponent;
    while (issColumnDef >> columnComponent) {
      columnComponents.push_back(columnComponent);
    }
    std::string columnName = columnComponents[0];
    std::string columnType = columnComponents[1];
    columns_names.push_back(columnName);
    columns_types.push_back(get_type(columnType));
    for (size_t i = 2; i < columnComponents.size(); i++) {
      if (columnComponents[i] == "PRIMARY") {
        primary_column_name = columnName;
      } else if (columnComponents[i] == "AUTO_INCREMENT") {
        auto_incr_columns_names.insert(columnName);
      } else if (columnComponents[i] == "NULL") {
        not_null_columns_names.insert(columnName);
      }
    }
  }

  for (const std::string& foreignKey : foreignKeys) {
    std::regex foreignKeyRegex(R"(FOREIGN KEY\s*\((\w+)\)\s*REFERENCES\s*(\w+)\s*\((\w+)\))");
    std::smatch match;
    if (std::regex_search(foreignKey, match, foreignKeyRegex)) {
      std::string tableColumnName = match[1].str();
      std::string foreignTableName = match[2].str();
      foreign_columns_and_tables_names.emplace_back(tableColumnName, foreignTableName);
    } else {
      throw std::invalid_argument("Invalid FOREIGN KEY: " + foreignKey);
    }
  }

  data_base_.create_table(tableName, columns_names, columns_types, primary_column_name,
                          auto_incr_columns_names, not_null_columns_names, foreign_columns_and_tables_names);
}
void ParserSQL::processSelect(const std::string& columns, const std::string& tableName) {
  std::vector<std::string> columns_names;
  if (columns == "*") {
    data_base_.print_table(tableName);
  } else {
    std::istringstream iss_n(columns);
    std::string column_name;
    while (std::getline(iss_n, column_name, ',')) {
      columns_names.push_back(column_name);
      while (std::isspace(iss_n.peek())) {
        iss_n.ignore();
      }
    }
    Table(data_base_.get_table(tableName), columns_names).print();
  }

}
void ParserSQL::processDelete(const std::string& tableName) {
  data_base_.clear_table(tableName);
}
void ParserSQL::processDropTable(const std::string& tableName) {
  data_base_.drop_table(tableName);
}
void ParserSQL::processInsertInto(const std::string& table_name,
                                  const std::string& column_names_str,
                                  const std::string& values_str) {
  std::vector<std::string> column_names;
  std::vector<field_type> column_values;

  std::istringstream iss_n(column_names_str);
  std::string column_name;
  std::istringstream iss_v(values_str);
  std::string column_value;
  while (std::getline(iss_n, column_name, ',') and std::getline(iss_v, column_value, ',')) {
    column_names.push_back(column_name);
    field_type value = cast(column_value, data_base_.get_column_type(table_name, column_name));
    column_values.emplace_back(value);
    while (std::isspace(iss_n.peek())) {
      iss_n.ignore();
    }
    while (std::isspace(iss_v.peek())) {
      iss_v.ignore();
    }
  }

  data_base_.insert(table_name, column_names, column_values);
}
void ParserSQL::processUpdate(const std::string& tableName,
                              const std::string& columnValuePairs,
                              const std::string& whereClause) {
  std::vector<std::string> column_names;
  std::vector<field_type> values;

  std::istringstream iss(columnValuePairs);
  std::string column_value_pair;
  while (std::getline(iss, column_value_pair, ',')) {
    std::istringstream isss(column_value_pair);
    std::string column;
    std::getline(isss, column, ' ');
    column_names.push_back(column);
    while (std::isspace(isss.peek()) or isss.peek() == '=') {
      isss.ignore();
    }
    std::string new_value;
    std::getline(isss, new_value, '=');
    field_type value = cast(new_value, data_base_.get_column_type(tableName, column));
    values.push_back(value);
    while (std::isspace(iss.peek())) {
      iss.ignore();
    }
  }
  if (!whereClause.empty()) {
    std::regex whereRegex(R"((\w+)\s*([=!<>]+)\s*(\w+))");
    std::smatch whereMatch;
    std::string compare_column_name;
    std::string op;
    field_type compare_value;
    if (std::regex_search(whereClause, whereMatch, whereRegex)) {
      compare_column_name = whereMatch[1];
      op = whereMatch[2];
      compare_value = cast(whereMatch[3], data_base_.get_column_type(tableName, compare_column_name));
    } else {
      throw std::invalid_argument("Invalid WHERE construction: " + whereClause);
    }
    data_base_.update(tableName, column_names, values, compare_column_name, op, compare_value);
  } else {
    data_base_.update(tableName, column_names, values);
  }
}
void ParserSQL::processJoin(const std::string& columns,
                            std::string& table_name_first,
                            std::string joinType,
                            std::string& table_name_second,
                            const std::string& joinCondition) {
  std::vector<std::string> first_table_column_names;
  std::vector<std::string> second_table_column_names;
  std::istringstream iss_n(columns);
  std::string column_name;
  while (std::getline(iss_n, column_name, ',')) {
    std::istringstream isss(column_name);
    std::string table;
    std::getline(isss, table, '.');
    std::string column;
    std::getline(isss, column, '.');
    if (table == table_name_first) {
      first_table_column_names.push_back(column);
    } else if (table == table_name_second) {
      second_table_column_names.push_back(column);
    } else {
      throw std::invalid_argument("Invalid column name: " + column_name);
    }
    while (std::isspace(iss_n.peek())) {
      iss_n.ignore();
    }
  }

  std::regex whereRegex(R"(\s*(\w+)\.(\w+)\s*([=!<>]+)\s*(\w+)\.(\w+)\s*)");
  std::smatch whereMatch;
  std::string compare_column_name_first;
  std::string op;
  std::string compare_column_name_second;
  if (std::regex_search(joinCondition, whereMatch, whereRegex)) {
    compare_column_name_first = whereMatch[1];
    compare_column_name_first += '.';
    compare_column_name_first += whereMatch[2];
    op = whereMatch[3];
    compare_column_name_second = whereMatch[4];
    compare_column_name_second += '.';
    compare_column_name_second += whereMatch[5];
  } else {
    throw std::invalid_argument("Invalid WHERE construction: " + joinCondition);
  }
  data_base_.join(first_table_column_names,
                  second_table_column_names,
                  table_name_first,
                  table_name_second,
                  compare_column_name_first,
                  op,
                  compare_column_name_second, joinType).print();
}
field_types ParserSQL::get_type(const std::string& type) {
  if (type == "INT") {
    return field_types::Int;
  } else if (type == "BOOL") {
    return field_types::Bool;
  } else if (!type.find("VARCHAR")) {
    return field_types::Varchar;
  } else if (type == "FLOAT") {
    return field_types::Float;
  } else if (type == "DOUBLE") {
    return field_types::Double;
  } else {
    throw std::invalid_argument("Invalid type: " + type);
  }
}
field_type ParserSQL::cast(const std::string& str_val, field_types cur_type) {
  field_type value;
  if (cur_type == field_types::Int) {
    value = std::stoi(str_val);
  } else if (cur_type == field_types::Bool) {
    if (str_val == "true" or str_val == "1") {
      value = true;
    } else if (str_val == "false" or str_val == "0") {
      value = false;
    } else {
      throw std::invalid_argument("Invalid value for boolean column.");
    }
  } else if (cur_type == field_types::Float) {
    value = std::stof(str_val);
  } else if (cur_type == field_types::Double) {
    value = std::stod(str_val);
  } else if (cur_type == field_types::Varchar) {
    value = varchar(static_cast<int>(str_val.length()), str_val);
  }

  return value;
}

void ParserSQL::Load(const std::filesystem::path& path) {
  std::ifstream file(path);

  char ch;
  std::string query;
  while (file.get(ch)) {
    if (ch == ';') {
      parse(query + ch);
      query = "";
      continue;
    } else if (ch == '\n') {
      continue;
    }

    query += ch;
  }

  file.close();
}
void ParserSQL::Save(const std::filesystem::path& path) {
  std::ofstream file(path);
  data_base_.save(file);
}