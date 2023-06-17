#include <iomanip>
#include <utility>
#include "DataBase.h"

std::ostream& operator<<(std::ostream& os, const varchar& v) {
  os << v.value_;
  return os;
}
bool operator==(const varchar& first, const varchar& second) {
  return first.value_ == second.value_;
}
bool operator!=(const varchar& first, const varchar& second) {
  return first.value_ != second.value_;
}
bool operator>(const varchar& first, const varchar& second) {
  return first.value_ > second.value_;
}
bool operator<(const varchar& first, const varchar& second) {
  return first.value_ < second.value_;
}
bool operator>=(const varchar& first, const varchar& second) {
  return first.value_ >= second.value_;
}
bool operator<=(const varchar& first, const varchar& second) {
  return first.value_ <= second.value_;
}

void DataBase::create_table(const std::string& table_name,
                            const std::vector<std::string>& columns_names,
                            const std::vector<field_types>& columns_types,
                            const std::string& primary_column_name,
                            const std::unordered_set<std::string>& auto_incr_columns_names,
                            const std::unordered_set<std::string>& not_null_columns_names,
                            const std::vector<std::pair<std::string, std::string>>& foreign_columns_and_tables_names) {
  if (columns_names.size() != columns_types.size()) {
    throw std::invalid_argument("Number of column names does not match number of column types.");
  }
  std::unordered_map<std::string, Table&> foreign_columns_names_and_tables;
  for (auto& it : foreign_columns_and_tables_names) {
    foreign_columns_names_and_tables.insert({it.first, get_table(it.second)});
  }
  data_structure_.insert({table_name, new Table(columns_names,
                                                columns_types,
                                                primary_column_name,
                                                auto_incr_columns_names,
                                                not_null_columns_names,
                                                foreign_columns_names_and_tables)});
}
void DataBase::insert(const std::string& table_name,
                      const std::vector<std::string>& unordered_columns,
                      const std::vector<field_type>& unordered_fields) {
  if (unordered_columns.size() != unordered_fields.size()) {
    throw std::invalid_argument("Number of column names does not match number of column values.");
  }
  for (auto& table : data_structure_) {
    if (table.first == table_name) {
      table.second->insert(unordered_columns, unordered_fields);
      break;
    }
  }
}
void DataBase::print_table(const std::string& table_name, std::ostream& stream) {
  bool flag_if_found = false;
  for (auto& table : data_structure_) {
    if (table.first == table_name) {
      flag_if_found = true;
      table.second->print(stream);
      break;
    }
  }
  if (!flag_if_found) {
    throw std::invalid_argument("Table not found.");
  }
}
Table& DataBase::get_table(const std::string& table_name) {
  auto it = data_structure_.find(table_name);
  if (it != data_structure_.end()) {
    return *(it->second);
  } else {
    throw std::invalid_argument("Table not found: " + table_name);
  }
}
void DataBase::clear_table(const std::string& table_name) {
  auto it = data_structure_.find(table_name);
  if (it == data_structure_.end()) {
    throw std::invalid_argument("Table not found: " + table_name);
  }
  Table& table = *(it->second);
  table.clear();
  std::cout << "Affected rows: " << table.rows_cnt << "\n";
  table.rows_cnt = 0;
}
void DataBase::update(const std::string& table_name,
                      const std::vector<std::string>& column_names,
                      const std::vector<field_type>& values,
                      const std::string& compare_column_name,
                      const std::string& op,
                      const field_type& compare_value) {
  get_table(table_name).update(column_names, values, compare_column_name, op, compare_value);
}
Table DataBase::join(std::vector<std::string>& first_table_column_names,
                     std::vector<std::string>& second_table_column_names,
                     std::string& table_name_first,
                     std::string& table_name_second,
                     std::string& compare_column_name_first,
                     std::string& op,
                     std::string& compare_column_name_second,
                     std::string& join_type) {
  std::istringstream iss1(compare_column_name_first);
  std::string table1;
  std::getline(iss1, table1, '.');
  std::string column1;
  std::getline(iss1, column1, '.');
  std::istringstream iss2(compare_column_name_second);
  std::string table2;
  std::getline(iss2, table2, '.');
  std::string column2;
  std::getline(iss2, column2, '.');
  if (table1 == table_name_first) {
    return Table(first_table_column_names,
                 second_table_column_names,
                 get_table(table_name_first),
                 get_table(table_name_second),
                 column1,
                 op,
                 column2,
                 join_type);
  } else {
    std::string op_new;
    if (op == "<=") {
      op_new = ">=";
    } else if (op == ">=") {
      op_new = "<=";
    } else if (op == "<") {
      op_new = ">";
    } else if (op == ">") {
      op_new = "<";
    } else {
      op_new = op;
    }
    return Table(first_table_column_names,
                 second_table_column_names,
                 get_table(table_name_first),
                 get_table(table_name_second),
                 column2,
                 op_new,
                 column1,
                 join_type);
  }

}
void DataBase::drop_table(const std::string& table_name) {
  for (auto& table_pair : data_structure_) {
    if (table_pair.first == table_name)
      delete table_pair.second;
  }
  data_structure_.erase(table_name);
}
void DataBase::save(std::ofstream& file) {
  for (auto& [name, table] : data_structure_) {
    file << "CREATE TABLE " + name + " (";
    int i = 0;
    for (auto& column : table->columns_names_) {
      file << column + " ";
      if (table->columns_types_[i] == field_types::Int) {
        file << "INT ";
      } else if (table->columns_types_[i] == field_types::Bool) {
        file << "BOOL ";
      } else if (table->columns_types_[i] == field_types::Float) {
        file << "FLOAT ";
      } else if (table->columns_types_[i] == field_types::Double) {
        file << "DOUBLE ";
      } else if (table->columns_types_[i] == field_types::Varchar) {
        file << "VARCHAR(255) ";
      }
      if (column == table->primary_column_name_) {
        file << " PRIMARY KEY ";
      }
      if (std::find(table->auto_incr_columns_names_.begin(), table->auto_incr_columns_names_.end(), column)
          != table->auto_incr_columns_names_.end()) {
        file << " AUTO_INCREMENT ";
      }
      if (std::find(table->not_null_columns_names_.begin(), table->not_null_columns_names_.end(), column)
          != table->not_null_columns_names_.end()) {
        file << " NOT NULL ";
      }
      file << ", ";
      i++;
    }
    file.seekp(-2, std::ios::cur);
    file << ");\n";
    table->save(file, name);
  }
  file.close();
}

Table::Table(const std::vector<std::string>& columns_names,
             const std::vector<field_types>& columns_types,
             std::string primary_column_name,
             std::unordered_set<std::string> auto_incr_columns_names,
             std::unordered_set<std::string> not_null_columns_names,
             std::unordered_map<std::string, Table&> foreign_columns)
    : columns_names_(columns_names),
      columns_types_(columns_types),
      primary_column_name_(std::move(primary_column_name)),
      auto_incr_columns_names_(std::move(auto_incr_columns_names)),
      not_null_columns_names_(std::move(not_null_columns_names)),
      foreign_columns_(std::move(foreign_columns)) {
  for (size_t i = 0; i < columns_names.size(); ++i) {
    const std::string& column_name = columns_names[i];
    const field_types& column_type = columns_types[i];
    if (column_type == field_types::Bool) {
      columns_bool_.emplace(column_name, Column<bool>());
    } else if (column_type == field_types::Int) {
      columns_int_.emplace(column_name, Column<int>());
    } else if (column_type == field_types::Double) {
      columns_double_.emplace(column_name, Column<double>());
    } else if (column_type == field_types::Float) {
      columns_float_.emplace(column_name, Column<float>());
    } else if (column_type == field_types::Varchar) {
      columns_varchar_.emplace(column_name, Column<varchar>());
    } else {
      throw std::invalid_argument("Invalid column type");
    }
  }
}
template<typename T>
Column<T>& Table::get_column(const std::string& column_name) {
  if constexpr (std::is_same_v<T, bool>) {
    auto it = columns_bool_.find(column_name);
    if (it != columns_bool_.end()) {
      return it->second;
    }
  } else if constexpr (std::is_same_v<T, int>) {
    auto it = columns_int_.find(column_name);
    if (it != columns_int_.end()) {
      return it->second;
    }
  } else if constexpr (std::is_same_v<T, double>) {
    auto it = columns_double_.find(column_name);
    if (it != columns_double_.end()) {
      return it->second;
    }
  } else if constexpr (std::is_same_v<T, float>) {
    auto it = columns_float_.find(column_name);
    if (it != columns_float_.end()) {
      return it->second;
    }
  } else if constexpr (std::is_same_v<T, varchar>) {
    auto it = columns_varchar_.find(column_name);
    if (it != columns_varchar_.end()) {
      return it->second;
    }
  }
  throw std::invalid_argument("Column not found: " + column_name);
}
void Table::insert(const std::vector<std::string>& unordered_columns, const std::vector<field_type>& unordered_fields) {
  for (int i = 0; i < unordered_columns.size(); ++i) {
    const std::string& column_name = unordered_columns[i];
    const field_type& column_value = unordered_fields[i];
    if (columns_bool_.count(column_name) > 0) {
      get_column<bool>(column_name).fields_.emplace_back(Field<bool>(column_value));
    } else if (columns_int_.count(column_name) > 0) {
      get_column<int>(column_name).fields_.emplace_back(Field<int>(column_value));
    } else if (columns_double_.count(column_name) > 0) {
      get_column<double>(column_name).fields_.emplace_back(Field<double>(column_value));
    } else if (columns_float_.count(column_name) > 0) {
      get_column<float>(column_name).fields_.emplace_back(Field<float>(column_value));
    } else if (columns_varchar_.count(column_name) > 0) {
      get_column<varchar>(column_name).fields_.emplace_back(Field<varchar>(column_value));
    } else {
      throw std::invalid_argument("Column not found: " + column_name);
    }
  }
  rows_cnt++;
  for (auto& it : columns_bool_) {
    insert_if_need<bool>(it);
  }
  for (auto& it : columns_int_) {
    insert_if_need<int>(it);
  }
  for (auto& it : columns_double_) {
    insert_if_need<double>(it);
  }
  for (auto& it : columns_float_) {
    insert_if_need<float>(it);
  }
  for (auto& it : columns_varchar_) {
    if (it.second.fields_.size() < rows_cnt) {
      if (not_null_columns_names_.find(it.first) != not_null_columns_names_.end()) {
        throw std::invalid_argument("Column " + it.first + " not define");
      } else {
        it.second.fields_.emplace_back(Field<varchar>(varchar(), true));
      }
    }
  }
  std::cout << "Affected rows: 1\n";
}
template<typename T>
void Table::insert_if_need(auto& it) {
  Column<T>& column = it.second;
  if (column.fields_.size() < rows_cnt) {
    if (auto_incr_columns_names_.find(it.first) != auto_incr_columns_names_.end()) {
      if (rows_cnt <= 1) {
        column.fields_.emplace_back(Field<T>(0));
      } else {
        column.fields_.emplace_back(Field<T>(column.fields_[column.fields_.size() - 1].value_ + 1));
      }
    } else if (not_null_columns_names_.find(it.first) != not_null_columns_names_.end()) {
      throw std::invalid_argument("Column " + it.first + " not define");
    } else {
      column.fields_.emplace_back(Field<T>(T(), true));
    }
  }
}
void Table::print(std::ostream& stream) {
  // Создаем вектор с ширинами столбцов, инициализированный нулями
  std::vector<int> columns_width(columns_names_.size(), 0);
  // Находим ширину для каждого столбца
  for (int i = 0; i < columns_names_.size(); ++i) {
    if (columns_types_[i] == field_types::Bool) {
      columns_width[i] = Field<bool>::kBoolWidth;
    } else if (columns_types_[i] == field_types::Int) {
      columns_width[i] = Field<int>::kIntWidth;
    } else if (columns_types_[i] == field_types::Double) {
      columns_width[i] = Field<double>::kDoubleWidth;
    } else if (columns_types_[i] == field_types::Float) {
      columns_width[i] = Field<float>::kFloatWidth;
    } else {
      columns_width[i] = Field<varchar>::kVarcharWidth;
    }
    columns_width[i] = std::max(columns_width[i], static_cast<int>(columns_names_[i].size()));
  }
  // Создаем вектор с высотой строк, инициализированный единицами
  std::vector<int> rows_height(rows_cnt, 1);
  // Находим высоту каждой строки
  for (auto& it : columns_varchar_) {
    Column<varchar>& column = it.second;
    for (int i = 0; i < column.fields_.size(); i++) {
      rows_height[i] =
          std::max(rows_height[i],
                   static_cast<int>(column.fields_[i].value_.length()) / Field<varchar>::kVarcharWidth + 1);
    }
  }
  print_frame(columns_width, stream);
  // Выводим названия столбцов
  stream << '|';
  for (size_t i = 0; i < columns_names_.size(); ++i) {
    stream << std::setw(columns_width[i]) << std::left << columns_names_[i] << '|';
  }
  stream << '\n';
  print_frame(columns_width, stream);
  int total_height;
  total_height = 0;
  for (int i : rows_height) {
    total_height += i;
  }
  std::vector<std::string> table_print(total_height, "");
  int cur_row = 0;
  for (int i = 0; i < rows_height.size(); i++) {
    for (int y = 0; y < rows_height[i]; y++) {
      table_print[cur_row + y].push_back('|');
    }
    int cur_col = 1;
    for (int j = 0; j < columns_width.size(); j++) {
      if (columns_types_[j] == field_types::Bool) {
        table_print[cur_row] += get_column<bool>(columns_names_[j]).fields_[i].to_string(columns_width[j]);
        for (int y = 1; y < rows_height[i]; y++) {
          table_print[cur_row + y] += std::string(columns_width[j], ' ');
        }
      } else if (columns_types_[j] == field_types::Int) {
        table_print[cur_row] += get_column<int>(columns_names_[j]).fields_[i].to_string(columns_width[j]);
        for (int y = 1; y < rows_height[i]; y++) {
          table_print[cur_row + y] += std::string(columns_width[j], ' ');
        }
      } else if (columns_types_[j] == field_types::Double) {
        table_print[cur_row] += get_column<double>(columns_names_[j]).fields_[i].to_string(columns_width[j]);
        for (int y = 1; y < rows_height[i]; y++) {
          table_print[cur_row + y] += std::string(columns_width[j], ' ');
        }
      } else if (columns_types_[j] == field_types::Float) {
        table_print[cur_row] += get_column<float>(columns_names_[j]).fields_[i].to_string(columns_width[j]);
        for (int y = 1; y < rows_height[i]; y++) {
          table_print[cur_row + y] += std::string(columns_width[j], ' ');
        }
      } else {
        std::string
            str = get_column<varchar>(columns_names_[j]).fields_[i].to_string(columns_width[j] * rows_height[i]);
        for (int y = 0; y < rows_height[i]; y++) {
          table_print[cur_row + y] += str.substr(y * columns_width[j], columns_width[j]);
        }
      }
      for (int y = 0; y < rows_height[i]; y++) {
        table_print[cur_row + y].push_back('|');
      }
      cur_col += columns_width[j] + 1;
    }
    cur_row += rows_height[i];
  }
  // Выводим содержимое колонок
  cur_row = 0;
  for (int i = 0; i < rows_cnt; i++) {
    for (int j = 0; j < rows_height[i]; j++, cur_row++) {
      stream << table_print[cur_row] << '\n';
    }
    print_frame(columns_width, stream);
  }
  stream << "Affected rows: " << rows_cnt << '\n';
}
void Table::print_frame(const std::vector<int>& columns_width, std::ostream& stream) const {
  stream << '+';
  for (size_t x = 0; x < columns_names_.size(); ++x) {
    stream << std::setw(columns_width[x]) << std::setfill('-') << '-' << '+';
  }
  stream << std::setfill(' ') << '\n';
}
void Table::clear() {
  for (auto& it : columns_bool_) {
    it.second.fields_.clear();
  }
  for (auto& it : columns_int_) {
    it.second.fields_.clear();
  }
  for (auto& it : columns_double_) {
    it.second.fields_.clear();
  }
  for (auto& it : columns_float_) {
    it.second.fields_.clear();
  }
  for (auto& it : columns_varchar_) {
    it.second.fields_.clear();
  }
}
void Table::update(const std::vector<std::string>& column_names,
                   const std::vector<field_type>& values,
                   const std::string& compare_column_name,
                   const std::string& op,
                   const field_type& compare_value) {
  int affected_rows_cnt = 0;
  for (int i = 0; i < rows_cnt; i++) {
    if (op.empty() or update_comparator(compare_column_name, op, compare_value, i)) {
      affected_rows_cnt++;
      for (int j = 0; j < column_names.size(); j++) {
        if (get_column_type(column_names[j]) == field_types::Int) {
          get_column<int>(column_names[j]).fields_[i].value_ = std::get<int>(values[j]);
          get_column<int>(column_names[j]).fields_[i].is_null_ = false;
        } else if (get_column_type(column_names[j]) == field_types::Bool) {
          get_column<bool>(column_names[j]).fields_[i].value_ = std::get<bool>(values[j]);
          get_column<bool>(column_names[j]).fields_[i].is_null_ = false;
        } else if (get_column_type(column_names[j]) == field_types::Float) {
          get_column<float>(column_names[j]).fields_[i].value_ = std::get<float>(values[j]);
          get_column<float>(column_names[j]).fields_[i].is_null_ = false;
        } else if (get_column_type(column_names[j]) == field_types::Double) {
          get_column<double>(column_names[j]).fields_[i].value_ = std::get<double>(values[j]);
          get_column<double>(column_names[j]).fields_[i].is_null_ = false;
        } else if (get_column_type(column_names[j]) == field_types::Varchar) {
          get_column<varchar>(column_names[j]).fields_[i].value_ = std::get<varchar>(values[j]);
          get_column<varchar>(column_names[j]).fields_[i].is_null_ = false;
        }
      }
    }
  }
  std::cout << "Affected rows: " << affected_rows_cnt << '\n';
}
field_types Table::get_column_type(const std::string& column_name) const {
  for (int i = 0; i < columns_names_.size(); i++) {
    if (columns_names_[i] == column_name) {
      return columns_types_[i];
    }
  }
  throw std::invalid_argument("Column doesnt exist: " + column_name);
}
template<typename P>
bool Table::compare(P first, P second, const std::string& op) const {
  if (op == "=") {
    return first == second;
  } else if (op == "!=" or op == "<>") {
    return first != second;
  } else if (op == ">=") {
    return first >= second;
  } else if (op == "<=") {
    return first <= second;
  } else if (op == ">") {
    return first > second;
  } else if (op == "<") {
    return first < second;
  }
  throw std::invalid_argument("Invalid operator: " + op);
}
bool Table::update_comparator(const std::string& comp_column,
                              const std::string& op,
                              field_type comp_value,
                              int i) {
  if (get_column_type(comp_column) == field_types::Int) {
    return compare<int>(get_column<int>(comp_column).fields_[i].value_, std::get<int>(comp_value), op);
  } else if (get_column_type(comp_column) == field_types::Bool) {
    return compare<bool>(get_column<bool>(comp_column).fields_[i].value_, std::get<bool>(comp_value), op);
  } else if (get_column_type(comp_column) == field_types::Float) {
    return compare<float>(get_column<float>(comp_column).fields_[i].value_, std::get<float>(comp_value), op);
  } else if (get_column_type(comp_column) == field_types::Double) {
    return compare<double>(get_column<double>(comp_column).fields_[i].value_,
                           std::get<double>(comp_value),
                           op);
  } else if (get_column_type(comp_column) == field_types::Varchar) {
    return compare<varchar>(get_column<varchar>(comp_column).fields_[i].value_,
                            std::get<varchar>(comp_value),
                            op);
  }
  throw std::invalid_argument("Invalid column type");
}
Table::Table(std::vector<std::string>& first_table_column_names,
             std::vector<std::string>& second_table_column_names,
             Table& table_first,
             Table& table_second,
             std::string& compare_column_name_first,
             std::string& op,
             std::string& compare_column_name_second,
             std::string& join_type) {
  if (join_type == "RIGHT") {
    std::swap(first_table_column_names, second_table_column_names);
    std::swap(table_first, table_second);
    std::swap(compare_column_name_first, compare_column_name_second);
    join_type = "LEFT";
    if (op == "<") op = ">";
    else if (op == ">") op = "<";
    else if (op == "<=") op = ">=";
    else if (op == ">=") op = "<=";
  }
  for (auto& column : first_table_column_names) {
    const std::string& column_name = column;
    const field_types& column_type = table_first.get_column_type(column);
    columns_names_.push_back(column);
    columns_types_.push_back(column_type);
    if (column_type == field_types::Bool) {
      columns_bool_.emplace(column_name, Column<bool>());
    } else if (column_type == field_types::Int) {
      columns_int_.emplace(column_name, Column<int>());
    } else if (column_type == field_types::Double) {
      columns_double_.emplace(column_name, Column<double>());
    } else if (column_type == field_types::Float) {
      columns_float_.emplace(column_name, Column<float>());
    } else if (column_type == field_types::Varchar) {
      columns_varchar_.emplace(column_name, Column<varchar>());
    } else {
      throw std::invalid_argument("Invalid column type");
    }
  }
  for (auto& column : second_table_column_names) {
    const std::string& column_name = column;
    const field_types& column_type = table_second.get_column_type(column);
    columns_names_.push_back(column);
    columns_types_.push_back(column_type);
    if (column_type == field_types::Bool) {
      columns_bool_.emplace(column_name, Column<bool>());
    } else if (column_type == field_types::Int) {
      columns_int_.emplace(column_name, Column<int>());
    } else if (column_type == field_types::Double) {
      columns_double_.emplace(column_name, Column<double>());
    } else if (column_type == field_types::Float) {
      columns_float_.emplace(column_name, Column<float>());
    } else if (column_type == field_types::Varchar) {
      columns_varchar_.emplace(column_name, Column<varchar>());
    } else {
      throw std::invalid_argument("Invalid column type");
    }
  }
  for (int i = 0; i < table_first.rows_cnt; i++) {
    bool left = false;
    for (int j = 0; j < table_second.rows_cnt; j++) {
      if (join_comparator(compare_column_name_first, op, compare_column_name_second, i, j, table_first, table_second)) {
        left = true;
        for (auto& column : first_table_column_names) {
          const field_types& column_type = table_first.get_column_type(column);
          if (column_type == field_types::Bool) {
            get_column<bool>(column).fields_.push_back(table_first.get_column<bool>(column).fields_[i]);
          } else if (column_type == field_types::Int) {
            get_column<int>(column).fields_.push_back(table_first.get_column<int>(column).fields_[i]);
          } else if (column_type == field_types::Double) {
            get_column<double>(column).fields_.push_back(table_first.get_column<double>(column).fields_[i]);
          } else if (column_type == field_types::Float) {
            get_column<float>(column).fields_.push_back(table_first.get_column<float>(column).fields_[i]);
          } else if (column_type == field_types::Varchar) {
            get_column<varchar>(column).fields_.push_back(table_first.get_column<varchar>(column).fields_[i]);
          }
        }
        for (auto& column : second_table_column_names) {
          const field_types& column_type = table_second.get_column_type(column);
          if (column_type == field_types::Bool) {
            get_column<bool>(column).fields_.push_back(table_second.get_column<bool>(column).fields_[j]);
          } else if (column_type == field_types::Int) {
            get_column<int>(column).fields_.push_back(table_second.get_column<int>(column).fields_[j]);
          } else if (column_type == field_types::Double) {
            get_column<double>(column).fields_.push_back(table_second.get_column<double>(column).fields_[j]);
          } else if (column_type == field_types::Float) {
            get_column<float>(column).fields_.push_back(table_second.get_column<float>(column).fields_[j]);
          } else if (column_type == field_types::Varchar) {
            get_column<varchar>(column).fields_.push_back(table_second.get_column<varchar>(column).fields_[j]);
          }
        }
        rows_cnt++;
      }
    }
    if (!left and join_type == "LEFT") {
      for (auto& column : first_table_column_names) {
        const field_types& column_type = table_first.get_column_type(column);
        if (column_type == field_types::Bool) {
          get_column<bool>(column).fields_.push_back(table_first.get_column<bool>(column).fields_[i]);
        } else if (column_type == field_types::Int) {
          get_column<int>(column).fields_.push_back(table_first.get_column<int>(column).fields_[i]);
        } else if (column_type == field_types::Double) {
          get_column<double>(column).fields_.push_back(table_first.get_column<double>(column).fields_[i]);
        } else if (column_type == field_types::Float) {
          get_column<float>(column).fields_.push_back(table_first.get_column<float>(column).fields_[i]);
        } else if (column_type == field_types::Varchar) {
          get_column<varchar>(column).fields_.push_back(table_first.get_column<varchar>(column).fields_[i]);
        }
      }
      for (auto& column : second_table_column_names) {
        const field_types& column_type = table_second.get_column_type(column);
        if (column_type == field_types::Bool) {
          get_column<bool>(column).fields_.emplace_back(bool(), true);
        } else if (column_type == field_types::Int) {
          get_column<int>(column).fields_.emplace_back(int(), true);
        } else if (column_type == field_types::Double) {
          get_column<double>(column).fields_.emplace_back(double(), true);
        } else if (column_type == field_types::Float) {
          get_column<float>(column).fields_.emplace_back(float(), true);
        } else if (column_type == field_types::Varchar) {
          get_column<varchar>(column).fields_.emplace_back(varchar(), true);
        }
      }
      rows_cnt++;
    }
  }
}

bool Table::join_comparator(const std::string& first_comp_column,
                            const std::string& op,
                            const std::string& second_comp_column,
                            int i, int j, Table& table_first, Table& table_second) const {
  if (get_column_type(first_comp_column) != get_column_type(second_comp_column)) {
    throw std::invalid_argument("Invalid columns to compare");
  }
  if (get_column_type(first_comp_column) == field_types::Int) {
    return compare<int>(table_first.get_column<int>(first_comp_column).fields_[i].value_,
                        table_second.get_column<int>(second_comp_column).fields_[j].value_,
                        op);
  } else if (get_column_type(first_comp_column) == field_types::Bool) {
    return compare<bool>(table_first.get_column<bool>(first_comp_column).fields_[i].value_,
                         table_second.get_column<bool>(second_comp_column).fields_[j].value_,
                         op);
  } else if (get_column_type(first_comp_column) == field_types::Float) {
    return compare<float>(table_first.get_column<float>(first_comp_column).fields_[i].value_,
                          table_second.get_column<float>(second_comp_column).fields_[j].value_,
                          op);
  } else if (get_column_type(first_comp_column) == field_types::Double) {
    return compare<double>(table_first.get_column<double>(first_comp_column).fields_[i].value_,
                           table_second.get_column<double>(second_comp_column).fields_[j].value_,
                           op);
  } else if (get_column_type(first_comp_column) == field_types::Varchar) {
    return compare<varchar>(table_first.get_column<varchar>(first_comp_column).fields_[i].value_,
                            table_second.get_column<varchar>(second_comp_column).fields_[j].value_,
                            op);
  }
  throw std::invalid_argument("Invalid column type");
}
Table::Table(const Table& table, const std::vector<std::string>& columns_names)
    : rows_cnt(table.rows_cnt),
      columns_names_(columns_names),
      columns_bool_(table.columns_bool_),
      columns_int_(table.columns_int_),
      columns_float_(table.columns_float_),
      columns_double_(table.columns_double_),
      columns_varchar_(table.columns_varchar_),
      auto_incr_columns_names_(table.auto_incr_columns_names_),
      not_null_columns_names_(table.not_null_columns_names_) {
  for (const std::string& column_name : columns_names) {
    columns_types_.push_back(table.get_column_type(column_name));
  }
}
void Table::save(std::ofstream& file, const std::string& name) {
  for (int i = 0; i < rows_cnt; i++) {
    file << "INSERT INTO " + name + " (";

    for (auto& column : columns_names_) {
      file << column + ", ";
    }
    file.seekp(-2, std::ios::cur);
    file << ") VALUES (";

    for (int j = 0; j < columns_types_.size(); j++) {
      if (columns_types_[j] == field_types::Int) {
        file << get_column<int>(columns_names_[j]).fields_[i].to_string() + ", ";
      } else if (columns_types_[j] == field_types::Bool) {
        file << get_column<bool>(columns_names_[j]).fields_[i].to_string() + ", ";
      } else if (columns_types_[j] == field_types::Float) {
        file << get_column<float>(columns_names_[j]).fields_[i].to_string() + ", ";
      } else if (columns_types_[j] == field_types::Double) {
        file << get_column<double>(columns_names_[j]).fields_[i].to_string() + ", ";
      } else if (columns_types_[j] == field_types::Varchar) {
        file << get_column<varchar>(columns_names_[j]).fields_[i].to_string() + ", ";
      }
    }
    file.seekp(-2, std::ios::cur);
    file << "); ";
    file << '\n';
  }
}

template<typename T>
std::string Field<T>::to_string(int str_len) {
  std::string str_field;
  if (str_len == -1) {
    str_len = get_default_string_length();
  }
  std::ostringstream oss;
  if (!is_null_) {
    if constexpr (std::is_same_v<T, bool>) {
      if (value_) {
        oss << std::setw(str_len) << std::left << "true";
      } else {
        oss << std::setw(str_len) << std::left << "false";
      }
    } else {
      oss << std::setw(str_len) << std::left << value_;
    }
  } else {
    oss << std::setw(str_len) << std::left << "NULL";
  }
  str_field = oss.str();

  return str_field;
}
template<typename T>
int Field<T>::get_default_string_length() {
  if (std::is_same_v<T, bool>) {
    return kBoolWidth;
  } else if (std::is_same_v<T, int>) {
    return kIntWidth;
  } else if (std::is_same_v<T, double>) {
    return kDoubleWidth;
  } else if (std::is_same_v<T, float>) {
    return kFloatWidth;
  } else {
    return kVarcharWidth;
  }
}

varchar::varchar(int max_length, std::string value) : max_length_(max_length), value_(std::move(value)) {
  if (value_.size() > max_length_) {
    throw std::invalid_argument("String length exceeds maximum allowed length.");
  }
}
uint64_t varchar::length() const {
  return value_.length();
}

