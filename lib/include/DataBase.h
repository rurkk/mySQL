#pragma once

#include <unordered_map>
#include <unordered_set>
#include <string>
#include <utility>
#include <vector>
#include <variant>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <fstream>

struct varchar {
  std::string value_{};
  int max_length_;
  explicit varchar(int max_length = 255, std::string value = "");
  [[nodiscard]] uint64_t length() const;
};

using field_type = std::variant<bool, int, double, float, varchar>;
enum field_types { Bool, Int, Double, Float, Varchar };

template<typename T>
struct Field {
  T value_;
  bool is_null_;
  explicit Field(field_type value, bool is_null = false) : value_(std::get<T>(value)), is_null_(is_null) {}
  explicit Field(T value, bool is_null = false) : value_(value), is_null_(is_null) {}
  ~Field() = default;
  static const uint8_t kBoolWidth = 5;
  static const uint8_t kIntWidth = 10;
  static const uint8_t kDoubleWidth = 17;
  static const uint8_t kFloatWidth = 10;
  static const uint8_t kVarcharWidth = 20;
  std::string to_string(int str_len = -1);
  int get_default_string_length();
};

template<typename T>
struct Column {
  std::vector<Field<T>> fields_;
  explicit Column(const std::vector<Field<T>>& fields = {}) : fields_(fields) {}
  ~Column() = default;
};

struct Table {
  int rows_cnt = 0;
  std::vector<std::string> columns_names_;
  std::vector<field_types> columns_types_;
  std::unordered_set<std::string> auto_incr_columns_names_;
  std::unordered_set<std::string> not_null_columns_names_;
  std::unordered_map<std::string, Column<bool>> columns_bool_;
  std::unordered_map<std::string, Column<int>> columns_int_;
  std::unordered_map<std::string, Column<double>> columns_double_;
  std::unordered_map<std::string, Column<float>> columns_float_;
  std::unordered_map<std::string, Column<varchar>> columns_varchar_;
  std::string primary_column_name_;
  std::unordered_map<std::string, Table&> foreign_columns_; //column name and main table

  Table(const std::vector<std::string>& columns_names,
        const std::vector<field_types>& columns_types,
        std::string primary_column_name,
        std::unordered_set<std::string> auto_incr_columns_names,
        std::unordered_set<std::string> not_null_columns_names,
        std::unordered_map<std::string, Table&> foreign_columns);
  Table(const Table& table, const std::vector<std::string>& columns_names);
  Table(std::vector<std::string>& first_table_column_names,
        std::vector<std::string>& second_table_column_names,
        Table& table_first,
        Table& table_second,
        std::string& compare_column_name_first,
        std::string& op,
        std::string& compare_column_name_second,
        std::string& join_type);
  ~Table() = default;

  template<typename T>
  Column<T>& get_column(const std::string& column_name);

  void insert(const std::vector<std::string>& unordered_columns, const std::vector<field_type>& unordered_fields);

  template<typename T>
  void insert_if_need(auto& it);

  void print(std::ostream& stream = std::cout);

  void print_frame(const std::vector<int>& columns_width, std::ostream& stream) const;

  void clear();

  field_types get_column_type(const std::string& column_name) const;

  void update(const std::vector<std::string>& column_names, const std::vector<field_type>& values,
              const std::string& compare_column_name, const std::string& op, const field_type& compare_value);

  bool update_comparator(const std::string& comp_column, const std::string& op, field_type comp_value, int i);

  bool join_comparator(const std::string& first_comp_column, const std::string& op,
                       const std::string& second_comp_column, int i, int j,
                       Table& table_first, Table& table_second) const;

  template<typename P>
  bool compare(P first, P second, const std::string& op) const;

  void save(std::ofstream& file, const std::string& name);
};

class DataBase {
 private:
  std::unordered_map<std::string, Table*> data_structure_{};
 public:
  DataBase() = default;
  ~DataBase() {
    for (auto& table_pair : data_structure_) {
      delete table_pair.second;
    }
  }

  void create_table(const std::string& table_name,
                    const std::vector<std::string>& columns_names,
                    const std::vector<field_types>& columns_types,
                    const std::string& primary_column_name = "",
                    const std::unordered_set<std::string>& auto_incr_columns_names = {},
                    const std::unordered_set<std::string>& not_null_columns_names = {},
                    const std::vector<std::pair<std::string, std::string>>& foreign_columns_and_tables_names = {});

  Table& get_table(const std::string& table_name);

  void insert(const std::string& table_name,
              const std::vector<std::string>& unordered_columns,
              const std::vector<field_type>& unordered_fields);

  void clear_table(const std::string& table_name);

  void drop_table(const std::string& table_name);

  void print_table(const std::string& table_name, std::ostream& stream = std::cout);
  field_types get_column_type(const std::string& table_name, const std::string& column_name) {
    return get_table(table_name).get_column_type(column_name);
  }
  void update(const std::string& table_name,
              const std::vector<std::string>& column_names,
              const std::vector<field_type>& values,
              const std::string& compare_column_name = "",
              const std::string& op = "",
              const field_type& compare_value = false);

  Table join(std::vector<std::string>& first_table_column_names,
             std::vector<std::string>& second_table_column_names,
             std::string& table_name_first,
             std::string& table_name_second,
             std::string& compare_column_name_first,
             std::string& op,
             std::string& compare_column_name_second,
             std::string& join_type);

  void save(std::ofstream& file);
};

