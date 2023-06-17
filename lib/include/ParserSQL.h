#pragma once

#include <regex>
#include <filesystem>
#include "DataBase.h"


class ParserSQL {
 public:
  void parse(const std::string& sql_request);

  void Load(const std::filesystem::path& path);
  void Save(const std::filesystem::path& path);

 private:
  DataBase data_base_;

  void processCreateTable(const std::string& tableName,
                          const std::string& columns);
  void processSelect(const std::string& columns,
                     const std::string& tableName);
  void processDelete(const std::string& tableName);
  void processDropTable(const std::string& tableName);
  void processInsertInto(const std::string& table_name,
                         const std::string& column_names_str,
                         const std::string& values_str);
  void processUpdate(const std::string& tableName,
                     const std::string& columnValuePairs,
                     const std::string& whereClause);

  void processJoin(const std::string& columns,
                   std::string& table_name_first,
                   std::string joinType,
                   std::string& table_name_second,
                   const std::string& joinCondition);

  static field_types get_type(const std::string& type);
  static field_type cast(const std::string& str_val, field_types cur_type);
};

