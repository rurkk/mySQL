#include <iostream>
#include "ParserSQL.h"

int main() {
//  rurk::DataBase data_base;
//  data_base.create_table("books",
//                         {"title", "pages", "in stock", "book_id"},
//                         {rurk::field_types::Varchar, rurk::field_types::Int, rurk::field_types::Bool, rurk::field_types::Int},
//                         "title",{"book_id"}, {});
//  std::vector<rurk::field_type> values;
//  values.emplace_back(rurk::varchar(100, "abdiakpoekdpoakoepdkopakdokaokdoakmdocregperpglpelrferp"));
//  values.emplace_back(true);
//  data_base.insert("books", {"title", "in stock"}, values);
//  values.emplace_back(1234567);
//  data_base.insert("books", {"title", "in stock", "pages"}, values);
//  data_base.print_table("books");
//  data_base.clear_table("books");



  ParserSQL parser;

  std::string code1 = "CREATE TABLE myTable (column1 INT PRIMARY KEY AUTO_INCREMENT, column2 VARCHAR(255) NOT NULL);";
  parser.parse(code1);

  code1 = "CREATE TABLE myTable2 (column3 INT PRIMARY KEY AUTO_INCREMENT, column4 VARCHAR(255) NOT NULL, FOREIGN KEY (column3) REFERENCES myTable(column1));";
  parser.parse(code1);

  std::string code2 = "INSERT INTO myTable (column1, column2) VALUES (123, 'Hello');";
  parser.parse(code2);
  code2 = "INSERT INTO myTable (column1, column2) VALUES (13, 'Hell');";
  parser.parse(code2);
  code2 = "INSERT INTO myTable2 (column3, column4) VALUES (123, 'h i!');";
  parser.parse(code2);
  code2 = "SELECT myTable.column1, myTable.column2, myTable2.column3, myTable2.column4 FROM myTable LEFT JOIN myTable2 ON myTable.column1 = myTable2.column3;";
  parser.parse(code2);

//  std::string code3 = "SELECT column2 FROM myTable;";
//  parser.parse(code3);
//
//  std::string code7 = "SELECT * FROM myTable;";
//  parser.parse(code7);
//
//  std::string code4 = "UPDATE myTable SET column2 = 'HR' WHERE column1 = 123;";
//  parser.parse(code4);
//
//  std::string code5 = "SELECT * FROM myTable;";
//  parser.parse(code5);

  parser.Load("C:\\Users\\raulm\\labwork-12-rurkk\\cmake-build-debug\\12345");

  return 0;
}