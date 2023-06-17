#include "gtest/gtest.h"
#include "ParserSQL.h"


ParserSQL GetTestTable() {
  ParserSQL parser;

  parser.parse("CREATE TABLE Customers (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL, age FLOAT, male BOOL);");

  parser.parse("INSERT INTO Customers (name, age, male) VALUES ('Michael Scott', 41.3, true);");
  parser.parse("INSERT INTO Customers (name) VALUES ('Dwight Schrute');");
  parser.parse("INSERT INTO Customers (name, age) VALUES ('Jim Halpert', 29.1);");
  parser.parse("INSERT INTO Customers (name, male) VALUES ('Pam Beesly', false);");

  return parser;
}

TEST(DBTestSuite, CreateTableTest) {
  ParserSQL parser;

  parser.parse("CREATE TABLE Customers (id INT, name VARCHAR(255), age INT, male BOOL);");
}

TEST(DBTestSuite, CreateTableWithPkAiTest) {
  ParserSQL parser;

  parser.parse("CREATE TABLE Customers (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255), age INT, male BOOL);");
}

TEST(DBTestSuite, CreateTableWithNnTest) {
  ParserSQL parser;

  parser.parse("CREATE TABLE Customers (id INT, name VARCHAR(255), age INT NOT NULL, male BOOL);");
}

TEST(DBTestSuite, InsertTest) {
  ParserSQL parser;

  parser.parse("CREATE TABLE Customers (id INT, name VARCHAR(255), age INT, male BOOL);");

  parser.parse("INSERT INTO Customers (id, name, age, male) VALUES (1, 'Bob', 11, true);");
  parser.parse("INSERT INTO Customers (id, name) VALUES (2, 'Dwight Schrute');");
  parser.parse("INSERT INTO Customers (id, name, age) VALUES (3, 'Jim Halpert', 29);");
  parser.parse("INSERT INTO Customers (id, name, male) VALUES (4, 'Pam Beesly', false);");
}

TEST(DBTestSuite, InsertWithPkAiNnTest) {
  ParserSQL parser;

  parser.parse("CREATE TABLE Customers (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255) NOT NULL, age FLOAT, male BOOL);");

  parser.parse("INSERT INTO Customers (name, age, male) VALUES ('Michael Scott', 41.3, true);");
  parser.parse("INSERT INTO Customers (name) VALUES ('Dwight Schrute');");
  parser.parse("INSERT INTO Customers (name, age) VALUES ('Jim Halpert', 29.1);");
  parser.parse("INSERT INTO Customers (name, male) VALUES ('Pam Beesly', false);");
}

TEST(DBTestSuite, Select1Test) {
  ParserSQL parser = GetTestTable();

  parser.parse("SELECT * FROM Customers;");
}

TEST(DBTestSuite, Select2Test) {
  ParserSQL parser = GetTestTable();

  parser.parse("SELECT name, age FROM Customers;");
}

TEST(DBTestSuite, DeleteTest) {
  ParserSQL parser = GetTestTable();
  parser.parse("SELECT * FROM Customers;");
  parser.parse("DELETE FROM Customers;");
  parser.parse("SELECT * FROM Customers;");
}

TEST(DBTestSuite, DropTest) {
  ParserSQL parser = GetTestTable();

  parser.parse("DROP TABLE Customers;");
}

TEST(DBTestSuite, UpdateTest) {
  ParserSQL parser = GetTestTable();
  parser.parse("SELECT * FROM Customers;");
  parser.parse("UPDATE Customers SET male = true WHERE age > 20;");
  parser.parse("SELECT * FROM Customers;");
}

TEST(DBTestSuite, InnerJoinTest) {
  ParserSQL parser = GetTestTable();
  parser.parse("CREATE TABLE myTable (column1 INT PRIMARY KEY AUTO_INCREMENT, column2 VARCHAR(255) NOT NULL);");
  parser.parse("CREATE TABLE myTable2 (column3 INT PRIMARY KEY AUTO_INCREMENT, column4 VARCHAR(255) NOT NULL, FOREIGN KEY (column3) REFERENCES myTable(column1));");
  parser.parse("INSERT INTO myTable (column1, column2) VALUES (123, 'Hello');");
  parser.parse("INSERT INTO myTable (column1, column2) VALUES (123, 'Hola');");
  parser.parse("INSERT INTO myTable (column1, column2) VALUES (13, 'Hell');");
  parser.parse("INSERT INTO myTable (column1, column2) VALUES (2, 'aaa');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (123, 'hi!');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (123, 'hehe');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (123, 'haha');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (1, 'aaaa');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (2, 'aaaa');");

  parser.parse("SELECT * FROM myTable;");
  parser.parse("SELECT * FROM myTable2;");

  parser.parse("SELECT myTable.column1, myTable.column2, myTable2.column3, myTable2.column4 FROM myTable INNER JOIN myTable2 ON myTable.column1 = myTable2.column3;");
}

TEST(DBTestSuite, LeftJoinTest) {
  ParserSQL parser = GetTestTable();
  parser.parse("CREATE TABLE myTable (column1 INT PRIMARY KEY AUTO_INCREMENT, column2 VARCHAR(255) NOT NULL);");
  parser.parse("CREATE TABLE myTable2 (column3 INT PRIMARY KEY AUTO_INCREMENT, column4 VARCHAR(255) NOT NULL, FOREIGN KEY (column3) REFERENCES myTable(column1));");
  parser.parse("INSERT INTO myTable (column1, column2) VALUES (123, 'Hello');");
  parser.parse("INSERT INTO myTable (column1, column2) VALUES (123, 'Hola');");
  parser.parse("INSERT INTO myTable (column1, column2) VALUES (13, 'Hell');");
  parser.parse("INSERT INTO myTable (column1, column2) VALUES (2, 'aaa');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (123, 'hi!');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (123, 'hehe');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (123, 'haha');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (1, 'aaaa');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (2, 'aaaa');");

  parser.parse("SELECT * FROM myTable;");
  parser.parse("SELECT * FROM myTable2;");

  parser.parse("SELECT myTable.column1, myTable.column2, myTable2.column3, myTable2.column4 FROM myTable LEFT JOIN myTable2 ON myTable.column1 = myTable2.column3;");
}

TEST(DBTestSuite, RightJoinTest) {
  ParserSQL parser = GetTestTable();
  parser.parse("CREATE TABLE myTable (column1 INT PRIMARY KEY AUTO_INCREMENT, column2 VARCHAR(255) NOT NULL);");
  parser.parse("CREATE TABLE myTable2 (column3 INT PRIMARY KEY AUTO_INCREMENT, column4 VARCHAR(255) NOT NULL, FOREIGN KEY (column3) REFERENCES myTable(column1));");
  parser.parse("INSERT INTO myTable (column1, column2) VALUES (123, 'Hello');");
  parser.parse("INSERT INTO myTable (column1, column2) VALUES (123, 'Hola');");
  parser.parse("INSERT INTO myTable (column1, column2) VALUES (13, 'Hell');");
  parser.parse("INSERT INTO myTable (column1, column2) VALUES (2, 'aaa');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (123, 'hi!');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (123, 'hehe');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (123, 'haha');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (1, 'aaaa');");
  parser.parse("INSERT INTO myTable2 (column3, column4) VALUES (2, 'aaaa');");

  parser.parse("SELECT * FROM myTable;");
  parser.parse("SELECT * FROM myTable2;");

  parser.parse("SELECT myTable.column1, myTable.column2, myTable2.column3, myTable2.column4 FROM myTable RIGHT JOIN myTable2 ON myTable.column1 = myTable2.column3;");
}