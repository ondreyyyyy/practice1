#ifndef AUXILIARY_H
#define AUXILIARY_H

#include <string>
#include "Vector.h"
#include "structures.h"
#include <fstream>

Vector<std::string> splitCSV(const std::string& line);
Vector<std::string> parseValues(const std::string& query);
void writeTitle(const DatabaseManager& DBmanager, const std::string& tableName, const std::string& csvPath); // insert
int CSVcount(const string& schemaName, const string& table);
string stripTable(const string& full); // delete
int columnIndex(const Vector<string>& cols, const string& colName); // filter
int precedence(const string& op);
bool isColumnRef(const string& s);

#endif
