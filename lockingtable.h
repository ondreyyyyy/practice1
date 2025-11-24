#ifndef LOCKINGTABLE_H
#define LOCKINGTABLE_H

#include <string>
#include "structures.h"

using namespace std;

bool isLockedTable(const DatabaseManager& DBmanager, const string& tableName);
void lockTable(const DatabaseManager& DBmanager, const string& tableName);
void unlockTable(const DatabaseManager& DBmanager, const string& tableName);

#endif