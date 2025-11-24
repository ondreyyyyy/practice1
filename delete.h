#ifndef DELETE_H
#define DELETE_H

#include "structures.h"

void deleteData(DatabaseManager& DBmanager, const Vector<string>& tableNames, const Vector<Condition>& conditions);

#endif