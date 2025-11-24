#include <iostream>
#include <fstream>
#include <string>
#include "structures.h"
#include "lockingtable.h"

using namespace std;

bool isLockedTable(const DatabaseManager& DBmanager, const string& tableName) {
    string path = DBmanager.getSchemaName() + "/" + tableName + "/" + tableName + "_lock";

    ifstream file(path);
    if (!file.is_open()) {
        cerr << "Не удалось открыть файл блокировки!\n";
        return true;
    }

    string state;
    file >> state;

    return state == "locked";
}

void lockTable(const DatabaseManager& DBmanager, const string& tableName) {
    if (isLockedTable(DBmanager, tableName)) {
        cerr << "Таблица уже заблокирована!\n";
        return;
    }

    string path = DBmanager.getSchemaName() + "/" + tableName + "/" + tableName + "_lock";

    ofstream file(path);
    if (!file.is_open()) {
        cerr << "Не удалось открыть файл блокировки!\n";
        return;
    }

    file << "locked";
}


void unlockTable(const DatabaseManager& DBmanager, const string& tableName) {
    if (!isLockedTable(DBmanager, tableName)) {
        cerr << "Таблица уже разблокирована!\n";
        return;
    }
    
    string path = DBmanager.getSchemaName() + "/" + tableName + "/" + tableName + "_lock";

    ofstream file(path);
    if (!file.is_open()) {
        cerr << "Не удалось открыть файл блокировки!\n";
        return;
    }

    file << "unlocked";
}