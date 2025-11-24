#include "interface.h"
#include "structures.h"
#include "file.h"
#include "lockingtable.h"
#include "insert.h"
#include "select.h"
#include "delete.h"
#include "auxiliary.h"
#include "Vector.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <filesystem>

using namespace std;
namespace fs = std::filesystem;

Vector<Condition> parseConditions(const string& where) {
    Vector<Condition> result;
    if (where.empty()) return result;

    string cur;
    bool inStr = false;
    Vector<string> tokens;
    for (size_t i = 0; i < where.size(); ++i) {
        char c = where[i];
        if (c == '\'') {
            cur += c;
            inStr = !inStr;
            continue;
        }
        if (inStr) {
            cur += c;
            continue;
        }
        if (c == '(' || c == ')') {
            if (!cur.empty()) { tokens.push_back(cur); cur.clear(); }
            string s(1, c);
            tokens.push_back(s);
            continue;
        }
        if (isspace((unsigned char)c)) {
            if (!cur.empty()) { tokens.push_back(cur); cur.clear(); }
            continue;
        }
        cur += c;
    }
    if (!cur.empty()) tokens.push_back(cur);

    for (size_t i = 0; i < tokens.get_size(); ++i) {
        string tk = tokens[i];
        if (tk == "(" || tk == ")") {
            Condition c; c.column = tk; result.push_back(c);
            continue;
        }
        if (tk == "AND" || tk == "OR") {
            Condition c; c.logicalOperator = tk; result.push_back(c);
            continue;
        }
        if (i + 2 < tokens.get_size() && tokens[i+1] == "=") {
            Condition c;
            c.column = tokens[i]; 
            string right = tokens[i+2];
            if (right.size() >= 2 && right.front() == '\'' && right.back() == '\'') {
                right = right.substr(1, right.size() - 2);
            }
            c.value = right;
            result.push_back(c);
            i += 2;
            continue;
        }
    }

    return result;
}

void executeInsert(DatabaseManager& dbManager, const string& command) {
    size_t intoPos = command.find("INTO ");
    if (intoPos == string::npos) {
        cout << "Ошибка: Неверный формат INSERT команды\n";
        return;
    }
    
    size_t valuesPos = command.find("VALUES");
    if (valuesPos == string::npos) {
        cout << "Ошибка: Неверный формат INSERT команды\n";
        return;
    }
    
    string tableName = command.substr(intoPos + 5, valuesPos - intoPos - 6);
    string values = command.substr(valuesPos + 7);
    
    if (values.front() == '(' && values.back() == ')') {
        values = values.substr(1, values.length() - 2);
    }
    
    if (isLockedTable(dbManager, tableName)) {
        cout << "Ошибка: Таблица '" << tableName << "' заблокирована\n";
        return;
    }
    
    lockTable(dbManager, tableName);
    
    try {
        string pkPath = dbManager.getSchemaName() + "/" + tableName + "/" + tableName + "_pk_sequence";
        ifstream pkFile(pkPath);
        int pk = 1;
        if (pkFile.is_open()) {
            pkFile >> pk;
            pkFile.close();
        }
        
        insertData(dbManager, tableName, values, pk);
    } catch (const exception& e) {
        cout << "Ошибка при вставке: " << e.what() << "\n";
    }
    
    unlockTable(dbManager, tableName);
}

void executeSelect(DatabaseManager& dbManager, const string& command) {
    size_t fromPos = command.find("FROM ");
    if (fromPos == string::npos) {
        cout << "Ошибка: Неверный формат SELECT команды\n";
        return;
    }
    
    string selectPart = command.substr(7, fromPos - 7);
    string rest = command.substr(fromPos + 5);
    
    Vector<string> selectColumns;
    if (selectPart != "*") {
        stringstream ss(selectPart);
        string column;
        while (getline(ss, column, ',')) {
            size_t start = column.find_first_not_of(' ');
            size_t end = column.find_last_not_of(' ');
            if (start != string::npos && end != string::npos) {
                selectColumns.push_back(column.substr(start, end - start + 1));
            }
        }
    }
    
    size_t wherePos = rest.find("WHERE ");
    Vector<string> tableNames;
    string whereClause;
    
    if (wherePos != string::npos) {
        string tablesPart = rest.substr(0, wherePos);
        whereClause = rest.substr(wherePos + 6);
        
        stringstream ss(tablesPart);
        string table;
        while (getline(ss, table, ',')) {
            size_t start = table.find_first_not_of(' ');
            size_t end = table.find_last_not_of(' ');
            if (start != string::npos && end != string::npos) {
                tableNames.push_back(table.substr(start, end - start + 1));
            }
        }
    } else {
        stringstream ss(rest);
        string table;
        while (getline(ss, table, ',')) {
            size_t start = table.find_first_not_of(' ');
            size_t end = table.find_last_not_of(' ');
            if (start != string::npos && end != string::npos) {
                tableNames.push_back(table.substr(start, end - start + 1));
            }
        }
    }
    
    for (size_t i = 0; i < tableNames.get_size(); i++) {
        if (isLockedTable(dbManager, tableNames[i])) {
            cout << "Ошибка: Таблица '" << tableNames[i] << "' заблокирована\n";
            return;
        }
    }
    
    for (size_t i = 0; i < tableNames.get_size(); i++) {
        lockTable(dbManager, tableNames[i]);
    }
    
    try {
        Vector<Condition> conditions = parseConditions(whereClause);
        
        if (selectColumns.empty()) {
            for (size_t ti = 0; ti < tableNames.get_size(); ti++) {
                DBtable& table = dbManager.getTable(tableNames[ti]);
                const Vector<string>& cols = table.getColumns();
                for (size_t ci = 0; ci < cols.get_size(); ci++) {
                    selectColumns.push_back(tableNames[ti] + "." + cols[ci]);
                }
            }
        }
        
        for (size_t i = 0; i < selectColumns.get_size(); i++) {
            cout << selectColumns[i];
            if (i + 1 < selectColumns.get_size()) {
                cout << " | ";
            }
        }
        cout << "\n";
        
        selectData(dbManager, selectColumns, tableNames, conditions);
        
    } catch (const exception& e) {
        cout << "Ошибка при выборке: " << e.what() << "\n";
    }
    
    for (size_t i = 0; i < tableNames.get_size(); i++) {
        unlockTable(dbManager, tableNames[i]);
    }
}

void executeDelete(DatabaseManager& dbManager, const string& command) {
    size_t fromPos = command.find("FROM ");
    if (fromPos == string::npos) {
        cout << "Ошибка: Неверный формат DELETE команды\n";
        return;
    }
    
    size_t wherePos = command.find("WHERE ");
    if (wherePos == string::npos) {
        cout << "Ошибка: DELETE требует условие WHERE\n";
        return;
    }
    
    string tableName = command.substr(fromPos + 5, wherePos - fromPos - 6);
    string whereClause = command.substr(wherePos + 6);
    
    size_t start = tableName.find_first_not_of(' ');
    size_t end = tableName.find_last_not_of(' ');
    if (start != string::npos && end != string::npos) {
        tableName = tableName.substr(start, end - start + 1);
    }
    
    if (isLockedTable(dbManager, tableName)) {
        cout << "Ошибка: Таблица '" << tableName << "' заблокирована\n";
        return;
    }
    
    lockTable(dbManager, tableName);
    
    try {
        Vector<Condition> conditions = parseConditions(whereClause);
        Vector<string> tableNames;
        tableNames.push_back(tableName);
        
        deleteData(dbManager, tableNames, conditions);
        
    } catch (const exception& e) {
        cout << "Ошибка при удалении: " << e.what() << "\n";
    }
    
    unlockTable(dbManager, tableName);
}

void processCommand(DatabaseManager& dbManager, const string& command) {
    string upperCommand = command;
    for (char& c : upperCommand) {
        c = toupper(c);
    }
    
    if (upperCommand.find("INSERT") == 0) {
        executeInsert(dbManager, command);
    } else if (upperCommand.find("SELECT") == 0) {
        executeSelect(dbManager, command);
    } else if (upperCommand.find("DELETE") == 0) {
        executeDelete(dbManager, command);
    } else if (upperCommand == "EXIT" || upperCommand == "QUIT") {
        cout << "Выход из СУБД\n";
        exit(0);
    } else if (upperCommand == "HELP") {
        cout << "Доступные команды:\n";
        cout << "INSERT INTO table_name VALUES ('value1', 'value2', ...)\n";
        cout << "SELECT col1, col2 FROM table1, table2 WHERE condition\n";
        cout << "DELETE FROM table_name WHERE condition\n";
        cout << "EXIT или QUIT - выход из программы\n";
        cout << "HELP - показать эту справку\n";
    } else {
        cout << "Ошибка: Неизвестная команда. Введите HELP для списка команд.\n";
    }
}

void runDatabaseInterface() {
    string schemaFile;
    cout << "Введите название JSON файла со схемой: ";
    getline(cin, schemaFile);

    DatabaseManager dbManager;
    loadSchema(dbManager, schemaFile);

    string schemaFolder = dbManager.getSchemaName();

    if (!fs::exists(schemaFolder)) {
        createFileStruct(dbManager);
    } else {
        cout << "Папка схемы уже существует.\n";
    }

    cout << "Схема '" << dbManager.getSchemaName() << "' успешно загружена!\n";
    cout << "Введите команды СУБД:\n";

    string command;
    while (true) {
        cout << "> ";
        getline(cin, command);
        if (command.empty()) continue;
        processCommand(dbManager, command);
    }
}