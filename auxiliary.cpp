#include "auxiliary.h"
#include "structures.h"
#include "Vector.h"
#include <iostream>
#include <fstream>

using namespace std;

Vector<string> splitCSV(const string& line) {
    Vector<string> res;
    string cell;
    for (char c : line) {
        if (c == ',') {
            res.push_back(cell);
            cell.clear();
        } else {
            cell += c;
        }
    }
    res.push_back(cell);
    return res;
}

Vector<string> parseValues(const string& query) {
    Vector<string> values;
    string cur;
    bool inside = false;

    for (char c : query) {
        if (c == '\'') {
            inside = !inside;
            if (!inside && !cur.empty()) {
                values.push_back(cur);
                cur.clear();
            }
        } else if (inside) {
            cur += c;
        }
    }
    return values;
}

void writeTitle(const DatabaseManager& DBmanager, const string& tableName, const string& csvPath) {
    const DBtable& tbl = DBmanager.getTable(tableName);  

    const Vector<string>& cols = tbl.getColumns();
    if (cols.get_size() == 0) {
        cerr << "У таблицы нет колонок.\n";
        return;
    }

    ofstream out(csvPath);
    if (!out.is_open()) {
        cerr << "Ошибка создания CSV файла.\n";
        return;
    }

    for (int i = 0; i < cols.get_size(); i++) {
        if (i > 0) out << ",";
        out << cols[i];
    }
    out << "\n";
}


int CSVcount(const string& schemaName, const string& table) {
    int count = 0;
    while (true) {
        string path = schemaName + "/" + table + "/" + to_string(count + 1) + ".csv";
        ifstream f(path);
        if (!f.is_open()) break;
        count++;
    }
    return count;
}

string stripTable(const string& full) {
    size_t pos = full.find('.');
    if (pos == string::npos) return full;
    return full.substr(pos + 1);
}

int columnIndex(const Vector<string>& cols, const string& colName) {
    for (size_t i = 0; i < cols.get_size(); i++) {
        if (cols[i] == colName) {
            return (int)i;
        }
    }
    return -1;
}

int precedence(const string& op) {
    if (op == "AND") return 2;
    if (op == "OR")  return 1;
    return 0;
}

bool isColumnRef(const string& s) {
    return s.find('.') != string::npos;
}