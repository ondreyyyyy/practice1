#include "select.h"
#include "structures.h"
#include "auxiliary.h"
#include "filter.h"
#include "Vector.h"
#include <fstream>
#include <iostream>

using namespace std;

void processLevel(
    int level,
    int tableCount,
    Vector<ifstream*>& files,
    Vector<int>& fileIndex,
    Vector<bool>& fileEnded,
    Vector<Vector<string>>& rowBuffers,
    const string& schema,
    const Vector<string>& tableNames,
    const Vector<Vector<string>>& tableColumns,
    const Vector<string>& selectColumns,
    const Vector<Condition>& conditions
)
{
    if (level == tableCount) {
        Vector<string> fullColumns;
        Vector<string> fullValues;

        // формируем полный список колонок и значений — пропускаем PK в CSV (PK в rowBuffers at index 0)
        for (int t = 0; t < tableCount; ++t) {
            size_t colsCount = tableColumns[t].get_size(); // number of user columns (no PK)
            for (size_t ci = 0; ci < colsCount; ++ci) {
                // tableColumns[t][ci] соответствует CSV column at index (ci+1)
                fullColumns.push_back(tableNames[t] + "." + tableColumns[t][ci]);

                // safety: если строка короткая — подставляем пустую
                if ((ci + 1) < rowBuffers[t].get_size())
                    fullValues.push_back(rowBuffers[t][ci + 1]);
                else
                    fullValues.push_back(string());
            }
        }

        if (!filterMatch(fullColumns, fullValues, conditions)) return;

        // вывод SELECT
        for (size_t si = 0; si < selectColumns.get_size(); ++si) {
            int idx = columnIndex(fullColumns, selectColumns[si]);
            if (idx >= 0 && idx < (int)fullValues.get_size()) cout << fullValues[idx];
            else cout << "NULL";
            if (si + 1 < selectColumns.get_size()) cout << " | ";
        }
        cout << "\n";
        return;
    }

    // читаем строки текущего уровня
    while (true) {
        if (fileEnded[level]) return;

        string line;
        if (!getline(*files[level], line)) {
            // текущий CSV закончился — открываем следующий
            files[level]->close();
            fileIndex[level]++;

            string next = schema + "/" + tableNames[level] + "/" + to_string(fileIndex[level]) + ".csv";
            files[level] = new ifstream(next);
            if (!files[level]->is_open()) {
                fileEnded[level] = true;
                return;
            }
            // пропускаем заголовок
            string hdr;
            getline(*files[level], hdr);
            // читаем первую строку данных
            if (!getline(*files[level], line)) {
                fileEnded[level] = true;
                return;
            }
        }

        if (line.empty()) continue;

        rowBuffers[level] = splitCSV(line);

        // перед рекурсией: сбросить все более глубокие уровни,
        // чтобы они начинали с 1.csv и первой data-строки
        for (int lvl = level + 1; lvl < tableCount; ++lvl) {
            if (files[lvl]) { files[lvl]->close(); delete files[lvl]; files[lvl] = nullptr; }
            fileIndex[lvl] = 1;
            fileEnded[lvl] = false;

            string path = schema + "/" + tableNames[lvl] + "/1.csv";
            files[lvl] = new ifstream(path);
            if (!files[lvl]->is_open()) {
                fileEnded[lvl] = true;
            } else {
                string hdr; getline(*files[lvl], hdr);
            }
        }

        // рекурсивно перебираем более глубокие таблицы
        processLevel(level + 1, tableCount, files, fileIndex, fileEnded, rowBuffers,
                     schema, tableNames, tableColumns, selectColumns, conditions);

        // После возврата — продолжаем чтение следующей строки текущего уровня
    }
}


void selectData(
    DatabaseManager& DBmanager,
    const Vector<string>& selectColumns,
    const Vector<string>& tableNames,
    const Vector<Condition>& conditions
)
{
    const string schema = DBmanager.getSchemaName();
    int tableCount = tableNames.get_size();
    if (tableCount == 0) {
        cerr << "Не указаны таблицы.\n";
        return;
    }

    Vector<Vector<string>> tableColumns;
    for (size_t ti = 0; ti < tableNames.get_size(); ++ti) {
        DBtable& T = DBmanager.getTable(tableNames[ti]);
        tableColumns.push_back(T.getColumns()); // tableColumns contains user columns (no PK)
    }

    Vector<int> fileIndex(tableCount, 1);
    Vector<ifstream*> files(tableCount);
    Vector<bool> fileEnded(tableCount);

    for (int i = 0; i < tableCount; ++i) {
        fileEnded[i] = false;
        string path = schema + "/" + tableNames[i] + "/1.csv";
        files[i] = new ifstream(path);
        if (!files[i]->is_open()) {
            cerr << "Ошибка открытия " << tableNames[i] << "\n";
            for (int j = 0; j < i; ++j) { if (files[j]) { files[j]->close(); delete files[j]; files[j] = nullptr; } }
            return;
        }
        string hdr; getline(*files[i], hdr); // skip header
    }

    Vector<Vector<string>> rowBuffers(tableCount); // csv rows include PK at index 0

    processLevel(0, tableCount, files, fileIndex, fileEnded, rowBuffers,
                 schema, tableNames, tableColumns, selectColumns, conditions);

    for (int i = 0; i < tableCount; ++i) {
        if (files[i]) { files[i]->close(); delete files[i]; files[i] = nullptr; }
    }
}