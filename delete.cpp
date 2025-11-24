#include "delete.h"
#include "structures.h"
#include "auxiliary.h"
#include "Vector.h"
#include "filter.h"
#include <fstream>
#include <iostream>

using namespace std;

void deleteData(
    DatabaseManager& DBmanager,
    const Vector<string>& tableNames,
    const Vector<Condition>& conditions)
{
    const string schema = DBmanager.getSchemaName();

    for (size_t ti = 0; ti < tableNames.get_size(); ti++)
    {
        const string& tableName = tableNames[ti];
        DBtable& table = DBmanager.getTable(tableName);

        // === Готовим условия в том же формате, что для SELECT ===
        Vector<Condition> localCond = conditions;

        for (size_t ci = 0; ci < localCond.get_size(); ci++)
        {
            Condition& cond = localCond[ci];

            // Пропускаем служебные элементы
            if (cond.column == "(" || cond.column == ")" ||
                cond.logicalOperator == "AND" ||
                cond.logicalOperator == "OR")
                continue;

            // Убираем имя таблицы из колонки
            string col = stripTable(cond.column);
            cond.column = col; // Используем только имя колонки без таблицы

            // Обрабатываем значение
            if (isColumnRef(cond.value))
            {
                // Если значение - ссылка на колонку, убираем имя таблицы
                string rightCol = stripTable(cond.value);
                cond.value = rightCol;
            }
            else
            {
                // Удаляем кавычки, если они есть
                if (!cond.value.empty() &&
                    cond.value.front() == '\'' &&
                    cond.value.back() == '\'')
                {
                    cond.value = cond.value.substr(1, cond.value.size() - 2);
                }
            }
        }

        int fileIndex = 1;
        bool deletedAny = false;

        // Обрабатываем все CSV таблицы
        while (true)
        {
            string csvPath = schema + "/" + tableName + "/" + to_string(fileIndex) + ".csv";

            ifstream in(csvPath);
            if (!in.is_open())
                break;

            string tempPath = csvPath + ".tmp";
            ofstream out(tempPath);
            if (!out.is_open())
            {
                cerr << "Ошибка создания временного файла.\n";
                return;
            }

            // ===== ЧИТАЕМ ЗАГОЛОВОК =====
            string header;
            if (!getline(in, header))
            {
                // Пустая таблица
                in.close();
                out.close();
                remove(csvPath.c_str());
                rename(tempPath.c_str(), csvPath.c_str());
                fileIndex++;
                continue;
            }

            out << header << "\n";

            Vector<string> headerCols = splitCSV(header);
            size_t colCount = headerCols.get_size();

            string line;

            // ===== ЧИТАЕМ ДАННЫЕ =====
            while (getline(in, line))
            {
                if (line.empty())
                    continue;

                Vector<string> values = splitCSV(line);

                if (values.get_size() < colCount)
                {
                    out << line << "\n";
                    continue;
                }

                // -------- Формируем колонки и значения --------
                Vector<string> filteredCols;
                Vector<string> filteredValues;

                // Пропускаем PK (первая колонка) и начинаем с индекса 1
                for (size_t ci = 1; ci < colCount; ci++)
                {
                    // Используем только имена колонок без префикса таблицы
                    filteredCols.push_back(headerCols[ci]);
                    filteredValues.push_back(values[ci]);
                }

                bool match = filterMatch(filteredCols, filteredValues, localCond);

                if (!match)
                    out << line << "\n";
                else
                    deletedAny = true;
            }

            in.close();
            out.close();

            // Заменяем файл результатом
            remove(csvPath.c_str());
            rename(tempPath.c_str(), csvPath.c_str());

            fileIndex++;
        }

        if (!deletedAny)
            cout << "Нет строк для удаления в таблице: " << tableName << "\n";
        else
            cout << "Удаление выполнено успешно в таблице: " << tableName << "\n";
    }
}