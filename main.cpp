#include "interface.h"
#include <iostream>
#include <exception>

int main() {
    try {
        runDatabaseInterface();
    } catch (const std::exception& e) {
        std::cerr << "Критическая ошибка: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Неизвестная критическая ошибка" << std::endl;
        return 1;
    }
    
    return 0;
}