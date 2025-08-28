#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// Procesa un bloque del archivo
void procesarBloque(const std::string& nombreArchivo, size_t inicio, size_t fin, const std::string& salidaTemp) {
    std::ifstream entrada(nombreArchivo, std::ios::binary);
    std::ofstream salida(salidaTemp);
    if (!entrada.is_open() || !salida.is_open()) {
        std::cerr << "Error abriendo archivos\n";
        return;
    }

    entrada.seekg(inicio);

    // Si no estamos al inicio, avanzar hasta el próximo salto de línea
    if (inicio != 0) {
        std::string dummy;
        std::getline(entrada, dummy);
    }

    std::string linea;
    size_t pos = entrada.tellg();
    while (pos < fin && std::getline(entrada, linea)) {
        std::istringstream iss(linea);
        std::string nombre, numero;
        if (iss >> nombre >> numero) {
            salida << nombre << " " << numero << "\n";
        }
        pos = entrada.tellg();
        if (pos == -1) break; // fin de archivo
    }
}

int main() {
    std::string archivoEntrada = "final_index.idx";
    std::ifstream entrada(archivoEntrada, std::ios::binary | std::ios::ate);
    if (!entrada.is_open()) {
        std::cerr << "Error al abrir entrada\n";
        return 1;
    }

    size_t tamano = entrada.tellg();
    entrada.close();

    size_t numThreads = std::thread::hardware_concurrency();
    size_t bloque = tamano / numThreads;

    std::vector<std::thread> threads;
    std::vector<std::string> archivosTemporales;

    for (size_t t = 0; t < numThreads; t++) {
        size_t inicio = t * bloque;
        size_t fin = (t == numThreads - 1) ? tamano : (t + 1) * bloque;
        std::string salidaTemp = "salida_" + std::to_string(t) + ".txt";
        archivosTemporales.push_back(salidaTemp);

        threads.emplace_back(procesarBloque, archivoEntrada, inicio, fin, salidaTemp);
    }

    for (auto& th : threads) th.join();

    // Unir todos los archivos temporales en salida.txt
    std::ofstream salidaFinal("simplified_index.txt");
    for (auto& temp : archivosTemporales) {
        std::ifstream tempIn(temp);
        salidaFinal << tempIn.rdbuf();
        tempIn.close();
        std::remove(temp.c_str()); // borrar archivo temporal
    }

    salidaFinal.close();
    std::cout << "Procesado en " << numThreads << " hilos.\n";
    return 0;
}
