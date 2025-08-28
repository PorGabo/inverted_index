#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>
#include <map>
#include <mutex>
#include <filesystem>
#include <cctype>
#include <algorithm>
#include <queue>



namespace fs = std::filesystem;
using namespace std;

int thread_block_size = 25000000;

std::mutex io_mutex;


std::string normalize(const std::string& input)
{
    static const std::unordered_map<std::string, char> repl = {
        {"á",'a'}, {"Á",'a'},
        {"é",'e'}, {"É",'e'},
        {"í",'i'}, {"Í",'i'},
        {"ó",'o'}, {"Ó",'o'},
        {"ú",'u'}, {"ü",'u'}, {"Ú",'u'}, {"Ü",'u'},
        {"ñ",'n'}, {"Ñ",'n'}
    };

    std::string out;
    out.reserve(input.size());

    for (size_t i = 0; i < input.size(); )
    {
        unsigned char c = static_cast<unsigned char>(input[i]);

        if (c < 0x80)
        {
            char lc = static_cast<char>(std::tolower(c));
            if (std::isalnum(static_cast<unsigned char>(lc))) // solo letras o dígitos
                out.push_back(lc);
            i++;
        }
        else
        {
            // UTF-8 multibyte: detecta longitud 2/3/4
            size_t len = 1;
            if ((c & 0xE0) == 0xC0) len = 2;
            else if ((c & 0xF0) == 0xE0) len = 3;
            else if ((c & 0xF8) == 0xF0) len = 4;

            std::string ch = input.substr(i, len); // tmp
            auto it = repl.find(ch);
            if (it != repl.end())
            {
                out.push_back(it->second);
            }
            i += len;
        }
    }
    return out;
}



int extract_block_number(const std::string& filename, const std::string& prefix)
{
    std::string num_str = filename.substr(prefix.size(), filename.size() - prefix.size() - 4);
    return std::stoi(num_str);
}


// listar .idx automáticamente en un directorio
std::vector<std::string> list_idx_files(const std::string& dir,
                              const std::string& prefix = "block_",
                              const std::string& ext = ".idx") {
    std::vector<std::pair<int,std::string>> with_num;
    std::vector<std::string> without_num;

    for (const auto& entry : fs::directory_iterator(dir)) {
        if (!entry.is_regular_file()) continue;
        auto path = entry.path();
        auto fname = path.filename().string();
        if (fname.size() >= ext.size() &&
            fname.rfind(ext) == fname.size() - ext.size()) {

            int n = extract_block_number(fname, prefix);
            if (n >= 0) with_num.emplace_back(n, path.string());
            else        without_num.push_back(path.string());
        }
    }
    sort(with_num.begin(), with_num.end(),
         [](auto& a, auto& b){ return a.first < b.first; });
    sort(without_num.begin(), without_num.end()); // lexicográfico

    std::vector<std::string> files;
    files.reserve(with_num.size() + without_num.size());
    for (auto& p : with_num) files.push_back(p.second);
    for (auto& s : without_num) files.push_back(s);
    return files;
}

// estructura de una línea del idx 
struct Entry
{
    std::string word;
    std::vector<size_t> positions;
    int file_id; // indice del archivo del que proviene
};

// parsea exactamente: palabra freq pos1 pos2 ...
bool parse_line(const std::string& line, int file_id, Entry& out)
{
    std::istringstream ss(line);
    out = Entry{};
    out.file_id = file_id;

    size_t freq = 0;
    if (!(ss >> out.word >> freq)) return false;

    out.positions.resize(freq);
    for (size_t i = 0; i < freq; ++i)
    {
        if (!(ss >> out.positions[i]))
        {
            // si la linea esta corrupta, recorta a lo leido
            out.positions.resize(i);
            break;
        }
    }
    return true;
}

// comparador para min-heap:
// 1) palabra mas chica primero
// 2) si palabra igual, archivo con file_id menor primero
struct EntryCmp
{
    bool operator()(const Entry& a, const Entry& b) const
    {
        if (a.word != b.word) return a.word > b.word; // min-heap por palabra
        return a.file_id > b.file_id; // estabilidad por orden de archivo
    }
};

// merge k-way
void merge_idx_files_auto(const std::string& dir_with_idx,
                          const std::string& output_file,
                          const std::string& prefix = "block_",
                          const std::string& ext = ".idx")
{
    // 1) descubrir archivos .idx
    std::vector<std::string> filenames = list_idx_files(dir_with_idx, prefix, ext);
    if (filenames.empty())
    {
        std::cerr << "No se encontraron archivos " << ext << " en " << dir_with_idx << "\n";
        return;
    }

    // 2) abrir todos
    std::vector<std::ifstream> files(filenames.size());
    for (size_t i = 0; i < filenames.size(); ++i) {
        files[i].open(filenames[i]);
        if (!files[i].is_open()) {
            std::cerr << "No se pudo abrir " << filenames[i] << "\n";
            return;
        }
    }
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "No se pudo crear " << output_file << "\n";
        return;
    }

    // 3) min-heap con la 1ra línea de cada archivo
    priority_queue<Entry, std::vector<Entry>, EntryCmp> pq;
    for (size_t i = 0; i < files.size(); ++i) {
        std::string line;
        if (getline(files[i], line)) {
            Entry e;
            if (parse_line(line, (int)i, e)) pq.push(std::move(e));
        }
    }

    std::string current_word;
    std::vector<size_t> merged_positions;

    // 4) k-way merge
    while (!pq.empty())
    {
        Entry top = pq.top(); pq.pop();

        if (current_word.empty())
        {
            current_word = top.word;
            merged_positions = std::move(top.positions);
        }
        else if (top.word == current_word)
        {
            // misma palabra: concatenar. gracias al tie-breaker por file_id,
            // los bloques se agregan en orden y las posiciones quedan ordenadas
            merged_positions.insert(merged_positions.end(), top.positions.begin(), top.positions.end());
        }
        else
        {
            // palabra cambia: volcar la anterior
            out << current_word << " " << merged_positions.size();
            for (auto p : merged_positions) out << " " << p;
            out << "\n";

            current_word = std::move(top.word);
            merged_positions = std::move(top.positions);
        }

        // avanzar en el archivo de donde salio 'top'
        std::string line;
        if (getline(files[top.file_id], line))
        {
            Entry e;
            if (parse_line(line, top.file_id, e)) pq.push(std::move(e));
        }
    }

    // ultimo flush
    if (!current_word.empty())
    {
        out << current_word << " " << merged_positions.size();
        for (auto p : merged_positions) out << " " << p;
        out << "\n";
    }

    out.close();
    for (auto& f : files) f.close();
}





// función que procesa un bloque de palabras y guarda en un archivo
void process_block(const std::vector<std::string>& words, int block_id, size_t offset)
{
    std::unordered_map<std::string, std::vector<size_t>> counter;
    size_t pos = 0;

    for (const auto& word : words)
    {
        counter[word].push_back(offset + pos);  // pos global
        pos++;
    }

    // guardar el bloque en un archivo
    fs::create_directories("blocks"); // asegura que exista
    std::string filename = "blocks/block_" + std::to_string(block_id) + ".idx";

    std::ofstream outfile(filename);
    if (!outfile.is_open())
    {
        std::cerr << "No se pudo abrir " << filename << std::endl;
        return;
    }

    // ordenar antes de escribir
    std::map<std::string, std::vector<size_t>> ordered(counter.begin(), counter.end());
    for (const auto& pair : ordered)
    {
        outfile << pair.first << " " << pair.second.size() << " ";
        for (const auto& p : pair.second)
        {
            outfile << p << " ";
        }
        outfile << "\n";
    }

    outfile.close();

    // to debug :)
    std::lock_guard<std::mutex> lock(io_mutex);
    std::cout << "Bloque " << block_id << " procesado y guardado en " << filename << std::endl;
}

int main()
{
    std::ifstream file("wikipedia.txt");
    if (!file.is_open())
    {
        std::cerr << "No se pudo abrir el archivo palabras.txt" << std::endl;
        return 1;
    }

    std::string word;
    std::vector<std::string> buffer;
    std::vector<std::thread> threads;
    int block_id = 0;

    unsigned int max_threads = std::thread::hardware_concurrency();
    if (max_threads == 0) max_threads = 4;

    std::string raw;
    while (file >> raw)
    {
        std::string normalized = normalize(raw);
    
        std::istringstream iss(normalized);
        std::string token;
        while (iss >> token)
        {
            buffer.push_back(token);
        }
    
        if ((int)buffer.size() >= thread_block_size)
        {
            size_t offset = block_id * thread_block_size;
    
            if (threads.size() >= max_threads) {
                threads[0].join();
                threads.erase(threads.begin());
            }
    
            threads.emplace_back(process_block, buffer, block_id, offset);
            block_id++;
            buffer.clear();
        }
    }

    // bloque incompleto final
    if (!buffer.empty())
    {
        size_t offset = block_id * thread_block_size;

        if (threads.size() >= max_threads)
        {
            threads[0].join();
            threads.erase(threads.begin());
        }

        threads.emplace_back(process_block, buffer, block_id, offset);
        block_id++;
    }


    for (auto& t : threads)
        t.join();

    file.close();

    std::cout << "Todos los bloques procesados." << std::endl;

    merge_idx_files_auto("blocks", "final_index.idx", "block_", ".idx");
    std::cout << "Merge completado en final_index.idx\n";


    return 0;
}
