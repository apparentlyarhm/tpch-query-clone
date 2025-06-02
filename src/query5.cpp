#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <unordered_map>
#include <iomanip>
#include <chrono>
#include <cerrno>
#include <cstring> 

using namespace std;

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], string& r_name, string& start_date, string& end_date, int& num_threads, string& table_path, string& result_path) {
    // Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
    unordered_map<string, string> allArgs;

    for (int i = 1; i < argc - 1; i += 2) {
        string key = argv[i];
        string value = argv[i + 1];
        if (key.rfind("--", 0) == 0) {  // starts with --
            allArgs[key] = value;
        } else {
            cerr << "Invalid argument format: " << key << endl;
            return false;
        }
    }

    try {
        r_name      = allArgs.at("--r_name");
        start_date  = allArgs.at("--start_date");
        end_date    = allArgs.at("--end_date");
        num_threads = stoi(allArgs.at("--threads"));
        table_path  = allArgs.at("--table_path");
        result_path = allArgs.at("--result_path");
    } catch (const out_of_range& e) {
        cerr << "Missing required argument: " << e.what() << endl;
        return false;
    } catch (const invalid_argument& e) {
        cerr << "Invalid number format for threads." << endl;
        return false;
    }

    return true;
}

// Each .tbl file:
// Is pipe (|) delimited
// Does not have a header row
// Each row ends with a | at the end

// Function to read TPCH data from the specified paths
bool readTPCHData(const string& table_path, vector<map<string, string>>& customer_data, vector<map<string, string>>& orders_data, vector<map<string, string>>& lineitem_data, vector<map<string, string>>& supplier_data, vector<map<string, string>>& nation_data, vector<map<string, string>>& region_data) {
    // Assuming that table_path is the directory containing the TPCH data files
    return readTable(table_path + "\\customer.tbl", CUSTOMER_COLS, customer_data) &&
           readTable(table_path + "\\orders.tbl", ORDERS_COLS, orders_data) &&
           readTable(table_path + "\\lineitem.tbl", LINEITEM_COLS, lineitem_data) &&
           readTable(table_path + "\\supplier.tbl", SUPPLIER_COLS, supplier_data) &&
           readTable(table_path + "\\nation.tbl", NATION_COLS, nation_data) &&
           readTable(table_path + "\\region.tbl", REGION_COLS, region_data);
        }

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const string& r_name, const string& start_date, const string& end_date, int num_threads, const vector<map<string, string>>& customer_data, const vector<map<string, string>>& orders_data, const vector<map<string, string>>& lineitem_data, const vector<map<string, string>>& supplier_data, const vector<map<string, string>>& nation_data, const vector<map<string, string>>& region_data, map<string, double>& results) {
    // TODO: Implement TPCH Query 5 using multithreading
    return false;
}

// Function to output results to the specified path
bool outputResults(const string& result_path, const map<string, double>& results) {
    // TODO: Implement outputting results to a file
    return false;
} 

// I'm going to treat this file as a utils source, that means I will add more functions here as needed.
void log(LogLevel level, const std::string& message) {
    const char* level_str;
    switch (level) {
        case LogLevel::INFO:    level_str = "INFO"; break;
        case LogLevel::WARNING: level_str = "WARN"; break;
        case LogLevel::ERROR:   level_str = "ERROR"; break;
    }

    // Timestamp
    auto now = std::chrono::system_clock::now();
    auto t_c = std::chrono::system_clock::to_time_t(now);
    std::tm tm = *std::localtime(&t_c);

    std::cout << "[" << std::put_time(&tm, "%Y-%m-%d %H:%M:%S")
              << "] [" << level_str << "] " << message << std::endl;
}

// Helper: split a string by delimiter
vector<string> split(const string& s, char delimiter) {
    vector<string> tokens;
    string token;
    istringstream tokenStream(s);
    while (getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// Helper: read a single TPCH table file
bool readTable(const string& file_path, const vector<string>& columns, vector<map<string, string>>& data) {
    size_t max_size_bytes = 10 * 1024 * 1024;

    std::ifstream file(file_path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << file_path
                  << " (" << std::strerror(errno) << ")" << std::endl;
        return false;
    }

    std::streamsize file_size = file.tellg();
    file.seekg(0, std::ios::beg); // reset position for reading

    if (file_size > static_cast<std::streamsize>(max_size_bytes)) {
        log(LogLevel::WARNING, "File too large to load into memory: " + file_path +
                               " (" + std::to_string(file_size) + " bytes)");
        file.close();
        return true; // large file exists, we won't load it now
    }

    std::string line;
    while (std::getline(file, line)) {
        // remove trailing pipe if it exists
        if (!line.empty() && line.back() == '|') {
            line.pop_back();
        }

        std::vector<std::string> values = split(line, '|');
        if (values.size() != columns.size()) {
            std::cerr << "Warning: Column size mismatch in " << file_path << std::endl;
            continue;
        }

        std::map<std::string, std::string> row;
        for (size_t i = 0; i < columns.size(); ++i) {
            row[columns[i]] = values[i];
        }

        data.push_back(row);
    }

    file.close();
    return true;
}