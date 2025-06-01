#include <vector>
#include <map>
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <unordered_map>
#include <iomanip>
#include <chrono>

enum class LogLevel { INFO, WARNING, ERROR };

using namespace std;

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], string& r_name, string& start_date, string& end_date, int& num_threads, string& table_path, string& result_path) {
    // TODO: Implement command line argument parsing
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

// Function to read TPCH data from the specified paths
bool readTPCHData(const string& table_path, vector<map<string, string>>& customer_data, vector<map<string, string>>& orders_data, vector<map<string, string>>& lineitem_data, vector<map<string, string>>& supplier_data, vector<map<string, string>>& nation_data, vector<map<string, string>>& region_data) {
    // TODO: Implement reading TPCH data from files
    return false;
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