#include "query5.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <map>

// TODO: Include additional headers as needed

int main(int argc, char* argv[]) {
    std::string r_name, start_date, end_date, table_path, result_path;
    int num_threads;

    if (!parseArgs(argc, argv, r_name, start_date, end_date, num_threads, table_path, result_path)) {
        log(LogLevel::ERROR, "Failed to parse command line arguments.");
        std::cerr << "Failed to parse command line arguments." << std::endl;
        return 1;
    }
    std::ostringstream oss;
    oss << "Parsed command line arguments successfully:\n"
        << "  Region Name   : " << r_name << "\n"
        << "  Start Date    : " << start_date << "\n"
        << "  End Date      : " << end_date << "\n"
        << "  Num Threads   : " << num_threads << "\n"
        << "  Table Path    : " << table_path << "\n"
        << "  Result Path   : " << result_path;

    log(LogLevel::INFO, oss.str());
    
    std::vector<std::map<std::string, std::string>> customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data;

    if (!readTPCHData(table_path, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data)) {
        log(LogLevel::ERROR, "Failed to read TPCH data.");
        return 1;
    }
    log(LogLevel::INFO, "Read data from TPCH tables successfully.");

    std::map<std::string, double> results;

    if (!executeQuery5(r_name, start_date, end_date, num_threads, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data, results)) {
        std::cerr << "Failed to execute TPCH Query 5." << std::endl;
        return 1;
    }

    if (!outputResults(result_path, results)) {
        std::cerr << "Failed to output results." << std::endl;
        return 1;
    }

    std::cout << "TPCH Query 5 implementation completed." << std::endl;
    return 0;
} 