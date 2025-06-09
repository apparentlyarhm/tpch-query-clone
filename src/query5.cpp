#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <unordered_map>
#include <iomanip>
#include <chrono>
#include <cerrno>
#include <cstring> 
#include <unordered_set>
#include <mutex>
#include <thread>

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
    return readTable(table_path + "/customer.tbl", CUSTOMER_COLS, customer_data) &&
           readTable(table_path + "/orders.tbl", ORDERS_COLS, orders_data) &&
           readTable(table_path + "/lineitem.tbl", LINEITEM_COLS, lineitem_data) &&
           readTable(table_path + "/supplier.tbl", SUPPLIER_COLS, supplier_data) &&
           readTable(table_path + "/nation.tbl", NATION_COLS, nation_data) &&
           readTable(table_path + "/region.tbl", REGION_COLS, region_data);
        }

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const string& r_name, const string& start_date, const string& end_date, int num_threads, const vector<map<string, string>>& customer_data, const vector<map<string, string>>& orders_data, const vector<map<string, string>>& lineitem_data, const vector<map<string, string>>& supplier_data, const vector<map<string, string>>& nation_data, const vector<map<string, string>>& region_data, map<string, double>& results, vector<pair<string, double>>& sorted_results) {
    log(LogLevel::INFO, "Starting execution of TPCH Query 5");

    // r_name = <region>
    string target_region_key;
    for (const auto& row : region_data) {
        if (row.at("r_name") == r_name) {
            target_region_key = row.at("r_regionkey");
            log(LogLevel::INFO, "Matched region: " + r_name + " -> regionkey: " + target_region_key);
            break;
        }
    }
    if (target_region_key.empty()) {
        log(LogLevel::ERROR, "Region " + r_name + " not found in region table.");
        return false;
    }

    // Join nation on n_regionkey = r_regionkey
    unordered_map<string, string> nation_name_by_key;
    unordered_set<string> valid_nation_keys;
    for (const auto& row : nation_data) {
        if (row.at("n_regionkey") == target_region_key) {
            valid_nation_keys.insert(row.at("n_nationkey"));
            nation_name_by_key[row.at("n_nationkey")] = row.at("n_name");
        }
    }
    log(LogLevel::INFO, "Valid nations in region: " + to_string(valid_nation_keys.size()));

    // Join supplier on s_nationkey = n_nationkey
    unordered_set<string> valid_supp_keys;
    unordered_map<string, string> suppkey_to_nationkey;
    for (const auto& row : supplier_data) {
        if (valid_nation_keys.count(row.at("s_nationkey"))) {
            valid_supp_keys.insert(row.at("s_suppkey"));
            suppkey_to_nationkey[row.at("s_suppkey")] = row.at("s_nationkey");
        }
    }
    log(LogLevel::INFO, "Suppliers from valid nations: " + to_string(valid_supp_keys.size()));

    // Join customer on c_nationkey = s_nationkey
    unordered_map<string, string> custkey_to_nationkey;
    for (const auto& row : customer_data) {
        if (valid_nation_keys.count(row.at("c_nationkey"))) {
            custkey_to_nationkey[row.at("c_custkey")] = row.at("c_nationkey");
        }
    }
    log(LogLevel::INFO, "Customers from valid nations: " + to_string(custkey_to_nationkey.size()));

    // Join orders on o_custkey = c_custkey and filter o_orderdate in range
    unordered_set<string> valid_order_keys;
    unordered_map<string, string> orderkey_to_cust_nationkey;
    auto in_date_range = [&](const string& date) {
        return date >= start_date && date < end_date;
    };

    int matched_orders = 0;
    for (const auto& row : orders_data) {
        string custkey = row.at("o_custkey");
        string orderkey = row.at("o_orderkey");
        string orderdate = row.at("o_orderdate");
        if (in_date_range(orderdate) && custkey_to_nationkey.count(custkey)) {
            valid_order_keys.insert(orderkey);
            orderkey_to_cust_nationkey[orderkey] = custkey_to_nationkey[custkey];
            matched_orders++;
        }
    }
    log(LogLevel::INFO, "Orders in date range with valid customers: " + to_string(matched_orders));

    // Join lineitem on l_orderkey = o_orderkey and l_suppkey = s_suppkey - i thought to parallelize this part
    mutex result_mutex;
    int processed_lines = 0;
    mutex count_mutex;

    auto worker = [&](int start, int end, int thread_id) {
        log(LogLevel::INFO, "Thread " + to_string(thread_id) + " processing from index " + to_string(start) + " to " + to_string(end));
        
        int local_count = 0;
        map<string, double> local_result;

        for (int i = start; i < end; ++i) {
            const auto& row = lineitem_data[i];
            string orderkey = row.at("l_orderkey");
            string suppkey = row.at("l_suppkey");

            if (!valid_order_keys.count(orderkey)) continue;
            if (!valid_supp_keys.count(suppkey)) continue;

            string nationkey = orderkey_to_cust_nationkey[orderkey];
            string nation = nation_name_by_key[nationkey];

            double extended_price = stod(row.at("l_extendedprice"));
            double discount = stod(row.at("l_discount"));
            double revenue = extended_price * (1 - discount);

            local_result[nation] += revenue;
            local_count++;
        }
        log(LogLevel::INFO, "Thread " + to_string(thread_id) + " finished with " + to_string(local_count) + " valid line items.");

        // Merge into shared result
        {
            lock_guard<mutex> lock(result_mutex);
            for (const auto& [nation, revenue] : local_result) {
                results[nation] += revenue;
            }
        }

        {
            lock_guard<mutex> lock(count_mutex);
            processed_lines += local_count;
        }
    };
    vector<thread> threads;
    int total = lineitem_data.size();
    int chunk = (total + num_threads - 1) / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        int start = i * chunk;
        int end = min(start + chunk, total);
        log(LogLevel::INFO, "Spawning thread " + to_string(i) + " for range [" + to_string(start) + ", " + to_string(end) + ")");
        threads.emplace_back(worker, start, end, i);
    }

    for (auto& t : threads) t.join();
    log(LogLevel::INFO, "Processed lineitem rows: " + to_string(processed_lines));

    sorted_results.assign(results.begin(), results.end());
    sort(sorted_results.begin(), sorted_results.end(), [](const auto& a, const auto& b) {
        return a.second > b.second;  // descending order
    });

    return true;
}

// Function to output results to the specified path
bool outputResults(const string& result_path, vector<pair<string, double>>& sorted_results) {
    string filepath = result_path;
    if (filepath.back() != '/' && filepath.back() != '\\') {
        filepath += "/";  // Windows and Unix path compatibility
    }
    filepath += "results.txt";

    ofstream outfile(result_path);
    if (!outfile.is_open()) {
        return false;  // Could not open file for writing
    }
    outfile << left << setw(25) << "n_name" << " |revenue\n";

    // Write each entry in the map
    for (const auto& [nation, revenue] : sorted_results) {
        outfile << setw(25) << nation << " |" << fixed << setprecision(2) << revenue << "\n";
    }

    outfile.close();
    return true;
} 

// I'm going to treat this file as a utils source, that means I will add more functions here as needed.
void log(LogLevel level, const string& message) {
    const char* level_str;
    switch (level) {
        case LogLevel::INFO:    level_str = "INFO"; break;
        case LogLevel::WARNING: level_str = "WARN"; break;
        case LogLevel::ERROR:   level_str = "ERROR"; break;
    }

    // Timestamp
    auto now = chrono::system_clock::now();
    auto t_c = chrono::system_clock::to_time_t(now);
    tm tm = *localtime(&t_c);

    cout << "[" << put_time(&tm, "%Y-%m-%d %H:%M:%S") << "] [" << level_str << "] " << message << endl;
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
    size_t max_size_bytes = 4096ULL * 1024 * 1024;
    size_t sample_limit = 30000; // number of rows to read in sample mode

    ifstream file(file_path, ios::binary | ios::ate);
    if (!file.is_open()) {
        cerr << "Error: Could not open file " << file_path << " (" << strerror(errno) << ")" << endl;
        return false;
    }

    streamsize file_size = file.tellg();
    file.seekg(0, ios::beg); // reset position for reading

    bool sample_mode = false;
    log(LogLevel::INFO, "Reading file: " + file_path + " (" + to_string(file_size) + " bytes)");
    if (file_size > static_cast<streamsize>(max_size_bytes)) {
        log(LogLevel::WARNING, "File too large to fully load: " + file_path +
                               " (" + to_string(file_size) + " bytes). Sampling first " +
                               to_string(sample_limit) + " rows.");
        sample_mode = true;
        log(LogLevel::WARNING, "Sampling mode is ENABLED for file: " + file_path + " Results may not be complete.");
    }

    string line;
    size_t row_count = 0;
    while (getline(file, line)) {
        if (!line.empty() && line.back() == '|') {
            line.pop_back();
        }

        vector<string> values = split(line, '|');
        if (values.size() != columns.size()) {
            cerr << "Warning: Column size mismatch in " << file_path << endl;
            continue;
        }

        map<string, string> row;
        for (size_t i = 0; i < columns.size(); ++i) {
            row[columns[i]] = values[i];
        }

        data.push_back(move(row));
        ++row_count;

        if (sample_mode && row_count >= sample_limit) break;
    }

    file.close();
    return true;
}