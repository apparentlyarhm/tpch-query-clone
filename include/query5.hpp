#ifndef QUERY5_HPP
#define QUERY5_HPP

#include <string>
#include <vector>
#include <map>
#include <unordered_map>

enum class LogLevel { 
    INFO, 
    WARNING, 
    ERROR 
};

// Vector of column names for each TPCH table
const std::vector<std::string> CUSTOMER_COLS = {
    "c_custkey",
    "c_name",
    "c_address",
    "c_nationkey",
    "c_phone", 
    "c_acctbal", 
    "c_mktsegment", 
    "c_comment"
};

const std::vector<std::string> ORDERS_COLS = {
    "o_orderkey",
    "o_custkey",
    "o_orderstatus",
    "o_totalprice",
    "o_orderdate",
    "o_orderpriority",
    "o_clerk",
    "o_shippriority",
    "o_comment"
};

const std::vector<std::string> LINEITEM_COLS = {
    "l_orderkey",
    "l_partkey",
    "l_suppkey",
    "l_linenumber",
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "l_returnflag",
    "l_linestatus",
    "l_shipdate",
    "l_commitdate",
    "l_receiptdate",
    "l_shipinstruct",
    "l_shipmode",
    "l_comment"
};

const std::vector<std::string> SUPPLIER_COLS = {
    "s_suppkey",
    "s_name",
    "s_address",
    "s_nationkey",
    "s_phone",
    "s_acctbal",
    "s_comment"
};

const std::vector<std::string> NATION_COLS = {
    "n_nationkey",
    "n_name",
    "n_regionkey",
    "n_comment"
};

const std::vector<std::string> REGION_COLS = {
    "r_regionkey",
    "r_name",
    "r_comment"
};


// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path);

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, std::vector<std::vector<std::string>>& customer_data, std::vector<std::vector<std::string>>& orders_data, std::vector<std::vector<std::string>>& lineitem_data, std::vector<std::vector<std::string>>& supplier_data, std::vector<std::vector<std::string>>& nation_data, std::vector<std::vector<std::string>>& region_data);

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(
    const std::string& r_name,
    const std::string& start_date,
    const std::string& end_date,
    int num_threads,
    const std::vector<std::vector<std::string>>& customer_data,
    const std::vector<std::vector<std::string>>& orders_data,
    const std::vector<std::vector<std::string>>& lineitem_data,
    const std::vector<std::vector<std::string>>& supplier_data,
    const std::vector<std::vector<std::string>>& nation_data,
    const std::vector<std::vector<std::string>>& region_data,
    const std::unordered_map<std::string, size_t>& customerIndex,
    const std::unordered_map<std::string, size_t>& ordersIndex,
    const std::unordered_map<std::string, size_t>& lineitemIndex,
    const std::unordered_map<std::string, size_t>& supplierIndex,
    const std::unordered_map<std::string, size_t>& nationIndex,
    const std::unordered_map<std::string, size_t>& regionIndex,
    std::map<std::string, double>& results,
    std::vector<std::pair<std::string, double>>& sorted_results
);

// Function to output results to the specified path
bool outputResults(const std::string& result_path, std::vector<std::pair<std::string, double>>& sorted_results);

// Logging function
void log(LogLevel level, const std::string& message);

// Helper function to read a table from a file
bool readTable(const std::string& file_path, const std::vector<std::string>& columns, std::vector<std::vector<std::string>>& data);

// Helper function to split a string by a delimiter
std::vector<std::string> split(const std::string& s, char delimiter);

// Function to compute index map for a given set of columns
std::unordered_map<std::string, std::size_t> computeIndexMap(const std::vector<std::string>& columns);

// Function to populate all index maps for TPCH tables. it just calls computeIndexMap for each table's columns-- can be thought of as a wrapper function.
bool populateAllIndexMaps(
    std::unordered_map<std::string, size_t>& regionIndex,
    std::unordered_map<std::string, size_t>& nationIndex,
    std::unordered_map<std::string, size_t>& supplierIndex,
    std::unordered_map<std::string, size_t>& customerIndex,
    std::unordered_map<std::string, size_t>& ordersIndex,
    std::unordered_map<std::string, size_t>& lineitemIndex
);



#endif // QUERY5_HPP 