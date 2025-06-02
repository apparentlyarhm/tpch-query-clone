#ifndef QUERY5_HPP
#define QUERY5_HPP

#include <string>
#include <vector>
#include <map>

enum class LogLevel { INFO, WARNING, ERROR };

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
bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data);

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results);

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results);

// Logging function
void log(LogLevel level, const std::string& message);

// Helper function to read a table from a file
bool readTable(const std::string& file_path, const std::vector<std::string>& columns, std::vector<std::map<std::string, std::string>>& data);

// Helper function to split a string by a delimiter
std::vector<std::string> split(const std::string& s, char delimiter);

#endif // QUERY5_HPP 