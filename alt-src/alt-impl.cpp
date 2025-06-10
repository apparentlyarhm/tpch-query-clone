#include <iostream>
#include <vector>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <algorithm>
#include <charconv> // For std::from_chars

#include <omp.h> // For OpenMP
#include "mio.hpp" // Memory-mapping library

// --- Zero-Allocation Parsing Utilities ---

// An iterator that yields string_views for each field in a line, separated by '|'.
// It performs no allocations and is the C++ equivalent of the Rust LineParser.
class LineParser {
private:
    std::string_view line_sv;

public:
    LineParser(std::string_view sv) : line_sv(sv) {}

    std::string_view next() {
        if (line_sv.empty()) {
            return {};
        }
        auto pos = line_sv.find('|');
        if (pos == std::string_view::npos) {
            auto result = line_sv;
            line_sv = {}; // Consume the rest
            return result;
        }
        auto result = line_sv.substr(0, pos);
        line_sv.remove_prefix(pos + 1);
        return result;
    }

    bool has_next() const {
        return !line_sv.empty();
    }
};

// Fast, non-allocating integer parsing
uint32_t parse_u32(std::string_view s) {
    uint32_t val = 0;
    std::from_chars(s.data(), s.data() + s.size(), val);
    return val;
}

uint16_t parse_u16(std::string_view s) {
    uint16_t val = 0;
    std::from_chars(s.data(), s.data() + s.size(), val);
    return val;
}

// Assumes two decimal places, e.g., "123.45"
// This integer-based approach avoids slower locale-dependent floating point parsing.
double parse_f64(std::string_view s) {
    long long val = 0;
    bool dot = false;
    for (char c : s) {
        if (c == '.') {
            dot = true;
        } else {
            val = val * 10 + (c - '0');
        }
    }
    return dot ? static_cast<double>(val) / 100.0 : static_cast<double>(val);
}

// Splits a large string_view into line-by-line views.
// This is done once per thread on its assigned chunk.
std::vector<std::string_view> get_lines(std::string_view sv) {
    std::vector<std::string_view> lines;
    size_t start = 0;
    for (size_t i = 0; i < sv.size(); ++i) {
        if (sv[i] == '\n') {
            lines.push_back(sv.substr(start, i - start));
            start = i + 1;
        }
    }
    // Add the last line if the file doesn't end with a newline
    if (start < sv.size()) {
        lines.push_back(sv.substr(start));
    }
    return lines;
}


// --- Main Logic ---

int main() {
    auto start_total = std::chrono::high_resolution_clock::now();

    // 1. Pre-process Nation data
    const std::unordered_map<uint16_t, const char*> nation_names = {
        {8, "INDIA"}, {9, "INDONESIA"}, {10, "IRAN"}, {11, "IRAQ"},
        {12, "JAPAN"}, {13, "JORDAN"}, {18, "CHINA"},
        {20, "SAUDI ARABIA"}, {21, "VIETNAM"}, {22, "RUSSIA"}
    };
    std::unordered_set<uint16_t> asian_nation_keys;
    for(const auto& pair : nation_names) {
        asian_nation_keys.insert(pair.first);
    }

    // Memory-map the input files
    mio::mmap_source customer_mmap("db-sf1/customer.tbl");
    mio::mmap_source supplier_mmap("db-sf1/supplier.tbl");
    mio::mmap_source orders_mmap("db-sf1/orders.tbl");
    mio::mmap_source lineitem_mmap("db-sf1/lineitem.tbl");

    std::string_view customer_sv(customer_mmap.data(), customer_mmap.size());
    std::string_view supplier_sv(supplier_mmap.data(), supplier_mmap.size());
    std::string_view orders_sv(orders_mmap.data(), orders_mmap.size());
    std::string_view lineitem_sv(lineitem_mmap.data(), lineitem_mmap.size());

    // 2. Filter CUSTOMER for Asian customers
    std::unordered_map<uint32_t, uint16_t> asian_customers;
    #pragma omp parallel
    {
        std::unordered_map<uint32_t, uint16_t> local_map;
        #pragma omp for nowait
        for (size_t i = 0; i < customer_sv.size(); ++i) {
            size_t start = i;
            while (i < customer_sv.size() && customer_sv[i] != '\n') { i++; }
            std::string_view line = customer_sv.substr(start, i - start);
            if (line.empty()) continue;

            LineParser parser(line);
            uint32_t c_custkey = parse_u32(parser.next());
            parser.next(); parser.next(); // Skip name, address
            uint16_t c_nationkey = parse_u16(parser.next());
            if (asian_nation_keys.count(c_nationkey)) {
                local_map[c_custkey] = c_nationkey;
            }
        }
        #pragma omp critical
        asian_customers.insert(local_map.begin(), local_map.end());
    }


    // 3. Filter SUPPLIER for Asian suppliers
    std::unordered_map<uint32_t, uint16_t> asian_suppliers;
    #pragma omp parallel
    {
        std::unordered_map<uint32_t, uint16_t> local_map;
        #pragma omp for nowait
        for (size_t i = 0; i < supplier_sv.size(); ++i) {
            size_t start = i;
            while (i < supplier_sv.size() && supplier_sv[i] != '\n') { i++; }
            std::string_view line = supplier_sv.substr(start, i - start);
            if (line.empty()) continue;

            LineParser parser(line);
            uint32_t s_suppkey = parse_u32(parser.next());
            parser.next(); parser.next(); // Skip name, address
            uint16_t s_nationkey = parse_u16(parser.next());
            if (asian_nation_keys.count(s_nationkey)) {
                local_map[s_suppkey] = s_nationkey;
            }
        }
        #pragma omp critical
        asian_suppliers.insert(local_map.begin(), local_map.end());
    }

    // 4. Filter ORDERS by date and by Asian customers
    std::unordered_map<uint32_t, uint16_t> relevant_orders;
    const std::string_view date_start = "1994-01-01";
    const std::string_view date_end = "1995-01-01";
    #pragma omp parallel
    {
        std::unordered_map<uint32_t, uint16_t> local_map;
        #pragma omp for nowait
        for (size_t i = 0; i < orders_sv.size(); ++i) {
            size_t start = i;
            while (i < orders_sv.size() && orders_sv[i] != '\n') { i++; }
            std::string_view line = orders_sv.substr(start, i - start);
            if (line.empty()) continue;

            LineParser parser(line);
            uint32_t o_orderkey = parse_u32(parser.next());
            uint32_t o_custkey = parse_u32(parser.next());
            parser.next(); parser.next(); // Skip status, totalprice
            std::string_view o_orderdate = parser.next();

            if (o_orderdate >= date_start && o_orderdate < date_end) {
                auto it = asian_customers.find(o_custkey);
                if (it != asian_customers.end()) {
                    local_map[o_orderkey] = it->second;
                }
            }
        }
        #pragma omp critical
        relevant_orders.insert(local_map.begin(), local_map.end());
    }

    // 5. Stream LINEITEM, perform final joins, and aggregate
    std::unordered_map<uint16_t, double> final_revenue_map;
    #pragma omp parallel
    {
        std::unordered_map<uint16_t, double> local_revenue_map;
        #pragma omp for nowait
        for (size_t i = 0; i < lineitem_sv.size(); ++i) {
            size_t start = i;
            while (i < lineitem_sv.size() && lineitem_sv[i] != '\n') { i++; }
            std::string_view line = lineitem_sv.substr(start, i - start);
            if (line.empty()) continue;

            LineParser parser(line);
            uint32_t l_orderkey = parse_u32(parser.next());

            auto order_it = relevant_orders.find(l_orderkey);
            if (order_it != relevant_orders.end()) {
                uint16_t cust_nationkey = order_it->second;
                parser.next(); // Skip partkey
                uint32_t l_suppkey = parse_u32(parser.next());

                auto supp_it = asian_suppliers.find(l_suppkey);
                if (supp_it != asian_suppliers.end()) {
                    uint16_t supp_nationkey = supp_it->second;
                    if (cust_nationkey == supp_nationkey) {
                        parser.next(); // Skip linenumber
                        parser.next(); // Skip quantity
                        double l_extendedprice = parse_f64(parser.next());
                        double l_discount = parse_f64(parser.next());
                        double revenue = l_extendedprice * (1.0 - l_discount);
                        local_revenue_map[cust_nationkey] += revenue;
                    }
                }
            }
        }
        #pragma omp critical
        {
            for (const auto& pair : local_revenue_map) {
                final_revenue_map[pair.first] += pair.second;
            }
        }
    }

    // 6. Sort and Print Results
    std::vector<std::pair<const char*, double>> sorted_revenues;
    for (const auto& pair : final_revenue_map) {
        sorted_revenues.push_back({nation_names.at(pair.first), pair.second});
    }

    std::sort(sorted_revenues.begin(), sorted_revenues.end(),
              [](const auto& a, const auto& b) {
                  return a.second > b.second;
              });
              
    printf("NATION_NAME | REVENUE\n");
    printf("------------+----------\n");
    for (const auto& pair : sorted_revenues) {
        printf("%-11s | %.2f\n", pair.first, pair.second);
    }
    
    auto end_total = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_total - start_total;
    std::cerr << "\nTotal execution time: " << elapsed.count() << " seconds\n";

    return 0;
}
