## Hello this is just a test, ts is my fork :broken_heart:

dont mind me bro

1. 1/6/25 [23:32] -- painfully learnt not to mess around with cmake/make binaries on windows, on WSL it just works out of the box (after installing `build-essentials` using apt). i was able to compile dbgen and generate sample data using 
```bash
./dbgen -s 2
```
2. 2/6/25 [14:08] -- i should have configured WSL only, anyways, using msys2, i was able to get the `mutex` and `thread` libraries working on windows (which was missing in minGW)
Also, beacuse of limited time, I could not finish the indexes stuff, so in case of larger db, I will simply read first 30K lines. This will result in incorrect results -- I am aware of this. Make sure under `CMake kits` You have:

```json
  {
    "name": "GCC 13.1.0 MinGW -- USE THIS BRO",
    "compilers": {
      "C": "C:\\msys64\\ucrt64\\bin\\gcc.exe",
      "CXX": "C:\\msys64\\ucrt64\\bin\\g++.exe"
    },
    "isTrusted": true
  },
```
3. 10/6/25 [00:34] -- just by shifting to `vector<string>` instead of the given `map<string, string>` allowed the entire data to be fed into memory. Since the column names are supposed to be constant, we can improve the memory footprint from the original implementation (non running vs ~10Gigs). its still very large so the only correct way is streaming. If we were using Java or Rust here, its iterators would have proved useful here. The answer is still wrong but at least it all loads. 
<b>I did, however, changed the function signatures and therefore the intitial state the assingment was given</b>

4. 10/6/25 [17:45] -- turns out i had missed a join. the output matches now. Still didnt implement it the streaming way but have added an alternate approach -- mostly after discussion with a friend, who's a rust connoisseur, to basically have a rustified solution. Runs in similar time frame but has better memory footprint. I believe that in a real DBMS, the engine has to do some sort of data analysis and stuff as part of optimizations to run the query. 

Original Readme below: 
<hr>

# TPCH Query 5 C++ Multithreading Project

## Overview
Task is to implement TPCH Query 5 using C++ and multithreading. 
1. Understand the query 5 given below.
2. Generate input data for the query using [TPCH Data Generation Tool](https://github.com/electrum/tpch-dbgen)
3. Clone the current repository to your personal github.
4. Complete the incomplete functions in query5.cpp file
5. Compile and run the program. Capture the result with single thread and 4 thread.
6. Share the link of your completed repository in email with below mentioned information.

## Submission (Reply with below information to the assignment email)
| S.No. | Item          | Description                                                                                                         |
|--------|---------------|---------------------------------------------------------------------------------------------------------------------|
| 1      | **GitHub Link**   | Share the GitHub link with your completed code. Make sure the program compiles and produces the same result as mentioned in point 2. |
| 2      | **Final Result**  | Provide the final result of the query at SF2 (Scale factor 2).                                                      |
| 3      | **Runtime**       | Share the runtime numbers for both single-threaded and 4-threaded execution.                                        |
| 4      | **Screenshot**    | Attach a screenshot showing the program running with the result visible.                                            |

>**Note : Submissions without above 4 details would be considered as incomplete** 

## Query 5
```sql
select 
n_name, 
sum(l_extendedprice * (1 - l_discount)) as revenue 
from 
customer, 
orders, 
lineitem, 
supplier, 
nation, 
region 
where 
c_custkey = o_custkey 
and l_orderkey = o_orderkey 
and l_suppkey = s_suppkey 
and c_nationkey = s_nationkey 
and s_nationkey = n_nationkey 
and n_regionkey = r_regionkey 
and r_name = 'ASIA' 
and o_orderdate >= '1994-01-01' 
and o_orderdate < '1995-01-01' 
group by 
n_name 
order by 
revenue desc 
```

## Prerequisites
- CMake (version 3.10 or higher)
- C++ compiler (supporting C++11 or later)
- [TPCH Data Generation Tool](https://github.com/electrum/tpch-dbgen) : Generate data for query using this tool at scale factor 2 

## Building the Project
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd tpch-query5-cpp
   ```

2. Create a build directory and navigate into it:
   ```bash
   mkdir build
   cd build
   ```

3. Generate the build files with CMake:
   ```bash
   cmake ..
   ```

4. Compile the project:
   ```bash
   make
   ```

## Running the Program
### Single-Threaded Execution
To run the program in single-threaded mode, use the following command:
```bash
./tpch_query5 --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 1 --table_path /path/to/tables --result_path /path/to/results
```

### Multi-Threaded Execution
To run the program in multi-threaded mode, specify the number of threads (e.g., 4):
```bash
./tpch_query5 --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
```

## Generating a Report
1. Run the program with the desired parameters.
2. The results will be output to the specified result path.
3. Analyze the results to compare performance between single-threaded and multi-threaded execution.

## Rationale Behind Speedup
- **Parallelization**: By dividing the workload among multiple threads, the program can process data concurrently, reducing the overall execution time.
- **Efficiency**: Multithreading is particularly effective for I/O-bound tasks (like reading data) and CPU-bound tasks (like processing and joining tables).
- **Scalability**: The speedup is expected to scale with the number of threads, up to the point where the overhead of thread management becomes significant.

## Additional Notes
- Ensure that the TPCH data is correctly generated and placed in the specified table path.
- Adjust the number of threads based on your system's capabilities and the size of the dataset.

## Troubleshooting
If you encounter any issues during build or execution, please check the following:
- Ensure all dependencies are installed.
- Verify that the TPCH data is correctly formatted and accessible.
- Check the command line arguments for correctness.
