cmake_minimum_required(VERSION 3.10)
project(tpch_query5 LANGUAGES CXX)

# # Set C++ standard
# set(CMAKE_CXX_STANDARD 11)
# set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add executable target
add_executable(tpch_query5 src/main.cpp src/query5.cpp)

# Include directories
target_include_directories(tpch_query5 PRIVATE include)

# Link libraries
find_package(Threads REQUIRED)
target_link_libraries(tpch_query5 PRIVATE Threads::Threads)


# The second executable target for the alternative implementation
add_executable(alt-build alt-src/alt-impl.cpp)

# Include directories
target_include_directories(alt-build PRIVATE alt-src/include)

find_package(OpenMP REQUIRED)
# Install target (optional)
# install(TARGETS tpch_query5 DESTINATION bin) 