cmake_minimum_required(VERSION 3.23.2)
project(MyCoolDB_lib)

set(CMAKE_CXX_STANDARD 23)

include_directories(lib/include)

add_subdirectory(lib)
add_subdirectory(bin)

enable_testing()
add_subdirectory(tests)