cmake_minimum_required (VERSION 2.8)
add_definitions(-std=c++11)

project(parallel-sudoku)

FIND_PACKAGE( OpenMP REQUIRED)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")

aux_source_directory(. DIR_SRCS)

add_executable(sudoku-solver ${DIR_SRCS})
