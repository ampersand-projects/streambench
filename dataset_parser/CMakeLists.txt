cmake_minimum_required(VERSION 3.16.3)
set(CMAKE_C_COMPILER clang)
set(CMAKE_CXX_COMPILER clang++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

project(dataset_parser)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

get_filename_component(PROJECT_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR} DIRECTORY)
set(Protobuf_IMPORT_DIRS ${PROJECT_ROOT_DIR}/dataset_util/protos)

find_package(Boost 1.78.0 REQUIRED COMPONENTS date_time filesystem)
find_package(protobuf CONFIG REQUIRED)

if(protobuf_VERBOSE)
  message(STATUS "Using Protocol Buffers ${protobuf_VERSION}")
endif()

include_directories(${PROJECT_ROOT_DIR}/dataset_util/include ${PROTOBUF_INCLUDE_DIRS} ${Boost_INCLUDE_DIR})
set(CMAKE_INCLUDE_CURRENT_DIR TRUE)

add_executable(parser parser.cpp)

target_link_libraries(parser protobuf::libprotobuf ${Boost_LIBRARIES})

set(_protobuf_include_path -I ${Protobuf_IMPORT_DIRS})
protobuf_generate(TARGET parser LANGUAGE cpp PROTOS ${Protobuf_IMPORT_DIRS}/taxi.proto)