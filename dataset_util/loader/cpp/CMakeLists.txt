cmake_minimum_required(VERSION 3.13.4)
set(CMAKE_C_COMPILER clang)
set(CMAKE_CXX_COMPILER clang++)

project(dataset_loader)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(protobuf CONFIG REQUIRED)

if(protobuf_VERBOSE)
  message(STATUS "Using Protocol Buffers ${protobuf_VERSION}")
endif()

get_filename_component(PARENT_DIR ${CMAKE_CURRENT_SOURCE_DIR} DIRECTORY)
get_filename_component(DATASET_UTIL_DIR ${PARENT_DIR} DIRECTORY)
set(Protobuf_IMPORT_DIRS ${DATASET_UTIL_DIR}/protos)

include_directories(${PROTOBUF_INCLUDE_DIRS} ${DATASET_UTIL_DIR}/include)
set(CMAKE_INCLUDE_CURRENT_DIR TRUE)

add_executable(loader loader.cpp)
target_link_libraries(loader protobuf::libprotobuf)

file(GLOB PROTO_FILES
  "${Protobuf_IMPORT_DIRS}/*.proto"
)

set(_protobuf_include_path -I ${Protobuf_IMPORT_DIRS})
protobuf_generate(TARGET loader LANGUAGE cpp PROTOS ${PROTO_FILES})