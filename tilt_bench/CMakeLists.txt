cmake_minimum_required(VERSION 3.13.4)
set(CMAKE_C_COMPILER clang)
set(CMAKE_CXX_COMPILER clang++)

project(tilt_bench)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(LLVM 11.0.0 REQUIRED CONFIG)
find_package(protobuf CONFIG REQUIRED)

message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
if(protobuf_VERBOSE)
  message(STATUS "Using Protocol Buffers ${protobuf_VERSION}")
endif()

get_filename_component(PROJECT_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR} DIRECTORY)
set(Protobuf_IMPORT_DIRS ${PROJECT_ROOT_DIR}/dataset_util/protos)

add_subdirectory(tilt/third_party/easy_jit)

include_directories(${LLVM_INCLUDE_DIRS} ${PROTOBUF_INCLUDE_DIRS} include tilt/tilt/include ${PROJECT_ROOT_DIR}/dataset_util/include)
set(CMAKE_INCLUDE_CURRENT_DIR TRUE)
add_definitions(${LLVM_DEFINITIONS})
llvm_map_components_to_libnames(llvm_libs native orcjit mcjit objcarcopts)
set(easyjit_lib "${CMAKE_BINARY_DIR}/tilt/third_party/easy_jit/bin/EasyJitPass.so")

add_subdirectory(tilt/tilt)

add_executable(main main.cpp)

target_link_libraries(main protobuf::libprotobuf tilt)

set(_protobuf_include_path -I ${Protobuf_IMPORT_DIRS})
protobuf_generate(TARGET main LANGUAGE cpp PROTOS ${Protobuf_IMPORT_DIRS}/taxi.proto)