cmake_minimum_required(VERSION 3.1)
set(CMAKE_C_COMPILER gcc)
set(CMAKE_CXX_COMPILER g++)

project(streambox_bench)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -include config.h -fdiagnostics-color=auto")
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++11 -g -O0 -Wall -DHAVE_NUMA -Wfatal-errors")
SET(CMAKE_CXX_FLAGS_RELEASE "-std=c++11 -g -Wall -O2 -DHAVE_NUMA")

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_definitions(-DCONFIG_KAGE_GLOBAL_DEBUG_LEVEL=30)
endif()

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    add_definitions(-DCONFIG_KAGE_GLOBAL_DEBUG_LEVEL=50)
    add_definitions(-DNDEBUG)
endif()

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread")

include_directories(
    include
    streambox/streambox
    streambox/CTPL
    streambox/Kaskade
)

find_package(Boost REQUIRED COMPONENTS date_time system thread program_options)
FIND_LIBRARY(tbb NAMES libtbb.so)

SET(STREAMBOX_SOURCE_DIR streambox/streambox)
SET(STREAMBENCH_SOURCE_DIR src)

SET(SRC_WINSUM
    ${STREAMBOX_SOURCE_DIR}/WinSum/WinSumBase.cpp
    ${STREAMBOX_SOURCE_DIR}/WinSum/WinSum_addlong.cpp
    ${STREAMBOX_SOURCE_DIR}/WinSum/WinSum_mergeset.cpp
    ${STREAMBOX_SOURCE_DIR}/WinSum/WinSum_mergevector.cpp
    ${STREAMBOX_SOURCE_DIR}/WinSum/WinSum_addlong_tvpair.cpp
)

SET(SRC_WKR
    ${STREAMBOX_SOURCE_DIR}/WinKeyReducer/WinKeyReducer.cpp
    ${STREAMBOX_SOURCE_DIR}/WinKeyReducer/WinKeyReducer-wc-winbundle.cpp
    ${STREAMBOX_SOURCE_DIR}/WinKeyReducer/WinKeyReducer-wc-recordbundle.cpp
    ${STREAMBOX_SOURCE_DIR}/WinKeyReducer/WinKeyReducer-netmon.cpp
    ${STREAMBOX_SOURCE_DIR}/WinKeyReducer/WinKeyReducerEval-wc-winbundle.cpp
    ${STREAMBOX_SOURCE_DIR}/WinKeyReducer/WinKeyReducerEval-wc-recordbundle.cpp
    ${STREAMBOX_SOURCE_DIR}/WinKeyReducer/WinKeyReducerEval-netmon.cpp
)

SET(SRC_STREAMBENCH
    ${STREAMBENCH_SOURCE_DIR}/Mapper/FilterMapper.cpp
    ${STREAMBENCH_SOURCE_DIR}/Mapper/ProjectMapper.cpp
    ${STREAMBENCH_SOURCE_DIR}/Mapper/TemporalWinMapper.cpp
    ${STREAMBENCH_SOURCE_DIR}/Mapper/TemporalKVMapper.cpp
    ${STREAMBENCH_SOURCE_DIR}/Source/BoundedInMem.cpp
)

SET(SOURCES
    ${STREAMBOX_SOURCE_DIR}/core/Pipeline.cpp
    ${STREAMBOX_SOURCE_DIR}/BoundedWindow.cpp
    ${STREAMBOX_SOURCE_DIR}/Values.cpp
    ${STREAMBOX_SOURCE_DIR}/core/Transform.cpp
    ${STREAMBOX_SOURCE_DIR}/Source/Unbounded.cpp
    ${STREAMBOX_SOURCE_DIR}/Mapper/WordCountMapper.cpp
    ${STREAMBOX_SOURCE_DIR}/Win/WinGBK.cpp
    ${STREAMBOX_SOURCE_DIR}/Mapper/GrepMapper.cpp
    ${STREAMBOX_SOURCE_DIR}/Mapper/WindowedGrepMapper.cpp
    ${STREAMBOX_SOURCE_DIR}/Sink/Sink.cpp
    ${STREAMBOX_SOURCE_DIR}/Sink/WindowsBundleSink.cpp
    ${STREAMBOX_SOURCE_DIR}/Sink/RecordBundleSink.cpp
    ${STREAMBOX_SOURCE_DIR}/LookAheadPipelineVisitor.cpp
    ${STREAMBOX_SOURCE_DIR}/Win/FixedWindowInto.cpp
    ${STREAMBOX_SOURCE_DIR}/measure.c
    ${STREAMBOX_SOURCE_DIR}/core/EvaluationBundleContext.cpp
    ${STREAMBOX_SOURCE_DIR}/Mapper/SimpleMapper.cpp
    ${STREAMBOX_SOURCE_DIR}/Join/Join.cpp
    ${STREAMBOX_SOURCE_DIR}/Source/NetworkLatencySource.cpp
    ${STREAMBOX_SOURCE_DIR}/Source/Unbounded_Join.cpp
    ${STREAMBOX_SOURCE_DIR}/test/test-common.cpp
    ${SRC_WINSUM}
    ${SRC_WKR}
    ${SRC_STREAMBENCH}
)

add_library(streambox STATIC
    ${SOURCES}
)

add_executable(main main.cpp)

target_link_libraries(main streambox ${Boost_LIBRARIES} numa ${tbb})