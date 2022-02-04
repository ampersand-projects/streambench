cmake_minimum_required(VERSION 3.6)
set(CMAKE_C_COMPILER clang)
set(CMAKE_CXX_COMPILER clang++)

project(lightsaber_bench)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_subdirectory(LightSaber)

include_directories(LightSaber/src include)

SET(CPP_FILES
        LightSaber/src/filesystem/File.cpp
        LightSaber/src/checkpoint/FileBackedCheckpointCoordinator.cpp
        LightSaber/src/checkpoint/BlockManager.cpp
        LightSaber/src/checkpoint/LineageGraph.cpp
        LightSaber/src/cql/expressions/Expression.cpp
        LightSaber/src/dispatcher/ITaskDispatcher.cpp
        LightSaber/src/dispatcher/JoinTaskDispatcher.cpp
        LightSaber/src/dispatcher/TaskDispatcher.cpp
        LightSaber/src/compression/CompressionCodeGenUtils.cpp
        LightSaber/src/compression/CompressionStatistics.cpp
        LightSaber/src/monitors/CompressionMonitor.cpp
        LightSaber/src/monitors/PerformanceMonitor.cpp
        LightSaber/src/monitors/Measurement.cpp
        LightSaber/src/monitors/LatencyMonitor.cpp
        LightSaber/src/processor/TaskProcessor.cpp
        LightSaber/src/result/ResultHandler.cpp
        LightSaber/src/tasks/NumaTaskQueueWrapper.cpp
        LightSaber/src/tasks/WindowBatch.cpp
        LightSaber/src/tasks/Task.cpp
        LightSaber/src/utils/AttributeType.cpp
        LightSaber/src/utils/Query.cpp
        LightSaber/src/utils/QueryApplication.cpp
        LightSaber/src/utils/Utils.cpp
        LightSaber/src/utils/SystemConf.cpp
        )

SET(RDMA_CPP_FILES
        LightSaber/src/RDMA/infinity/core/Context.cpp
        LightSaber/src/RDMA/infinity/memory/Atomic.cpp
        LightSaber/src/RDMA/infinity/memory/Buffer.cpp
        LightSaber/src/RDMA/infinity/memory/Region.cpp
        LightSaber/src/RDMA/infinity/memory/RegionToken.cpp
        LightSaber/src/RDMA/infinity/memory/RegisteredMemory.cpp
        LightSaber/src/RDMA/infinity/queues/QueuePair.cpp
        LightSaber/src/RDMA/infinity/queues/QueuePairFactory.cpp
        LightSaber/src/RDMA/infinity/requests/RequestToken.cpp
        LightSaber/src/RDMA/infinity/utils/Address.cpp
        )

add_executable(lightsaber ${CPP_FILES} ${RDMA_CPP_FILES} main.cpp)
target_link_options(lightsaber PRIVATE -Wl,--unresolved-symbols=ignore-all)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(TestGAG ${Boost_LIBRARIES})
endif ()
target_link_libraries(lightsaber
        z
        boost_iostreams
        boost_fiber
        operatorJITLib
        tbb ibverbs
        pthread dl aio uuid stdc++fs)
target_compile_options(lightsaber PRIVATE -Wall -Wextra -O3 -march=native)
# target_compile_definitions(lightsaber PUBLIC TCP_OUTPUT)

add_executable(remotesink remote/remoteSink.cpp)
target_link_options(remotesink PRIVATE -Wl,--unresolved-symbols=ignore-all)
target_compile_options(remotesink PRIVATE -Wall -Wextra -O3 -march=native)