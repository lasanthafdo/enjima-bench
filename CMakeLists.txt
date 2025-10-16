cmake_minimum_required(VERSION 3.16)
project(enjima_benchmarks)

set(CMAKE_CXX_STANDARD 20)

include_directories(
        ./
        ./include
)

set(TBB_TEST CACHE INTERNAL OFF)

include(FetchContent)
FetchContent_Declare(
        tbb
        GIT_REPOSITORY https://github.com/oneapi-src/oneTBB.git
        GIT_TAG v2021.12.0
        GIT_SHALLOW ON
)
FetchContent_MakeAvailable(tbb)

set(BOOST_INCLUDE_LIBRARIES functional)
set(BOOST_ENABLE_CMAKE ON)
FetchContent_Declare(
        BoostFunctional
        GIT_REPOSITORY https://github.com/boostorg/functional.git
        GIT_TAG boost-1.84.0
        GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(BoostFunctional)

FetchContent_Declare(
        yaml-cpp
        GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
        GIT_TAG 0.8.0 # Can be a tag (yaml-cpp-x.x.x), a commit hash, or a branch name (master)
        GIT_SHALLOW ON
)
FetchContent_MakeAvailable(yaml-cpp)

# For GCC
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -Wno-error=maybe-uninitialized -fPIE -pie -pthread -mavx -march=native")

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "12.0")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --param destructive-interference-size=64")
    endif()
endif()

# For Clang
#set(CMAKE_CXX_COMPILER "clang++-18")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -fPIE -pthread")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -O2")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -O2")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=shift -O2")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")

#set(CMAKE_CXX_FLAGS_DEBUG "-g")
#set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# When you want your debug build program to go faster
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O2")

link_directories(lib)
find_library(EnjimaRelease NAMES libEnjimaRelease)

add_compile_definitions(ENJIMA_METRICS_LEVEL=0)
add_compile_definitions(ENJIMA_QUEUE_BASED_Q_TYPE=0)
add_compile_definitions(ENJIMA_MEM_CPY_METHOD=0)
add_compile_definitions(ENJIMA_BUFFER_ALIGNMENT=128)

add_executable(enjima_benchmarks main.cpp
        enjima/benchmarks/workload/StreamingBenchmark.h
        enjima/benchmarks/workload/LinearRoadBenchmark.tpp
        enjima/benchmarks/workload/LinearRoadBenchmark.h
        enjima/benchmarks/workload/NewYorkTaxiBenchmark.h
        enjima/benchmarks/workload/NewYorkTaxiBenchmark.tpp
        enjima/benchmarks/workload/LRBValidateBenchmark.cpp
        enjima/benchmarks/workload/LRBValidateBenchmark.h
        enjima/benchmarks/workload/StreamingBenchmark.cpp
        enjima/benchmarks/workload/YahooStreamingBenchmark.tpp
        enjima/benchmarks/workload/YahooStreamingBenchmark.h
        enjima/benchmarks/workload/functions/YSBFunctions.h
        enjima/benchmarks/workload/operators/InMemoryRateLimitedYSBSourceOperator.h
        enjima/benchmarks/workload/operators/InMemoryRateLimitedLRBSourceOperator.h
        enjima/benchmarks/workload/operators/InMemoryRateLimitedNYTSourceOperator.h
        enjima/benchmarks/workload/WorkloadException.h
        enjima/benchmarks/workload/operators/InMemoryFixedRateYSBSourceOperator.h
        enjima/benchmarks/workload/operators/InMemoryFixedRateLRBSourceOperator.h
        enjima/benchmarks/workload/operators/InMemoryFixedRateNYTSourceOperator.h
        enjima/benchmarks/workload/YSBSourceBenchmark.h
        enjima/benchmarks/workload/YSBSourceBenchmark.tpp
        enjima/benchmarks/workload/YSBFilterBenchmark.h
        enjima/benchmarks/workload/YSBFilterBenchmark.tpp
        enjima/benchmarks/workload/MixedWorkloadLRBYSB.h
        enjima/benchmarks/workload/MixedWorkloadLRBYSB.tpp
        enjima/benchmarks/workload/MixedWorkloadYSBNYT.h
        enjima/benchmarks/workload/MixedWorkloadYSBNYT.tpp
)

target_link_libraries(enjima_benchmarks debug EnjimaDebug TBB::tbb yaml-cpp::yaml-cpp)
target_link_libraries(enjima_benchmarks optimized EnjimaRelease TBB::tbb yaml-cpp::yaml-cpp)
#target_link_libraries(enjima_benchmarks optimized EnjimaRelWithDebInfo TBB::tbb yaml-cpp::yaml-cpp)
