cmake_minimum_required(VERSION 2.8.5 FATAL_ERROR)
project(dmfs)

# Compiler options
set (CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wsign-compare")
set (CMAKE_CXX_FLAGS "-g ${CMAKE_C_FLAGS} -std=c++11 -Wall -O0 -DTHREAD -D_FILE_OFFSET_BITS=64")

option (SANITIZE "Turn on sanitization" OFF)
if (SANITIZE)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
endif()

# Find 3rd party libs
list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
find_package(Crypto REQUIRED)
include_directories(${CRYPTO_INCLUDE_DIR})
#message("-------: ${CRYPTO_INCLUDE_DIR}")
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(RDMA_CORE_ROOT "" CACHE PATH "Path to rdma-core source tree (contains build/lib and build/include)")
if (RDMA_CORE_ROOT)
    set(RDMA_CORE_LIB_DIR "${RDMA_CORE_ROOT}/build/lib")
    set(RDMA_CORE_INCLUDE_DIR "${RDMA_CORE_ROOT}/build/include")

    if (NOT EXISTS "${RDMA_CORE_LIB_DIR}")
        message(FATAL_ERROR "RDMA_CORE_ROOT is set, but ${RDMA_CORE_LIB_DIR} does not exist")
    endif()
    if (NOT EXISTS "${RDMA_CORE_INCLUDE_DIR}")
        message(FATAL_ERROR "RDMA_CORE_ROOT is set, but ${RDMA_CORE_INCLUDE_DIR} does not exist")
    endif()

    unset(IBVERBS_LIB CACHE)
    find_library(IBVERBS_LIB NAMES ibverbs HINTS ${RDMA_CORE_LIB_DIR} NO_DEFAULT_PATH)
    if (NOT IBVERBS_LIB)
        message(FATAL_ERROR "Cannot find libibverbs under ${RDMA_CORE_LIB_DIR}")
    endif()

    set(RDMA_INCLUDE_DIR ${RDMA_CORE_INCLUDE_DIR})
    set(CMAKE_BUILD_RPATH "${RDMA_CORE_LIB_DIR}")
    set(CMAKE_INSTALL_RPATH "${RDMA_CORE_LIB_DIR}")
    set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
    message(STATUS "Using rdma-core from ${RDMA_CORE_ROOT}")
else()
    unset(IBVERBS_LIB CACHE)
    find_library(IBVERBS_LIB NAMES ibverbs)
    if (NOT IBVERBS_LIB)
        message(FATAL_ERROR "libibverbs not found (try installing libibverbs-dev)")
    endif()
    set(RDMA_INCLUDE_DIR "/usr/local/ofed/include")
endif()

find_library(FUSE_LIB fuse)
if (NOT FUSE_LIB)
    message(FATAL_ERROR "libfuse not found (try installing libfuse-dev)")
endif()

# Incs and Libs
set(INCLUDE_BASE ${PROJECT_SOURCE_DIR}/include)
include_directories(${RDMA_INCLUDE_DIR} ${INCLUDE_BASE})
link_libraries(${CRYPTO_LIBRARIES} ${CMAKE_DL_LIBS})
#message("---: ${CRYPTO_LIBRARIES}")

# Source file define
set(FS_SRC ${PROJECT_SOURCE_DIR}/src/fs)
set(NET_SRC ${PROJECT_SOURCE_DIR}/src/net)
set(TOOLS_SRC ${PROJECT_SOURCE_DIR}/src/tools)

# Build server
file(GLOB_RECURSE SERVER_SRC ${FS_SRC}/*.cpp ${FS_SRC}/*.c ${NET_SRC}/*.cpp ${TOOLS_SRC}/*.cpp)
add_executable(dmfs ${SERVER_SRC})
target_link_libraries(dmfs Threads::Threads ${IBVERBS_LIB} ${FUSE_LIB})

# Build client library
find_package(JNI)
include_directories(${JNI_INCLUDE_DIRS})
#link_libraries(${JNI_LIBRARIES})
message("===== ${JNI_INCLUDE_DIRS} =====lib:${JNI_LIBRARIES}")
set(CLIENT_BASE ${PROJECT_SOURCE_DIR}/client)
set(CNET_SRC ${PROJECT_SOURCE_DIR}/src/net)
set(CLIB_SRC ${PROJECT_SOURCE_DIR}/src/client)
file(GLOB_RECURSE NRFS_LIB_SRC ${CNET_SRC}/*.cpp ${CLIB_SRC}/*.cpp ${TOOLS_SRC}/*.cpp ${FS_SRC}/*.cpp)
add_library(nrfsc SHARED ${NRFS_LIB_SRC})
add_library(jninrfs SHARED ${NRFS_LIB_SRC} ${CLIB_SRC}/JavaApiImpl.cpp)
target_link_libraries(nrfsc ${IBVERBS_LIB} ${FUSE_LIB})
target_link_libraries(jninrfs ${IBVERBS_LIB} ${FUSE_LIB})
# Build client test
find_package(MPI REQUIRED)
set (CMAKE_C_COMPILER ${MPI_C_COMPILER})
set (CMAKE_CXX_COMPILER ${MPI_CXX_COMPILER})
#set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -L/usr/lib64/libfuse.so")
#set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -L/usr/local/lib/libfuse.so")

link_libraries(nrfsc)

#add_executable(ttt ${PROJECT_SOURCE_DIR}/src/test/ttt.cpp)

file(GLOB TEST_SOURCES ${PROJECT_SOURCE_DIR}/src/test/*.cpp)
foreach (TSTSRC ${TEST_SOURCES})
    get_filename_component(TSTNAME ${TSTSRC} NAME_WE)
    add_executable(${TSTNAME} ${TSTSRC})
endforeach()

