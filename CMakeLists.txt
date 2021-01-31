cmake_minimum_required(VERSION 3.6)
project(MapReduce)


option(USE_MPI "Enable MPI"  ON )
option(USE_OMP "Enable OpenMP"  ON )

find_package(OpenMP)
if (OPENMP_FOUND)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

SET(CMAKE_C_COMPILER /home/alex/.openmpi/bin/mpicc)
SET(CMAKE_CXX_COMPILER /home/alex/.openmpi/bin/mpicxx)



add_executable(MapReduce MapReduce.cpp Tools.cpp Tools.h)



