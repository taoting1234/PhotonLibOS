add_executable(
    example-packaged-task
    ${CMAKE_SOURCE_DIR}/alilfs-extend/example/example-packaged-task.cpp)
target_include_directories(example-packaged-task PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(example-packaged-task PRIVATE photon_shared)

# shared_mutex unit tests
add_executable(
    test-shared-mutex
    ${CMAKE_SOURCE_DIR}/alilfs-extend/example/test-shared-mutex.cpp)
target_include_directories(test-shared-mutex PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(test-shared-mutex PRIVATE photon_shared ${testing_libs})
add_test(NAME test-shared-mutex COMMAND $<TARGET_FILE:test-shared-mutex>)

# shared_mutex performance benchmark
add_executable(
    perf-shared-mutex
    ${CMAKE_SOURCE_DIR}/alilfs-extend/example/perf-shared-mutex.cpp)
target_include_directories(perf-shared-mutex PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(perf-shared-mutex PRIVATE photon_shared)
