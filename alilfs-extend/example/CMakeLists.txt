add_executable(
    logcompress
    ${CMAKE_SOURCE_DIR}/alilfs-extend/example/example-logfile-compress.cpp)
target_include_directories(logcompress PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(logcompress PRIVATE photon_shared)

add_executable(
    example-packaged-task
    ${CMAKE_SOURCE_DIR}/alilfs-extend/example/example-packaged-task.cpp)
target_include_directories(example-packaged-task PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(example-packaged-task PRIVATE photon_shared)
