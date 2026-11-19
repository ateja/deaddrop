cmake_minimum_required(VERSION 3.18)
project(deaddrop_listener LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Repository root and where `cargo build --release -p deaddrop-ffi` puts the library.
get_filename_component(DEADDROP_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)
set(DEADDROP_LIB_DIR "${DEADDROP_ROOT}/target/release" CACHE PATH "Directory containing the deaddrop library")

add_executable(listener listener.cpp)
target_include_directories(listener PRIVATE "${DEADDROP_ROOT}/crates/deaddrop-ffi/include")

if(MSVC)
  target_compile_options(listener PRIVATE /W4 /EHsc)
else()
  target_compile_options(listener PRIVATE -Wall -Wextra)
endif()

if(WIN32)
  # Rust's MSVC cdylib: deaddrop.dll plus its import library deaddrop.dll.lib.
  set(DEADDROP_IMPORT_LIB "${DEADDROP_LIB_DIR}/deaddrop.dll.lib")
  if(NOT EXISTS "${DEADDROP_IMPORT_LIB}")
    message(FATAL_ERROR "Missing ${DEADDROP_IMPORT_LIB}. Run: cargo build --release -p deaddrop-ffi")
  endif()
  target_link_libraries(listener PRIVATE "${DEADDROP_IMPORT_LIB}")
  # Put the DLL next to the executable so it is found at run time.
  add_custom_command(TARGET listener POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different "${DEADDROP_LIB_DIR}/deaddrop.dll" "$<TARGET_FILE_DIR:listener>")
else()
  # Prefer the shared library (.dylib / .so); the static archive needs extra system libs.
  find_library(DEADDROP_LIB NAMES deaddrop PATHS "${DEADDROP_LIB_DIR}" NO_DEFAULT_PATH)
  if(NOT DEADDROP_LIB)
    message(FATAL_ERROR "libdeaddrop not found in ${DEADDROP_LIB_DIR}. Run: cargo build --release -p deaddrop-ffi")
  endif()
  target_link_libraries(listener PRIVATE "${DEADDROP_LIB}")
  # Let the binary find the library without DYLD_/LD_LIBRARY_PATH.
  set_target_properties(listener PROPERTIES BUILD_RPATH "${DEADDROP_LIB_DIR}")
endif()
