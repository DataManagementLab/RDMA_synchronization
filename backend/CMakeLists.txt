# ---------------------------------------------------------------------------
# NAM-DB
# ---------------------------------------------------------------------------
project(nam)

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------
file(GLOB_RECURSE NAM_CC **.cpp **/**.cpp **.hpp **/**.hpp)
if (NOT UNIX)
    message(SEND_ERROR "unsupported platform")
endif ()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(nam STATIC ${NAM_CC})

OPTION(SANI "Compile nam with sanitizers" OFF)
IF(SANI)
  if (CMAKE_BUILD_TYPE MATCHES Debug)
    message("Compiling with Sanitizers")
    target_compile_options(nam PUBLIC -fsanitize=address)
    target_link_libraries(nam asan)
  endif ()
ENDIF(SANI)

target_link_libraries(nam gflags Threads::Threads atomic numa rdmacm ibverbs aio) 
target_include_directories(nam PUBLIC ${SHARED_INCLUDE_DIRECTORY})
target_include_directories(nam PRIVATE ${CMAKE_CURRENT_LIST_DIR})
# ---------------------------------------------------------------------------
set(NAM_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})
set_property(TARGET nam APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${NAM_INCLUDE_DIR})
