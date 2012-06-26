cmake_minimum_required (VERSION 2.6)
project (u1db)

# Allow the developer to select if Dynamic or Static libraries are built
OPTION (BUILD_SHARED_LIBS "Build Shared Libraries" OFF)

#Set the LIB_TYPE variable to STATIC
SET (LIB_TYPE STATIC)
IF (BUILD_SHARED_LIBS)
  # User wants to build Dynamic Libraries, so change the LIB_TYPE variable to CMake keyword 'SHARED'
  SET (LIB_TYPE SHARED)
ENDIF (BUILD_SHARED_LIBS)


set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/../cmake/Modules/")
include_directories(${CMAKE_SOURCE_DIR}/../include)

find_package(CURL REQUIRED)
if (CURL_FOUND)
  include_directories(${CURL_INCLUDE_DIRS})
endif (CURL_FOUND)

find_package(OAUTH REQUIRED)
if (OAUTH_FOUND)
  include_directories(${OAUTH_INCLUDE_DIRS})
endif (OAUTH_FOUND)

find_package(JSON REQUIRED)
if (JSON_FOUND)
  include_directories(${JSON_INCLUDE_DIRS})
endif (JSON_FOUND)

find_package(Sqlite3 REQUIRED)
if (Sqlite3_FOUND)
  include_directories(${Sqlite3_INCLUDE_DIRS})
endif (Sqlite3_FOUND)

add_custom_command (
  OUTPUT ${CMAKE_SOURCE_DIR}/u1db_schema.c
  COMMAND python ${CMAKE_SOURCE_DIR}/../sql_to_c.py ${CMAKE_SOURCE_DIR}/../u1db/backends/dbschema.sql u1db__schema ${CMAKE_SOURCE_DIR}/u1db_schema.c
  DEPENDS ${CMAKE_SOURCE_DIR}/../u1db/backends/dbschema.sql ${CMAKE_SOURCE_DIR}/../sql_to_c.py
  )
add_library (u1db ${LIB_TYPE} mkstemp_compat.c u1db.c u1db_http_sync_target.c u1db_query.c  u1db_sync_target.c  u1db_uuid.c  u1db_vectorclock.c u1db_schema.c)

SET_TARGET_PROPERTIES(u1db PROPERTIES SOVERSION 1)
