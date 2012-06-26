# - Try to find JSON
# Once done, this will define
#
#  JSON_FOUND - system has JSON
#  JSON_INCLUDE_DIRS - the JSON include directories
#  JSON_LIBRARIES - link these to use JSON

find_package(PkgConfig)
pkg_check_modules(PC_JSON json)

FIND_PATH(JSON_INCLUDE_DIR json.h
  HINTS
  ${PC_JSON_INCLUDE_DIRS}
  /usr/include
  /usr/local/include
  /opt/local/include
  PATH_SUFFIXES json
)

FIND_LIBRARY(JSON_LIBRARY
  NAMES ${JSON_NAMES} libjson.so libjson.dylib
  HINTS ${PC_JSON_LIBRARY_DIRS}
  /usr/lib /usr/local/lib /opt/local/lib
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(JSON DEFAULT_MSG JSON_LIBRARY JSON_INCLUDE_DIR)

IF(JSON_FOUND)
  SET(JSON_LIBRARIES ${JSON_LIBRARY})
  SET(JSON_INCLUDE_DIRS ${JSON_INCLUDE_DIR})
ENDIF(JSON_FOUND)
