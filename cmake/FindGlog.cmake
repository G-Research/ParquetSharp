
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(Glog_INCLUDE_DIR glog/logging.h)

if (NOT Glog_LIBRARIES)
	find_library(Glog_LIBRARY_RELEASE NAMES glog PATHS ${CMAKE_PREFIX_PATH}/lib NO_DEFAULT_PATH)
	find_library(Glog_LIBRARY_DEBUG NAMES glog PATHS ${CMAKE_PREFIX_PATH}/debug/lib NO_DEFAULT_PATH)
    SELECT_LIBRARY_CONFIGURATIONS(Glog)
endif()

mark_as_advanced(Glog_FOUND Glog_INCLUDE_DIR Glog_LIBRARY_RELEASE Glog_LIBRARY_DEBUG)
find_package_handle_standard_args(Glog REQUIRED_VARS Glog_INCLUDE_DIR Glog_LIBRARIES)
	
if(Glog_FOUND)
	set(Glog_INCLUDE_DIRS ${Glog_INCLUDE_DIR})
endif()
