
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(Snappy_INCLUDE_DIR snappy.h)

if (NOT Snappy_LIBRARIES)
	find_library(Snappy_LIBRARY_RELEASE NAMES snappy PATH_SUFFIXES lib)
	find_library(Snappy_LIBRARY_DEBUG NAMES snappyd PATH_SUFFIXES debug debug/lib)	
    SELECT_LIBRARY_CONFIGURATIONS(Snappy)
endif()

mark_as_advanced(Snappy_FOUND Snappy_INCLUDE_DIR Snappy_LIBRARY_RELEASE Snappy_LIBRARY_DEBUG)
find_package_handle_standard_args(Snappy REQUIRED_VARS Snappy_INCLUDE_DIR Snappy_LIBRARIES)
	
if(Snappy_FOUND)
	set(Snappy_INCLUDE_DIRS ${Snappy_INCLUDE_DIR})
endif()
