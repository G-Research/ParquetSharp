
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(BZip2_INCLUDE_DIR bzlib.h)

if (NOT BZip2_LIBRARIES)
	find_library(BZip2_LIBRARY_RELEASE NAMES bz2 PATH_SUFFIXES lib)
	find_library(BZip2_LIBRARY_DEBUG NAMES bz2d PATH_SUFFIXES debug debug/lib)	
    SELECT_LIBRARY_CONFIGURATIONS(BZip2)
endif()

mark_as_advanced(BZip2_FOUND BZip2_INCLUDE_DIR BZip2_LIBRARY_RELEASE BZip2_LIBRARY_DEBUG)
find_package_handle_standard_args(BZip2 REQUIRED_VARS BZip2_INCLUDE_DIR BZip2_LIBRARIES)
	
if(BZip2_FOUND)
	set(BZip2_INCLUDE_DIRS ${BZip2_INCLUDE_DIR})
endif()
