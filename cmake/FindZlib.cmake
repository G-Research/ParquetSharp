
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(Zlib_INCLUDE_DIR zlib.h)

if (NOT Zlib_LIBRARIES)
	find_library(Zlib_LIBRARY_RELEASE NAMES zlib PATH_SUFFIXES lib)
	find_library(Zlib_LIBRARY_DEBUG NAMES zlibd PATH_SUFFIXES debug/lib)	
    SELECT_LIBRARY_CONFIGURATIONS(Zlib)
endif()

mark_as_advanced(Zlib_FOUND Zlib_INCLUDE_DIR Zlib_LIBRARY_RELEASE Zlib_LIBRARY_DEBUG)
find_package_handle_standard_args(Zlib REQUIRED_VARS Zlib_INCLUDE_DIR Zlib_LIBRARIES)
	
if(Zlib_FOUND)
	set(Zlib_INCLUDE_DIRS ${Zlib_INCLUDE_DIR})
endif()
