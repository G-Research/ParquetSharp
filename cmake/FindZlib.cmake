
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(Zlib_INCLUDE_DIR zlib.h PATH_SUFFIXES include)

if (NOT Zlib_LIBRARIES)
	
	find_library(Zlib_LIBRARY_RELEASE NAMES z zlib PATH_SUFFIXES lib)
	find_library(Zlib_LIBRARY_DEBUG NAMES zd zlibd PATH_SUFFIXES debug debug/lib)	
    
	SELECT_LIBRARY_CONFIGURATIONS(Zlib)
	
endif()

find_package_handle_standard_args(Zlib REQUIRED_VARS Zlib_INCLUDE_DIR Zlib_LIBRARIES)
