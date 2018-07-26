
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(Lz4_INCLUDE_DIR lz4.h)

if (NOT Lz4_LIBRARIES)
	find_library(Lz4_LIBRARY_RELEASE NAMES lz4 PATH_SUFFIXES lib)
	find_library(Lz4_LIBRARY_DEBUG NAMES lz4d PATH_SUFFIXES debug debug/lib)	
    SELECT_LIBRARY_CONFIGURATIONS(Lz4)
endif()

mark_as_advanced(Lz4_FOUND Lz4_INCLUDE_DIR Lz4_LIBRARY_RELEASE Lz4_LIBRARY_DEBUG)
find_package_handle_standard_args(Lz4 REQUIRED_VARS Lz4_INCLUDE_DIR Lz4_LIBRARIES)
	
if(Lz4_FOUND)
	set(Lz4_INCLUDE_DIRS ${Lz4_INCLUDE_DIR})
endif()
