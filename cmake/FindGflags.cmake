
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(Gflags_INCLUDE_DIR gflags/gflags.h)

if (NOT Gflags_LIBRARIES)
	find_library(Gflags_LIBRARY_RELEASE NAMES gflags gflags_static PATH_SUFFIXES lib)
	find_library(Gflags_LIBRARY_DEBUG NAMES gflags_debug gflags_static_debug PATH_SUFFIXES debug debug/lib)	
    SELECT_LIBRARY_CONFIGURATIONS(Gflags)
endif()

mark_as_advanced(Gflags_FOUND Gflags_INCLUDE_DIR Gflags_LIBRARY_RELEASE Gflags_LIBRARY_DEBUG)
find_package_handle_standard_args(Gflags REQUIRED_VARS Gflags_INCLUDE_DIR Gflags_LIBRARIES)
	
if(Gflags_FOUND)
	set(Gflags_INCLUDE_DIRS ${Gflags_INCLUDE_DIR})
endif()
