
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(Zstd_INCLUDE_DIR zstd.h)

if (NOT Zstd_LIBRARIES)
	find_library(Zstd_LIBRARY_RELEASE NAMES zstd zstd_static PATH_SUFFIXES lib)
	find_library(Zstd_LIBRARY_DEBUG NAMES zstdd zstd_staticd PATH_SUFFIXES debug debug/lib)	
    SELECT_LIBRARY_CONFIGURATIONS(Zstd)
endif()

mark_as_advanced(Zstd_FOUND Zstd_INCLUDE_DIR Zstd_LIBRARY_RELEASE Zstd_LIBRARY_DEBUG)
find_package_handle_standard_args(Zstd REQUIRED_VARS Zstd_INCLUDE_DIR Zstd_LIBRARIES)
	
if(Zstd_FOUND)
	set(Zstd_INCLUDE_DIRS ${Zstd_INCLUDE_DIR})
endif()
