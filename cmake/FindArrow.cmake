
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(Arrow_INCLUDE_DIR arrow/api.h)

if (NOT Arrow_LIBRARIES)
	find_library(Arrow_LIBRARY_RELEASE NAMES arrow arrow_static PATHS ${CMAKE_PREFIX_PATH}/lib NO_DEFAULT_PATH)
	find_library(Arrow_LIBRARY_DEBUG NAMES arrow arrow_static PATHS ${CMAKE_PREFIX_PATH}/debug/lib NO_DEFAULT_PATH)
    SELECT_LIBRARY_CONFIGURATIONS(Arrow)
endif()

mark_as_advanced(Arrow_FOUND Arrow_INCLUDE_DIR Arrow_LIBRARY_RELEASE Arrow_LIBRARY_DEBUG)
find_package_handle_standard_args(Arrow REQUIRED_VARS Arrow_INCLUDE_DIR Arrow_LIBRARIES)
	
if(Arrow_FOUND)
	set(Arrow_INCLUDE_DIRS ${Arrow_INCLUDE_DIR})
endif()
