
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(utf8proc_INCLUDE_DIR utf8proc.h)

if (NOT utf8proc_LIBRARIES)
    find_library(utf8proc_LIBRARY_RELEASE NAMES utf8proc utf8proc_static PATHS ${CMAKE_PREFIX_PATH}/lib NO_DEFAULT_PATH)
    find_library(utf8proc_LIBRARY_DEBUG NAMES utf8proc utf8proc_static PATHS ${CMAKE_PREFIX_PATH}/debug/lib NO_DEFAULT_PATH)
    SELECT_LIBRARY_CONFIGURATIONS(utf8proc)
endif()

mark_as_advanced(utf8proc_FOUND utf8proc_INCLUDE_DIR utf8proc_LIBRARY_RELEASE utf8proc_LIBRARY_DEBUG)
find_package_handle_standard_args(utf8proc REQUIRED_VARS utf8proc_INCLUDE_DIR utf8proc_LIBRARIES)

if(utf8proc_FOUND)
    set(utf8proc_INCLUDE_DIRS ${utf8proc_INCLUDE_DIR})
endif()
