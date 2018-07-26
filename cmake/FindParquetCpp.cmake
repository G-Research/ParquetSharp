
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(ParquetCpp_INCLUDE_DIR parquet/parquet_version.h)

if (NOT ParquetCpp_LIBRARIES)
	find_library(ParquetCpp_LIBRARY_RELEASE NAMES parquet parquet_static PATH_SUFFIXES lib)
	find_library(ParquetCpp_LIBRARY_DEBUG NAMES parquetd parquet_staticd PATH_SUFFIXES debug debug/lib)	
    SELECT_LIBRARY_CONFIGURATIONS(ParquetCpp)
endif()

mark_as_advanced(ParquetCpp_FOUND ParquetCpp_INCLUDE_DIR ParquetCpp_LIBRARY_RELEASE ParquetCpp_LIBRARY_DEBUG)
find_package_handle_standard_args(ParquetCpp REQUIRED_VARS ParquetCpp_INCLUDE_DIR ParquetCpp_LIBRARIES)
	
if(ParquetCpp_FOUND)
	set(ParquetCpp_INCLUDE_DIRS ${ParquetCpp_INCLUDE_DIR})
endif()
