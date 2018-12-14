
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(Thrift_INCLUDE_DIR thrift/Thrift.h PATH_SUFFIXES include)

if (NOT Thrift_LIBRARIES)
	
	find_library(Thrift_Common_LIBRARY_RELEASE NAMES thrift thriftmd PATH_SUFFIXES lib)
	find_library(Thrift_Common_LIBRARY_DEBUG NAMES thriftd thriftmdd PATH_SUFFIXES debug debug/lib)	
    
	find_library(Thrift_Nb_LIBRARY_RELEASE NAMES thriftnb thriftnbmd PATH_SUFFIXES lib)
	find_library(Thrift_Nb_LIBRARY_DEBUG NAMES thriftnbd thriftnbmdd PATH_SUFFIXES debug debug/lib)	
    
	find_library(Thrift_Z_LIBRARY_RELEASE NAMES thriftz thriftzmd PATH_SUFFIXES lib)
	find_library(Thrift_Z_LIBRARY_DEBUG NAMES thriftzd thriftzmdd PATH_SUFFIXES debug debug/lib)	
    
	SELECT_LIBRARY_CONFIGURATIONS(Thrift_Common)
	SELECT_LIBRARY_CONFIGURATIONS(Thrift_Nb)
	SELECT_LIBRARY_CONFIGURATIONS(Thrift_Z)
	
endif()

mark_as_advanced(Thrift_Common_FOUND Thrift_INCLUDE_DIR Thrift_Common_LIBRARY_RELEASE Thrift_Common_LIBRARY_DEBUG)
mark_as_advanced(Thrift_Nb_FOUND Thrift_INCLUDE_DIR Thrift_Nb_LIBRARY_RELEASE Thrift_Nb_LIBRARY_DEBUG)
mark_as_advanced(Thrift_Z_FOUND Thrift_INCLUDE_DIR Thrift_Z_LIBRARY_RELEASE Thrift_Z_LIBRARY_DEBUG)

find_package_handle_standard_args(Thrift REQUIRED_VARS Thrift_INCLUDE_DIR Thrift_Common_LIBRARIES Thrift_Nb_LIBRARIES Thrift_Z_LIBRARIES)
	
if(Thrift_Common_FOUND AND Thrift_Nb_FOUND AND Thrift_Z_FOUND)
	set(Thrift_INCLUDE_DIRS ${Thrift_INCLUDE_DIR})
	set(Thrift_LIBRARIES ${Thrift_Common_LIBRARIES} ${Thrift_Nb_LIBRARIES} ${Thrift_Z_LIBRARIES})
endif()
