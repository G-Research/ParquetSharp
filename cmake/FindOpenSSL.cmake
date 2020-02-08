
include(SelectLibraryConfigurations)
include(FindPackageHandleStandardArgs)

find_path(SSL_INCLUDE_DIR openssl/ssl.h)

if (NOT SSL_LIBRARIES)
	find_library(SSL_LIBRARY_RELEASE NAMES crypto libcrypto PATHS ${CMAKE_PREFIX_PATH}/lib NO_DEFAULT_PATH)
	find_library(SSL_LIBRARY_DEBUG NAMES crypto libcrypto PATHS ${CMAKE_PREFIX_PATH}/debug/lib NO_DEFAULT_PATH)	
    SELECT_LIBRARY_CONFIGURATIONS(SSL)
endif()

mark_as_advanced(SSL_FOUND SSL_INCLUDE_DIR SSL_LIBRARY_RELEASE SSL_LIBRARY_DEBUG)
find_package_handle_standard_args(SSL REQUIRED_VARS SSL_INCLUDE_DIR SSL_LIBRARIES)
	
if(SSL_FOUND)
	set(SSL_INCLUDE_DIRS ${SSL_INCLUDE_DIR})
endif()
