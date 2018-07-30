cmake_minimum_required(VERSION 3.8)
project(ParquetSharp)

# Exclude MinSizeRel and RelWithDebugInfo, to simplify integration with C#
set(CMAKE_CONFIGURATION_TYPES "Debug;Release" CACHE STRING "Semicolon separated list of supported configuration types" FORCE)

set(CMAKE_DEBUG_POSTFIX d)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
# set(CMAKE_PREFIX_PATH "F:/Temporary/Parquet/20180517/x64-windows-static" CACHE STRING "The default location for CMake to look libraries in.")

set(BOOST_LIBRARYDIR "${CMAKE_PREFIX_PATH}/debug/lib")
set(ZLIB_ROOT "${CMAKE_PREFIX_PATH}/debug")

if (MSVC)

	foreach (flag_var CMAKE_CXX_FLAGS CMAKE_CXX_FLAGS_DEBUG CMAKE_CXX_FLAGS_RELEASE CMAKE_CXX_FLAGS_MINSIZEREL CMAKE_CXX_FLAGS_RELWITHDEBINFO)
		if (${flag_var} MATCHES "/MD")
			string(REGEX REPLACE "/MD" "/MT" ${flag_var} "${${flag_var}}")
		endif()
	endforeach()

	add_compile_options($<$<CXX_COMPILER_ID:MSVC>:/MP>)

endif ()

include_external_msproject(ParquetSharp ${CMAKE_SOURCE_DIR}/csharp/ParquetSharp.csproj TYPE FAE04EC0-301F-11D3-BF4B-00C04F79EFBC)
add_dependencies(ParquetSharp ParquetSharpNative)

include_external_msproject(ParquetSharp.Test ${CMAKE_SOURCE_DIR}/csharp.test/ParquetSharp.Test.csproj TYPE FAE04EC0-301F-11D3-BF4B-00C04F79EFBC)
add_dependencies(ParquetSharp.Test ParquetSharp)

add_subdirectory(cpp)
