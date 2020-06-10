cmake_minimum_required(VERSION 3.8)
project(ParquetSharp)

# Exclude MinSizeRel and RelWithDebugInfo, to simplify integration with C#
if (MSVC)
	set(CMAKE_CONFIGURATION_TYPES "Debug;Release" CACHE STRING "Semicolon separated list of supported configuration types" FORCE)
endif ()

# On Linux, default to Release if not specified.
if (UNIX AND NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Specifies the build type on single-configuration generators" FORCE)
endif ()

set(CMAKE_DEBUG_POSTFIX d)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
set (CMAKE_CXX_STANDARD 11)

foreach (OUTPUTCONFIG ${CMAKE_CONFIGURATION_TYPES})
    string(TOUPPER ${OUTPUTCONFIG} OUTPUTCONFIG)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${PROJECT_SOURCE_DIR}/bin)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${PROJECT_SOURCE_DIR}/bin)
endforeach()

if (MSVC)

	foreach (flag_var CMAKE_CXX_FLAGS CMAKE_CXX_FLAGS_DEBUG CMAKE_CXX_FLAGS_RELEASE CMAKE_CXX_FLAGS_MINSIZEREL CMAKE_CXX_FLAGS_RELWITHDEBINFO)
		if (${flag_var} MATCHES "/MD")
			string(REGEX REPLACE "/MD" "/MT" ${flag_var} "${${flag_var}}")
		endif()
	endforeach()

	add_compile_options("/MP") # Compile files in parallel.
	add_compile_options("/WX") # Threat warnings as errors.

endif ()

if (UNIX)
	
	set(CMAKE_SHARED_LIBRARY_PREFIX )

	add_compile_options("-Wall")
	add_compile_options("-Werror")
	add_compile_options("-fvisibility-inlines-hidden")

  if(CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-undefined")
  endif()

endif ()

if (MSVC)
	include_external_msproject(ParquetSharp ${CMAKE_SOURCE_DIR}/csharp/ParquetSharp.csproj TYPE 9A19103F-16F7-4668-BE54-9A1E7A4F7556 PLATFORM x64)
	add_dependencies(ParquetSharp ParquetSharpNative)

	include_external_msproject(ParquetSharp.Test ${CMAKE_SOURCE_DIR}/csharp.test/ParquetSharp.Test.csproj TYPE 9A19103F-16F7-4668-BE54-9A1E7A4F7556 PLATFORM x64)
	add_dependencies(ParquetSharp.Test ParquetSharp)
endif ()

add_subdirectory(cpp)
