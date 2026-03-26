include(GenerateExportHeader)

find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

#TODO: workaround for https://github.com/microsoft/vcpkg/pull/48373/changes#r2952691564
if(ARROW_MIMALLOC)
	find_package(mimalloc CONFIG REQUIRED)
	target_link_libraries(Arrow::arrow_static INTERFACE mimalloc-static)
endif()

# TODO: workaround for https://github.com/apache/arrow/issues/49499
# Arrow's FindSnappyAlt and FindBrotliAlt modules only set the generic
# IMPORTED_LOCATION (which resolves to the debug library). On multi-config
# generators like Visual Studio this causes the Release build to link against
# debug libraries. Fix up the targets with per-configuration paths.
if (MSVC)
	set(_VCPKG_LIB_DIR "${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}")

	foreach(_snappy_target Snappy::snappy-static Snappy::snappy)
		if (TARGET ${_snappy_target})
			set_target_properties(${_snappy_target} PROPERTIES
				IMPORTED_LOCATION_RELEASE "${_VCPKG_LIB_DIR}/lib/snappy.lib"
				IMPORTED_LOCATION_DEBUG   "${_VCPKG_LIB_DIR}/debug/lib/snappy.lib"
			)
			set_property(TARGET ${_snappy_target} PROPERTY IMPORTED_LOCATION)
		endif()
	endforeach()

	foreach(_brotli_target brotlicommon brotlidec brotlienc)
		if (TARGET Brotli::${_brotli_target})
			set_target_properties(Brotli::${_brotli_target} PROPERTIES
				IMPORTED_LOCATION_RELEASE "${_VCPKG_LIB_DIR}/lib/${_brotli_target}.lib"
				IMPORTED_LOCATION_DEBUG   "${_VCPKG_LIB_DIR}/debug/lib/${_brotli_target}.lib"
			)
			set_property(TARGET Brotli::${_brotli_target} PROPERTY IMPORTED_LOCATION)
		endif()
	endforeach()
endif()

add_library(ParquetSharpNative SHARED 
	AesKey.h
	Buffer.cpp
	BufferReader.cpp
	BufferOutputStream.cpp
	ColumnChunkMetaData.cpp
	ColumnCryptoMetaData.cpp
	ColumnDecryptionProperties.cpp
	ColumnDecryptionPropertiesBuilder.cpp
	ColumnDescriptor.cpp
	ColumnEncryptionProperties.cpp
	ColumnEncryptionPropertiesBuilder.cpp
	ColumnPath.cpp
	ColumnReader.cpp
	ColumnWriter.cpp
	CString.h
	Enums.cpp
	ExceptionInfo.h
	ExceptionInfo.cpp
	FileDecryptionProperties.cpp
	FileDecryptionPropertiesBuilder.cpp
	FileEncryptionProperties.cpp
	FileEncryptionPropertiesBuilder.cpp
	FileMetaData.cpp
	GroupNode.cpp
	KeyValueMetadata.cpp
	LogicalType.cpp
	ManagedAadPrefixVerifier.h
	ManagedDecryptionKeyRetriever.h
	ManagedOutputStream.cpp
	ManagedRandomAccessFile.cpp
	MemoryPool.cpp
	Node.cpp
	OutputStream.cpp
	ParquetFileReader.cpp
	ParquetFileWriter.cpp
	PrimitiveNode.cpp
	RandomAccessFile.cpp
	ReaderProperties.cpp
	ResizableBuffer.cpp
	RowGroupMetaData.cpp
	RowGroupReader.cpp
	RowGroupWriter.cpp
	SchemaDescriptor.cpp
	Statistics.cpp
	TypedColumnReader.cpp
	TypedColumnWriter.cpp
	TypedStatistics.cpp
	WriterProperties.cpp
	WriterPropertiesBuilder.cpp
	arrow/ArrowReaderProperties.cpp
	arrow/ArrowWriterProperties.cpp
	arrow/ArrowWriterPropertiesBuilder.cpp
	arrow/FileReader.cpp
	arrow/FileWriter.cpp
	arrow/SchemaField.cpp
	arrow/SchemaManifest.cpp
	encryption/CryptoFactory.cpp
	encryption/DecryptionConfiguration.cpp
	encryption/EncryptionConfiguration.cpp
	encryption/KmsConnectionConfig.cpp
	encryption/ManagedKmsClient.h
	encryption/ManagedKmsClientFactory.h
)

generate_export_header(ParquetSharpNative 
	EXPORT_MACRO_NAME PARQUETSHARP_EXPORT 
	EXPORT_FILE_NAME ParquetSharpExport.h
)

include_directories(
	${PROJECT_BINARY_DIR}
	${PARQUET_INCLUDE_DIRS})

target_link_libraries(ParquetSharpNative PRIVATE
	Parquet::parquet_static
	Arrow::arrow_static
)

add_definitions(-DARROW_STATIC)
add_definitions(-DARROW_NO_DEPRECATED_API)
add_definitions(-DPARQUET_STATIC)

if (MSVC AND (MSVC_VERSION GREATER 1900))
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /guard:cf")
endif()

if (UNIX AND CMAKE_BUILD_TYPE STREQUAL Release)
	if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin")
		set(STRIP_OPTIONS -x)
	endif()
	add_custom_command(TARGET ParquetSharpNative POST_BUILD
		COMMAND ${CMAKE_STRIP} ${STRIP_OPTIONS} $<TARGET_FILE:ParquetSharpNative>)
endif ()
