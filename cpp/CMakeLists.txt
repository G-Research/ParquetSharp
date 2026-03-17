include(GenerateExportHeader)

find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

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
