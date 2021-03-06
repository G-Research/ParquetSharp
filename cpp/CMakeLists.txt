include(GenerateExportHeader)

find_package(Arrow REQUIRED)
find_package(Boost COMPONENTS filesystem regex system REQUIRED)
find_package(unofficial-brotli CONFIG REQUIRED)
find_package(BZip2 REQUIRED)
find_package(gflags CONFIG REQUIRED)
find_package(glog CONFIG REQUIRED)
find_package(lz4 CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(re2 CONFIG REQUIRED)
find_package(Snappy CONFIG REQUIRED)
find_package(Thrift CONFIG REQUIRED)
find_package(utf8proc REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Zstd REQUIRED)
find_package(ParquetCpp REQUIRED)

if (WIN32)
	set(Crypto_LIBRARIES crypt32)
endif ()

if (UNIX)
	find_package(Threads REQUIRED)
	set(Threads_LIBRARIES Threads::Threads)
endif ()

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
)

generate_export_header(ParquetSharpNative 
	EXPORT_MACRO_NAME PARQUETSHARP_EXPORT 
	EXPORT_FILE_NAME ParquetSharpExport.h
)

include_directories(
	${PROJECT_BINARY_DIR}
	${ParquetCpp_INCLUDE_DIRS})

target_link_libraries(ParquetSharpNative PRIVATE
	${ParquetCpp_LIBRARIES}
	${Arrow_LIBRARIES}
	${Boost_LIBRARIES}
	unofficial::brotli::brotlidec-static unofficial::brotli::brotlienc-static unofficial::brotli::brotlicommon-static
	BZip2::BZip2
	${Crypto_LIBRARIES}
	glog::glog
	lz4::lz4
	${SSL_LIBRARIES}
	re2::re2
	Snappy::snappy
	thrift::thrift
	${utf8proc_LIBRARIES}
	ZLIB::ZLIB
	${Zstd_LIBRARIES} 
	${Threads_LIBRARIES}
	${CMAKE_DL_LIBS}
)
	
add_definitions(-DARROW_STATIC)
add_definitions(-DARROW_NO_DEPRECATED_API)
add_definitions(-DPARQUET_STATIC)

if (UNIX AND CMAKE_BUILD_TYPE STREQUAL Release)
  if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin")
    set(STRIP_OPTIONS -x)
  endif()
  add_custom_command(TARGET ParquetSharpNative POST_BUILD
    COMMAND ${CMAKE_STRIP} ${STRIP_OPTIONS} $<TARGET_FILE:ParquetSharpNative>)
endif ()
