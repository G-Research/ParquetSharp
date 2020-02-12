include(GenerateExportHeader)

find_package(Arrow REQUIRED)
find_package(Boost COMPONENTS filesystem regex system REQUIRED)
find_package(Brotli REQUIRED)
find_package(BZip2 REQUIRED)
find_package(double-conversion REQUIRED)
find_package(Gflags REQUIRED)
find_package(Glog REQUIRED)
find_package(Lz4 REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Snappy REQUIRED)
find_package(Thrift REQUIRED)
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
	ColumnDecryptionProperties.cpp
	ColumnDecryptionPropertiesBuilder.cpp
	ColumnDescriptor.cpp
	ColumnEncryptionProperties.cpp
	ColumnEncryptionPropertiesBuilder.cpp
	ColumnPath.cpp
	ColumnReader.cpp
	ColumnWriter.cpp
	CString.h
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
	${Arrow_INCLUDE_DIRS} 
	${Boost_INCLUDE_DIRS} 
	${Brotli_INCLUDE_DIRS} 
	${BZip2_INCLUDE_DIRS}
	${Gflags_INCLUDE_DIRS}
	${Glog_INCLUDE_DIRS}
	${Lz4_INCLUDE_DIRS} 
	${Snappy_INCLUDE_DIRS}
	${SSL_INCLUDE_DIRS}
	${Thrift_INCLUDE_DIRS}
	${ZLIB_INCLUDE_DIRS}
	${Zstd_INCLUDE_DIRS} 
	${ParquetCpp_INCLUDE_DIRS})

target_link_libraries(ParquetSharpNative 
	${ParquetCpp_LIBRARIES}
	${Arrow_LIBRARIES} 
	${Boost_LIBRARIES}
	${Brotli_LIBRARIES}
	${BZip2_LIBRARIES}
	${Crypto_LIBRARIES}
	double-conversion::double-conversion
	${Glog_LIBRARIES}
	${Gflags_LIBRARIES}
	${Lz4_LIBRARIES}
	${Snappy_LIBRARIES}
	${SSL_LIBRARIES}
	${Thrift_LIBRARIES}
	${ZLIB_LIBRARIES}
	${Zstd_LIBRARIES} 
	${Threads_LIBRARIES}
	${CMAKE_DL_LIBS}
)
	
add_definitions(-DARROW_STATIC)
add_definitions(-DARROW_NO_DEPRECATED_API)
add_definitions(-DPARQUET_STATIC)
