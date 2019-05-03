include(GenerateExportHeader)

find_package(Arrow REQUIRED)
find_package(Boost COMPONENTS filesystem regex system REQUIRED)
find_package(Brotli REQUIRED)
find_package(Lz4 REQUIRED)
find_package(Snappy REQUIRED)
find_package(Thrift REQUIRED)
find_package(Zlib REQUIRED)
find_package(Zstd REQUIRED)
find_package(ParquetCpp REQUIRED)

if (UNIX)
	find_package(Threads REQUIRED)
	set(Threads_LIBRARIES Threads::Threads)
endif ()

add_library(ParquetSharpNative SHARED 
	Buffer.cpp
	BufferReader.cpp
	BufferOutputStream.cpp
	ColumnChunkMetaData.cpp
	ColumnDescriptor.cpp
	ColumnPath.cpp
	ColumnReader.cpp
	ColumnWriter.cpp
	CString.h
	ExceptionInfo.h
	ExceptionInfo.cpp
	FileMetaData.cpp
	GroupNode.cpp
	InputStream.cpp
	KeyValueMetadata.cpp
	Node.cpp
	OutputStream.cpp
	ParquetFileReader.cpp
	ParquetFileWriter.cpp
	PrimitiveNode.cpp
	ResizableBuffer.cpp
	RowGroupMetaData.cpp
	RowGroupReader.cpp
	RowGroupStatistics.cpp
	RowGroupWriter.cpp
	SchemaDescriptor.cpp
	TypedColumnReader.cpp
	TypedColumnWriter.cpp
	TypedRowGroupStatistics.cpp
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
	${Lz4_INCLUDE_DIRS} 
	${Snappy_INCLUDE_DIRS}
	${Thrift_INCLUDE_DIRS}
	${Zlib_INCLUDE_DIRS}
	${Zstd_INCLUDE_DIRS} 
	${ParquetCpp_INCLUDE_DIRS})

target_link_libraries(ParquetSharpNative 
	${ParquetCpp_LIBRARIES}
	${Arrow_LIBRARIES} 
	${Boost_LIBRARIES}
	${Brotli_LIBRARIES}
	${Lz4_LIBRARIES}
	${Snappy_LIBRARIES}
	${Thrift_LIBRARIES}
	${Zlib_LIBRARIES}
	${Zstd_LIBRARIES} 
	${Threads_LIBRARIES}
)
	
add_definitions(-DARROW_STATIC)
add_definitions(-DPARQUET_STATIC)
