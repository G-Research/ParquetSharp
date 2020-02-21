
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/file_reader.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileReader_OpenFile(
		const char* const path,
		const ReaderProperties* reader_properties,
		ParquetFileReader** reader)
	{
		TRYCATCH(*reader = ParquetFileReader::OpenFile(path, false, *reader_properties).release();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileReader_Open(
		std::shared_ptr<arrow::io::RandomAccessFile>* readable_file_interface, 
		const ReaderProperties* reader_properties,
		ParquetFileReader** reader)
	{
		TRYCATCH(*reader = ParquetFileReader::Open(*readable_file_interface, *reader_properties).release();)
	}

	PARQUETSHARP_EXPORT void ParquetFileReader_Free(ParquetFileReader* reader)
	{
		delete reader;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileReader_Close(ParquetFileReader* reader)
	{
		TRYCATCH(reader->Close();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileReader_MetaData(const ParquetFileReader* reader, std::shared_ptr<FileMetaData>** fileMetaData)
	{
		TRYCATCH(*fileMetaData = new std::shared_ptr<FileMetaData>(reader->metadata());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileReader_RowGroup(ParquetFileReader* reader, int i, std::shared_ptr<RowGroupReader>** rowGroupReader)
	{
		TRYCATCH(*rowGroupReader = new std::shared_ptr<RowGroupReader>(reader->RowGroup(i));)
	}
}
