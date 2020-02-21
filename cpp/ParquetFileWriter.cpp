
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/io/file.h>
#include <parquet/file_writer.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_OpenFile(
		const char* const path, 
		const std::shared_ptr<schema::GroupNode>* schema, 
		const std::shared_ptr<WriterProperties>* writer_properties, 
		const std::shared_ptr<const KeyValueMetadata>* key_value_metadata,
		ParquetFileWriter** writer)
	{
		TRYCATCH
		(
			PARQUET_ASSIGN_OR_THROW(
				std::shared_ptr<::arrow::io::FileOutputStream> file,
				::arrow::io::FileOutputStream::Open(path));

			*writer = ParquetFileWriter::Open(file, *schema, *writer_properties, key_value_metadata == nullptr ? nullptr : *key_value_metadata).release();
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_Open(
		std::shared_ptr<arrow::io::OutputStream>* output_stream,
		const std::shared_ptr<schema::GroupNode>* schema, 
		const std::shared_ptr<WriterProperties>* writer_properties, 
		const std::shared_ptr<const KeyValueMetadata>* key_value_metadata,
		ParquetFileWriter** writer)
	{
		TRYCATCH(*writer = ParquetFileWriter::Open(*output_stream, *schema, *writer_properties, key_value_metadata == nullptr ? nullptr : *key_value_metadata).release();)
	}

	PARQUETSHARP_EXPORT void ParquetFileWriter_Free(ParquetFileWriter* writer)
	{
		delete writer;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_Close(ParquetFileWriter* writer)
	{
		TRYCATCH(writer->Close();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_AppendRowGroup(ParquetFileWriter* writer, RowGroupWriter** row_group_writer)
	{
		TRYCATCH(*row_group_writer = writer->AppendRowGroup();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_AppendBufferedRowGroup(ParquetFileWriter* writer, RowGroupWriter** row_group_writer)
	{
		TRYCATCH(*row_group_writer = writer->AppendBufferedRowGroup();)
	}
}
