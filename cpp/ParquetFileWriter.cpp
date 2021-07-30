
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
				const std::shared_ptr<::arrow::io::FileOutputStream> file,
				::arrow::io::FileOutputStream::Open(path));

			*writer = ParquetFileWriter::Open(file, *schema, *writer_properties, key_value_metadata == nullptr ? nullptr : *key_value_metadata).release();
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_Open(
		std::shared_ptr<::arrow::io::OutputStream>* output_stream,
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

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_Num_Columns(ParquetFileWriter* writer, int* num_columns)
	{
		TRYCATCH(*num_columns = writer->num_columns();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_Num_Rows(ParquetFileWriter* writer, int64_t* num_rows)
	{
		TRYCATCH(*num_rows = writer->num_rows();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_Num_Row_Groups(ParquetFileWriter* writer, int* num_row_groups)
	{
		TRYCATCH(*num_row_groups = writer->num_row_groups();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_Properties(ParquetFileWriter* writer, const std::shared_ptr<WriterProperties>** properties)
	{
		TRYCATCH(*properties = new std::shared_ptr(writer->properties());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_Schema(ParquetFileWriter* writer, const SchemaDescriptor** schema)
	{
		TRYCATCH(*schema = writer->schema();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_Descr(ParquetFileWriter* writer, const int i, const ColumnDescriptor** descr)
	{
		TRYCATCH(*descr = writer->descr(i);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_Key_Value_Metadata(ParquetFileWriter* writer, const std::shared_ptr<const KeyValueMetadata>** key_value_metadata)
	{
		TRYCATCH
		(
			const auto& m = writer->key_value_metadata();
			*key_value_metadata = m ? new std::shared_ptr(m) : nullptr;
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ParquetFileWriter_Metadata(ParquetFileWriter* writer, const std::shared_ptr<FileMetaData>** metadata)
	{
		TRYCATCH
		(
			const auto m = writer->metadata();
			*metadata = m ? new std::shared_ptr(m) : nullptr;
		)
	}
}
