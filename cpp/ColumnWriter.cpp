
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/column_writer.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ColumnWriter_Close(ColumnWriter* column_writer, int64_t* column_size)
	{
		TRYCATCH(*column_size = column_writer->Close();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnWriter_Descr(const ColumnWriter* column_writer, const ColumnDescriptor** column_descriptor)
	{
		TRYCATCH(*column_descriptor = column_writer->descr();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnWriter_Properties(ColumnWriter* column_writer, std::shared_ptr<const WriterProperties>** writer_properties)
	{
		// Take a no-op ownership of the writer properties, as all the rest of the API uses shared_ptr.
		TRYCATCH(*writer_properties = new std::shared_ptr<const WriterProperties>(column_writer->properties(), [](const WriterProperties*) {});)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnWriter_Rows_Written(const ColumnWriter* column_writer, int64_t* rows_written)
	{
		TRYCATCH(*rows_written = column_writer->rows_written();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnWriter_Total_Compressed_Bytes(const ColumnWriter* column_writer, int64_t* total_compressed_bytes)
	{
		TRYCATCH(*total_compressed_bytes = column_writer->total_compressed_bytes();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnWriter_Total_Bytes_Written(const ColumnWriter* column_writer, int64_t* total_bytes_written)
	{
		TRYCATCH(*total_bytes_written = column_writer->total_bytes_written();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnWriter_Total_Compressed_Bytes_Written(const ColumnWriter* column_writer, int64_t* total_compressed_bytes_written)
	{
		TRYCATCH(*total_compressed_bytes_written = column_writer->total_compressed_bytes_written();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnWriter_Estimated_Buffered_Stats(
		const ColumnWriter* column_writer,
		int64_t* def_level_bytes,
		int64_t* rep_level_bytes,
		int64_t* value_bytes,
		int64_t* dict_bytes)
	{
		TRYCATCH(
			*def_level_bytes = column_writer->estimated_buffered_def_level_bytes();
			*rep_level_bytes = column_writer->estimated_buffered_rep_level_bytes();
			*value_bytes = column_writer->estimated_buffered_value_bytes();
			*dict_bytes = column_writer->estimated_buffered_dict_bytes();
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnWriter_Type(const ColumnWriter* column_writer, Type::type* type)
	{
		TRYCATCH(*type = column_writer->type();)
	}
}
