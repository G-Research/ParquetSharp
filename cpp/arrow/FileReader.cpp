#include <numeric>
#include <arrow/io/file.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include "cpp/ParquetSharpExport.h"
#include "../ExceptionInfo.h"

using namespace parquet::arrow;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* FileReader_OpenPath(
		const char* const path,
    const parquet::ReaderProperties* reader_properties,
    const parquet::ArrowReaderProperties* arrow_reader_properties,
    FileReader** reader)
	{
    TRYCATCH
    (
      arrow::MemoryPool* pool = arrow::default_memory_pool();
      std::shared_ptr<arrow::io::ReadableFile> input_file;
      std::unique_ptr<FileReader> reader_ptr;
      PARQUET_ASSIGN_OR_THROW(input_file, arrow::io::ReadableFile::Open(path, pool));
      FileReaderBuilder builder;
      PARQUET_THROW_NOT_OK(builder.Open(input_file, *reader_properties));
      if (arrow_reader_properties != nullptr) {
        builder.properties(*arrow_reader_properties);
      }
      PARQUET_THROW_NOT_OK(builder.Build(&reader_ptr));
      *reader = reader_ptr.release();
    )
	}

  PARQUETSHARP_EXPORT ExceptionInfo* FileReader_OpenFile(
      std::shared_ptr<::arrow::io::RandomAccessFile>* readable_file_interface,
      const parquet::ReaderProperties* reader_properties,
      const parquet::ArrowReaderProperties* arrow_reader_properties,
      FileReader** reader)
  {
    TRYCATCH
    (
      std::unique_ptr<FileReader> reader_ptr;
      FileReaderBuilder builder;
      PARQUET_THROW_NOT_OK(builder.Open(*readable_file_interface, *reader_properties));
      if (arrow_reader_properties != nullptr) {
        builder.properties(*arrow_reader_properties);
      }
      PARQUET_THROW_NOT_OK(builder.Build(&reader_ptr));
      *reader = reader_ptr.release();
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* FileReader_GetSchema(FileReader* reader, struct ArrowSchema* schema_out)
  {
    TRYCATCH
    (
      std::shared_ptr<arrow::Schema> schema;
      PARQUET_THROW_NOT_OK(reader->GetSchema(&schema));
      PARQUET_THROW_NOT_OK(arrow::ExportSchema(*schema, schema_out));
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* FileReader_NumRowGroups(FileReader* reader, int* num_row_groups)
  {
    TRYCATCH(*num_row_groups = reader->num_row_groups();)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* FileReader_GetRecordBatchReader(FileReader* reader, struct ArrowArrayStream* stream_out)
  {
    TRYCATCH
    (
        std::shared_ptr<arrow::RecordBatchReader> batch_reader;
        // TODO: Use method that doesn't require passing row groups vector when updating to latest Arrow
        std::vector<int> row_groups(reader->num_row_groups());
        std::iota(row_groups.begin(), row_groups.end(), 0);
        PARQUET_THROW_NOT_OK(reader->GetRecordBatchReader(row_groups, &batch_reader));
        PARQUET_THROW_NOT_OK(arrow::ExportRecordBatchReader(batch_reader, stream_out));
    )
  }

  PARQUETSHARP_EXPORT void FileReader_Free(FileReader* reader)
  {
    delete reader;
  }
}
