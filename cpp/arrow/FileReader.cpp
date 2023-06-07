#include <arrow/io/file.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <parquet/arrow/reader.h>

#include "cpp/ParquetSharpExport.h"
#include "../ExceptionInfo.h"

using namespace parquet::arrow;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* FileReader_OpenPath(
		const char* const path,
    FileReader** reader)
	{
    TRYCATCH
    (
      arrow::MemoryPool* pool = arrow::default_memory_pool();
      std::shared_ptr<arrow::io::ReadableFile> input_file;
      std::unique_ptr<FileReader> reader_ptr;
      PARQUET_ASSIGN_OR_THROW(input_file, arrow::io::ReadableFile::Open(path, pool));
      PARQUET_THROW_NOT_OK(OpenFile(input_file, pool, &reader_ptr));
      *reader = reader_ptr.release();
    )
	}

  PARQUETSHARP_EXPORT ExceptionInfo* FileReader_OpenFile(
      std::shared_ptr<::arrow::io::RandomAccessFile>* readable_file_interface,
      FileReader** reader)
  {
    TRYCATCH
    (
      arrow::MemoryPool* pool = arrow::default_memory_pool();
      std::unique_ptr<FileReader> reader_ptr;
      PARQUET_THROW_NOT_OK(OpenFile(*readable_file_interface, pool, &reader_ptr));
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

  PARQUETSHARP_EXPORT void FileReader_Free(FileReader* reader)
  {
    delete reader;
  }
}
