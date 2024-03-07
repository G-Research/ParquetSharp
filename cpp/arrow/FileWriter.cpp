#include <arrow/io/file.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>

#include "cpp/ParquetSharpExport.h"
#include "../ExceptionInfo.h"

using namespace parquet::arrow;

extern "C"
{
  PARQUETSHARP_EXPORT ExceptionInfo* FileWriter_OpenPath(
    const char* const path,
    struct ArrowSchema* schema,
    const std::shared_ptr<parquet::WriterProperties>* writer_properties,
    const std::shared_ptr<parquet::ArrowWriterProperties>* arrow_writer_properties,
    FileWriter** writer_out)
  {
    TRYCATCH
    (
      arrow::MemoryPool* pool = arrow::default_memory_pool();

      std::shared_ptr<::arrow::io::OutputStream> output_stream;
      PARQUET_ASSIGN_OR_THROW(output_stream, ::arrow::io::FileOutputStream::Open(path));

      std::shared_ptr<parquet::WriterProperties> properties = writer_properties == nullptr
          ? parquet::default_writer_properties()
          : *writer_properties;

      std::shared_ptr<parquet::ArrowWriterProperties> arrow_properties = arrow_writer_properties == nullptr
          ? parquet::default_arrow_writer_properties()
          : *arrow_writer_properties;

      std::shared_ptr<arrow::Schema> imported_schema;
      PARQUET_ASSIGN_OR_THROW(imported_schema, arrow::ImportSchema(schema));

      std::unique_ptr<FileWriter> writer;
      PARQUET_ASSIGN_OR_THROW(writer, FileWriter::Open(
          *imported_schema, pool, output_stream, properties, arrow_properties));

      *writer_out = writer.release();
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* FileWriter_OpenStream(
      std::shared_ptr<::arrow::io::OutputStream>* output_stream,
      struct ArrowSchema* schema,
      const std::shared_ptr<parquet::WriterProperties>* writer_properties,
      const std::shared_ptr<parquet::ArrowWriterProperties>* arrow_writer_properties,
      FileWriter** writer_out)
  {
    TRYCATCH
    (
      arrow::MemoryPool* pool = arrow::default_memory_pool();

      std::shared_ptr<parquet::WriterProperties> properties = writer_properties == nullptr
          ? parquet::default_writer_properties()
          : *writer_properties;

      std::shared_ptr<parquet::ArrowWriterProperties> arrow_properties = arrow_writer_properties == nullptr
          ? parquet::default_arrow_writer_properties()
          : *arrow_writer_properties;

      std::shared_ptr<arrow::Schema> imported_schema;
      PARQUET_ASSIGN_OR_THROW(imported_schema, arrow::ImportSchema(schema));

      std::unique_ptr<FileWriter> writer;
      PARQUET_ASSIGN_OR_THROW(writer, FileWriter::Open(
          *imported_schema, pool, *output_stream, properties, arrow_properties));

      *writer_out = writer.release();
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* FileWriter_GetSchema(FileWriter* writer, struct ArrowSchema* schema_out)
  {
    TRYCATCH
    (
      std::shared_ptr<arrow::Schema> schema = writer->schema();
      PARQUET_THROW_NOT_OK(arrow::ExportSchema(*schema, schema_out));
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* FileWriter_WriteTable(
      FileWriter* writer, struct ArrowArrayStream* stream, int64_t chunk_size)
  {
    TRYCATCH
    (
      std::shared_ptr<arrow::RecordBatchReader> reader;
      PARQUET_ASSIGN_OR_THROW(reader, arrow::ImportRecordBatchReader(stream));
      std::shared_ptr<arrow::Table> table;
      PARQUET_ASSIGN_OR_THROW(table, reader->ToTable());
      PARQUET_THROW_NOT_OK(writer->WriteTable(*table, chunk_size));
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* FileWriter_WriteRecordBatches(
      FileWriter* writer, struct ArrowArrayStream* stream, int64_t chunk_size)
  {
    TRYCATCH
    (
        std::shared_ptr<arrow::RecordBatchReader> reader;
        PARQUET_ASSIGN_OR_THROW(reader, arrow::ImportRecordBatchReader(stream));
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        PARQUET_ASSIGN_OR_THROW(batches, reader->ToRecordBatches());
        for (const auto& batch : batches)
        {
          PARQUET_THROW_NOT_OK(writer->WriteRecordBatch(*batch));
        }
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* FileWriter_NewRowGroup(FileWriter* writer, int64_t chunk_size)
  {
    TRYCATCH(PARQUET_THROW_NOT_OK(writer->NewRowGroup(chunk_size));)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* FileWriter_NewBufferedRowGroup(FileWriter* writer)
  {
    TRYCATCH(PARQUET_THROW_NOT_OK(writer->NewBufferedRowGroup());)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* FileWriter_WriteColumnChunk(
      FileWriter* writer, struct ArrowArray* c_array, struct ArrowSchema* c_array_type)
  {
    TRYCATCH
    (
      std::shared_ptr<arrow::DataType> array_type;
      PARQUET_ASSIGN_OR_THROW(array_type, arrow::ImportType(c_array_type));
      std::shared_ptr<arrow::Array> array;
      PARQUET_ASSIGN_OR_THROW(array, arrow::ImportArray(c_array, array_type));
      PARQUET_THROW_NOT_OK(writer->WriteColumnChunk(*array));
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* FileWriter_WriteChunkedColumnChunk(
      FileWriter* writer, struct ArrowArrayStream* stream)
  {
    TRYCATCH
    (
      std::shared_ptr<arrow::RecordBatchReader> reader;
      PARQUET_ASSIGN_OR_THROW(reader, arrow::ImportRecordBatchReader(stream));
      std::shared_ptr<arrow::Table> table;
      PARQUET_ASSIGN_OR_THROW(table, reader->ToTable());
      if (table->num_columns() != 1)
      {
        throw parquet::ParquetException("Expected a single column for column chunk");
      }
      std::shared_ptr<arrow::ChunkedArray> array = table->column(0);
      PARQUET_THROW_NOT_OK(writer->WriteColumnChunk(array));
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* FileWriter_Close(FileWriter* writer)
  {
    TRYCATCH(PARQUET_THROW_NOT_OK(writer->Close());)
  }

  PARQUETSHARP_EXPORT void FileWriter_Free(FileWriter* writer)
  {
    delete writer;
  }
}
