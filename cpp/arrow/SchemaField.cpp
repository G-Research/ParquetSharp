#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <parquet/arrow/schema.h>
#include <parquet/exception.h>

#include "cpp/ParquetSharpExport.h"
#include "../ExceptionInfo.h"

using namespace parquet::arrow;

extern "C"
{
  PARQUETSHARP_EXPORT ExceptionInfo* SchemaField_ChildrenLength(const SchemaField* field, int32_t* length)
  {
    TRYCATCH(*length = static_cast<int32_t>(field->children.size());)
  }

  PARQUETSHARP_EXPORT ExceptionInfo* SchemaField_Child(const SchemaField* field, int32_t index, const SchemaField** child)
  {
    TRYCATCH(
      if (index >= static_cast<int32_t>(field->children.size()))
      {
        throw std::runtime_error("Child field index out of range");
      }
      *child = &(field->children[index]);
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* SchemaField_ColumnIndex(const SchemaField* field, int32_t* column_index)
  {
    TRYCATCH(
      *column_index = field->column_index;
    )
  }

  PARQUETSHARP_EXPORT ExceptionInfo* SchemaField_Field(const SchemaField* field, struct ArrowSchema* arrow_field)
  {
    TRYCATCH(
      PARQUET_THROW_NOT_OK(arrow::ExportField(*(field->field), arrow_field));
    )
  }
}
