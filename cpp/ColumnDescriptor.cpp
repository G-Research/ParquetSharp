
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/schema.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ColumnDescriptor_Max_Definition_Level(const ColumnDescriptor* column_descriptor, int16_t* max_definition_level)
	{
		TRYCATCH(*max_definition_level = column_descriptor->max_definition_level();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnDescriptor_Max_Repetition_Level(const ColumnDescriptor* column_descriptor, int16_t* max_repetition_level)
	{
		TRYCATCH(*max_repetition_level = column_descriptor->max_repetition_level();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnDescriptor_Physical_Type(const ColumnDescriptor* column_descriptor, Type::type* physical_type)
	{
		TRYCATCH(*physical_type = column_descriptor->physical_type();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnDescriptor_Logical_Type(const ColumnDescriptor* column_descriptor, const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr<const LogicalType>(column_descriptor->logical_type());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnDescriptor_ColumnOrder(const ColumnDescriptor* column_descriptor, ColumnOrder::type* column_order)
	{
		TRYCATCH(*column_order = column_descriptor->column_order().get_order();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnDescriptor_SortOrder(const ColumnDescriptor* column_descriptor, SortOrder::type* sort_order)
	{
		TRYCATCH(*sort_order = column_descriptor->sort_order();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnDescriptor_Name(const ColumnDescriptor* column_descriptor, const char** name)
	{
		TRYCATCH(*name = column_descriptor->name().c_str();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnDescriptor_Path(const ColumnDescriptor* column_descriptor, std::shared_ptr<schema::ColumnPath>** path)
	{
		TRYCATCH(*path = new std::shared_ptr<schema::ColumnPath>(column_descriptor->path());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnDescriptor_Schema_Node(const ColumnDescriptor* column_descriptor, std::shared_ptr<schema::Node>** schema_node)
	{
		TRYCATCH(*schema_node = new std::shared_ptr<schema::Node>(column_descriptor->schema_node());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnDescriptor_Type_Length(const ColumnDescriptor* column_descriptor, int* type_length)
	{
		TRYCATCH(*type_length = column_descriptor->type_length();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnDescriptor_Type_Precision(const ColumnDescriptor* column_descriptor, int* type_precision)
	{
		TRYCATCH(*type_precision = column_descriptor->type_precision();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnDescriptor_Type_Scale(const ColumnDescriptor* column_descriptor, int* type_scale)
	{
		TRYCATCH(*type_scale = column_descriptor->type_scale();)
	}

	//const std::shared_ptr<schema::ColumnPath> path() const;
}
