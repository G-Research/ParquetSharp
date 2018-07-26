
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/schema.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* SchemaDescriptor_Column(const SchemaDescriptor* descriptor, int i, const ColumnDescriptor** column_descriptor)
	{
		TRYCATCH(*column_descriptor = descriptor->Column(i);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* SchemaDescriptor_ColumnIndex_ByNode(const SchemaDescriptor* descriptor, std::shared_ptr<const schema::Node>* node, int* column_index)
	{
		TRYCATCH(*column_index = descriptor->ColumnIndex(**node);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* SchemaDescriptor_ColumnIndex_ByPath(const SchemaDescriptor* descriptor, const char* const path, int* column_index)
	{
		TRYCATCH(*column_index = descriptor->ColumnIndex(path);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* SchemaDescriptor_Get_Column_Root(const SchemaDescriptor* descriptor, int i, std::shared_ptr<const schema::Node>** column_root)
	{
		// Take a no-op ownership of the node, as all the rest of the API uses shared_ptr.
		TRYCATCH(*column_root = new std::shared_ptr<const schema::Node>(descriptor->GetColumnRoot(i), [](const schema::Node* ptr) {});)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* SchemaDescriptor_Group_Node(const SchemaDescriptor* descriptor, std::shared_ptr<const schema::GroupNode>** group_node)
	{
		// Take a no-op ownership of the node, as all the rest of the API uses shared_ptr.
		TRYCATCH(*group_node = new std::shared_ptr<const schema::GroupNode>(descriptor->group_node(), [](const schema::GroupNode* ptr) {});)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* SchemaDescriptor_Name(const SchemaDescriptor* descriptor, const char** name)
	{
		TRYCATCH(*name = descriptor->name().c_str();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* SchemaDescriptor_Num_Columns(const SchemaDescriptor* descriptor, int* num_columns)
	{
		TRYCATCH(*num_columns = descriptor->num_columns();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* SchemaDescriptor_Schema_Root(const SchemaDescriptor* descriptor, std::shared_ptr<const schema::Node>** schema_root)
	{
		TRYCATCH(*schema_root = new std::shared_ptr<const schema::Node>(descriptor->schema_root());)
	}
}
