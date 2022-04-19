
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/schema.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* PrimitiveNode_Make(
		const char* const name, 
		const Repetition::type repetition, 
		const std::shared_ptr<const LogicalType>* logical_type, 
		const Type::type primitive_type, 
		const int primitive_length, 
		std::shared_ptr<schema::Node>** primitive_node)
	{
		TRYCATCH(*primitive_node = new std::shared_ptr<schema::Node>(schema::PrimitiveNode::Make(name, repetition, logical_type == nullptr ? nullptr : *logical_type, primitive_type, primitive_length));)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* PrimitiveNode_Column_Order(const std::shared_ptr<schema::PrimitiveNode>* node, ColumnOrder::type* column_order)
	{
		TRYCATCH(*column_order = (*node)->column_order().get_order();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* PrimitiveNode_Physical_Type(const std::shared_ptr<schema::PrimitiveNode>* node, Type::type* physical_type)
	{
		TRYCATCH(*physical_type = (*node)->physical_type();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* PrimitiveNode_Type_Length(const std::shared_ptr<schema::PrimitiveNode>* node, int32_t* type_length)
	{
		TRYCATCH(*type_length = (*node)->type_length();)
	}
}
