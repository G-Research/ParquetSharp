
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/schema.h>

using parquet::LogicalType;
using parquet::Repetition;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* GroupNode_Make(
		const char* const name, 
		const Repetition::type repetition, 
		const std::shared_ptr<parquet::schema::Node>** fields, 
		const int numFields, 
		const std::shared_ptr<const LogicalType>* logical_type, 
		std::shared_ptr<parquet::schema::Node>** groupNode)
	{
		TRYCATCH
		(
			parquet::schema::NodeVector nodes;

			for (int i = 0; i != numFields; ++i)
			{
				nodes.push_back(*fields[i]);
			}

			*groupNode = new std::shared_ptr(parquet::schema::GroupNode::Make(name, repetition, nodes, logical_type == nullptr ? nullptr : *logical_type));
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* GroupNode_Field(const std::shared_ptr<parquet::schema::GroupNode>* group_node, int i, std::shared_ptr<parquet::schema::Node>** field)
	{
		TRYCATCH(*field = new std::shared_ptr((*group_node)->field(i));)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* GroupNode_Field_Count(const std::shared_ptr<parquet::schema::GroupNode>* group_node, int* field_count)
	{
		TRYCATCH(*field_count = (*group_node)->field_count();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* GroupNode_Field_Index_By_Name(const std::shared_ptr<parquet::schema::GroupNode>* group_node, const char* const name, int* index)
	{
		TRYCATCH(*index = (*group_node)->FieldIndex(name);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* GroupNode_Field_Index_By_Node(const std::shared_ptr<parquet::schema::GroupNode>* group_node, const std::shared_ptr<const parquet::schema::Node>* node, int* index)
	{
		TRYCATCH(*index = (*group_node)->FieldIndex(**node);)
	}
}
