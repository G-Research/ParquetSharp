
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/schema.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT void Node_Free(const std::shared_ptr<const schema::Node>* node)
	{
		delete node;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* Node_Equals(const std::shared_ptr<const schema::Node>* node, const std::shared_ptr<const schema::Node>* other, bool* equals)
	{
		TRYCATCH(*equals = (*node)->Equals(other->get());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* Node_Field_Id(const std::shared_ptr<const schema::Node>* node, int* id)
	{
		TRYCATCH(*id = (*node)->field_id();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* Node_Logical_Type(const std::shared_ptr<const schema::Node>* node, const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr((*node)->logical_type());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* Node_Name(const std::shared_ptr<const schema::Node>* node, const char** name)
	{
		TRYCATCH(*name = (*node)->name().c_str();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* Node_Node_Type(const std::shared_ptr<const schema::Node>* node, schema::Node::type* node_type)
	{
		TRYCATCH(*node_type = (*node)->node_type();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* Node_Parent(const std::shared_ptr<const schema::Node>* node, const std::shared_ptr<const schema::Node>** parent)
	{
		TRYCATCH
		(
			// Take a no-op ownership of the node, as all the rest of the API uses shared_ptr.
			auto p = (*node)->parent();
			*parent = (p == nullptr) ? nullptr : new std::shared_ptr<const schema::Node>(p, [](const schema::Node* ptr) {});
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* Node_Path(const std::shared_ptr<const schema::Node>* node, std::shared_ptr<const schema::ColumnPath>** path)
	{
		TRYCATCH(*path = new std::shared_ptr<const schema::ColumnPath>((*node)->path());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* Node_Repetition(const std::shared_ptr<const schema::Node>* node, Repetition::type* repetition)
	{
		TRYCATCH(*repetition = (*node)->repetition();)
	}
}
