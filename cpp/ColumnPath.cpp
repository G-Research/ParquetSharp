
#include "cpp/ParquetSharpExport.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <parquet/schema.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* ColumnPath_Make(const char** path, int length, std::shared_ptr<const schema::ColumnPath>** column_path)
	{
		TRYCATCH
		(
			const std::vector<std::string> v(path, path + length);
			*column_path = new std::shared_ptr<const schema::ColumnPath>(new schema::ColumnPath(v));
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnPath_MakeFromDotString(const char* const dot_string, std::shared_ptr<const schema::ColumnPath>** column_path)
	{
		TRYCATCH(*column_path = new std::shared_ptr<const schema::ColumnPath>(schema::ColumnPath::FromDotString(dot_string));)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnPath_MakeFromNode(const std::shared_ptr<const schema::Node>* const node, std::shared_ptr<const schema::ColumnPath>** column_path)
	{
		TRYCATCH(*column_path = new std::shared_ptr<const schema::ColumnPath>(schema::ColumnPath::FromNode(**node));)
	}

	PARQUETSHARP_EXPORT void ColumnPath_Free(const std::shared_ptr<const schema::ColumnPath>* column_path)
	{
		delete column_path;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnPath_Extend(const std::shared_ptr<const schema::ColumnPath>* const column_path, const char* const node_name, std::shared_ptr<const schema::ColumnPath>** new_column_path)
	{
		TRYCATCH(*new_column_path = new std::shared_ptr<const schema::ColumnPath>((*column_path)->extend(node_name));)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnPath_ToDotString(const std::shared_ptr<const schema::ColumnPath>* const column_path, const char** dot_string)
	{
		TRYCATCH(*dot_string = AllocateCString((*column_path)->ToDotString());)
	}

	PARQUETSHARP_EXPORT void ColumnPath_ToDotString_Free(const char* dot_string)
	{
		FreeCString(dot_string);
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnPath_ToDotVector(const std::shared_ptr<const schema::ColumnPath>* const column_path, char*** dot_vector, int* length)
	{
		TRYCATCH
		(
			const auto& v = (*column_path)->ToDotVector();
			auto const strings = new char*[v.size()];

			for (size_t i = 0; i != v.size(); ++i)
			{
				strings[i] = AllocateCString(v[i]);
			}

			*dot_vector = strings;
			*length = static_cast<int>(v.size());
		)
	}

	PARQUETSHARP_EXPORT void ColumnPath_ToDotVector_Free(const char** dot_vector, int length)
	{
		for (int i = 0; i != length; ++i)
		{
			FreeCString(dot_vector[i]);
		}
	}
}
