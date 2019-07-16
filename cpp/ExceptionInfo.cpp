
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <typeinfo>

ExceptionInfo::ExceptionInfo(const char* const type, const char* const message)
	: Type(type), Message(message)
{
}

ExceptionInfo::ExceptionInfo(const std::exception& exception)
	: Type(typeid(exception).name()), Message(exception.what())
{
}

ExceptionInfo::~ExceptionInfo()
{
}

extern "C"
{
	PARQUETSHARP_EXPORT void ExceptionInfo_Free(const ExceptionInfo* exception_info)
	{
		delete exception_info;
	}

	PARQUETSHARP_EXPORT const char* ExceptionInfo_Type(const ExceptionInfo* exception_info)
	{
		return exception_info->Type.c_str();
	}

	PARQUETSHARP_EXPORT const char* ExceptionInfo_Message(const ExceptionInfo* exception_info)
	{
		return exception_info->Message.c_str();
	}
}