#pragma once

#include <exception>
#include <string>

struct ExceptionInfo final
{
	ExceptionInfo(const char* type, const char* message);
	ExceptionInfo(const std::exception& exception);
	~ExceptionInfo();

	const std::string Type;
	const std::string Message;
};

#define SINGLE_ARG(...) __VA_ARGS__
#define TRYCATCH(expression)												\
	try																		\
	{																		\
		expression															\
		return nullptr;														\
	}																		\
	catch (const std::exception& exception)									\
	{																		\
		return new ExceptionInfo(exception);								\
	}																		\
	catch (...)																\
	{																		\
		return new ExceptionInfo("unkown", "uncaught exception");			\
	}																		\


