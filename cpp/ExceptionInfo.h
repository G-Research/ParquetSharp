#pragma once

#include <exception>
#include <string>
#ifndef _MSC_VER
#include <cxxabi.h>
#endif

#include <parquet/exception.h>

struct ExceptionInfo final
{
	ExceptionInfo(const char* type, const char* message);
	ExceptionInfo(const std::exception& exception);
	~ExceptionInfo();

	const std::string Type;
	const std::string Message;
};

#ifdef _MSC_VER
#define GET_EXCEPTION_NAME() "Unknown exception"
#else
#define GET_EXCEPTION_NAME() abi::__cxa_current_exception_type()->name()
#endif

#define SINGLE_ARG(...) __VA_ARGS__
#define TRYCATCH(expression)                                             \
  try                                                                    \
  {                                                                      \
    expression                                                           \
    return nullptr;                                                      \
  }                                                                      \
  catch (const std::bad_alloc& exception)                                \
  {                                                                      \
    return new ExceptionInfo("OutOfMemoryException", exception.what());  \
  }                                                                      \
  catch (const parquet::ParquetStatusException& exception)               \
  {                                                                      \
    return exception.status().IsOutOfMemory()                            \
      ? new ExceptionInfo("OutOfMemoryException", exception.what())      \
      : new ExceptionInfo(exception);                                    \
  }                                                                      \
  catch (const std::exception& exception)                                \
  {                                                                      \
    return new ExceptionInfo(exception);                                 \
  }                                                                      \
  catch (...)                                                            \
  {                                                                      \
    return new ExceptionInfo("unknown", GET_EXCEPTION_NAME());           \
  }                                                                      \

