#pragma once

#include <exception>
#include <string>

#include <parquet/exception.h>

struct ExceptionInfo final
{
	ExceptionInfo(const char* type, const char* message);
	ExceptionInfo(const std::exception& exception);
	~ExceptionInfo();

	const std::string Type;
	const std::string Message;
};

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
  catch (const std::out_of_range& exception)                             \
  {                                                                      \
    return new ExceptionInfo(exception);                                 \
  }                                                                      \
  catch (const std::length_error& exception)                             \
  {                                                                      \
    return new ExceptionInfo(exception);                                 \
  }                                                                      \
  catch (const std::exception& exception)                                \
  {                                                                      \
    return new ExceptionInfo(exception);                                 \
  }                                                                      \
  catch (...)                                                            \
  {                                                                      \
    return new ExceptionInfo("unknown", "uncaught exception");           \
  }                                                                      \

