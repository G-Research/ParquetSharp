
#include "cpp/ParquetSharpExport.h"
#include "CString.h"
#include "ExceptionInfo.h"

#include <parquet/types.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT void LogicalType_Free(const std::shared_ptr<const LogicalType>* logical_type)
	{
		delete logical_type;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_Type(const std::shared_ptr<const LogicalType>* logical_type, LogicalType::Type::type* type)
	{
		TRYCATCH(*type = (*logical_type)->type();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_Equals(const std::shared_ptr<const LogicalType>* left, const std::shared_ptr<const LogicalType>* right, bool* equals)
	{
		TRYCATCH(*equals = (*left)->Equals(**right);)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_ToString(const std::shared_ptr<const LogicalType>* logical_type, const char** to_string)
	{
		TRYCATCH(*to_string = AllocateCString((*logical_type)->ToString());)
	}

	PARQUETSHARP_EXPORT void LogicalType_ToString_Free(const char* to_string)
	{
		FreeCString(to_string);
	}

	// Logical type constructors.

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_String(const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::String());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_Map(const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::Map());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_List(const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::List());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_Enum(const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::Enum());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_Decimal(const int32_t precision, const int32_t scale, const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::Decimal(precision, scale));)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_Date(const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::Date());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_Time(const bool is_adjusted_to_utc, const LogicalType::TimeUnit::unit time_unit, const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::Time(is_adjusted_to_utc, time_unit));)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_Timestamp(const bool is_adjusted_to_utc, const LogicalType::TimeUnit::unit time_unit, const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::Timestamp(is_adjusted_to_utc, time_unit));)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_Interval(const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::Interval());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_Int(const int32_t bit_width, const bool is_signed, const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::Int(bit_width, is_signed));)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_Null(const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::Null());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_JSON(const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::JSON());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_BSON(const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::BSON());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_UUID(const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::UUID());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* LogicalType_None(const std::shared_ptr<const LogicalType>** logical_type)
	{
		TRYCATCH(*logical_type = new std::shared_ptr(LogicalType::None());)
	}

	// Typed properties
	PARQUETSHARP_EXPORT ExceptionInfo* DecimalLogicalType_Precision(const std::shared_ptr<const DecimalLogicalType>* logical_type, int32_t* precision)
	{
		TRYCATCH(*precision = (*logical_type)->precision();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* DecimalLogicalType_Scale(const std::shared_ptr<const DecimalLogicalType>* logical_type, int32_t* scale)
	{
		TRYCATCH(*scale = (*logical_type)->scale();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* TimeLogicalType_IsAdjustedToUtc(const std::shared_ptr<const TimeLogicalType>* logical_type, bool* is_adjusted_to_utc)
	{
		TRYCATCH(*is_adjusted_to_utc = (*logical_type)->is_adjusted_to_utc();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* TimeLogicalType_TimeUnit(const std::shared_ptr<const TimeLogicalType>* logical_type, LogicalType::TimeUnit::unit* time_unit)
	{
		TRYCATCH(*time_unit = (*logical_type)->time_unit();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* TimestampLogicalType_IsAdjustedToUtc(const std::shared_ptr<const TimestampLogicalType>* logical_type, bool* is_adjusted_to_utc)
	{
		TRYCATCH(*is_adjusted_to_utc = (*logical_type)->is_adjusted_to_utc();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* TimestampLogicalType_TimeUnit(const std::shared_ptr<const TimestampLogicalType>* logical_type, LogicalType::TimeUnit::unit* time_unit)
	{
		TRYCATCH(*time_unit = (*logical_type)->time_unit();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* IntLogicalType_BitWidth(const std::shared_ptr<const IntLogicalType>* logical_type, int32_t* bitWidth)
	{
		TRYCATCH(*bitWidth = (*logical_type)->bit_width();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* IntLogicalType_IsSigned(const std::shared_ptr<const IntLogicalType>* logical_type, bool* is_signed)
	{
		TRYCATCH(*is_signed = (*logical_type)->is_signed();)
	}
}
