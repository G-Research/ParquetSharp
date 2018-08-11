
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/statistics.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT void RowGroupStatistics_Free(const std::shared_ptr<RowGroupStatistics>* statistics)
	{
		delete statistics;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupStatistics_Distinct_Count(const std::shared_ptr<RowGroupStatistics>* statistics, int64_t* distinct_count)
	{
		TRYCATCH(*distinct_count = (*statistics)->distinct_count();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupStatistics_HasMinMax(const std::shared_ptr<RowGroupStatistics>* statistics, bool* has_min_max)
	{
		TRYCATCH(*has_min_max = (*statistics)->HasMinMax();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupStatistics_Null_Count(const std::shared_ptr<RowGroupStatistics>* statistics, int64_t* null_count)
	{
		TRYCATCH(*null_count = (*statistics)->null_count();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupStatistics_Num_Values(const std::shared_ptr<RowGroupStatistics>* statistics, int64_t* num_values)
	{
		TRYCATCH(*num_values = (*statistics)->num_values();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* RowGroupStatistics_Physical_Type(const std::shared_ptr<RowGroupStatistics>* statistics, Type::type* physical_type)
	{
		TRYCATCH(*physical_type = (*statistics)->physical_type();)
	}
}
