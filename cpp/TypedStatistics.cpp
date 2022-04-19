
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/statistics.h>

using namespace parquet;

extern "C"
{

	using Int96Statistics = TypedStatistics<Int96Type>;

#define DEFINE_TYPED_STATISTICS(ParquetType, NativeType)									\
                                                                                            \
    PARQUETSHARP_EXPORT ExceptionInfo* TypedStatistics_Min_##ParquetType(					\
        const std::shared_ptr<Statistics>* statistics,										\
        NativeType* min)                                                                    \
    {                                                                                       \
        TRYCATCH(*min = static_cast<ParquetType##Statistics&>(**statistics).min();)         \
    }                                                                                       \
                                                                                            \
    PARQUETSHARP_EXPORT ExceptionInfo* TypedStatistics_Max_##ParquetType(					\
        const std::shared_ptr<Statistics>* statistics,										\
        NativeType* max)                                                                    \
    {                                                                                       \
        TRYCATCH(*max = static_cast<ParquetType##Statistics&>(**statistics).max();)         \
    }                                                                                       \


	DEFINE_TYPED_STATISTICS(Bool, bool)
	DEFINE_TYPED_STATISTICS(Int32, int32_t)
	DEFINE_TYPED_STATISTICS(Int64, int64_t)
	DEFINE_TYPED_STATISTICS(Int96, Int96)
	DEFINE_TYPED_STATISTICS(Float, float)
	DEFINE_TYPED_STATISTICS(Double, double)
	DEFINE_TYPED_STATISTICS(ByteArray, ByteArray)
	DEFINE_TYPED_STATISTICS(FLBA, FixedLenByteArray)

}
