
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/statistics.h>

using namespace parquet;

extern "C"
{

#define DEFINE_TYPED_ROW_GROUP_STATISTICS(ParquetType, NativeType)                          \
                                                                                            \
    PARQUETSHARP_EXPORT ExceptionInfo* TypedRowGroupStatistics_Min_##ParquetType(           \
        const std::shared_ptr<RowGroupStatistics>* statistics,                              \
        NativeType* min)                                                                    \
    {                                                                                       \
        TRYCATCH(*min = static_cast<ParquetType##Statistics&>(**statistics).min();)         \
    }                                                                                       \
                                                                                            \
    PARQUETSHARP_EXPORT ExceptionInfo* TypedRowGroupStatistics_Max_##ParquetType(           \
        const std::shared_ptr<RowGroupStatistics>* statistics,                              \
        NativeType* max)                                                                    \
    {                                                                                       \
        TRYCATCH(*max = static_cast<ParquetType##Statistics&>(**statistics).max();)         \
    }                                                                                       \


    DEFINE_TYPED_ROW_GROUP_STATISTICS(Bool, bool)
    DEFINE_TYPED_ROW_GROUP_STATISTICS(Int32, int32_t)
    DEFINE_TYPED_ROW_GROUP_STATISTICS(Int64, int64_t)
    DEFINE_TYPED_ROW_GROUP_STATISTICS(Int96, Int96)
    DEFINE_TYPED_ROW_GROUP_STATISTICS(Float, float)
    DEFINE_TYPED_ROW_GROUP_STATISTICS(Double, double)
    DEFINE_TYPED_ROW_GROUP_STATISTICS(ByteArray, ByteArray)
    DEFINE_TYPED_ROW_GROUP_STATISTICS(FLBA, FixedLenByteArray)

}
