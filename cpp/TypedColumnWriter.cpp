
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/column_writer.h>

using namespace parquet;

extern "C"
{

#define DEFINE_TYPED_COLUMN_WRITER_METHODS(ParquetType, NativeType)                         \
                                                                                            \
    PARQUETSHARP_EXPORT ExceptionInfo* TypedColumnWriter_WriteBatch_##ParquetType(			\
        ColumnWriter* columnWriter,                                                         \
        int64_t num_values,                                                                 \
        const int16_t* def_levels,                                                          \
        const int16_t* rep_levels,                                                          \
        NativeType* values)                                                                 \
    {                                                                                       \
        TRYCATCH(                                                                           \
            static_cast<ParquetType##Writer&>(*columnWriter).WriteBatch(                    \
                num_values,                                                                 \
                def_levels,                                                                 \
                rep_levels,                                                                 \
                values);)                                                                   \
    }                                                                                       \
                                                                                            \
    PARQUETSHARP_EXPORT ExceptionInfo* TypedColumnWriter_WriteBatchSpaced_##ParquetType(	\
        ColumnWriter* columnWriter,                                                         \
        int64_t num_values,                                                                 \
        const int16_t* def_levels,                                                          \
        const int16_t* rep_levels,                                                          \
        const uint8_t* valid_bits,                                                          \
        int64_t valid_bits_offset,                                                          \
        const NativeType* values)                                                           \
    {                                                                                       \
        TRYCATCH(                                                                           \
            static_cast<ParquetType##Writer&>(*columnWriter).WriteBatchSpaced(              \
                num_values,                                                                 \
                def_levels,                                                                 \
                rep_levels,                                                                 \
                valid_bits,                                                                 \
                valid_bits_offset,                                                          \
                values);)                                                                   \
    }                                                                                       \


    DEFINE_TYPED_COLUMN_WRITER_METHODS(Bool, bool)
    DEFINE_TYPED_COLUMN_WRITER_METHODS(Int32, int32_t)
    DEFINE_TYPED_COLUMN_WRITER_METHODS(Int64, int64_t)
    DEFINE_TYPED_COLUMN_WRITER_METHODS(Int96, Int96)
    DEFINE_TYPED_COLUMN_WRITER_METHODS(Float, float)
    DEFINE_TYPED_COLUMN_WRITER_METHODS(Double, double)
    DEFINE_TYPED_COLUMN_WRITER_METHODS(ByteArray, ByteArray)
    DEFINE_TYPED_COLUMN_WRITER_METHODS(FixedLenByteArray, FixedLenByteArray)

}
