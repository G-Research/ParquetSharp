
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/column_reader.h>

using namespace parquet;

extern "C"
{

#define DEFINE_TYPED_COLUMN_READER_METHODS(ParquetType, NativeType)								\
																								\
	PARQUETSHARP_EXPORT ExceptionInfo* TypedColumnReader_ReadBatch_##ParquetType(				\
		std::shared_ptr<ColumnReader>* columnReader, 											\
		int64_t batch_size, 																	\
		int16_t* def_levels, 																	\
		int16_t* rep_levels,																	\
		NativeType* values, 																	\
		int64_t* values_read,																	\
		int64_t* levels_read)																	\
	{																							\
        TRYCATCH(																				\
			*levels_read = static_cast<ParquetType##Reader&>(**columnReader).ReadBatch(			\
				batch_size, 																	\
				def_levels, 																	\
				rep_levels,																		\
				values, 																		\
				values_read);)																	\
	}																							\
																								\
	PARQUETSHARP_EXPORT ExceptionInfo* TypedColumnReader_Skip_##ParquetType(					\
		std::shared_ptr<ColumnReader>* columnReader,											\
		int64_t num_rows_to_skip,																\
		int64_t* levels_skipped)																\
	{																							\
        TRYCATCH(																				\
			*levels_skipped = static_cast<ParquetType##Reader&>(**columnReader).Skip(			\
				num_rows_to_skip);)																\
	}																							\


	DEFINE_TYPED_COLUMN_READER_METHODS(Bool, bool)
	DEFINE_TYPED_COLUMN_READER_METHODS(Int32, int32_t)
	DEFINE_TYPED_COLUMN_READER_METHODS(Int64, int64_t)
	DEFINE_TYPED_COLUMN_READER_METHODS(Int96, Int96)
	DEFINE_TYPED_COLUMN_READER_METHODS(Float, float)
	DEFINE_TYPED_COLUMN_READER_METHODS(Double, double)
	DEFINE_TYPED_COLUMN_READER_METHODS(ByteArray, ByteArray)
	DEFINE_TYPED_COLUMN_READER_METHODS(FixedLenByteArray, FixedLenByteArray)

}
