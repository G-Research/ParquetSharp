
#include <parquet/properties.h>

using namespace parquet;

namespace
{

	// Arrow does not offer guarantees around C++ ABI compatibility.
	// Since for the moment the C# API uses the same enum values as the C++ API, verify that they match.
	// This should stop us from releasing new versions of ParquetSharp with silent breaks in enum values.
	[[maybe_unused]] void CheckEnumsAbiCompatibility()
	{
		static_assert(ColumnOrder::UNDEFINED == 0);
		static_assert(ColumnOrder::TYPE_DEFINED_ORDER == 1);

		static_assert(Compression::UNCOMPRESSED == 0);
		static_assert(Compression::SNAPPY == 1);
		static_assert(Compression::GZIP == 2);
		static_assert(Compression::BROTLI == 3);
		static_assert(Compression::ZSTD == 4);
		static_assert(Compression::LZ4 == 5);
		static_assert(Compression::LZ4_FRAME == 6);
		static_assert(Compression::LZO == 7);
		static_assert(Compression::BZ2 == 8);
		static_assert(Compression::LZ4_HADOOP == 9);

		static_assert(Encoding::PLAIN == 0);
		static_assert(Encoding::PLAIN_DICTIONARY == 2);
		static_assert(Encoding::RLE == 3);
		static_assert(Encoding::BIT_PACKED == 4);
		static_assert(Encoding::DELTA_BINARY_PACKED == 5);
		static_assert(Encoding::DELTA_LENGTH_BYTE_ARRAY == 6);
		static_assert(Encoding::DELTA_BYTE_ARRAY == 7);
		static_assert(Encoding::RLE_DICTIONARY == 8);
		static_assert(Encoding::BYTE_STREAM_SPLIT == 9);
		static_assert(Encoding::UNDEFINED == 10);
		static_assert(Encoding::UNKNOWN == 999);

		static_assert(LogicalType::Type::UNDEFINED == 0);
		static_assert(LogicalType::Type::STRING == 1);
		static_assert(LogicalType::Type::MAP == 2);
		static_assert(LogicalType::Type::LIST == 3);
		static_assert(LogicalType::Type::ENUM == 4);
		static_assert(LogicalType::Type::DECIMAL == 5);
		static_assert(LogicalType::Type::DATE == 6);
		static_assert(LogicalType::Type::TIME == 7);
		static_assert(LogicalType::Type::TIMESTAMP == 8);
		static_assert(LogicalType::Type::INTERVAL == 9);
		static_assert(LogicalType::Type::INT == 10);
		static_assert(LogicalType::Type::NIL == 11);
		static_assert(LogicalType::Type::JSON == 12);
		static_assert(LogicalType::Type::BSON == 13);
		static_assert(LogicalType::Type::UUID == 14);
		static_assert(LogicalType::Type::NONE == 15);

		static_assert(ParquetCipher::AES_GCM_V1 == 0);
		static_assert(ParquetCipher::AES_GCM_CTR_V1 == 1);

		static_assert(ParquetVersion::PARQUET_1_0 == 0);
		ARROW_SUPPRESS_DEPRECATION_WARNING
		static_assert(ParquetVersion::PARQUET_2_0 == 1);
		ARROW_UNSUPPRESS_DEPRECATION_WARNING
		static_assert(ParquetVersion::PARQUET_2_4 == 2);
		static_assert(ParquetVersion::PARQUET_2_6 == 3);
		static_assert(ParquetVersion::PARQUET_2_LATEST == 3);

		static_assert(Type::BOOLEAN == 0);
		static_assert(Type::INT32 == 1);
		static_assert(Type::INT64 == 2);
		static_assert(Type::INT96 == 3);
		static_assert(Type::FLOAT == 4);
		static_assert(Type::DOUBLE == 5);
		static_assert(Type::BYTE_ARRAY == 6);
		static_assert(Type::FIXED_LEN_BYTE_ARRAY == 7);
		static_assert(Type::UNDEFINED == 8);

		static_assert(Repetition::REQUIRED == 0);
		static_assert(Repetition::OPTIONAL == 1);
		static_assert(Repetition::REPEATED == 2);
		static_assert(Repetition::UNDEFINED == 3);

		static_assert(SortOrder::SIGNED == 0);
		static_assert(SortOrder::UNSIGNED == 1);
		static_assert(SortOrder::UNKNOWN == 2);

		static_assert(LogicalType::TimeUnit::UNKNOWN == 0);
		static_assert(LogicalType::TimeUnit::MILLIS == 1);
		static_assert(LogicalType::TimeUnit::MICROS == 2);
		static_assert(LogicalType::TimeUnit::NANOS == 3);
	}

}