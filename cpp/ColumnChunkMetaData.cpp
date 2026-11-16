
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/metadata.h>
#include <parquet/size_statistics.h>
#include <parquet/statistics.h>

using namespace parquet;

extern "C"
{
	// TODO native API that still needs to be ported.
	//const std::string& file_path() const;
	//std::shared_ptr<schema::ColumnPath> path_in_schema() const;

	//int64_t has_dictionary_page() const;
	//int64_t dictionary_page_offset() const;
	//int64_t data_page_offset() const;
	//int64_t index_page_offset() const;

	PARQUETSHARP_EXPORT void ColumnChunkMetaData_Free(const ColumnChunkMetaData* column_chunk_meta_data)
	{
		delete column_chunk_meta_data;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnChunkMetaData_Compression(const ColumnChunkMetaData* column_chunk_meta_data, Compression::type* compression)
	{
		TRYCATCH(*compression = column_chunk_meta_data->compression();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnChunkMetaData_CryptoMetadata(const ColumnChunkMetaData* column_chunk_meta_data, std::shared_ptr<const ColumnCryptoMetaData>** column_crypto_meta_data)
	{
		TRYCATCH
		(
			auto crypto = column_chunk_meta_data->crypto_metadata();
			*column_crypto_meta_data = crypto ? new std::shared_ptr<const ColumnCryptoMetaData>(std::move(crypto)) : nullptr;
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnChunkMetaData_Encodings(const ColumnChunkMetaData* column_chunk_meta_data, const Encoding::type** encodings)
	{
		TRYCATCH(*encodings = column_chunk_meta_data->encodings().data();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnChunkMetaData_Encodings_Count(const ColumnChunkMetaData* column_chunk_meta_data, size_t* count)
	{
		TRYCATCH(*count = column_chunk_meta_data->encodings().size();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnChunkMetaData_File_Offset(const ColumnChunkMetaData* column_chunk_meta_data, int64_t* file_offset)
	{
		TRYCATCH(*file_offset = column_chunk_meta_data->file_offset();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnChunkMetaData_Is_Stats_Set(const ColumnChunkMetaData* column_chunk_meta_data, bool* is_stats_set)
	{
		TRYCATCH(*is_stats_set = column_chunk_meta_data->is_stats_set();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnChunkMetaData_Num_Values(const ColumnChunkMetaData* column_chunk_meta_data, int64_t* num_values)
	{
		TRYCATCH(*num_values = column_chunk_meta_data->num_values();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnChunkMetaData_Statistics(const ColumnChunkMetaData* column_chunk_meta_data, std::shared_ptr<Statistics>** statistics)
	{
		TRYCATCH
		(
			const auto s = column_chunk_meta_data->statistics();
			*statistics = s ? new std::shared_ptr(s) : nullptr;
		)
	}

	// Fields are copied out individually rather than exposing parquet::SizeStatistics directly,
	// so that the ABI does not depend on the layout of the Arrow struct.
	// The histogram pointers remain valid for the lifetime of the column chunk metadata,
	// which holds the size statistics.
	PARQUETSHARP_EXPORT ExceptionInfo* ColumnChunkMetaData_Size_Statistics(
		const ColumnChunkMetaData* column_chunk_meta_data,
		bool* has_size_statistics,
		bool* has_unencoded_byte_array_data_bytes,
		int64_t* unencoded_byte_array_data_bytes,
		const int64_t** definition_level_histogram,
		size_t* definition_level_histogram_length,
		const int64_t** repetition_level_histogram,
		size_t* repetition_level_histogram_length)
	{
		TRYCATCH(
			const auto stats = column_chunk_meta_data->size_statistics();
			*has_size_statistics = stats != nullptr;
			*has_unencoded_byte_array_data_bytes = stats && stats->unencoded_byte_array_data_bytes.has_value();
			*unencoded_byte_array_data_bytes = *has_unencoded_byte_array_data_bytes ? *stats->unencoded_byte_array_data_bytes : 0;
			*definition_level_histogram = stats ? stats->definition_level_histogram.data() : nullptr;
			*definition_level_histogram_length = stats ? stats->definition_level_histogram.size() : 0;
			*repetition_level_histogram = stats ? stats->repetition_level_histogram.data() : nullptr;
			*repetition_level_histogram_length = stats ? stats->repetition_level_histogram.size() : 0;
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnChunkMetaData_Total_Compressed_Size(const ColumnChunkMetaData* column_chunk_meta_data, int64_t* total_compressed_size)
	{
		TRYCATCH(*total_compressed_size = column_chunk_meta_data->total_compressed_size();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnChunkMetaData_Total_Uncompressed_Size(const ColumnChunkMetaData* column_chunk_meta_data, int64_t* total_uncompressed_size)
	{
		TRYCATCH(*total_uncompressed_size = column_chunk_meta_data->total_uncompressed_size();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnChunkMetaData_Type(const ColumnChunkMetaData* column_chunk_meta_data, Type::type* type)
	{
		TRYCATCH(*type = column_chunk_meta_data->type();)
	}
}
