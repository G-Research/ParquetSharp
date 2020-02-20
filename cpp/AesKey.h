#pragma once

#include <cstdint>
#include <string>

class AesKey final
{
public:

	AesKey() = default;
	
	explicit AesKey(const std::string& parquet_key)
	{
		std::copy(parquet_key.begin(), parquet_key.end(), reinterpret_cast<char*>(key_));
		size_ = static_cast<uint32_t>(parquet_key.size());
	}

	std::string ToParquetKey() const
	{
		return std::string(reinterpret_cast<const char*>(key_), size_);
	}

private:

	std::uint64_t key_[4]{};
	std::uint32_t size_{};
};
