#pragma once

#include <cstdint>
#include <string>
#include "arrow/util/secure_string.h"
using ::arrow::util::SecureString;

class AesKey final
{
public:

	AesKey() = default;
	
	explicit AesKey(const std::string& parquet_key)
	{
		std::copy(parquet_key.begin(), parquet_key.end(), reinterpret_cast<char*>(key_));
		size_ = static_cast<uint32_t>(parquet_key.size());
	}

	SecureString ToParquetKey() const
	{
	    std::string tmp(reinterpret_cast<const char*>(key_), size_);
		return SecureString(std::move(tmp));
	}

private:

	std::uint64_t key_[4]{};
	std::uint32_t size_{};
};
