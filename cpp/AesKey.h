#pragma once

#include <cstdint>
#include <string>
#include <arrow/util/secure_string.h>

class AesKey final
{
public:

	AesKey() = default;

	explicit AesKey(const arrow::util::SecureString& secure_key)
	{
		const auto view = secure_key.as_view();
		std::copy(view.begin(), view.end(), reinterpret_cast<char*>(key_));
		size_ = static_cast<uint32_t>(view.size());
	}

	arrow::util::SecureString ToParquetKey() const
	{
		return arrow::util::SecureString(std::string(reinterpret_cast<const char*>(key_), size_));
	}

private:

	std::uint64_t key_[4]{};
	std::uint32_t size_{};
};
