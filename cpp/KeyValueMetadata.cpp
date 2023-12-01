
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <arrow/util/key_value_metadata.h>
#include <parquet/metadata.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT ExceptionInfo* KeyValueMetadata_MakeEmpty(std::shared_ptr<KeyValueMetadata>** key_value_metadata)
	{
		TRYCATCH(*key_value_metadata = new std::shared_ptr<KeyValueMetadata>(new KeyValueMetadata());)
	}

	PARQUETSHARP_EXPORT void KeyValueMetadata_Free(const std::shared_ptr<const KeyValueMetadata>* key_value_metadata)
	{
		delete key_value_metadata;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* KeyValueMetadata_Size(const std::shared_ptr<const KeyValueMetadata>* key_value_metadata, int64_t* size)
	{
		TRYCATCH(*size = (*key_value_metadata)->size();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* KeyValueMetadata_Append(const std::shared_ptr<KeyValueMetadata>* key_value_metadata, const char* key, const char* value)
	{
		TRYCATCH(
			(*key_value_metadata)->Append(key, value);
		)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* KeyValueMetadata_Get_Entries(const std::shared_ptr<const KeyValueMetadata>* key_value_metadata, const char*** keys, const char*** values)
	{
		TRYCATCH
		(
			const int64_t size = (*key_value_metadata)->size();

			std::unique_ptr<char*[]> keys_ptr(new char*[size]);
			std::unique_ptr<char*[]> values_ptr(new char*[size]);

			try
			{
				for (int64_t i = 0; i != size; ++i)
				{
					const std::string k = (*key_value_metadata)->key(i);
					const std::string v = (*key_value_metadata)->value(i);

					keys_ptr[i] = new char[k.length() + 1];
					values_ptr[i] = new char[v.length() + 1];

					std::memcpy(keys_ptr[i], k.c_str(), k.length() + 1);
					std::memcpy(values_ptr[i], v.c_str(), v.length() + 1);
				}
			}
			catch (...)
			{
				for (int64_t i = 0; i != size; ++i)
				{
					delete[] keys_ptr[i];
					delete[] values_ptr[i];
				}

				throw;
			}

			*keys = (const char**) keys_ptr.release();
			*values = (const char**) values_ptr.release();
		)
	}

	PARQUETSHARP_EXPORT void KeyValueMetadata_Free_Entries(const std::shared_ptr<const KeyValueMetadata>* key_value_metadata, const char** keys, const char** values)
	{
		const int64_t size = (*key_value_metadata)->size();

		for (int i = 0; i != size; ++i)
		{
			delete[] keys[i];
			delete[] values[i];
		}

		delete[] keys;
		delete[] values;
	}
}
