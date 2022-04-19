
#include "cpp/ParquetSharpExport.h"
#include "ExceptionInfo.h"

#include <parquet/metadata.h>

using namespace parquet;

extern "C"
{
	PARQUETSHARP_EXPORT void ColumnCryptoMetaData_Free(const std::shared_ptr<ColumnCryptoMetaData>* column_crypto_meta_data)
	{
		delete column_crypto_meta_data;
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnCryptoMetaData_Path_In_Schema(const std::shared_ptr<const ColumnCryptoMetaData>* column_crypto_meta_data, const std::shared_ptr<const schema::ColumnPath>** column_path)
	{
		TRYCATCH(*column_path = new std::shared_ptr<const schema::ColumnPath>((*column_crypto_meta_data)->path_in_schema());)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnCryptoMetaData_Encrypted_With_Footer_Key(const std::shared_ptr<const ColumnCryptoMetaData>* column_crypto_meta_data, bool* encrypted_with_footer_key)
	{
		TRYCATCH(*encrypted_with_footer_key = (*column_crypto_meta_data)->encrypted_with_footer_key();)
	}

	PARQUETSHARP_EXPORT ExceptionInfo* ColumnCryptoMetaData_Key_Metadata(const std::shared_ptr<const ColumnCryptoMetaData>* column_crypto_meta_data, const char** key_metadata)
	{
		TRYCATCH(*key_metadata = (*column_crypto_meta_data)->key_metadata().c_str();)
	}
}
