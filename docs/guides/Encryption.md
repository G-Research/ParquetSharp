# Parquet Modular Encryption

The Parquet format supports [modular encryption](https://github.com/apache/parquet-format/blob/master/Encryption.md),
where different parts of a Parquet file can be encrypted separately.
This allows controlling access to data on a per-column basis for example,
or allowing some clients to read the file schema and metadata but not the column data.

ParquetSharp supports two approaches for working with encryption;
the recommended approach is to use the high-level Key Management Tools API,
but you may also use the lower-level API to configure data encryption keys directly.
Note that PyArrow only supports the Key Management Tools API,
so this should be used if compatibility with PyArrow is required.

## Key Management Tools

_This API was added in ParquetSharp 15.0.0_

The Key Management Tools API implements envelope encryption,
where data is encrypted with randomly generated data encryption keys (DEKs),
and the DEKs are encrypted with master encryption keys (MEKs).
The master encryption keys are managed by a Key Management Service (KMS).

If double wrapping is used, DEKs are first encrypted with key encryption keys (KEKs),
which are then encrypted with MEKs. The KEKs are cached to reduce interaction with the KMS.
Double wrapping is enabled by default.

For further details, see the
[Key Management Tools design document](https://docs.google.com/document/d/1bEu903840yb95k9q2X-BlsYKuXoygE4VnMDl9xz_zhk).

The Key Management Tools API is contained in the @ParquetSharp.Encryption namespace.
In order to use this API,
a client for a Key Management Service must be implemented:

```c#
using ParquetSharp.Encryption;

internal sealed class MyKmsClient : IKmsClient
{
    public MyKmsClient(ReadonlyKmsConnectionConfig config)
    {
        // KMS specific initialization using the connection configuration
    }

    public string WrapKey(byte[] keyBytes, string masterKeyIdentifier)
    {
        // Use the KMS to wrap (encrypt) keyBytes using the specified master key,
        // and return the wrapped key as a string that can be stored in the Parquet metadata.
    }

    public byte[] UnwrapKey(string wrappedKey, string masterKeyIdentifier)
    {
        // Use the KMS to unwrap the wrapped key using the specified master key
    }
}
```

The main entrypoint for the Key Management Tools API is the
@ParquetSharp.Encryption.CryptoFactory class.
This requires a factory method for creating KMS clients,
which are cached internally and periodically recreated:

```c#
using var cryptoFactory = new CryptoFactory(config => new MyKmsClient(config));
```

### Writing Encrypted Files

To write an encrypted Parquet file, the KMS connection is first configured:

```c#
using var kmsConnectionConfig = new KmsConnectionConfig();
// ParquetSharp doesn't require any config fields to be set,
// the fields needed will depend on the IKmsClient implementation
kmsConnectionConfig.KmsInstanceId = ...;
kmsConnectionConfig.KmsInstanceUrl = ...;
kmsConnectionConfig.KeyAccessToken = ...;
```

Then to configure how the file is encrypted, an @ParquetSharp.Encryption.EncryptionConfiguration is created:

```c#
string footerKeyId = ...;
using var encryptionConfig = new EncryptionConfiguration(footerKeyId);
```

You can specify that uniform encryption is used, in which case all columns
are encrypted with the same master key as the footer:

```c#
encryptionConfig.UniformEncryption = true;
```

Or you can specify master encryption keys to use per column:
```c#
encryptionConfig.ColumnKeys = new Dictionary<string, IReadOnlyList<string>>
{
    {"MasterKey1", new[] {"Column0", "Column1", "Column2"}},
    {"MasterKey2", new[] {"Column3", "Column4"}},
};
```

And you can configure whether double or single wrapping is used:

```c#
encryptionConfig.DoubleWrapping = false;  // Single-wrapping mode
```

You can also disable footer encryption, in which case the file schema and metadata
may be read by any user able to read the file:

```c#
encryptionConfig.PlaintextFooter = true;
```

The `kmsConnectionConfig` and `encryptionConfiguration` are used to generate
file encryption properties, which are used to build the @ParquetSharp.WriterProperties:

```c#
using var fileEncryptionProperties = cryptoFactory.GetFileEncryptionProperties(
    kmsConnectionConfig, encryptionConfig);

using var writerPropertiesBuilder = new WriterPropertiesBuilder();
using var writerProperties = writerPropertiesBuilder
                .Compression(Compression.Snappy)
                .Encryption(fileEncryptionProperties)
                .Build();
```

Finally, the Parquet file can be written using the @ParquetSharp.WriterProperties:

```c#
Column[] columns = ...;
using var fileWriter = new ParquetFileWriter(parquetFilePath, columns, writerProperties);
// Write data with fileWriter
```

### Reading Encrypted Files

Reading encrypted files requires creating @ParquetSharp.FileDecryptionProperties
with a @ParquetSharp.Encryption.CryptoFactory, and adding these to the
@ParquetSharp.ReaderProperties:

```c#
using var decryptionConfig = new DecryptionConfiguration();
using var fileDecryptionProperties = cryptoFactory.GetFileDecryptionProperties(
    kmsConnectionConfig, decryptionConfig);

using var readerProperties = ReaderProperties.GetDefaultReaderProperties();
readerProperties.FileDecryptionProperties = fileDecryptionProperties;

using var fileReader = new ParquetFileReader(parquetFilePath, readerProperties);
// Read data as normal
```

**Important**: The `CryptoFactory` instance used to generate the `FileDecryptionProperties`
must remain alive and not disposed until after the file has been read,
as internally the `FileDecryptionProperties` contains references to
data in the `CryptoFactory` that cannot be managed by ParquetSharp.
Failure to do so may result in native memory access violations and crashes that cannot be caught as exceptions.

### External Metadata and Key Rotation

Key material is stored inside the Parquet file metadata by default,
but key material can also be stored in separate JSON files alongside Parquet files,
to allow rotation of master keys without needing to rewrite the Parquet files.

This is configured in the @ParquetSharp.Encryption.EncryptionConfiguration:

```c#
using var encryptionConfig = new EncryptionConfiguration(footerKeyId);
encryptionConfig.InternalKeyMaterial = false;  // External key material
```

When using external key material, the path to the Parquet file being written or read
must be specified when creating @ParquetSharp.FileEncryptionProperties and
@ParquetSharp.FileDecryptionProperties:

```c#
using var fileEncryptionProperties = cryptoFactory.GetFileEncryptionProperties(
    kmsConnectionConfig, encryptionConfig, parquetFilePath);

// ...

using var fileDecryptionProperties = cryptoFactory.GetFileDecryptionProperties(
    kmsConnectionConfig, decryptionConfig, parquetFilePath);
```

After writing a Parquet file using external key material, master keys can be rotated with a `CryptoFactory`:

```c#
cryptoFactory.RotateMasterKeys(kmsConnectionConfig, parquetFilePath, doubleWrapping: true);
```

Key rotation requires that the KMS supports versioning, such that the old master key is
used when unwrapping a key, and the latest version is used when wrapping a key.

## Low-Level Encryption

It is also possible to directly specify the AES keys used for footer and column encryption
by creating `FileEncryptionProperties` and `FileDecryptionProperties` manually,
without using a `CryptoFactory`.
These properties can be then used as above, by building `WriterProperties` or `ReaderProperties`.

This example demonstrates creating encryption properties:

```c#
byte[] key0 = ...; // Bytes for 128, 192 or 256 bit AES key
byte[] key1 = ...;
byte[] key2 = ...;

// Use key0 as the footer key
using var builder = new FileEncryptionPropertiesBuilder(key0);

// Configure encryption for two columns, using different keys.
// Key metadata can be set in order to identify which key to use when later decrypting data.
using var col0Builder = new ColumnEncryptionPropertiesBuilder("Column0");
using var col0Properties = col0Builder.Key(key1).KeyMetadata("key1").Build();

using var col1Builder = new ColumnEncryptionPropertiesBuilder("Column1");
using var col1Properties = col1Builder.Key(key2).KeyMetadata("key2").Build();

using var fileEncryptionProperties = builder
    .FooterKeyMetadata("key0")
    .EncryptedColumns(new[]
    {
        col0Properties,
        col1Properties,
    })
    .Build();
```

Creating decryption properties works similarly:

```c#
using var builder = new FileDecryptionPropertiesBuilder();

using var col0Builder = new ColumnDecryptionPropertiesBuilder("Column0");
using var col0Properties = col0Builder.Key(key1).Build();

using var col1Builder = new ColumnDecryptionPropertiesBuilder("Column1");
using var col1Properties = col1Builder.Key(key2).Build();

using var fileDecryptionProperties = builder
    .FooterKey(key0)
    .ColumnKeys(new[] {col0Properties, col1Properties})
    .Build();
```

Rather than having to specify decryption keys directly, a
@ParquetSharp.DecryptionKeyRetriever can be used to retrieve keys
based on the key metadata, to allow more flexibility:

```c#
internal sealed class MyKeyRetriever : ParquetSharp.DecryptionKeyRetriever
{
    public override byte[] GetKey(string keyMetadata)
    {
        // Return AES key bytes based on the contents of the key metadata
    }
}
```

The `FileDecryptionProperties` are then built using the key retriever:

```c#
using var builder = new FileDecryptionPropertiesBuilder();
using var fileDecryptionProperties = builder
    .KeyRetriever(new MyKeyRetriever())
    .Build();
```

### AAD Verification

When using the lower-level encryption API, you may specify "additional authenticated data" (AAD)
to allow verifying that data has not been replaced with different data encrypted with the same key.
See the [Parquet format AAD documentation](https://github.com/apache/parquet-format/blob/master/Encryption.md#44-additional-authenticated-data)
for details of how this is implemented.

An AAD prefix can be specified when creating `FileEncryptionProperties`:

```c#
using var builder = new FileEncryptionPropertiesBuilder(key0);
using var fileEncryptionProperties = builder
    .FooterKeyMetadata("key0")
    .AadPrefix("expected-prefix")
    .Build();
```

And then the expected prefix should be provided when creating `FileDecryptionProperties`:

```c#
using var builder = new FileDecryptionPropertiesBuilder();
using var fileDecryptionProperties = builder
    .KeyRetriever(new MyKeyRetriever())
    .AadPrefix("expected-prefix")
    .Build();
```

If the AAD prefix doesn't match the expected prefix an exception will be thrown when reading the file.

Alternatively, you can implement an @ParquetSharp.AadPrefixVerifier if you have more complex verification logic:

```c#
internal sealed class MyAadVerifier : ParquetSharp.AadPrefixVerifier
{
    public override void Verify(string aadPrefix)
    {
        if (aadPrefix != "expected-prefix")
        {
            throw new Exception($"Got unexpected AAD prefix: {aadPrefix}");
        }
    }
}
```

And then provide an instance of this verifier when creating decryption properties:
```c#
using var builder = new FileDecryptionPropertiesBuilder();
using var fileDecryptionProperties = builder
    .KeyRetriever(new MyKeyRetriever())
    .AadPrefixVerifier(new MyAadVerifier())
    .Build();
```

## Arrow API Compatibility

Note that the above examples use the @ParquetSharp.ParquetFileReader and
@ParquetSharp.ParquetFileWriter classes, but encryption may also be used with the Arrow API.
The @ParquetSharp.Arrow.FileReader and @ParquetSharp.Arrow.FileWriter constructors
accept @ParquetSharp.ReaderProperties and @ParquetSharp.WriterProperties parameters
respectively, which can have encryption properties configured.
