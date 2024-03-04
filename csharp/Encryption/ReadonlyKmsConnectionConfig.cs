using System.Collections.Generic;

namespace ParquetSharp.Encryption
{
    /// <summary>
    /// Readonly version of KmsConnectionConfig. This is passed to KmsClient factories
    /// </summary>
    public class ReadonlyKmsConnectionConfig
    {
        internal ReadonlyKmsConnectionConfig(
            string kmsInstanceId,
            string kmsInstanceUrl,
            string keyAccessToken,
            IReadOnlyDictionary<string, string> customKmsConf)
        {
            KmsInstanceId = kmsInstanceId;
            KmsInstanceUrl = kmsInstanceUrl;
            KeyAccessToken = keyAccessToken;
            CustomKmsConf = customKmsConf;
        }

        /// <summary>
        /// ID of the KMS instance that will be used for encryption
        /// </summary>
        public string KmsInstanceId { get; }

        /// <summary>
        /// URL of the KMS instance
        /// </summary>
        public string KmsInstanceUrl { get; }

        /// <summary>
        /// Authorization token that will be passed to the KMS
        /// </summary>
        public string KeyAccessToken { get; }

        /// <summary>
        /// KMS-type-specific configuration
        /// </summary>
        public IReadOnlyDictionary<string, string> CustomKmsConf { get; }
    }
}
