﻿namespace ParquetSharp.Encryption
{
    /// <summary>
    /// Interface for Key Management System (KMS) client implementations
    /// </summary>
    public interface IKmsClient
    {
        /// <summary>
        /// Wrap a key - encrypt it with the master key
        /// </summary>
        public string WrapKey(byte[] keyBytes, string masterKeyIdentifier);

        /// <summary>
        /// Unwrap a key - decrypt it with the master key
        /// </summary>
        public byte[] UnwrapKey(string wrappedKey, string masterKeyIdentifier);
    }
}
